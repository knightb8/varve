-- varve opencode adapter (W2). Reads opencode's SQLite store via DuckDB's sqlite
-- extension and derives the four normalized per-runtime staging tables
-- (_oc_sessions, _oc_messages, _oc_api_calls, _oc_tool_calls) plus a
-- file->session->date map (_oc_files). sql/stage.sql unions these into stg_*.
--
-- The justfile ATTACHes the read-only snapshot as schema `oc` BEFORE running this
-- file (ATTACH needs a literal path, which the justfile holds): it clones
-- opencode.db (+ -wal/-shm) with an APFS copy-on-write clone (`cp -c`, instant,
-- no extra space) so ATTACH READ_ONLY can neither write nor lock the operator's
-- live opencode session -- the live store is provably untouched.
--
-- Depends on sql/prelude.sql (run first): _attr_rules, _pricing, varve_harness(),
-- varve_campaign(). Variables (set by the justfile):
--   opencode_db : path to the LIVE db, recorded verbatim in _oc_files so the
--                 manifest change-detection keys on the real file (not the clone).
--   repo_dir    : this repo root.
--
-- Source shape (supporting both OpenCode V1 and V2 stores):
--   * OpenCode V1 contract tables: session / message / part.
--   * OpenCode V2 tables: session_v2 / session_message (since early October 2026,
--     OpenCode writes new sessions exclusively to session_v2 and session_message).
--   * Sessions: read from session_v2, unioning session for any older sessions
--     not present in session_v2 (deduplicated by session_id).
--   * Messages: read from session_message for sessions not present in message;
--     read from message + part for V1 sessions. Content is aggregated from text/
--     reasoning elements.
--   * API calls: assistant turns carrying rolled-up cost + tokens.
--     In V1: message data carrying modelID, providerID, tokens, cost.
--     In V2: session_message data carrying model.{id, providerID}, tokens, cost.
--   * Tool calls:
--     In V1: part data where type = 'tool', carrying callID, tool, state.{status, time}.
--     In V2: session_message assistant content elements where type = 'tool',
--     carrying id/name/state.status.
--
-- Cost (cost:0 defense, ADR-0006). Assistant data.cost is authoritative when
-- present and > 0 -> use it. Some rows record cost 0 (the historic opencode
-- discrepancy, and GitLab-Duo / local models like strata or ollama that are $0).
-- When the inline cost is 0/absent AND the model is in the pricing seed, compute
-- from tokens x seed. When 0/absent AND unpriced (Duo/local/gpt/gemini not in the
-- seed), keep 0 and flag unpriced_model -- fabricating a per-token cost for a
-- local or subscription model would be wrong. (RFC-0001 records this.)
--
-- (`oc` is ATTACHed read-only by the justfile before this file runs.)

-- ---------------------------------------------------------------------------
-- sessions: one row per session. cwd = directory; attribution via the shared
-- rules/macros. parent_session_id populated from the session parent chain.
CREATE OR REPLACE TABLE _oc_sessions AS
WITH raw_sessions AS (
  SELECT id::varchar                       AS session_id,
         directory                         AS cwd,
         to_timestamp(time_created/1000.0) AS started_at,
         parent_id::varchar                AS parent_session_id,
         version                           AS oc_version,
         agent                             AS agent
  FROM oc.session_v2
  WHERE time_created IS NOT NULL
  UNION ALL
  SELECT id::varchar                       AS session_id,
         directory                         AS cwd,
         to_timestamp(time_created/1000.0) AS started_at,
         parent_id::varchar                AS parent_session_id,
         version                           AS oc_version,
         agent                             AS agent
  FROM oc.session
  WHERE time_created IS NOT NULL
    AND id NOT IN (SELECT id FROM oc.session_v2)
),
base AS (
  SELECT * FROM raw_sessions
),
matched AS (
  SELECT b.*, r.estate, r.harness AS harness_tok, r.campaign AS campaign_tok
  FROM base b
  JOIN _attr_rules r
    ON coalesce(b.cwd,'') LIKE replace(r.cwd_glob, '*', '%')
  QUALIFY row_number() OVER (PARTITION BY b.session_id ORDER BY r.priority) = 1
)
SELECT
  session_id,
  'opencode'                            AS runtime,
  cwd,
  varve_harness(cwd, harness_tok)       AS harness,
  estate,
  varve_campaign(cwd, campaign_tok)     AS campaign,
  started_at::timestamp                 AS started_at,
  parent_session_id,
  oc_version,
  agent,
  strftime(started_at, '%Y-%m-%d')      AS date
FROM matched;

-- ---------------------------------------------------------------------------
-- messages: one row per message (all roles). Content inlined from the text/
-- reasoning parts (RFC-0001: inline content enables W7 fts/vss; opencode stores
-- text in `part`, not on the message, or inside session_message content).
-- is_sidechain is false -- opencode has no sidechain flag; its parent/child
-- structure lives at SESSION grain (parent_id).
CREATE OR REPLACE TABLE _oc_messages AS
WITH txt AS (
  SELECT message_id,
         string_agg(json_extract_string(data, '$.text'), '\n') AS content
  FROM oc.part
  WHERE json_extract_string(data, '$.type') IN ('text','reasoning')
    AND json_extract_string(data, '$.text') IS NOT NULL
  GROUP BY message_id
),
v1_msgs AS (
  SELECT
    m.id::varchar                                    AS message_id,
    m.session_id::varchar                            AS session_id,
    json_extract_string(m.data, '$.role')            AS role,
    json_extract_string(m.data, '$.parentID')        AS parent_id,
    false                                            AS is_sidechain,
    to_timestamp(m.time_created/1000.0)::timestamp   AS timestamp,
    json_extract_string(m.data, '$.role')            AS record_type,
    NULL::varchar                                    AS api_message_id,
    txt.content                                      AS content
  FROM oc.message m
  LEFT JOIN txt ON txt.message_id = m.id
),
v2_msgs AS (
  SELECT
    sm.id::varchar                                   AS message_id,
    sm.session_id::varchar                           AS session_id,
    sm.type                                          AS role,
    NULL::varchar                                    AS parent_id,
    false                                            AS is_sidechain,
    to_timestamp(coalesce(json_extract(sm.data, '$.time.created')::bigint, sm.time_created)/1000.0)::timestamp AS timestamp,
    sm.type                                          AS record_type,
    NULL::varchar                                    AS api_message_id,
    CASE
      WHEN sm.type = 'user' THEN json_extract_string(sm.data, '$.text')
      WHEN sm.type = 'assistant' THEN (
        SELECT string_agg(json_extract_string(elem.value, '$.text'), E'\n')
        FROM json_each(json_extract(sm.data, '$.content')) elem
        WHERE json_extract_string(elem.value, '$.type') IN ('text', 'reasoning')
          AND json_extract_string(elem.value, '$.text') IS NOT NULL
      )
      ELSE NULL
    END                                              AS content
  FROM oc.session_message sm
  WHERE sm.type IN ('user', 'assistant')
    AND sm.session_id NOT IN (SELECT DISTINCT session_id FROM oc.message)
),
all_msgs AS (
  SELECT * FROM v1_msgs
  UNION ALL
  SELECT * FROM v2_msgs
)
SELECT
  m.message_id,
  m.session_id,
  m.role,
  m.parent_id,
  m.is_sidechain,
  m.timestamp,
  m.record_type,
  m.api_message_id,
  m.content,
  'opencode'                                       AS runtime,
  ss.date                                          AS date
FROM all_msgs m
JOIN _oc_sessions ss ON m.session_id = ss.session_id;

-- ---------------------------------------------------------------------------
-- api_calls: the ccusage UsageEntry spine. Grain = one assistant MESSAGE (the
-- rolled-up cost/tokens live there; per-step step-finish parts are not read to
-- avoid double counting -- RFC-0001). cache read/write map to the nested
-- tokens.cache.{read,write}.
CREATE OR REPLACE TABLE _oc_api_calls AS
WITH asst_v1 AS (
  SELECT
    m.session_id::varchar                                        AS session_id,
    m.id::varchar                                                AS message_id,
    to_timestamp(json_extract(m.data,'$.time.created')::bigint/1000.0)::timestamp AS timestamp,
    json_extract_string(m.data,'$.modelID')                      AS model,
    json_extract_string(m.data,'$.providerID')                   AS provider,
    coalesce(json_extract(m.data,'$.tokens.input')::bigint,0)        AS input,
    coalesce(json_extract(m.data,'$.tokens.output')::bigint,0)       AS output,
    coalesce(json_extract(m.data,'$.tokens.cache.read')::bigint,0)   AS cache_read,
    coalesce(json_extract(m.data,'$.tokens.cache.write')::bigint,0)  AS cache_write,
    try_cast(json_extract(m.data,'$.cost') AS double)            AS inline_cost
  FROM oc.message m
  WHERE json_extract_string(m.data,'$.role') = 'assistant'
),
asst_v2 AS (
  SELECT
    sm.session_id::varchar                                       AS session_id,
    sm.id::varchar                                               AS message_id,
    to_timestamp(coalesce(json_extract(sm.data,'$.time.created')::bigint, sm.time_created)/1000.0)::timestamp AS timestamp,
    coalesce(json_extract_string(sm.data,'$.model.id'), json_extract_string(sm.data,'$.modelID')) AS model,
    coalesce(json_extract_string(sm.data,'$.model.providerID'), json_extract_string(sm.data,'$.providerID')) AS provider,
    coalesce(json_extract(sm.data,'$.tokens.input')::bigint,0)        AS input,
    coalesce(json_extract(sm.data,'$.tokens.output')::bigint,0)       AS output,
    coalesce(json_extract(sm.data,'$.tokens.cache.read')::bigint,0)   AS cache_read,
    coalesce(json_extract(sm.data,'$.tokens.cache.write')::bigint,0)  AS cache_write,
    try_cast(json_extract(sm.data,'$.cost') AS double)            AS inline_cost
  FROM oc.session_message sm
  WHERE sm.type = 'assistant'
    AND sm.session_id NOT IN (SELECT DISTINCT session_id FROM oc.message)
),
asst AS (
  SELECT * FROM asst_v1
  UNION ALL
  SELECT * FROM asst_v2
)
SELECT
  a.session_id,
  a.message_id,
  NULL::varchar                              AS api_message_id,
  NULL::varchar                              AS request_id,
  a.timestamp,
  a.model,
  a.provider,
  a.input, a.output, a.cache_read, a.cache_write,
  -- cost:0 defense: trust inline cost when > 0; else compute from seed if the
  -- model is priced; else keep the runtime's 0 (unpriced/subscription).
  CASE
    WHEN a.inline_cost IS NOT NULL AND a.inline_cost > 0 THEN a.inline_cost
    WHEN p.model IS NOT NULL THEN round(
        a.input       * coalesce(p.input_cost,0)
      + a.output      * coalesce(p.output_cost,0)
      + a.cache_read  * coalesce(p.cache_read_cost,0)
      + a.cache_write * coalesce(p.cache_creation_cost,0), 6)
    ELSE coalesce(a.inline_cost, 0)
  END                                        AS cost_usd,
  (p.model IS NULL)                          AS unpriced_model,
  'opencode'                                 AS runtime,
  ss.date                                    AS date
FROM asst a
LEFT JOIN _pricing p    ON a.model = p.model
JOIN _oc_sessions ss    ON a.session_id = ss.session_id;

-- ---------------------------------------------------------------------------
-- tool_calls: one row per tool part or content block. opencode V1 tool parts
-- record tool timing (started_at/ended_at from state.time.{start,end}, duration_ms).
-- V2 records tool invocations inside assistant message content.
CREATE OR REPLACE TABLE _oc_tool_calls AS
WITH tool_v1 AS (
  SELECT
    session_id::varchar                                    AS session_id,
    message_id::varchar                                    AS message_id,
    json_extract_string(data,'$.callID')                   AS tool_use_id,
    json_extract_string(data,'$.tool')                     AS tool_name,
    json_extract(data,'$.state.time.start')::bigint        AS t_start,
    json_extract(data,'$.state.time.end')::bigint          AS t_end,
    json_extract_string(data,'$.state.status')             AS decision
  FROM oc.part
  WHERE json_extract_string(data,'$.type') = 'tool'
),
tool_v2 AS (
  SELECT
    sm.session_id::varchar                                 AS session_id,
    sm.id::varchar                                         AS message_id,
    coalesce(json_extract_string(elem.value,'$.id'), json_extract_string(elem.value,'$.callID')) AS tool_use_id,
    coalesce(json_extract_string(elem.value,'$.name'), json_extract_string(elem.value,'$.tool')) AS tool_name,
    json_extract(elem.value,'$.state.time.start')::bigint  AS t_start,
    json_extract(elem.value,'$.state.time.end')::bigint    AS t_end,
    json_extract_string(elem.value,'$.state.status')       AS decision
  FROM oc.session_message sm,
       json_each(json_extract(sm.data, '$.content')) elem
  WHERE sm.type = 'assistant'
    AND json_extract_string(elem.value, '$.type') = 'tool'
    AND sm.session_id NOT IN (SELECT DISTINCT session_id FROM oc.message)
),
tool AS (
  SELECT * FROM tool_v1
  UNION ALL
  SELECT * FROM tool_v2
)
SELECT
  t.session_id,
  t.message_id,
  t.tool_use_id,
  t.tool_name,
  to_timestamp(t.t_start/1000.0)::timestamp                AS started_at,
  to_timestamp(t.t_end/1000.0)::timestamp                  AS ended_at,
  CASE WHEN t.t_start IS NOT NULL AND t.t_end IS NOT NULL
       THEN (t.t_end - t.t_start)::bigint END              AS duration_ms,
  t.decision,
  'opencode'                                               AS runtime,
  ss.date                                                  AS date
FROM tool t
JOIN _oc_sessions ss ON t.session_id = ss.session_id;

-- ---------------------------------------------------------------------------
-- file -> session -> date map. opencode is a single db file, so the LIVE db path
-- (not the snapshot clone) maps to EVERY opencode session's date: a change to
-- the db marks all opencode date partitions for rewrite. The manifest keys on
-- opencode_db so change-detection tracks the real file.
CREATE OR REPLACE TABLE _oc_files AS
SELECT DISTINCT getvariable('opencode_db') AS path, session_id, date
FROM _oc_sessions;
