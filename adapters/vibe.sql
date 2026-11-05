-- varve Mistral Vibe CLI adapter. Reads Mistral Vibe CLI session logs under
-- ~/.vibe/logs/session/session_*/ (VARVE_VIBE_ROOT overrides) and derives
-- the four normalized per-runtime staging tables (_vibe_sessions, _vibe_messages,
-- _vibe_api_calls, _vibe_tool_calls) plus the file->session->date map (_vibe_files)
-- for incremental delta detection. sql/stage.sql unions these into the shared stg_* tables.
--
-- Depends on sql/prelude.sql (run first): _attr_rules, _pricing, varve_harness(),
-- varve_campaign(). Variables (set by the justfile):
--   vibe_meta       : glob $VARVE_VIBE_ROOT/logs/session/session_*/meta.json
--   vibe_messages   : glob $VARVE_VIBE_ROOT/logs/session/session_*/messages.jsonl
--   json_max_object : maximum JSON line buffer in bytes

-- ---------------------------------------------------------------------------
-- Raw metadata scan from session_*/meta.json.
CREATE OR REPLACE TABLE _vibe_meta_raw AS
SELECT
  filename,
  regexp_extract(filename, '/logs/session/([^/]+)/meta\.json$', 1)            AS dir_key,
  coalesce(nullif(session_id::varchar, ''),
           regexp_extract(filename, '/logs/session/([^/]+)/meta\.json$', 1))  AS session_id,
  nullif(parent_session_id::varchar, '')                                      AS parent_session_id,
  try_cast(regexp_replace(start_time::varchar, '([+-][0-9]{2}:[0-9]{2}|Z)$', '') AS timestamp) AS started_at,
  try_cast(regexp_replace(end_time::varchar, '([+-][0-9]{2}:[0-9]{2}|Z)$', '') AS timestamp)   AS ended_at,
  git_commit::varchar                                                         AS git_commit,
  git_branch::varchar                                                         AS git_branch,
  environment->>'working_directory'                                           AS cwd,
  title::varchar                                                              AS title,
  coalesce(nullif(config->>'active_model', ''), 'mistral-vibe-unknown')       AS model,
  coalesce(try_cast(stats->>'session_prompt_tokens' AS bigint), 0)             AS prompt_tokens,
  coalesce(try_cast(stats->>'session_completion_tokens' AS bigint), 0)         AS completion_tokens,
  try_cast(stats->>'session_cost' AS double)                                  AS inline_cost
FROM read_json(
  getvariable('vibe_meta'),
  format                  = 'auto',
  columns                 = {
    session_id: 'VARCHAR',
    parent_session_id: 'VARCHAR',
    start_time: 'VARCHAR',
    end_time: 'VARCHAR',
    git_commit: 'VARCHAR',
    git_branch: 'VARCHAR',
    environment: 'JSON',
    title: 'VARCHAR',
    config: 'JSON',
    stats: 'JSON'
  },
  filename                = true,
  ignore_errors           = true,
  maximum_object_size     = getvariable('json_max_object'),
  map_inference_threshold = -1
)
WHERE regexp_extract(filename, '/logs/session/([^/]+)/meta\.json$', 1) <> '';

-- ---------------------------------------------------------------------------
-- Raw messages scan from session_*/messages.jsonl.
CREATE OR REPLACE TABLE _vibe_messages_raw AS
SELECT
  filename,
  regexp_extract(filename, '/logs/session/([^/]+)/messages\.jsonl$', 1) AS dir_key,
  row_number() OVER (PARTITION BY filename)                            AS line_no,
  role::varchar                                                        AS role,
  content::varchar                                                     AS content,
  coalesce(injected::boolean, false)                                   AS injected,
  message_id::varchar                                                  AS message_id,
  reasoning_content::varchar                                           AS reasoning_content,
  reasoning_message_id::varchar                                        AS reasoning_message_id,
  tool_calls::json                                                     AS tool_calls,
  name::varchar                                                        AS name,
  tool_call_id::varchar                                                AS tool_call_id
FROM read_json(
  getvariable('vibe_messages'),
  format                  = 'newline_delimited',
  columns                 = {
    role: 'VARCHAR',
    content: 'VARCHAR',
    injected: 'BOOLEAN',
    message_id: 'VARCHAR',
    reasoning_content: 'VARCHAR',
    reasoning_message_id: 'VARCHAR',
    tool_calls: 'JSON',
    name: 'VARCHAR',
    tool_call_id: 'VARCHAR'
  },
  filename                = true,
  ignore_errors           = true,
  maximum_object_size     = getvariable('json_max_object'),
  map_inference_threshold = -1
)
WHERE regexp_extract(filename, '/logs/session/([^/]+)/messages\.jsonl$', 1) <> '';

-- ---------------------------------------------------------------------------
-- sessions: one row per session from meta.json, matched against _attr_rules.
CREATE OR REPLACE TABLE _vibe_sessions AS
WITH matched AS (
  SELECT
    m.*,
    r.estate,
    r.harness AS harness_tok,
    r.campaign AS campaign_tok
  FROM _vibe_meta_raw m
  JOIN _attr_rules r
    ON coalesce(m.cwd, '') LIKE replace(r.cwd_glob, '*', '%')
  QUALIFY row_number() OVER (PARTITION BY m.session_id ORDER BY r.priority) = 1
)
SELECT
  session_id,
  'vibe'                                AS runtime,
  cwd,
  varve_harness(cwd, harness_tok)       AS harness,
  estate,
  varve_campaign(cwd, campaign_tok)     AS campaign,
  started_at,
  ended_at,
  parent_session_id,
  strftime(started_at, '%Y-%m-%d')      AS date
FROM matched
WHERE started_at IS NOT NULL;

-- Helper map linking directory key to resolved session metadata.
CREATE OR REPLACE TABLE _vibe_session_meta AS
SELECT
  m.dir_key,
  m.filename AS meta_path,
  m.model,
  m.prompt_tokens,
  m.completion_tokens,
  m.inline_cost,
  s.session_id,
  s.runtime,
  s.cwd,
  s.harness,
  s.estate,
  s.campaign,
  s.started_at,
  s.ended_at,
  s.parent_session_id,
  s.date
FROM _vibe_meta_raw m
JOIN _vibe_sessions s ON s.session_id = m.session_id;

-- ---------------------------------------------------------------------------
-- messages: human turns (user) and assistant turns (assistant). Monotonic
-- timestamp per line ensures downstream ASOF joins preserve turn order.
CREATE OR REPLACE TABLE _vibe_messages AS
WITH filtered AS (
  SELECT
    coalesce(nullif(m.message_id, ''), s.session_id || ':' || m.line_no) AS message_id,
    s.session_id,
    m.role,
    m.line_no,
    (s.parent_session_id IS NOT NULL)                                    AS is_sidechain,
    s.started_at + (m.line_no - 1) * INTERVAL 1 MILLISECOND              AS timestamp,
    m.role                                                               AS record_type,
    NULL::varchar                                                        AS api_message_id,
    nullif(m.content, '')                                                AS content,
    nullif(m.reasoning_content, '')                                      AS vibe_reasoning,
    'vibe'                                                               AS runtime,
    s.date
  FROM _vibe_messages_raw m
  JOIN _vibe_session_meta s ON s.dir_key = m.dir_key
  WHERE m.role IN ('user', 'assistant')
    AND coalesce(m.injected, false) = false
)
SELECT
  message_id,
  session_id,
  role,
  lag(message_id) OVER (PARTITION BY session_id ORDER BY line_no)        AS parent_id,
  is_sidechain,
  timestamp,
  record_type,
  api_message_id,
  content,
  vibe_reasoning,
  runtime,
  date
FROM filtered;

-- ---------------------------------------------------------------------------
-- api_calls: per-session token usage and inline/computed cost from meta.json.
CREATE OR REPLACE TABLE _vibe_api_calls AS
SELECT
  s.session_id,
  NULL::varchar                                      AS message_id,
  NULL::varchar                                      AS api_message_id,
  NULL::varchar                                      AS request_id,
  s.started_at                                       AS timestamp,
  s.model,
  'mistral'                                          AS provider,
  s.prompt_tokens                                    AS input,
  s.completion_tokens                                AS output,
  0::bigint                                          AS cache_read,
  0::bigint                                          AS cache_write,
  coalesce(
    s.inline_cost,
    CASE WHEN p.model IS NOT NULL
         THEN round(s.prompt_tokens * coalesce(p.input_cost, 0) + s.completion_tokens * coalesce(p.output_cost, 0), 6)
         ELSE 0 END
  )                                                  AS cost_usd,
  (s.inline_cost IS NULL AND p.model IS NULL)        AS unpriced_model,
  (s.parent_session_id IS NOT NULL)                  AS is_sidechain,
  'vibe'                                             AS runtime,
  s.date
FROM _vibe_session_meta s
LEFT JOIN _pricing p ON s.model = p.model
WHERE s.prompt_tokens > 0 OR s.completion_tokens > 0 OR s.inline_cost IS NOT NULL;

-- ---------------------------------------------------------------------------
-- tool_calls: assistant tool_calls unnested and paired with role='tool' results.
CREATE OR REPLACE TABLE _vibe_tool_calls AS
WITH unnested AS (
  SELECT
    req.dir_key,
    req.line_no                                                          AS req_line_no,
    req.message_id                                                       AS req_message_id,
    tc_idx,
    tc
  FROM _vibe_messages_raw req,
  LATERAL (
    SELECT unnest(cast(req.tool_calls AS json[]))                       AS tc,
           generate_subscripts(cast(req.tool_calls AS json[]), 1)        AS tc_idx
  )
  WHERE req.role = 'assistant'
    AND req.tool_calls IS NOT NULL
    AND json_type(req.tool_calls) = 'ARRAY'
),
paired AS (
  SELECT
    u.dir_key,
    u.req_line_no,
    u.req_message_id,
    u.tc_idx,
    u.tc,
    res.line_no                                                          AS res_line_no,
    res.name                                                             AS res_name,
    res.content                                                          AS res_content
  FROM unnested u
  LEFT JOIN _vibe_messages_raw res
    ON res.dir_key = u.dir_key
   AND res.role = 'tool'
   AND res.tool_call_id = json_extract_string(u.tc, '$.id')
  QUALIFY row_number() OVER (PARTITION BY u.dir_key, u.req_line_no, u.tc_idx ORDER BY res.line_no) = 1
),
timed AS (
  SELECT
    s.session_id,
    coalesce(nullif(p.req_message_id, ''), s.session_id || ':' || p.req_line_no) AS message_id,
    coalesce(json_extract_string(p.tc, '$.id'), s.session_id || ':' || p.req_line_no || ':' || p.tc_idx) AS tool_use_id,
    coalesce(json_extract_string(p.tc, '$.function.name'), p.res_name)   AS tool_name,
    json_extract_string(p.tc, '$.function.arguments')                    AS arguments,
    s.started_at + (p.req_line_no - 1) * INTERVAL 1 MILLISECOND         AS started_at,
    CASE WHEN p.res_line_no IS NOT NULL
         THEN s.started_at + (p.res_line_no - 1) * INTERVAL 1 MILLISECOND
    END                                                                  AS ended_at,
    p.res_line_no,
    p.res_content,
    (s.parent_session_id IS NOT NULL)                                    AS is_sidechain,
    'vibe'                                                               AS runtime,
    s.date
  FROM paired p
  JOIN _vibe_session_meta s ON s.dir_key = p.dir_key
)
SELECT
  session_id,
  message_id,
  tool_use_id,
  tool_name,
  started_at,
  ended_at,
  CASE WHEN ended_at IS NOT NULL AND ended_at >= started_at
       THEN datediff('millisecond', started_at, ended_at)
  END                                                                    AS duration_ms,
  CASE
    WHEN coalesce(res_content, '') ILIKE '%rejected by user%'
      OR coalesce(res_content, '') ILIKE '%user rejected%'
      OR coalesce(res_content, '') ILIKE '%user denied%'
      OR coalesce(res_content, '') ILIKE '%hook denied%'
      OR coalesce(res_content, '') ILIKE '%Tool execution rejected%'     THEN 'denied'
    WHEN regexp_matches(coalesce(res_content, ''), 'returncode:\s*[1-9][0-9]*')
      OR coalesce(res_content, '') ILIKE 'Error:%'                       THEN 'error'
    WHEN res_line_no IS NOT NULL                                         THEN 'completed'
    ELSE 'running'
  END                                                                    AS decision,
  arguments,
  res_content                                                            AS result_text,
  is_sidechain,
  runtime,
  date
FROM timed
WHERE tool_name IS NOT NULL;

-- ---------------------------------------------------------------------------
-- file -> session -> date map for incremental delta detection.
CREATE OR REPLACE TABLE _vibe_files AS
SELECT DISTINCT path, session_id, date
FROM (
  SELECT m.filename AS path, s.session_id, s.date
  FROM _vibe_meta_raw m
  JOIN _vibe_sessions s ON s.session_id = m.session_id
  WHERE m.filename IS NOT NULL
  UNION ALL
  SELECT msg.filename AS path, s.session_id, s.date
  FROM _vibe_messages_raw msg
  JOIN _vibe_session_meta s ON s.dir_key = msg.dir_key
  WHERE msg.filename IS NOT NULL
);

DROP TABLE IF EXISTS _vibe_session_meta;
DROP TABLE IF EXISTS _vibe_messages_raw;
DROP TABLE IF EXISTS _vibe_meta_raw;
