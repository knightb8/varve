-- varve Google Antigravity adapter. Reads Antigravity CLI / IDE conversation
-- transcripts under ~/.gemini/antigravity-cli/brain/<id>/.system_generated/logs/
-- (VARVE_AGY_ROOT overrides) and derives the four normalized per-runtime staging
-- tables (_agy_sessions, _agy_messages, _agy_api_calls, _agy_tool_calls) plus
-- the file->session->date map (_agy_files) for incremental delta detection.
-- sql/stage.sql unions these into the shared stg_* tables.
--
-- Source shape (validated against 290 local sessions / 111k steps, 2026-11-05):
--   * Path: $VARVE_AGY_ROOT/brain/<conversation-id>/.system_generated/logs/
--           transcript.jsonl (compact; truncates large fields with a
--           `truncated_fields` marker) and transcript_full.jsonl (untruncated).
--           When both exist for a session, transcript_full.jsonl is preferred;
--           transcript.jsonl is the fallback when transcript_full.jsonl is absent.
--   * Each line is a step object: {step_index, source, type, status, created_at,
--     content, thinking, tool_calls, input_tokens, cache_read_tokens,
--     output_tokens, exit_code, error, error_code, media, truncated_fields}.
--   * Top-level sessions begin at step_index=0 with source='USER_EXPLICIT',
--     type='USER_INPUT'. Subagent sessions begin at step_index=0 with
--     source='SYSTEM', type='SYSTEM_MESSAGE' carrying `sender=<parent-uuid>`.
--   * USER_INPUT wraps operator prompts in `<USER_REQUEST>...</USER_REQUEST>`
--     followed by `<ADDITIONAL_METADATA>` and optional `<USER_SETTINGS_CHANGE>`
--     (which records model selection transitions). The wrapper is stripped for
--     `messages.content` so downstream lexical/rewrite correction detectors see
--     clean operator prose.
--   * PLANNER_RESPONSE carries `tool_calls` as an array of `{name, args}` (or
--     inline `{id, tool_name, arguments, status, result}`), while execution
--     outcomes (`status`, `exit_code`, `error`, `error_code`, `content`) are
--     recorded on the immediately following MODEL step(s) (step_index + k).
--
-- Depends on sql/prelude.sql (run first): _attr_rules, _pricing, varve_harness(),
-- varve_campaign(). Variables (set by the justfile):
--   agy_source      : glob of transcript*.jsonl files under brain/
--   json_max_object : maximum JSON line buffer in bytes

CREATE OR REPLACE TABLE _agy_scan AS
SELECT
  regexp_extract(filename, '/brain/([^/]+)/', 1)               AS session_id,
  filename,
  step_index::bigint                                           AS step_index,
  source::varchar                                              AS source,
  type::varchar                                                AS type,
  status::varchar                                              AS status,
  try_cast(replace(created_at::varchar, 'Z', '') AS timestamp) AS ts,
  content::varchar                                             AS content,
  thinking::varchar                                            AS thinking,
  tool_calls::json                                             AS tool_calls,
  input_tokens::bigint                                         AS input_tokens,
  cache_read_tokens::bigint                                    AS cache_read_tokens,
  output_tokens::bigint                                        AS output_tokens,
  exit_code::bigint                                            AS exit_code,
  error::varchar                                               AS error,
  error_code::varchar                                          AS error_code
FROM read_json(
  getvariable('agy_source'),
  format                  = 'newline_delimited',
  columns                 = {
    step_index: 'BIGINT',
    source: 'VARCHAR',
    type: 'VARCHAR',
    status: 'VARCHAR',
    created_at: 'VARCHAR',
    content: 'VARCHAR',
    thinking: 'VARCHAR',
    tool_calls: 'JSON',
    input_tokens: 'BIGINT',
    cache_read_tokens: 'BIGINT',
    output_tokens: 'BIGINT',
    exit_code: 'BIGINT',
    error: 'VARCHAR',
    error_code: 'VARCHAR'
  },
  filename                = true,
  ignore_errors           = true,
  maximum_object_size     = getvariable('json_max_object'),
  map_inference_threshold = -1
)
WHERE regexp_extract(filename, '/brain/([^/]+)/', 1) <> '';

-- Prefer transcript_full.jsonl per session when present; fall back to transcript.jsonl.
CREATE OR REPLACE TABLE _agy_raw AS
WITH pref AS (
  SELECT session_id,
         max_by(filename, CASE WHEN filename LIKE '%/transcript_full.jsonl' THEN 2 ELSE 1 END) AS chosen_file
  FROM (SELECT DISTINCT session_id, filename FROM _agy_scan)
  GROUP BY session_id
)
SELECT s.*
FROM _agy_scan s
JOIN pref p ON s.session_id = p.session_id AND s.filename = p.chosen_file
WHERE s.step_index IS NOT NULL AND s.ts IS NOT NULL
QUALIFY row_number() OVER (PARTITION BY s.session_id, s.step_index ORDER BY s.ts DESC) = 1;

-- ---------------------------------------------------------------------------
-- sessions: one row per conversation directory under brain/<session_id>/.
-- Resolves parent_session_id from step 0 SYSTEM_MESSAGE `sender=<uuid>` and
-- cwd from workspace metadata, repo paths in tool calls, or initial prompts.
CREATE OR REPLACE TABLE _agy_sessions AS
WITH base AS (
  SELECT
    session_id,
    min(ts) AS started_at,
    max(ts) AS ended_at,
    max(nullif(regexp_extract(content, 'sender=([0-9a-zA-Z_-]+)', 1), ''))
      FILTER (WHERE step_index = 0 AND source = 'SYSTEM') AS parent_session_id,
    coalesce(
      max(nullif(trim(regexp_extract(content, 'Command Working Directory:[ \t]*([^\r\n<]+)', 1)), '')),
      max(nullif(trim(regexp_extract(content, '"workspaceUris":[ \t\r\n]*\[[ \t\r\n]*"file://([^"\\]+)', 1)), '')),
      max(nullif(regexp_extract(coalesce(tool_calls::varchar, '') || ' ' || coalesce(content, ''),
        '(/(?:var/home|home|Users|workspaces|workspace)/[^"''\s<>\\]+/(?:gitlab\.com|github\.com)/[^/"''\s<>\\]+/[^/"''\s<>\\]+)', 1), '')),
      max(nullif(regexp_extract(coalesce(tool_calls::varchar, '') || ' ' || coalesce(content, ''),
        '((?:/var)?/home/[^/"''\s<>]+/[^/"''\s<>]+/[^/"''\s<>]+)', 1), '')),
      max(nullif(regexp_extract(coalesce(tool_calls::varchar, ''),
        '"(?:Path|DirectoryPath)"\s*:\s*"\\"?(/[^"\\]+/[^/"\\]+)', 1), ''))
    ) AS cwd
  FROM _agy_raw
  GROUP BY session_id
  HAVING min(ts) IS NOT NULL
),
matched AS (
  SELECT b.*, r.estate, r.harness AS harness_tok, r.campaign AS campaign_tok
  FROM base b
  JOIN _attr_rules r
    ON coalesce(b.cwd, '') LIKE replace(r.cwd_glob, '*', '%')
  QUALIFY row_number() OVER (PARTITION BY b.session_id ORDER BY r.priority) = 1
)
SELECT
  session_id,
  'antigravity'                         AS runtime,
  cwd,
  varve_harness(cwd, harness_tok)       AS harness,
  estate,
  varve_campaign(cwd, campaign_tok)     AS campaign,
  started_at,
  ended_at,
  parent_session_id,
  strftime(started_at, '%Y-%m-%d')      AS date
FROM matched;

-- ---------------------------------------------------------------------------
-- messages: human turns (USER_INPUT) and assistant turns (PLANNER_RESPONSE).
-- Strips `<USER_REQUEST>...</USER_REQUEST>` wrapper from USER_INPUT so
-- downstream correction detectors match operator speech directly.
CREATE OR REPLACE TABLE _agy_messages AS
SELECT
  r.session_id || ':' || r.step_index                                     AS message_id,
  r.session_id,
  CASE
    WHEN r.source = 'USER_EXPLICIT' OR r.type = 'USER_INPUT' THEN 'user'
    WHEN r.type = 'PLANNER_RESPONSE'                         THEN 'assistant'
    ELSE 'system'
  END                                                                     AS role,
  CASE WHEN r.step_index > 0
       THEN r.session_id || ':' || (r.step_index - 1) END                 AS parent_id,
  (s.parent_session_id IS NOT NULL)                                       AS is_sidechain,
  r.ts                                                                    AS timestamp,
  r.type                                                                  AS record_type,
  NULL::varchar                                                           AS api_message_id,
  CASE
    WHEN (r.source = 'USER_EXPLICIT' OR r.type = 'USER_INPUT')
         AND regexp_matches(coalesce(r.content, ''), '(?s)<USER_REQUEST>.*</USER_REQUEST>')
      THEN nullif(trim(regexp_extract(r.content, '(?s)<USER_REQUEST>\s*(.*?)\s*</USER_REQUEST>', 1)), '')
    ELSE nullif(r.content, '')
  END                                                                     AS content,
  nullif(r.thinking, '')                                                  AS agy_thinking,
  'antigravity'                                                           AS runtime,
  s.date
FROM _agy_raw r
JOIN _agy_sessions s ON s.session_id = r.session_id
WHERE r.type IN ('USER_INPUT', 'PLANNER_RESPONSE') OR r.source = 'USER_EXPLICIT';

-- ---------------------------------------------------------------------------
-- api_calls: token usage from PLANNER_RESPONSE / CHECKPOINT steps, paired with
-- the active model carried forward from `<USER_SETTINGS_CHANGE>` annotations.
CREATE OR REPLACE TABLE _agy_api_calls AS
WITH model_steps AS (
  SELECT
    r.*,
    nullif(
      regexp_replace(
        lower(trim(regexp_replace(
          regexp_extract(coalesce(r.content, ''), 'Model Selection` from [^`]+ to ([^.\r\n<]+)', 1),
          '\s*\([^)]*\)\s*$', ''
        ))),
        '[^a-z0-9]+', '-', 'g'
      ),
      ''
    ) AS extracted_model
  FROM _agy_raw r
),
with_model AS (
  SELECT
    m.*,
    coalesce(
      last_value(extracted_model IGNORE NULLS) OVER (
        PARTITION BY session_id ORDER BY step_index
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
      ),
      'antigravity-unknown'
    ) AS model
  FROM model_steps m
)
SELECT
  w.session_id,
  w.session_id || ':' || w.step_index                AS message_id,
  NULL::varchar                                      AS api_message_id,
  NULL::varchar                                      AS request_id,
  w.ts                                               AS timestamp,
  w.model,
  'google'                                           AS provider,
  coalesce(w.input_tokens, 0)::bigint                AS input,
  coalesce(w.output_tokens, 0)::bigint               AS output,
  coalesce(w.cache_read_tokens, 0)::bigint           AS cache_read,
  0::bigint                                          AS cache_write,
  CASE WHEN p.model IS NOT NULL THEN round(
        coalesce(w.input_tokens, 0)      * coalesce(p.input_cost, 0)
      + coalesce(w.output_tokens, 0)     * coalesce(p.output_cost, 0)
      + coalesce(w.cache_read_tokens, 0) * coalesce(p.cache_read_cost, 0), 6)
       ELSE 0 END                                    AS cost_usd,
  (p.model IS NULL)                                  AS unpriced_model,
  (s.parent_session_id IS NOT NULL)                  AS is_sidechain,
  'antigravity'                                      AS runtime,
  s.date
FROM with_model w
JOIN _agy_sessions s ON s.session_id = w.session_id
LEFT JOIN _pricing p ON w.model = p.model
WHERE w.input_tokens IS NOT NULL
   OR w.output_tokens IS NOT NULL
   OR w.cache_read_tokens IS NOT NULL;

-- ---------------------------------------------------------------------------
-- tool_calls: unnested from PLANNER_RESPONSE.tool_calls and paired with the
-- subsequent MODEL tool-execution step(s) (or inline status/result fields).
CREATE OR REPLACE TABLE _agy_planner_tools AS
SELECT
  r.session_id,
  r.step_index,
  r.ts AS started_at,
  coalesce(
    (SELECT min(r2.step_index) FROM _agy_raw r2
     WHERE r2.session_id = r.session_id
       AND r2.step_index > r.step_index
       AND (r2.type IN ('PLANNER_RESPONSE', 'USER_INPUT') OR r2.source <> 'MODEL')),
    999999999
  ) AS next_turn_step,
  r.tool_calls
FROM _agy_raw r
WHERE r.type = 'PLANNER_RESPONSE'
  AND r.tool_calls IS NOT NULL
  AND json_type(r.tool_calls) = 'ARRAY';

CREATE OR REPLACE TABLE _agy_tool_calls AS
WITH unnested AS (
  SELECT
    p.session_id,
    p.step_index,
    p.started_at,
    p.next_turn_step,
    tc_idx,
    tc
  FROM _agy_planner_tools p,
  LATERAL (
    SELECT unnest(cast(p.tool_calls AS json[])) AS tc,
           generate_subscripts(cast(p.tool_calls AS json[]), 1) AS tc_idx
  )
),
paired AS (
  SELECT
    u.session_id,
    u.session_id || ':' || u.step_index                                     AS message_id,
    coalesce(u.tc->>'id', u.session_id || ':' || u.step_index || ':' || u.tc_idx) AS tool_use_id,
    coalesce(u.tc->>'name', u.tc->>'tool_name')                             AS tool_name,
    coalesce((u.tc->'args')::varchar, (u.tc->'arguments')::varchar)         AS arguments,
    u.started_at,
    res.ts                                                                  AS ended_at,
    coalesce(res.status, u.tc->>'status')                                   AS raw_status,
    res.exit_code                                                           AS exit_code,
    coalesce(res.error_code, u.tc->>'error_code')                           AS error_code,
    coalesce(res.error, u.tc->>'error')                                     AS error_message,
    coalesce(res.content, (u.tc->'result')::varchar)                        AS result_text,
    u.tc                                                                    AS tc_json
  FROM unnested u
  LEFT JOIN _agy_raw res
    ON res.session_id = u.session_id
   AND res.step_index = u.step_index + u.tc_idx
   AND res.step_index < u.next_turn_step
   AND res.source = 'MODEL'
   AND res.type <> 'PLANNER_RESPONSE'
)
SELECT
  p.session_id,
  p.message_id,
  p.tool_use_id,
  p.tool_name,
  p.started_at,
  p.ended_at,
  CASE WHEN p.ended_at IS NOT NULL AND p.ended_at >= p.started_at
       THEN datediff('millisecond', p.started_at, p.ended_at) END          AS duration_ms,
  CASE
    WHEN lower(coalesce(p.error_code, '')) IN ('denied', 'permission_denied', 'user_denied')
      OR coalesce(p.error_message, '') ILIKE '%denied%'
      OR coalesce(p.error_message, '') ILIKE '%rejected by user%'
      OR coalesce(p.result_text, '') ILIKE '%user denied%'
      OR coalesce(p.result_text, '') ILIKE '%user rejected%'               THEN 'denied'
    WHEN lower(coalesce(p.error_code, '')) IN ('rejected', 'declined')     THEN 'rejected'
    WHEN coalesce(p.raw_status, '') IN ('ERROR', 'INVALID')
      OR coalesce(p.exit_code, 0) <> 0
      OR p.error_message IS NOT NULL                                       THEN 'error'
    WHEN coalesce(p.raw_status, '') = 'RUNNING'                            THEN 'running'
    WHEN coalesce(p.raw_status, '') = 'DONE'                               THEN 'completed'
    ELSE 'completed'
  END                                                                      AS decision,
  CASE
    WHEN p.tool_name = 'call_mcp_tool' THEN (p.tc_json->'args')->>'ServerName'
    WHEN p.tool_name LIKE 'mcp_%'      THEN regexp_extract(p.tool_name, '^mcp_([^_]+)_', 1)
  END                                                                      AS mcp_server,
  CASE
    WHEN p.tool_name = 'call_mcp_tool' THEN (p.tc_json->'args')->>'ToolName'
    WHEN p.tool_name LIKE 'mcp_%'      THEN regexp_extract(p.tool_name, '^mcp_[^_]+_(.+)$', 1)
  END                                                                      AS mcp_tool,
  p.arguments,
  p.error_code,
  p.error_message,
  p.result_text,
  (s.parent_session_id IS NOT NULL)                                        AS is_sidechain,
  'antigravity'                                                            AS runtime,
  s.date
FROM paired p
JOIN _agy_sessions s ON s.session_id = p.session_id
WHERE p.tool_name IS NOT NULL;

DROP TABLE _agy_planner_tools;

-- ---------------------------------------------------------------------------
-- file -> session -> date map for incremental delta detection. Includes both
-- transcript.jsonl and transcript_full.jsonl so changes to either file mark
-- the session's date partition for rebuild.
CREATE OR REPLACE TABLE _agy_files AS
SELECT DISTINCT sc.filename AS path, sc.session_id, s.date
FROM _agy_scan sc
JOIN _agy_sessions s ON s.session_id = sc.session_id
WHERE sc.filename IS NOT NULL;

DROP TABLE _agy_raw;
DROP TABLE _agy_scan;
