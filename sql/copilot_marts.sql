-- varve copilot marts (fork addition, 2026-11-04). Query-time views in varve.duckdb
-- over the core tool_calls table, created by `just _views` only when the lake holds
-- copilot data (they bind copilot-only columns: mcp_tool, arguments, result_text).
--
-- abap_attempts: one row per ABAP write/activate attempt through an ADT MCP server
-- (vsp and its predecessors), with the outcome parsed from the tool's JSON result,
-- plus per-object sequence features for fix-loop analysis.
--
-- Outcome precedence (first match wins; syntax/activation before write_failed):
--   denied / cancelled  operator declined the call
--   tool_error          the MCP call itself failed (decision = 'error'), e.g. missing
--                       argument, ADT HTTP error, WebSocket handshake
--   syntax_error        >= 1 syntaxErrors entry with severity 'E', or vsp's pre-save
--                       gate ("Edit would introduce N syntax error(s)", "Source has N
--                       syntax errors")
--   warning_blocked     vsp refused to save because of warnings ("Edit has N syntax
--                       warning(s). Use ignore_warnings=true")
--   activation_failed   activation.success = false (WriteSource/EditSource), an
--                       Activate call with an 'E' message or success:false, or an
--                       "Aktivierung wurde abgebrochen" message
--   edit_mismatch       EditSource old_string not found -- the model edited a stale or
--                       imagined version of the source
--   object_missing      update on an object that does not exist (wrong mode)
--   locked              ADT 423 (locked) / 409 (conflict)
--   write_failed        any other payload success:false / ImportFromFile `errors`
--   ok                  none of the above (activation verified or not reported)
--
-- Object identity: upper(url_decode(...)) over the many argument spellings vsp
-- accepts (name / object_name / objectName / object_url tail / file name), falling
-- back to the result's objectName. system = mcp_server with the tool-profile suffix
-- (_coder/_ddic/_all) stripped, so one SAP system is one system.
--
-- Loop features (per session x system x object, ordered by started_at):
--   attempt_no            1..n
--   after_failure         previous attempt on this object failed
--   resubmitted_defective this attempt's source hash equals an earlier attempt on the
--                         same object that failed ON CODE QUALITY (syntax_error or
--                         activation_failed) -- the model pushed code
--                         that was already rejected. Lock/HTTP retries do not count;
--                         ImportFromFile is excluded (hash = file path, not content).
--   same_error_as_prev    first error text identical to the previous attempt's
--   fail_streak           length of the failure run this attempt belongs to (0 if ok)

CREATE OR REPLACE VIEW abap_attempts AS
WITH base AS (
  SELECT
    t.session_id, t.date, t.tool_use_id, t.message_id, t.started_at, t.ended_at,
    t.duration_ms, t.decision, t.cp_model AS model, t.mcp_tool AS op,
    t.mcp_server,
    regexp_replace(t.mcp_server, '_(coder|ddic|all)$', '')                AS system,
    t.arguments::json                                                     AS a,
    CASE WHEN json_valid(t.result_text) THEN t.result_text::json END      AS r,
    t.error_message
  FROM tool_calls t
  WHERE t.runtime = 'copilot'
    AND t.mcp_tool IN ('WriteSource', 'EditSource', 'ImportFromFile', 'Activate')
),
parsed AS (
  SELECT
    b.*,
    -- object_url: the segment after the ADT collection, so .../classes/X/includes/
    -- testclasses is X (not TESTCLASSES); abapGit file names encode '/' as '#'.
    replace(upper(url_decode(coalesce(
      nullif(b.a->>'name', ''), nullif(b.a->>'object_name', ''), nullif(b.a->>'objectName', ''),
      nullif(regexp_extract(b.a->>'object_url',
        '/sap/bc/adt/(?:oo/classes|oo/interfaces|programs/programs|programs/includes|functions/groups|ddic/[a-z]+)/([^/]+)', 1), ''),
      nullif(regexp_extract(b.a->>'file_path', '([^/.]+)\.[^/]*$', 1), ''),
      b.r->>'objectName'))), '#', '/')                                    AS object,
    md5(coalesce(b.a->>'source', b.a->>'new_string', b.a->>'new_str', b.a->>'new_code',
                 b.a->>'file_path', ''))                                  AS src_hash,
    coalesce(len(list_filter(from_json(b.r->'syntaxErrors', '["json"]'),
                             lambda e: e->>'severity' = 'E')), 0)         AS n_syntax_e,
    coalesce(len(list_filter(from_json(b.r->'syntaxErrors', '["json"]'),
                             lambda e: e->>'severity' = 'W')), 0)         AS n_syntax_w,
    try_cast(b.r->'activation'->>'success' AS boolean)                    AS activation_ok,
    try_cast(b.r->>'success' AS boolean)                                  AS payload_ok,
    coalesce(json_array_length(b.r->'errors'), 0)                         AS n_import_errors,
    coalesce(len(list_filter(from_json(b.r->'messages', '["json"]'),
                             lambda m: coalesce(m->>'type', m->>'severity') = 'E')), 0) AS n_activate_msgs_e
  FROM base b
),
classified AS (
  SELECT
    p.*,
    CASE
      WHEN p.decision IN ('denied', 'rejected')                                  THEN 'denied'
      WHEN p.decision = 'cancelled'                                              THEN 'cancelled'
      WHEN p.decision IN ('error', 'running')                                    THEN 'tool_error'
      -- payload success:false often CARRIES the real cause, so the specific
      -- outcomes are tested first. Messages are vsp's own (2.x wording).
      WHEN p.n_syntax_e > 0
        OR (p.r->>'message') SIMILAR TO '.*(would introduce [0-9]+ syntax error|has [0-9]+ syntax error).*'
                                                                                 THEN 'syntax_error'
      WHEN p.r->>'message' SIMILAR TO '.*has [0-9]+ syntax warning.*Changes NOT saved.*'
                                                                                 THEN 'warning_blocked'
      WHEN p.activation_ok = false
        OR (p.op = 'Activate' AND (p.n_activate_msgs_e > 0 OR p.payload_ok = false))
        OR (p.r->>'message') ILIKE '%Aktivierung%' OR (p.r->>'message') ILIKE '%activat%fail%'
                                                                                 THEN 'activation_failed'
      WHEN p.r->>'message' ILIKE '%old_string not found%'                        THEN 'edit_mismatch'
      WHEN p.r->>'message' ILIKE '%does not exist (use mode=create%'             THEN 'object_missing'
      WHEN coalesce(p.r->>'message', p.r->'errors'->>0) SIMILAR TO '.*status (423|409).*'
                                                                                 THEN 'locked'
      WHEN p.payload_ok = false OR p.n_import_errors > 0                         THEN 'write_failed'
      ELSE 'ok'
    END                                                                          AS outcome,
    coalesce(
      p.error_message,
      list_filter(from_json(p.r->'syntaxErrors', '["json"]'),
                  lambda e: e->>'severity' = 'E')[1]->>'text',
      p.r->'activation'->'messages'->0->>'shortText',
      p.r->'messages'->0->>'shortText',
      p.r->'errors'->>0,
      CASE WHEN p.payload_ok = false THEN p.r->>'message' END)                   AS first_error
  FROM parsed p
),
seq AS (
  SELECT
    c.*,
    c.outcome NOT IN ('ok', 'denied', 'cancelled')                               AS failed,
    row_number() OVER w                                                          AS attempt_no,
    lag(c.outcome) OVER w                                                        AS prev_outcome,
    lag(c.first_error) OVER w                                                    AS prev_error
  FROM classified c
  WINDOW w AS (PARTITION BY c.session_id, c.system, c.object ORDER BY c.started_at, c.tool_use_id)
),
flags AS (
  SELECT
    s.*,
    coalesce(s.prev_outcome NOT IN ('ok', 'denied', 'cancelled'), false)
                                                                                 AS after_failure,
    coalesce(s.first_error IS NOT NULL AND s.first_error = s.prev_error, false)  AS same_error_as_prev,
    -- island id: increments on every non-failed attempt, so a failure run shares one id
    sum(CASE WHEN s.failed THEN 0 ELSE 1 END) OVER (
      PARTITION BY s.session_id, s.system, s.object ORDER BY s.started_at, s.tool_use_id
      ROWS UNBOUNDED PRECEDING)                                                  AS island
  FROM seq s
)
SELECT
  f.session_id, f.date, f.system, f.mcp_server, f.object, f.op, f.model,
  f.started_at, f.duration_ms, f.attempt_no, f.outcome, f.failed,
  f.n_syntax_e, f.n_syntax_w, f.activation_ok, f.first_error,
  f.after_failure, f.same_error_as_prev,
  EXISTS (
    SELECT 1 FROM flags g
    WHERE g.session_id = f.session_id AND g.system = f.system AND g.object = f.object
      AND g.attempt_no < f.attempt_no
      AND g.outcome IN ('syntax_error', 'activation_failed')   -- not warning_blocked: resending with ignore_warnings=true is vsp's intended path
      AND g.src_hash = f.src_hash AND f.src_hash <> md5('')
      AND f.op <> 'ImportFromFile'   -- its hash is the file PATH; content may differ
  )                                                                              AS resubmitted_defective,
  CASE WHEN f.failed THEN count(*) FILTER (WHERE f.failed) OVER (
    PARTITION BY f.session_id, f.system, f.object, f.island) ELSE 0 END          AS fail_streak,
  f.src_hash, f.tool_use_id, f.message_id
FROM flags f;
