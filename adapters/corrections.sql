-- varve corrections adapter (W7 phase 1 -- mechanical correction MINING, mining-first
-- revision per RFC-0002 2026-07-06). Reworked from the withdrawn hand-fed-ledger shape.
-- Near-pure DuckDB SQL (ADR-0002). Three products, all written to the lake:
--   1. correction_candidates -- one row per (detector, locus), derived PURELY from the
--      lake (messages/tool_calls/api_calls/sessions views), so it rebuilds
--      deterministically on every ingest (no manifest interaction; same rebuild
--      discipline as the read-side views). Four detectors, tuned precision-over-recall.
--   2. operator_messages -- METADATA ONLY, no content: one row per genuine operator
--      message (same `is_operator` predicate _op_ctx already computes for D2/D3/D4 --
--      "operator message" means exactly what the detectors mean by it, no logic
--      duplication) plus `flagged` (true iff the message's (runtime, session_id,
--      message_uuid) also appears as a candidate locus, any detector). This is the
--      RFC-0002 G-recall-floor denominator: `queries/recall_floor_sample.sql` samples
--      flagged=false rows for adjudication of the miss rate. Same rebuild discipline as
--      correction_candidates (pure function of the lake; deterministic on re-run).
--   3. correction_events -- confirmed candidates, promoted after the operator adjudicates
--      a sample. The withdrawn design's DDL survives; channel becomes {mined | opt_in}
--      ('curated' retired).
--
-- Invoked by the justfile `mine` recipe, which _pipeline calls AFTER the core tables are
-- written and the read-side views rebuilt (ingest/backfill are the sole lake writers).
-- Variables (set by the justfile):
--   lake          : <data_home>/lake (to read core Parquet + write the new tables)
--   repo_dir      : this repo root (to read the taxonomy PRIOR seed)
--   adjudications : path to a JSONL adjudication ledger. This is OPERATOR DATA and lives
--                   OUTSIDE the repo at a configured path (public-instrument rule: varve
--                   is going public, so no operator data in git). Defaults to
--                   <data_home>/adjudications.jsonl (VARVE_ADJUDICATIONS overrides). The
--                   justfile guarantees the path EXISTS (it substitutes an empty temp file
--                   when the ledger is absent), so the read below never errors on a missing
--                   file -- absent ledger => zero rows => zero promoted events.
--
-- =====================================================================================
-- NOISE EXCLUSION (the load-bearing precision control; RFC-0002 "CRITICAL noise
-- exclusion"). Transcripts are full of harness-injected text that is NOT operator
-- speech; treating it as such explodes false positives. Every exclusion below was
-- confirmed against the live corpus 2026-07-06. The lake `messages` view does NOT carry
-- Claude Code's `isMeta` flag (adding it would need a full backfill to repopulate
-- history, which mining must not trigger), so meta rows are excluded by CONTENT pattern
-- instead -- the enumerated markers below cover every isMeta signature observed:
--   * <system-reminder> ......... harness reminder blocks (122 CC user rows)
--   * <command-name>/<command-message>/<command-args> .. slash-command invocations (50)
--   * <local-command-stdout>/<local-command-caveat> + "Caveat: The messages below were
--     generated" ................ slash-command mechanics + the local-command caveat (55)
--   * <task-notification> ....... subagent task-completion notifications (706 -- the
--                                 single largest noise class)
--   * <user-prompt-submit-hook> . prompt-submit hook output
--   * "This session is being continued" .. compaction summaries (42)
--   * "[SYSTEM NOTIFICATION - NOT USER INPUT]" .. autonomous-loop notifications (10)
--   * "Base directory for this skill:" .. skill-body injections, a text-block array that
--                                 would otherwise read as operator text (82)
--   * "The coordinator sent a message while you were working:" .. session-manager
--                                 coordinator injections (6)
--   * <<autonomous-loop...>> .... loop-control injections (6)
-- Also excluded structurally (not operator speech):
--   * tool_result-carrier user rows (47,908 of 47,163+ array user rows) -- the model's
--     tool results fed back under the user role. These are the LOCUS for detector D1
--     (tool-decision rejection) but never operator utterances for D3/D4.
--   * the bare interrupt marker "[Request interrupted by user...]" -- stripped from the
--     body; a message that is ONLY the marker has an empty body and is not a redirect.
--
-- CONFIDENTIALITY (ADR-0004; public-instrument rule -- I4/I5 hold):
--   * Candidates + events live ONLY in the local lake. Loci are the non-identifying
--     `lake:<session_id>/<message_uuid>` form; session/message uuids are opaque and
--     resolve to a path only on this disk. No account-bearing path is stored.
--   * operator_messages is COUNTS/METADATA ONLY -- runtime, ids, timestamp, attribution,
--     a bool. No content column, no excerpt, no `body`/`raw_txt`.
--   * `features` carries METADATA only (matched marker label, overlap score, mode) --
--     never verbatim transcript content, so even the lake-local candidate table holds no
--     operator/model prose beyond the loci it points at.
--   * `note` on a promoted event is the adjudicator's role-level gloss only (I4).
-- =====================================================================================

-- Detector + marker-list versions. Bump the relevant one on any logic/marker change so
-- `detector_version` on historical candidates stays honest (RFC-0002 detector-drift
-- mitigation). The lexical marker list version rides D3's version.
--   D1 tool_reject       v2   D2 interrupt_redirect v1
--   D3 lexical (markers) v1   D4 rewrite (jaccard)  v2
-- v2 (2026-07-07): D1 + D4 fixed from adjudication ROUND-0 EVIDENCE (model-suggested
-- labels over the stratified sample -- advisory, not the G-precision measurement). Both
-- fixes are STRUCTURAL (a co-present template literal; an identity/echo band), defensible
-- without operator labels. D2's weak next_message prior and D3's negation/actually FP
-- classes are ALSO round-0 findings, but their fixes need content judgment -- deferred
-- until the operator's independent labels land, to avoid tuning judgment calls on model
-- suggestions alone.

-- -------------------------------------------------------------------------------------
-- Lake-local read views over the freshly-written core Parquet (same read_parquet shape
-- as sql/views.sql; local to this mining session so `mine` needs no attached db).
CREATE OR REPLACE VIEW _lk_messages AS
  SELECT * FROM read_parquet(getvariable('lake') || '/messages/**/*.parquet',
    hive_partitioning = true, union_by_name = true);
CREATE OR REPLACE VIEW _lk_tool_calls AS
  SELECT * FROM read_parquet(getvariable('lake') || '/tool_calls/**/*.parquet',
    hive_partitioning = true, union_by_name = true);
CREATE OR REPLACE VIEW _lk_api_calls AS
  SELECT * FROM read_parquet(getvariable('lake') || '/api_calls/**/*.parquet',
    hive_partitioning = true, union_by_name = true);
CREATE OR REPLACE VIEW _lk_sessions AS
  SELECT * FROM read_parquet(getvariable('lake') || '/sessions/**/*.parquet',
    hive_partitioning = true, union_by_name = true);

-- Versioned lexical marker list (RFC-0002 D3). Anchored at message start (RE2 `^`),
-- matched AFTER an optional leading interjection is stripped (see _op below). Each row's
-- `marker` label lands in features so per-marker precision is measurable at adjudication.
-- "no" is intentionally NOT in the interjection strip-list (it is itself a marker).
CREATE OR REPLACE TABLE _corr_markers AS
SELECT * FROM (VALUES
  ('^(no|nope|nah)([^a-z0-9]|$)',                                  'negation'),
  ('^(don''?t|do not)([^a-z0-9]|$)',                               'prohibition'),
  ('^stop([^a-z0-9]|$)',                                           'stop'),
  ('^(that''?s not|that is not|not what|that''?s not what)\b',     'contradiction'),
  ('^(wrong|that''?s wrong|incorrect|that''?s incorrect)\b',       'wrong'),
  ('^actually\b',                                                  'actually'),
  ('^wait\b',                                                      'wait'),
  ('^(i meant|i didn''?t mean|i said)\b',                          'i_meant'),
  ('^(instead|rather)\b',                                          'redirect'),
  ('^(undo|revert)\b',                                             'reversal'),
  ('^(never ?mind|scratch that|ignore (that|the|my))\b',          'discard')
) AS t(pattern, marker);

-- Versioned taxonomy PRIOR (NOT the controlled vocabulary -- a phase-3 output). Used for
-- SOFT validation of promoted-event categories (flag, never crash).
CREATE OR REPLACE TABLE _corr_taxonomy AS
SELECT * FROM read_csv(getvariable('repo_dir') || '/adapters/divergence_taxonomy.csv',
  header = true, comment = '#', auto_detect = true);
CREATE OR REPLACE TABLE _corr_known AS SELECT list(category) AS known FROM _corr_taxonomy;

-- -------------------------------------------------------------------------------------
-- Operator-text extraction. Runtime-aware: Claude Code stores message.content as JSON
-- (a block array, or a bare quoted string); opencode stores content as already-aggregated
-- plain text. The json[] cast MUST bind only to array rows (the planner otherwise hoists
-- it above a CASE type-guard and errors on string content -- see adapters/claude_code.sql
-- _cc_asst_blocks), so CC array rows are materialized first. Blocks are joined with
-- per-row list functions in array order, so text concatenation (hence first-token
-- matching) is DETERMINISTIC and streams; a correlated string_agg here does not spill.
CREATE OR REPLACE TABLE _cc_user_arr AS
SELECT message_id, session_id, timestamp AS ts, content
FROM _lk_messages
WHERE runtime = 'claude-code' AND role = 'user' AND json_type(content) = 'ARRAY';

-- string_agg semantics over a list: NULL elements skipped, NULL when none remain.
CREATE OR REPLACE MACRO _join_blocks(l) AS
  CASE WHEN len(list_filter(l, lambda x: x IS NOT NULL)) > 0 THEN array_to_string(l, '\n') END;
CREATE OR REPLACE MACRO _block_type(j) AS json_extract_string(j, '$.type');

CREATE OR REPLACE TABLE _cc_user_arr_x AS
SELECT
  message_id, session_id, ts,
  _join_blocks(list_transform(list_filter(blocks, lambda j: _block_type(j) = 'text'),
                              lambda j: json_extract_string(j, '$.text')))     AS raw_txt,
  len(list_filter(blocks, lambda j: _block_type(j) = 'tool_result')) > 0      AS has_tool_result,
  _join_blocks(list_transform(list_filter(blocks, lambda j: _block_type(j) = 'tool_result'),
                              lambda j: (j -> 'content')::varchar))           AS tool_result_txt
FROM (SELECT message_id, session_id, ts, cast(content AS json[]) AS blocks FROM _cc_user_arr);
DROP TABLE _cc_user_arr;

-- Unified raw user-message text across runtimes + shapes.
-- pi is DELIBERATELY absent from _um -- and so from every detector and the
-- recall-floor denominator: the pi adapter is the least-validated of the three
-- (adapters/pi.sql STATUS) and its message content shape is unverified against
-- a real corpus at scale. Mining is precision-first (RFC-0002 gates), so
-- excluding pi beats guessing; RFC-0001 records the coverage limitation. Add
-- pi here once the adapter is validated.
CREATE OR REPLACE TABLE _um AS
  SELECT message_id, session_id, timestamp AS ts, 'claude-code' AS runtime,
         json_extract_string(content, '$') AS raw_txt,
         false AS has_tool_result, NULL::varchar AS tool_result_txt
  FROM _lk_messages
  WHERE runtime = 'claude-code' AND role = 'user' AND json_type(content) = 'VARCHAR'
  UNION ALL BY NAME
  SELECT message_id, session_id, ts, 'claude-code' AS runtime,
         raw_txt, has_tool_result, tool_result_txt
  FROM _cc_user_arr_x
  UNION ALL BY NAME
  SELECT message_id, session_id, timestamp AS ts, 'opencode' AS runtime,
         content AS raw_txt, false AS has_tool_result, NULL::varchar AS tool_result_txt
  FROM _lk_messages
  WHERE runtime = 'opencode' AND role = 'user' AND content IS NOT NULL
  UNION ALL BY NAME
  -- copilot: plain-text content like opencode (adapters/copilot.sql). Abort rows carry
  -- NULL content and so never count as operator speech.
  SELECT message_id, session_id, timestamp AS ts, 'copilot' AS runtime,
         content AS raw_txt, false AS has_tool_result, NULL::varchar AS tool_result_txt
  FROM _lk_messages
  WHERE runtime = 'copilot' AND role = 'user' AND content IS NOT NULL
  UNION ALL BY NAME
  -- antigravity: plain-text content with <USER_REQUEST> wrapper already stripped by
  -- adapters/antigravity.sql.
  SELECT message_id, session_id, timestamp AS ts, 'antigravity' AS runtime,
         content AS raw_txt, false AS has_tool_result, NULL::varchar AS tool_result_txt
  FROM _lk_messages
  WHERE runtime = 'antigravity' AND role = 'user' AND content IS NOT NULL
  UNION ALL BY NAME
  -- vibe: plain-text content from messages.jsonl.
  SELECT message_id, session_id, timestamp AS ts, 'vibe' AS runtime,
         content AS raw_txt, false AS has_tool_result, NULL::varchar AS tool_result_txt
  FROM _lk_messages
  WHERE runtime = 'vibe' AND role = 'user' AND content IS NOT NULL;

-- Operator body (interrupt marker stripped) + the noise flag (the NOISE EXCLUSION list
-- documented in the header). `is_operator` = genuine typed operator speech.
CREATE OR REPLACE TABLE _op AS
SELECT
  *,
  trim(regexp_replace(coalesce(raw_txt, ''), '^\[Request interrupted by user[^\]]*\]', '')) AS body,
  (    raw_txt ILIKE '%<system-reminder>%'
    OR raw_txt ILIKE '%<command-name>%' OR raw_txt ILIKE '%<command-message>%' OR raw_txt ILIKE '%<command-args>%'
    OR raw_txt ILIKE '%<local-command-stdout>%' OR raw_txt ILIKE '%<local-command-caveat>%'
    OR raw_txt ILIKE 'Caveat: The messages below were generated%'
    OR raw_txt ILIKE '%<task-notification>%'
    OR raw_txt ILIKE '%<user-prompt-submit-hook>%'
    OR raw_txt ILIKE '%This session is being continued%'
    OR raw_txt ILIKE '%[SYSTEM NOTIFICATION - NOT USER INPUT]%'
    OR raw_txt ILIKE 'Base directory for this skill:%'
    OR raw_txt ILIKE 'The coordinator sent a message while you were working:%'
    OR raw_txt ILIKE '%<<autonomous-loop%'
  ) AS is_noise
FROM _um;
DROP TABLE _um;

-- Assistant text (the corrected DRAFT), same materialize-then-index discipline for CC.
CREATE OR REPLACE TABLE _cc_asst_arr AS
SELECT message_id, session_id, timestamp AS ts, content
FROM _lk_messages
WHERE runtime = 'claude-code' AND role = 'assistant' AND json_type(content) = 'ARRAY';

CREATE OR REPLACE TABLE _asst AS
  SELECT message_id, session_id, ts, 'claude-code' AS runtime,
    _join_blocks(list_transform(list_filter(cast(content AS json[]), lambda j: _block_type(j) = 'text'),
                                lambda j: json_extract_string(j, '$.text'))) AS atxt
  FROM _cc_asst_arr
  UNION ALL BY NAME
  SELECT message_id, session_id, timestamp AS ts, 'opencode' AS runtime, content AS atxt
  FROM _lk_messages
  WHERE runtime = 'opencode' AND role = 'assistant' AND content IS NOT NULL
  UNION ALL BY NAME
  SELECT message_id, session_id, timestamp AS ts, 'copilot' AS runtime, content AS atxt
  FROM _lk_messages
  WHERE runtime = 'copilot' AND role = 'assistant' AND content IS NOT NULL
  UNION ALL BY NAME
  SELECT message_id, session_id, timestamp AS ts, 'antigravity' AS runtime, content AS atxt
  FROM _lk_messages
  WHERE runtime = 'antigravity' AND role = 'assistant' AND content IS NOT NULL
  UNION ALL BY NAME
  SELECT message_id, session_id, timestamp AS ts, 'vibe' AS runtime, content AS atxt
  FROM _lk_messages
  WHERE runtime = 'vibe' AND role = 'assistant' AND content IS NOT NULL;
DROP TABLE _cc_asst_arr;

-- Draft context per user message: nearest PRECEDING assistant message that carries text
-- (the draft being corrected) + the model that produced the draft (nearest api_call at or
-- before the correction). ASOF joins pick the closest match on ts within (runtime, session).
-- Both right sides are unique on (runtime, session_id, ts) so each ASOF match is unique;
-- same-ts ties keep the lowest message_id / model.
CREATE OR REPLACE TABLE _asst_txt AS
SELECT * FROM _asst WHERE atxt IS NOT NULL AND length(trim(atxt)) > 0
QUALIFY row_number() OVER (PARTITION BY runtime, session_id, ts ORDER BY message_id) = 1;
DROP TABLE _asst;
CREATE OR REPLACE TABLE _api AS
SELECT session_id, runtime, timestamp AS ts, min(model) AS model
FROM _lk_api_calls WHERE model IS NOT NULL GROUP BY ALL;

CREATE OR REPLACE TABLE _op_ctx AS
SELECT
  o.*,
  a.message_id AS prior_asst_uuid,
  a.atxt       AS prior_asst_txt,
  c.model      AS model_id,
  ((NOT o.is_noise) AND (NOT o.has_tool_result) AND length(trim(o.body)) > 0) AS is_operator
FROM _op o
ASOF LEFT JOIN _asst_txt a
  ON o.runtime = a.runtime AND o.session_id = a.session_id AND o.ts > a.ts
ASOF LEFT JOIN _api c
  ON o.runtime = c.runtime AND o.session_id = c.session_id AND o.ts >= c.ts;

-- A message recorded in more than one session (the same line uuid in two session files)
-- belongs to the earliest-started session; the other copies are excluded from both
-- products so a typed message counts once.
CREATE OR REPLACE TABLE _copies AS
SELECT o.runtime, o.session_id, o.message_id
FROM (SELECT DISTINCT runtime, session_id, message_id FROM _op) o
JOIN _lk_sessions s ON s.runtime = o.runtime AND s.session_id = o.session_id
QUALIFY row_number() OVER (PARTITION BY o.runtime, o.message_id
                           ORDER BY s.started_at NULLS LAST, o.session_id) > 1;

-- token set of a text (lowercased word unigrams), for the D4 jaccard.
CREATE OR REPLACE MACRO _tok(s) AS
  list_distinct(list_filter(regexp_split_to_array(lower(coalesce(s, '')), '[^a-z0-9]+'),
                            lambda x: length(x) > 0));

-- =====================================================================================
-- DETECTORS. Each emits the RFC-0002 candidate grain: runtime, session_id, message_uuid
-- (the corrective user message), prior_assistant_uuid (the corrected draft), ts, detector,
-- detector_version, features (JSON metadata), model_id. Attribution/date are joined last.
-- A single message may be flagged by more than one detector -- that is the intended
-- "one row per (detector, locus)" grain (RFC-0002).
-- =====================================================================================

-- D1 -- tool-decision rejections.
--   Claude Code: a tool_result carrying the exact rejection boilerplate. NOTE: match the
--   full literal, NOT "want to proceed" -- the AskUserQuestion tool result "...how do you
--   want to proceed..." is a legitimate answer, not a rejection (confirmed 2026-07-06).
--   v2: ALSO exclude tool_results co-carrying "The user wants to clarify these questions"
--   -- the AskUserQuestion "clarify" selection emits BOTH that template and the full
--   rejection literal, but it is the operator engaging with a question the model asked,
--   not a correction of work (round 0: 8/8 of D1's suggested FPs were exactly this).
--   Latent risk still open: a tool_result QUOTING the boilerplate as data (this corpus is
--   agent transcripts) -- not observed firing yet; revisit if adjudication surfaces it.
--   opencode: its native tool `decision` column (state.status). The V1 store records only
--   EXECUTION status {completed|error|running|pending} -- there is NO user-verdict value
--   in this corpus (the `permission` table is empty), so the opencode branch is written
--   forward-compatibly (fires iff a rejection-like status ever appears) and yields 0 today.
CREATE OR REPLACE TABLE _d1 AS
  SELECT
    x.runtime, x.session_id, x.message_id AS message_uuid, x.prior_asst_uuid, x.ts,
    'tool_reject' AS detector, 'v2' AS detector_version,
    json_object('marker', 'tool_use_rejected', 'source', 'tool_result') AS features,
    x.model_id
  FROM _cc_user_arr_x cc
  JOIN _op_ctx x ON x.runtime = 'claude-code' AND x.session_id = cc.session_id
                AND x.message_id = cc.message_id
  WHERE cc.tool_result_txt ILIKE '%The user doesn''t want to proceed with this tool use%'
    AND cc.tool_result_txt NOT ILIKE '%The user wants to clarify these questions%'
  UNION ALL BY NAME
  SELECT
    tc.runtime, tc.session_id, tc.message_id AS message_uuid,
    tc.message_id AS prior_asst_uuid, tc.started_at AS ts,
    'tool_reject' AS detector, 'v2' AS detector_version,
    json_object('marker', 'decision_' || tc.decision, 'source', 'decision_column') AS features,
    NULL::varchar AS model_id
  FROM _lk_tool_calls tc
  -- copilot / antigravity / vibe (fork additions): decision = 'denied' when the operator
  -- denied the permission prompt, see adapters/{copilot,antigravity,vibe}.sql.
  WHERE tc.runtime IN ('opencode', 'copilot', 'antigravity', 'vibe')
    AND lower(coalesce(tc.decision, '')) IN ('rejected', 'denied', 'declined', 'reject', 'deny');

-- D2 -- interrupt-then-redirect (Claude Code only; opencode records no interrupt marker).
--   An interrupt marker followed by an operator redirect. Two shapes, both precise:
--     inline       -- the interrupt message itself carries redirect text after the marker.
--     next_message -- a bare interrupt; the redirect is the next operator message, GUARDED
--                     to the SAME draft (redirect.prior_asst = interrupt.prior_asst) so no
--                     new assistant turn intervened. The candidate locus is the redirect.
CREATE OR REPLACE TABLE _interrupts AS
SELECT
  ctx.message_id AS interrupt_uuid, ctx.session_id, ctx.runtime, ctx.ts,
  ctx.prior_asst_uuid, ctx.model_id,
  ((NOT ctx.is_noise) AND length(trim(ctx.body)) > 20) AS has_inline_redirect
FROM _op_ctx ctx
WHERE ctx.runtime = 'claude-code'
  AND (ctx.raw_txt ILIKE '%[Request interrupted by user%'
    OR ctx.tool_result_txt ILIKE '%Request interrupted by user%');

CREATE OR REPLACE TABLE _op_operator AS SELECT * FROM _op_ctx WHERE is_operator;

CREATE OR REPLACE TABLE _d2 AS
  -- inline: interrupt + redirect in one message
  SELECT
    i.runtime, i.session_id, i.interrupt_uuid AS message_uuid, i.prior_asst_uuid, i.ts,
    'interrupt_redirect' AS detector, 'v1' AS detector_version,
    json_object('mode', 'inline') AS features,
    i.model_id
  FROM _interrupts i
  WHERE i.has_inline_redirect
  UNION ALL BY NAME
  -- next_message: bare interrupt, redirect is the next operator message on the same draft
  SELECT
    r.runtime, r.session_id, r.message_id AS message_uuid, r.prior_asst_uuid, r.ts,
    'interrupt_redirect' AS detector, 'v1' AS detector_version,
    json_object('mode', 'next_message', 'interrupt_uuid', i.interrupt_uuid) AS features,
    r.model_id
  FROM _interrupts i
  JOIN LATERAL (
    SELECT o2.* FROM _op_operator o2
    WHERE o2.runtime = i.runtime AND o2.session_id = i.session_id AND o2.ts > i.ts
    ORDER BY o2.ts, o2.message_id LIMIT 1
  ) r ON true
  WHERE NOT i.has_inline_redirect
    AND r.prior_asst_uuid IS NOT DISTINCT FROM i.prior_asst_uuid
  -- consecutive bare interrupts (repeated ESC) fan into ONE redirect; keep the nearest
  -- interrupt so the locus stays one row per (detector, redirect).
  QUALIFY row_number() OVER (PARTITION BY r.runtime, r.session_id, r.message_id
                             ORDER BY i.ts DESC, i.interrupt_uuid DESC) = 1;

-- D3 -- lexical correction openers. Operator message opening (after an optional leading
--   interjection) with a versioned correction marker, directly following an assistant draft.
CREATE OR REPLACE TABLE _d3 AS
SELECT
  ctx.runtime, ctx.session_id, ctx.message_id AS message_uuid, ctx.prior_asst_uuid, ctx.ts,
  'lexical' AS detector, 'v1' AS detector_version,
  json_object('marker', mk.marker) AS features,
  ctx.model_id
FROM _op_ctx ctx
JOIN _corr_markers mk
  ON regexp_matches(
       regexp_replace(lower(trim(ctx.body)),
         '^(ok|okay|so|well|hmm+|hm|yeah|yep|yes|ah|oh|sorry|hey|right|hi)[^a-z0-9]+', ''),
       mk.pattern)
WHERE ctx.is_operator AND ctx.prior_asst_uuid IS NOT NULL
QUALIFY row_number() OVER (PARTITION BY ctx.runtime, ctx.session_id, ctx.message_id
                           ORDER BY mk.marker) = 1;

-- D4 -- rewrite detection. Operator message with high token-set overlap against the
--   preceding assistant draft but not identical (operator restating/rewriting the model).
--   jaccard on lowercased word unigrams (cheap set overlap, RFC-0002 phase-1); threshold
--   0.5 chosen from the corpus overlap distribution (>=0.5 => 8 CC pairs; 0.3 => 12).
--   v2: BANDED [0.5, 0.9) -- jaccard at/near 1.0 is an ECHO, not a rewrite: the operator
--   pasting model text forward (handoff/resume prompts, directives, status summaries) or
--   re-feeding model output as input. A rewrite by definition changes tokens and lowers
--   overlap (round 0: the one genuine rewrite sat at 0.52; the near-1.0 rows with equal
--   body/prior token counts were all echoes). The echo band is a different, real signal
--   (operator REUSES model text verbatim = implicit acceptance) -- a future detector,
--   not this one.
CREATE OR REPLACE TABLE _d4 AS
SELECT * FROM (
  SELECT
    ctx.runtime, ctx.session_id, ctx.message_id AS message_uuid, ctx.prior_asst_uuid, ctx.ts,
    'rewrite' AS detector, 'v2' AS detector_version,
    len(list_intersect(_tok(ctx.body), _tok(ctx.prior_asst_txt)))::double
      / nullif(len(list_distinct(list_concat(_tok(ctx.body), _tok(ctx.prior_asst_txt)))), 0) AS jaccard,
    len(_tok(ctx.body)) AS body_tokens, len(_tok(ctx.prior_asst_txt)) AS prior_tokens,
    ctx.model_id
  FROM _op_ctx ctx
  WHERE ctx.is_operator AND ctx.prior_asst_txt IS NOT NULL
    AND length(trim(ctx.prior_asst_txt)) >= 20 AND length(trim(ctx.body)) >= 20
    AND lower(trim(ctx.body)) <> lower(trim(ctx.prior_asst_txt))
) s
WHERE jaccard >= 0.5 AND jaccard < 0.9;

-- =====================================================================================
-- Assemble candidates: union detectors, attach W2 attribution + partition date from the
-- session (co-partitions a session's candidates; deterministic rebuild unit), mint a
-- deterministic candidate_id from (detector, locus) so re-runs are byte-identical.
-- =====================================================================================
CREATE OR REPLACE TABLE stg_correction_candidates AS
WITH d AS (
  SELECT runtime, session_id, message_uuid, prior_asst_uuid AS prior_assistant_uuid, ts,
         detector, detector_version, features, model_id FROM _d1
  UNION ALL BY NAME
  SELECT runtime, session_id, message_uuid, prior_asst_uuid AS prior_assistant_uuid, ts,
         detector, detector_version, features, model_id FROM _d2
  UNION ALL BY NAME
  SELECT runtime, session_id, message_uuid, prior_asst_uuid AS prior_assistant_uuid, ts,
         detector, detector_version, features, model_id FROM _d3
  UNION ALL BY NAME
  SELECT runtime, session_id, message_uuid, prior_asst_uuid AS prior_assistant_uuid, ts,
         detector, detector_version,
         json_object('jaccard', round(jaccard, 4), 'body_tokens', body_tokens,
                     'prior_tokens', prior_tokens) AS features,
         model_id FROM _d4
)
SELECT
  'cand-' || md5(d.detector || '|' || d.runtime || '|' || d.session_id || '|'
                 || coalesce(d.message_uuid, ''))                       AS candidate_id,
  d.runtime, d.session_id, d.message_uuid, d.prior_assistant_uuid, d.ts,
  d.detector, d.detector_version, d.features, d.model_id,
  s.estate, s.harness, s.campaign,
  s.date                                                                AS date
FROM d
ANTI JOIN _copies cp
  ON cp.runtime = d.runtime AND cp.session_id = d.session_id AND cp.message_id = d.message_uuid
JOIN _lk_sessions s ON s.runtime = d.runtime AND s.session_id = d.session_id
-- candidate_id is the (detector, locus) primary key; a total order keeps the surviving
-- row identical run to run (G-idempotence) regardless of detector fan-in.
QUALIFY row_number() OVER (PARTITION BY candidate_id
                           ORDER BY d.features::varchar, d.prior_assistant_uuid, d.ts, d.model_id) = 1;

-- The justfile `mine` recipe COPYs stg_correction_candidates to the lake (literal path;
-- COPY TO cannot take getvariable) -- a full deterministic rebuild every ingest, ORDER BY
-- fixing row order so two runs are byte-identical (G-idempotence), same PARTITION_BY/zstd
-- conventions as the core tables.

-- =====================================================================================
-- operator_messages -- RFC-0002 G-recall-floor support. One row per genuine operator
-- message, using the EXACT SAME `is_operator` predicate _op_ctx already computes for
-- D2/D3/D4 (no second definition of "operator message" to drift out of sync), plus a
-- `flagged` bool: true iff this message's (runtime, session_id, message_uuid) is also a
-- candidate locus under ANY detector. METADATA ONLY -- no content column. The DISTINCT
-- subquery collapses detector fan-in (one message can be flagged by more than one
-- detector) and the outer DISTINCT a line recorded twice in one session, so this stays
-- one row per operator message.
--
-- D1 (tool_reject) candidate loci are tool-result-carrier rows by construction
-- (has_tool_result=true), which `is_operator` excludes by definition -- those loci never
-- appear in operator_messages at all, so they never contribute to `flagged`. That is
-- correct, not a gap: D1 flags a TOOL DECISION, not typed operator speech, so it has no
-- membership in this table's population either way.
-- =====================================================================================
CREATE OR REPLACE TABLE stg_operator_messages AS
SELECT DISTINCT
  ctx.runtime, ctx.session_id, ctx.message_id AS message_uuid, ctx.ts,
  s.estate, s.harness, s.campaign,
  s.date                                       AS date,
  (fl.message_uuid IS NOT NULL)                AS flagged
FROM _op_ctx ctx
ANTI JOIN _copies cp
  ON cp.runtime = ctx.runtime AND cp.session_id = ctx.session_id AND cp.message_id = ctx.message_id
JOIN _lk_sessions s ON s.runtime = ctx.runtime AND s.session_id = ctx.session_id
LEFT JOIN (
  SELECT DISTINCT runtime, session_id, message_uuid FROM stg_correction_candidates
) fl ON fl.runtime = ctx.runtime AND fl.session_id = ctx.session_id
    AND fl.message_uuid = ctx.message_id
WHERE ctx.is_operator;

-- The justfile `mine` recipe COPYs stg_operator_messages to the lake, same ORDER BY /
-- PARTITION_BY (runtime, date) / zstd discipline as stg_correction_candidates.

-- =====================================================================================
-- ADJUDICATION -> correction_events. The human loop, relocated from capture time to
-- review time (RFC-0002). The operator labels a stratified candidate sample; labels are
-- appended to the configured adjudications ledger, which lives in the data home (outside
-- the repo, public-instrument rule) so it survives a lake rebuild (`just backfill` clears
-- lake/ only, not the data home) without putting operator data in git. This adapter joins
-- labels back and promotes CONFIRMED candidates.
--
-- Adjudication JSONL line format (one object per line; role-level, non-identifying):
--   { "candidate_id":      "cand-<md5>",                     -- REQUIRED; joins a candidate
--     "verdict":           "true_positive|false_positive|unclear",  -- REQUIRED
--     "context_judgment":  "had_context|lacked_context|ambiguous",  -- the confound control
--     "note":              "<role-level gloss only>",        -- optional; NO verbatim text,
--                                                            --   no account/sector/internal-project ids
--     "adjudicated_at":    "2026-07-06T12:00:00" }           -- optional ISO timestamp
-- Only verdict=true_positive candidates promote to correction_events.
-- =====================================================================================
CREATE OR REPLACE TABLE _adj AS
SELECT
  j ->> 'candidate_id'                                    AS candidate_id,
  j ->> 'verdict'                                         AS verdict,
  coalesce(j ->> 'context_judgment', 'ambiguous')         AS context_judgment,
  j ->> 'note'                                            AS note,
  TRY_CAST(j ->> 'adjudicated_at' AS TIMESTAMP)           AS adjudicated_at
FROM read_json_objects(getvariable('adjudications'),
       format = 'newline_delimited', ignore_errors = true) t(j)
WHERE (j ->> 'candidate_id') IS NOT NULL;

-- Promote confirmed candidates. channel='mined' + adjudication provenance. The withdrawn
-- design's correction_events DDL survives unchanged in shape; mining fills the columns it
-- can and leaves phase-2/3 fields (category, direction, durability) NULL. NOT-NULL
-- acceptance fields (context_judgment, selection, privacy_scope) default safely.
CREATE OR REPLACE TABLE _ce_mined AS
SELECT
  'ce-' || md5(c.candidate_id)                            AS event_id,
  c.date                                                  AS date,          -- partition
  c.ts                                                    AS event_ts,
  a.adjudicated_at                                        AS adjudicated_at,
  'mined'                                                 AS channel,        -- partition
  a.verdict                                               AS verdict,        -- adjudication provenance
  c.detector                                              AS detector,
  c.detector_version                                      AS detector_version,
  c.candidate_id                                          AS candidate_id,
  NULL::varchar                                           AS artifact_type,
  NULL::varchar[]                                         AS category,       -- phase-3 output
  NULL::varchar                                           AS direction,
  NULL::varchar                                           AS magnitude,
  a.context_judgment                                      AS context_judgment,  -- NOT NULL
  'organic'                                               AS selection,         -- NOT NULL (mined = organic)
  c.model_id                                              AS model_id,
  NULL::varchar                                           AS model_version,
  'lake:' || c.session_id || '/' || coalesce(c.prior_assistant_uuid, '') AS draft_ref,
  'lake:' || c.session_id || '/' || c.message_uuid        AS rewrite_ref,
  NULL::varchar                                           AS context_ref,
  CASE WHEN c.detector = 'tool_reject' THEN 'rejected' ELSE NULL END AS decision,
  NULL::varchar                                           AS distilled_rule_id,
  NULL::varchar                                           AS recurrence_cluster_id,
  NULL::varchar                                           AS intervention_ref,   -- durability block
  NULL::timestamp                                         AS bake_date,
  NULL::boolean                                           AS recurred_after_bake,
  NULL::bigint                                            AS observation_window_days,
  'shareable_abstraction'                                 AS privacy_scope,      -- NOT NULL
  NULL::varchar                                           AS redaction_status,
  c.runtime                                               AS runtime,
  a.note                                                  AS note
FROM stg_correction_candidates c
JOIN _adj a ON a.candidate_id = c.candidate_id
WHERE a.verdict = 'true_positive';

-- Events = mined promotions (+ opt_in when a source exists) with SOFT taxonomy validation
-- against the PRIOR (flag, never crash). Mined events carry no category yet (phase-3
-- assigns it), so unknown_category is false for them and only trips on a categorized event
-- whose label falls outside the prior. opt_in (deliberate draft-vs-rewrite diffs) rides
-- the SAME schema, rare, retired-'curated'-free; no opt_in source exists yet, so union it
-- into `base` with channel='opt_in' when one lands. The justfile `mine` recipe COPYs
-- stg_correction_events partitioned by channel/date (channel is the correction_events
-- analog of the core tables' runtime dimension); zero rows (no adjudications yet) writes
-- no partition files and does not error -- the view stays empty until the first round.
CREATE OR REPLACE TABLE stg_correction_events AS
WITH base AS (SELECT * FROM _ce_mined)
SELECT b.*,
       (b.category IS NOT NULL AND NOT list_has_all(k.known, b.category)) AS unknown_category
FROM base b, _corr_known k;
