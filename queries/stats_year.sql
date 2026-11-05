-- QUESTION: What did the year look like? An opencode-stats-style year summary: one
--   local-time day per cell, plus year totals. `just stats` renders it
--   (sql/stats_render.sql); this file computes it. ADR-0007 records the definitions.
-- WINDOW: local-time days from Jan 1 of `year` to min(Dec 31, `today`). Every event is
--   bucketed by the local day of its row timestamp, not the session-start partition.
-- METRIC (per runtime, summed over the runtime filter):
--   session     = one top-level transcript: a sessions row with parent_session_id NULL,
--                 counted on its started_at day.
--   subagent    = one child transcript or sidechain: a Claude Code sidechain root
--                 (is_sidechain message with no parent_id), or an opencode session with
--                 parent_session_id set. pi writes no child transcript: always 0.
--   prompt      = one human-authored user turn: an operator_messages row (the W7
--                 is_operator predicate: tool results and harness injections excluded)
--                 that is neither a sidechain message nor in an opencode child session;
--                 pi, absent from operator_messages, counts its role='user' messages.
--   step        = one assistant turn: one api_calls row.
--   tokens      = input + output + cache_read + cache_write over api_calls.
--   cost_usd    = sum(api_calls.cost_usd), priced at ingest (adapters, pricing.csv).
--   tool success= tool results not marked as errors / all tool results. Claude Code:
--                 tool_result blocks in user messages, error = is_error, one per
--                 tool_use_id. opencode: tool_calls with decision completed|error.
--                 pi: tool_calls (from toolResult), error = decision 'error'.
--   active day  = a local day with at least one prompt.
--   best streak = the longest run of consecutive active days inside the window.
--   level       = heatmap intensity of an active day: 1..4 by the `metric` value
--                 against its quartiles over the window's active days; 0 if inactive.
-- PARAMETERS: year (default: the year of `today`), runtime claude|pi|opencode|copilot|antigravity|vibe|all
--   (default all), metric prompts|steps|tokens (default prompts), today (default
--   current_date). Local time is the DuckDB TimeZone setting (from TZ).
-- CAVEATS: Claude Code and pi timestamps are UTC in the lake; opencode timestamps are
--   wall-clock in the TimeZone of the ingest run (to_timestamp cast to TIMESTAMP), so
--   they are taken as local as-is. A streak that runs across Jan 1 is cut at the window.
-- Run: just stats [--year N] [--runtime R] [--metric M], or just sql -f queries/stats_year.sql
CREATE OR REPLACE TEMP MACRO stats_local_day(rt, ts) AS
  CASE WHEN rt = 'opencode' THEN ts
       ELSE timezone(current_setting('TimeZone'), timezone('UTC', ts)) END::date;

CREATE OR REPLACE TEMP TABLE stats_window AS
WITH p AS (
  SELECT coalesce(try_cast(getvariable('today') AS DATE), current_date)        AS today,
         lower(coalesce(getvariable('runtime')::varchar, 'all'))               AS rt,
         lower(coalesce(getvariable('metric')::varchar, 'prompts'))            AS metric
)
SELECT
  y                                                           AS year,
  CASE rt WHEN 'claude' THEN 'claude-code' ELSE rt END        AS runtime,
  metric,
  today,
  make_date(y, 1, 1)                                          AS first_day,
  least(make_date(y, 12, 31), today)                          AS last_day
FROM p, (SELECT coalesce(try_cast(getvariable('year') AS INTEGER), year(today)) AS y FROM p);

CREATE OR REPLACE TEMP TABLE stats_cc_tool_carriers AS
SELECT runtime, timestamp, content
FROM messages
WHERE runtime = 'claude-code' AND role = 'user'
  AND json_type(content) = 'ARRAY' AND content LIKE '%tool_result%';

CREATE OR REPLACE TEMP TABLE stats_events AS
SELECT runtime, stats_local_day(runtime, started_at) AS day, 'session' AS kind, 1::double AS n
FROM sessions WHERE parent_session_id IS NULL
UNION ALL
SELECT runtime, stats_local_day(runtime, started_at), 'subagent', 1
FROM sessions WHERE parent_session_id IS NOT NULL
UNION ALL
SELECT runtime, stats_local_day(runtime, min(timestamp)), 'subagent', 1
FROM messages
WHERE runtime = 'claude-code' AND is_sidechain AND parent_id IS NULL
GROUP BY runtime, message_id
UNION ALL
SELECT o.runtime, stats_local_day(o.runtime, o.ts), 'prompt', 1
FROM operator_messages o
JOIN sessions s ON s.runtime = o.runtime AND s.session_id = o.session_id
WHERE s.parent_session_id IS NULL
  AND NOT EXISTS (SELECT 1 FROM messages m
                  WHERE m.runtime = o.runtime AND m.session_id = o.session_id
                    AND m.message_id = o.message_uuid AND m.is_sidechain)
UNION ALL
SELECT runtime, stats_local_day(runtime, timestamp), 'prompt', 1
FROM messages WHERE runtime = 'pi' AND role = 'user'
UNION ALL
SELECT runtime, stats_local_day(runtime, timestamp), unnest(['step', 'tokens', 'cost']),
       unnest([1, input + output + cache_read + cache_write, cost_usd])
FROM api_calls
UNION ALL
SELECT runtime, stats_local_day(runtime, started_at), unnest(['tool_result', 'tool_error']),
       unnest([1, (decision IS NOT DISTINCT FROM 'error')::int])
FROM tool_calls
WHERE runtime = 'pi' OR (runtime IN ('opencode', 'copilot', 'antigravity', 'vibe') AND decision IN ('completed', 'error'))
UNION ALL
SELECT runtime, min(day), unnest(['tool_result', 'tool_error']), unnest([1, max(is_error)::int])
FROM (
  SELECT runtime, stats_local_day(runtime, timestamp) AS day,
         blk ->> 'tool_use_id' AS tool_use_id,
         coalesce(try_cast(blk ->> 'is_error' AS BOOLEAN), false) AS is_error
  FROM (SELECT runtime, timestamp, unnest(cast(content AS json[])) AS blk FROM stats_cc_tool_carriers)
  WHERE blk ->> 'type' = 'tool_result'
)
GROUP BY runtime, tool_use_id;

DROP TABLE stats_cc_tool_carriers;

CREATE OR REPLACE TEMP TABLE stats_days AS
WITH days AS (
  SELECT unnest(generate_series(w.first_day, w.last_day, INTERVAL 1 DAY))::date AS day
  FROM stats_window w
),
per_day AS (
  SELECT day,
         sum(n) FILTER (WHERE kind = 'session')     AS sessions,
         sum(n) FILTER (WHERE kind = 'subagent')    AS subagents,
         sum(n) FILTER (WHERE kind = 'prompt')      AS prompts,
         sum(n) FILTER (WHERE kind = 'step')        AS steps,
         sum(n) FILTER (WHERE kind = 'tokens')      AS tokens,
         sum(n) FILTER (WHERE kind = 'cost')        AS cost_usd,
         sum(n) FILTER (WHERE kind = 'tool_result') AS tool_results,
         sum(n) FILTER (WHERE kind = 'tool_error')  AS tool_errors
  FROM stats_events e, stats_window w
  WHERE (w.runtime = 'all' OR e.runtime = w.runtime) AND e.day BETWEEN w.first_day AND w.last_day
  GROUP BY day
),
filled AS (
  SELECT d.day,
         coalesce(sessions, 0)::bigint     AS sessions,
         coalesce(subagents, 0)::bigint    AS subagents,
         coalesce(prompts, 0)::bigint      AS prompts,
         coalesce(steps, 0)::bigint        AS steps,
         coalesce(tokens, 0)::bigint       AS tokens,
         coalesce(cost_usd, 0)             AS cost_usd,
         coalesce(tool_results, 0)::bigint AS tool_results,
         coalesce(tool_errors, 0)::bigint  AS tool_errors,
         coalesce(prompts, 0) > 0          AS active
  FROM days d LEFT JOIN per_day USING (day)
),
valued AS (
  SELECT f.*,
         CASE w.metric WHEN 'steps' THEN steps WHEN 'tokens' THEN tokens ELSE prompts END AS value
  FROM filled f, stats_window w
),
quartiles AS (
  SELECT quantile_disc(value, [0.25, 0.5, 0.75]) AS q FROM valued WHERE active
)
SELECT v.*,
       CASE WHEN NOT active THEN 0
            ELSE 1 + (value > q[1])::int + (value > q[2])::int + (value > q[3])::int END AS level
FROM valued v, quartiles;

CREATE OR REPLACE TEMP TABLE stats_totals AS
WITH runs AS (
  SELECT count(*) AS len
  FROM (SELECT day, day - (row_number() OVER (ORDER BY day))::int AS grp FROM stats_days WHERE active)
  GROUP BY grp
)
SELECT
  w.year, w.runtime, w.metric, w.today, w.first_day, w.last_day,
  sum(sessions)                         AS sessions,
  sum(subagents)                        AS subagents,
  sum(prompts)                          AS prompts,
  sum(steps)                            AS steps,
  sum(tokens)                           AS tokens,
  round(sum(cost_usd), 2)               AS cost_usd,
  sum(tool_results)                     AS tool_results,
  sum(tool_errors)                      AS tool_errors,
  round(100.0 * (sum(tool_results) - sum(tool_errors)) / nullif(sum(tool_results), 0), 1)
                                        AS tool_success_pct,
  count(*) FILTER (WHERE active)        AS active_days,
  coalesce((SELECT max(len) FROM runs), 0) AS best_streak
FROM stats_window w LEFT JOIN stats_days ON true
GROUP BY ALL;

SELECT * FROM stats_totals;
