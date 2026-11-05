-- ABAP fix loops (fork addition, 2026-11-04). Reads the abap_attempts view
-- (sql/copilot_marts.sql): write/activate attempts through ADT MCP servers (vsp),
-- with outcome + per-object sequence flags.
--
-- Result sets:
--   1. totals           attempts, failures, retries after a failure, resubmitted
--                       defective source, attempts inside failure streaks of >= 3
--   2. outcome mix      per outcome; failed outcomes are what loops are made of
--   3. worst objects    session x system x object with >= 5 failed attempts
--   4. loop errors      the first error text inside streaks of >= 3, most frequent first
--   5. per operation    fail rate and resubmission rate by vsp tool
--
-- Usage: just sql -f queries/abap_fix_loops.sql

SELECT count(*)                                            AS attempts,
       count(*) FILTER (WHERE failed)                      AS failed,
       count(*) FILTER (WHERE after_failure)               AS retries_after_failure,
       count(*) FILTER (WHERE resubmitted_defective)       AS resubmitted_defective,
       count(*) FILTER (WHERE same_error_as_prev)          AS same_error_again,
       count(*) FILTER (WHERE fail_streak >= 3)            AS in_streaks_3plus,
       count(DISTINCT session_id || system || object)      AS objects
FROM abap_attempts;

SELECT outcome, failed, count(*) AS n,
       count(*) FILTER (WHERE resubmitted_defective)       AS resubmitted
FROM abap_attempts
GROUP BY ALL
ORDER BY n DESC;

SELECT session_id[1:8]                                     AS session,
       system, object,
       count(*)                                            AS attempts,
       count(*) FILTER (WHERE failed)                      AS fails,
       max(fail_streak)                                    AS worst_streak,
       count(*) FILTER (WHERE resubmitted_defective)       AS resubmitted,
       arg_max(outcome, attempt_no)                        AS last_outcome
FROM abap_attempts
GROUP BY ALL
HAVING count(*) FILTER (WHERE failed) >= 5
ORDER BY fails DESC
LIMIT 25;

SELECT outcome,
       left(regexp_replace(first_error, '[A-Z0-9_/]{6,}', 'X', 'g'), 120) AS error_signature,
       count(*)                                            AS n,
       count(DISTINCT session_id)                          AS sessions
FROM abap_attempts
WHERE fail_streak >= 3
GROUP BY ALL
ORDER BY n DESC
LIMIT 25;

SELECT op,
       count(*)                                            AS attempts,
       round(100.0 * count(*) FILTER (WHERE failed) / count(*), 1)                AS fail_pct,
       round(100.0 * count(*) FILTER (WHERE resubmitted_defective) / count(*), 1) AS resubmit_pct
FROM abap_attempts
GROUP BY ALL
ORDER BY attempts DESC;
