-- MCP tool failure profile (fork addition, 2026-11-04). Runtime-agnostic: reads the
-- nullable mcp_server / mcp_tool / error_message columns (populated by the copilot
-- adapter today; NULL for runtimes that do not record them, which just drop out).
--
-- Three result sets:
--   1. per server        : calls, error/denied counts, error rate
--   2. per server x tool : same, tools with >= 5 calls, worst first
--   3. error signatures  : error_message normalized (quoted names, long upper-case
--                          identifiers, numbers and URLs' tails collapsed) so the same
--                          failure across objects/systems groups into one row
--
-- Usage:  just sql -f queries/mcp_failures.sql
--         filter one server family by editing the WHERE in _mcp below, e.g.
--         AND mcp_server SIMILAR TO '.*(HNA|ECC).*'

CREATE OR REPLACE TEMP VIEW _mcp AS
SELECT runtime, session_id, mcp_server, mcp_tool, decision, error_code, error_message,
       regexp_replace(regexp_replace(regexp_replace(regexp_replace(
         coalesce(error_message, ''),
         '''[^'']*''', '''…''', 'g'),        -- quoted server/object names
         '[A-Z][A-Z0-9_/]{5,}', 'X', 'g'),    -- ABAP-style identifiers
         '\d+', 'N', 'g'),                    -- numbers (keeps "status N")
         '(/sap/bc/adt/[a-z/]+)[^ ]*', '\1', 'g') AS error_sig
FROM tool_calls
WHERE mcp_server IS NOT NULL;

SELECT mcp_server,
       count(*)                                         AS calls,
       count(*) FILTER (WHERE decision = 'error')       AS errors,
       count(*) FILTER (WHERE decision = 'denied')      AS denied,
       round(100.0 * count(*) FILTER (WHERE decision = 'error') / count(*), 1) AS err_pct
FROM _mcp
GROUP BY ALL
ORDER BY calls DESC;

SELECT mcp_server, mcp_tool,
       count(*)                                         AS calls,
       count(*) FILTER (WHERE decision = 'error')       AS errors,
       round(100.0 * count(*) FILTER (WHERE decision = 'error') / count(*), 1) AS err_pct
FROM _mcp
GROUP BY ALL
HAVING count(*) >= 5
ORDER BY errors DESC, calls DESC
LIMIT 40;

SELECT left(error_sig, 140)                             AS error_signature,
       count(*)                                         AS n,
       count(DISTINCT session_id)                       AS sessions,
       list_sort(list_distinct(list(mcp_tool)))[1:5]    AS tools
FROM _mcp
WHERE decision = 'error'
GROUP BY ALL
ORDER BY n DESC
LIMIT 40;
