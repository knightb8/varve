-- varve staging assembly (W2). Runs after prelude + whichever runtime adapters
-- were included this cycle, before sql/affected.sql. Unions the per-runtime
-- staging tables (_cc_*, _pi_*, _oc_*, _cp_*) into the shared stg_* that the pipeline
-- COPYs to the lake, and the per-runtime file maps into _files.
--
-- A runtime whose adapter did NOT run (source absent, or no session files
-- found) has no _<rt>_* tables. The stubs below create them
-- empty so the union always resolves; IF NOT EXISTS is a no-op when the adapter
-- already built the real table. Stubs carry the partition-critical columns
-- (session_id, runtime, date) so COPY ... PARTITION_BY (runtime, date) and
-- affected.sql's stg_sessions.date resolve even in an all-absent (no-op) run.
--
-- UNION ALL BY NAME reconciles differing column sets across runtimes: each
-- runtime's rows keep their own columns and get NULL for the others, which is the
-- nullable-runtime-specific-fields contract (RFC-0001). Empty stubs contribute
-- zero rows.

CREATE TABLE IF NOT EXISTS _cc_sessions   (session_id VARCHAR, runtime VARCHAR, date VARCHAR);
CREATE TABLE IF NOT EXISTS _pi_sessions   (session_id VARCHAR, runtime VARCHAR, date VARCHAR);
CREATE TABLE IF NOT EXISTS _oc_sessions   (session_id VARCHAR, runtime VARCHAR, date VARCHAR);
CREATE TABLE IF NOT EXISTS _cp_sessions   (session_id VARCHAR, runtime VARCHAR, date VARCHAR);
CREATE TABLE IF NOT EXISTS _agy_sessions  (session_id VARCHAR, runtime VARCHAR, date VARCHAR);
CREATE TABLE IF NOT EXISTS _vibe_sessions (session_id VARCHAR, runtime VARCHAR, date VARCHAR);
CREATE TABLE IF NOT EXISTS _cc_messages   (session_id VARCHAR, runtime VARCHAR, date VARCHAR);
CREATE TABLE IF NOT EXISTS _pi_messages   (session_id VARCHAR, runtime VARCHAR, date VARCHAR);
CREATE TABLE IF NOT EXISTS _oc_messages   (session_id VARCHAR, runtime VARCHAR, date VARCHAR);
CREATE TABLE IF NOT EXISTS _cp_messages   (session_id VARCHAR, runtime VARCHAR, date VARCHAR);
CREATE TABLE IF NOT EXISTS _agy_messages  (session_id VARCHAR, runtime VARCHAR, date VARCHAR);
CREATE TABLE IF NOT EXISTS _vibe_messages (session_id VARCHAR, runtime VARCHAR, date VARCHAR);
CREATE TABLE IF NOT EXISTS _cc_api_calls  (session_id VARCHAR, runtime VARCHAR, date VARCHAR);
CREATE TABLE IF NOT EXISTS _pi_api_calls  (session_id VARCHAR, runtime VARCHAR, date VARCHAR);
CREATE TABLE IF NOT EXISTS _oc_api_calls  (session_id VARCHAR, runtime VARCHAR, date VARCHAR);
CREATE TABLE IF NOT EXISTS _cp_api_calls  (session_id VARCHAR, runtime VARCHAR, date VARCHAR);
CREATE TABLE IF NOT EXISTS _agy_api_calls (session_id VARCHAR, runtime VARCHAR, date VARCHAR);
CREATE TABLE IF NOT EXISTS _vibe_api_calls (session_id VARCHAR, runtime VARCHAR, date VARCHAR);
CREATE TABLE IF NOT EXISTS _cc_tool_calls (session_id VARCHAR, runtime VARCHAR, date VARCHAR);
CREATE TABLE IF NOT EXISTS _pi_tool_calls (session_id VARCHAR, runtime VARCHAR, date VARCHAR);
CREATE TABLE IF NOT EXISTS _oc_tool_calls (session_id VARCHAR, runtime VARCHAR, date VARCHAR);
CREATE TABLE IF NOT EXISTS _cp_tool_calls (session_id VARCHAR, runtime VARCHAR, date VARCHAR);
CREATE TABLE IF NOT EXISTS _agy_tool_calls (session_id VARCHAR, runtime VARCHAR, date VARCHAR);
CREATE TABLE IF NOT EXISTS _vibe_tool_calls (session_id VARCHAR, runtime VARCHAR, date VARCHAR);
CREATE TABLE IF NOT EXISTS _cc_files (path VARCHAR, session_id VARCHAR, date VARCHAR);
CREATE TABLE IF NOT EXISTS _pi_files (path VARCHAR, session_id VARCHAR, date VARCHAR);
CREATE TABLE IF NOT EXISTS _oc_files (path VARCHAR, session_id VARCHAR, date VARCHAR);
CREATE TABLE IF NOT EXISTS _cp_files (path VARCHAR, session_id VARCHAR, date VARCHAR);
CREATE TABLE IF NOT EXISTS _agy_files (path VARCHAR, session_id VARCHAR, date VARCHAR);
CREATE TABLE IF NOT EXISTS _vibe_files (path VARCHAR, session_id VARCHAR, date VARCHAR);

CREATE OR REPLACE TABLE stg_sessions AS
  SELECT * FROM _cc_sessions
  UNION ALL BY NAME SELECT * FROM _pi_sessions
  UNION ALL BY NAME SELECT * FROM _oc_sessions
  UNION ALL BY NAME SELECT * FROM _cp_sessions
  UNION ALL BY NAME SELECT * FROM _agy_sessions
  UNION ALL BY NAME SELECT * FROM _vibe_sessions;

CREATE OR REPLACE TABLE stg_messages AS
  SELECT * FROM _cc_messages
  UNION ALL BY NAME SELECT * FROM _pi_messages
  UNION ALL BY NAME SELECT * FROM _oc_messages
  UNION ALL BY NAME SELECT * FROM _cp_messages
  UNION ALL BY NAME SELECT * FROM _agy_messages
  UNION ALL BY NAME SELECT * FROM _vibe_messages;

CREATE OR REPLACE TABLE stg_api_calls AS
  SELECT * FROM _cc_api_calls
  UNION ALL BY NAME SELECT * FROM _pi_api_calls
  UNION ALL BY NAME SELECT * FROM _oc_api_calls
  UNION ALL BY NAME SELECT * FROM _cp_api_calls
  UNION ALL BY NAME SELECT * FROM _agy_api_calls
  UNION ALL BY NAME SELECT * FROM _vibe_api_calls;

CREATE OR REPLACE TABLE stg_tool_calls AS
  SELECT * FROM _cc_tool_calls
  UNION ALL BY NAME SELECT * FROM _pi_tool_calls
  UNION ALL BY NAME SELECT * FROM _oc_tool_calls
  UNION ALL BY NAME SELECT * FROM _cp_tool_calls
  UNION ALL BY NAME SELECT * FROM _agy_tool_calls
  UNION ALL BY NAME SELECT * FROM _vibe_tool_calls;

CREATE OR REPLACE TABLE _files AS
  SELECT * FROM _cc_files
  UNION ALL BY NAME SELECT * FROM _pi_files
  UNION ALL BY NAME SELECT * FROM _oc_files
  UNION ALL BY NAME SELECT * FROM _cp_files
  UNION ALL BY NAME SELECT * FROM _agy_files
  UNION ALL BY NAME SELECT * FROM _vibe_files;

-- Copilot-only tables (adapters/copilot.sql). Stubbed when copilot did not run so
-- the justfile's COPY statements always resolve; empty stubs write no partitions.
CREATE TABLE IF NOT EXISTS _cp_events          (session_id VARCHAR, runtime VARCHAR, date VARCHAR);
CREATE TABLE IF NOT EXISTS _cp_checkpoints     (session_id VARCHAR, runtime VARCHAR, date VARCHAR);
CREATE TABLE IF NOT EXISTS _cp_session_files   (session_id VARCHAR, runtime VARCHAR, date VARCHAR);
CREATE TABLE IF NOT EXISTS _cp_session_refs    (session_id VARCHAR, runtime VARCHAR, date VARCHAR);
CREATE TABLE IF NOT EXISTS _cp_artifacts       (session_id VARCHAR, runtime VARCHAR, date VARCHAR);
