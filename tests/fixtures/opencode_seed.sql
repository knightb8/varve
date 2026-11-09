-- varve CI fixture: minimal opencode V1 and V2 contract SQLite store
-- (session/session_v2/message/session_message/part), matching the shape
-- adapters/opencode.sql documents. Run with the target sqlite file already
-- ATTACHed read-write as schema `oc` (see justfile `test` recipe -- ATTACH needs
-- a literal path, same constraint noted in adapters/opencode.sql, so it happens
-- in the recipe, not here). Exercises both V1 (session/message/part) and
-- V2 (session_v2/session_message) stores.

CREATE TABLE oc.session (
  id VARCHAR, parent_id VARCHAR, directory VARCHAR, version VARCHAR,
  agent VARCHAR, model VARCHAR, time_created BIGINT
);
CREATE TABLE oc.session_v2 (
  id VARCHAR, parent_id VARCHAR, directory VARCHAR, version VARCHAR,
  agent VARCHAR, model VARCHAR, time_created BIGINT
);
CREATE TABLE oc.message (
  id VARCHAR, session_id VARCHAR, time_created BIGINT, data VARCHAR
);
CREATE TABLE oc.session_message (
  id VARCHAR, session_id VARCHAR, type VARCHAR, time_created BIGINT, data VARCHAR
);
CREATE TABLE oc.part (
  id VARCHAR, message_id VARCHAR, session_id VARCHAR, time_created BIGINT, data VARCHAR
);

INSERT INTO oc.session VALUES
  ('oc-session-1', NULL, '/home/runner/gitlab.com/testns/testrepo', '1.0.0', 'build',
   'claude-sonnet-5', 1785715200000);

INSERT INTO oc.session_v2 VALUES
  ('oc-session-1', NULL, '/home/runner/gitlab.com/testns/testrepo', '1.0.0', 'build',
   'claude-sonnet-5', 1785715200000),
  ('oc-session-v2', NULL, '/home/runner/gitlab.com/testns/testrepo', '2.0.0', 'build',
   'claude-sonnet-5', 1800000000000);

INSERT INTO oc.message VALUES
  ('oc-msg-1', 'oc-session-1', 1785715200000,
   '{"role":"user"}'),
  ('oc-msg-2', 'oc-session-1', 1785715201000,
   '{"role":"assistant","modelID":"claude-sonnet-5","providerID":"anthropic","time":{"created":1785715201000,"completed":1785715202000},"cost":0.001,"tokens":{"input":100,"output":50,"reasoning":0,"cache":{"read":0,"write":0}}}');

INSERT INTO oc.part VALUES
  ('oc-part-1', 'oc-msg-1', 'oc-session-1', 1785715200000,
   '{"type":"text","text":"please write a hello function"}'),
  ('oc-part-2', 'oc-msg-2', 'oc-session-1', 1785715201500,
   '{"type":"text","text":"Sure thing."}'),
  ('oc-part-3', 'oc-msg-2', 'oc-session-1', 1785715201600,
   '{"type":"tool","callID":"call-1","tool":"bash","state":{"status":"completed","time":{"start":1785715201600,"end":1785715201700}}}');

INSERT INTO oc.session_message VALUES
  ('oc-msg-v2-1', 'oc-session-v2', 'user', 1800000000000,
   '{"text":"run the test suite","time":{"created":1800000000000}}'),
  ('oc-msg-v2-2', 'oc-session-v2', 'assistant', 1800000001000,
   '{"agent":"build","model":{"providerID":"anthropic","id":"claude-sonnet-5"},"time":{"created":1800000001000,"completed":1800000002000},"cost":0.002,"tokens":{"input":150,"output":75,"reasoning":0,"cache":{"read":0,"write":0}},"content":[{"type":"text","text":"Running tests now."},{"type":"tool","id":"call-v2-1","name":"bash","state":{"status":"completed"}}]}');
