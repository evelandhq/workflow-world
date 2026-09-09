-- A hook token is not bounded by anything this package controls. eve builds a
-- subagent's continuation token from the model-issued tool-call id, and some
-- providers encode a reasoning signature into that id, so a token can run to
-- several kilobytes. The plain btree on (tenant_id, token) then refuses the row:
-- "index row size N exceeds btree version 4 maximum 2704". The child run can
-- never register its hook, its parent times out with "Hook not found", and the
-- dispatcher retries the doomed delivery for days (eveland#521).
--
-- Index the md5 of the token instead, which PostgreSQL itself suggests in that
-- error. Every by-token lookup adds `md5(token) = md5($1)` so the planner can
-- use it, and keeps `token = $1` so a digest collision still cannot resolve to
-- another token.
drop index if exists workflow.workflow_hooks_tenant_token_index;
create index if not exists workflow_hooks_tenant_token_md5_index
  on workflow.workflow_hooks (tenant_id, md5(token));
