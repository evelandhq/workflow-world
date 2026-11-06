-- Atomic start Hook admission (`start(workflow, args, { experimental_hook })`,
-- `capabilities.atomicStartHook`; eve 1.0.0-rc.6 starts every session this
-- way). Upstream's world-postgres added the same shape in its 0028, keyed by
-- tenant here like every other table.
--
-- The column additions and the unique index take locks that queue every other
-- query on workflow_runs and workflow_hooks behind them, so give up after 10
-- seconds (unless the session set its own `lock_timeout`) and leave the
-- migration unapplied and retryable rather than stall the World. The setting
-- lasts for the migrator's transaction.
do $$
begin
  if current_setting('lock_timeout') = '0' then
    perform set_config('lock_timeout', '10s', true);
  end if;
end $$;

-- One immutable accept-or-reject decision per candidate run, written in the
-- transaction that creates an accepted run. Rows older than a candidate's 24h
-- window are deleted as admissions go (`created_at` index).
create table if not exists workflow.workflow_start_hook_admissions (
  tenant_id varchar not null,
  run_id varchar not null,
  token varchar not null,
  decision varchar not null,
  conflicting_run_id varchar,
  created_at timestamptz default now() not null,
  constraint workflow_start_hook_admissions_pkey primary key (tenant_id, run_id)
);
create index if not exists workflow_start_hook_admissions_created_at_index
  on workflow.workflow_start_hook_admissions (created_at);

-- A run's start Hook is an ordinary row in workflow_hooks, written by its
-- admission. Every Hook row written from here on claims its token, and
-- claiming rows are unique per token within a tenant, so two admissions racing
-- for one token, or an admission and another run's createHook, conflict on the
-- insert instead of both winning. Rows written before this migration do not
-- claim their token, so the index cannot fail on existing data.
--
-- The index keys on a digest of the token, not the token: a token can run to
-- several kilobytes (see 0018), past what a btree entry holds. sha256 rather
-- than 0018's md5 because this index decides ownership, so a collision would
-- refuse a token that nobody holds. sha256 takes bytes, and `convert_to` is
-- only STABLE (it reads the server encoding), which an index expression may
-- not call; a database's server encoding is fixed when it is created, so the
-- wrapper is immutable in fact and is declared so.
create or replace function workflow.hook_token_digest(token text) returns bytea
  language sql immutable strict parallel safe
  as $fn$ select sha256(convert_to(token, 'UTF8')) $fn$;
alter table workflow.workflow_hooks
  add column if not exists claims_token boolean default false not null;
create unique index if not exists workflow_hooks_token_claim_unique
  on workflow.workflow_hooks (tenant_id, workflow.hook_token_digest(token))
  where claims_token;

-- The start Hook a run was admitted with (without its metadata), echoed on the
-- run. Last, so the index build above has scanned workflow_hooks before this
-- takes workflow_runs' lock.
alter table workflow.workflow_runs add column if not exists start_hook_cbor bytea;
