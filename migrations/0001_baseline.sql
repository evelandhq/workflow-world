-- The workflow world schema.
--
-- Differences from @workflow/world-postgres, which this is ported from:
--   * every table carries `tenant_id` and it leads every primary key;
--   * `workflow_events` and `workflow_stream_chunks` are LIST-partitioned by
--     `tenant_id`, so deleting a tenant is a DROP TABLE of its partitions
--     rather than an unbounded DELETE;
--   * entity payloads are CBOR only (`*_cbor` columns);
--   * runs carry retention deadlines, and dead letters record the deliveries
--     the dispatcher gave up on.
--
-- Plain CREATEs, not IF NOT EXISTS: this runs once, on an empty schema, and a
-- database that already holds these tables is not one this file applies to.

create type workflow.status as enum ('pending', 'running', 'completed', 'failed', 'cancelled');
create type workflow.step_status as enum ('pending', 'running', 'completed', 'failed', 'cancelled');
create type workflow.wait_status as enum ('waiting', 'completed');

-- Runs ------------------------------------------------------------------------

create table workflow.workflow_runs (
  tenant_id varchar not null,
  id varchar not null,
  output_cbor bytea,
  deployment_id varchar not null,
  status workflow.status not null,
  name varchar not null,
  -- eve's queue namespace as the creating deployment resolved it; '' for
  -- none. The dispatcher's boot sweep rebuilds a run's topic from it, because
  -- the host's own environment is the wrong authority.
  queue_namespace varchar default '' not null,
  spec_version integer,
  execution_context_cbor bytea,
  encryption_public_key varchar,
  input_cbor bytea,
  error_cbor bytea,
  error_code varchar,
  -- Why the host settled the run (`reconcileWorkflowRuns`), for operators.
  reconciled_reason text,
  attributes jsonb default '{}'::jsonb not null,
  created_at timestamp default now() not null,
  updated_at timestamp default now() not null,
  completed_at timestamp,
  started_at timestamp,
  expired_at timestamp,
  retention_class varchar default 'interactive' not null,
  -- The workflow graph's root, materialized so maintenance can protect a whole
  -- lineage without walking attributes on every pass.
  retention_root_run_id varchar not null,
  compact_after timestamp,
  expire_after timestamp,
  detail_expire_after timestamp,
  -- The start Hook the run was admitted with, without its metadata.
  start_hook_cbor bytea,
  constraint workflow_runs_pkey primary key (tenant_id, id),
  constraint workflow_runs_retention_class_check
    check (retention_class in ('scheduled', 'interactive', 'persistent'))
);

create index workflow_runs_name_index on workflow.workflow_runs (name);
create index workflow_runs_tenant_status_index on workflow.workflow_runs (tenant_id, status);
create index workflow_runs_tenant_created_index on workflow.workflow_runs (tenant_id, created_at);
create index workflow_runs_retention_lineage_index
  on workflow.workflow_runs (tenant_id, retention_root_run_id, status);
create index workflow_runs_compact_after_index
  on workflow.workflow_runs (compact_after, tenant_id, id)
  where compact_after is not null;
create index workflow_runs_expire_after_index
  on workflow.workflow_runs (expire_after, tenant_id, id)
  where expire_after is not null;
create index workflow_runs_detail_expire_after_index
  on workflow.workflow_runs (detail_expire_after, tenant_id, id)
  where detail_expire_after is not null;
-- Caller-selected stream cleanup by terminal age.
create index workflow_runs_terminal_retention_index
  on workflow.workflow_runs ((coalesce(completed_at, updated_at)), tenant_id, id)
  where status in ('completed', 'failed', 'cancelled');
-- The deployment retention guard's only query. Partial, because the share of
-- rows that are still pending or running stays small as history grows.
create index workflow_runs_active_deployment_index
  on workflow.workflow_runs (deployment_id)
  where status in ('pending', 'running');

-- Resolves the retention root on insert, and assigns deadlines whenever a run
-- enters a terminal state or changes class.
create function workflow.set_run_retention_deadlines()
returns trigger
language plpgsql
as $$
declare
  terminal_at timestamp;
  declared_parent varchar;
begin
  if new.retention_root_run_id is null then
    new.retention_root_run_id := coalesce(
      new.attributes ->> '$rootRunId',
      new.attributes ->> '$eve.root'
    );
    if new.retention_root_run_id is null then
      declared_parent := coalesce(
        new.attributes ->> '$parentRunId',
        new.attributes ->> '$eve.parent'
      );
      if declared_parent is not null then
        select coalesce(parent.retention_root_run_id, parent.id)
          into new.retention_root_run_id
          from workflow.workflow_runs as parent
         where parent.tenant_id = new.tenant_id
           and parent.id = declared_parent;
        new.retention_root_run_id := coalesce(new.retention_root_run_id, declared_parent);
      else
        new.retention_root_run_id := new.id;
      end if;
    end if;
  end if;

  if new.status not in ('completed', 'failed', 'cancelled') then
    new.compact_after := null;
    new.expire_after := null;
    new.detail_expire_after := null;
    return new;
  end if;

  terminal_at := coalesce(new.completed_at, new.updated_at, now()::timestamp);
  if new.retention_class = 'scheduled' then
    new.compact_after := terminal_at + interval '1 minute';
    if new.status = 'completed' then
      new.expire_after := terminal_at + interval '15 minutes';
      new.detail_expire_after := terminal_at + interval '24 hours';
    elsif new.status = 'failed' then
      new.expire_after := terminal_at + interval '1 hour';
      new.detail_expire_after := terminal_at + interval '7 days';
    else
      new.expire_after := terminal_at + interval '1 hour';
      new.detail_expire_after := terminal_at + interval '3 days';
    end if;
  elsif new.retention_class = 'interactive' then
    new.compact_after := terminal_at + interval '5 minutes';
    new.expire_after := terminal_at + interval '24 hours';
    new.detail_expire_after := terminal_at + interval '30 days';
  else
    new.compact_after := null;
    new.expire_after := null;
    new.detail_expire_after := null;
  end if;
  return new;
end;
$$;

create trigger workflow_runs_set_retention_deadlines
before insert or update of status, completed_at, retention_class
on workflow.workflow_runs
for each row execute function workflow.set_run_retention_deadlines();

-- Steps, hooks, waits ---------------------------------------------------------

create table workflow.workflow_steps (
  tenant_id varchar not null,
  run_id varchar not null,
  step_id varchar not null,
  step_name varchar not null,
  status workflow.step_status not null,
  input_cbor bytea,
  output_cbor bytea,
  error_cbor bytea,
  attempt integer not null,
  started_at timestamp,
  completed_at timestamp,
  created_at timestamp default now() not null,
  updated_at timestamp default now() not null,
  retry_after timestamp,
  spec_version integer,
  -- The delivery whose `step_started` started the step, so a restart can be
  -- refused while that delivery is still in flight. NULL when queue-owned.
  owner_message_id varchar,
  constraint workflow_steps_pkey primary key (tenant_id, step_id)
);

create index workflow_steps_tenant_run_index on workflow.workflow_steps (tenant_id, run_id);
create index workflow_steps_tenant_status_index on workflow.workflow_steps (tenant_id, status);

-- An immutable wrapper over `convert_to`, which is only STABLE because it reads
-- the server encoding. A database's server encoding is fixed when it is
-- created, so the wrapper is immutable in fact, which an index expression needs.
create function workflow.hook_token_digest(token text) returns bytea
  language sql immutable strict parallel safe
  as $fn$ select sha256(convert_to(token, 'UTF8')) $fn$;

create table workflow.workflow_hooks (
  tenant_id varchar not null,
  run_id varchar not null,
  hook_id varchar not null,
  token varchar not null,
  owner_id varchar not null,
  -- Upstream's column, distinct from `tenant_id`; world-postgres writes ''.
  project_id varchar not null,
  environment varchar not null,
  created_at timestamp default now() not null,
  -- Keeps the token reserved past the run's end. NULL: delete with the run.
  token_retention_until timestamp with time zone,
  metadata_cbor bytea,
  spec_version integer,
  is_webhook boolean default true,
  is_system boolean default false,
  -- Set on a hook that took its token over from another run (force-claim).
  claimed_from bytea,
  -- At most one claiming row per token within a tenant.
  claims_token boolean default false not null,
  constraint workflow_hooks_pkey primary key (tenant_id, hook_id)
);

create index workflow_hooks_tenant_run_index on workflow.workflow_hooks (tenant_id, run_id);
-- Digests, not the token: a token can run to several kilobytes, past what a
-- btree entry holds. Lookups filter on `md5(token)` to use the first; the
-- second decides ownership, so it uses sha256.
create index workflow_hooks_tenant_token_md5_index
  on workflow.workflow_hooks (tenant_id, md5(token));
create unique index workflow_hooks_token_claim_unique
  on workflow.workflow_hooks (tenant_id, workflow.hook_token_digest(token))
  where claims_token;

create table workflow.workflow_waits (
  tenant_id varchar not null,
  wait_id varchar not null,
  run_id varchar not null,
  status workflow.wait_status not null,
  resume_at timestamp,
  completed_at timestamp,
  created_at timestamp default now() not null,
  updated_at timestamp default now() not null,
  spec_version integer,
  constraint workflow_waits_pkey primary key (tenant_id, wait_id)
);

create index workflow_waits_tenant_run_index on workflow.workflow_waits (tenant_id, run_id);

-- One immutable accept-or-reject decision per start Hook candidate run, written
-- in the transaction that creates an accepted run. Rows older than a
-- candidate's window are deleted as admissions go.
create table workflow.workflow_start_hook_admissions (
  tenant_id varchar not null,
  run_id varchar not null,
  token varchar not null,
  decision varchar not null,
  conflicting_run_id varchar,
  created_at timestamptz default now() not null,
  constraint workflow_start_hook_admissions_pkey primary key (tenant_id, run_id)
);

create index workflow_start_hook_admissions_created_at_index
  on workflow.workflow_start_hook_admissions (created_at);

-- Partitioned tables ----------------------------------------------------------
--
-- No DEFAULT partition, deliberately. Attaching a new partition while a default
-- exists forces Postgres to scan the default for rows that belong in the new
-- one, which would turn tenant provisioning into an O(fleet) operation. Without
-- a default, writing for a tenant whose partition is missing raises
-- `no partition of relation ... found for row`.

-- Event ids are per-run slot positions (`evnt_` and a zero-padded decimal), so
-- the key includes the run.
create table workflow.workflow_events (
  tenant_id varchar not null,
  id varchar not null,
  type varchar not null,
  correlation_id varchar,
  created_at timestamp default now() not null,
  run_id varchar not null,
  payload_cbor bytea,
  spec_version integer,
  constraint workflow_events_pkey primary key (tenant_id, run_id, id)
) partition by list (tenant_id);

create index workflow_events_tenant_correlation_index
  on workflow.workflow_events (tenant_id, correlation_id);
-- Runtime-correlated one-shot events are unique per (run, correlation), so two
-- concurrent invocations producing identical correlation ids cannot both
-- insert. `events.create` translates the violation into EntityConflictError.
create unique index workflow_events_entity_creation_unique
  on workflow.workflow_events (tenant_id, run_id, correlation_id, type)
  where type in ('step_created', 'hook_created', 'wait_created', 'attr_set');

-- One row per run. The next slot is read from workflow_events and occupied by
-- the same INSERT, so this counts only the in-band writer fence's accepted
-- positions (`run_created` included).
create table workflow.workflow_event_slots (
  tenant_id varchar not null,
  run_id varchar not null,
  seq_in_band integer default 1 not null,
  constraint workflow_event_slots_pkey primary key (tenant_id, run_id)
);

create table workflow.workflow_stream_chunks (
  tenant_id varchar not null,
  id varchar not null,
  stream_id varchar not null,
  run_id varchar,
  data bytea not null,
  created_at timestamp default now() not null,
  eof boolean not null,
  -- A packed block keeps its logical chunk ids inside `data`; `id` is the
  -- first of them, so cursors stay valid after a rewrite.
  codec_version integer,
  chunk_count integer,
  last_chunk_id varchar,
  constraint workflow_stream_chunks_pkey primary key (tenant_id, stream_id, id)
) partition by list (tenant_id);

create index workflow_stream_chunks_tenant_run_index
  on workflow.workflow_stream_chunks (tenant_id, run_id);
create index workflow_stream_chunks_block_tail_index
  on workflow.workflow_stream_chunks (tenant_id, stream_id, last_chunk_id)
  where eof = false and last_chunk_id is not null;
-- Once a stream is packed its EOF leaves this index, keeping the dispatcher's
-- candidate scan proportional to the pending backlog.
create index workflow_stream_chunks_pending_pack_index
  on workflow.workflow_stream_chunks (tenant_id, run_id, stream_id, id)
  where eof = true and codec_version is distinct from 2;

-- Dead letters ----------------------------------------------------------------
--
-- Deliveries the dispatcher gave up on. The message is preserved verbatim and
-- can be replayed once the cause is fixed. While unresolved, a row also
-- quarantines its active run from boot recovery; setting resolved_at makes it
-- eligible again.
create table workflow.dispatch_dead_letters (
  id bigserial primary key,
  tenant_id varchar not null,
  deployment_id varchar,
  run_id varchar,
  message_id varchar not null,
  job_name varchar not null,
  queue_name varchar not null,
  attempt integer not null,
  reason text not null,
  payload jsonb not null,
  created_at timestamptz default now() not null,
  resolved_at timestamptz
);

create index dispatch_dead_letters_tenant_index
  on workflow.dispatch_dead_letters (tenant_id, created_at desc);
-- The alarm query: anything unresolved is work the platform has dropped.
create index dispatch_dead_letters_unresolved_index
  on workflow.dispatch_dead_letters (created_at)
  where resolved_at is null;
-- Boot recovery and deployment retention anti-join active runs against it.
create index dispatch_dead_letters_unresolved_run_index
  on workflow.dispatch_dead_letters (tenant_id, run_id)
  where resolved_at is null and run_id is not null;

-- Once a run is terminal there is nothing left to redeliver or hold back, so
-- its unresolved dead letters are resolved. A trigger, because the World
-- writes a terminal status from several places (the terminal event paths and
-- host reconciliation), and this is an invariant of the table.
create function workflow.resolve_dead_letters_on_terminal_run()
returns trigger
language plpgsql
as $$
begin
  update workflow.dispatch_dead_letters
     set resolved_at = now()
   where tenant_id = new.tenant_id
     and run_id = new.id
     and resolved_at is null;
  return null;
end;
$$;

-- Only on a real transition, so re-running a terminal write does not move an
-- operator's own resolution timestamp.
create trigger workflow_runs_resolve_dead_letters
after update of status
on workflow.workflow_runs
for each row
when (
  new.status in ('completed', 'failed', 'cancelled')
  and old.status is distinct from new.status
)
execute function workflow.resolve_dead_letters_on_terminal_run();
