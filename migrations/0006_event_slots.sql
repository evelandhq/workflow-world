-- Event ids for newly-created runs become per-run slot positions (`evnt_`
-- plus a zero-padded decimal). Runs created before this migration have no row
-- in workflow_event_slots and keep their `wevt_` ULIDs for their whole life.
--
-- The new primary key includes tenant_id because workflow_events is LIST
-- partitioned by it, and includes run_id because slot 1 exists once per run.
-- Replacing a primary key takes ACCESS EXCLUSIVE, so fail instead of waiting
-- indefinitely behind a long reader. Operators with a large event table should
-- apply this in a maintenance window and may raise the timeout deliberately.

set local lock_timeout = '10s';

alter table workflow.workflow_events
  drop constraint workflow_events_pkey;

alter table workflow.workflow_events
  add constraint workflow_events_pkey primary key (tenant_id, run_id, id);

-- The new primary key already serves every tenant+run range scan.
drop index if exists workflow.workflow_events_tenant_run_index;

-- A marker only. The next position is read from workflow_events and occupied by
-- the same INSERT, so failed writes never advance anything and cannot make a
-- hole in the dense log.
create table if not exists workflow.workflow_event_slots (
  tenant_id varchar not null,
  run_id varchar not null,
  constraint workflow_event_slots_pkey primary key (tenant_id, run_id)
);
