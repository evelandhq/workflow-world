-- Durable run quarantine markers for the external-only cutover.
--
-- A control-plane fence protects the Gateway/API/Worker, but the dispatcher's
-- boot recovery reads only this database — a run that must not be replayed
-- needs a marker the sweep, the enqueue path and the dispatch handler all see.
-- Markers survive restarts and are only closed by explicit resolution or a
-- completed managed termination, never by a process restart.
create table if not exists workflow.run_quarantines (
  tenant_id text not null,
  run_id text not null,
  operation_id text not null,
  reason text not null,
  created_at timestamptz not null default now(),
  resolved_at timestamptz,
  resolved_by text,
  constraint run_quarantines_pkey primary key (tenant_id, run_id)
);

-- Boot recovery anti-joins active runs against unresolved markers on every
-- start; the partial index keeps that from scanning resolved history.
create index if not exists run_quarantines_unresolved_index
  on workflow.run_quarantines (tenant_id, run_id)
  where resolved_at is null;
