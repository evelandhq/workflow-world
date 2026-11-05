-- The in-band writer fence for single-orchestrator runs (World spec 9,
-- `capabilities.inBandFence`): how many positions the run's orchestrator
-- writes have been accepted for, `run_created` included. Every existing run
-- starts at 1, the value a run with no fenced write has; only spec 9 runs ever
-- advance it. Upstream's world-postgres added the same column in its 0026.
--
-- Adding a column with a constant default is a catalog-only change in
-- Postgres 11+, so this takes a brief ACCESS EXCLUSIVE lock and rewrites no
-- rows.
alter table workflow.workflow_event_slots
  add column if not exists seq_in_band integer not null default 1;
