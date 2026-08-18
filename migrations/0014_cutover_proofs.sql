-- World-visible cutover proof. The dispatcher must not read the control-plane
-- database, yet its recover-paused startup has to know that the operator's
-- cutover postcondition actually ran and passed against THIS database — a
-- control-plane fence alone is invisible here. The cutover command records a
-- proof row after its postcondition passes; the dispatcher's preflight
-- requires a passed proof for its exact operation before boot recovery runs.
create table if not exists workflow.cutover_proofs (
  operation_id text not null,
  passed boolean not null,
  claimable_unscoped_jobs integer not null,
  blocking_runs integer not null,
  recorded_at timestamptz not null default now(),
  recorded_by text not null,
  constraint cutover_proofs_pkey primary key (operation_id, recorded_at)
);
