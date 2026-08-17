-- Boot recovery and deployment retention both correlate active runs with an
-- unresolved dead letter. The original indexes lead with created_at, which
-- cannot support the tenant/run anti-join and turn every sweep into repeated
-- dead-letter scans.
create index if not exists dispatch_dead_letters_unresolved_run_index
  on workflow.dispatch_dead_letters (tenant_id, run_id)
  where resolved_at is null and run_id is not null;
