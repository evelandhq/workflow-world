-- Supports bounded deletion of stream snapshots after their run's terminal
-- state has aged past the host-defined retention window.
create index if not exists workflow_runs_terminal_retention_index
  on workflow.workflow_runs (
    (coalesce(completed_at, updated_at)),
    tenant_id,
    id
  )
  where status in ('completed', 'failed', 'cancelled');
