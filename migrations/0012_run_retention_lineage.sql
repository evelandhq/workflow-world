-- Materialize the workflow graph root so deadline maintenance can protect a
-- complete lineage without recursively walking JSON attributes on every pass.
alter table workflow.workflow_runs
  add column if not exists retention_root_run_id varchar;

-- Roots are self-owned. Parent-only descendants inherit through as many
-- already persisted generations as are available.
update workflow.workflow_runs
   set retention_root_run_id = coalesce(
     attributes ->> '$rootRunId',
     attributes ->> '$eve.root',
     id
   )
 where retention_root_run_id is null
   and coalesce(attributes ->> '$parentRunId', attributes ->> '$eve.parent') is null;

with recursive resolved as (
  select tenant_id, id, retention_root_run_id
    from workflow.workflow_runs
   where retention_root_run_id is not null
  union
  select child.tenant_id, child.id, parent.retention_root_run_id
    from workflow.workflow_runs as child
    join resolved as parent
      on parent.tenant_id = child.tenant_id
     and parent.id = coalesce(
       child.attributes ->> '$parentRunId',
       child.attributes ->> '$eve.parent'
     )
   where child.retention_root_run_id is null
)
update workflow.workflow_runs as runs
   set retention_root_run_id = resolved.retention_root_run_id
  from resolved
 where runs.tenant_id = resolved.tenant_id
   and runs.id = resolved.id
   and runs.retention_root_run_id is null;

-- Explicit roots and unresolved legacy parents still form a stable protective
-- group. Forward writes resolve parent-only lineage from the stored parent row.
update workflow.workflow_runs
   set retention_root_run_id = coalesce(
     attributes ->> '$rootRunId',
     attributes ->> '$eve.root',
     attributes ->> '$parentRunId',
     attributes ->> '$eve.parent',
     id
   )
 where retention_root_run_id is null;

alter table workflow.workflow_runs
  alter column retention_root_run_id set not null;

create index if not exists workflow_runs_retention_lineage_index
  on workflow.workflow_runs (tenant_id, retention_root_run_id, status);

create or replace function workflow.set_run_retention_deadlines()
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
