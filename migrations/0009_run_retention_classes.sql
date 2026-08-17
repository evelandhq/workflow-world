alter table workflow.workflow_runs
  add column if not exists retention_class varchar not null default 'interactive',
  add column if not exists compact_after timestamp,
  add column if not exists expire_after timestamp,
  add column if not exists detail_expire_after timestamp;

alter table workflow.workflow_runs
  drop constraint if exists workflow_runs_retention_class_check,
  add constraint workflow_runs_retention_class_check
    check (retention_class in ('scheduled', 'interactive', 'persistent'));

create or replace function workflow.set_run_retention_deadlines()
returns trigger
language plpgsql
as $$
declare
  terminal_at timestamp;
begin
  if new.status not in ('completed', 'failed', 'cancelled') then
    new.compact_after := null;
    new.expire_after := null;
    new.detail_expire_after := null;
    return new;
  end if;

  terminal_at := coalesce(new.completed_at, new.updated_at, now()::timestamp);
  if new.retention_class = 'scheduled' then
    new.compact_after := terminal_at + interval '1 minute';
    new.expire_after := terminal_at + interval '15 minutes';
    new.detail_expire_after := terminal_at + interval '7 days';
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

drop trigger if exists workflow_runs_set_retention_deadlines
  on workflow.workflow_runs;
create trigger workflow_runs_set_retention_deadlines
before insert or update of status, completed_at, retention_class
on workflow.workflow_runs
for each row execute function workflow.set_run_retention_deadlines();

-- Existing terminal rows inherit the historical interactive policy. The EOF
-- marker is not governed by expire_after and remains durable.
update workflow.workflow_runs
   set compact_after = coalesce(compact_after, coalesce(completed_at, updated_at) + interval '5 minutes'),
       expire_after = coalesce(expire_after, coalesce(completed_at, updated_at) + interval '24 hours'),
       detail_expire_after = coalesce(
         detail_expire_after,
         coalesce(completed_at, updated_at) + interval '30 days'
       )
 where status in ('completed', 'failed', 'cancelled')
   and retention_class = 'interactive';

create index if not exists workflow_runs_compact_after_index
  on workflow.workflow_runs (compact_after, tenant_id, id)
  where compact_after is not null;

create index if not exists workflow_runs_expire_after_index
  on workflow.workflow_runs (expire_after, tenant_id, id)
  where expire_after is not null;

create index if not exists workflow_runs_detail_expire_after_index
  on workflow.workflow_runs (detail_expire_after, tenant_id, id)
  where detail_expire_after is not null;
