-- Scheduled success is cheap to diagnose from Eveland's long-lived read model,
-- while failures and cancellations keep their execution detail longer.
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

-- Existing terminal rows keep their already assigned deadlines. This avoids a
-- migration racing an older dispatcher that does not yet have graph-level GC
-- guards. A later bounded backfill may opt them into the shorter policy after
-- every dispatcher has been upgraded.
