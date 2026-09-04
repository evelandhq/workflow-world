-- An unresolved dead letter is a claim about the present: this installation
-- dropped a delivery, the message is still replayable, and while the row
-- stands it also quarantines its run from boot recovery. Every part of that
-- needs a run that can still be replayed. Once the run is terminal there is
-- nothing left to redeliver and nothing left to hold back, so the row is
-- history — but nothing ever said so, and `resolved_at` had no writer at all.
--
-- The visible cost is the operator-facing count, which only ever grew: one
-- Deployment that can never activate again produces a letter per delivery, the
-- host's own reconciliation then settles its runs, and the letters stay
-- outstanding forever. A number that is supposed to mean "dropped work waiting
-- for an operator" turns into a count of everything that ever went wrong.
--
-- A trigger rather than a call at each terminal transition: the World writes a
-- terminal status from several places (the run_completed / run_failed /
-- run_cancelled event paths, the legacy event path, host reconciliation), and
-- this is an invariant of the table rather than a courtesy of one writer.
create or replace function workflow.resolve_dead_letters_on_terminal_run()
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

drop trigger if exists workflow_runs_resolve_dead_letters
  on workflow.workflow_runs;
-- Only on a real transition: the terminal write paths are conditional on the
-- run still being active and re-run harmlessly, and re-stamping an already
-- resolved letter would move an operator's own decision timestamp.
create trigger workflow_runs_resolve_dead_letters
after update of status
on workflow.workflow_runs
for each row
when (
  new.status in ('completed', 'failed', 'cancelled')
  and old.status is distinct from new.status
)
execute function workflow.resolve_dead_letters_on_terminal_run();

-- The accumulated history. Two kinds, both unreplayable and neither actionable:
-- the run reached a terminal status before this trigger existed, or the run row
-- is gone entirely (retention prunes terminal runs, and a letter naming a run
-- nobody can produce is not a decision anyone can make). The payload column is
-- untouched either way — resolving a letter records that it needs no operator,
-- it does not discard the dropped message.
update workflow.dispatch_dead_letters as dead
   set resolved_at = now()
 where dead.resolved_at is null
   and dead.run_id is not null
   and not exists (
     select 1
       from workflow.workflow_runs as runs
      where runs.tenant_id = dead.tenant_id
        and runs.id = dead.run_id
        and runs.status in ('pending', 'running')
   );
