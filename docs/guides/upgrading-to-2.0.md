# Upgrading to 2.0

[Documentation](../README.md) · [Getting started](./getting-started.md) ·
[Configuration](../reference/configuration.md)

2.0 moves the serialization of a run's orchestrator deliveries out of the World
and into the dispatcher. A 1.x World put each of a run's orchestrator messages in
a graphile queue of its own (`wfrun:<tenant>:<run>`); a 2.0 World enqueues
without a queue name, and the 2.0 dispatcher holds a gate per run instead,
coalescing wakes that arrive while the run's delivery is in flight. See
[delivery and failure semantics](../design.md#delivery-and-failure-semantics).

2.0 also removes stream snapshot stripping and terminal stream block repacking,
both deprecated in 1.1: eve 1.0 writes delta-only stream appends, so stripping
never changed a chunk, and repacking only rewrote rows that stream expiry
deletes soon after. Packing at write time and reading both row formats are
unchanged; the settings and exports that went with them are listed under
[configuration](#configuration-changes) and [code](#code-changes) changes.

The database needs **no rebuild and no migration**: the schema is unchanged,
and the jobs a 1.x World or dispatcher left behind are handled as described
[below](#what-happens-to-jobs-1x-left-behind).

## The dispatcher and every deployment move together

The dispatch contract is now version 2, and each side refuses the other's
version:

| dispatcher | deployment's World | result                                                          |
| ---------- | ------------------ | --------------------------------------------------------------- |
| 2.0        | 2.0                | delivered                                                       |
| 2.0        | 1.x                | the World answers 400 ("Rebuild the deployment"), dead-lettered |
| 1.x        | 2.0                | the World answers 400 ("Upgrade the dispatcher"), dead-lettered |
| 1.x        | 1.x                | delivered, as before                                            |

A 1.x dispatcher has no gate and a 2.0 World writes no per-run queue, so the
2.0 World refuses that pairing rather than run a run's deliveries side by side.

A deployment's World is baked in at build time, so every deployment has to be
**rebuilt with 2.0**. A run is pinned to the deployment that created it: a run
still pinned to a 1.x build cannot be delivered by a 2.0 dispatcher, and lands
in `dispatch_dead_letters`, parked rather than lost. Either let those runs
finish before the cutover, or rebuild their deployment with 2.0 under the same
deployment id where your host allows it (the same workflow code replays the same
log), and resolve their dead letters afterwards.

## What a host platform changes

- **Advertise dispatch protocol 2.** Register the 2.0 dispatcher with the
  protocol window `2`..`2`, and attest protocol `2` for every Release built
  with 2.0 (and `1` for those still built with 1.x). The 2.0 dispatcher refuses
  an activation whose selected protocol is outside `2`..`2`: it releases the
  lease and dead-letters the message as not activatable, so a control plane
  that still selects `1` parks every delivery.
- **Drop any per-run-queue enqueue capability.** A capability the platform
  records for this World's enqueue (that a run's orchestrator messages wait in
  a graphile queue per run) no longer describes it: 2.0 writes no queue name,
  and the dispatcher's in-memory run gate serializes those deliveries.
  Nothing should rely on `wfrun:` queues existing, and queries that read them
  (to count a tenant's backlog, say) should read the jobs' `runId` instead.
- **Read the run gate if you report dispatcher state.**
  `service.runtime.runGate.snapshot()` gives `{ held, pendingWakes }`;
  `fairness.snapshot()` and `liveFeedGrants.snapshot()` keep their shape.

## The cutover

1. **Build every deployment with 2.0**, without serving it yet.
2. **Stop the 1.x dispatcher.** Deliveries pause; jobs keep queueing.
3. **Switch traffic to the 2.0 builds** and retire the 1.x ones once their
   pinned runs are done (see above).
4. **Start the 2.0 dispatcher.** Its boot recovery releases the 1.x
   dispatcher's locks and handles the jobs 1.x left.

To see what is still in flight before you start:

```sql
select tenant_id, deployment_id, status, count(*)
  from workflow.workflow_runs
 where status in ('pending', 'running')
 group by 1, 2, 3;
```

## What happens to jobs 1.x left behind

- **Flow jobs in a `wfrun:` queue**, written by a 1.x World or dispatcher, are
  claimed and delivered by the 2.0 dispatcher like any other flow job, through
  the run's gate. Boot recovery counts them as their run's job (by the queue's
  name, since their payload carries no run id) and releases a dead 1.x
  dispatcher's lock on them and on their queue.
- **Queue rows** (`_private_job_queues`): nothing creates them any more. Each
  dispatcher boot runs graphile's `GC_JOB_QUEUES` cleanup once, which deletes
  every row that holds no job; a row whose job is still queued goes at a later
  boot. To see what is left:

  ```sql
  select count(*) from graphile_worker._private_job_queues where queue_name like 'wfrun:%';
  ```

- **Held timers** (`eveland_wf_timers` jobs, a 1.x dispatcher's delayed
  deliveries) are not migrated, and nothing claims them. Boot recovery treats a
  run whose only job is such a timer as having none, and replays it, which
  re-arms its sleep from the wait's own deadline. The timer rows themselves wake
  nothing; to retire them through graphile's public API:

  ```sql
  select graphile_worker.permanently_fail_jobs(
    array(
      select jobs.id
        from graphile_worker._private_jobs as jobs
        join graphile_worker._private_tasks as tasks on tasks.id = jobs.task_id
       where tasks.identifier = 'eveland_wf_timers'
    ),
    'retired by the 2.0 upgrade'
  );
  ```

  graphile's `DELETE_PERMAFAILED_JOBS` cleanup then deletes them.

## Configuration changes

These are no longer read. Remove them:

- `WORKFLOW_DISPATCHER_QUEUE_GC_INTERVAL_MS`: there are no per-run queue rows to
  collect; boot recovery collects the ones 1.x left.
- `WORKFLOW_DISPATCHER_TIMER_SWEEP_INTERVAL_MS`: a delayed delivery is an
  ordinary flow job with a later `run_at`; there is no timer hop.
- `WORKFLOW_WORLD_STREAM_COMPACTION`: write-side snapshot stripping is gone.
- `WORKFLOW_DISPATCHER_MAINTENANCE_MAX_STREAMS_TO_PACK`: storage maintenance no
  longer repacks terminal stream blocks.

## Code changes

Only code that imports from the package needs these:

- `DISPATCH_VERSION` is `2`, and `MIN_DISPATCH_VERSION` (`2`) is new;
  `checkDispatchVersion` refuses a version below it.
- `runQueueName`, `runQueueFor` and `TIMER_JOB_NAME` are removed, and so are
  `moveDelayedJobsToTimers`, `TIMER_HORIZON_SECONDS` and
  `DEFAULT_TIMER_SWEEP_INTERVAL_MS` from `/dispatcher`.
- `DispatcherConfig` no longer takes `queueGcIntervalMs` or
  `timerSweepIntervalMs`. `DispatcherConfiguration` no longer has those, nor
  `maintenanceMaxStreamsToPack` or `maintenanceCompactSnapshots`.
- `packTerminalStreamBlocks`, `PackTerminalStreamBlocksOptions`,
  `PackTerminalStreamBlocksResult` and `resolveStreamCompaction` are no longer
  exported.
- The World config no longer takes `compactStreamSnapshots`.
- The storage maintenance result no longer has a `blocks` entry. The
  `compact_after` column and its trigger stay in the schema, unread; there is no
  migration.
- `DispatcherRuntime.runGate` is new: `runGate.snapshot()` reports the runs
  whose delivery is in flight and how many have a wake pending.
  `fairness.snapshot()` is unchanged.
- A flow job's payload carries the run's id as `runId`.

The [changelog](../../CHANGELOG.md) lists every breaking change.
