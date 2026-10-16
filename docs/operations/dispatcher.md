# Dispatcher operations

The external runner requires an always-running dispatcher and a host activation
API. Start with the [integration guide](../guides/getting-started.md), then use
the [configuration reference](../reference/configuration.md) for environment
variables and the [dispatch contract](../design.md#the-dispatch-contract) for
the host API.

## Sizing the dispatcher pool

The pool and the concurrency are independent knobs. Graphile checks a connection
out of the pool for `getJob` and for `completeJob`, and returns it in between:
`makeWithPgClientFromPool` acquires around a callback and releases in its
`finally`, and the task handler is invoked outside that callback. A dispatch held
open for minutes waiting on an executor therefore occupies **no** connection —
only a socket and an in-flight lease renewal.

Two connections in the pool are held for the process lifetime: the lifecycle
advisory lock (session-scoped, so its client cannot be returned) and Graphile's
LISTEN. Everything above that is transient, so size the pool against how fast
jobs are claimed and completed, not against how many are running.

Measured on graphile-worker 0.16.6 at `concurrency=50`, median wall-clock:

| pool | 50 held dispatches (2s each) | 500 fast dispatches (50ms each) | 1000 instant jobs |
| ---- | ---------------------------- | ------------------------------- | ----------------- |
| 3    | 2078ms                       | 724ms                           | 1282ms            |
| 4    | 2070ms                       | 643ms                           | 857ms             |
| 6    | 2065ms                       | 597ms                           | 626ms             |
| 10   | 2043ms                       | 599ms                           | 381ms             |
| 16   | 2057ms                       | 655ms                           | 290ms             |
| 52   | 2076ms                       | 659ms                           | 211ms             |

Held dispatches — the real workload — are flat: pool 3 and pool 52 finish in the
same time against a 2000ms floor. Only the rightmost column, where handlers do
nothing at all and the job loop is pure SQL, rewards a large pool, and no real
dispatch behaves that way. The default of 10 sits at the knee for realistic
dispatch rates; the useful range is 6–16 regardless of concurrency.

So to run more dispatches at once, raise `WORKFLOW_DISPATCHER_CONCURRENCY` and
leave the pool alone. Earlier versions rejected a concurrency above
`poolSize - 2`, on the belief that a running job holds a connection; that bound
is gone, and only a floor of 4 on the pool itself remains.

## Delayed deliveries and claim cost

graphile-worker 0.16 claims a job by first locking every available named queue
(`for update skip locked` over `_private_job_queues`) and only then picking a
due job; 0.17 and 0.18 still do this by default. Each lock is a WAL record and
each claim a commit, so a claim costs one row lock **per queue that holds any
job, due or not**. Per-run serialization gives every run its own
`wfrun:<tenant>:<run>` queue, and a sleeping run's delayed continuation used to
keep that queue alive for the whole sleep — eve's session timeout re-arms every
23 hours for as long as the session lives. A few hundred live sessions meant a
few hundred row locks on every claim, and with a high
`WORKFLOW_DISPATCHER_CONCURRENCY` and a short poll interval that was a steady
stream of WAL and fsyncs (evelandhq/workflow-world#115).

The dispatcher therefore holds any delivery due more than a minute out as an
`eveland_wf_timers` job with no queue. Every
`WORKFLOW_DISPATCHER_TIMER_SWEEP_INTERVAL_MS`, and once at startup, it moves such
jobs off their run queues and reclaims the queues they leave empty; its own
`{ timeoutSeconds }` reschedules skip the queue from the start. When a timer
comes due the dispatcher re-adds it to the run's queue as an ordinary flow job,
under the same job key, so the delivery itself is still serialized. Deployments
enqueue exactly what they did before, so this applies to every build already
deployed, and no deployment ever writes a job that only a newer dispatcher could
claim. Jobs that have been attempted keep their place, since moving one would
reset its retry count. An `embedded` runner does the same for its own tenant,
under its per-tenant task names: it holds far-off deliveries as timers when it
enqueues them, and moves its older ones once at startup.

**Rolling the dispatcher back below this version** leaves held timers with no
consumer: an older dispatcher does not claim `eveland_wf_timers`. Stop it and
return them to their run queues first:

```sql
begin;
insert into graphile_worker._private_job_queues (queue_name)
select distinct jobs.payload ->> 'runQueue'
  from graphile_worker._private_jobs as jobs
  join graphile_worker._private_tasks as tasks on tasks.id = jobs.task_id
 where tasks.identifier = 'eveland_wf_timers'
on conflict do nothing;
update graphile_worker._private_jobs as jobs
   set task_id = flow.id,
       job_queue_id = queues.id,
       flags = jsonb_build_object('project:' || (jobs.payload ->> 'tenantId'), true)
  from graphile_worker._private_tasks as timer,
       graphile_worker._private_tasks as flow,
       graphile_worker._private_job_queues as queues
 where timer.identifier = 'eveland_wf_timers'
   and flow.identifier = 'eveland_wf_flows'
   and jobs.task_id = timer.id
   and queues.queue_name = jobs.payload ->> 'runQueue'
   and jobs.locked_at is null;
commit;
```

An `embedded` tenant rolled back to an older build has the same gap for its
own timers, until a build of this version or later runs again.

The number of `_private_job_queues` rows now tracks runs with work due, not
runs that are alive:

```sql
select count(*) from graphile_worker._private_job_queues;
select count(*) from graphile_worker._private_jobs jobs
  join graphile_worker._private_tasks tasks on tasks.id = jobs.task_id
 where tasks.identifier = 'eveland_wf_timers';
```

## Executors that are always running

Set `WORKFLOW_DISPATCHER_STATIC_ENDPOINTS` instead of the activation API when
the executors do not scale to zero — a service running its own replicas:

```bash
WORKFLOW_DISPATCHER_STATIC_ENDPOINTS=jiri-v4=http://jiri-v4.internal:3000
```

Each entry is a deployment id and the origin its executors are reached at; put a
load balancer or a Kubernetes Service there and let it pick the replica. Things
that differ from a platform host:

- **Raise `WORKFLOW_DISPATCHER_MAX_INFLIGHT_PER_TENANT`.** Its default is a
  fairness ceiling derived from the dispatcher's cores (2–16). With one tenant
  it is the cap on the whole service; set it to the concurrency. A hook wake
  that has to reach a running step, such as a cancel, has a cap of its own but
  still needs a free dispatcher slot, so keep the concurrency above the
  deliveries you expect in flight at once.
- **Upgrade the dispatcher before the agents** when moving to a World that sends
  hook wakes past held run queues. An older dispatcher has no wake cap, so it
  never holds those wakes back, and its boot recovery does not release a dead
  worker's lock on one. The upgraded dispatcher also applies migration 0019 at
  start, which that World needs: every `step_started` it writes sets
  `workflow_steps.owner_message_id`. Without a dispatcher (embedded mode), run
  `workflow-world-setup` before the agents start.
- **Leave `WORKFLOW_OPTIMISTIC_INLINE_START` unset on the agents.** With it on,
  a step's body starts before its claim resolves, and a hook wake running beside
  another delivery can then run a step a second time (design.md, "Delivery and
  failure semantics").
- **Keep the flow route private.** `/.well-known/workflow/` receives dispatches
  carrying the runtime secret. Only the dispatcher should reach it; do not
  publish it through the ingress.
- **Drain before stopping a replica.** Fail its readiness check, wait until
  `inflightDeliveries()` (root export) is zero, then send the signal. In
  Kubernetes that is a `preStop` hook polling a route the application serves,
  with `terminationGracePeriodSeconds` at least `WORKFLOW_DISPATCHER_DISPATCH_TIMEOUT_MS`.
  A replica stopped mid-step loses nothing, but the step runs again.
- **A build that cannot replay the previous one's runs is a new deployment id.**
  List both while the old one still has runs, each at its own origin, and remove
  the old entry once they are over. Messages for a deployment that is no longer
  listed are dead-lettered.

## Readiness and boot recovery

The dispatcher binds no port. Readiness is the literal line
`workflow-dispatcher: ready` on stdout — a stable contract, matched by supervisors
and by the conformance harness.

The current dispatcher is deliberately single-instance. On startup it holds a
PostgreSQL advisory lock for its whole lifetime, reclaims the old Graphile
worker ids found on active runs' exact `wfrun:<tenant>:<run>` queues and on jobs
in no queue (hook wakes that bypassed one, held timers), re-enqueues
the active runs that have no job left on their queue, and only then starts its
worker pool and reports ready. A run whose job is still queued — a pending
delivery, a sleep's timer (on its queue or held as an `eveland_wf_timers` job),
or the delivery the dead dispatcher was holding — is
left to that job, and a run that holds a hook is left to that hook's
resolution; re-enqueueing beside either only bought a cold start per run. The
runs it does re-enqueue are released a few deployments at a time
(`WORKFLOW_DISPATCHER_BOOT_RECOVERY_DEPLOYMENTS_PER_WAVE` every
`WORKFLOW_DISPATCHER_BOOT_RECOVERY_WAVE_INTERVAL_MS`), so a restart does not ask
the host to cold-start every deployment with an active run at once. A second
dispatcher pointed at the same database fails closed instead of sharing claims.
When first upgrading from a version that did not take this ownership lock, stop
the old dispatcher before starting the new one; the new lock cannot fence a
binary that never participates in it.

## Ownership liveness

A session-level advisory lock is released when its session ends, and PostgreSQL
only learns that a session has ended when the socket closes. A dispatcher that
crashes on the same host as the database closes its socket at once. One whose
host lost power, or whose connection crosses a NAT, VPN, or port forward that
swallows the close, leaves an idle backend holding the lock until the kernel's
TCP keepalive gives up — over two hours at Linux defaults — and every restart in
that window would find the database owned by a ghost.

So the owning session is made to prove it is alive, in two independent layers
derived from one budget, `WORKFLOW_DISPATCHER_OWNERSHIP_LIVENESS_MS` (`L`):

- server-side TCP keepalives on the owning session (`tcp_keepalives_idle = L/3`,
  `tcp_keepalives_interval = L/9`, three probes), which notice a vanished peer
  within `2L/3` with no cooperation from the client;
- `idle_session_timeout = L` on the owning session (PostgreSQL 14+) plus a
  `select 1` heartbeat every `L/3`, so a session that stops heartbeating — a
  wedged process, a black-holed network — is terminated by the server itself.

The heartbeat is also the client's own view of the same fact: when it fails, or
the server ends the session (an operator's `pg_terminate_backend`, the timeout
above, a restart), the dispatcher reports `ownership_lost` through its lifecycle
callback and stops itself. It no longer owns the database and must not claim; the
host's supervisor starts a fresh process, which acquires the lock anew. A server
that refuses any of the settings (an older major, a platform without keepalive
control) logs `workflow_dispatcher.ownership_liveness_unavailable` at startup
and runs with whatever layers it accepted.

A dispatcher that finds the lock held now waits for it instead of exiting: it
retries every `WORKFLOW_DISPATCHER_OWNERSHIP_RETRY_INTERVAL_MS`, logging
`workflow_dispatcher.ownership_wait` with the holder's pid, `application_name`,
client address and connection time from `pg_locks` each time, and starts the
moment the holder is gone. That is what a restart during the liveness window
looks like — a few warnings, then `ready` — rather than a crash loop that trips
the supervisor's start limit. Set `WORKFLOW_DISPATCHER_OWNERSHIP_WAIT_MS` to
bound the wait; `0` restores the old fail-fast behaviour. The holder is also
readable programmatically through `readDispatcherOwnershipHolder`, and
`terminateDispatcherOwnershipHolder` is the escape hatch for a holder the layers
cannot reach (a pre-0.16 dispatcher that never set them).

## Dead letters and host reconciliation

An exhausted or terminal dispatch is written to `workflow.dispatch_dead_letters`,
and so is a run whose executor keeps answering `5xx` delivery after delivery (see
`WORKFLOW_DISPATCHER_EXECUTOR_FAILURE_LIMIT`). While that row is unresolved, the
still-active workflow run is quarantined: boot recovery skips it and live dispatch
drops its messages with a log line; resolving it makes the run deliverable again.
The dispatcher does not manufacture a workflow `run_failed` event or stream EOF for
a transport failure, so operators can choose between replay and an explicit workflow
cancel/fail action without losing the original message. Taking the second choice
resolves the letters by itself: a run reaching a terminal status resolves every
unresolved letter it left behind, because neither the replay nor the quarantine
those rows stand for can mean anything once the run is over.

The host has two seams over this machinery. `reconcileWorkflowRuns` (root export)
is the supported write path for runs whose executor the host knows is gone for
good: it moves selected `pending`/`running` runs to `failed`/`cancelled` with the
World's own terminal semantics, or quarantines them behind an unresolved dead
letter whose payload is replayable exactly like an organic one. And
`startDispatcherService({ filterBootRecoveryRuns })` lets the host decide, per
boot, which of the sweep's candidates are worth replaying — a run bound to a
Deployment that is not activatable can be skipped without being settled; it stays
active and is offered again on the next boot.

## Queue namespaces

`WORKFLOW_QUEUE_NAMESPACE` is eve's, read by eve's own resolver on the deployment
side only. Do not set it on the host: the dispatcher must take the namespace from
the run it is recovering, never from its own environment.

## Upgrading from 0.3.0 or earlier

Runs created before this version have no recorded queue namespace, and boot
recovery can only fall back to the default `__wkf_workflow_` prefix for them. For
a namespaced deployment that fallback is refused with `400 Unhandled queue`, so
**drain or cancel active runs before cutting over**. The dispatcher logs every
run it recovers without a recorded namespace, and a
`runsWithUnknownQueueNamespace` count alongside the boot-recovery summary, so an
incomplete drain is visible rather than silent. See
[Upgrading past 0.3.0](../design.md#upgrading-past-030).
