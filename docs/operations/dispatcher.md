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

## Readiness and boot recovery

The dispatcher binds no port. Readiness is the literal line
`workflow-dispatcher: ready` on stdout — a stable contract, matched by supervisors
and by the conformance harness.

The current dispatcher is deliberately single-instance. On startup it holds a
PostgreSQL advisory lock for its whole lifetime, reclaims only the old Graphile
worker ids found on active runs' exact `wfrun:<tenant>:<run>` queues, re-enqueues
the active runs that have no job left on their queue, and only then starts its
worker pool and reports ready. A run whose job is still queued — a pending
delivery, a sleep's timer, or the delivery the dead dispatcher was holding — is
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
