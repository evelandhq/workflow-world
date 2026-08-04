# Known gaps

Open problems in this package, with the evidence for each. Nothing here is
speculative — every item was observed, and the ones that were _expected_ but did
not survive measurement say so.

Every gap identified so far is closed. The entries below are kept as the record of
what was wrong, how it was measured, and which choices were deliberate — several
of them document a decision (bounded rather than durable dedup; refuse rather than
clamp) that would otherwise read as an oversight.

---

## G1 — RESOLVED: `external` mode serializes deliveries per run

Fixed by giving every run its own graphile queue. graphile executes jobs sharing a
`queueName` strictly one at a time, which is the guarantee `external` mode
otherwise lost: the embedded task handler's `inflightWorkflowRuns` map is
unreachable when no in-process runner is registered, and a process-local map could
not coordinate N dispatchers anyway.

`runQueueName(tenantId, runId)` has one definition, in `src/dispatch-contract.ts`,
because all three enqueue paths must derive it identically or the serialization is
silently partial: the World's own send, the dispatcher's reschedule, and boot
recovery. Those are the only three `addJob` call sites in the package.

### Measured, not assumed

`src/dispatcher/run-serialization.integration.test.ts` asserts the mechanism
against a real database rather than trusting the documentation:

- six jobs sharing a run queue at concurrency 8 → peak observed overlap **1**;
- six jobs in _distinct_ run queues → peak overlap **> 1**, so the fix did not
  turn the dispatcher into a single-threaded queue;
- a drained run queue leaves its `_private_job_queues` row behind, and
  `GC_JOB_QUEUES` removes it.

End to end, `conformance/serialization.test.mts` fires twelve duplicate deliveries
for one live run through the **production** boot sweep (no hand-built
`MessageData`) and asserts no step body runs twice. Before the fix it overshot by
three body executions in 2 of 3 runs; after, it is clean in 5 of 5. Flow
invocations for that case fell from 34–63 to 16–18, because serializing the
deliveries removed the redundant replays outright.

### Two things worth keeping in mind

**The naive detector does not work.** Overshoot alone cannot distinguish duplicate
delivery from ordinary replay: eve can re-execute an uncommitted step body during
a legitimate replay, so a _clean_ run occasionally overshot too. That is why the
test keeps a control alongside the gate — the control is what makes the gate mean
anything, and an earlier investigation's quantitative claim about R1 rested on a
control that had never been run.

**Never assert this through the event log.** The correlated-event unique index
absorbs the losing insert, so 8 concurrent duplicate deliveries against a
300-step run produced a perfect event histogram and `duplicateCorrelatedEvents:
0` while bodies were running twice. Side effects are the only honest signal.

### The queue rows do need sweeping

graphile does **not** reclaim a queue row when its last job completes — measured.
One row per run would accumulate for ever, so `startDispatcher` runs
`GC_JOB_QUEUES` on an interval (`WORKFLOW_DISPATCHER_QUEUE_GC_INTERVAL_MS`,
default 5 minutes). Cheap and idempotent; it only deletes queues with no jobs
left.

### Still open: message-level redelivery

Ordering is now guaranteed; _deduplication_ of an already-completed message is
not. The embedded handler's `completedMessages` LRU and `inflightMessages` map are
dead in external mode for the same structural reason, and nothing replaces them.
The consequence is bounded — a redelivered message replays the run rather than
corrupting it, and replay is the runtime's normal mode — but it is not the same
guarantee embedded mode gives. Closing it needs a durable per-message completion
record, which is a schema change and belongs in its own change.

## G2 — RESOLVED: the server-supplied event limit is implemented

`@workflow/world-testing`'s `eventLimit` suite now passes (both `turbo=1` and
`turbo=0`), and it is called explicitly from `conformance/spec.test.mts` because
`createTestSuite` does not include it.

`EventResult.maxEvents` is documented in `@workflow/world` as "server-owned max
event count for the run (run-lifecycle responses); the runtime enforces it". The
World reports it; the runtime compares the loaded event count against it and
fails the run with `MAX_EVENTS_EXCEEDED`. A World that omits the field leaves the
runtime with no ceiling at all, so a runaway workflow grows its log without
bound.

`src/storage.ts` now attaches it on all four `events.create` responses that carry
a run, mirroring `@workflow/world-local`'s three sites — same
`WORKFLOW_MAX_EVENTS` override and same 25,000 default, so the two agree.

Note that upstream's `world-postgres` does **not** implement this, so it is a
strict addition over the reference World rather than a port of it.

## G3 — PARTLY RESOLVED: renewal failures are tolerated; coverage still thin

`src/dispatcher/lease.ts` used to abort the in-flight dispatch on the **first**
failed renewal. The TTL is several renewal intervals wide by construction —
`resolveDispatcherConfig` defaults the interval to a third of the TTL and refuses
a configuration where it is not well below — so one 503 from the control API left
plenty of headroom, and aborting turned a blip into a burned graphile attempt.
Three of those dead-letter the run.

Renewal failures are now absorbed while the lease still has headroom, and the
dispatch is aborted only once sustained failure means it is about to lapse. A
success resets the tolerance, so alternating pass/fail cannot keep a dying lease
alive indefinitely. With no `leaseTtlMs` supplied the first failure still aborts,
preserving the old, safe default. Four tests in `lease.test.ts` cover those cases.

**Still thin:** at default settings no renewal fires at all during a conformance
run (`renew: 0` every time — the longest dispatch is far shorter than the 60s
interval), so the CI matrix runs the suite twice more with a 100ms interval and
with renewal forced to fail. That exercises the path but not a long step; a real
multi-minute step under renewal pressure is still untested.

## G4 — resolved during the move, recorded so the reasoning is not lost

- **`concurrency` above the pool size.** Upstream's pairing was `poolSize: 10`
  with `concurrency: 50`, which made graphile warn on every boot and put the
  shared database's connection count out of the operator's hands. `concurrency`
  now defaults to `poolSize - 1` and a config that inverts them throws. See the
  note in `src/dispatcher/config.ts`.
- **Double-prefixed queue name in boot recovery.** `boot-recovery.ts` stored an
  already-prefixed value in `MessageData.id` while the delivery side prefixes it
  again, producing `__wkf_workflow___wkf_workflow_<name>`. eve answers 400 to
  that, and a 400 is non-retryable, so every recovered run dead-lettered. It now
  stores the bare sub-queue id, matching the World's own enqueue path.
- **`SIGTERM` killed the process mid-shutdown.** graphile installs its own signal
  handlers that end by re-raising SIGTERM, so `stop()` never settled and neither
  the pool nor the telemetry sink was drained. The runner now passes
  `noHandleSignals: true` and `src/dispatcher/main.ts` owns the signals, with a
  30s grace window and a distinct exit code if it is exceeded.
- **The published tarball was mislicensed.** It declared `Apache-2.0` while
  shipping the AGPL text hoisted from the monorepo root, and carried no attribution
  for code derived from `@workflow/world-postgres`. Fixed by this repo existing:
  `LICENSE` is Apache-2.0 and `NOTICE` carries the §4(b) statement of changes.
- **Two names for one database.** An intermediate draft had the dispatcher on a
  `WORKFLOW_*` namespace while the deployment side stayed on `EVELAND_*` — for the
  database URL and for the dispatch secret. Either mismatch is silent: the
  dispatcher starts clean and then polls a database nothing writes to, or 401s
  every dispatch. Both ends now read one ordered list, and
  `src/env-contract.test.ts` asserts that every name is honoured by both.

---

## G5 — RESOLVED: hook token retention is implemented

`tokenRetentionUntil` on `hook_created` now persists in a `token_retention_until`
column (migration `0003_hook_token_retention.sql`, nullable, `IF NOT EXISTS`), and
the three run-termination deletes carry a `hookRetentionEnded` predicate so a
retained hook outlives its run instead of being collected with it. `getByToken`
resolves a retained token after the run has finished, which is the point of the
feature. `capabilities: { hookRetention: { active: true } }` is declared, because a
World that stays silent is treated as not supporting it.

A request beyond `WORKFLOW_POSTGRES_HOOK_RETENTION_LIMIT_DAYS` (default 30,
matching upstream) is **refused rather than clamped**: a caller that asked for 90
days and was quietly given 30 would believe its token was reserved for three
months. A non-positive limit throws at construction.

`hook_disposed` still deletes unconditionally — that is an explicit dispose, not
end-of-run collection, and retention is not meant to override it.

## G6 — resolved during the source-bug sweep

Each was found by porting upstream's suite, and each is covered by a test that
was verified to fail without the fix:

- **`hook_received` on a finished run.** No branch in the terminal-run guard, so
  it fell through to the generic INSERT. Both paths now take the run row with
  `FOR UPDATE`; the legacy path needed it most, because legacy runs are routed
  before the terminal validation block is reached and so were unguarded entirely.
- **Duplicate or late `run_created`.** `onConflictDoNothing()` returned no row and
  the code fell through, leaving `result.run` undefined where eve's `start()`
  asserts it and appending a second `run_created` to the log. Now throws
  `EntityConflictError`, which `start()` already treats as benign.
- **eve's queue namespace ignored.** `WORKFLOW_QUEUE_NAMESPACE` is live in the
  installed runtime; a deployment that set it had eve registering
  `__<ns>_wkf_workflow_*` while this world addressed `__wkf_workflow_*`, and every
  message dead-lettered on a non-retryable 400. The namespace now travels on the
  message, because the dispatcher runs in a different process and its own
  environment holds the host's value, not the tenant's.
- **`runs.getMany` unimplemented.** Optional in the interface, but omitting it
  meant callers fell back to one query per id on eve's batched replay path. Now
  implemented, tenant-scoped, with a test that it cannot reach across tenants.
- **`close()` was not idempotent.** node-postgres is strict — a second
  `pool.end()` raises "Called end on pool more than once" and the streamer's
  LISTEN client raises "Client was closed and is not queryable" — so a shutdown
  path and an error path both calling `close()` turned a correct teardown into a
  thrown error. Both layers now guard. Pool _ownership_ was already right and is
  now asserted: a caller-supplied pool survives, a world-created one is ended.
- **`parseInt(env ?? "") ?? 50` yielded NaN**, the fallback being unreachable
  because NaN is not nullish. Masked downstream by a `Number.isFinite` guard, so
  shipped behaviour was correct while `resolveConfig` alone was not.
