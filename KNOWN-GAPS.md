# Known gaps

Open problems in this package, with the evidence for each. Nothing here is
speculative — every item was observed, and the ones that were _expected_ but did
not survive measurement say so.

This package is not ready to run production traffic. G1 is the blocker.

---

## G1 — `external` mode has no per-run serialization, and no message-level dedup

**Status:** open, blocking. **Evidence:** code paths, verified by reading.

In `embedded` mode the task handler holds three guards, all declared in
`src/queue.ts`:

| Guard                     | Declared           | Used       | Protects against                                      |
| ------------------------- | ------------------ | ---------- | ----------------------------------------------------- |
| `inflightWorkflowRuns`    | `queue.ts:227`     | `:627-636` | two replays mutating one run's event log concurrently |
| `completedMessages` (LRU) | `queue.ts:234-236` | `:645-665` | redelivery of a message already completed             |
| `inflightMessages`        | `queue.ts:229`     | `:645-665` | concurrent delivery of the same message               |

All three live inside `createTaskHandler`, which is registered only by
`setupListeners`, which is reachable only through
`startRunnerWhenExecutorIsReady` — and that returns immediately when
`runner === "external"`. Every path into `setupListeners` is downstream of that
same gate, so in external mode all three are dead code.

The dispatcher replaces none of them:

- `src/dispatcher/runner.ts` — `addJob` passes `jobKey`, `runAt`, `maxAttempts`
  and `flags`, but **no graphile `queueName`**, so graphile's own per-queue
  serialization is unused.
- `fairness.acquire` runs _inside_ the handler, after the claim, so it bounds
  in-flight work per tenant but does not order deliveries for one run.

Duplicate delivery for one run is ordinary, not exotic:

- The World's own flow enqueue uses `jobKey: idempotencyKey ?? messageId`
  (`queue.ts:559`) where `messageId` is a fresh ULID per send. Two sends for one
  run — two hooks resolving together, a step completion racing a hook resume —
  are two independently claimable graphile jobs.
- `reenqueueActiveRunsForAllTenants` uses `msg_recover_<runId>`
  (`src/dispatcher/boot-recovery.ts`), which collapses only against another
  sweep, never against a live World job. A dispatcher restarting while work is in
  flight adds a delivery.

**The fix cannot be upstream's `Map`.** It is per-process, and external mode has
N dispatcher processes by design. Serialization has to move into Postgres — a
run-keyed advisory lock, or a per-run graphile `queueName`. And because the
`completedMessages` guard is dead too, ordering alone is not enough: a redelivered
message would still execute twice in sequence, so the fix needs a message-level
completion check as well.

### What could not be reproduced deterministically

An earlier investigation reported a quantitative reproduction: `brokenWf` from
`@workflow/world-testing` has 20 steps whose bodies each increment a module-level
counter, and a run with 12 injected duplicate deliveries recorded 23 body
executions — overshoot `[21,22,23]` — while a clean control recorded exactly
`1..20`.

**That control does not hold here.** Three runs of the control on this
configuration (`conformance/serialization.probe.mts`):

```
run 1   no overshoot
run 2   no overshoot
run 3   values [1,2,3,4,5,9,10,…,23] for 20 steps → overshoot [21,22,23]
```

A clean external-mode run already re-executes step bodies during replay — the
counter advances for a body whose result is then discarded. So the overshoot
signal conflates ordinary replay with duplicate delivery, and it cannot be used
as a gate. The duplicate-delivery case was correspondingly flaky: overshoot in 2
of 3 runs.

G1 therefore rests on the code paths above, which are not in doubt. What is
missing is a deterministic detector, and it needs to key on a _committed_ side
effect rather than a process-local counter.

Note also that lowering `concurrency` from 50 to `poolSize - 1` = 9 (see G4)
narrowed the race window. It did not close it, and must not be mistaken for a fix.

**Reproduce:** `mv conformance/serialization.probe.mts conformance/serialization.test.mts`
and run the conformance project.

---

## G2 — the server-supplied event limit is not implemented

**Status:** open, needs a dependency bump first. **Evidence:** measured.

`@workflow/world-testing`'s `eventLimit` suite requires a runaway run to fail with
`errorCode === 'MAX_EVENTS_EXCEEDED'` under `WORKFLOW_MAX_EVENTS`. Against this
World the run completes instead. Both variants (`turbo=1`, `turbo=0`) fail.

It is version skew, not a regression:

- our `@workflow/world` pin is `5.0.0-beta.19`, whose `dist/` has **zero**
  occurrences of `stateEventCount`, `WORKFLOW_MAX_EVENTS` or
  `MAX_EVENTS_EXCEEDED`;
- the harness (`5.0.0-beta.39`) bundles a runtime that enforces the limit and
  expects the World to report `stateEventCount`;
- upstream's own `world-postgres` does not implement it either — same zero hits
  across its `src/`. That is why the aggregate `createTestSuite` does not call
  `eventLimit`.

Currently `describe.skip`'d in `conformance/spec.test.mts` with the reason inline.
Bump the `@workflow/world` pin, un-skip, then implement `stateEventCount`.

---

## G3 — the activation lease renewal path is unexercised at default settings

**Status:** open, needs CI coverage. **Evidence:** measured.

Across every conformance run the stub reports `renew: 0`: the longest single
dispatch is well under the default 60s renewal interval, so no renewal ever
fires. Lease renewal is the most package-specific logic here — it is what keeps a
long step's executor alive — and it ships untested by the default gate.

The stub already has the knobs (`STUB_RENEW_FAIL=1`, and the interval is
`WORKFLOW_DISPATCHER_LEASE_RENEW_INTERVAL_MS`). What is missing is a CI matrix
that runs the suite a second and third time with a fast interval and with forced
renewal failure.

Related: `src/dispatcher/lease.ts` aborts on the **first** failed renewal. With a
180s TTL and 60s renewals there is room for two more attempts before the lease
could actually expire, so one flaky response to the control API currently burns a
graphile attempt. It should retry inside the TTL.

---

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
