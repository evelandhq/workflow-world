# Following Eve

An eve release is almost never a reason to do anything here. What matters is not
that eve shipped, but whether the `@workflow/*` set it bundles moved. The pins
here follow the set eve 1.0.0 bundles: core 5.5.0, world 5.2.0, world-local
5.3.0, errors 5.1.1 and utils 5.1.0. eve bundles that set under
`dist/src/compiled/@workflow` rather than installing it, and records each
version in `dist/src/compiled/.vendor-stamp.json`, which is what
`src/eve-pin-contract.test.ts` and `scripts/eve-pin-drift.mjs` read.
`e2e-tests/eve-versions.mts` is the table of record. Exact patches matter:
Workflow pins can move within an eve line, so a line is not a set.

World 5.2.0 adds only optional members, each behind a capability or setting
that fails closed when a World leaves it unset:

- `inBandFence`, which spec 9 (single-orchestrator) runs require: a per-run
  in-band position count, a `snapshot` on `events.list`, and refusal of a stale
  in-band write with `InBandSupersededError`. Declared since 0.30.0
  (`docs/design.md`, In-band writer fence), so `WORKFLOW_SPEC_VERSION=9`, eve
  1.0's default, creates spec 9 runs; 0.29.0 created spec 8 runs and logged
  `Ignoring WORKFLOW_SPEC_VERSION=9`. `src/storage.in-band-fence.test.ts` runs
  upstream's own conformance module for it.
- `maxConcurrency`, a queue that delivers a spec 9 run's orchestrator messages
  one at a time. Declared with the fence: a spec 9 run's orchestrator messages
  wait in its run queue and none bypasses it, while its step messages keep full
  parallelism (`runQueueFor` in `src/dispatch-contract.ts`).
- `atomicStartHook`, `start({ experimental_hook })` admission: the run is
  admitted together with a Hook on the given token, so a duplicate start is
  refused with `HookConflictError` naming the owner. eve 1.0 starts every
  session this way, claiming its channel address, and fails to start one on a
  World that does not declare it. Declared since 0.31.0 (`docs/design.md`,
  Atomic start Hook); `src/storage.atomic-start-hook.test.ts` runs upstream's
  own conformance module for it.
- `hookResumeDedup` and `resolvedHookResume`, deduplicated and cached hook
  resumes. Not declared: they need a `(runId, resumeId)` constraint this World
  does not have (`docs/design.md`). Without them core keeps the plain
  single-writer `hook_received` write, and eve resolves a signed inbox handle
  (`EVE_INBOX_HANDLE_SECRET`) by token instead of using it directly.
- `queue.queueAuthMode`, how the runtime authenticates flow-route deliveries.
  Declared `'skip'`, as upstream's world-postgres does: platform dispatch
  carries the runtime secret instead (`createQueueHandler` in `src/queue.ts`),
  and the internal Local World only reads a queue auth key, never writes one.
- `dynamicWorkflowCode`, storing a dynamic run's code. eve does not use it.

Core 5.5 drives a run with one of two runtimes, picked by the run's stamped
version: `runtime/spec8` for spec 8 runs, and `runtime/spec9` for
single-orchestrator runs. Runs keep the version they were stamped with, so spec 8
runs created before 0.30.0 finish on spec 8. The hook-wake bypass applies to
spec 8 runs only, and relies on how both runtimes start step bodies, which
`src/core-concurrency-contract.test.ts` pins against the installed dist.

eve moves a session to a fresh workflow run (after a compaction, or onto a newer
deployment) only when the World declares both `capabilities.hookForceClaim` and
`capabilities.hookRetention.active`, because the successor takes the session's
hook tokens over with `createHook({ experimental_force: true })`. A World
without the capability gets `session handoff unavailable: the Workflow world
cannot force-claim hooks` and a session that never leaves its first run, whose
event log then grows until `WORKFLOW_MAX_EVENTS` (25,000) fails it with
`MAX_EVENTS_EXCEEDED`. The force-claim takeover is described in
`docs/design.md` (Hook force-claim), and `e2e-tests/session-handoff.test.mts`
proves the handoff on the real eve.

The one contract change in that stretch is on the read side. `@workflow/world`
widened event-log reads to `EventsResolveData`, which adds `'skip-step-inputs'`:
the runtime replays with it, and a World may leave step inputs out of
`step_created` and `step_started` under it but must otherwise read it as
`'all'`. Core beta.57 already sends it. This World has always tested only
`=== "none"`, so the change is type-level -- the legacy event handler maps the
value down to an entity `ResolveData` before using it -- and
`src/storage.events.test.ts` pins the `'all'` reading. The World does not
implement the input omission. The stable releases since only add optional
members a World may ignore: a stream writer's `release()`, an advisory
`afterStepBody` mark on step writes, and a hook's `nodeVersion`.

World beta.38 also moved `SPEC_VERSION_CURRENT` to 8, the hook force-claim
reader contract, and 5.0.1 keeps it there. This World declares and stamps 8
since it implements the takeover; through 0.26 it capped the declaration at 7,
because 8 would have shut out eve 0.66.2 and older, for a contract it never
exercised. The note on `specVersion` in `src/index.ts` says what each version
obliges a World to; the runtime's accepted range starts at slot identity (6)
and ends at 8 on every line in the matrix.

The conformance harness moved with it. `@workflow/world-testing` from beta.56
on asserts every run is stamped at least what its own runtime mints (8), so
while this World declared 7 the harness was held on beta.55. It now tracks the
newest stable harness, 5.1.0, which ships its own runtime bundle on core 5.5.0
/ world 5.2.0 -- the same set eve 1.0.0 bundles, by timing rather than by
constraint; see the exemption in `src/eve-pin-contract.test.ts`. Its
`resolveData` family (this World's `'skip-step-inputs'` reading) passes, and so
does its `inBandFence` family since 0.30.0. Run the suite with
`WORKFLOW_SPEC_VERSION=9` as well, which drives every case on spec 9 runs.
no force-claim cases; those live in `src/storage.hook-force-claim.test.ts`
and, end to end, in the e2e suite. The e2e matrix, which runs real eve builds,
is still what covers the pinned core.

The other axis an eve release can move is the message stream, which the World
touches through write-side snapshot stripping. eve 0.50.0 took it to v25, where
appends carry a delta and no cumulative snapshot -- the same shape a compacted
v24 row already had. eve 0.69.0 moved it to v26 without changing the append
events, so the stripping stays a no-op on every supported line. Read-side
rehydration existed only for v24 runtimes, which served persisted appends
verbatim and expected the snapshot to be there; it was removed once 0.49.x left
the supported window, because a v25 runtime normalized the rebuilt snapshot
away again before the wire and the rebuild cost O(n²) bytes per read.

Two versions with very different cadences are easy to conflate:

- **`eve` is a devDependency.** It exists so `src/eve-pin-contract.test.ts` has
  something to read the expected `@workflow/*` versions out of. Consumers never
  resolve it, so bumping it alone is not a reason to release — it can ride along
  with the next real change.
- **The `@workflow/*` runtime packages are real dependencies.** When those move,
  what consumers resolve moves with them, so the change belongs in the next
  release even if that release is intentionally deferred.

The check runs weekly and files an issue only when a newer eve actually moves the
set. Expect it to be silent for months:

```bash
pnpm run check:eve-drift
```

When it does fire, it is a heads-up rather than a deadline. Nothing can be
deployed until Eveland's `packages/core/src/eve-compatibility.ts` verifies a
version on that line, and the pin here should follow **that** version rather than
npm's `latest` — the two are routinely different. Then bump the devDependency and
let the contract test name what has to move with it. The 0.38.3 alignment opts
this World into spec v6 slot identity: new runs use dense `evnt_` positions,
while runs created before migration 0006 remain on their original `wevt_` ULID
scheme. Neither detail is visible from version numbers alone, so typecheck and
run real eve builds against both old and new releases before believing a bump is
inert.

The shape of the `specVersion` check is itself not fixed. Through eve 0.33.1 it
is literal equality against the runtime's `SPEC_VERSION_CURRENT`;
`@workflow/core` beta.41, which eve 0.33.2 is the first release to install,
widened it to the range `[SPEC_VERSION_CURRENT, SPEC_VERSION_MAX_SUPPORTED]` and
ships it as `>= 5 && <= 6`. That is a loosening and cannot newly reject anything,
but it loosens in one direction only: the floor is still the runtime's current
version, so a World pinned behind it fails just as it did before. The headroom
above was the staging space for slot identity. In beta.42 the runtime floor and
`SPEC_VERSION_CURRENT` are both 6, and slots are part of the World contract
rather than an optional capability. Compatibility with existing v5 runs is
pinned by a per-run scheme marker rather than by rewriting their event ids.

`@workflow/world` beta.32 (eve 0.49) split the two ends of that range: the
runtime floor stays at slot identity (v6) while `SPEC_VERSION_CURRENT` moves to
v7, the "sealed log", and the runtime stamps each new run with whatever the
World declares. This World declares `mintedSpecVersion()` (`src/index.ts`), as
upstream recommends: v8, which it has returned since beta.38 (this World capped
it at v7 until it implemented force-claim). Declaring v7 changed nothing in storage. Its reader contract is
the `noop` filler a backend emits when it pre-assigns event positions, and this
World allocates each position inside the INSERT that occupies it, so it never
has a hole to seal and never writes one. What the declaration buys is not being
left behind: upstream intends v5 stable to ship on v7, and a runtime that
raises its floor to v7 rejects a World still declaring v6 at startup.

Through 0.17.0 this World pinned v6 so a Release built against a v6-only eve
(0.47.x and older) could never meet a run it cannot read. Every line in the
window has read v7 since eve 0.49, and an in-flight run is pinned to the
deployment that created it, so a v7 run stamped by a new Release is never
replayed by an older one.

See the [test commands](./testing.md) and the
[upstream compatibility contract](../world-postgres-beta34-compatibility.md).
