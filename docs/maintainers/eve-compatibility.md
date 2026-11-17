# Following Eve

An eve release is almost never a reason to do anything here. What matters is
whether the `@workflow/*` set it bundles moved. The pins here follow the set eve
1.0.0 bundles: core 5.5.0, world 5.2.0, world-local 5.3.0, errors 5.1.1 and
utils 5.1.0. eve bundles that set under `dist/src/compiled/@workflow` rather
than installing it, and records each version in
`dist/src/compiled/.vendor-stamp.json`, which is what
`src/eve-pin-contract.test.ts` and `scripts/eve-pin-drift.mjs` read.
`e2e-tests/eve-versions.mts` is the table of record. Exact patches matter:
Workflow pins can move within an eve line, so a line is not a set.

## What this World declares

eve checks the World's `specVersion` against the range its runtime reads and,
before that, the major and prerelease tag of the `@workflow/world` line the
package declares. This World declares spec 9 (single orchestrator), the version
eve 1.0 asks for with `WORKFLOW_SPEC_VERSION=9`, and refuses to start when a
deployment asks for any other. Spec 9 includes the reader contracts below it:
the sealed log (7), which this World never has to seal because it allocates each
event position inside the INSERT that occupies it, and the hook force-claim
disposal (8).

Its capabilities, each of which fails closed when a World leaves it unset:

- `inBandFence`, which spec 9 runs require: a per-run in-band position count, a
  `snapshot` on `events.list`, and refusal of a stale in-band write with
  `InBandSupersededError` (`docs/design.md`, In-band writer fence).
  `src/storage.in-band-fence.test.ts` runs upstream's own conformance module.
- `maxConcurrency`: a run's orchestrator messages wait in its run queue and are
  delivered one at a time, while its step messages keep full parallelism
  (`runQueueFor` in `src/dispatch-contract.ts`).
- `atomicStartHook`: `start({ experimental_hook })` admits the run together with
  a Hook on the given token, so a duplicate start is refused with
  `HookConflictError` naming the owner. eve 1.0 starts every session this way
  and fails to start one on a World that does not declare it
  (`docs/design.md`, Atomic start Hook). `src/storage.atomic-start-hook.test.ts`
  runs upstream's own conformance module.
- `hookForceClaim` with `hookRetention.active`: eve moves a session to a fresh
  workflow run (after a compaction, or onto a newer deployment) only when the
  World declares both, because the successor takes the session's hook tokens
  over with `createHook({ experimental_force: true })`. Without them a session
  never leaves its first run, and its event log grows until
  `WORKFLOW_MAX_EVENTS` (25,000) fails it with `MAX_EVENTS_EXCEEDED`.
  `e2e-tests/session-handoff.test.mts` proves the handoff on the real eve.
- `deploymentAffinity`: eve reads it as "this World routes each run to its own
  deployment", which Deployments on different eve versions side by side need.

It implements the optional `events.subscribe` live feed, which core's spec 9
delivery opens while it runs inline steps and while it lingers for another
writer's event. core lingers only while the feed is live, so the World opens
one only in a delivery the dispatcher granted one (`x-eveland-live-feed`) and
refuses the rest through `onError`, which leaves those deliveries polling and
returning as soon as their work is done (`docs/design.md`, The World surface).
`src/run-events.integration.test.ts` runs core's own `LiveLogFeed` inside the
real `createQueueHandler`, and `e2e-tests/agent-turn.test.mts` checks that a
granted delivery of a real eve lingers.

And what it leaves unset:

- `hookResumeDedup` and `resolvedHookResume`, deduplicated and cached hook
  resumes. They need a `(runId, resumeId)` constraint this World does not have
  (`docs/design.md`). Without them core keeps the plain single-writer
  `hook_received` write, and eve resolves a signed inbox handle
  (`EVE_INBOX_HANDLE_SECRET`) by token instead of using it directly. No event
  carries a `resumeId` here either, so the live feed delivers a resume like any
  other event rather than ending at it as upstream's does.
- `dynamicWorkflowCode` and `invoke`, which eve does not use.

The queue declares `queueAuthMode: 'skip'`, as upstream's world-postgres does:
platform dispatch carries the runtime secret instead (`createQueueHandler` in
`src/queue.ts`), and the internal Local World only reads a queue auth key, never
writes one.

Event-log reads take `EventsResolveData`, which adds `'skip-step-inputs'`: the
runtime replays with it, and a World may leave step inputs out of `step_created`
and `step_started` under it but must otherwise read it as `'all'`. This World
reads it as `'all'`; `src/storage.events.test.ts` pins that.

eve 1.0 starts sessions with `$retention: 0` unless its tracing records inputs
and outputs. Zero retention is purged per run at that run's terminal transition, and stream
rows by the run id their writer passed. eve writes a session's stream under the
session's first run, which stays running for the whole session, so a handoff
successor that finishes purges its own data and leaves the session stream
alone. eve reads `expiredAt` from `runs.get(id, { resolveData: 'none' })` to
tell that a session's history is gone; `src/run-data-purge.integration.test.ts`
pins both.

The other surface an eve release can move is the message stream, which the
World touches through write-side snapshot stripping. eve's appends carry a
delta and no cumulative snapshot, so the stripping is a no-op guard.

## The conformance harness

`@workflow/world-testing` ships its own runtime bundle and tracks its own
release line; it is exempt from the pin contract (see
`src/eve-pin-contract.test.ts`). It asserts every run is stamped at least what
its own runtime mints, and `conformance/spec.test.mts` asserts that this World's
declaration lies in the range the harness's own `@workflow/world` accepts. The
e2e matrix, which runs real eve builds, is what covers the pinned core.

## Bumping

Two versions with very different cadences are easy to conflate:

- **`eve` is a devDependency.** It exists so `src/eve-pin-contract.test.ts` has
  something to read the expected `@workflow/*` versions out of. Consumers never
  resolve it, so bumping it alone is not a reason to release — it can ride along
  with the next real change.
- **The `@workflow/*` runtime packages are real dependencies.** When those move,
  what consumers resolve moves with them, so the change belongs in the next
  release.

The drift check runs weekly and files an issue only when a newer eve actually
moves the set:

```bash
pnpm run check:eve-drift
```

When it fires, follow the eve version Eveland verifies rather than npm's
`latest`. Bump the devDependency and let the contract test name what has to
move with it. Then run the conformance suite and the real eve builds before
believing a bump is inert: a release can start relying on a capability of an
unchanged set, and the `specVersion` range the runtime accepts is compiled into
eve per release.

See the [test commands](./testing.md) and the
[upstream compatibility contract](../world-postgres-compatibility.md).
