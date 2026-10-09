# Real-eve-agent end-to-end

Builds an actual eve agent with `eve build`, points it at this package as its
World, starts it, and drives one real agent turn through eve's own session API —
then asserts what that turn wrote into the World.

## How this differs from `conformance/`

They are two halves and neither replaces the other.

`conformance/` runs upstream's `@workflow/world-testing` suite. That harness ships
its **own** bundled runtime (a ~5 MB esbuild bundle of the workflow runtime), so it
proves this World satisfies the spec — but it says nothing about any particular
eve release, because it never loads one.

This suite is the other direction: it proves a **released eve** can resolve, bundle
and drive this World. It installs `eve@<version>` for real, runs `eve build`, and
boots the built server.

## Why it drives an agent turn rather than a hand-written workflow

Because that is how a real agent uses a World.

`eve info` reports exactly one workflow — `workflow//eve//workflowEntry`, compiled
from eve's own execution module. User-authored `"use workflow"` functions are not
part of eve's compile surface: `workflows/` is not a recognised agent directory
(eve warns `discover/unsupported-directory`), and the compiled agent manifest has
no workflow registry. A hand-written `"use workflow"` function in an agent is just
an async function — it returns the right answer and persists nothing, which is a
convincing-looking way to test nothing at all.

eve's agent turn, by contrast, _is_ a durable workflow. One turn produces three
runs (`workflowEntry`, `turnWorkflow`, and a `sessionTimeoutWorkflow` that gets
cancelled), plus steps, hooks and a wait — so it exercises storage, the step
lifecycle, the hook lifecycle, the wait table and the queue in one go.

## Why no model credentials

The turn's model call fails with an AI Gateway auth error, and that is fine: by
then eve has already written its runs, steps, hooks and waits through the World.
Requiring credentials would make the suite unrunnable in CI without adding a single
assertion about _this_ package.

## Running it

```bash
WORKFLOW_WORLD_E2E_URL=postgres://user:pass@127.0.0.1:5432/postgres pnpm run test:e2e

# Run one enabled version, as each CI matrix job does.
EVE_VERSION=0.75.1 WORKFLOW_WORLD_E2E_URL=postgres://user:pass@127.0.0.1:5432/postgres pnpm run test:e2e
```

The URL is used both to create the per-version database and, rewritten, to connect
to it. Skips cleanly when unset. With no `EVE_VERSION`, a local run covers every
enabled entry; CI passes the matrix value so each named job builds exactly one
release. An unknown or disabled value fails instead of silently running nothing.

Each enabled eve version costs an `npm install` plus a full `eve build`, so
`eve-versions.mts` enables them deliberately rather than all at once. Eveland's
supported window comes from its `packages/core/src/eve-compatibility.ts`; it is
currently {0.74.x, 0.75.x}, and this package pins its newest line's verified
release, eve 0.75.1. A minor does not reliably identify a `@workflow/*` set, so
the entries name exact patches. The window carries two sets: world and
world-local 5.0.2 with core 5.1.0 from 0.73.0 through 0.75.0, and the stable
5.2 family (world 5.0.3, world-local 5.1.0, core 5.2.0) from 0.75.1 on. The
execution model is the same on both -- turns run inside the session's own run
-- so the enabled entries cover each set once (0.74.0 and 0.75.1);
`eve-versions.mts` records why each one earns its install. This is especially
worth proving for `@workflow/world-local`, because this package wraps its
`createQueueHandler`.

`.work/` holds the per-version scratch builds and is gitignored.
