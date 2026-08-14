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
WORKFLOW_WORLD_E2E_URL=postgres://user:pass@127.0.0.1:5432/postgres npm run test:e2e
```

The URL is used both to create the per-version database and, rewritten, to connect
to it. Skips cleanly when unset.

Each enabled eve version costs an `npm install` plus a full `eve build`, so
`eve-versions.mts` enables them deliberately rather than all at once. The supported
window comes from Eveland's `packages/core/src/eve-compatibility.ts`, and this
package pins 0.37.1. A minor does not reliably identify a `@workflow/*` set, so
the entries name exact patches. The current window has two distinct sets:
0.34.0 carries world beta.25 and world-local beta.34, while 0.35.0 through
0.37.1 carry world beta.26 and world-local beta.35; all four carry core beta.41.
The enabled entries cover each set once (0.34.0 and 0.37.1). This is especially
worth proving for `@workflow/world-local`, because this package wraps its
`createQueueHandler`.

`.work/` holds the per-version scratch builds and is gitignored.
