# Following Eve

An eve release is almost never a reason to do anything here. What matters is not
that eve shipped, but whether the `@workflow/*` set it installs moved. The pins
here follow eve 0.67.0: world beta.38, world-local beta.47, errors beta.23 and
core beta.56, the set 0.66.3 moved to. Eveland's supported window is
{0.62.x, 0.66.x}, verified at 0.62.0 and 0.66.1, and both of those sit on
earlier sets: world beta.36, world-local beta.45 and core beta.53 on 0.62
(unchanged since 0.61.0), and world beta.37, world-local beta.46 and core
beta.55 from 0.64.0 through 0.66.2, which workflow-world 0.22.0 pins and stays
the match for Eveland. The 0.67 pin exists for a consumer that runs eve 0.67
in External mode outside Eveland, and the e2e matrix covers each set once.
`e2e-tests/eve-versions.mts` is the table of record. Exact patches still matter:
Workflow pins have moved within an eve minor line before (0.66.3 is the latest
example), so a minor is not a set.

World beta.38 also moved `SPEC_VERSION_CURRENT` to 8, the hook force-claim
reader contract. This World keeps declaring and stamping 7: a runtime refuses a
World above its ceiling, so 8 would shut out eve 0.62 through 0.66.2 -- the
whole window Eveland deploys -- for a contract this World never exercises (it
never takes a hook token over). The note on `specVersion` in `src/index.ts` says
why 7 holds by construction and when the cap comes off; the runtime's accepted
range still starts at slot identity (6) and now ends at 8, so 7 is inside it on
every line in the matrix. The conformance harness is the one thing that minds:
`@workflow/world-testing` beta.56 asserts a World stamps what its own runtime
mints (8), so the harness stays on beta.55 -- which reads 7 -- until the cap
comes off. The e2e matrix, which runs real eve builds, is what covers core
beta.56.

The other axis an eve release can move is the message stream, which the World
touches through write-side snapshot stripping. eve 0.50.0 took it to v25, where
appends carry a delta and no cumulative snapshot -- the same shape a compacted
v24 row already had. Read-side rehydration existed only for v24 runtimes, which
served persisted appends verbatim and expected the snapshot to be there; it was
removed once 0.49.x left the supported window, because a v25 runtime normalized
the rebuilt snapshot away again before the wire and the rebuild cost O(n²) bytes
per read.

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
upstream recommends: v7 by default, or v6 when `WORKFLOW_SEALED_LOG=0` opts a
deployment out. Declaring v7 changes nothing in storage. Its reader contract is
the `noop` filler a backend emits when it pre-assigns event positions, and this
World allocates each position inside the INSERT that occupies it, so it never
has a hole to seal and never writes one. What the declaration buys is not being
left behind: upstream intends v5 stable to ship on v7, and a runtime that
raises its floor to v7 rejects a World still declaring v6 at startup.

Through 0.17.0 this World pinned v6 so a Release built against a v6-only eve
(0.47.x and older) could never meet a run it cannot read. Every line in the
window has read v7 since eve 0.49, and an in-flight run is pinned to the
deployment that created it, so a v7 run stamped by a new Release is never
replayed by an older one. The env var is the fallback if a v6-only executor
ever has to be served again; it needs no release.

See the [test commands](./testing.md) and the
[upstream compatibility contract](../world-postgres-beta34-compatibility.md).
