/**
 * Exact releases this World is verified against. The `@workflow/*` set an eve
 * release installs, not npm's `latest`, is what decides whether an entry earns
 * its install.
 *
 * Eveland's supported window is {0.81.x, 0.83.x} -- verified at 0.81.1 and
 * 0.83.3 -- and the two lines bundle two patch releases of the stable 5.2
 * family. The pins here follow the newer one, which eve 0.83.0 moved to and
 * 0.84.0 still bundles; eve compares only a World's major line, so the one
 * World serves both:
 *
 * | package                 | 0.75.1-0.82.0 | 0.83.0-0.84.0 |
 * | ----------------------- | ------------- | ------------- |
 * | `@workflow/world`       | 5.0.3         | 5.0.5         |
 * | `@workflow/world-local` | 5.1.0         | 5.1.2         |
 * | `@workflow/core`        | 5.2.0         | 5.2.3         |
 * | `@workflow/errors`      | 5.0.3         | 5.0.5         |
 * | `@workflow/utils`       | 5.0.1         | 5.0.2         |
 *
 * 5.0.5 only adds optional World members (`regionForRunId`, an
 * `inBandFence` capability with in-band event writes, queue route and handler
 * registration, delivery-count metadata, an `eventCursor` on event writes), and
 * `events.list` may return a `snapshot` beside its page. Core 5.2.3 calls only
 * `regionForRunId` of those, guarded; without an `eventCursor` it reloads
 * history in full where it would otherwise skip ahead. Spec versions did not
 * move: the set mints 8 and accepts 6 through 8, and this World declares 8
 * (`src/index.ts`).
 *
 * World beta.37 added an opt-in request/response path (`capabilities.invoke`,
 * `World.invoke()`, optional `invoke`/`requestId`/`input` on queue messages)
 * that this World does not declare, so every delivery stays an ordinary wake.
 * Event-log reads take `EventsResolveData`, which adds `'skip-step-inputs'`
 * for replay; a World MUST read anything but `'none'` as `'all'`, which this
 * World always has (`src/storage.events.test.ts` pins it).
 *
 * The whole window writes delta-only appends on message stream v26 and shares
 * one execution model: every turn runs as steps inside the session's own
 * `workflowEntry` run (the model 0.57 introduced), with a session inbox rebuilt
 * on hook tokens (`<sessionId>:anchor`, `<runId>:handoff`), deployment handoff
 * and renewable stream leases.
 *
 * - 0.81.1 is the window's floor, which existing Eveland Releases run, on the
 *   older Workflow set.
 * - 0.83.3 is Eveland's verified release on the newest line, on the newer set.
 *   Both move a session -- after a compaction, or onto a newer deployment --
 *   only when the World declares `capabilities.hookForceClaim` and
 *   `capabilities.hookRetention`, by force-claiming the session's hook tokens
 *   from the successor run (`session-handoff.test.mts`, `handsOffByForceClaim`),
 *   and both read `capabilities.deploymentAffinity`. The two entries sample the
 *   two Workflow sets under the same handoff protocol.
 *
 * Exact patches matter because Workflow pins move within a minor line: 0.51.1
 * did it, 0.54.4 did it again, and so did 0.66.3, 0.67.2, 0.70.2 and 0.75.1.
 * Each enabled entry costs an npm install plus a full `eve build`. Local runs
 * cover all enabled entries; CI supplies EVE_VERSION so each matrix job covers
 * only the release named in its label.
 */
export type EveVersion = {
  version: string;
  /** Set false to keep the entry documented without paying for it on every run. */
  enabled: boolean;
};

export const EVE_VERSIONS: readonly EveVersion[] = [
  { version: "0.81.1", enabled: true },
  { version: "0.83.3", enabled: true },
];

const eveVersionUnderTest = process.env.EVE_VERSION;

export const ENABLED_EVE_VERSIONS = EVE_VERSIONS.filter(
  (entry) => entry.enabled && (!eveVersionUnderTest || entry.version === eveVersionUnderTest),
);

if (eveVersionUnderTest && ENABLED_EVE_VERSIONS.length === 0) {
  throw new Error(`EVE_VERSION "${eveVersionUnderTest}" is not enabled`);
}

/**
 * Whether this eve moves a session to a fresh run by force-claiming its hook
 * tokens (`createHook({ experimental_force: true })`), which is what
 * `capabilities.hookForceClaim` exists for. eve 0.81.1 (54d63fd) introduced it;
 * earlier releases hand a session over by releasing and re-claiming instead,
 * and never ask the World to take a token over.
 */
export function handsOffByForceClaim(eveVersion: string): boolean {
  const match = /^(\d+)\.(\d+)\.(\d+)$/.exec(eveVersion);
  if (!match) throw new Error(`unrecognized eve version ${eveVersion}`);
  const [major, minor, patch] = match.slice(1).map(Number) as [number, number, number];
  if (major !== 0) return true;
  return minor > 81 || (minor === 81 && patch >= 1);
}
