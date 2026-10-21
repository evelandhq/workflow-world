/**
 * Exact releases this World is verified against. The `@workflow/*` set an eve
 * release installs, not npm's `latest`, is what decides whether an entry earns
 * its install.
 *
 * Eveland's supported window is {0.79.x, 0.81.x} -- verified at 0.79.0 and
 * 0.81.1 -- and the pins here follow the stable 5.2 family that eve 0.75.1
 * moved to and every release since, through 0.81.1, bundles. Eveland retired
 * 0.76 when 0.81 entered, as it retired 0.74 when 0.79 did; neither took a
 * `@workflow/*` set with it this time. The window carries ONE set:
 *
 * | package                 | 0.75.1-0.81.1 |
 * | ----------------------- | ------------- |
 * | `@workflow/world`       | 5.0.3         |
 * | `@workflow/world-local` | 5.1.0         |
 * | `@workflow/core`        | 5.2.0         |
 * | `@workflow/errors`      | 5.0.3         |
 * | `@workflow/utils`       | 5.0.1         |
 *
 * World beta.37 added an opt-in request/response path (`capabilities.invoke`,
 * `World.invoke()`, optional `invoke`/`requestId`/`input` on queue messages)
 * that this World does not declare, so every delivery stays an ordinary wake.
 * The one contract change since is on the read side: event-log reads take
 * `EventsResolveData`, which adds `'skip-step-inputs'` for replay. Core beta.57
 * already sent it; a World MUST read anything but `'none'` as `'all'`, which
 * this World always has (`src/storage.events.test.ts` pins it). The 5.x
 * releases since only add optional members (a stream writer's `release`, an
 * advisory `afterStepBody` mark on step writes, a hook's `nodeVersion`). Spec
 * versions did not move: the set mints 8 and accepts 6 through 8, and this
 * World declares 8 (`src/index.ts`).
 *
 * The whole window writes delta-only appends on message stream v26, which
 * kept v25's append events -- and shares one execution
 * model: every turn runs as steps inside the session's own `workflowEntry` run
 * (the model 0.57 introduced), with a session inbox rebuilt on hook tokens
 * (`<sessionId>:anchor`, `<runId>:handoff`), deployment handoff and renewable
 * stream leases. The per-turn child `turnWorkflow` run left with 0.55.
 *
 * - 0.79.0 is the window's floor, which existing Eveland Releases run. It
 *   moves a session to a fresh run with its older release-then-claim handoff
 *   and never asks the World to force-claim a hook.
 * - 0.81.1 is Eveland's verified release on the newest line. It moves a
 *   session -- after a compaction, or onto a newer deployment -- only when the
 *   World declares `capabilities.hookForceClaim` and
 *   `capabilities.hookRetention`, by force-claiming the session's hook tokens
 *   from the successor run. `session-handoff.test.mts` proves that path
 *   (`handsOffByForceClaim`). Both read `capabilities.deploymentAffinity` (from
 *   0.76.1), so the two entries sample eve's two handoff protocols on the same
 *   Workflow set.
 *
 * Every entry is past 0.67.0, which removed the `conversation` / `task` run
 * mode: every session parks after its turn, so a first turn no longer
 * completes a run or disposes a hook (`event-types.mts`).
 *
 * Exact patches matter because Workflow pins move within a minor line: 0.51.1
 * did it, 0.54.4 did it again, and so did 0.66.3, 0.67.2, 0.70.2 and 0.75.1.
 * Each
 * enabled entry costs an npm install plus a full `eve build`. Local runs cover
 * all enabled entries; CI supplies EVE_VERSION so each matrix job covers only
 * the release named in its label.
 */
export type EveVersion = {
  version: string;
  /** Set false to keep the entry documented without paying for it on every run. */
  enabled: boolean;
};

export const EVE_VERSIONS: readonly EveVersion[] = [
  { version: "0.79.0", enabled: true },
  { version: "0.81.1", enabled: true },
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
