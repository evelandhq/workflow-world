/**
 * Exact releases this World is verified against. The `@workflow/*` set an eve
 * release installs, not npm's `latest`, is what decides whether an entry earns
 * its install.
 *
 * Eveland's supported window is {0.74.x, 0.75.x} -- contiguous, verified at
 * 0.74.0 and 0.75.1 -- and the pins here follow its newest line, eve 0.75.1.
 * Eveland retired 0.68 when 0.75 entered, so the core beta.57 set it carried
 * left the matrix with it, as the stable 5.0.1 set did when 0.72 was retired
 * for 0.73. The window carries TWO `@workflow/*` sets: 0.73.0 moved to core
 * 5.1.0 with world, world-local and errors 5.0.2 and kept that set through
 * 0.75.0, and 0.75.1 moved to the stable 5.2 family:
 *
 * | package                 | 0.73.0-0.75.0 | 0.75.1 |
 * | ----------------------- | ------------- | ------ |
 * | `@workflow/world`       | 5.0.2         | 5.0.3  |
 * | `@workflow/world-local` | 5.0.2         | 5.1.0  |
 * | `@workflow/core`        | 5.1.0         | 5.2.0  |
 * | `@workflow/errors`      | 5.0.2         | 5.0.3  |
 * | `@workflow/utils`       | 5.0.1         | 5.0.1  |
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
 * versions did not move: both sets mint 8 and accept 6 through 8, and this
 * World keeps declaring 7 (`src/index.ts`).
 *
 * The whole window writes delta-only appends on message stream v26, which
 * kept v25's append events -- and shares one execution
 * model: every turn runs as steps inside the session's own `workflowEntry` run
 * (the model 0.57 introduced), with a session inbox rebuilt on hook tokens
 * (`<sessionId>:anchor`, `<runId>:handoff`), deployment handoff and renewable
 * stream leases. The per-turn child `turnWorkflow` run left with 0.55.
 *
 * - 0.74.0 is the window's floor, which existing Eveland Releases run, and
 *   samples the core 5.1.0 / world 5.0.2 set.
 * - 0.75.1 is Eveland's verified release on the newest line, what the pins
 *   follow, and samples the stable 5.2 family.
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
  { version: "0.74.0", enabled: true },
  { version: "0.75.1", enabled: true },
];

const eveVersionUnderTest = process.env.EVE_VERSION;

export const ENABLED_EVE_VERSIONS = EVE_VERSIONS.filter(
  (entry) => entry.enabled && (!eveVersionUnderTest || entry.version === eveVersionUnderTest),
);

if (eveVersionUnderTest && ENABLED_EVE_VERSIONS.length === 0) {
  throw new Error(`EVE_VERSION "${eveVersionUnderTest}" is not enabled`);
}
