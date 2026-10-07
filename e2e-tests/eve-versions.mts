/**
 * Exact releases this World is verified against. The `@workflow/*` set an eve
 * release installs, not npm's `latest`, is what decides whether an entry earns
 * its install.
 *
 * Eveland's supported window is {0.68.x, 0.72.x} -- gapped, verified at
 * 0.68.0 and 0.72.1 -- and the pins here follow its newest line, eve 0.72.1.
 * Eveland retired 0.62 and 0.66, so the sets they carried (core beta.53 and
 * beta.55) left the matrix with them, as did 0.67.0's beta.56. The window
 * carries TWO `@workflow/*` sets: 0.67.2 moved to core beta.57 and kept it
 * through 0.70.1, and 0.70.2 moved to the first stable 5.0 line, which 0.72.1
 * still installs:
 *
 * | package                 | 0.67.2-0.70.1 | 0.70.2-0.72.1 |
 * | ----------------------- | ------------- | ------------- |
 * | `@workflow/world`       | beta.39       | 5.0.1         |
 * | `@workflow/world-local` | beta.48       | 5.0.1         |
 * | `@workflow/core`        | beta.57       | 5.0.1         |
 * | `@workflow/errors`      | beta.24       | 5.0.1         |
 * | `@workflow/utils`       | beta.10       | 5.0.0         |
 *
 * World beta.37 added an opt-in request/response path (`capabilities.invoke`,
 * `World.invoke()`, optional `invoke`/`requestId`/`input` on queue messages)
 * that this World does not declare, so every delivery stays an ordinary wake.
 * The one contract change since is on the read side: event-log reads take
 * `EventsResolveData`, which adds `'skip-step-inputs'` for replay. Core beta.57
 * already sends it; a World MUST read anything but `'none'` as `'all'`, which
 * this World always has (`src/storage.events.test.ts` pins it). Spec versions
 * did not move: both sets mint 8 and accept 6 through 8, and this World keeps
 * declaring 7 (`src/index.ts`).
 *
 * The whole window writes delta-only appends -- message stream v25 on 0.68,
 * v26 from 0.69, which kept v25's append events -- and shares one execution
 * model: every turn runs as steps inside the session's own `workflowEntry` run
 * (the model 0.57 introduced), with a session inbox rebuilt on hook tokens
 * (`<sessionId>:anchor`, `<runId>:handoff`), deployment handoff and renewable
 * stream leases. The per-turn child `turnWorkflow` run left with 0.55.
 *
 * - 0.68.0 is the window's floor, which existing Eveland Releases run, and
 *   samples the core beta.57 set.
 * - 0.72.1 is Eveland's verified release on the newest line, what the pins
 *   follow, and samples the stable 5.0.1 set.
 *
 * Every entry is past 0.67.0, which removed the `conversation` / `task` run
 * mode: every session parks after its turn, so a first turn no longer
 * completes a run or disposes a hook (`event-types.mts`).
 *
 * Exact patches matter because Workflow pins move within a minor line: 0.51.1
 * did it, 0.54.4 did it again, and so did 0.66.3, 0.67.2 and 0.70.2. Each
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
  { version: "0.68.0", enabled: true },
  { version: "0.72.1", enabled: true },
];

const eveVersionUnderTest = process.env.EVE_VERSION;

export const ENABLED_EVE_VERSIONS = EVE_VERSIONS.filter(
  (entry) => entry.enabled && (!eveVersionUnderTest || entry.version === eveVersionUnderTest),
);

if (eveVersionUnderTest && ENABLED_EVE_VERSIONS.length === 0) {
  throw new Error(`EVE_VERSION "${eveVersionUnderTest}" is not enabled`);
}
