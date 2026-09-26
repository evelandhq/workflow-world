/**
 * Exact releases this World is verified against. The `@workflow/*` set an eve
 * release installs, not npm's `latest`, is what decides whether an entry earns
 * its install.
 *
 * The pins here follow eve 0.67.0, the set 0.66.3 moved to. Eveland's supported
 * window is {0.62.x, 0.66.x}, verified at 0.62.0 and 0.66.1, and stays on the
 * previous set; workflow-world 0.22.0 is its match, and this release exists for
 * the consumer that runs eve 0.67 in External mode outside Eveland. The window
 * plus the pin carry THREE `@workflow/*` sets:
 *
 * | package                 | 0.61.0-0.63.0 | 0.64.0-0.66.2 | 0.66.3-0.67.0 |
 * | ----------------------- | ------------- | ------------- | ------------- |
 * | `@workflow/world`       | beta.36       | beta.37       | beta.38       |
 * | `@workflow/world-local` | beta.45       | beta.46       | beta.47       |
 * | `@workflow/core`        | beta.53       | beta.55       | beta.56       |
 * | `@workflow/errors`      | beta.21       | beta.22       | beta.23       |
 * | `@workflow/utils`       | beta.10       | beta.10       | beta.10       |
 *
 * World beta.37 added an opt-in request/response path (`capabilities.invoke`,
 * `World.invoke()`, optional `invoke`/`requestId`/`input` on queue messages)
 * that this World does not declare, so every delivery stays an ordinary wake.
 * World beta.38, world-local beta.47 and core beta.56 change nothing this World
 * calls: the pins move so a consumer's single `@workflow/world` resolution is
 * the one its eve was built against.
 *
 * The whole window speaks message stream v25 and one execution model: every
 * turn runs as steps inside the session's own `workflowEntry` run (the model
 * 0.57 introduced), with a session inbox rebuilt on hook tokens
 * (`<sessionId>:anchor`, `<runId>:handoff`), deployment handoff and renewable
 * stream leases. The per-turn child `turnWorkflow` run left with 0.55.
 *
 * - 0.62.0 is the floor, which existing Eveland Releases run, and samples the
 *   oldest set. It still has the unstamped `taskRunWorkflow` run behind
 *   background `defineTool`.
 * - 0.66.1 is Eveland's verified release on the newer line and samples the
 *   middle set. Background work is a workflow-tool run, and a background
 *   subagent's admission is no longer announced as `subagent.completed`.
 * - 0.67.0 is what the pins follow and samples the newest set. It also removed
 *   the `conversation` / `task` run mode: every session parks after its turn,
 *   so a first turn no longer completes a run or disposes a hook
 *   (`event-types.mts`).
 *
 * Exact patches matter because Workflow pins move within a minor line: 0.51.1
 * did it, 0.54.4 did it again, and 0.66.3 did it once more. Each enabled entry
 * costs an npm install plus a full `eve build`. Local runs cover all enabled
 * entries; CI supplies EVE_VERSION so each matrix job covers only the release
 * named in its label.
 */
export type EveVersion = {
  version: string;
  /** Set false to keep the entry documented without paying for it on every run. */
  enabled: boolean;
};

export const EVE_VERSIONS: readonly EveVersion[] = [
  { version: "0.62.0", enabled: true },
  { version: "0.66.1", enabled: true },
  { version: "0.67.0", enabled: true },
];

const eveVersionUnderTest = process.env.EVE_VERSION;

export const ENABLED_EVE_VERSIONS = EVE_VERSIONS.filter(
  (entry) => entry.enabled && (!eveVersionUnderTest || entry.version === eveVersionUnderTest),
);

if (eveVersionUnderTest && ENABLED_EVE_VERSIONS.length === 0) {
  throw new Error(`EVE_VERSION "${eveVersionUnderTest}" is not enabled`);
}
