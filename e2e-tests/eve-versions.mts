/**
 * Exact releases from Eveland's supported eve window. The compatibility policy,
 * not npm's `latest`, is the authority on what may be deployed.
 *
 * The current window is {0.62.x, 0.63.x} -- contiguous again, verified at
 * 0.62.0 and 0.63.0. 0.58.x slid out on 2026-09-20 (0.59, 0.60 and 0.61 were
 * skipped on 2026-09-19 and never verified). The window carries ONE
 * `@workflow/*` set -- 0.61.0 moved the bundled set, and 0.62.0 and 0.63.0
 * re-bundled it unchanged:
 *
 * | package                 | 0.54.4-0.60.1 | 0.61.0-0.63.0 |
 * | ----------------------- | ------------- | ------------- |
 * | `@workflow/world`       | beta.35       | beta.36       |
 * | `@workflow/world-local` | beta.44       | beta.45       |
 * | `@workflow/core`        | beta.51       | beta.53       |
 * | `@workflow/errors`      | beta.21       | beta.21       |
 * | `@workflow/utils`       | beta.10       | beta.10       |
 *
 * World beta.36 widens `runs.list({ status })` to accept an array (this World
 * now implements it); world-local beta.45 and core beta.53 change nothing this
 * World calls. The pins here follow that set.
 *
 * The whole window speaks message stream v25 and one execution model: every
 * turn runs as steps inside the session's own `workflowEntry` run (the model
 * 0.57 introduced), with a session inbox rebuilt on hook tokens
 * (`<sessionId>:anchor`, `<runId>:handoff`), deployment handoff and renewable
 * stream leases. The per-turn child `turnWorkflow` run left with 0.55.
 *
 * - 0.62.0 is the floor, which existing Releases run. It still has the
 *   unstamped `taskRunWorkflow` run behind background `defineTool`.
 * - 0.63.0 is what new builds get. It removed that run: background work is a
 *   workflow-tool run there, and a background subagent's admission is no
 *   longer announced as `subagent.completed`. Neither adds a World call.
 *
 * Exact patches matter because Workflow pins move within a minor line: 0.51.1
 * did it, and 0.54.4 did it again. Each enabled entry costs an npm install plus
 * a full `eve build`. Local runs cover all enabled entries; CI supplies
 * EVE_VERSION so each matrix job covers only the release named in its label.
 */
export type EveVersion = {
  version: string;
  /** Set false to keep the entry documented without paying for it on every run. */
  enabled: boolean;
};

export const EVE_VERSIONS: readonly EveVersion[] = [
  { version: "0.62.0", enabled: true },
  { version: "0.63.0", enabled: true },
];

const eveVersionUnderTest = process.env.EVE_VERSION;

export const ENABLED_EVE_VERSIONS = EVE_VERSIONS.filter(
  (entry) => entry.enabled && (!eveVersionUnderTest || entry.version === eveVersionUnderTest),
);

if (eveVersionUnderTest && ENABLED_EVE_VERSIONS.length === 0) {
  throw new Error(`EVE_VERSION "${eveVersionUnderTest}" is not enabled`);
}
