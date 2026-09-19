/**
 * Exact releases from Eveland's supported eve window. The compatibility policy,
 * not npm's `latest`, is the authority on what may be deployed.
 *
 * The current window is {0.58.x, 0.62.x} -- a gapped window, verified at
 * 0.58.1 and 0.62.0: 0.59, 0.60 and 0.61 were each superseded within a day
 * (0.62.0 shipped on 2026-09-18, a day and a half after 0.58.1) before any
 * Eveland release carried them and are rejected. 0.55.x slid out on
 * 2026-09-19. The window carries TWO `@workflow/*` sets again -- 0.61.0 moved
 * the bundled set, and 0.62.0 re-bundled it unchanged:
 *
 * | package                 | 0.54.4-0.60.1 | 0.61.0-0.62.0 |
 * | ----------------------- | ------------- | ------------- |
 * | `@workflow/world`       | beta.35       | beta.36       |
 * | `@workflow/world-local` | beta.44       | beta.45       |
 * | `@workflow/core`        | beta.51       | beta.53       |
 * | `@workflow/errors`      | beta.21       | beta.21       |
 * | `@workflow/utils`       | beta.10       | beta.10       |
 *
 * World beta.36 widens `runs.list({ status })` to accept an array (this World
 * now implements it); world-local beta.45 and core beta.53 change nothing this
 * World calls. The pins here follow the newer set, so the older entry is what
 * proves a Release built on the previous set still runs against this package.
 *
 * The whole window speaks message stream v25 and one execution model: every
 * turn runs as steps inside the session's own `workflowEntry` run (the model
 * 0.57 introduced), with a session inbox rebuilt on hook tokens
 * (`<sessionId>:anchor`, `<runId>:handoff`), deployment handoff and renewable
 * stream leases. The per-turn child `turnWorkflow` run left with 0.55.
 *
 * - 0.58.1 samples the older bundled set, which existing Releases run.
 * - 0.62.0 samples the newer set and is what new builds get. It also creates
 *   sessions before their first message (0.59) and stamps a third workflow,
 *   `executeAgentRouterTool` (0.60.1), neither of which adds a World call.
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
  { version: "0.58.1", enabled: true },
  { version: "0.62.0", enabled: true },
];

const eveVersionUnderTest = process.env.EVE_VERSION;

export const ENABLED_EVE_VERSIONS = EVE_VERSIONS.filter(
  (entry) => entry.enabled && (!eveVersionUnderTest || entry.version === eveVersionUnderTest),
);

if (eveVersionUnderTest && ENABLED_EVE_VERSIONS.length === 0) {
  throw new Error(`EVE_VERSION "${eveVersionUnderTest}" is not enabled`);
}
