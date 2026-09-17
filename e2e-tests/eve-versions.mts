/**
 * Exact releases from Eveland's supported eve window. The compatibility policy,
 * not npm's `latest`, is the authority on what may be deployed.
 *
 * The current window is {0.55.x, 0.56.x, 0.57.x} -- three consecutive lines,
 * verified at 0.55.0, 0.56.0 and 0.57.0. 0.54.x slid out on 2026-09-16 and took
 * the older bundled set with it, so for the first time since 0.51.1 the whole
 * window carries ONE `@workflow/*` set (0.57.0 re-bundled it, moving only the
 * vendor stamp's script hash):
 *
 * | package                 | 0.54.4-0.57.0 |
 * | ----------------------- | ------------- |
 * | `@workflow/world`       | beta.35       |
 * | `@workflow/world-local` | beta.44       |
 * | `@workflow/core`        | beta.51       |
 * | `@workflow/errors`      | beta.21       |
 * | `@workflow/utils`       | beta.10       |
 *
 * The whole window speaks message stream v25, so the World's snapshot
 * stripping and rehydration see one wire. With the set and the wire uniform,
 * the axis that remains is how eve drives runs through this World:
 *
 * - 0.55.0 and 0.56.0 dispatch a child `turnWorkflow` run for every message,
 *   owned by a long-lived `workflowEntry` session driver -- one run and one
 *   hook exchange per turn. 0.56.0 differs from 0.55.0 only in extension
 *   contracts and audience defaults, neither of which touches the World, so
 *   0.55.0 samples that model and 0.56.0 stays documented but disabled.
 * - 0.57.0 executes every turn as steps inside the session's own
 *   `workflowEntry` run: no per-turn run, a session inbox rebuilt on hook
 *   tokens (`<sessionId>:anchor`, `<runId>:handoff`), deployment handoff that
 *   starts a successor run targeted at another deployment id, and renewable
 *   stream leases. That is new run, hook and stream traffic, so it earns its
 *   own entry and is what new builds get.
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
  { version: "0.55.0", enabled: true },
  { version: "0.56.0", enabled: false },
  { version: "0.57.0", enabled: true },
];

const eveVersionUnderTest = process.env.EVE_VERSION;

export const ENABLED_EVE_VERSIONS = EVE_VERSIONS.filter(
  (entry) => entry.enabled && (!eveVersionUnderTest || entry.version === eveVersionUnderTest),
);

if (eveVersionUnderTest && ENABLED_EVE_VERSIONS.length === 0) {
  throw new Error(`EVE_VERSION "${eveVersionUnderTest}" is not enabled`);
}
