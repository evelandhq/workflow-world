/**
 * Exact releases from Eveland's supported eve window. The compatibility policy,
 * not npm's `latest`, is the authority on what may be deployed.
 *
 * The current window is {0.52.x, 0.53.x, 0.54.x} -- three consecutive lines,
 * verified at 0.52.5, 0.53.1, and 0.54.3. 0.50.x and 0.51.x slid out together
 * on 2026-09-12. eve 0.53.0 moved the bundled Workflow packages and 0.54.x
 * keeps that set, so the window carries two distinct sets, split at the 0.53.0
 * minor that moved them:
 *
 * | package                 | 0.51.1-0.52.5 | 0.53.0-0.54.3 |
 * | ----------------------- | ------------- | ------------- |
 * | `@workflow/world`       | beta.33       | beta.34       |
 * | `@workflow/world-local` | beta.42       | beta.43       |
 * | `@workflow/core`        | beta.48       | beta.50       |
 * | `@workflow/errors`      | beta.20       | beta.21       |
 * | `@workflow/utils`       | beta.10       | beta.10       |
 *
 * The whole window speaks message stream v25, so the World's snapshot
 * stripping and rehydration see one wire. 0.52.5 is the only remaining sample
 * of set A, and the line whose inline workflow tools route through one ordered
 * parent inbox with a retried dispatch allowed to start another run. 0.53.1 is
 * the first of set B: `ctx.agent(target, input)` delegation and the
 * `experimental_retention` start option, which reaches this World as the
 * reserved `$retention` run attribute it stores but does not enforce. 0.54.3
 * shares set B and still earns its entry: it is the newest line, so it is what
 * new builds get, and 0.54.1 removed the `task_update` progress callbacks and
 * resettled overlapping background tasks across launch turns -- exactly the
 * hook and run traffic this suite exercises.
 *
 * Exact patches matter because Workflow pins have moved within a minor line in
 * the past. Each enabled entry costs an npm install plus a full `eve build`.
 * Local runs cover all enabled entries; CI supplies EVE_VERSION so each matrix
 * job covers only the release named in its label.
 */
export type EveVersion = {
  version: string;
  /** Set false to keep the entry documented without paying for it on every run. */
  enabled: boolean;
};

export const EVE_VERSIONS: readonly EveVersion[] = [
  { version: "0.52.5", enabled: true },
  { version: "0.53.1", enabled: true },
  { version: "0.54.3", enabled: true },
];

const eveVersionUnderTest = process.env.EVE_VERSION;

export const ENABLED_EVE_VERSIONS = EVE_VERSIONS.filter(
  (entry) => entry.enabled && (!eveVersionUnderTest || entry.version === eveVersionUnderTest),
);

if (eveVersionUnderTest && ENABLED_EVE_VERSIONS.length === 0) {
  throw new Error(`EVE_VERSION "${eveVersionUnderTest}" is not enabled`);
}
