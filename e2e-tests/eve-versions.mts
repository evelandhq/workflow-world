/**
 * Exact releases from Eveland's supported eve window. The compatibility policy,
 * not npm's `latest`, is the authority on what may be deployed.
 *
 * The current window is {0.50.x, 0.51.x, 0.52.x} -- three consecutive lines,
 * verified at 0.50.0, 0.51.1, and 0.52.2. 0.49.x has slid out. eve 0.52.2
 * bundles the same Workflow packages as 0.51.1, so the window still carries two
 * distinct sets, split at the 0.51.1 patch that moved them:
 *
 * | package                 | 0.50.0-0.51.0 | 0.51.1-0.52.2 |
 * | ----------------------- | ------------- | ------------- |
 * | `@workflow/world`       | beta.32       | beta.33       |
 * | `@workflow/world-local` | beta.41       | beta.42       |
 * | `@workflow/core`        | beta.47       | beta.48       |
 * | `@workflow/errors`      | beta.19       | beta.20       |
 * | `@workflow/utils`       | beta.10       | beta.10       |
 *
 * The message-stream axis is gone: 0.49.x was the last line on v24, and the
 * whole window now speaks v25, so the World's snapshot stripping and
 * rehydration see one wire. 0.50.0 is the only sample of set A and 0.51.1 the
 * first of set B. 0.52.2 shares set B with 0.51.1 and still earns its entry: it
 * is the newest line, so it is what new builds get, and eve 0.52 rewired how
 * inline turns handle workflow tools -- they now route through one ordered
 * parent inbox, and a retried dispatch may start another run -- which is
 * exactly the World traffic this suite exercises. 0.51.1 stays because it is
 * the release that introduced set B and subagent calls as durable tool runs,
 * under the pre-0.52 dispatch path.
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
  { version: "0.50.0", enabled: true },
  { version: "0.51.1", enabled: true },
  { version: "0.52.2", enabled: true },
];

const eveVersionUnderTest = process.env.EVE_VERSION;

export const ENABLED_EVE_VERSIONS = EVE_VERSIONS.filter(
  (entry) => entry.enabled && (!eveVersionUnderTest || entry.version === eveVersionUnderTest),
);

if (eveVersionUnderTest && ENABLED_EVE_VERSIONS.length === 0) {
  throw new Error(`EVE_VERSION "${eveVersionUnderTest}" is not enabled`);
}
