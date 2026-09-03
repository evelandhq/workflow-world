/**
 * Exact releases from Eveland's supported eve window. The compatibility policy,
 * not npm's `latest`, is the authority on what may be deployed.
 *
 * The current window is {0.49.x, 0.50.x}. Its two lines are consecutive, and
 * for the first time they install one single Workflow package set:
 *
 * | package                 | 0.49.0–0.50.0 |
 * | ----------------------- | ------------- |
 * | `@workflow/world`       | beta.32       |
 * | `@workflow/world-local` | beta.41       |
 * | `@workflow/core`        | beta.47       |
 * | `@workflow/errors`      | beta.19       |
 * | `@workflow/utils`       | beta.10       |
 *
 * Both entries stay enabled anyway, because the set is no longer the only axis
 * that matters here: 0.49.x speaks message stream v24 and 0.50.x speaks v25,
 * and the World's snapshot stripping and rehydration sit directly on that wire.
 * One entry per line is what proves both readers still see a correct stream.
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
  { version: "0.49.0", enabled: true },
  { version: "0.50.0", enabled: true },
];

const eveVersionUnderTest = process.env.EVE_VERSION;

export const ENABLED_EVE_VERSIONS = EVE_VERSIONS.filter(
  (entry) => entry.enabled && (!eveVersionUnderTest || entry.version === eveVersionUnderTest),
);

if (eveVersionUnderTest && ENABLED_EVE_VERSIONS.length === 0) {
  throw new Error(`EVE_VERSION "${eveVersionUnderTest}" is not enabled`);
}
