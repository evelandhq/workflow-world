/**
 * Exact releases from Eveland's supported eve window. The compatibility policy,
 * not npm's `latest`, is the authority on what may be deployed.
 *
 * The current window is {0.49.x, 0.50.x, 0.51.x} -- three consecutive lines,
 * verified at 0.49.0, 0.50.0, and 0.51.1. eve 0.51.1 moved the bundled Workflow
 * packages at a PATCH, so the window carries two distinct sets again:
 *
 * | package                 | 0.49.0-0.51.0 | 0.51.1  |
 * | ----------------------- | ------------- | ------- |
 * | `@workflow/world`       | beta.32       | beta.32 |
 * | `@workflow/world-local` | beta.41       | beta.42 |
 * | `@workflow/core`        | beta.47       | beta.48 |
 * | `@workflow/errors`      | beta.19       | beta.19 |
 * | `@workflow/utils`       | beta.10       | beta.10 |
 *
 * The set is not the only axis that matters here: 0.49.x speaks message stream
 * v24 while 0.50.x and 0.51.x speak v25, and the World's snapshot stripping and
 * rehydration sit directly on that wire. Crossing the two axes leaves all three
 * entries earning their cost, because each is the window's only sample of its
 * combination -- 0.49.0 is set A on v24, 0.50.0 is set A on v25, and 0.51.1 is
 * set B on v25. 0.51.1 additionally exercises subagent calls as durable tool
 * runs, which eve 0.51 introduced by preparing the framework agent tool as a
 * workflow tool.
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
  { version: "0.49.0", enabled: true },
  { version: "0.50.0", enabled: true },
  { version: "0.51.1", enabled: true },
];

const eveVersionUnderTest = process.env.EVE_VERSION;

export const ENABLED_EVE_VERSIONS = EVE_VERSIONS.filter(
  (entry) => entry.enabled && (!eveVersionUnderTest || entry.version === eveVersionUnderTest),
);

if (eveVersionUnderTest && ENABLED_EVE_VERSIONS.length === 0) {
  throw new Error(`EVE_VERSION "${eveVersionUnderTest}" is not enabled`);
}
