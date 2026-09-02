/**
 * Exact releases from Eveland's supported eve window. The compatibility policy,
 * not npm's `latest`, is the authority on what may be deployed.
 *
 * The current window is {0.47.x, 0.49.x}. 0.48 is skipped: 0.49.0 superseded it
 * within hours of the window sliding onto it, so no deployment ever ran on it.
 * The window contains two distinct Workflow package sets:
 *
 * | package                 | 0.47.0–0.47.7 | 0.49.0  |
 * | ----------------------- | ------------- | ------- |
 * | `@workflow/world`       | beta.28       | beta.32 |
 * | `@workflow/world-local` | beta.37       | beta.41 |
 * | `@workflow/core`        | beta.43       | beta.47 |
 * | `@workflow/errors`      | beta.17       | beta.19 |
 * | `@workflow/utils`       | beta.8        | beta.10 |
 *
 * Enabled entries cover each set once, at the release Eveland verified for that
 * line: 0.47.7 and 0.49.0. The window's floor, 0.47.0, stays documented but
 * disabled because it installs the same set as 0.47.7. Exact patches matter
 * because Workflow pins have moved within a minor line in the past. Each enabled
 * entry costs an npm install plus a full `eve build`. Local runs cover all
 * enabled entries; CI supplies EVE_VERSION so each matrix job covers only the
 * release named in its label.
 */
export type EveVersion = {
  version: string;
  /** Set false to keep the entry documented without paying for it on every run. */
  enabled: boolean;
};

export const EVE_VERSIONS: readonly EveVersion[] = [
  { version: "0.47.0", enabled: false },
  { version: "0.47.7", enabled: true },
  { version: "0.49.0", enabled: true },
];

const eveVersionUnderTest = process.env.EVE_VERSION;

export const ENABLED_EVE_VERSIONS = EVE_VERSIONS.filter(
  (entry) => entry.enabled && (!eveVersionUnderTest || entry.version === eveVersionUnderTest),
);

if (eveVersionUnderTest && ENABLED_EVE_VERSIONS.length === 0) {
  throw new Error(`EVE_VERSION "${eveVersionUnderTest}" is not enabled`);
}
