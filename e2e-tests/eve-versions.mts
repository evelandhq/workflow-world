/**
 * Exact releases from Eveland's supported eve window. The compatibility policy,
 * not npm's `latest`, is the authority on what may be deployed.
 *
 * The current window contains two distinct Workflow package sets:
 *
 * | package                 | 0.34.0  | 0.35.0–0.37.1 |
 * | ----------------------- | ------- | ------------- |
 * | `@workflow/world`       | beta.25 | beta.26       |
 * | `@workflow/world-local` | beta.34 | beta.35       |
 * | `@workflow/core`        | beta.41 | beta.41       |
 * | `@workflow/errors`      | beta.16 | beta.16       |
 * | `@workflow/utils`       | beta.8  | beta.8        |
 *
 * Enabled entries cover each set once: the oldest supported release and the
 * latest verified release pinned by this package. The two intermediate releases
 * remain documented but disabled because they install the same set as 0.37.1.
 * Exact patches matter because Workflow pins have moved within a minor line in
 * the past. Each enabled entry costs an npm install plus a full `eve build`.
 */
export type EveVersion = {
  version: string;
  /** Set false to keep the entry documented without paying for it on every run. */
  enabled: boolean;
};

export const EVE_VERSIONS: readonly EveVersion[] = [
  { version: "0.34.0", enabled: true },
  { version: "0.35.0", enabled: false },
  { version: "0.36.0", enabled: false },
  { version: "0.37.1", enabled: true },
];

export const ENABLED_EVE_VERSIONS = EVE_VERSIONS.filter((entry) => entry.enabled);
