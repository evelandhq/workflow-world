/**
 * Exact releases this World is verified against. The `@workflow/*` set an eve
 * release bundles, not npm's `latest`, is what decides whether an entry earns
 * its install.
 *
 * Eveland supports one eve line, 1.0.x. eve 1.0 no longer installs the
 * Workflow runtime as dependencies: it bundles it, and records the set in
 * `dist/src/compiled/.vendor-stamp.json`. 1.0.0-rc.5 bundles the Workflow 5.4
 * runtime the pins here follow:
 *
 * | package                 | 1.0.0-rc.5 |
 * | ----------------------- | ---------- |
 * | `@workflow/core`        | 5.4.0      |
 * | `@workflow/world`       | 5.1.0      |
 * | `@workflow/world-local` | 5.2.1      |
 * | `@workflow/errors`      | 5.1.0      |
 * | `@workflow/utils`       | 5.0.2      |
 *
 * eve 1.0 creates spec 9 (single-orchestrator) runs by default
 * (`WORKFLOW_SPEC_VERSION=9`) on a World that declares
 * `capabilities.inBandFence`, and spec 8 runs on one that does not. It moves a
 * session to a fresh run by force-claiming the session's hook tokens
 * (`capabilities.hookForceClaim`, `session-handoff.test.mts`) and reads
 * `capabilities.deploymentAffinity`.
 *
 * Exact patches matter because Workflow pins move within a line: eve 1.0.0-rc.3
 * moved them, and so did rc.5. Each enabled entry costs an npm install plus a
 * full `eve build`. Local runs cover all enabled entries; CI supplies
 * EVE_VERSION so each matrix job covers only the release named in its label.
 */
export type EveVersion = {
  version: string;
  /** Set false to keep the entry documented without paying for it on every run. */
  enabled: boolean;
};

export const EVE_VERSIONS: readonly EveVersion[] = [{ version: "1.0.0-rc.5", enabled: true }];

const eveVersionUnderTest = process.env.EVE_VERSION;

export const ENABLED_EVE_VERSIONS = EVE_VERSIONS.filter(
  (entry) => entry.enabled && (!eveVersionUnderTest || entry.version === eveVersionUnderTest),
);

if (eveVersionUnderTest && ENABLED_EVE_VERSIONS.length === 0) {
  throw new Error(`EVE_VERSION "${eveVersionUnderTest}" is not enabled`);
}
