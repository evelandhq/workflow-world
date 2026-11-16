/**
 * Exact releases this World is verified against. The `@workflow/*` set an eve
 * release bundles, not npm's `latest`, is what decides whether an entry earns
 * its install.
 *
 * eve bundles the Workflow runtime rather than installing it, and records the
 * set in `dist/src/compiled/.vendor-stamp.json`. 1.0.0 bundles the Workflow
 * 5.5 runtime the pins here follow:
 *
 * | package                 | 1.0.0 |
 * | ----------------------- | ----- |
 * | `@workflow/core`        | 5.5.0 |
 * | `@workflow/world`       | 5.2.0 |
 * | `@workflow/world-local` | 5.3.0 |
 * | `@workflow/errors`      | 5.1.1 |
 * | `@workflow/utils`       | 5.1.0 |
 *
 * eve 1.0 creates spec 9 (single-orchestrator) runs (`WORKFLOW_SPEC_VERSION=9`)
 * on a World that declares `capabilities.inBandFence`. It moves a session to a
 * fresh run by force-claiming the session's hook tokens
 * (`capabilities.hookForceClaim`, `session-handoff.test.mts`), reads
 * `capabilities.deploymentAffinity`, and starts every session with an atomic
 * start Hook on the session's channel address (`capabilities.atomicStartHook`),
 * refusing to start one on a World that does not declare it.
 *
 * Exact patches matter because Workflow pins can move within a line. Each
 * enabled entry costs an npm install plus a full `eve build`. Local runs cover
 * all enabled entries; CI supplies EVE_VERSION so each matrix job covers only
 * the release named in its label.
 */
export type EveVersion = {
  version: string;
  /** Set false to keep the entry documented without paying for it on every run. */
  enabled: boolean;
};

export const EVE_VERSIONS: readonly EveVersion[] = [{ version: "1.0.0", enabled: true }];

const eveVersionUnderTest = process.env.EVE_VERSION;

export const ENABLED_EVE_VERSIONS = EVE_VERSIONS.filter(
  (entry) => entry.enabled && (!eveVersionUnderTest || entry.version === eveVersionUnderTest),
);

if (eveVersionUnderTest && ENABLED_EVE_VERSIONS.length === 0) {
  throw new Error(`EVE_VERSION "${eveVersionUnderTest}" is not enabled`);
}
