/**
 * Exact releases from Eveland's supported eve window. The compatibility policy,
 * not npm's `latest`, is the authority on what may be deployed.
 *
 * The current window is {0.54.x, 0.55.x} -- two consecutive lines, verified at
 * 0.54.5 and 0.55.0. 0.52.x and 0.53.x slid out together on 2026-09-15. eve
 * 0.54.4 moved the bundled Workflow packages at a patch and 0.55.x keeps that
 * set, so the window carries two distinct sets, split inside the 0.54 line:
 *
 * | package                 | 0.53.0-0.54.3 | 0.54.4-0.55.0 |
 * | ----------------------- | ------------- | ------------- |
 * | `@workflow/world`       | beta.34       | beta.35       |
 * | `@workflow/world-local` | beta.43       | beta.44       |
 * | `@workflow/core`        | beta.50       | beta.51       |
 * | `@workflow/errors`      | beta.21       | beta.21       |
 * | `@workflow/utils`       | beta.10       | beta.10       |
 *
 * The whole window speaks message stream v25, so the World's snapshot
 * stripping and rehydration see one wire. 0.54.3 is the only remaining sample
 * of set A, which every Release built on 0.54.0 through 0.54.3 still runs.
 * 0.54.5 is the release Eveland verified for the 0.54 line and samples set B
 * under 0.54 code: its `@workflow/world` adds an optional `Queue.queueBatch`,
 * which eve replaces with one `queue` call per message when a World does not
 * implement it (this one does not), and an optional `runContext` on step
 * messages, which this World stores and forwards untouched. 0.55.0 shares set B
 * and still earns its entry: it is the newest line, so it is what new builds
 * get, and it moves the session inbox wire to v7 and reattributes background
 * task activity -- exactly the hook and run traffic this suite exercises.
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
  { version: "0.54.3", enabled: true },
  { version: "0.54.5", enabled: true },
  { version: "0.55.0", enabled: true },
];

const eveVersionUnderTest = process.env.EVE_VERSION;

export const ENABLED_EVE_VERSIONS = EVE_VERSIONS.filter(
  (entry) => entry.enabled && (!eveVersionUnderTest || entry.version === eveVersionUnderTest),
);

if (eveVersionUnderTest && ENABLED_EVE_VERSIONS.length === 0) {
  throw new Error(`EVE_VERSION "${eveVersionUnderTest}" is not enabled`);
}
