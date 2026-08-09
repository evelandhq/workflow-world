/**
 * The eve versions this suite runs against.
 *
 * Eveland's supported window is defined in
 * `packages/core/src/eve-compatibility.ts` as lines with one verified version
 * each, projected into pnpm catalogs (`eve-matrix`) that
 * `packages/sandbox-bwrap` consumes. That file, not this one and not npm's
 * `latest`, is the authority on which versions may be deployed. This list is the
 * same idea for the same reason: an agent may be built on any verified minor in
 * the window, so the World has to work on all of them. The window slid once when
 * eve 0.31 shipped (0.28.0 fell out, so its entry is gone rather than disabled)
 * and again when it verified the 0.31.2+ line, which is what this package now
 * pins.
 *
 * This package pins 0.31.3, so the skew runs backwards down the window:
 *
 * | package                | 0.29.5    | 0.30.6    | 0.31.0    | 0.31.3 (ours) |
 * | ---------------------- | --------- | --------- | --------- | ------------- |
 * | `@workflow/world`      | beta.23   | beta.24   | beta.24   | beta.25       |
 * | `@workflow/world-local`| beta.32   | beta.33   | beta.33   | beta.34       |
 * | `@workflow/core`       | beta.38   | beta.39   | beta.39   | beta.40       |
 * | `@workflow/errors`     | beta.14   | beta.15   | beta.15   | beta.16       |
 * | `@workflow/utils`      | beta.8    | beta.8    | beta.8    | beta.8        |
 *
 * The `0.31.0` column is the 0.31 line only up to 0.31.1; the set moved at
 * 0.31.2, which is where our column comes from. So a version's minor does not
 * identify its `@workflow/*` set and these entries have to name exact patches —
 * bumping one to eve's `latest` on the assumption that a line is homogeneous
 * would install a set the World does not declare. That is the deploy-time
 * pairing failure `src/eve-pin-contract.test.ts` exists to prevent, and this
 * suite, being the only thing here that loads a real eve, is where it would
 * actually surface. `scripts/eve-pin-drift.mjs` reports the *earliest* release
 * that moved the set for exactly this reason.
 *
 * `specVersion` agreement survives all of it — `SPEC_VERSION_CURRENT` is 5 in
 * beta.23, beta.24 and beta.25 alike — so eve's exact-equality runtime check
 * cannot reject the World on any line in the window. What is worth proving is
 * the `world-local` skew, because this package wraps its `createQueueHandler`,
 * and that skew is now two patches wide (beta.32 on the oldest line against the
 * beta.34 we pin) rather than one. So the *oldest* line stays the interesting
 * one.
 *
 * Enabled entries cover each distinct `@workflow/*` set once: 0.29.5 for
 * beta.23, 0.31.0 for beta.24, and 0.31.3 for the beta.25 we pin. 0.30.6 stays
 * disabled because 0.31.0 carries its set exactly. Note that beta.24 needs its
 * own enabled entry now — while this package pinned 0.30.6 that set was covered
 * by definition, and moving the pin to beta.25 is what left it unproven.
 *
 * Each entry costs an npm install plus a full `eve build` (~1-2 min), so they are
 * enabled deliberately rather than all at once.
 */
export type EveVersion = {
  version: string;
  /** Set false to keep the entry documented without paying for it on every run. */
  enabled: boolean;
};

export const EVE_VERSIONS: readonly EveVersion[] = [
  { version: "0.29.5", enabled: true },
  { version: "0.30.6", enabled: false },
  { version: "0.31.0", enabled: true },
  { version: "0.31.3", enabled: true },
];

export const ENABLED_EVE_VERSIONS = EVE_VERSIONS.filter((entry) => entry.enabled);
