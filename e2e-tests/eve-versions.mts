/**
 * The eve versions this suite runs against.
 *
 * Eveland's supported window is defined in
 * `packages/core/src/eve-compatibility.ts` as three lines with one verified
 * version each — 0.28.0, 0.29.5, 0.30.6 — and projected into pnpm catalogs
 * (`eve-matrix`) that `packages/sandbox-bwrap` consumes. This list is the same
 * idea for the same reason: an agent may be built on any verified minor in the
 * window, so the World has to work on all of them. The window slid when eve
 * 0.30 shipped: 0.27.13 fell out of it, so its entry is gone rather than
 * disabled.
 *
 * This package pins eve's newest verified line, so the skew now runs backwards
 * down the window instead of being a single patch:
 *
 * | package                | 0.28.0    | 0.29.5    | 0.30.6 (ours) |
 * | ---------------------- | --------- | --------- | ------------- |
 * | `@workflow/world`      | beta.23   | beta.23   | beta.24       |
 * | `@workflow/world-local`| beta.31   | beta.32   | beta.33       |
 * | `@workflow/core`       | beta.37   | beta.38   | beta.39       |
 * | `@workflow/errors`     | beta.13   | beta.14   | beta.15       |
 * | `@workflow/utils`      | beta.7    | beta.8    | beta.8        |
 *
 * `specVersion` agreement survives all of it — `SPEC_VERSION_CURRENT` is 5 in
 * both beta.23 and beta.24 — so eve's exact-equality runtime check still cannot
 * reject the World on any line. What is worth proving is the `world-local` skew,
 * up to two patches now, because this package wraps its `createQueueHandler`.
 * That makes the *oldest* line the interesting one, which is why 0.28.0 is
 * enabled alongside the newest rather than the middle.
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
  { version: "0.28.0", enabled: true },
  { version: "0.29.5", enabled: false },
  { version: "0.30.6", enabled: true },
];

export const ENABLED_EVE_VERSIONS = EVE_VERSIONS.filter((entry) => entry.enabled);
