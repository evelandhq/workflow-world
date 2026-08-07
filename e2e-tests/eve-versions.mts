/**
 * The eve versions this suite runs against.
 *
 * Eveland's supported window is defined in
 * `packages/core/src/eve-compatibility.ts` as three lines with one verified
 * version each — 0.29.5, 0.30.6, 0.31.0 — and projected into pnpm catalogs
 * (`eve-matrix`) that `packages/sandbox-bwrap` consumes. This list is the same
 * idea for the same reason: an agent may be built on any verified minor in the
 * window, so the World has to work on all of them. The window slid when eve
 * 0.31 shipped: 0.28.0 fell out of it, so its entry is gone rather than
 * disabled.
 *
 * This package pins 0.30.6, and 0.31.0 installs the identical `@workflow/*`
 * set (checked against the registry, 2026-08-07), so the skew still runs
 * backwards down the window:
 *
 * | package                | 0.29.5    | 0.30.6 (ours) | 0.31.0    |
 * | ---------------------- | --------- | ------------- | --------- |
 * | `@workflow/world`      | beta.23   | beta.24       | beta.24   |
 * | `@workflow/world-local`| beta.32   | beta.33       | beta.33   |
 * | `@workflow/core`       | beta.38   | beta.39       | beta.39   |
 * | `@workflow/errors`     | beta.14   | beta.15       | beta.15   |
 * | `@workflow/utils`      | beta.8    | beta.8        | beta.8    |
 *
 * `specVersion` agreement survives all of it — `SPEC_VERSION_CURRENT` is 5 in
 * both beta.23 and beta.24 — so eve's exact-equality runtime check still cannot
 * reject the World on any line. What is worth proving is the `world-local` skew,
 * one patch now, because this package wraps its `createQueueHandler`. That
 * makes the *oldest* line the interesting one, which is why 0.29.5 is enabled
 * alongside the newest rather than the middle: 0.30.6 carries exactly the
 * `@workflow/*` set 0.31.0 already proves.
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
];

export const ENABLED_EVE_VERSIONS = EVE_VERSIONS.filter((entry) => entry.enabled);
