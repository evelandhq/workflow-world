/**
 * The eve versions this suite runs against.
 *
 * Eveland's supported window is defined in
 * `packages/core/src/eve-compatibility.ts` as three lines with one verified
 * version each — 0.27.13, 0.28.0, 0.29.5 — and projected into pnpm catalogs
 * (`eve-matrix`) that `packages/sandbox-bwrap` consumes. This list is the same
 * idea for the same reason: an agent may be built on any verified minor in the
 * window, so the World has to work on all of them.
 *
 * All three pin `@workflow/world` 5.0.0-beta.23 — the version this package pins —
 * so `specVersion` is 5 across the window and eve's exact-equality runtime check
 * cannot reject the World on any of them. The only drift is `@workflow/core`
 * (beta.37 on 0.27/0.28, beta.38 on 0.29) and `@workflow/world-local`
 * (beta.31 vs beta.32). Since this package wraps world-local's
 * `createQueueHandler`, that one-patch skew is the thing worth proving.
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
  { version: "0.27.13", enabled: false },
  { version: "0.28.0", enabled: false },
  { version: "0.29.5", enabled: true },
];

export const ENABLED_EVE_VERSIONS = EVE_VERSIONS.filter((entry) => entry.enabled);
