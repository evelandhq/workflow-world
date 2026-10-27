import { afterEach, expect, test, vi } from "vitest";

afterEach(() => {
  vi.unstubAllEnvs();
});

test("a matrix job runs only its selected Eve version", async () => {
  vi.stubEnv("EVE_VERSION", "0.81.1");

  const { ENABLED_EVE_VERSIONS } = await import("./eve-versions.mts?matrix-selected");

  expect(ENABLED_EVE_VERSIONS.map(({ version }) => version)).toEqual(["0.81.1"]);
});

test("an unknown matrix version fails instead of silently running no tests", async () => {
  vi.stubEnv("EVE_VERSION", "9.9.9");

  await expect(import("./eve-versions.mts?matrix-unknown")).rejects.toThrow(
    'EVE_VERSION "9.9.9" is not enabled',
  );
});

test("only eve 0.81.1 and later hand a session over by force-claiming its hooks", async () => {
  const { handsOffByForceClaim } = await import("./eve-versions.mts?handoff");

  expect(handsOffByForceClaim("0.79.0")).toBe(false);
  expect(handsOffByForceClaim("0.81.0")).toBe(false);
  expect(handsOffByForceClaim("0.81.1")).toBe(true);
  expect(handsOffByForceClaim("0.82.0")).toBe(true);
  expect(handsOffByForceClaim("1.0.0")).toBe(true);
  expect(() => handsOffByForceClaim("0.81")).toThrow("unrecognized eve version");
});
