import { afterEach, expect, test, vi } from "vitest";

afterEach(() => {
  vi.unstubAllEnvs();
});

test("a matrix job runs only its selected Eve version", async () => {
  vi.stubEnv("EVE_VERSION", "0.37.1");

  const { ENABLED_EVE_VERSIONS } = await import("./eve-versions.mts?matrix-selected");

  expect(ENABLED_EVE_VERSIONS.map(({ version }) => version)).toEqual(["0.37.1"]);
});

test("an unknown matrix version fails instead of silently running no tests", async () => {
  vi.stubEnv("EVE_VERSION", "9.9.9");

  await expect(import("./eve-versions.mts?matrix-unknown")).rejects.toThrow(
    'EVE_VERSION "9.9.9" is not enabled',
  );
});
