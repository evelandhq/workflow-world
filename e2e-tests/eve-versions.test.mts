import { afterEach, expect, test, vi } from "vitest";

afterEach(() => {
  vi.unstubAllEnvs();
});

test("a matrix job runs only its selected Eve version", async () => {
  vi.stubEnv("EVE_VERSION", "1.0.0");

  const { ENABLED_EVE_VERSIONS } = await import("./eve-versions.mts?matrix-selected");

  expect(ENABLED_EVE_VERSIONS.map(({ version }) => version)).toEqual(["1.0.0"]);
});

test("a matrix job can select a newer Eve version", async () => {
  vi.stubEnv("EVE_VERSION", "1.3.0");

  const { ENABLED_EVE_VERSIONS } = await import("./eve-versions.mts?matrix-newer");

  expect(ENABLED_EVE_VERSIONS.map(({ version }) => version)).toEqual(["1.3.0"]);
});

test("a documented but disabled Eve version fails instead of running no tests", async () => {
  vi.stubEnv("EVE_VERSION", "1.2.0");

  await expect(import("./eve-versions.mts?matrix-disabled")).rejects.toThrow(
    'EVE_VERSION "1.2.0" is not enabled',
  );
});

test("an unknown matrix version fails instead of silently running no tests", async () => {
  vi.stubEnv("EVE_VERSION", "9.9.9");

  await expect(import("./eve-versions.mts?matrix-unknown")).rejects.toThrow(
    'EVE_VERSION "9.9.9" is not enabled',
  );
});
