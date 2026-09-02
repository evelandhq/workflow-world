import { createTestSuite } from "@workflow/world-testing";
import { eventLimit } from "@workflow/world-testing/dist/src/event-limit.mjs";
import { describe, expect, test } from "vitest";
import { createWorld } from "../src/index.js";
import { DEPLOYMENT_ID, PACKAGE_NAME, EXECUTOR_PORT, TENANT_ID } from "./env.mts";

/**
 * Guards the setup itself. Every assertion below is something that, if wrong,
 * makes the suite pass for the wrong reason — an embedded-mode run would go green
 * while proving nothing about the dispatcher.
 */
test("the harness is really configured for external mode", () => {
  expect(process.env.WORKFLOW_WORLD_RUNNER).toBe("external");
  expect(Number(process.env.PORT)).toBe(EXECUTOR_PORT);
  expect(process.env.WORKFLOW_WORLD_RUNTIME_SECRET).toBeTruthy();
  expect(process.env.WORKFLOW_WORLD_TENANT_ID).toBe(TENANT_ID);
  expect(process.env.WORKFLOW_WORLD_DEPLOYMENT_ID).toBe(DEPLOYMENT_ID);
});

/**
 * `assertWorldSupportsRuntimeProtocol` compares `world.specVersion` against the
 * constants compiled into the runtime bundle: exact equality through
 * `@workflow/core` beta.40, and the range
 * `[SPEC_VERSION_CURRENT, SPEC_VERSION_MAX_SUPPORTED]` from beta.41 on. Our
 * `specVersion` comes from our own `@workflow/world` pin, and the harness ships
 * its own copy, so nothing forces the two to agree — several versions of
 * `@workflow/world` coexist in this tree.
 *
 * In beta.42 the package default is v6 and slot identity is mandatory. From
 * beta.32 of `@workflow/world` (eve 0.49) the runtime floor is the slot-identity
 * version and `SPEC_VERSION_CURRENT` sits one above it at the sealed log, so
 * the two no longer coincide: this World deliberately declares the floor (see
 * `src/index.ts`) because every eve line Eveland hosts must be able to read
 * the version it stamps. Assert the floor, and that it stays inside the
 * runtime's accepted range.
 *
 * Asserting it here means a bump on either side surfaces as a version mismatch
 * instead of as a mysterious dispatch failure twelve tests later.
 */
test("this World's specVersion matches what the test runtime demands", async () => {
  const world = createWorld({ tenantId: TENANT_ID, deploymentId: DEPLOYMENT_ID });
  const { SPEC_VERSION_SUPPORTS_SLOT_IDENTITY, SPEC_VERSION_MAX_SUPPORTED } =
    await import("@workflow/world");
  expect(world.specVersion).toBe(SPEC_VERSION_SUPPORTS_SLOT_IDENTITY);
  expect(world.specVersion).toBeLessThanOrEqual(SPEC_VERSION_MAX_SUPPORTED);
});

createTestSuite(PACKAGE_NAME);

/**
 * Not part of `createTestSuite`, so it has to be called explicitly — and it is
 * worth calling: it is the one place the per-run event ceiling is exercised, and
 * that ceiling is the World's responsibility, not the runtime's.
 *
 * Upstream's `world-postgres` does not implement it at all, so this suite is a
 * strict addition to what the reference World satisfies.
 */
describe("server-supplied event limit", () => {
  eventLimit(PACKAGE_NAME);
});
