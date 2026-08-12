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
 * The assertion below stays equality, which is deliberately stricter than the
 * range a beta.41 runtime would accept. The slack in that range is for a World
 * that opts into a version above the default — `world-vercel` declares the
 * slot-identity version so its runs get slot event ids — and this World mints
 * ULIDs and stays on the default. So for us anything other than equality is
 * skew rather than an intentional opt-in, and the stricter assertion is the one
 * that catches it.
 *
 * Asserting it here means a bump on either side surfaces as a version mismatch
 * instead of as a mysterious dispatch failure twelve tests later.
 */
test("this World's specVersion matches what the test runtime demands", async () => {
  const world = createWorld({ tenantId: TENANT_ID, deploymentId: DEPLOYMENT_ID });
  const { SPEC_VERSION_CURRENT } = await import("@workflow/world");
  expect(world.specVersion).toBe(SPEC_VERSION_CURRENT);
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
