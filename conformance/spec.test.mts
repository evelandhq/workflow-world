import { createRequire } from "node:module";
import { createTestSuite } from "@workflow/world-testing";
import { eventLimit } from "@workflow/world-testing/dist/src/event-limit.mjs";
import { describe, expect, test } from "vitest";
import { createWorld } from "../src/index.js";
import { DEPLOYMENT_ID, PACKAGE_NAME, EXECUTOR_PORT, TENANT_ID } from "./env.mts";

const require = createRequire(import.meta.url);

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
 * `assertWorldSupportsRuntimeProtocol` admits a World whose `specVersion` lies
 * in `[SPEC_VERSION_SUPPORTS_SLOT_IDENTITY, SPEC_VERSION_MAX_SUPPORTED]` as
 * compiled into the runtime bundle. Our declaration comes from our own
 * `@workflow/world` pin, and the harness ships its own runtime built from a
 * different pin, so nothing forces the two to agree -- several versions of
 * `@workflow/world` coexist in this tree.
 *
 * This World declares `mintedSpecVersion()` (see `src/index.ts`): the sealed
 * log, 7, unless `WORKFLOW_SEALED_LOG=0` opts the process back to slot
 * identity. Assert that the declaration is exactly that, and that it sits
 * inside the range the harness's own `@workflow/world` accepts, resolved from
 * the harness rather than from us so a harness that has not learned the
 * version we stamp fails here rather than twelve tests later.
 */
test("this World's specVersion matches what the test runtime demands", async () => {
  const world = createWorld({ tenantId: TENANT_ID, deploymentId: DEPLOYMENT_ID });
  const { mintedSpecVersion } = await import("@workflow/world");
  expect(world.specVersion).toBe(mintedSpecVersion());

  const harnessRequire = createRequire(require.resolve("@workflow/world-testing/package.json"));
  const harnessWorld = harnessRequire("@workflow/world") as typeof import("@workflow/world");
  expect(world.specVersion).toBeGreaterThanOrEqual(
    harnessWorld.SPEC_VERSION_SUPPORTS_SLOT_IDENTITY,
  );
  expect(world.specVersion).toBeLessThanOrEqual(harnessWorld.SPEC_VERSION_MAX_SUPPORTED);
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
