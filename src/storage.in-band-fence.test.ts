import { inBandFenceConformance } from "@workflow/world/test-support/in-band-fence-conformance.js";
import { Pool } from "pg";
import { afterAll, beforeAll, describe, expect, test } from "vitest";
import {
  createWorld,
  dropTenantPartitions,
  ensureTenantPartitions,
  runMigrations,
} from "./index.js";

/**
 * The in-band writer fence spec 9 (single-orchestrator) runs need
 * (`capabilities.inBandFence`), checked with upstream's own conformance
 * module: a per-run in-band count beside the slot counter, a `snapshot` on
 * `events.list`, a stale in-band write refused with `InBandSupersededError`
 * before it writes anything, out-of-band writes that never move the count,
 * one winner among concurrent writers holding the same count, and the spec 9
 * step bookkeeping surviving the round trip. Two worlds over the same
 * database share one fence, because the count lives in Postgres.
 */
const testUrl = process.env.EVELAND_WORKFLOW_WORLD_TEST_URL;
const TENANT = "prj_in_band_fence";

type World = ReturnType<typeof createWorld>;

let pool: Pool | undefined;
let world: World | undefined;
let secondWorld: World | undefined;

describe.skipIf(!testUrl)("in-band writer fence", () => {
  beforeAll(async () => {
    pool = new Pool({ connectionString: testUrl, max: 12 });
    await runMigrations(pool);
    await ensureTenantPartitions(pool, TENANT);
    const options = {
      pool,
      tenantId: TENANT,
      deploymentId: "dep_fence",
      runner: "external" as const,
    };
    world = createWorld(options);
    secondWorld = createWorld(options);
  }, 60_000);

  afterAll(async () => {
    await world?.close?.();
    await secondWorld?.close?.();
    if (pool) await dropTenantPartitions(pool, TENANT).catch(() => {});
    await pool?.end();
  });

  test("a World that declares the fence mints spec 9 runs when asked to", async () => {
    const previous = process.env.WORKFLOW_SPEC_VERSION;
    process.env.WORKFLOW_SPEC_VERSION = "9";
    try {
      const asked = createWorld({
        pool: pool!,
        tenantId: TENANT,
        deploymentId: "dep_fence",
        runner: "external",
      });
      expect(asked.specVersion).toBe(9);
      await asked.close?.();
    } finally {
      if (previous === undefined) delete process.env.WORKFLOW_SPEC_VERSION;
      else process.env.WORKFLOW_SPEC_VERSION = previous;
    }
  });

  inBandFenceConformance({
    name: "@evelandhq/workflow-world",
    events: () => world!.events,
    capabilities: () => world!.capabilities,
    newRunId: () => null,
    atRunCreation: 1,
    secondEvents: () => secondWorld!.events,
  });
});
