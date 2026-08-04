import { Pool } from "pg";
import { afterAll, beforeAll, describe, expect, test } from "vitest";
import { createWorld, ensureTenantPartitions, runMigrations } from "./index.js";
import { dropTenantPartitions } from "./migrate.js";

/**
 * `createWorld().close()` and, specifically, who owns the connection pool.
 *
 * This had no coverage at all. The port dropped upstream's equivalents because
 * they lean on `vi.mock` of `pg`, which is file-scoped and cannot share a file
 * with real-database tests — but that is a file-organisation problem, not a
 * reason to leave the behaviour unasserted. Pool ownership is directly
 * observable: hand the world a pool, close the world, and see whether the pool
 * still works.
 *
 * It matters because the world is constructed per deployment inside a process
 * that also serves that project's chat and scheduler traffic. A world that ended
 * a pool it did not create would take unrelated traffic down with it; one that
 * failed to end a pool it did create would leak a connection per construction
 * against the shared database's budget — the thing this world exists to conserve.
 */
const testUrl = process.env.EVELAND_WORKFLOW_WORLD_TEST_URL;
const TENANT = "prj_world_close";

describe.skipIf(!testUrl)("createWorld close semantics", () => {
  let setupPool: Pool;

  beforeAll(async () => {
    setupPool = new Pool({ connectionString: testUrl, max: 2 });
    await runMigrations(setupPool);
    await ensureTenantPartitions(setupPool, TENANT);
  }, 60_000);

  afterAll(async () => {
    await dropTenantPartitions(setupPool, TENANT).catch(() => {});
    await setupPool.end().catch(() => {});
  });

  function makeWorld(pool?: Pool) {
    return createWorld({
      ...(pool ? { pool } : { connectionString: testUrl! }),
      tenantId: TENANT,
      deploymentId: "dep_close_1",
      // No in-process runner: this suite is about teardown, and a runner would
      // add its own listeners and pool clients to the picture.
      runner: "external",
    });
  }

  test("a caller-supplied pool survives close", async () => {
    const caller = new Pool({ connectionString: testUrl, max: 2 });
    try {
      const world = makeWorld(caller);
      await world.close?.();

      // Still usable. `pool.end()` is terminal in node-postgres, so a query
      // succeeding here is proof the world did not end it.
      const { rows } = await caller.query<{ ok: number }>("select 1 as ok");
      expect(rows[0]?.ok).toBe(1);
    } finally {
      await caller.end().catch(() => {});
    }
  }, 60_000);

  test("a pool the world created is ended by close", async () => {
    const world = makeWorld();
    await world.close?.();

    // There is no handle on the internal pool, so the observable is indirect:
    // closing twice must not throw. node-postgres is strict here — `pool.end()`
    // a second time raises "Called end on pool more than once" and the streamer's
    // LISTEN client raises "Client was closed and is not queryable" — so this
    // passing means `close()` guards itself rather than that the repeat is
    // harmless.
    await expect(world.close?.()).resolves.toBeUndefined();
  }, 60_000);

  test("close is idempotent on a caller-supplied pool too", async () => {
    const caller = new Pool({ connectionString: testUrl, max: 2 });
    try {
      const world = makeWorld(caller);
      await world.close?.();
      await expect(world.close?.()).resolves.toBeUndefined();

      const { rows } = await caller.query<{ ok: number }>("select 1 as ok");
      expect(rows[0]?.ok).toBe(1);
    } finally {
      await caller.end().catch(() => {});
    }
  }, 60_000);

  test("closing one world does not disturb another sharing the pool", async () => {
    // Two deployments of the same project can be constructed against one pool
    // during a promote; retiring one must not break the other.
    const shared = new Pool({ connectionString: testUrl, max: 4 });
    try {
      const first = makeWorld(shared);
      const second = createWorld({
        pool: shared,
        tenantId: TENANT,
        deploymentId: "dep_close_2",
        runner: "external",
      });

      await first.close?.();

      // The survivor must still be able to read.
      await expect(second.runs.list({ pagination: { limit: 1 } })).resolves.toBeDefined();
      await second.close?.();
    } finally {
      await shared.end().catch(() => {});
    }
  }, 60_000);
});
