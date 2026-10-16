import { sql } from "drizzle-orm";
import { makeWorkerUtils, type WorkerUtils } from "graphile-worker";
import { Pool } from "pg";
import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";
import { deliveryInFlight } from "./delivery-liveness.js";
import { createClient, type Drizzle } from "./drizzle/index.js";

/**
 * Against a real graphile schema, because the read is graphile's private job
 * table and a savepoint inside the caller's transaction.
 *
 * Set `EVELAND_WORKFLOW_WORLD_TEST_URL` to a scratch database to run it.
 */
const testUrl = process.env.EVELAND_WORKFLOW_WORLD_TEST_URL;
const MESSAGE = "msg_liveness_probe";

describe.skipIf(!testUrl)("deliveryInFlight", () => {
  let pool: Pool;
  let drizzle: Drizzle;
  let workerUtils: WorkerUtils;

  beforeAll(async () => {
    pool = new Pool({ connectionString: testUrl, max: 3 });
    drizzle = createClient(pool);
    workerUtils = await makeWorkerUtils({ pgPool: pool });
    await workerUtils.migrate();
  }, 60_000);

  beforeEach(async () => {
    await pool.query("delete from graphile_worker._private_jobs where payload->>'messageId' = $1", [
      MESSAGE,
    ]);
  });

  afterAll(async () => {
    await pool?.query(
      "delete from graphile_worker._private_jobs where payload->>'messageId' = $1",
      [MESSAGE],
    );
    await workerUtils?.release();
    await pool?.end();
  });

  async function addJob(lockedAt: string | null): Promise<void> {
    await workerUtils.addJob("wfw_liveness_probe", { messageId: MESSAGE });
    if (lockedAt) {
      await pool.query(
        `update graphile_worker._private_jobs
            set locked_at = ${lockedAt}, locked_by = 'worker_probe'
          where payload->>'messageId' = $1`,
        [MESSAGE],
      );
    }
  }

  it("answers yes while a worker holds the delivery's job", async () => {
    await addJob("now()");
    await expect(deliveryInFlight(drizzle, MESSAGE)).resolves.toBe(true);
  });

  it("answers no for a job no worker holds", async () => {
    await addJob(null);
    await expect(deliveryInFlight(drizzle, MESSAGE)).resolves.toBe(false);
  });

  it("answers no for a lock past graphile's four-hour stale rule", async () => {
    await addJob("now() - interval '5 hours'");
    await expect(deliveryInFlight(drizzle, MESSAGE)).resolves.toBe(false);
  });

  it("leaves the caller's transaction usable when the read fails", async () => {
    const outcome = drizzle.transaction(async (tx) => {
      // Renamed inside this transaction only; the rollback below restores it.
      await tx.execute(
        sql`alter table graphile_worker._private_jobs rename to _private_jobs_hidden`,
      );
      const inFlight = await deliveryInFlight(tx, MESSAGE);
      const { rows } = await tx.execute(sql`select 1 as ok`);
      throw Object.assign(new Error("rollback"), { inFlight, rows });
    });

    await expect(outcome).rejects.toMatchObject({ inFlight: false, rows: [{ ok: 1 }] });
  });
});
