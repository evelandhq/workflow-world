import { getQueueTopicPrefix, type ValidQueueName } from "@workflow/world";
import { makeWorkerUtils, type WorkerUtils } from "graphile-worker";
import { Pool } from "pg";
import { afterAll, beforeAll, describe, expect, test } from "vitest";
import { createWorld, ensureTenantPartitions, runMigrations } from "./index.js";
import { reenqueueActiveRunsForAllTenants } from "./dispatcher/boot-recovery.js";
import { dropTenantPartitions } from "./migrate.js";
import {
  isRunQuarantined,
  listUnresolvedRunQuarantines,
  quarantineRun,
  QUARANTINE_PARK_RUN_AT,
  releaseParkedRunJobs,
  resolveRunQuarantine,
} from "./quarantine.js";

/**
 * The durable run quarantine contract: a marker in the World database that
 * boot recovery, the deployment-side enqueue and the dispatch surface all
 * honour, so a fenced run cannot be replayed no matter which path reaches it
 * first. Control-plane state alone can never provide this — the dispatcher
 * reads only this database.
 *
 * Set `EVELAND_WORKFLOW_WORLD_TEST_URL` to a scratch database to run them.
 */
const testUrl = process.env.EVELAND_WORKFLOW_WORLD_TEST_URL;
const suffix = `${String(process.pid)}${Date.now().toString(36)}`;
const QUARANTINED = `p_quar_a_${suffix}`;
const HEALTHY = `p_quar_b_${suffix}`;

describe.skipIf(!testUrl)("durable run quarantine markers", () => {
  let admin: Pool;
  let workerUtils: WorkerUtils;
  const worlds: Array<ReturnType<typeof createWorld>> = [];

  beforeAll(async () => {
    admin = new Pool({ connectionString: testUrl, max: 4 });
    await runMigrations(admin);
    for (const tenant of [QUARANTINED, HEALTHY]) {
      await ensureTenantPartitions(admin, tenant);
    }
    workerUtils = await makeWorkerUtils({ pgPool: admin });
    await workerUtils.migrate();
  }, 60_000);

  afterAll(async () => {
    await Promise.all(worlds.map(async (world) => await world.close?.()));
    await workerUtils?.release();
    await admin
      .query("delete from graphile_worker._private_jobs where payload->>'tenantId' = any($1)", [
        [QUARANTINED, HEALTHY],
      ])
      .catch(() => {});
    await admin
      .query("delete from workflow.workflow_runs where tenant_id = any($1)", [
        [QUARANTINED, HEALTHY],
      ])
      .catch(() => {});
    await admin
      .query("delete from workflow.run_quarantines where tenant_id = any($1)", [
        [QUARANTINED, HEALTHY],
      ])
      .catch(() => {});
    for (const tenant of [QUARANTINED, HEALTHY]) {
      await dropTenantPartitions(admin, tenant).catch(() => {});
    }
    await admin?.end().catch(() => {});
  });

  function tenantWorld(tenantId: string) {
    const world = createWorld({
      connectionString: testUrl!,
      tenantId,
      deploymentId: `dep_${tenantId}`,
      runner: "external",
    });
    worlds.push(world);
    return world;
  }

  /** A run created through the real event path, with its original delivery kept. */
  async function createActiveRun(
    world: ReturnType<typeof createWorld>,
    tenantId: string,
    workflowName: string,
  ): Promise<string> {
    const created = await world.events.create(null, {
      eventType: "run_created",
      eventData: {
        deploymentId: `dep_${tenantId}`,
        workflowName,
        input: [],
      },
      specVersion: 5,
    });
    const runId = created.run!.runId;
    await world.queue(`${getQueueTopicPrefix("workflow")}${workflowName}` as ValidQueueName, {
      runId,
    });
    return runId;
  }

  async function runnableJobCount(tenantId: string): Promise<number> {
    const { rows } = await admin.query<{ count: string }>(
      `select count(*)::text as count
         from graphile_worker._private_jobs
        where payload->>'tenantId' = $1
          and run_at < $2`,
      [tenantId, QUARANTINE_PARK_RUN_AT],
    );
    return Number(rows[0]!.count);
  }

  test("quarantine parks existing jobs, blocks boot recovery and new enqueues; resolution reopens them", async () => {
    const world = tenantWorld(QUARANTINED);
    const healthyWorld = tenantWorld(HEALTHY);
    const runId = await createActiveRun(world, QUARANTINED, "greet");
    const healthyRunId = await createActiveRun(healthyWorld, HEALTHY, "greet");

    // The original delivery is runnable before the marker exists.
    expect(await runnableJobCount(QUARANTINED)).toBeGreaterThan(0);

    await quarantineRun(admin, workerUtils, {
      tenantId: QUARANTINED,
      runId,
      operationId: "cut_op_test",
      reason: "corrupted event log (test)",
    });

    expect(await isRunQuarantined(admin, QUARANTINED, runId)).toBe(true);
    expect(await listUnresolvedRunQuarantines(admin, QUARANTINED)).toMatchObject([
      { runId, operationId: "cut_op_test" },
    ]);

    // Existing jobs are parked with payload intact, not deleted.
    expect(await runnableJobCount(QUARANTINED)).toBe(0);
    const parked = await admin.query<{ payload: { tenantId?: string } }>(
      "select payload from graphile_worker._private_jobs where payload->>'tenantId' = $1",
      [QUARANTINED],
    );
    expect(parked.rows.length).toBeGreaterThan(0);

    // Boot recovery must not create a synthetic job for the quarantined run —
    // while the healthy tenant's run still recovers.
    await reenqueueActiveRunsForAllTenants({ pool: admin, workerUtils });
    const recovered = await admin.query(
      "select 1 from graphile_worker._private_jobs where payload->>'messageId' = $1",
      [`msg_recover_${runId}`],
    );
    expect(recovered.rows).toHaveLength(0);
    const healthyRecovered = await admin.query(
      "select 1 from graphile_worker._private_jobs where payload->>'messageId' = $1",
      [`msg_recover_${healthyRunId}`],
    );
    expect(healthyRecovered.rows).toHaveLength(1);

    // Deployment-side enqueue fails closed even if the owner is re-activated.
    await expect(
      world.queue(`${getQueueTopicPrefix("workflow")}greet` as ValidQueueName, { runId }),
    ).rejects.toThrow(/quarantine/i);

    // Releasing parked jobs is refused while the marker stands.
    await expect(releaseParkedRunJobs(admin, workerUtils, QUARANTINED, runId)).rejects.toThrow(
      /unresolved quarantine/i,
    );

    // Explicit resolution — never a restart — reopens the run.
    expect(
      await resolveRunQuarantine(admin, {
        tenantId: QUARANTINED,
        runId,
        resolvedBy: "operator-test",
      }),
    ).toBe(true);
    expect(await isRunQuarantined(admin, QUARANTINED, runId)).toBe(false);
    const released = await releaseParkedRunJobs(admin, workerUtils, QUARANTINED, runId);
    expect(released).toBeGreaterThan(0);
    expect(await runnableJobCount(QUARANTINED)).toBeGreaterThan(0);

    await reenqueueActiveRunsForAllTenants({ pool: admin, workerUtils });
    const recoveredAfterResolve = await admin.query(
      "select 1 from graphile_worker._private_jobs where payload->>'messageId' = $1",
      [`msg_recover_${runId}`],
    );
    expect(recoveredAfterResolve.rows).toHaveLength(1);
  }, 60_000);
});
