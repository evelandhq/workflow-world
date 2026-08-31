import { getQueueTopicPrefix, type ValidQueueName } from "@workflow/world";
import { makeWorkerUtils, type WorkerUtils } from "graphile-worker";
import { Pool } from "pg";
import { afterAll, beforeAll, describe, expect, test } from "vitest";
import { createWorld, ensureTenantPartitions, runMigrations } from "../index.js";
import { dropTenantPartitions } from "../migrate.js";
import { reenqueueActiveRunsForAllTenants, type BootRecoveryRun } from "./boot-recovery.js";

/**
 * The host's say over what boot recovery replays.
 *
 * Without the filter, every dispatcher boot re-enqueues every active run — the
 * observed failure mode being ~250 orphaned runs bound to dead Deployments,
 * replayed on every cold start. The host knows which Deployments are
 * activatable; the sweep does not. The seam is a single callback over the full
 * candidate list, and a skipped run must stay exactly as it was: still active,
 * offered again next boot.
 *
 * Set `EVELAND_WORKFLOW_WORLD_TEST_URL` to a scratch database to run them.
 */
const testUrl = process.env.EVELAND_WORKFLOW_WORLD_TEST_URL;
const suffix = `${String(process.pid)}${Date.now().toString(36)}`;
const TENANT = `p_bootfilter_${suffix}`;

describe.skipIf(!testUrl)("boot recovery honours the host's run filter", () => {
  let admin: Pool;
  let workerUtils: WorkerUtils;
  const worlds: Array<ReturnType<typeof createWorld>> = [];

  beforeAll(async () => {
    admin = new Pool({ connectionString: testUrl, max: 4 });
    await runMigrations(admin);
    await ensureTenantPartitions(admin, TENANT);
    workerUtils = await makeWorkerUtils({ pgPool: admin });
    await workerUtils.migrate();
  }, 60_000);

  afterAll(async () => {
    await Promise.all(worlds.map(async (world) => await world.close?.()));
    await workerUtils?.release();
    await admin
      .query("delete from graphile_worker._private_jobs where payload->>'tenantId' = $1", [TENANT])
      .catch(() => {});
    await admin
      .query("delete from graphile_worker._private_jobs where payload->>'messageId' like $1", [
        "msg_recover_%",
      ])
      .catch(() => {});
    await admin
      .query("delete from workflow.workflow_runs where tenant_id = $1", [TENANT])
      .catch(() => {});
    await dropTenantPartitions(admin, TENANT).catch(() => {});
    await admin?.end().catch(() => {});
  });

  /** An active run with no job left to wake it — boot recovery's whole case. */
  async function createActiveRun(deploymentId: string, workflowName: string): Promise<string> {
    const world = createWorld({
      connectionString: testUrl!,
      tenantId: TENANT,
      deploymentId,
      runner: "external",
    });
    worlds.push(world);
    const created = await world.events.create(null, {
      eventType: "run_created",
      eventData: { deploymentId, workflowName, input: [] },
      specVersion: 5,
    });
    const runId = created.run!.runId;
    await world.queue(`${getQueueTopicPrefix("workflow")}${workflowName}` as ValidQueueName, {
      runId,
    });
    await admin.query("delete from graphile_worker._private_jobs where payload->>'tenantId' = $1", [
      TENANT,
    ]);
    return runId;
  }

  async function recovered(runId: string): Promise<boolean> {
    const { rows } = await admin.query(
      "select 1 from graphile_worker._private_jobs where payload->>'messageId' = $1",
      [`msg_recover_${runId}`],
    );
    return rows.length > 0;
  }

  test("only runs the filter returns are re-enqueued, and skipping is not settling", async () => {
    const live = await createActiveRun("dep_live", "greet");
    const dead = await createActiveRun("dep_retired", "greet");

    const offered: BootRecoveryRun[] = [];
    const enqueued = await reenqueueActiveRunsForAllTenants({
      pool: admin,
      workerUtils,
      filterRuns: (runs) => {
        offered.push(...runs.filter((run) => run.tenantId === TENANT));
        return runs.filter((run) => run.deploymentId !== "dep_retired");
      },
    });

    // The filter saw the full candidate rows, payload-free but identifying.
    expect(offered.map((run) => run.runId).sort()).toEqual([live, dead].sort());
    for (const run of offered) {
      expect(run).toMatchObject({ tenantId: TENANT, workflowName: "greet" });
    }
    expect(enqueued).toBeGreaterThanOrEqual(1);
    expect(await recovered(live)).toBe(true);
    expect(await recovered(dead)).toBe(false);

    // The skipped run is still active and offered again on the next sweep: the
    // filter defers replay, it does not settle anything.
    const { rows } = await admin.query<{ status: string }>(
      "select status from workflow.workflow_runs where tenant_id = $1 and id = $2",
      [TENANT, dead],
    );
    expect(rows[0]!.status).toBe("pending");
    const secondOffer: string[] = [];
    await reenqueueActiveRunsForAllTenants({
      pool: admin,
      workerUtils,
      filterRuns: (runs) => {
        secondOffer.push(...runs.filter((run) => run.tenantId === TENANT).map((run) => run.runId));
        return runs;
      },
    });
    expect(secondOffer).toContain(dead);
    expect(await recovered(dead)).toBe(true);
  }, 60_000);

  test("entries the sweep never offered are ignored, not enqueued", async () => {
    const runId = await createActiveRun("dep_live", "greet");
    await admin.query(
      "delete from graphile_worker._private_jobs where payload->>'messageId' like 'msg_recover_%'",
    );

    await reenqueueActiveRunsForAllTenants({
      pool: admin,
      workerUtils,
      filterRuns: (runs) => [
        ...runs.filter((run) => run.tenantId === TENANT && run.runId === runId),
        // A fabricated entry must not conjure a job for a run that was never
        // a candidate.
        {
          tenantId: TENANT,
          runId: "wrun_fabricated",
          workflowName: "greet",
          deploymentId: "dep_live",
          queueNamespace: null,
        },
      ],
    });

    expect(await recovered(runId)).toBe(true);
    expect(await recovered("wrun_fabricated")).toBe(false);
  }, 60_000);
});
