import { makeWorkerUtils, type WorkerUtils } from "graphile-worker";
import { Pool } from "pg";
import { afterAll, beforeAll, describe, expect, test } from "vitest";
import { createClient } from "../drizzle/index.js";
import { ensureTenantPartitions, runMigrations } from "../index.js";
import { dropTenantPartitions } from "../migrate.js";
import { createEventsStorage } from "../storage.js";
import { reenqueueActiveRunsForAllTenants, type BootRecoveryCandidate } from "./boot-recovery.js";

/**
 * The host's veto over boot recovery (issue #57).
 *
 * Boot recovery re-enqueues every active run it can see, and for a run bound
 * to a Deployment the host knows can never activate, that replay is a
 * guaranteed dead letter — once per run, per dispatcher restart. The
 * `shouldRecoverRun` filter lets the host skip those candidates; a filter
 * error fails open, because re-enqueueing is the safe default.
 *
 * Set `EVELAND_WORKFLOW_WORLD_TEST_URL` to a scratch database to run it.
 */
const testUrl = process.env.EVELAND_WORKFLOW_WORLD_TEST_URL;
const suffix = `${String(process.pid)}${Date.now().toString(36)}`;
const TENANT = `p_bootfilter_${suffix}`;

describe.skipIf(!testUrl)("boot recovery honours the host's run filter", () => {
  let pool: Pool;
  let workerUtils: WorkerUtils;
  let healthyRun: string;
  let doomedRun: string;

  beforeAll(async () => {
    pool = new Pool({ connectionString: testUrl, max: 4 });
    await runMigrations(pool);
    await ensureTenantPartitions(pool, TENANT);
    workerUtils = await makeWorkerUtils({ pgPool: pool });
    await workerUtils.migrate();

    const events = createEventsStorage(createClient(pool), TENANT);
    const create = async (deploymentId: string) => {
      const created = await events.create(null, {
        eventType: "run_created",
        eventData: {
          deploymentId,
          workflowName: "filter-test-workflow",
          input: new Uint8Array(),
        },
      });
      return created.run!.runId;
    };
    healthyRun = await create("dep_filter_healthy");
    doomedRun = await create("dep_filter_doomed");
  }, 60_000);

  afterAll(async () => {
    await pool
      .query("delete from graphile_worker._private_jobs where payload->>'messageId' like $1", [
        "msg_recover_%",
      ])
      .catch(() => {});
    await pool
      .query("delete from workflow.workflow_runs where tenant_id = $1", [TENANT])
      .catch(() => {});
    await dropTenantPartitions(pool, TENANT).catch(() => {});
    await workerUtils?.release();
    await pool?.end().catch(() => {});
  });

  async function recoveryJobExists(runId: string): Promise<boolean> {
    const { rows } = await pool.query(
      "select 1 from graphile_worker._private_jobs where payload->>'messageId' = $1",
      [`msg_recover_${runId}`],
    );
    return rows.length > 0;
  }

  async function clearRecoveryJobs(): Promise<void> {
    await pool.query(
      "delete from graphile_worker._private_jobs where payload->>'messageId' like $1",
      ["msg_recover_%"],
    );
  }

  test("a vetoed run is skipped and everything else still recovers", async () => {
    const seen: BootRecoveryCandidate[] = [];
    await reenqueueActiveRunsForAllTenants({
      pool,
      workerUtils,
      shouldRecoverRun: (candidate) => {
        if (candidate.tenantId === TENANT) seen.push(candidate);
        return candidate.deploymentId !== "dep_filter_doomed";
      },
    });

    expect(await recoveryJobExists(healthyRun)).toBe(true);
    expect(await recoveryJobExists(doomedRun)).toBe(false);
    expect(seen.find((candidate) => candidate.runId === doomedRun)).toMatchObject({
      tenantId: TENANT,
      deploymentId: "dep_filter_doomed",
    });
  });

  test("skipping is per-sweep: an unfiltered boot recovers the run after all", async () => {
    await clearRecoveryJobs();
    await reenqueueActiveRunsForAllTenants({ pool, workerUtils });
    expect(await recoveryJobExists(doomedRun)).toBe(true);
  });

  test("a throwing filter fails open", async () => {
    await clearRecoveryJobs();
    const logged: string[] = [];
    await reenqueueActiveRunsForAllTenants({
      pool,
      workerUtils,
      log: (message) => logged.push(message),
      shouldRecoverRun: () => {
        throw new Error("host predicate exploded");
      },
    });
    expect(await recoveryJobExists(healthyRun)).toBe(true);
    expect(await recoveryJobExists(doomedRun)).toBe(true);
    expect(logged).toContain("boot recovery filter failed; recovering the run anyway");
  });
});
