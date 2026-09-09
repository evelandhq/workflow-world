import { getQueueTopicPrefix, type ValidQueueName } from "@workflow/world";
import { makeWorkerUtils, type WorkerUtils } from "graphile-worker";
import { Pool } from "pg";
import { afterAll, beforeAll, describe, expect, test } from "vitest";
import { createWorld, ensureTenantPartitions, runMigrations } from "../index.js";
import { dropTenantPartitions } from "../migrate.js";
import { reenqueueActiveRunsForAllTenants } from "./boot-recovery.js";

/**
 * What boot recovery leaves alone, and how it paces what it does write.
 *
 * The observed failure: a dispatcher restart re-enqueued every active run —
 * 172 of them across 20 deployments — and each enqueue cost one cold-start
 * activation, so the host was asked for 20 cold starts in the same second. A
 * run whose own job is still queued needs no recovery job at all, and the
 * ones that do need one are released a few deployments at a time.
 *
 * Set `EVELAND_WORKFLOW_WORLD_TEST_URL` to a scratch database to run them.
 */
const testUrl = process.env.EVELAND_WORKFLOW_WORLD_TEST_URL;
const suffix = `${String(process.pid)}${Date.now().toString(36)}`;
const TENANT = `p_bootpace_${suffix}`;

describe.skipIf(!testUrl)("boot recovery skips live jobs and paces by deployment", () => {
  let admin: Pool;
  let workerUtils: WorkerUtils;
  const worlds = new Map<string, ReturnType<typeof createWorld>>();

  beforeAll(async () => {
    admin = new Pool({ connectionString: testUrl, max: 4 });
    await runMigrations(admin);
    await ensureTenantPartitions(admin, TENANT);
    workerUtils = await makeWorkerUtils({ pgPool: admin });
    await workerUtils.migrate();
  }, 60_000);

  afterAll(async () => {
    await Promise.all([...worlds.values()].map(async (world) => await world.close?.()));
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

  /** An active run created and enqueued the way a deployment does it. */
  async function createActiveRun(deploymentId: string): Promise<string> {
    let world = worlds.get(deploymentId);
    if (!world) {
      world = createWorld({
        connectionString: testUrl!,
        tenantId: TENANT,
        deploymentId,
        runner: "external",
      });
      worlds.set(deploymentId, world);
    }
    const created = await world.events.create(null, {
      eventType: "run_created",
      eventData: { deploymentId, workflowName: "greet", input: [] },
      specVersion: 5,
    });
    const runId = created.run!.runId;
    await world.queue(`${getQueueTopicPrefix("workflow")}greet` as ValidQueueName, { runId });
    return runId;
  }

  async function dropOwnJob(runId: string): Promise<void> {
    await admin.query(
      `delete from graphile_worker._private_jobs
        where payload->>'tenantId' = $1
          and payload->>'messageId' not like 'msg_recover_%'
          and job_queue_id = (
            select id from graphile_worker._private_job_queues where queue_name = $2
          )`,
      [TENANT, `wfrun:${TENANT}:${runId}`],
    );
  }

  async function recoveryJob(runId: string): Promise<{ run_at: Date } | undefined> {
    const { rows } = await admin.query<{ run_at: Date }>(
      "select run_at from graphile_worker._private_jobs where payload->>'messageId' = $1",
      [`msg_recover_${runId}`],
    );
    return rows[0];
  }

  test("a run whose own job is still queued is not re-enqueued", async () => {
    const pending = await createActiveRun("dep_pace_live");
    const sleeping = await createActiveRun("dep_pace_live");
    await admin.query(
      `update graphile_worker._private_jobs set run_at = now() + interval '1 hour'
        where job_queue_id = (
          select id from graphile_worker._private_job_queues where queue_name = $1
        )`,
      [`wfrun:${TENANT}:${sleeping}`],
    );
    const orphaned = await createActiveRun("dep_pace_live");
    await dropOwnJob(orphaned);

    const logged: Array<{ message: string; meta?: Record<string, unknown> }> = [];
    await reenqueueActiveRunsForAllTenants({
      pool: admin,
      workerUtils,
      log: (message, meta) => logged.push({ message, ...(meta ? { meta } : {}) }),
    });

    // The pending first delivery and the sleep's timer are the runs' own
    // wake-ups; only the run with nothing left gets a recovery job.
    expect(await recoveryJob(pending)).toBeUndefined();
    expect(await recoveryJob(sleeping)).toBeUndefined();
    expect(await recoveryJob(orphaned)).toBeDefined();
    const skipped = logged.find((entry) => /still queued/.test(entry.message));
    expect(skipped?.meta?.runs).toBeGreaterThanOrEqual(2);
  }, 60_000);

  test("a run parked on a hook is not re-enqueued", async () => {
    // The incident's residue: with live-job runs skipped, every remaining
    // candidate on the host was a session waiting on its inbox hook, and
    // recovering them still woke eighteen deployments. The hook's resolution
    // enqueues the run itself, so boot has nothing to add.
    const parked = await createActiveRun("dep_pace_hook");
    await dropOwnJob(parked);
    const world = worlds.get("dep_pace_hook")!;
    await world.events.create(parked, {
      eventType: "hook_created",
      correlationId: `whk_${suffix}_${parked.slice(-6)}`,
      eventData: { token: `inbox:${parked}` },
      specVersion: 5,
    } as Parameters<typeof world.events.create>[1]);
    const orphaned = await createActiveRun("dep_pace_hook");
    await dropOwnJob(orphaned);

    const logged: Array<{ message: string; meta?: Record<string, unknown> }> = [];
    await reenqueueActiveRunsForAllTenants({
      pool: admin,
      workerUtils,
      log: (message, meta) => logged.push({ message, ...(meta ? { meta } : {}) }),
    });

    expect(await recoveryJob(parked)).toBeUndefined();
    expect(await recoveryJob(orphaned)).toBeDefined();
    const skipped = logged.find((entry) => /parked on a hook/.test(entry.message));
    expect(skipped?.meta?.runs).toBeGreaterThanOrEqual(1);
  }, 60_000);

  test("a job graphile has given up on does not count as live", async () => {
    const exhausted = await createActiveRun("dep_pace_exhausted");
    await admin.query(
      `update graphile_worker._private_jobs set attempts = max_attempts, last_error = 'gave up'
        where job_queue_id = (
          select id from graphile_worker._private_job_queues where queue_name = $1
        )`,
      [`wfrun:${TENANT}:${exhausted}`],
    );

    await reenqueueActiveRunsForAllTenants({ pool: admin, workerUtils });

    expect(await recoveryJob(exhausted)).toBeDefined();
  }, 60_000);

  test("recovered runs are released one deployment wave at a time", async () => {
    const runs = {
      a1: await createActiveRun("dep_pace_a"),
      a2: await createActiveRun("dep_pace_a"),
      b1: await createActiveRun("dep_pace_b"),
      c1: await createActiveRun("dep_pace_c"),
    };
    for (const runId of Object.values(runs)) await dropOwnJob(runId);
    await admin.query(
      "delete from graphile_worker._private_jobs where payload->>'messageId' like 'msg_recover_%'",
    );

    const logged: Array<{ message: string; meta?: Record<string, unknown> }> = [];
    const before = Date.now();
    await reenqueueActiveRunsForAllTenants({
      pool: admin,
      workerUtils,
      pacing: { deploymentsPerWave: 1, waveIntervalMs: 60_000 },
      log: (message, meta) => logged.push({ message, ...(meta ? { meta } : {}) }),
      // Other files leave active runs in the shared database, and the tests
      // above leave this tenant's; keep the wave arithmetic to these three.
      filterRuns: (candidates) =>
        candidates.filter(
          (run) =>
            run.tenantId === TENANT &&
            ["dep_pace_a", "dep_pace_b", "dep_pace_c"].includes(run.deploymentId),
        ),
    });

    const dueAt = async (runId: string) => (await recoveryJob(runId))!.run_at.getTime();
    // Both of a deployment's runs share its wave; the next deployment is a
    // wave later; the first wave is due now.
    expect(await dueAt(runs.a1)).toBe(await dueAt(runs.a2));
    expect(await dueAt(runs.a1)).toBeLessThan(before + 5_000);
    expect((await dueAt(runs.b1)) - (await dueAt(runs.a1))).toBeGreaterThanOrEqual(55_000);
    expect((await dueAt(runs.c1)) - (await dueAt(runs.b1))).toBeGreaterThanOrEqual(55_000);
    const plan = logged.find((entry) => /waves/.test(entry.message));
    expect(plan?.meta).toMatchObject({ deploymentsPerWave: 1, waveIntervalMs: 60_000 });
    expect(plan?.meta?.waves).toBeGreaterThanOrEqual(3);
  }, 60_000);
});
