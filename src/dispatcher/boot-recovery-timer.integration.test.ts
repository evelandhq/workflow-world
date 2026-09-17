import { getQueueTopicPrefix, type ValidQueueName } from "@workflow/world";
import { makeWorkerUtils, type WorkerUtils } from "graphile-worker";
import { Pool } from "pg";
import { afterAll, beforeAll, describe, expect, test } from "vitest";
import { createWorld, ensureTenantPartitions, runMigrations } from "../index.js";
import { dropTenantPartitions } from "../migrate.js";
import { reenqueueActiveRunsForAllTenants } from "./boot-recovery.js";

/**
 * A run parked on a timer is recovered even while it holds hooks.
 *
 * Boot recovery skips hook-holding runs because a hook is normally the run's
 * other wake-up (a session on its inbox). From eve 0.57 the durable sleep tool
 * is a child run that holds an abort hook and its callback hook while it
 * sleeps, and its only driver is the delayed job. If that job is lost, the
 * `waiting` wait with a `resume_at` is what says "this run is inside a sleep";
 * recovery must replay it rather than wait for hooks nobody else resolves
 * (evelandhq/workflow-world#89).
 *
 * Set `EVELAND_WORKFLOW_WORLD_TEST_URL` to a scratch database to run it.
 */
const testUrl = process.env.EVELAND_WORKFLOW_WORLD_TEST_URL;
const suffix = `${String(process.pid)}${Date.now().toString(36)}`;
const TENANT = `p_boottimer_${suffix}`;

describe.skipIf(!testUrl)("boot recovery replays a hook-holding run parked on a timer", () => {
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

  /** An active run with no job left to wake it, holding one hook. */
  async function createHookedRun(label: string): Promise<{
    runId: string;
    world: ReturnType<typeof createWorld>;
  }> {
    const world = createWorld({
      connectionString: testUrl!,
      tenantId: TENANT,
      deploymentId: "dep_sleeper",
      runner: "external",
    });
    worlds.push(world);
    const created = await world.events.create(null, {
      eventType: "run_created",
      eventData: { deploymentId: "dep_sleeper", workflowName: "sleeper", input: [] },
      specVersion: 5,
    });
    const runId = created.run!.runId;
    await world.events.create(runId, {
      eventType: "hook_created",
      correlationId: `hook_${label}_${suffix}`,
      eventData: { token: `abrt_${label}_${suffix}` },
    } as Parameters<typeof world.events.create>[1]);
    await world.queue(`${getQueueTopicPrefix("workflow")}sleeper` as ValidQueueName, { runId });
    await admin.query("delete from graphile_worker._private_jobs where payload->>'tenantId' = $1", [
      TENANT,
    ]);
    return { runId, world };
  }

  async function recovered(runId: string): Promise<boolean> {
    const { rows } = await admin.query(
      "select 1 from graphile_worker._private_jobs where payload->>'messageId' = $1",
      [`msg_recover_${runId}`],
    );
    return rows.length > 0;
  }

  test("a lost sleep timer is re-enqueued; a plain hook is still left alone", async () => {
    const sleeping = await createHookedRun("sleeping");
    await sleeping.world.events.create(sleeping.runId, {
      eventType: "wait_created",
      correlationId: `wait_${suffix}`,
      eventData: { resumeAt: new Date(Date.now() + 60_000) },
    } as Parameters<typeof sleeping.world.events.create>[1]);

    const woken = await createHookedRun("woken");
    await woken.world.events.create(woken.runId, {
      eventType: "wait_created",
      correlationId: `wait_done_${suffix}`,
      eventData: { resumeAt: new Date(Date.now() - 1_000) },
    } as Parameters<typeof woken.world.events.create>[1]);
    await woken.world.events.create(woken.runId, {
      eventType: "wait_completed",
      correlationId: `wait_done_${suffix}`,
      eventData: {},
    } as Parameters<typeof woken.world.events.create>[1]);

    const parked = await createHookedRun("parked");

    const log: Array<[string, unknown]> = [];
    await reenqueueActiveRunsForAllTenants({
      pool: admin,
      workerUtils,
      filterRuns: (runs) => runs.filter((run) => run.tenantId === TENANT),
      log: (message, attributes) => log.push([message, attributes]),
    });

    // Inside a sleep: the hooks are its own, the timer is its only driver.
    expect(await recovered(sleeping.runId)).toBe(true);
    // The sleep already ended: nothing says a timer is owed, the hook decides.
    expect(await recovered(woken.runId)).toBe(false);
    // Never slept: a session on its inbox, exactly what the hook rule protects.
    expect(await recovered(parked.runId)).toBe(false);
    expect(log).toContainEqual([
      "recovering hook-holding runs whose sleep timer was lost",
      { runs: 1 },
    ]);
  }, 60_000);
});
