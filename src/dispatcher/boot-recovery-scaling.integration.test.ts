import { makeWorkerUtils, type WorkerUtils } from "graphile-worker";
import { Pool } from "pg";
import { afterAll, beforeAll, describe, expect, test } from "vitest";
import { runQueueName, TIMER_JOB_NAME } from "../dispatch-contract.js";
import { dropTenantPartitions, ensureTenantPartitions, runMigrations } from "../migrate.js";
import { registerTimerTask } from "../timers.js";
import {
  reclaimAndReenqueueActiveRunsForAllTenants,
  reenqueueActiveRunsForAllTenants,
} from "./boot-recovery.js";

/**
 * Boot recovery with held timers, at the scale the fix exists for.
 *
 * A timer has no queue row, so the sweep finds it through the queue name on its
 * payload. Done as a correlated lookup that is one read of the jobs table per
 * active run — with every live session holding one timer, quadratic in exactly
 * the number #115 is about. Measured as plan work rather than wall-clock time,
 * which a warm cache on a fast machine hides. (Adapted from
 * evelandhq/workflow-world#116.)
 *
 * Set `EVELAND_WORKFLOW_WORLD_TEST_URL` to a scratch database to run it.
 */
const testUrl = process.env.EVELAND_WORKFLOW_WORLD_TEST_URL;
const suffix = `${String(process.pid)}${Date.now().toString(36)}`;
const TENANT = `p_recscale_${suffix}`;
const OTHER_TENANT = `p_recscale_other_${suffix}`;
const RUNS = 10_000;

type PlanNode = {
  "Node Type"?: string;
  "Relation Name"?: string;
  "Actual Loops"?: number;
  Plans?: PlanNode[];
};

describe.skipIf(!testUrl)("boot recovery with held timers", () => {
  let pool: Pool;
  let workerUtils: WorkerUtils;

  beforeAll(async () => {
    pool = new Pool({ connectionString: testUrl, max: 4 });
    await runMigrations(pool);
    await ensureTenantPartitions(pool, TENANT);
    await ensureTenantPartitions(pool, OTHER_TENANT);
    workerUtils = await makeWorkerUtils({ pgPool: pool });
    await workerUtils.migrate();
    await registerTimerTask(pool);
  }, 60_000);

  afterAll(async () => {
    await pool
      ?.query("delete from graphile_worker._private_jobs where payload->>'tenantId' = any($1)", [
        [TENANT, OTHER_TENANT],
      ])
      .catch(() => {});
    await pool
      ?.query("delete from workflow.workflow_runs where tenant_id = any($1)", [
        [TENANT, OTHER_TENANT],
      ])
      .catch(() => {});
    await dropTenantPartitions(pool, TENANT).catch(() => {});
    await dropTenantPartitions(pool, OTHER_TENANT).catch(() => {});
    await workerUtils?.release();
    await pool?.end();
  });

  async function insertRun(tenantId: string, runId: string): Promise<void> {
    await pool.query(
      `insert into workflow.workflow_runs (tenant_id, id, deployment_id, status, name, spec_version)
       values ($1, $2, 'dep_scale', 'running', 'sleeper', 7)`,
      [tenantId, runId],
    );
  }

  async function addTimer(tenantId: string, runId: string): Promise<string> {
    const job = await workerUtils.addJob(
      TIMER_JOB_NAME,
      { tenantId, runQueue: runQueueName(tenantId, runId) },
      { runAt: new Date(Date.now() + 3_600_000) },
    );
    return job.id;
  }

  test("the jobs table is read once however many runs are asleep", async () => {
    await pool.query(
      `insert into workflow.workflow_runs (tenant_id, id, deployment_id, status, name, spec_version)
       select $1, 'wrun_scale_' || n, 'dep_scale', 'running', 'sleeper', 7
         from generate_series(1, $2::int) n`,
      [TENANT, RUNS],
    );
    await pool.query(
      `insert into graphile_worker._private_jobs (task_id, payload, run_at)
       select tasks.id,
              json_build_object('tenantId', $1::text, 'runQueue', 'wfrun:' || $1 || ':wrun_scale_' || n),
              now() + interval '1 hour'
         from generate_series(1, $2::int) n
        cross join graphile_worker._private_tasks as tasks
        where tasks.identifier = $3`,
      [TENANT, RUNS, TIMER_JOB_NAME],
    );
    await pool.query("analyze graphile_worker._private_jobs");
    await pool.query("analyze workflow.workflow_runs");

    const loops: number[] = [];
    const visit = (node: PlanNode) => {
      // The jobs table, and the timer set built from it, are each read once.
      if (node["Relation Name"] === "_private_jobs" || node["Node Type"] === "CTE Scan") {
        loops.push(node["Actual Loops"] ?? 0);
      }
      for (const child of node.Plans ?? []) visit(child);
    };
    const offered: string[] = [];
    const measured = {
      async query(sql: string, values?: unknown[]) {
        if (/^\s*with timers/.test(sql)) {
          const { rows } = await pool.query(`explain (analyze, format json) ${sql}`, values);
          visit((rows[0]["QUERY PLAN"] as Array<{ Plan: PlanNode }>)[0]!.Plan);
        }
        return pool.query(sql, values);
      },
    } as unknown as Pool;

    await reenqueueActiveRunsForAllTenants({
      pool: measured,
      workerUtils,
      filterRuns: (runs) => {
        offered.push(...runs.filter((run) => run.tenantId === TENANT).map((run) => run.runId));
        return [];
      },
    });

    expect(loops.length).toBeGreaterThan(0);
    expect(Math.max(...loops)).toBeLessThanOrEqual(1);
    // And the answer is still right: every one of them has its timer.
    expect(offered).toEqual([]);

    await pool.query("delete from graphile_worker._private_jobs where payload->>'tenantId' = $1", [
      TENANT,
    ]);
    await pool.query("delete from workflow.workflow_runs where tenant_id = $1", [TENANT]);
  }, 60_000);

  test("every timer lock is reclaimed and timers stay scoped to their tenant", async () => {
    const shared = `wrun_shared_${suffix}`;
    const spent = `wrun_spent_${suffix}`;
    await insertRun(TENANT, shared);
    await insertRun(OTHER_TENANT, shared);
    await insertRun(TENANT, spent);

    // Two timers for one run, each locked by a different dead worker.
    for (const worker of [`worker_a_${suffix}`, `worker_b_${suffix}`]) {
      const id = await addTimer(TENANT, shared);
      await pool.query(
        "update graphile_worker._private_jobs set locked_by = $2, locked_at = now() where id = $1",
        [id, worker],
      );
    }
    // A timer that has spent its attempts delivers nothing.
    const spentId = await addTimer(TENANT, spent);
    await pool.query(
      "update graphile_worker._private_jobs set attempts = max_attempts where id = $1",
      [spentId],
    );

    const recovered = await reclaimAndReenqueueActiveRunsForAllTenants({
      pool,
      workerUtils,
      filterRuns: (runs) => runs.filter((run) => [TENANT, OTHER_TENANT].includes(run.tenantId)),
    });
    expect(recovered).toBe(2);

    const { rows: locks } = await pool.query<{ locked_by: string | null }>(
      "select locked_by from graphile_worker._private_jobs where payload->>'runQueue' = $1",
      [runQueueName(TENANT, shared)],
    );
    expect(locks).toEqual([{ locked_by: null }, { locked_by: null }]);

    // The same run id in another tenant has no timer of its own, and the spent
    // timer does not count: both are recovered. The live timers' run is not.
    const { rows } = await pool.query<{ tenant_id: string; message_id: string }>(
      `select payload->>'tenantId' as tenant_id, payload->>'messageId' as message_id
         from graphile_worker._private_jobs
        where payload->>'tenantId' = any($1) and payload->>'messageId' like 'msg_recover_%'`,
      [[TENANT, OTHER_TENANT]],
    );
    expect(rows).toHaveLength(2);
    expect(rows).toEqual(
      expect.arrayContaining([
        { tenant_id: OTHER_TENANT, message_id: `msg_recover_${shared}` },
        { tenant_id: TENANT, message_id: `msg_recover_${spent}` },
      ]),
    );
  });
});
