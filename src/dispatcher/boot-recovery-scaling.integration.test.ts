import { makeWorkerUtils, type WorkerUtils } from "graphile-worker";
import { Pool } from "pg";
import { afterAll, beforeAll, describe, expect, test } from "vitest";
import { runMigrations } from "../index.js";
import {
  reclaimAndReenqueueActiveRunsForAllTenants,
  reenqueueActiveRunsForAllTenants,
} from "./boot-recovery.js";

const testUrl = process.env.EVELAND_WORKFLOW_WORLD_TEST_URL;
const TENANT = `p_recovery_scale_${process.pid}`;
const OTHER_TENANT = `${TENANT}_other`;

type PlanNode = {
  "Relation Name"?: string;
  "Actual Loops"?: number;
  "CTE Name"?: string;
  "Rows Removed by Join Filter"?: number;
  Plans?: PlanNode[];
};

describe.skipIf(!testUrl)("boot recovery query scaling", () => {
  let pool: Pool;
  let workerUtils: WorkerUtils;

  beforeAll(async () => {
    pool = new Pool({ connectionString: testUrl, max: 4 });
    await runMigrations(pool);
    workerUtils = await makeWorkerUtils({ pgPool: pool });
    await workerUtils.migrate();
  }, 60_000);

  afterAll(async () => {
    await pool?.query(
      "delete from graphile_worker._private_jobs where payload->>'tenantId' = any($1)",
      [[TENANT, OTHER_TENANT]],
    );
    await pool?.query("delete from workflow.workflow_runs where tenant_id = any($1)", [
      [TENANT, OTHER_TENANT],
    ]);
    await workerUtils?.release();
    await pool?.end();
  });

  test("job-table scans stay bounded as the number of sleeping runs grows", async () => {
    await pool.query(
      `insert into workflow.workflow_runs (tenant_id, id, deployment_id, status, name)
       select $1, 'wrun_scale_' || n, 'dep_scale', 'running', 'sleeper'
         from generate_series(1, 10000) n`,
      [TENANT],
    );
    await pool.query(
      "insert into graphile_worker._private_tasks (identifier) values ('recovery_scale') on conflict do nothing",
    );
    await pool.query(
      `insert into graphile_worker._private_jobs (task_id, payload, run_at)
       select tasks.id, json_build_object('tenantId', $1::text, 'runId', 'wrun_scale_' || n), now() + interval '1 hour'
         from generate_series(1, 10000) n cross join graphile_worker._private_tasks tasks
        where tasks.identifier = 'recovery_scale'`,
      [TENANT],
    );
    await pool.query("analyze graphile_worker._private_jobs");
    await pool.query("analyze workflow.workflow_runs");

    const scans: number[] = [];
    const timerJoinComparisons: number[] = [];
    const containsTimers = (node: PlanNode): boolean =>
      node["CTE Name"] === "timer_jobs" || (node.Plans ?? []).some(containsTimers);
    const visit = (node: PlanNode) => {
      if (node["Relation Name"] === "_private_jobs") scans.push(node["Actual Loops"] ?? 0);
      if (containsTimers(node)) timerJoinComparisons.push(node["Rows Removed by Join Filter"] ?? 0);
      for (const child of node.Plans ?? []) visit(child);
    };
    const measuredPool = {
      async query(sql: string) {
        const { rows } = await pool.query(`explain (analyze, format json) ${sql}`);
        visit((rows[0]["QUERY PLAN"] as Array<{ Plan: PlanNode }>)[0]!.Plan);
        return pool.query(sql);
      },
    } as unknown as Pool;
    await reenqueueActiveRunsForAllTenants({
      pool: measuredPool,
      workerUtils,
      filterRuns: () => [],
    });

    // Check the work done, not wall-clock time: a correlated scan revisits all
    // timer jobs once per run even on a fast machine with a warm buffer cache.
    expect(scans.length).toBeGreaterThan(0);
    expect(Math.max(...scans)).toBeLessThanOrEqual(1);
    // A merge on tenant alone still compares every run with every timer in the
    // tenant, even when the underlying jobs table was only scanned once.
    expect(Math.max(0, ...timerJoinComparisons)).toBeLessThanOrEqual(10_000 * 4);
  }, 30_000);

  test("grouped timers retain every worker lock and keep run identity tenant-scoped", async () => {
    const sharedRunId = "wrun_shared_timer";
    const exhaustedRunId = "wrun_exhausted_timer";
    await pool.query(
      `insert into workflow.workflow_runs (tenant_id, id, deployment_id, status, name)
       values ($1, $3, 'dep_scale', 'running', 'sleeper'),
              ($2, $3, 'dep_scale', 'running', 'sleeper'),
              ($1, $4, 'dep_scale', 'running', 'sleeper')`,
      [TENANT, OTHER_TENANT, sharedRunId, exhaustedRunId],
    );
    for (const worker of ["worker_timer_a", "worker_timer_b"]) {
      const job = await workerUtils.addJob("recovery_scale", {
        tenantId: TENANT,
        runId: sharedRunId,
      });
      await pool.query(
        "update graphile_worker._private_jobs set locked_by = $2, locked_at = now(), attempts = 1 where id = $1",
        [job.id, worker],
      );
    }
    const exhausted = await workerUtils.addJob("recovery_scale", {
      tenantId: TENANT,
      runId: exhaustedRunId,
    });
    await pool.query(
      "update graphile_worker._private_jobs set attempts = max_attempts where id = $1",
      [exhausted.id],
    );

    const count = await reclaimAndReenqueueActiveRunsForAllTenants({
      pool,
      workerUtils,
      filterRuns: (runs) => runs.filter((run) => [TENANT, OTHER_TENANT].includes(run.tenantId)),
    });
    expect(count).toBe(2);
    const { rows: timers } = await pool.query<{ locked_by: string | null }>(
      "select locked_by from graphile_worker._private_jobs where payload->>'tenantId' = $1 and payload->>'runId' = $2",
      [TENANT, sharedRunId],
    );
    expect(timers).toEqual([{ locked_by: null }, { locked_by: null }]);
    const { rows: recovered } = await pool.query<{ tenant_id: string; run_id: string }>(
      `select payload->>'tenantId' as tenant_id, payload->>'runId' as run_id
         from graphile_worker._private_jobs where payload->>'tenantId' = any($1)
          and payload->>'messageId' like 'msg_recover_%' order by payload->>'tenantId'`,
      [[TENANT, OTHER_TENANT]],
    );
    expect(recovered).toEqual([
      { tenant_id: TENANT, run_id: exhaustedRunId },
      { tenant_id: OTHER_TENANT, run_id: sharedRunId },
    ]);
  });
});
