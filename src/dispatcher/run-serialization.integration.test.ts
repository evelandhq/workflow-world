import { makeWorkerUtils, run, type Runner, type WorkerUtils } from "graphile-worker";
import { Pool } from "pg";
import { afterAll, beforeAll, describe, expect, test } from "vitest";
import { runQueueName } from "../dispatch-contract.js";

/**
 * The load-bearing assumption behind the fix for per-run serialization: graphile
 * runs jobs that share a `queueName` strictly one at a time, even at high
 * concurrency, and jobs in *different* queues still run in parallel.
 *
 * Asserted against a real database rather than taken from the documentation,
 * because the entire correctness argument for `external` mode rests on it. In
 * `embedded` mode an in-process map provides the guarantee; when no in-process
 * runner exists, this is what replaces it, and a process-local map could not do
 * the job anyway with N dispatchers.
 *
 * Also pins the lifecycle of the queue rows themselves. graphile does not reclaim
 * them when the last job completes — measured, not assumed — so a queue per run
 * would be a slow table leak without an explicit `GC_JOB_QUEUES` sweep. Both
 * halves of that are asserted below, because the leak is invisible until it is
 * large.
 */
const testUrl = process.env.EVELAND_WORKFLOW_WORLD_TEST_URL;

describe.skipIf(!testUrl)("graphile per-queue serialization", () => {
  let pool: Pool;
  let workerUtils: WorkerUtils;
  let runner: Runner | undefined;

  // Distinct task name per file so a parallel suite cannot claim these jobs.
  const TASK = "wfw_serialization_probe";

  beforeAll(async () => {
    pool = new Pool({ connectionString: testUrl, max: 12 });
    workerUtils = await makeWorkerUtils({ pgPool: pool });
    await workerUtils.migrate();
  }, 60_000);

  afterAll(async () => {
    await runner?.stop();
    await workerUtils?.release();
    await pool?.end();
  });

  /**
   * Records the observed overlap: each job marks itself in-flight, sleeps, and
   * reports the peak number of concurrently in-flight jobs it saw.
   */
  async function measurePeakOverlap(
    jobs: { queueName?: string }[],
    concurrency: number,
  ): Promise<number> {
    let inFlight = 0;
    let peak = 0;
    let done = 0;
    let settled = (): void => {};
    const allDone = new Promise<void>((resolve) => {
      settled = () => {
        if (done === jobs.length) resolve();
      };
    });

    runner = await run({
      pgPool: pool,
      concurrency,
      pollInterval: 50,
      noHandleSignals: true,
      taskList: {
        [TASK]: async () => {
          inFlight += 1;
          peak = Math.max(peak, inFlight);
          await new Promise((resolve) => setTimeout(resolve, 120));
          inFlight -= 1;
          done += 1;
          settled();
        },
      },
    });

    for (const [index, job] of jobs.entries()) {
      await workerUtils.addJob(TASK, { index }, job.queueName ? { queueName: job.queueName } : {});
    }

    await allDone;
    await runner.stop();
    runner = undefined;
    return peak;
  }

  test("jobs sharing a run queue never overlap, even at concurrency 8", async () => {
    const queueName = runQueueName("prj_serialization", "wrun_shared");
    const peak = await measurePeakOverlap(
      Array.from({ length: 6 }, () => ({ queueName })),
      8,
    );
    // The whole point: one at a time.
    expect(peak).toBe(1);
  }, 60_000);

  test("jobs in different run queues still run in parallel", async () => {
    // Guards against over-serializing: if this also came back 1, the fix would
    // have turned the dispatcher into a single-threaded queue.
    const peak = await measurePeakOverlap(
      Array.from({ length: 6 }, (_unused, index) => ({
        queueName: runQueueName("prj_serialization", `wrun_distinct_${String(index)}`),
      })),
      8,
    );
    expect(peak).toBeGreaterThan(1);
  }, 60_000);

  test("a drained run queue leaves its row behind until GC_JOB_QUEUES runs", async () => {
    const queueName = runQueueName("prj_serialization", "wrun_cleanup");
    const countRows = async () => {
      const { rows } = await pool.query<{ count: string }>(
        `select count(*)::text as count
           from graphile_worker._private_job_queues
          where queue_name = $1`,
        [queueName],
      );
      return rows[0]?.count;
    };

    await measurePeakOverlap([{ queueName }, { queueName }], 4);

    // The row survives its jobs. This is the whole reason the dispatcher sweeps:
    // one row per run, accumulating, would otherwise be permanent.
    expect(await countRows()).toBe("1");

    await workerUtils.cleanup({ tasks: ["GC_JOB_QUEUES"] });
    expect(await countRows()).toBe("0");
  }, 60_000);
});
