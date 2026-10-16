import { createServer } from "node:http";
import { getQueueTopicPrefix, type ValidQueueName } from "@workflow/world";
import { run } from "graphile-worker";
import { Pool } from "pg";
import { afterAll, beforeAll, describe, expect, test } from "vitest";
import { createWorld, runMigrations } from "../index.js";
import { FLOW_JOB_NAME, runQueueName } from "../dispatch-contract.js";
import { startDispatcher } from "./runner.js";

const testUrl = process.env.EVELAND_WORKFLOW_WORLD_TEST_URL;
const TENANT = `p_timer_compat_${process.pid}`;
const RUN_ID = "wrun_timer_compat";

describe.skipIf(!testUrl)("delayed jobs across dispatcher upgrades", () => {
  let pool: Pool;
  let world: ReturnType<typeof createWorld>;

  beforeAll(async () => {
    pool = new Pool({ connectionString: testUrl, max: 8 });
    await runMigrations(pool);
    world = createWorld({
      connectionString: testUrl!,
      tenantId: TENANT,
      deploymentId: "dep_timer_compat",
      runner: "external",
    });
  }, 60_000);

  afterAll(async () => {
    await world?.close?.();
    await pool?.query("delete from graphile_worker._private_jobs where payload->>'tenantId' = $1", [
      TENANT,
    ]);
    await pool?.end();
  });

  test("an older consumer cannot claim timers; the new dispatcher serializes their deliveries", async () => {
    const topic = `${getQueueTopicPrefix("workflow")}test` as ValidQueueName;
    const immediate = await world.queue(topic, { runId: RUN_ID });
    const delayed = await world.queue(topic, { runId: RUN_ID }, { delaySeconds: 60 });
    await pool.query(
      "update graphile_worker._private_jobs set run_at = now() where payload->>'tenantId' = $1",
      [TENANT],
    );

    const oldDeliveries: string[] = [];
    // This is the pre-upgrade consumer's task list. It does not understand the
    // deferred marker, so every job it can claim would be dispatched directly.
    const older = await run({
      pgPool: pool,
      concurrency: 2,
      pollInterval: 20,
      noHandleSignals: true,
      taskList: {
        [FLOW_JOB_NAME]: async (payload) => {
          oldDeliveries.push((payload as { messageId: string }).messageId);
        },
      },
    });
    try {
      await waitFor(async () => {
        const { rows } = await pool.query(
          `select 1 from graphile_worker._private_jobs jobs
             join graphile_worker._private_tasks tasks on tasks.id = jobs.task_id
            where jobs.payload->>'tenantId' = $1 and tasks.identifier = $2`,
          [TENANT, FLOW_JOB_NAME],
        );
        return rows.length === 0;
      });
    } finally {
      await older.stop();
    }
    expect(oldDeliveries).toEqual([immediate.messageId]);

    const delivered: Array<{ messageId: string; queueName: string | null }> = [];
    const server = createServer((req, res) => {
      req.resume();
      req.on("end", () => {
        void (async () => {
          const messageId = req.headers["x-vqs-message-id"] as string;
          const { rows } = await pool.query<{ queue_name: string | null }>(
            `select queues.queue_name from graphile_worker._private_jobs jobs
               left join graphile_worker._private_job_queues queues on queues.id = jobs.job_queue_id
              where jobs.payload->>'tenantId' = $1 and jobs.payload->>'messageId' = $2
                and jobs.locked_by is not null and jobs.job_queue_id is not null`,
            [TENANT, messageId],
          );
          delivered.push({ messageId, queueName: rows[0]?.queue_name ?? null });
          res.writeHead(200, { "content-type": "application/json" });
          res.end("{}");
        })();
      });
    });
    await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
    const address = server.address();
    if (!address || typeof address === "string") throw new Error("no executor port");
    const dispatcher = await startDispatcher({
      pool,
      config: {
        concurrency: 2,
        pollIntervalMs: 20,
        maxInFlightPerTenant: 2,
        queueGcIntervalMs: 3_600_000,
      },
      deps: {
        activation: {
          async activate() {
            return {
              type: "activated",
              activation: { leaseId: "lease_compat", endpointPort: address.port },
            };
          },
          async renew() {
            return true;
          },
          async release() {},
        },
        runLookup: async () => null,
        runtimeSecret: "compat-secret",
        dispatchTimeoutMs: 5_000,
        leaseRenewIntervalMs: 60_000,
        activationLeaseTtlMs: 180_000,
      },
    });
    try {
      await waitFor(async () => delivered.length === 1);
      expect(delivered).toEqual([
        { messageId: delayed.messageId, queueName: runQueueName(TENANT, RUN_ID) },
      ]);
    } finally {
      await dispatcher.stop();
      await new Promise<void>((resolve) => server.close(() => resolve()));
    }
  }, 30_000);
});

async function waitFor(predicate: () => Promise<boolean>): Promise<void> {
  const deadline = Date.now() + 10_000;
  while (!(await predicate())) {
    if (Date.now() >= deadline) throw new Error("timed out waiting for timer delivery");
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
}
