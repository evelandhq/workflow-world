import { makeWorkerUtils, type WorkerUtils } from "graphile-worker";
import { createServer, type Server } from "node:http";
import { Pool } from "pg";
import { afterAll, beforeAll, beforeEach, describe, expect, test } from "vitest";
import { FLOW_JOB_NAME, tenantFlag } from "../dispatch-contract.js";
import { deliveryInFlight } from "../delivery-liveness.js";
import { createClient, type Drizzle } from "../drizzle/index.js";
import { MessageData, withRunId } from "../message.js";
import { runMigrations } from "../migrate.js";
import type { ActivationClient } from "./activation-client.js";
import { startDispatcher, type DispatcherRuntime } from "./runner.js";

/**
 * The run gate against real graphile claims: a run's orchestrator deliveries
 * never overlap, other runs' do, a step message is never held back, wakes
 * coalesce into the holder's job, which stays locked throughout, and a message
 * that carries data waits its turn on the queue instead.
 *
 * Also what the gate replaced: no job waits in a graphile named queue any more,
 * so `_private_job_queues` gets no row per run, and a delayed delivery is an
 * ordinary flow job that fires when due.
 *
 * The executor here is a bare HTTP server, not eve's handler: eve's own
 * handler serializes a run's deliveries within one process, which would hide
 * an overlap the dispatcher let through.
 *
 * Set `WORKFLOW_WORLD_TEST_URL` to a scratch database to run it.
 */
const testUrl = process.env.WORKFLOW_WORLD_TEST_URL;
const suffix = `${String(process.pid)}${Date.now().toString(36)}`;
const TENANT = `p_gate_${suffix}`;
const DEPLOYMENT = `dep_gate_${suffix}`;

type Delivery = {
  runId: string;
  messageId: string;
  attempt: number;
  deliveryCount: number;
  body: Record<string, unknown>;
};
type Answer = { status: number; body: unknown };

describe.skipIf(!testUrl)("the run gate against graphile", () => {
  let pool: Pool;
  let db: Drizzle;
  let workerUtils: WorkerUtils;
  let dispatcher: DispatcherRuntime;
  let agent: Server;
  /** Every delivery of this tenant the agent received, in order. */
  let delivered: Delivery[] = [];
  /** How the agent answers a delivery; by default after a short pause, ok. */
  let respond: (delivery: Delivery) => Promise<Answer> = async () => {
    await sleep(100);
    return { status: 200, body: { ok: true } };
  };
  const inFlight = new Map<string, number>();
  const peak = new Map<string, number>();
  let peakAll = 0;
  let inFlightAll = 0;

  beforeAll(async () => {
    pool = new Pool({ connectionString: testUrl, max: 12 });
    await runMigrations(pool);
    db = createClient(pool);
    workerUtils = await makeWorkerUtils({ pgPool: pool });
    await workerUtils.migrate();

    agent = createServer((req, res) => {
      const chunks: Buffer[] = [];
      req.on("data", (chunk: Buffer) => chunks.push(chunk));
      req.on("end", () => {
        void (async () => {
          // The dispatcher claims across every tenant; another suite's
          // leftover job may land here, and is answered and ignored.
          if (req.headers["x-eveland-project-id"] !== TENANT) {
            res.writeHead(200, { "content-type": "application/json" });
            res.end(JSON.stringify({ ok: true }));
            return;
          }
          const body = JSON.parse(Buffer.concat(chunks).toString("utf8")) as Record<
            string,
            unknown
          >;
          const delivery: Delivery = {
            runId: String(body.runId),
            messageId: String(req.headers["x-vqs-message-id"]),
            attempt: Number(req.headers["x-vqs-message-attempt"]),
            deliveryCount: Number(req.headers["x-vqs-message-delivery-count"]),
            body,
          };
          delivered.push(delivery);
          const key = body.stepId === undefined ? delivery.runId : `step:${String(body.stepId)}`;
          inFlight.set(key, (inFlight.get(key) ?? 0) + 1);
          peak.set(key, Math.max(peak.get(key) ?? 0, inFlight.get(key)!));
          inFlightAll += 1;
          peakAll = Math.max(peakAll, inFlightAll);
          try {
            const answer = await respond(delivery);
            res.writeHead(answer.status, { "content-type": "application/json" });
            res.end(JSON.stringify(answer.body));
          } finally {
            inFlight.set(key, inFlight.get(key)! - 1);
            inFlightAll -= 1;
          }
        })();
      });
    });
    await new Promise<void>((resolve) => agent.listen(0, "127.0.0.1", resolve));
    const address = agent.address();
    if (typeof address === "string" || !address) throw new Error("no agent port");
    const activation: ActivationClient = {
      async activate() {
        return {
          type: "activated",
          activation: { leaseId: "lease_gate", endpointPort: address.port },
        };
      },
      async renew() {
        return true;
      },
      async release() {},
    };

    dispatcher = await startDispatcher({
      pool,
      config: { concurrency: 8, pollIntervalMs: 50, maxInFlightPerTenant: 8 },
      deps: {
        activation,
        runtimeSecret: "gate-test-secret",
        dispatchTimeoutMs: 10_000,
        leaseRenewIntervalMs: 60_000,
        activationLeaseTtlMs: 180_000,
      },
    });
  }, 60_000);

  beforeEach(() => {
    delivered = [];
    inFlight.clear();
    peak.clear();
    peakAll = 0;
    respond = async () => {
      await sleep(100);
      return { status: 200, body: { ok: true } };
    };
  });

  afterAll(async () => {
    await dispatcher?.stop().catch(() => {});
    await new Promise<void>((resolve) => agent?.close(() => resolve()));
    await pool
      ?.query("delete from graphile_worker._private_jobs where payload->>'tenantId' = $1", [TENANT])
      .catch(() => {});
    await pool?.end().catch(() => {});
  });

  let next = 0;
  /** A job written the way the World writes one. */
  async function enqueue(
    body: Record<string, unknown>,
    options: { messageId?: string; runAt?: Date } = {},
  ): Promise<string> {
    const messageId = options.messageId ?? `msg_gate_${suffix}_${String((next += 1))}`;
    const message: MessageData = {
      id: "greet",
      data: Buffer.from(JSON.stringify(body)),
      attempt: 1,
      messageId: messageId as MessageData["messageId"],
      tenantId: TENANT,
      deploymentId: DEPLOYMENT,
    };
    await workerUtils.addJob(FLOW_JOB_NAME, MessageData.encode(withRunId(message)), {
      jobKey: messageId,
      maxAttempts: 49,
      flags: [tenantFlag(TENANT)],
      ...(options.runAt ? { runAt: options.runAt } : {}),
    });
    return messageId;
  }

  /** Jobs of this tenant still in graphile's table. */
  async function jobs(): Promise<
    Array<{ message_id: string; locked: boolean; run_at: Date; payload: MessageData }>
  > {
    const { rows } = await pool.query(
      `select payload->>'messageId' as message_id, locked_at is not null as locked, run_at,
              payload
         from graphile_worker._private_jobs
        where payload->>'tenantId' = $1`,
      [TENANT],
    );
    return rows;
  }

  async function drained(): Promise<void> {
    await waitFor(async () => (await jobs()).length === 0 && inFlightAll === 0, 30_000, "drain");
  }

  const wake = (runId: string) => ({ runId, runSpecVersion: 9 });

  test("a run's orchestrator jobs never overlap at concurrency 8; different runs do", async () => {
    const shared = `wrun_shared_${suffix}`;
    for (let index = 0; index < 6; index += 1) await enqueue(wake(shared));
    for (let index = 0; index < 6; index += 1)
      await enqueue(wake(`wrun_distinct_${suffix}_${index}`));
    await drained();

    expect(peak.get(shared)).toBe(1);
    // Six wakes enqueued at once collapse into far fewer deliveries.
    const sharedDeliveries = delivered.filter((d) => d.runId === shared);
    expect(sharedDeliveries.length).toBeGreaterThanOrEqual(1);
    expect(sharedDeliveries.length).toBeLessThanOrEqual(6);
    // Guards against over-serializing: the gate is per run, not global.
    expect(peakAll).toBeGreaterThan(1);
  }, 60_000);

  test("a step message of a run whose delivery is in flight is never gated", async () => {
    const runId = `wrun_step_${suffix}`;
    let release!: () => void;
    const held = new Promise<void>((resolve) => (release = resolve));
    respond = async (delivery) => {
      if (delivery.body.stepId === undefined) await held;
      return { status: 200, body: { ok: true } };
    };
    await enqueue(wake(runId));
    await waitFor(() => delivered.length === 1, 10_000, "orchestrator delivery");
    await enqueue({ ...wake(runId), stepId: `step_${suffix}` });
    await waitFor(() => delivered.length === 2, 10_000, "step delivery while held");
    expect(delivered[1]!.body.stepId).toBe(`step_${suffix}`);
    release();
    await drained();
  }, 30_000);

  test("wakes coalesce into the holder's job, which stays locked across its re-run", async () => {
    const runId = `wrun_rerun_${suffix}`;
    const answers: Array<(answer: Answer) => void> = [];
    respond = (delivery) =>
      delivery.runId === runId
        ? new Promise<Answer>((resolve) => answers.push(resolve))
        : Promise.resolve({ status: 200, body: { ok: true } });

    const holder = await enqueue(wake(runId));
    await waitFor(() => answers.length === 1, 10_000, "holder delivery");
    expect(await deliveryInFlight(db, holder)).toBe(true);

    const wakes = [
      await enqueue(wake(runId)),
      await enqueue({ ...wake(runId), hookResumeTiming: { resumeRequestedAtMs: 1 } }),
      await enqueue(wake(runId)),
    ];
    // Each wake's job completes undelivered while the holder runs.
    await waitFor(
      async () => (await jobs()).every((job) => !wakes.includes(job.message_id)),
      10_000,
      "wakes coalesced",
    );
    expect(answers).toHaveLength(1);

    answers[0]!({ status: 200, body: { ok: true } });
    await waitFor(() => answers.length === 2, 10_000, "re-run");
    // The re-run is the holder's own message, under the same locked job.
    expect(delivered.at(-1)).toMatchObject({
      messageId: holder,
      attempt: 1,
      deliveryCount: 2,
      body: { hookResumeTiming: { resumeRequestedAtMs: 1 } },
    });
    expect(await deliveryInFlight(db, holder)).toBe(true);
    expect((await jobs()).find((job) => job.message_id === holder)?.locked).toBe(true);

    answers[1]!({ status: 200, body: { ok: true } });
    await drained();
    expect(await deliveryInFlight(db, holder)).toBe(false);
    expect(delivered.filter((d) => d.runId === runId)).toHaveLength(2);
    expect(peak.get(runId)).toBe(1);
  }, 30_000);

  test("a message carrying data that finds the gate held is delivered later, exactly once", async () => {
    const runId = `wrun_data_${suffix}`;
    let release!: () => void;
    const held = new Promise<void>((resolve) => (release = resolve));
    respond = async (delivery) => {
      if (delivery.body.runInput === undefined) await held;
      return { status: 200, body: { ok: true } };
    };
    await enqueue(wake(runId));
    await waitFor(() => delivered.length === 1, 10_000, "holder delivery");

    const start = await enqueue({ ...wake(runId), runInput: { specVersion: 9, input: [] } });
    // Set aside: still a job, under its own key, due a little later.
    await waitFor(
      async () =>
        (await jobs()).some(
          (job) => job.message_id === start && job.payload.setAside === 1 && !job.locked,
        ),
      10_000,
      "set aside",
    );
    expect(delivered).toHaveLength(1);

    release();
    await waitFor(() => delivered.some((d) => d.messageId === start), 15_000, "data delivery");
    await drained();
    const starts = delivered.filter((d) => d.messageId === start);
    expect(starts).toHaveLength(1);
    expect(starts[0]).toMatchObject({ attempt: 1, deliveryCount: 1 });
    expect(peak.get(runId)).toBe(1);
  }, 30_000);

  test("a delayed delivery is an ordinary flow job that fires when due", async () => {
    const runId = `wrun_sleep_${suffix}`;
    respond = async (delivery) =>
      delivery.deliveryCount === 1
        ? { status: 200, body: { timeoutSeconds: 2 } }
        : { status: 200, body: { ok: true } };
    const messageId = await enqueue(wake(runId));
    await waitFor(() => delivered.length === 1, 10_000, "first delivery");

    // The continuation: the same message, on the flow task, in no queue.
    let continuation: { task: string; job_queue_id: string | null; run_at: Date } | undefined;
    await waitFor(
      async () => {
        const { rows } = await pool.query(
          `select tasks.identifier as task, jobs.job_queue_id, jobs.run_at
             from graphile_worker._private_jobs as jobs
             join graphile_worker._private_tasks as tasks on tasks.id = jobs.task_id
            where jobs.payload->>'messageId' = $1 and jobs.locked_at is null`,
          [messageId],
        );
        continuation = rows[0];
        return continuation !== undefined;
      },
      10_000,
      "continuation job",
    );
    expect(continuation).toMatchObject({ task: FLOW_JOB_NAME, job_queue_id: null });
    expect(continuation!.run_at.getTime()).toBeGreaterThan(Date.now());

    await waitFor(() => delivered.length === 2, 15_000, "delayed delivery");
    expect(delivered[1]).toMatchObject({ messageId, deliveryCount: 2 });
    await drained();
  }, 30_000);

  test("no run leaves a graphile queue row behind", async () => {
    const { rows } = await pool.query<{ count: string }>(
      `select count(*)::text as count
         from graphile_worker._private_job_queues
        where queue_name like '%' || $1 || '%'`,
      [TENANT],
    );
    expect(rows[0]!.count).toBe("0");
  });
});

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function waitFor(
  predicate: () => boolean | Promise<boolean>,
  timeoutMs: number,
  description: string,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!(await predicate())) {
    if (Date.now() > deadline) throw new Error(`timed out waiting for ${description}`);
    await sleep(50);
  }
}
