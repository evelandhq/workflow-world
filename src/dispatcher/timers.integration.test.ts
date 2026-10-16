import { createServer, type Server } from "node:http";
import { makeWorkerUtils, type WorkerUtils } from "graphile-worker";
import { Pool } from "pg";
import { afterAll, beforeAll, describe, expect, test } from "vitest";
import { FLOW_JOB_NAME, runQueueName, TIMER_JOB_NAME } from "../dispatch-contract.js";
import { MessageData } from "../message.js";
import { derivePartitionName } from "../tenant.js";
import { dropTenantPartitions, ensureTenantPartitions, runMigrations } from "../migrate.js";
import type { ActivationClient } from "./activation-client.js";
import { reenqueueActiveRunsForAllTenants } from "./boot-recovery.js";
import { startDispatcher, type DispatcherRuntime } from "./runner.js";
import { createTimerHandler, moveDelayedJobsToTimers, registerTimerTask } from "../timers.js";

/**
 * A delivery that is not due yet must not hold its run's graphile queue
 * (evelandhq/workflow-world#115).
 *
 * graphile 0.16 locks every available named queue on every claim, so a queue
 * kept alive by a sleeping run's continuation costs every claim a row lock for
 * the whole sleep. These pin the three halves of the fix against a real
 * database: the sweep moves far-future flow jobs off their queues, a timer that
 * comes due goes back onto the run's queue under its own key, and boot recovery
 * still treats a held timer as the run's wake-up.
 *
 * Set `EVELAND_WORKFLOW_WORLD_TEST_URL` to a scratch database to run it.
 */
const testUrl = process.env.EVELAND_WORKFLOW_WORLD_TEST_URL;
const suffix = `${String(process.pid)}${Date.now().toString(36)}`;
const TENANT = `p_timers_${suffix}`;
const DEPLOYMENT = `dep_timers_${suffix}`;
const RUNTIME_SECRET = "timers-test-runtime-secret-0000000";
const OTHER_TASK = `wfw_timers_other_${suffix}`;

type JobRow = {
  id: string;
  task: string;
  queue_name: string | null;
  key: string | null;
  run_at: Date;
  payload: Record<string, unknown>;
  flags: Record<string, boolean> | null;
};

describe.skipIf(!testUrl)("delayed deliveries leave their run queue", () => {
  let admin: Pool;
  let workerUtils: WorkerUtils;

  beforeAll(async () => {
    admin = new Pool({ connectionString: testUrl, max: 4 });
    await runMigrations(admin);
    await ensureTenantPartitions(admin, TENANT);
    workerUtils = await makeWorkerUtils({ pgPool: admin });
    await workerUtils.migrate();
    // The dispatcher registers the timer task when its runner starts; these
    // tests drive the sweep without one.
    await registerTimerTask(admin);
  }, 60_000);

  afterAll(async () => {
    await admin
      ?.query("delete from graphile_worker._private_jobs where payload->>'tenantId' = $1", [TENANT])
      .catch(() => {});
    await admin
      ?.query(
        "delete from graphile_worker._private_jobs where task_id in (select id from graphile_worker._private_tasks where identifier = any($1))",
        [
          [
            OTHER_TASK,
            derivePartitionName(FLOW_JOB_NAME, TENANT),
            derivePartitionName(TIMER_JOB_NAME, TENANT),
          ],
        ],
      )
      .catch(() => {});
    await admin
      ?.query("delete from workflow.workflow_runs where tenant_id = $1", [TENANT])
      .catch(() => {});
    await dropTenantPartitions(admin, TENANT).catch(() => {});
    await workerUtils?.cleanup({ tasks: ["GC_JOB_QUEUES"] }).catch(() => {});
    await workerUtils?.release();
    await admin?.end();
  });

  function message(runId: string, messageId: string, idempotencyKey?: string): MessageData {
    return {
      id: "sleeper",
      data: Buffer.from(JSON.stringify({ runId })),
      attempt: 1,
      messageId: messageId as MessageData["messageId"],
      ...(idempotencyKey ? { idempotencyKey } : {}),
      tenantId: TENANT,
      deploymentId: DEPLOYMENT,
    };
  }

  /** What a deployment's world enqueues: a flow job on the run's own queue. */
  async function enqueueLikeTheWorld(
    runId: string,
    input: { messageId: string; delaySeconds: number; idempotencyKey?: string; task?: string },
  ): Promise<string> {
    const job = await workerUtils.addJob(
      input.task ?? FLOW_JOB_NAME,
      MessageData.encode(message(runId, input.messageId, input.idempotencyKey)),
      {
        jobKey: input.idempotencyKey ?? input.messageId,
        runAt: new Date(Date.now() + input.delaySeconds * 1000),
        queueName: runQueueName(TENANT, runId),
        maxAttempts: 49,
        flags: [`project:${TENANT}`],
      },
    );
    return job.id;
  }

  async function readJob(id: string): Promise<JobRow | undefined> {
    const { rows } = await admin.query<JobRow>(
      `select jobs.id::text as id, tasks.identifier as task, queues.queue_name, jobs.key,
              jobs.run_at, jobs.payload, jobs.flags
         from graphile_worker._private_jobs as jobs
         join graphile_worker._private_tasks as tasks on tasks.id = jobs.task_id
         left join graphile_worker._private_job_queues as queues on queues.id = jobs.job_queue_id
        where jobs.id = $1::bigint`,
      [id],
    );
    return rows[0];
  }

  async function jobsForRun(runId: string): Promise<JobRow[]> {
    const { rows } = await admin.query<JobRow>(
      `select jobs.id::text as id, tasks.identifier as task, queues.queue_name, jobs.key,
              jobs.run_at, jobs.payload, jobs.flags
         from graphile_worker._private_jobs as jobs
         join graphile_worker._private_tasks as tasks on tasks.id = jobs.task_id
         left join graphile_worker._private_job_queues as queues on queues.id = jobs.job_queue_id
        where jobs.payload->>'tenantId' = $1
          and convert_from(decode(jobs.payload->>'data', 'base64'), 'utf8')::jsonb->>'runId' = $2
        order by jobs.id`,
      [TENANT, runId],
    );
    return rows;
  }

  async function queueExists(runId: string): Promise<boolean> {
    const { rows } = await admin.query(
      "select 1 from graphile_worker._private_job_queues where queue_name = $1",
      [runQueueName(TENANT, runId)],
    );
    return rows.length > 0;
  }

  test("the sweep moves far-future run deliveries and nothing else", async () => {
    const sleeping = `wrun_sleeping_${suffix}`;
    const soon = `wrun_soon_${suffix}`;
    const retried = `wrun_retried_${suffix}`;
    const foreign = `wrun_foreign_${suffix}`;

    const sleepingJob = await enqueueLikeTheWorld(sleeping, {
      messageId: `msg_sleep_${suffix}`,
      idempotencyKey: `wait_${suffix}:hop-7`,
      delaySeconds: 23 * 3600,
    });
    const soonJob = await enqueueLikeTheWorld(soon, {
      messageId: `msg_soon_${suffix}`,
      delaySeconds: 10,
    });
    const retriedJob = await enqueueLikeTheWorld(retried, {
      messageId: `msg_retried_${suffix}`,
      delaySeconds: 3600,
    });
    // A failed attempt's backoff: moving it would restart its attempt count.
    await admin.query("update graphile_worker._private_jobs set attempts = 1 where id = $1", [
      retriedJob,
    ]);
    const foreignJob = await enqueueLikeTheWorld(foreign, {
      messageId: `msg_foreign_${suffix}`,
      delaySeconds: 3600,
      task: OTHER_TASK,
    });

    expect(await moveDelayedJobsToTimers(admin)).toBeGreaterThanOrEqual(1);

    const moved = await readJob(sleepingJob);
    expect(moved).toMatchObject({
      task: TIMER_JOB_NAME,
      queue_name: null,
      key: `wait_${suffix}:hop-7`,
      flags: null,
    });
    expect(moved!.payload.runQueue).toBe(runQueueName(TENANT, sleeping));
    // Still a complete message: the timer handler reads it back as one.
    expect(MessageData.parse(moved!.payload).messageId).toBe(`msg_sleep_${suffix}`);

    expect(await readJob(soonJob)).toMatchObject({
      task: FLOW_JOB_NAME,
      queue_name: runQueueName(TENANT, soon),
    });
    expect(await readJob(retriedJob)).toMatchObject({
      task: FLOW_JOB_NAME,
      queue_name: runQueueName(TENANT, retried),
    });
    expect(await readJob(foreignJob)).toMatchObject({
      task: OTHER_TASK,
      queue_name: runQueueName(TENANT, foreign),
    });

    // The moved job's queue is empty now, so the GC reclaims it — which is the
    // point: it no longer costs a claim anything.
    await workerUtils.cleanup({ tasks: ["GC_JOB_QUEUES"] });
    expect(await queueExists(sleeping)).toBe(false);
    expect(await queueExists(soon)).toBe(true);

    // Idempotent: a second pass finds nothing of ours to move.
    await moveDelayedJobsToTimers(admin);
    expect(await readJob(soonJob)).toMatchObject({ task: FLOW_JOB_NAME });
  });

  test("an embedded tenant's jobs move only under that tenant's own names", async () => {
    const runId = `wrun_embedded_${suffix}`;
    const flowName = derivePartitionName(FLOW_JOB_NAME, TENANT);
    const timerName = derivePartitionName(TIMER_JOB_NAME, TENANT);
    await registerTimerTask(admin, timerName);
    const jobId = await enqueueLikeTheWorld(runId, {
      messageId: `msg_embedded_${suffix}`,
      delaySeconds: 3600,
      task: flowName,
    });

    // The dispatcher's sweep leaves it alone: only that tenant's runner can
    // claim the timer it would become.
    await moveDelayedJobsToTimers(admin);
    expect(await readJob(jobId)).toMatchObject({ task: flowName });

    await moveDelayedJobsToTimers(admin, { flowJobName: flowName, timerJobName: timerName });
    expect(await readJob(jobId)).toMatchObject({ task: timerName, queue_name: null });
  });

  test("a newer enqueue under the same key takes the timer back onto the queue", async () => {
    const runId = `wrun_rekeyed_${suffix}`;
    const key = `wait_rekeyed_${suffix}`;
    const jobId = await enqueueLikeTheWorld(runId, {
      messageId: `msg_rekeyed_${suffix}`,
      idempotencyKey: key,
      delaySeconds: 3600,
    });
    await moveDelayedJobsToTimers(admin);
    expect(await readJob(jobId)).toMatchObject({ task: TIMER_JOB_NAME });

    // graphile's replace-by-key re-points the same row, task and queue included.
    await enqueueLikeTheWorld(runId, {
      messageId: `msg_rekeyed_now_${suffix}`,
      idempotencyKey: key,
      delaySeconds: 0,
    });
    const jobs = await jobsForRun(runId);
    expect(jobs).toHaveLength(1);
    expect(jobs[0]).toMatchObject({
      id: jobId,
      task: FLOW_JOB_NAME,
      queue_name: runQueueName(TENANT, runId),
      key,
    });
  });

  test("a timer that comes due goes back onto its run queue under its own key", async () => {
    const runId = `wrun_promoted_${suffix}`;
    const key = `wait_promoted_${suffix}`;
    const jobId = await enqueueLikeTheWorld(runId, {
      messageId: `msg_promoted_${suffix}`,
      idempotencyKey: key,
      delaySeconds: 3600,
    });
    await moveDelayedJobsToTimers(admin);
    const timer = await readJob(jobId);

    await createTimerHandler({
      addJob: (identifier, payload, spec) => workerUtils.addJob(identifier, payload, spec),
      flowJobName: FLOW_JOB_NAME,
    })(timer!.payload, { job: { key } });

    const jobs = await jobsForRun(runId);
    const promoted = jobs.find((job) => job.task === FLOW_JOB_NAME);
    expect(promoted).toMatchObject({
      queue_name: runQueueName(TENANT, runId),
      key,
      flags: { [`project:${TENANT}`]: true },
    });
    expect(promoted!.run_at.getTime()).toBeLessThanOrEqual(Date.now() + 1_000);
    // Exactly the message the deployment enqueued, minus the timer's own field.
    expect(promoted!.payload.runQueue).toBeUndefined();
    expect(MessageData.parse(promoted!.payload)).toEqual(
      MessageData.parse(MessageData.encode(message(runId, `msg_promoted_${suffix}`, key))),
    );
  });

  test("boot recovery counts a held timer as the run's live job", async () => {
    const runId = `wrun_bootheld_${suffix}`;
    await admin.query(
      `insert into workflow.workflow_runs
         (tenant_id, id, deployment_id, status, name, spec_version, queue_namespace)
       values ($1, $2, $3, 'running', 'sleeper', 7, null)`,
      [TENANT, runId, DEPLOYMENT],
    );
    await enqueueLikeTheWorld(runId, {
      messageId: `msg_bootheld_${suffix}`,
      delaySeconds: 3600,
    });
    await moveDelayedJobsToTimers(admin);
    await workerUtils.cleanup({ tasks: ["GC_JOB_QUEUES"] });
    expect(await queueExists(runId)).toBe(false);

    const offered: string[] = [];
    await reenqueueActiveRunsForAllTenants({
      pool: admin,
      workerUtils,
      filterRuns: (runs) => {
        offered.push(...runs.filter((run) => run.tenantId === TENANT).map((run) => run.runId));
        return [];
      },
    });
    expect(offered).not.toContain(runId);
  });
});

describe.skipIf(!testUrl)("the dispatcher keeps delayed deliveries off run queues", () => {
  const BACKLOG_RUN = `wrun_backlog_${suffix}`;
  let admin: Pool;
  let dispatcherPool: Pool;
  let dispatcher: DispatcherRuntime;
  let agent: Server;
  const deliveries: Array<{ messageId: string; attempt: string }> = [];
  let answers: string[] = [];

  beforeAll(async () => {
    admin = new Pool({ connectionString: testUrl, max: 2 });
    await runMigrations(admin);

    agent = createServer((req, res) => {
      req.resume();
      req.on("end", () => {
        deliveries.push({
          messageId: String(req.headers["x-vqs-message-id"]),
          attempt: String(req.headers["x-vqs-message-attempt"]),
        });
        res.writeHead(200, { "content-type": "application/json" });
        res.end(answers.shift() ?? "{}");
      });
    });
    await new Promise<void>((resolve) => agent.listen(0, "127.0.0.1", resolve));
    const address = agent.address();
    if (typeof address === "string" || !address) throw new Error("no port");

    const activation: ActivationClient = {
      async activate() {
        return {
          type: "activated",
          activation: { leaseId: `lease_${suffix}`, endpointPort: address.port },
        };
      },
      async renew() {
        return true;
      },
      async release() {},
    };

    // A backlog left by an older dispatcher: a sleeping run's continuation
    // still on its run queue when this one starts.
    await admin.query(
      `select graphile_worker.add_job(
         $1, $2::json, queue_name => $3, run_at => now() + interval '1 hour', job_key => $4,
         max_attempts => 49
       )`,
      [
        FLOW_JOB_NAME,
        JSON.stringify(
          MessageData.encode({
            id: "sleeper",
            data: Buffer.from(JSON.stringify({ runId: BACKLOG_RUN })),
            attempt: 1,
            messageId: `msg_backlog_${suffix}` as MessageData["messageId"],
            tenantId: TENANT,
            deploymentId: DEPLOYMENT,
          }),
        ),
        runQueueName(TENANT, BACKLOG_RUN),
        `msg_backlog_${suffix}`,
      ],
    );

    dispatcherPool = new Pool({ connectionString: testUrl, max: 4 });
    dispatcher = await startDispatcher({
      pool: dispatcherPool,
      config: {
        concurrency: 4,
        pollIntervalMs: 100,
        maxInFlightPerTenant: 4,
        queueGcIntervalMs: 3_600_000,
        timerSweepIntervalMs: 3_600_000,
      },
      deps: {
        activation,
        runtimeSecret: RUNTIME_SECRET,
        dispatchTimeoutMs: 10_000,
        leaseRenewIntervalMs: 60_000,
        activationLeaseTtlMs: 180_000,
      },
    });
  }, 60_000);

  afterAll(async () => {
    await dispatcher?.stop().catch(() => {});
    await new Promise<void>((resolve) => agent?.close(() => resolve()));
    await admin
      ?.query("delete from graphile_worker._private_jobs where payload->>'tenantId' = $1", [TENANT])
      .catch(() => {});
    await dispatcherPool?.end().catch(() => {});
    await admin?.end().catch(() => {});
  });

  test("a backlog on run queues is cleared when the dispatcher starts", async () => {
    await waitFor(async () => {
      const { rows } = await admin.query(
        "select 1 from graphile_worker._private_job_queues where queue_name = $1",
        [runQueueName(TENANT, BACKLOG_RUN)],
      );
      return rows.length === 0 ? true : undefined;
    });
    const { rows } = await admin.query<{ task: string }>(
      `select tasks.identifier as task
         from graphile_worker._private_jobs as jobs
         join graphile_worker._private_tasks as tasks on tasks.id = jobs.task_id
        where jobs.key = $1`,
      [`msg_backlog_${suffix}`],
    );
    expect(rows).toEqual([{ task: TIMER_JOB_NAME }]);
  });

  test("a long reschedule waits off the queue, then is delivered on it", async () => {
    const runId = `wrun_resched_${suffix}`;
    const messageId = `msg_resched_${suffix}`;
    answers = [JSON.stringify({ timeoutSeconds: 3600 }), "{}"];

    await dispatcher.workerUtils.addJob(
      FLOW_JOB_NAME,
      MessageData.encode({
        id: "sleeper",
        data: Buffer.from(JSON.stringify({ runId })),
        attempt: 1,
        messageId: messageId as MessageData["messageId"],
        tenantId: TENANT,
        deploymentId: DEPLOYMENT,
      }),
      {
        jobKey: messageId,
        queueName: runQueueName(TENANT, runId),
        maxAttempts: 49,
        flags: [`project:${TENANT}`],
      },
    );

    const timer = await waitFor(async () => {
      const { rows } = await admin.query<{ id: string; job_queue_id: number | null; key: string }>(
        `select jobs.id::text as id, jobs.job_queue_id, jobs.key
           from graphile_worker._private_jobs as jobs
           join graphile_worker._private_tasks as tasks on tasks.id = jobs.task_id
          where tasks.identifier = $1 and jobs.payload->>'messageId' = $2`,
        [TIMER_JOB_NAME, messageId],
      );
      return rows[0];
    });
    expect(timer).toMatchObject({ job_queue_id: null, key: messageId });
    expect(deliveries.filter((delivery) => delivery.messageId === messageId)).toHaveLength(1);

    // Bring the timer due instead of waiting an hour.
    await admin.query("update graphile_worker._private_jobs set run_at = now() where id = $1", [
      timer!.id,
    ]);
    await waitFor(async () =>
      deliveries.filter((delivery) => delivery.messageId === messageId).length >= 2
        ? true
        : undefined,
    );

    // Delivered, completed, and nothing left behind: no timer, no flow job.
    await waitFor(async () => {
      const { rows } = await admin.query(
        "select 1 from graphile_worker._private_jobs where payload->>'messageId' = $1",
        [messageId],
      );
      return rows.length === 0 ? true : undefined;
    });
  }, 60_000);
});

async function waitFor<T>(probe: () => Promise<T | undefined>, timeoutMs = 20_000): Promise<T> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const value = await probe();
    if (value !== undefined) return value;
    if (Date.now() > deadline) throw new Error("timed out");
    await new Promise((resolve) => setTimeout(resolve, 100));
  }
}
