import { getQueueTopicPrefix, type ValidQueueName } from "@workflow/world";
import { spawn, type ChildProcessWithoutNullStreams } from "node:child_process";
import { once } from "node:events";
import { fileURLToPath } from "node:url";
import { createServer, type Server } from "node:http";
import { makeWorkerUtils, type WorkerUtils } from "graphile-worker";
import { Pool } from "pg";
import { afterAll, beforeAll, describe, expect, test } from "vitest";
import { createWorld, ensureTenantPartitions, runMigrations } from "../index.js";
import { dropTenantPartitions } from "../migrate.js";
import { runQueueName } from "../dispatch-contract.js";
import type { ActivationClient } from "./activation-client.js";
import { startDispatcherService, type DispatcherService } from "./service.js";
import { FLOW_JOB_NAME } from "./runner.js";

/**
 * The restart failure this suite guards is a queue lock, not merely a missing
 * job. A separate process claims a real per-run Graphile job and is SIGKILLed
 * while its handler is blocked, leaving both job and queue locked by its random
 * worker id. The replacement service must recover before starting its workers.
 *
 * Set `EVELAND_WORKFLOW_WORLD_TEST_URL` to a scratch database to run it.
 */
const testUrl = process.env.EVELAND_WORKFLOW_WORLD_TEST_URL;
const suffix = `${String(process.pid)}${Date.now().toString(36)}`;
const TENANT = `p_bootlock_${suffix}`;
const DEPLOYMENT = `dep_bootlock_${suffix}`;
const NAMESPACE = "locktest";
const RUNTIME_SECRET = "boot-lock-runtime-secret-000000";
const LIVE_TASK = `wfw_live_lock_probe_${suffix}`;
const LIVE_QUEUE = `wflive:${suffix}`;
const previousRuntimeSecret = process.env.EVELAND_SCHEDULER_RUNTIME_SECRET;
const previousDeploymentId = process.env.EVELAND_DEPLOYMENT_ID;
const BLOCKER_SCRIPT = fileURLToPath(
  new URL("../../scripts/blocking-graphile-worker.mjs", import.meta.url),
);

describe.skipIf(!testUrl)("boot recovery of a stranded Graphile queue lock", () => {
  let admin: Pool;
  let workerUtils: WorkerUtils;
  let world: ReturnType<typeof createWorld>;
  let agent: Server;
  let agentPort: number;
  let service: DispatcherService | undefined;
  let staleWorker: ChildProcessWithoutNullStreams | undefined;
  let liveWorker: ChildProcessWithoutNullStreams | undefined;
  let targetRunId: string | undefined;
  let inFlight = 0;
  let peakInFlight = 0;
  const delivered: string[] = [];
  const workerIdsToClean = new Set<string>();

  beforeAll(async () => {
    process.env.EVELAND_SCHEDULER_RUNTIME_SECRET = RUNTIME_SECRET;
    process.env.EVELAND_DEPLOYMENT_ID = DEPLOYMENT;

    admin = new Pool({ connectionString: testUrl, max: 6 });
    await runMigrations(admin);
    await ensureTenantPartitions(admin, TENANT);
    workerUtils = await makeWorkerUtils({ pgPool: admin });
    await workerUtils.migrate();

    world = createWorld({
      connectionString: testUrl!,
      tenantId: TENANT,
      deploymentId: DEPLOYMENT,
      runner: "external",
      queueNamespace: NAMESPACE,
    });

    const handler = world.createQueueHandler(
      getQueueTopicPrefix("workflow", NAMESPACE),
      async (message, meta) => {
        if ((message as { runId?: string }).runId !== targetRunId) return;
        inFlight += 1;
        peakInFlight = Math.max(peakInFlight, inFlight);
        try {
          await new Promise((resolve) => setTimeout(resolve, 150));
          delivered.push(meta.messageId);
        } finally {
          inFlight -= 1;
        }
      },
    );

    agent = createServer((req, res) => {
      const chunks: Buffer[] = [];
      req.on("data", (chunk: Buffer) => chunks.push(chunk));
      req.on("end", () => {
        void (async () => {
          const response = await handler(
            new Request(`http://127.0.0.1${req.url ?? "/"}`, {
              method: req.method ?? "POST",
              headers: req.headers as Record<string, string>,
              body: Buffer.concat(chunks),
            }),
          );
          res.writeHead(response.status, { "content-type": "application/json" });
          res.end(await response.text());
        })();
      });
    });
    await new Promise<void>((resolve) => agent.listen(0, "127.0.0.1", resolve));
    const address = agent.address();
    if (typeof address === "string" || !address) throw new Error("no agent port");
    agentPort = address.port;
  }, 120_000);

  afterAll(async () => {
    await service?.stop().catch(() => {});
    await killWorker(staleWorker);
    await killWorker(liveWorker);
    if (workerIdsToClean.size > 0) {
      await workerUtils?.forceUnlockWorkers([...workerIdsToClean]).catch(() => {});
    }
    await world?.close?.().catch(() => {});
    if (agent) {
      await new Promise<void>((resolve) => agent.close(() => resolve()));
    }
    await admin
      ?.query("delete from graphile_worker._private_jobs where payload->>'tenantId' = $1", [TENANT])
      .catch(() => {});
    await admin
      ?.query(
        "delete from graphile_worker._private_jobs where task_id = (select id from graphile_worker._private_tasks where identifier = $1)",
        [LIVE_TASK],
      )
      .catch(() => {});
    await admin
      ?.query("delete from workflow.dispatch_dead_letters where tenant_id = $1", [TENANT])
      .catch(() => {});
    await admin
      ?.query("delete from workflow.workflow_runs where tenant_id = $1", [TENANT])
      .catch(() => {});
    await dropTenantPartitions(admin, TENANT).catch(() => {});
    await workerUtils?.cleanup({ tasks: ["GC_JOB_QUEUES"] }).catch(() => {});
    await Promise.resolve(workerUtils?.release()).catch(() => {});
    await admin?.end().catch(() => {});
    restoreEnv("EVELAND_SCHEDULER_RUNTIME_SECRET", previousRuntimeSecret);
    restoreEnv("EVELAND_DEPLOYMENT_ID", previousDeploymentId);
  });

  test("a replacement dispatcher unlocks only the dead generation and resumes within seconds", async () => {
    const created = await world.events.create(null, {
      eventType: "run_created",
      eventData: { deploymentId: DEPLOYMENT, workflowName: "greet", input: [] },
      specVersion: 5,
    });
    targetRunId = created.run!.runId;
    const queueName = runQueueName(TENANT, targetRunId);
    await world.queue(`${getQueueTopicPrefix("workflow", NAMESPACE)}greet` as ValidQueueName, {
      runId: targetRunId,
    });
    await admin.query(
      `update graphile_worker._private_jobs as jobs
          set priority = -32768
         from graphile_worker._private_job_queues as queues
        where queues.id = jobs.job_queue_id
          and queues.queue_name = $1`,
      [queueName],
    );
    staleWorker = await startBlockingWorker(FLOW_JOB_NAME);

    const staleLock = await waitForQueueLock(queueName, 10_000);
    workerIdsToClean.add(staleLock.queue_locked_by);
    await killWorker(staleWorker);
    staleWorker = undefined;

    const stranded = await readQueueLock(queueName);
    expect(stranded?.queue_locked_by).toBe(staleLock.queue_locked_by);

    await workerUtils.addJob(LIVE_TASK, { probe: suffix }, { queueName: LIVE_QUEUE });
    liveWorker = await startBlockingWorker(LIVE_TASK);
    const liveLock = await waitForQueueLock(LIVE_QUEUE, 10_000);
    workerIdsToClean.add(liveLock.queue_locked_by);

    const activation: ActivationClient = {
      async activate() {
        return {
          type: "activated",
          activation: { leaseId: `lease_${suffix}`, endpointPort: agentPort },
        };
      },
      async renew() {
        return true;
      },
      async release() {},
    };
    const startedAt = Date.now();
    service = await startDispatcherService({
      env: {
        NODE_ENV: "development",
        WORKFLOW_WORLD_URL: testUrl!,
        WORKFLOW_WORLD_RUNTIME_SECRET: RUNTIME_SECRET,
        WORKFLOW_DISPATCHER_ACTIVATION_API_URL: "http://activation.test",
        WORKFLOW_DISPATCHER_POOL_SIZE: "5",
        WORKFLOW_DISPATCHER_CONCURRENCY: "2",
        WORKFLOW_DISPATCHER_POLL_INTERVAL_MS: "50",
        WORKFLOW_DISPATCHER_MAINTENANCE_INTERVAL_MS: "0",
      },
      activation,
      telemetry: { emit() {}, async shutdown() {} },
    });

    await waitFor(
      () => delivered.includes(`msg_recover_${targetRunId!}`),
      30_000,
      "recovered run delivery",
    );
    expect(Date.now() - startedAt).toBeLessThan(30_000);
    expect(peakInFlight).toBe(1);

    const stillLive = await readQueueLock(LIVE_QUEUE);
    expect(stillLive?.queue_locked_by).toBe(liveLock.queue_locked_by);
    expect(liveWorker.exitCode).toBeNull();
  }, 90_000);

  async function readQueueLock(queueName: string): Promise<QueueLock | undefined> {
    const { rows } = await admin.query<QueueLock>(
      `select jobs.locked_by as job_locked_by, queues.locked_by as queue_locked_by
         from graphile_worker._private_jobs as jobs
         join graphile_worker._private_job_queues as queues on queues.id = jobs.job_queue_id
        where queues.queue_name = $1
          and jobs.locked_by is not null
        limit 1`,
      [queueName],
    );
    return rows[0];
  }

  async function waitForQueueLock(queueName: string, timeoutMs: number): Promise<QueueLock> {
    let lock: QueueLock | undefined;
    await waitFor(
      async () => {
        lock = await readQueueLock(queueName);
        return lock !== undefined;
      },
      timeoutMs,
      `queue lock ${queueName}`,
    );
    return lock!;
  }
});

type QueueLock = {
  job_locked_by: string;
  queue_locked_by: string;
};

async function startBlockingWorker(taskName: string): Promise<ChildProcessWithoutNullStreams> {
  const child = spawn(process.execPath, [BLOCKER_SCRIPT, taskName], {
    env: { ...process.env, EVELAND_WORKFLOW_WORLD_TEST_URL: testUrl! },
  });
  try {
    await waitForOutput(child, "wfw-blocker:ready", 30_000);
    const claimed = waitForOutput(child, "wfw-blocker:claimed", 30_000);
    child.stdin.end("start\n");
    await claimed;
    return child;
  } catch (error) {
    await killWorker(child);
    throw error;
  }
}

async function killWorker(child: ChildProcessWithoutNullStreams | undefined): Promise<void> {
  if (!child || child.exitCode !== null || child.signalCode !== null) return;
  const exited = once(child, "exit");
  child.kill("SIGKILL");
  await exited;
}

function waitForOutput(
  child: ChildProcessWithoutNullStreams,
  marker: string,
  timeoutMs: number,
): Promise<void> {
  return new Promise((resolve, reject) => {
    let output = "";
    let errors = "";
    const timer = setTimeout(() => {
      cleanup();
      reject(new Error(`timed out waiting for ${marker}; stdout=${output}; stderr=${errors}`));
    }, timeoutMs);
    const onOutput = (chunk: Buffer) => {
      output += chunk.toString("utf8");
      if (output.includes(marker)) {
        cleanup();
        resolve();
      }
    };
    const onErrorOutput = (chunk: Buffer) => {
      errors += chunk.toString("utf8");
    };
    const onExit = (code: number | null, signal: NodeJS.Signals | null) => {
      cleanup();
      reject(new Error(`blocking worker exited before ${marker}: code=${code} signal=${signal}`));
    };
    const onError = (error: Error) => {
      cleanup();
      reject(error);
    };
    const cleanup = () => {
      clearTimeout(timer);
      child.stdout.off("data", onOutput);
      child.stderr.off("data", onErrorOutput);
      child.off("exit", onExit);
      child.off("error", onError);
    };
    child.stdout.on("data", onOutput);
    child.stderr.on("data", onErrorOutput);
    child.once("exit", onExit);
    child.once("error", onError);
  });
}

async function waitFor(
  predicate: () => boolean | Promise<boolean>,
  timeoutMs: number,
  description: string,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!(await predicate())) {
    if (Date.now() > deadline) throw new Error(`timed out waiting for ${description}`);
    await new Promise((resolve) => setTimeout(resolve, 100));
  }
}

function restoreEnv(name: string, value: string | undefined): void {
  if (value === undefined) {
    delete process.env[name];
  } else {
    process.env[name] = value;
  }
}
