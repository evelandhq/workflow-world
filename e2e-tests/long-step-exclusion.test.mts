import { createRequire } from "node:module";
import { createServer, request, type Server } from "node:http";
import path from "node:path";
import { pathToFileURL } from "node:url";
import { getQueueTopicPrefix } from "@workflow/world";
import { makeWorkerUtils, run as runWorker, type Runner, type WorkerUtils } from "graphile-worker";
import { Pool } from "pg";
import { afterAll, beforeAll, describe, expect, test } from "vitest";
import { runQueueName } from "../src/dispatch-contract.js";
import { startDispatcherService, type DispatcherService } from "../src/dispatcher/service.js";
import { createWorld, ensureTenantPartitions, runMigrations } from "../src/index.js";
import { ENABLED_EVE_VERSIONS } from "./eve-versions.mts";
import {
  buildAgent,
  databaseFor,
  deploymentFor,
  E2E_RUNTIME_SECRET,
  packWorld,
  startAgent,
  tenantFor,
  type StartedAgent,
} from "./harness.mts";

/**
 * A step that a bypassing hook wake started runs on one replica only, even
 * past the runtime's ownership lease.
 *
 * Such a step holds no run queue. Once its lease has lapsed, a later replay
 * enqueues a step message for it, and with the run's queue free another
 * replica claims that message while the step still runs. Storage refuses that
 * start while the owner's delivery is in flight (evelandhq/workflow-world#114).
 * Two replicas are needed: in one process the runtime's single-flight would
 * absorb the second start and hide a missing fence.
 *
 * The scenario, with a 2 s ownership lease and the fixture's tool set to 20 s:
 *  1. A worker of this test holds the run's queue, so the first message's wake
 *     bypasses it and starts the turn, and the tool, on replica A. The queue is
 *     then released; the step holds none.
 *  2. Deliveries now go to replica B. The test enqueues the step message a
 *     replay past the lease would enqueue, through the World: when the runtime
 *     gets to it depends on eve's timing, which no test should wait on.
 *  3. B claims it and runs the runtime's own queued-step path, which must not
 *     start the tool again while A still runs it.
 */
const baseUrl = process.env.WORKFLOW_WORLD_E2E_URL;
const adminUrl = process.env.WORKFLOW_WORLD_E2E_ADMIN_URL ?? baseUrl;
const HOLDER_TASK = "e2e_run_queue_holder";

describe.skipIf(!baseUrl)("a long step started by a bypassing wake", () => {
  let worldTarball: string;

  beforeAll(() => {
    worldTarball = packWorld();
  });

  for (const [index, entry] of ENABLED_EVE_VERSIONS.entries()) {
    describe(`eve ${entry.version}`, () => {
      const tenantId = `${tenantFor(entry.version)}_exclusion`;
      const deploymentId = `${deploymentFor(entry.version)}_exclusion`;
      const database = `${databaseFor(entry.version)}_exclusion`;
      const portA = 41960 + index * 3;
      const portB = portA + 1;
      const proxyPort = portA + 2;

      let pool: Pool;
      let dir: string;
      let replicaA: StartedAgent | undefined;
      let replicaB: StartedAgent | undefined;
      let dispatcher: DispatcherService | undefined;
      let proxy: Server | undefined;
      let holderUtils: WorkerUtils | undefined;
      let holder: Runner | undefined;
      let world: ReturnType<typeof createWorld> | undefined;
      let worldOptions: { connectionString: string; tenantId: string; deploymentId: string };
      let releaseQueue = (): void => {};
      let target = portA;
      /** Step messages delivered to replica B, and whether A's tool was still running then. */
      const stepMessagesOnB: { stepId: string; whileRunning: boolean }[] = [];

      beforeAll(async () => {
        const admin = new Pool({ connectionString: adminUrl, max: 1 });
        try {
          await admin.query(`drop database if exists ${database}`);
          await admin.query(`create database ${database}`);
        } finally {
          await admin.end();
        }
        const url = new URL(baseUrl!);
        url.pathname = `/${database}`;
        const databaseUrl = url.toString();
        pool = new Pool({ connectionString: databaseUrl, max: 4 });
        await runMigrations(pool);
        await ensureTenantPartitions(pool, tenantId);

        dir = buildAgent({
          eveVersion: entry.version,
          worldTarball,
          fixture: "fixture-cancel",
          dependencies: { zod: "4.6.5" },
        });
        const agentEnv = {
          // vitest sets NODE_ENV=test, under which eve swaps the agent's own
          // model for its mock and the scripted tool call never happens.
          NODE_ENV: "production",
          // Short, so the runtime's own ownership check lets B run the step message;
          // the fence is then the only thing left to refuse it.
          WORKFLOW_INLINE_OWNERSHIP_LEASE_SECONDS: "2",
          // Long enough that the step message handed to B lands while A still runs it.
          E2E_TOOL_SECONDS: "20",
        };
        for (const [port, assign] of [
          [portA, (agent: StartedAgent) => (replicaA = agent)],
          [portB, (agent: StartedAgent) => (replicaB = agent)],
        ] as const) {
          assign(
            await startAgent({
              dir,
              port,
              databaseUrl,
              tenantId,
              deploymentId,
              runner: "external",
              env: agentEnv,
            }),
          );
        }

        // The deployment's one origin, routed to whichever replica `target` names.
        proxy = createServer((req, res) => {
          const chunks: Buffer[] = [];
          req.on("data", (chunk: Buffer) => chunks.push(chunk));
          req.on("end", () => {
            const body = Buffer.concat(chunks);
            const port = target;
            const stepId = port === portB ? parseStepId(body) : undefined;
            if (stepId) {
              const whileRunning = !replicaA!.log().includes("[e2e-cancel] sent");
              stepMessagesOnB.push({ stepId, whileRunning });
            }
            const upstream = request(
              { host: "127.0.0.1", port, path: req.url, method: req.method, headers: req.headers },
              (answer) => {
                res.writeHead(answer.statusCode ?? 502, answer.headers);
                answer.pipe(res);
              },
            );
            upstream.on("error", () => {
              res.writeHead(502);
              res.end();
            });
            upstream.end(body);
          });
        });
        await new Promise<void>((resolve) => proxy!.listen(proxyPort, "127.0.0.1", resolve));

        dispatcher = await startDispatcherService({
          env: {
            NODE_ENV: "development",
            WORKFLOW_WORLD_URL: databaseUrl,
            WORKFLOW_WORLD_RUNTIME_SECRET: E2E_RUNTIME_SECRET,
            WORKFLOW_DISPATCHER_STATIC_ENDPOINTS: `${deploymentId}=http://127.0.0.1:${String(proxyPort)}`,
            WORKFLOW_DISPATCHER_POLL_INTERVAL_MS: "100",
            WORKFLOW_DISPATCHER_MAINTENANCE_INTERVAL_MS: "0",
          },
          telemetry: { emit() {}, async shutdown() {} },
        });

        worldOptions = { connectionString: databaseUrl, tenantId, deploymentId };
        holderUtils = await makeWorkerUtils({ pgPool: pool });
        holder = await runWorker({
          pgPool: pool,
          concurrency: 1,
          pollInterval: 100,
          noHandleSignals: true,
          taskList: {
            [HOLDER_TASK]: () => new Promise<void>((resolve) => (releaseQueue = resolve)),
          },
        });
      });

      afterAll(async () => {
        releaseQueue();
        await holder?.stop().catch(() => {});
        await holderUtils?.release();
        await world?.close?.().catch(() => {});
        await dispatcher?.stop().catch(() => {});
        await replicaA?.stop();
        await replicaB?.stop();
        await new Promise<void>((resolve) => (proxy ? proxy.close(() => resolve()) : resolve()));
        await pool?.end().catch(() => {});
        const admin = new Pool({ connectionString: adminUrl, max: 1 });
        await admin.query(`drop database if exists ${database}`).catch(() => {});
        await admin.end();
      });

      test("the other replica does not start the step again while it runs", async () => {
        const { EveAgentStore, defaultMessageReducer } = (await import(
          pathToFileURL(createRequire(path.join(dir, "package.json")).resolve("eve/client")).href
        )) as typeof import("eve/client");
        const store = new EveAgentStore({
          host: `http://127.0.0.1:${String(portA)}`,
          reducer: defaultMessageReducer(),
        });
        const starts = () =>
          [replicaA!.log(), replicaB!.log()].map(
            (log) => [...log.matchAll(/\[e2e-cancel\] started/g)].length,
          );

        diagnose = async () => {
          const { rows } = await pool.query<{ t: string; type: string }>(
            `select to_char(created_at, 'HH24:MI:SS.MS') as t, type
               from workflow.workflow_events where tenant_id = $1 order by created_at`,
            [tenantId],
          );
          const jobs = await pool.query<{
            key: string | null;
            queue: string | null;
            locked: boolean;
            error: string | null;
            attempts: number;
          }>(
            `select jobs.key, queues.queue_name as queue, jobs.locked_at is not null as locked,
                    left(jobs.last_error, 200) as error, jobs.attempts
               from graphile_worker._private_jobs as jobs
               left join graphile_worker._private_job_queues as queues on queues.id = jobs.job_queue_id`,
          );
          return [
            `jobs: ${JSON.stringify(jobs.rows)}`,
            `step messages on B: ${JSON.stringify(stepMessagesOnB)}`,
            `events: ${rows.map((row) => `${row.t} ${row.type}`).join(", ")}`,
            `replica A: ${replicaA!.log().slice(-1500)}`,
            `replica B: ${replicaB!.log().slice(-1500)}`,
          ].join("\n");
        };
        await store.prewarm();
        const run = await waitForValue(async () => {
          const { rows } = await pool.query<{
            id: string;
            name: string;
            queue_namespace: string | null;
          }>(
            `select id, name, queue_namespace from workflow.workflow_runs
              where tenant_id = $1 and name = 'workflow//eve//workflowEntry'`,
            [tenantId],
          );
          return rows[0];
        }, "the session's run");
        const runId = run.id;
        // The step message goes to the run's own flow queue, namespace included.
        world = createWorld({
          ...worldOptions,
          runner: "external",
          ...(run.queue_namespace ? { queueNamespace: run.queue_namespace } : {}),
        });

        await holderUtils!.addJob(HOLDER_TASK, {}, { queueName: runQueueName(tenantId, runId) });
        // The holder's own claim, not any lock on the queue: the session's start
        // may hold it a moment longer, and a message sent as that ends would queue.
        await waitForValue(async () => {
          const { rows } = await pool.query(
            `select 1
               from graphile_worker._private_jobs as jobs
               join graphile_worker._private_tasks as tasks on tasks.id = jobs.task_id
              where tasks.identifier = $1 and jobs.locked_at is not null`,
            [HOLDER_TASK],
          );
          return rows.length > 0 ? true : undefined;
        }, "the run's queue to be held");

        void store.send({ message: "Send the newsletter." }).catch(() => {});
        await waitForValue(async () => (starts()[0]! > 0 ? true : undefined), "the tool on A");
        releaseQueue();
        target = portB;

        const step = await waitForValue(async () => {
          const { rows } = await pool.query<{ step_id: string; step_name: string }>(
            `select step_id, step_name from workflow.workflow_steps
              where tenant_id = $1 and run_id = $2 and status = 'running'`,
            [tenantId, runId],
          );
          return rows[0];
        }, "the running step");
        // Past the lease, as the replay that enqueues it would be: inside it, the
        // runtime on B declines a step another delivery owns, fence or no fence.
        await new Promise((resolve) => setTimeout(resolve, 3_000));
        await world.queue(
          `${getQueueTopicPrefix("workflow", run.queue_namespace ?? undefined)}${run.name}` as never,
          { runId, stepId: step.step_id, stepName: step.step_name },
          { idempotencyKey: step.step_id },
        );

        // The scenario happened: B was handed the step message while A still ran the step.
        await waitForValue(
          async () => (stepMessagesOnB.some((message) => message.whileRunning) ? true : undefined),
          "a step message on replica B while A runs the step",
          15_000,
        );
        // A's tool finishes, then give B time to have started the tool too, had it.
        await waitForValue(
          async () => (replicaA!.log().includes("[e2e-cancel] sent") ? true : undefined),
          "the tool on A to finish",
        );
        await new Promise((resolve) => setTimeout(resolve, 3_000));
        expect(starts()).toEqual([1, 0]);
      }, 180_000);
    });
  }
});

async function waitForValue<T>(
  read: () => Promise<T | undefined>,
  what: string,
  timeoutMs = 60_000,
): Promise<T> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const value = await read();
    if (value !== undefined) return value;
    if (Date.now() > deadline)
      throw new Error(`timed out waiting for ${what}\n${await diagnose()}`);
    await new Promise((resolve) => setTimeout(resolve, 100));
  }
}

let diagnose = async (): Promise<string> => "";

function parseStepId(body: Buffer): string | undefined {
  try {
    const { stepId } = JSON.parse(body.toString("utf8")) as { stepId?: unknown };
    return typeof stepId === "string" ? stepId : undefined;
  } catch {
    return undefined;
  }
}
