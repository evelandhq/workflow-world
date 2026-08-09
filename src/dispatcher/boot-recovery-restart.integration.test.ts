import { getQueueTopicPrefix } from "@workflow/world";
import { createServer, type Server } from "node:http";
import { Pool } from "pg";
import { afterAll, beforeAll, describe, expect, test } from "vitest";
import { createWorld, ensureTenantPartitions, runMigrations } from "../index.js";
import { dropTenantPartitions } from "../migrate.js";
import type { ActivationClient } from "./activation-client.js";
import { reenqueueActiveRunsForAllTenants } from "./boot-recovery.js";
import { startDispatcher, type DispatcherRuntime } from "./runner.js";

/**
 * A namespaced deployment surviving a dispatcher restart, against eve's real
 * queue handler.
 *
 * `boot-recovery-namespace.test.ts` pins the reconstructed message. This pins
 * what the message is *for*: that the executor accepts it. The distinction
 * matters because every unit test around the dispatch loop talks to a fake agent
 * that accepts any queue name, and a fake written from the same misunderstanding
 * as the code is exactly how an invalid topic ships green. Only eve's own
 * handler rejects `__wkf_workflow_greet` at an executor that registered
 * `__acme_wkf_workflow_greet`, and it rejects it with the non-retryable 400 that
 * dead-letters the run.
 *
 * Set `EVELAND_WORKFLOW_WORLD_TEST_URL` to a scratch database to run it.
 */
const testUrl = process.env.EVELAND_WORKFLOW_WORLD_TEST_URL;
const suffix = `${String(process.pid)}${Date.now().toString(36)}`;
const TENANT = `p_bootrs_${suffix}`;
const DEPLOYMENT = `dep_bootrs_${suffix}`;
const NAMESPACE = "acme";
const RUNTIME_SECRET = "boot-restart-runtime-secret-00000";

describe.skipIf(!testUrl)("namespaced boot recovery against eve's real handler", () => {
  let admin: Pool;
  let dispatcherPool: Pool;
  let world: ReturnType<typeof createWorld>;
  let dispatcher: DispatcherRuntime;
  let agent: Server;
  let agentPort: number;
  /** Messages eve's handler accepted and passed to the runtime. */
  const delivered: Array<{ queueName: string; messageId: string }> = [];
  /**
   * Every request the handler answered, tagged with the message it carried.
   *
   * The dispatcher claims across all tenants by design, and the fake activation
   * below hands out this port for any of them, so on a shared database another
   * suite's leftover run can be swept up by the same boot recovery this test
   * calls and land here — same topic, different deployment, refused 401 by the
   * deployment binding. That is the binding working. Assertions therefore key on
   * the message id, which is the only thing that identifies *this* dispatch.
   */
  const answered: Array<{ queueName: string | null; messageId: string | null; status: number }> =
    [];

  beforeAll(async () => {
    process.env.EVELAND_SCHEDULER_RUNTIME_SECRET = RUNTIME_SECRET;
    process.env.EVELAND_DEPLOYMENT_ID = DEPLOYMENT;

    admin = new Pool({ connectionString: testUrl, max: 4 });
    await runMigrations(admin);
    await ensureTenantPartitions(admin, TENANT);

    world = createWorld({
      connectionString: testUrl!,
      tenantId: TENANT,
      deploymentId: DEPLOYMENT,
      runner: "external",
      queueNamespace: NAMESPACE,
    });

    // The namespaced registration, which is the point: this handler owns
    // `__acme_wkf_workflow_*` and nothing else.
    const handler = world.createQueueHandler(
      getQueueTopicPrefix("workflow", NAMESPACE),
      async (_message, meta) => {
        delivered.push({ queueName: meta.queueName, messageId: meta.messageId });
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
          answered.push({
            queueName: (req.headers["x-vqs-queue-name"] as string | undefined) ?? null,
            messageId: (req.headers["x-vqs-message-id"] as string | undefined) ?? null,
            status: response.status,
          });
          res.writeHead(response.status, { "content-type": "application/json" });
          res.end(await response.text());
        })();
      });
    });
    await new Promise<void>((resolve) => agent.listen(0, "127.0.0.1", resolve));
    const address = agent.address();
    if (typeof address === "string" || !address) throw new Error("no port");
    agentPort = address.port;

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

    dispatcherPool = new Pool({ connectionString: testUrl, max: 4 });
    dispatcher = await startDispatcher({
      pool: dispatcherPool,
      config: {
        concurrency: 4,
        pollIntervalMs: 100,
        maxInFlightPerTenant: 4,
        queueGcIntervalMs: 3_600_000,
      },
      deps: {
        activation,
        runtimeSecret: RUNTIME_SECRET,
        dispatchTimeoutMs: 10_000,
        leaseRenewIntervalMs: 60_000,
        activationLeaseTtlMs: 180_000,
      },
    });
  }, 120_000);

  afterAll(async () => {
    await dispatcher?.stop().catch(() => {});
    await world?.close?.().catch(() => {});
    await new Promise<void>((resolve) => agent?.close(() => resolve()));
    await dispatcherPool?.end().catch(() => {});
    await admin
      .query("delete from workflow.dispatch_dead_letters where tenant_id = $1", [TENANT])
      .catch(() => {});
    await admin
      .query("delete from graphile_worker._private_jobs where payload->>'tenantId' = $1", [TENANT])
      .catch(() => {});
    // The sweep under test is global: it re-enqueues every active run in the
    // database, including ones other files left behind. Those jobs are this
    // file's doing, and a later file's dispatcher would claim them and refuse
    // them 401 for a deployment it is not.
    await admin
      .query("delete from graphile_worker._private_jobs where payload->>'messageId' like $1", [
        "msg_recover_%",
      ])
      .catch(() => {});
    // `workflow_runs` is NOT partitioned, so the partition drop below does not
    // reach it. An active run left here is one a later file's boot-recovery
    // sweep will re-enqueue into that file's agent.
    await admin
      .query("delete from workflow.workflow_runs where tenant_id = $1", [TENANT])
      .catch(() => {});
    await dropTenantPartitions(admin, TENANT).catch(() => {});
    await admin?.end().catch(() => {});
  });

  test("a namespaced run stranded by a dispatcher restart resumes without dead-lettering", async () => {
    const created = await world.events.create(null, {
      eventType: "run_created",
      eventData: { deploymentId: DEPLOYMENT, workflowName: "greet", input: [] },
      specVersion: 5,
    });
    const runId = created.run!.runId;

    // The dispatcher died mid-flight: the run is still active, its job is gone.
    await admin.query("delete from graphile_worker._private_jobs where payload->>'tenantId' = $1", [
      TENANT,
    ]);

    // Boot recovery is what the restarted dispatcher runs, and the running
    // dispatcher above is what claims the job it writes.
    const enqueued = await reenqueueActiveRunsForAllTenants({
      pool: admin,
      workerUtils: dispatcher.workerUtils,
    });
    expect(enqueued).toBeGreaterThan(0);

    await waitFor(() => delivered.length > 0, 30_000);

    // The topic the executor registered — not the default prefix, which this
    // handler does not own.
    expect(delivered[0]!.queueName).toBe("__acme_wkf_workflow_greet");
    expect(delivered[0]!.messageId).toBe(`msg_recover_${runId}`);

    // Nothing addressed to this deployment's topic was refused. The failure this
    // guards is a 400 "Unhandled queue" — what eve answers when the recovered
    // message names `__wkf_workflow_greet` at a handler registered for
    // `__acme_wkf_workflow_greet`.
    const ours = answered.filter((entry) => entry.messageId === `msg_recover_${runId}`);
    expect(ours.length).toBeGreaterThan(0);
    expect(ours.every((entry) => entry.queueName === "__acme_wkf_workflow_greet")).toBe(true);
    expect(ours.every((entry) => entry.status === 200)).toBe(true);

    // The observable symptom of the defect: a 400 is terminal, so a recovered
    // run that addressed the wrong topic landed here instead of resuming.
    const { rows } = await admin.query<{ reason: string }>(
      "select reason from workflow.dispatch_dead_letters where tenant_id = $1",
      [TENANT],
    );
    expect(rows).toEqual([]);
  }, 90_000);
});

async function waitFor(predicate: () => boolean, timeoutMs: number): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!predicate()) {
    if (Date.now() > deadline) throw new Error("timed out waiting for the dispatch loop");
    await new Promise((resolve) => setTimeout(resolve, 100));
  }
}
