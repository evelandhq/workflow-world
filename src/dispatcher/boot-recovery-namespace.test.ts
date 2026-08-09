import { getQueueTopicPrefix, type ValidQueueName } from "@workflow/world";
import { makeWorkerUtils, type WorkerUtils } from "graphile-worker";
import { Pool } from "pg";
import { afterAll, beforeAll, describe, expect, test } from "vitest";
import { createWorld, ensureTenantPartitions, runMigrations } from "../index.js";
import { MessageData } from "../message.js";
import { dropTenantPartitions } from "../migrate.js";
import { reenqueueActiveRunsForAllTenants } from "./boot-recovery.js";

/**
 * Boot recovery has to preserve eve's queue namespace.
 *
 * `queue-namespace.test.ts` pins the *live* enqueue path: the deployment
 * resolves the namespace, `parseQueueName` strips the prefix off the name, and
 * the resolved value is recorded on the message so the delivery side can rebuild
 * `__<ns>_wkf_workflow_<id>`. That path was already right.
 *
 * The external dispatcher's boot sweep is the one that is not. It reconstructs
 * `MessageData` from a run row rather than from an enqueue call, so it has no
 * deployment closure to read the namespace from — and the dispatcher's own
 * environment is the wrong authority, because it runs on the host rather than in
 * the tenant's container. Reconstructing without a namespace addresses
 * `__wkf_workflow_<name>` at an executor that registered
 * `__<ns>_wkf_workflow_<name>`, which eve answers 400 "Unhandled queue" — and a
 * 400 is non-retryable, so every recovered run dead-letters and the run stays
 * active for the next boot to try again.
 *
 * The namespace therefore has to be durable on the run itself. These tests pin
 * that it is written at run creation and read back by the sweep.
 *
 * Set `EVELAND_WORKFLOW_WORLD_TEST_URL` to a scratch database to run them.
 */
const testUrl = process.env.EVELAND_WORKFLOW_WORLD_TEST_URL;
const suffix = `${String(process.pid)}${Date.now().toString(36)}`;
const NAMESPACED = `p_bootns_a_${suffix}`;
const PLAIN = `p_bootns_b_${suffix}`;
const LEGACY = `p_bootns_c_${suffix}`;

describe.skipIf(!testUrl)("boot recovery preserves the queue namespace", () => {
  let admin: Pool;
  let workerUtils: WorkerUtils;
  const worlds: Array<ReturnType<typeof createWorld>> = [];

  beforeAll(async () => {
    admin = new Pool({ connectionString: testUrl, max: 4 });
    await runMigrations(admin);
    for (const tenant of [NAMESPACED, PLAIN, LEGACY]) {
      await ensureTenantPartitions(admin, tenant);
    }
    workerUtils = await makeWorkerUtils({ pgPool: admin });
    await workerUtils.migrate();
  }, 60_000);

  afterAll(async () => {
    await Promise.all(worlds.map(async (world) => await world.close?.()));
    await workerUtils?.release();
    // These jobs are never claimed — every world here is `external` and no
    // dispatcher runs in this file — so they would sit in the shared graphile
    // table for whichever later suite starts a real one.
    await admin
      .query("delete from graphile_worker._private_jobs where payload->>'tenantId' = any($1)", [
        [NAMESPACED, PLAIN, LEGACY],
      ])
      .catch(() => {});
    // The sweep under test is global: it re-enqueues every active run in the
    // database, including ones other files left behind. Those jobs are this
    // file's doing and nobody else's, and a later file's dispatcher would claim
    // them and refuse them 401 for a deployment it is not.
    await admin
      .query("delete from graphile_worker._private_jobs where payload->>'messageId' like $1", [
        "msg_recover_%",
      ])
      .catch(() => {});
    // `workflow_runs` is NOT partitioned, so dropping the partitions leaves these
    // rows behind — and they are `pending`, which is exactly what a later file's
    // boot-recovery sweep re-enqueues. Those deliveries then reach whatever agent
    // that file stood up, get refused 401 by the deployment binding, and look
    // like a product bug in a suite that shares one database.
    await admin
      .query("delete from workflow.workflow_runs where tenant_id = any($1)", [
        [NAMESPACED, PLAIN, LEGACY],
      ])
      .catch(() => {});
    for (const tenant of [NAMESPACED, PLAIN, LEGACY]) {
      await dropTenantPartitions(admin, tenant).catch(() => {});
    }
    await admin?.end().catch(() => {});
  });

  /**
   * A run created through the real event path, exactly as a deployment creates
   * one. Seeding the row directly would let the test pass while the production
   * write path still dropped the namespace — which is the whole defect.
   */
  async function createActiveRun(input: {
    tenantId: string;
    queueNamespace?: string;
    workflowName: string;
  }): Promise<string> {
    const world = createWorld({
      connectionString: testUrl!,
      tenantId: input.tenantId,
      deploymentId: `dep_${input.tenantId}`,
      // `external` so no in-process runner claims the job before we read it.
      runner: "external",
      ...(input.queueNamespace !== undefined ? { queueNamespace: input.queueNamespace } : {}),
    });
    worlds.push(world);

    const created = await world.events.create(null, {
      eventType: "run_created",
      eventData: {
        deploymentId: `dep_${input.tenantId}`,
        workflowName: input.workflowName,
        input: [],
      },
      specVersion: 5,
    });
    const runId = created.run!.runId;

    // The original delivery, then its loss: this is the state boot recovery
    // exists for — an active run with no job left to wake it.
    await world.queue(
      `${getQueueTopicPrefix("workflow", input.queueNamespace)}${input.workflowName}` as ValidQueueName,
      { runId },
    );
    await admin.query("delete from graphile_worker._private_jobs where payload->>'tenantId' = $1", [
      input.tenantId,
    ]);

    return runId;
  }

  /** The message the sweep reconstructed for one run. */
  async function recoveredMessage(runId: string): Promise<MessageData> {
    const { rows } = await admin.query<{ payload: unknown }>(
      "select payload from graphile_worker._private_jobs where payload->>'messageId' = $1",
      [`msg_recover_${runId}`],
    );
    expect(rows).toHaveLength(1);
    return MessageData.parse(rows[0]!.payload);
  }

  test("a namespaced run recovers to the topic its executor actually owns", async () => {
    const runId = await createActiveRun({
      tenantId: NAMESPACED,
      queueNamespace: "acme",
      workflowName: "greet",
    });

    await reenqueueActiveRunsForAllTenants({ pool: admin, workerUtils });

    const message = await recoveredMessage(runId);
    expect(message.queueNamespace).toBe("acme");
    // Built the way the dispatcher's handler builds it.
    expect(`${getQueueTopicPrefix("workflow", message.queueNamespace)}${message.id}`).toBe(
      "__acme_wkf_workflow_greet",
    );
  }, 60_000);

  test("an un-namespaced run still recovers to the default prefix", async () => {
    const runId = await createActiveRun({ tenantId: PLAIN, workflowName: "greet" });

    await reenqueueActiveRunsForAllTenants({ pool: admin, workerUtils });

    const message = await recoveredMessage(runId);
    // Absent rather than empty on the wire, so a dispatcher running older code
    // parses it identically to a message enqueued before the field existed.
    expect(message.queueNamespace).toBeUndefined();
    expect(`${getQueueTopicPrefix("workflow", message.queueNamespace)}${message.id}`).toBe(
      "__wkf_workflow_greet",
    );
  }, 60_000);

  test("a run created before the column existed is reported, not silently defaulted", async () => {
    // A row from before the migration. NULL there is genuinely ambiguous: it can
    // mean an un-namespaced run, or a namespaced one created when there was
    // nowhere to record the namespace. New rows never look like this — they
    // write the empty string for "no namespace" — so the sweep can tell the two
    // apart and say so rather than guessing quietly.
    const runId = await createActiveRun({
      tenantId: LEGACY,
      queueNamespace: "acme",
      workflowName: "greet",
    });
    await admin.query(
      "update workflow.workflow_runs set queue_namespace = null where tenant_id = $1 and id = $2",
      [LEGACY, runId],
    );

    const logged: Array<{ message: string; meta?: Record<string, unknown> }> = [];
    await reenqueueActiveRunsForAllTenants({
      pool: admin,
      workerUtils,
      log: (message, meta) => logged.push({ message, ...(meta ? { meta } : {}) }),
    });

    const message = await recoveredMessage(runId);
    expect(message.queueNamespace).toBeUndefined();

    const warning = logged.find((entry) => entry.meta?.runId === runId);
    expect(warning).toBeDefined();
    expect(warning!.message).toMatch(/namespace/i);
  }, 60_000);
});
