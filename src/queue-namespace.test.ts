import { getQueueTopicPrefix, type ValidQueueName } from "@workflow/world";
import { Pool } from "pg";
import { afterAll, beforeAll, describe, expect, test } from "vitest";
import { MessageData } from "./message.js";
import { createWorld, ensureTenantPartitions, runMigrations } from "./index.js";
import { dropTenantPartitions } from "./migrate.js";

/**
 * eve's queue namespace has to survive the trip from the enqueuing deployment to
 * whichever process delivers the message.
 *
 * When `WORKFLOW_QUEUE_NAMESPACE` is set, eve's runtime registers handlers for
 * `__<namespace>_wkf_workflow_*`. `MessageData.id` carries only the bare
 * sub-queue id — the enqueue path strips the prefix through `parseQueueName` — so
 * the delivery side rebuilds the full name. Get the namespace wrong there and
 * every dispatch is answered "Unhandled queue" with a 400, which is
 * non-retryable, so every message dead-letters.
 *
 * The trap this guards is specific: the dispatcher runs on the HOST, in a
 * different process from the deployment. Resolving the namespace from its own
 * environment would read the host's value. So it must come off the message, and
 * the message must carry it.
 *
 * Note this is not tenancy. Tenancy is the `tenant_id` column; prefix-based
 * isolation is deliberately never used for it. This is eve's own naming, and it
 * has to round-trip faithfully rather than be normalised away.
 */
const testUrl = process.env.EVELAND_WORKFLOW_WORLD_TEST_URL;
const TENANT = "prj_queue_namespace";

describe.skipIf(!testUrl)("eve queue namespace round-trip", () => {
  let pool: Pool;

  beforeAll(async () => {
    pool = new Pool({ connectionString: testUrl, max: 4 });
    await runMigrations(pool);
    await ensureTenantPartitions(pool, TENANT);
  }, 60_000);

  afterAll(async () => {
    // These jobs are never claimed — the world runs in `external` mode and no
    // dispatcher is running here — so they would sit in the shared graphile table
    // and be picked up by whichever later suite starts a real dispatcher. Scoped
    // to this tenant's payloads; a blanket delete would eat other suites' jobs.
    await deleteOwnJobs();
    await dropTenantPartitions(pool, TENANT).catch(() => {});
    await pool.end().catch(() => {});
  });

  async function deleteOwnJobs(): Promise<void> {
    await pool
      .query(`delete from graphile_worker._private_jobs where payload->>'tenantId' = $1`, [TENANT])
      .catch(() => {
        // graphile's schema only exists once something has been enqueued.
      });
  }

  async function enqueueAndReadBack(queueNamespace: string | undefined): Promise<MessageData> {
    const world = createWorld({
      pool,
      tenantId: TENANT,
      deploymentId: "dep_ns_1",
      // `external` so no in-process runner claims the job before we can read it.
      runner: "external",
      ...(queueNamespace !== undefined ? { queueNamespace } : {}),
    });
    try {
      const prefix = getQueueTopicPrefix("workflow", queueNamespace);
      await world.queue(`${prefix}greet` as ValidQueueName, { runId: "wrun_ns_probe" });

      const { rows } = await pool.query<{ payload: unknown }>(
        `select payload from graphile_worker._private_jobs
          where task_id = (select id from graphile_worker._private_tasks
                            where identifier = 'eveland_wf_flows')
          order by id desc limit 1`,
      );
      return MessageData.parse(rows[0]?.payload);
    } finally {
      await world.close?.();
    }
  }

  test("a namespaced world records its namespace on the job", async () => {
    const message = await enqueueAndReadBack("acme");

    expect(message.queueNamespace).toBe("acme");
    // The stored id is bare: the prefix was stripped on the way in.
    expect(message.id).toBe("greet");
  }, 60_000);

  test("the delivery side reconstructs the namespaced name the executor registered", async () => {
    const message = await enqueueAndReadBack("acme");

    // This is exactly what the dispatcher's handler builds.
    const rebuilt = `${getQueueTopicPrefix("workflow", message.queueNamespace)}${message.id}`;
    expect(rebuilt).toBe("__acme_wkf_workflow_greet");

    // And the failure being prevented: ignoring the namespace addresses a queue
    // the executor does not own.
    expect(`${getQueueTopicPrefix("workflow")}${message.id}`).not.toBe(rebuilt);
  }, 60_000);

  test("an un-namespaced world omits the field and rebuilds the default prefix", async () => {
    const message = await enqueueAndReadBack(undefined);

    // Absent rather than empty, so a message enqueued before the field existed
    // parses identically to one enqueued now.
    expect(message.queueNamespace).toBeUndefined();
    expect(`${getQueueTopicPrefix("workflow", message.queueNamespace)}${message.id}`).toBe(
      "__wkf_workflow_greet",
    );
  }, 60_000);

  test("the namespace comes from the message, never from the delivering process", async () => {
    // Simulates the real hazard: the host has its own value set, and it differs.
    const previous = process.env.WORKFLOW_QUEUE_NAMESPACE;
    process.env.WORKFLOW_QUEUE_NAMESPACE = "host-side-value";
    try {
      const message = MessageData.parse({
        attempt: 1,
        messageId: "msg_ns",
        id: "greet",
        data: Buffer.from("{}").toString("base64"),
        tenantId: TENANT,
        deploymentId: "dep_ns_1",
        queueNamespace: "acme",
      });

      // Built the way the dispatcher builds it — from the message.
      expect(`${getQueueTopicPrefix("workflow", message.queueNamespace)}${message.id}`).toBe(
        "__acme_wkf_workflow_greet",
      );
    } finally {
      if (previous === undefined) delete process.env.WORKFLOW_QUEUE_NAMESPACE;
      else process.env.WORKFLOW_QUEUE_NAMESPACE = previous;
    }
  });
});
