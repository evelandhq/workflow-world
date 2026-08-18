import { getQueueTopicPrefix, type ValidQueueName } from "@workflow/world";
import { makeWorkerUtils, type WorkerUtils } from "graphile-worker";
import { Pool } from "pg";
import { afterAll, beforeAll, describe, expect, test } from "vitest";
import { FLOW_JOB_NAME, runQueueName } from "./dispatch-contract.js";
import { createWorld, ensureTenantPartitions, runMigrations } from "./index.js";
import { countClaimableUnscopedFlowJobs, migrateUnscopedRunJobs } from "./job-migration.js";
import { MessageData } from "./message.js";
import { dropTenantPartitions } from "./migrate.js";

/**
 * The early-external in-place job migration: jobs enqueued before per-run
 * serialization existed sit in the plain queue and must be re-parented onto
 * `wfrun:<tenant>:<run>` without losing identity, payload, schedule or attempt
 * history. Anything unprovable is parked, never guessed at, and the
 * postcondition count is what lets a dispatcher refuse to start while any
 * claimable unscoped job remains.
 *
 * Set `EVELAND_WORKFLOW_WORLD_TEST_URL` to a scratch database to run them.
 */
const testUrl = process.env.EVELAND_WORKFLOW_WORLD_TEST_URL;
const suffix = `${String(process.pid)}${Date.now().toString(36)}`;
const TENANT = `p_jobmig_${suffix}`;
const DEPLOYMENT = `dep_jobmig_${suffix}`;

describe.skipIf(!testUrl)("early-external unscoped job migration", () => {
  let admin: Pool;
  let workerUtils: WorkerUtils;
  const worlds: Array<ReturnType<typeof createWorld>> = [];

  beforeAll(async () => {
    admin = new Pool({ connectionString: testUrl, max: 4 });
    await runMigrations(admin);
    await ensureTenantPartitions(admin, TENANT);
    workerUtils = await makeWorkerUtils({ pgPool: admin });
    await workerUtils.migrate();
  }, 60_000);

  afterAll(async () => {
    await Promise.all(worlds.map(async (world) => await world.close?.()));
    await workerUtils?.release();
    await admin
      .query("delete from graphile_worker._private_jobs where payload->>'tenantId' = $1", [TENANT])
      .catch(() => {});
    await admin
      .query("delete from workflow.workflow_runs where tenant_id = $1", [TENANT])
      .catch(() => {});
    await dropTenantPartitions(admin, TENANT).catch(() => {});
    await admin?.end().catch(() => {});
  });

  async function createActiveRun(workflowName: string, queueNamespace?: string): Promise<string> {
    const world = createWorld({
      connectionString: testUrl!,
      tenantId: TENANT,
      deploymentId: DEPLOYMENT,
      runner: "external",
      ...(queueNamespace !== undefined ? { queueNamespace } : {}),
    });
    worlds.push(world);
    const created = await world.events.create(null, {
      eventType: "run_created",
      eventData: { deploymentId: DEPLOYMENT, workflowName, input: [] },
      specVersion: 5,
    });
    const runId = created.run!.runId;
    await world.queue(
      `${getQueueTopicPrefix("workflow", queueNamespace)}${workflowName}` as ValidQueueName,
      { runId },
    );
    // Drop the modern (already scoped) delivery; this suite fabricates the
    // early-external shape below.
    // CASE guards the decode: the malformed-payload fixture elsewhere in this
    // suite must not blow up this cleanup.
    await admin.query(
      `delete from graphile_worker._private_jobs
        where payload->>'tenantId' = $1
          and case
                when payload->>'data' ~ '^[A-Za-z0-9+/=]*$'
                then convert_from(decode(payload->>'data', 'base64'), 'utf8')::jsonb->>'runId'
              end = $2`,
      [TENANT, runId],
    );
    return runId;
  }

  /** A job exactly as the early external generation enqueued it: no queueName. */
  async function addUnscopedJob(
    runId: string,
    options: { messageId?: string; namespace?: string; deploymentId?: string } = {},
  ): Promise<string> {
    const messageId = (options.messageId ??
      `msg_early_${runId}_${Math.random().toString(36).slice(2)}`) as MessageData["messageId"];
    const message: MessageData = {
      id: "greet",
      data: Buffer.from(JSON.stringify({ runId })),
      attempt: 1,
      messageId,
      tenantId: TENANT,
      deploymentId: options.deploymentId ?? DEPLOYMENT,
      ...(options.namespace !== undefined ? { queueNamespace: options.namespace } : {}),
    };
    const job = await workerUtils.addJob(FLOW_JOB_NAME, MessageData.encode(message), {
      maxAttempts: 10,
      flags: [`project:${TENANT}`],
    });
    return String(job.id);
  }

  async function jobRow(jobId: string) {
    const { rows } = await admin.query<{
      id: string;
      payload: unknown;
      attempts: number;
      run_at: Date;
      queue_name: string | null;
      key: string | null;
    }>(
      `select jobs.id::text as id, jobs.payload, jobs.attempts, jobs.run_at, jobs.key,
              queues.queue_name
         from graphile_worker._private_jobs as jobs
         left join graphile_worker._private_job_queues as queues on queues.id = jobs.job_queue_id
        where jobs.id = $1::bigint`,
      [jobId],
    );
    return rows[0] ?? null;
  }

  test("scopes provable jobs in place, preserves everything else, and is idempotent", async () => {
    const runId = await createActiveRun("greet");
    const first = await addUnscopedJob(runId);
    const second = await addUnscopedJob(runId);
    expect(await countClaimableUnscopedFlowJobs(admin)).toBeGreaterThanOrEqual(2);
    const before = await jobRow(first);

    const result = await migrateUnscopedRunJobs(admin);
    expect(result.scoped).toBeGreaterThanOrEqual(2);
    expect(result.parked).toEqual([]);

    // Same job id, same payload, same schedule and attempts — only the queue moved.
    const after = await jobRow(first);
    expect(after?.id).toBe(before?.id);
    expect(after?.payload).toEqual(before?.payload);
    expect(after?.attempts).toBe(before?.attempts);
    expect(after?.run_at.toISOString()).toBe(before?.run_at.toISOString());
    expect(after?.queue_name).toBe(runQueueName(TENANT, runId));
    // Both old jobs share the run's one queue: graphile serializes them.
    expect((await jobRow(second))?.queue_name).toBe(runQueueName(TENANT, runId));

    expect(await countClaimableUnscopedFlowJobs(admin)).toBe(0);

    // Re-running migrates nothing further and duplicates nothing.
    const again = await migrateUnscopedRunJobs(admin);
    expect(again.scoped).toBe(0);
    expect(again.alreadyScoped).toBeGreaterThanOrEqual(2);
    const { rows: countRows } = await admin.query<{ count: string }>(
      `select count(*)::text as count from graphile_worker._private_jobs
        where payload->>'tenantId' = $1
          and convert_from(decode(payload->>'data', 'base64'), 'utf8')::jsonb->>'runId' = $2`,
      [TENANT, runId],
    );
    expect(Number(countRows[0]!.count)).toBe(2);
  }, 60_000);

  test("different runs keep different queues — migration must not serialize globally", async () => {
    const runA = await createActiveRun("greet");
    const runB = await createActiveRun("greet");
    const jobA = await addUnscopedJob(runA);
    const jobB = await addUnscopedJob(runB);

    await migrateUnscopedRunJobs(admin);

    expect((await jobRow(jobA))?.queue_name).toBe(runQueueName(TENANT, runA));
    expect((await jobRow(jobB))?.queue_name).toBe(runQueueName(TENANT, runB));
    expect(runQueueName(TENANT, runA)).not.toBe(runQueueName(TENANT, runB));
  }, 60_000);

  test("backfills a NULL run namespace from a consistent payload as immutable provenance", async () => {
    const runId = await createActiveRun("greet");
    await admin.query(
      "update workflow.workflow_runs set queue_namespace = null where tenant_id = $1 and id = $2",
      [TENANT, runId],
    );
    await addUnscopedJob(runId, { namespace: "acme" });

    const result = await migrateUnscopedRunJobs(admin);
    expect(result.backfilledNamespaces).toBeGreaterThanOrEqual(1);

    const { rows } = await admin.query<{ queue_namespace: string | null }>(
      "select queue_namespace from workflow.workflow_runs where tenant_id = $1 and id = $2",
      [TENANT, runId],
    );
    expect(rows[0]?.queue_namespace).toBe("acme");
  }, 60_000);

  test("a job on the WRONG per-run queue is re-parented onto its exact queue", async () => {
    const runA = await createActiveRun("greet");
    const runB = await createActiveRun("greet");
    const jobId = await addUnscopedJob(runA);
    // A `wfrun:` prefix proves nothing: park run A's job on run B's queue.
    await admin.query(
      `insert into graphile_worker._private_job_queues (queue_name)
       values ($1) on conflict (queue_name) do nothing`,
      [runQueueName(TENANT, runB)],
    );
    await admin.query(
      `update graphile_worker._private_jobs
          set job_queue_id = (select id from graphile_worker._private_job_queues where queue_name = $2)
        where id = $1::bigint`,
      [jobId, runQueueName(TENANT, runB)],
    );

    // The postcondition must refuse this state — wrong-queue serialization is
    // exactly the replay race the exact queue exists to prevent.
    expect(await countClaimableUnscopedFlowJobs(admin)).toBeGreaterThanOrEqual(1);

    const result = await migrateUnscopedRunJobs(admin);
    expect(result.scoped).toBeGreaterThanOrEqual(1);
    expect((await jobRow(jobId))?.queue_name).toBe(runQueueName(TENANT, runA));
    expect(await countClaimableUnscopedFlowJobs(admin)).toBe(0);
  }, 60_000);

  test("a malformed payload body neither crashes the count nor gets guessed at", async () => {
    // Not valid base64/JSON in `data`; the count and migration must survive it
    // and treat the job as unprovable.
    const job = await workerUtils.addJob("eveland_wf_flows", {
      tenantId: TENANT,
      data: "%%not-base64%%",
      id: "greet",
      attempt: 1,
      messageId: "msg_malformed_1",
      deploymentId: DEPLOYMENT,
    });
    expect(await countClaimableUnscopedFlowJobs(admin)).toBeGreaterThanOrEqual(1);

    const result = await migrateUnscopedRunJobs(admin);
    expect(result.parked.map((entry) => entry.jobId)).toContain(String(job.id));
    expect(await countClaimableUnscopedFlowJobs(admin)).toBe(0);
  }, 60_000);

  test("parks what it cannot prove instead of guessing, and the postcondition clears", async () => {
    const runId = await createActiveRun("greet");
    // Namespace conflict: the run recorded one value, the job carries another.
    await admin.query(
      "update workflow.workflow_runs set queue_namespace = 'real' where tenant_id = $1 and id = $2",
      [TENANT, runId],
    );
    const conflicted = await addUnscopedJob(runId, { namespace: "forged" });
    // Owner conflict: the job names a deployment that does not own the run.
    const foreign = await addUnscopedJob(runId, { deploymentId: "dep_other" });
    // A run nothing recorded.
    const orphan = await addUnscopedJob("run_missing_entirely");
    // A payload that is not a workflow message at all.
    const garbageJob = await workerUtils.addJob(FLOW_JOB_NAME, { tenantId: TENANT, junk: true });

    const result = await migrateUnscopedRunJobs(admin);
    const parkedIds = result.parked.map((entry) => entry.jobId);
    expect(parkedIds).toEqual(
      expect.arrayContaining([conflicted, foreign, orphan, String(garbageJob.id)]),
    );

    // Parked, not deleted: payloads survive for diagnosis and explicit release.
    for (const jobId of [conflicted, foreign, orphan]) {
      const row = await jobRow(jobId);
      expect(row).not.toBeNull();
      expect(row!.run_at.getFullYear()).toBeGreaterThan(9000);
    }
    // And none of them count as claimable for the startup postcondition.
    expect(await countClaimableUnscopedFlowJobs(admin)).toBe(0);
  }, 60_000);
});
