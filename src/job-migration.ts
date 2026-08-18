import type { Pool, PoolClient } from "pg";
import { FLOW_JOB_NAME, runQueueName } from "./dispatch-contract.js";
import { MessageData } from "./message.js";
import { QUARANTINE_PARK_RUN_AT } from "./quarantine.js";

/**
 * In-place migration for early-external Graphile jobs that predate per-run
 * serialization (3f0f483). Those jobs sit in the plain queue, so a 0.9.0
 * dispatcher would expose them to its worker pool concurrently with its own
 * per-run recovery jobs — the exact replay race the per-run queue closes.
 *
 * The migration only re-parents each job onto its run's exact
 * `wfrun:<tenant>:<run>` queue. Identity, payload, key, priority, schedule and
 * attempt history are untouched; deleting and re-enqueueing is forbidden
 * because a continuation or hook input that never reached the event log exists
 * only in that payload. Jobs that cannot be proven — undecodable payload, no
 * run id, missing run row, conflicting owner or namespace — are parked
 * unclaimable with their payload preserved, never guessed at.
 *
 * Graphile's private schema is a version boundary: this helper is pinned to
 * the graphile-worker release this package ships and tested against the real
 * schema. Hosts must not reimplement it.
 */

export type UnscopedJobMigrationResult = {
  /** Jobs re-parented onto their run's exact queue by this invocation. */
  scoped: number;
  /** Flow jobs already on a per-run queue; untouched. */
  alreadyScoped: number;
  /** Run rows whose NULL queue_namespace was backfilled from a proven payload. */
  backfilledNamespaces: number;
  /** Jobs parked unclaimable because their provenance could not be proven. */
  parked: Array<{ jobId: string; reason: string }>;
};

/** Serializes concurrent migration attempts; never held by normal operation. */
const JOB_MIGRATION_LOCK_KEY = 0x65_76_6a_6d; // "evjm"

export async function migrateUnscopedRunJobs(
  pool: Pool,
  options: { log?: (message: string, meta?: Record<string, unknown>) => void } = {},
): Promise<UnscopedJobMigrationResult> {
  const log = options.log ?? (() => {});
  const client = await pool.connect();
  const result: UnscopedJobMigrationResult = {
    scoped: 0,
    alreadyScoped: 0,
    backfilledNamespaces: 0,
    parked: [],
  };
  try {
    await client.query("select pg_advisory_lock($1)", [JOB_MIGRATION_LOCK_KEY]);

    const alreadyScoped = await client.query<{ count: string }>(
      `select count(*)::text as count
         from graphile_worker._private_jobs as jobs
         join graphile_worker._private_tasks as tasks on tasks.id = jobs.task_id
         join graphile_worker._private_job_queues as queues on queues.id = jobs.job_queue_id
        where tasks.identifier = $1
          and queues.queue_name like 'wfrun:%'`,
      [FLOW_JOB_NAME],
    );
    result.alreadyScoped = Number(alreadyScoped.rows[0]!.count);

    const { rows: candidates } = await client.query<{
      id: string;
      payload: unknown;
    }>(
      `select jobs.id::text as id, jobs.payload
         from graphile_worker._private_jobs as jobs
         join graphile_worker._private_tasks as tasks on tasks.id = jobs.task_id
         left join graphile_worker._private_job_queues as queues on queues.id = jobs.job_queue_id
        where tasks.identifier = $1
          and (queues.queue_name is null or queues.queue_name not like 'wfrun:%')
          and jobs.locked_by is null
        order by jobs.id`,
      [FLOW_JOB_NAME],
    );

    for (const candidate of candidates) {
      const verdict = await assessCandidate(client, candidate.payload);
      if (verdict.outcome === "park") {
        await parkJob(client, candidate.id);
        result.parked.push({ jobId: candidate.id, reason: verdict.reason });
        log("parked an unprovable early-external job", {
          jobId: candidate.id,
          reason: verdict.reason,
        });
        continue;
      }
      // One transaction per job: the queue re-parent and any namespace
      // backfill land together or not at all, and a mid-run crash leaves every
      // untouched job exactly where a re-run will find it.
      await client.query("begin");
      try {
        if (verdict.backfillNamespace) {
          await client.query(
            `update workflow.workflow_runs
                set queue_namespace = $3
              where tenant_id = $1 and id = $2 and queue_namespace is null`,
            [verdict.tenantId, verdict.runId, verdict.backfillNamespace],
          );
          result.backfilledNamespaces += 1;
        }
        const queueName = runQueueName(verdict.tenantId, verdict.runId);
        await client.query(
          `insert into graphile_worker._private_job_queues (queue_name)
           values ($1)
           on conflict (queue_name) do nothing`,
          [queueName],
        );
        const updated = await client.query(
          `update graphile_worker._private_jobs
              set job_queue_id = (
                    select id from graphile_worker._private_job_queues where queue_name = $2
                  ),
                  updated_at = now()
            where id = $1::bigint and locked_by is null`,
          [candidate.id, queueName],
        );
        await client.query("commit");
        if ((updated.rowCount ?? 0) > 0) {
          result.scoped += 1;
          log("scoped an early-external job onto its run queue", {
            jobId: candidate.id,
            queueName,
          });
        }
      } catch (error) {
        await client.query("rollback").catch(() => {});
        throw error;
      }
    }
    return result;
  } finally {
    await client.query("select pg_advisory_unlock($1)", [JOB_MIGRATION_LOCK_KEY]).catch(() => {});
    client.release();
  }
}

type CandidateVerdict =
  | { outcome: "park"; reason: string }
  | {
      outcome: "scope";
      tenantId: string;
      runId: string;
      backfillNamespace?: string;
    };

async function assessCandidate(client: PoolClient, payload: unknown): Promise<CandidateVerdict> {
  const parsed = MessageData.safeParse(payload);
  if (!parsed.success) {
    return { outcome: "park", reason: "payload does not decode as a workflow message" };
  }
  const message = parsed.data;
  const runId = readRunIdFromBody(message.data);
  if (!runId) {
    return { outcome: "park", reason: "message body names no workflow run" };
  }
  const run = await client.query<{
    deployment_id: string | null;
    queue_namespace: string | null;
  }>(
    `select deployment_id, queue_namespace from workflow.workflow_runs where tenant_id = $1 and id = $2`,
    [message.tenantId, runId],
  );
  const row = run.rows[0];
  if (!row) {
    return {
      outcome: "park",
      reason: `run ${runId} does not exist for tenant ${message.tenantId}`,
    };
  }
  if (!row.deployment_id) {
    return { outcome: "park", reason: `run ${runId} records no owner deployment` };
  }
  if (message.deploymentId && message.deploymentId !== row.deployment_id) {
    return {
      outcome: "park",
      reason: `job names deployment ${message.deploymentId} but run ${runId} is owned by ${row.deployment_id}`,
    };
  }
  const messageNamespace = message.queueNamespace;
  if (row.queue_namespace !== null) {
    if (messageNamespace !== undefined && messageNamespace !== row.queue_namespace) {
      return {
        outcome: "park",
        reason: `job namespace "${messageNamespace}" conflicts with run namespace "${row.queue_namespace}"`,
      };
    }
    return { outcome: "scope", tenantId: message.tenantId, runId };
  }
  // NULL means "the creator never recorded one". A consistent payload value is
  // admissible evidence and is backfilled as immutable run provenance.
  return {
    outcome: "scope",
    tenantId: message.tenantId,
    runId,
    ...(messageNamespace !== undefined ? { backfillNamespace: messageNamespace } : {}),
  };
}

function readRunIdFromBody(data: Uint8Array): string | undefined {
  try {
    const body = JSON.parse(Buffer.from(data).toString("utf8")) as {
      runId?: unknown;
      workflowRunId?: unknown;
    };
    if (typeof body.runId === "string") return body.runId;
    if (typeof body.workflowRunId === "string") return body.workflowRunId;
  } catch {
    // fall through
  }
  return undefined;
}

async function parkJob(client: PoolClient, jobId: string): Promise<void> {
  await client.query(
    `update graphile_worker._private_jobs
        set run_at = $2, updated_at = now()
      where id = $1::bigint and locked_by is null`,
    [jobId, QUARANTINE_PARK_RUN_AT],
  );
}

/**
 * The dispatcher-startup postcondition: how many flow jobs are still claimable
 * outside a per-run queue. Non-zero means boot recovery and resume must fail
 * closed — a worker pool would race those jobs against per-run deliveries.
 */
export async function countClaimableUnscopedFlowJobs(pool: Pool): Promise<number> {
  const { rows } = await pool.query<{ count: string }>(
    `select count(*)::text as count
       from graphile_worker._private_jobs as jobs
       join graphile_worker._private_tasks as tasks on tasks.id = jobs.task_id
       left join graphile_worker._private_job_queues as queues on queues.id = jobs.job_queue_id
      where tasks.identifier = $1
        and (queues.queue_name is null or queues.queue_name not like 'wfrun:%')
        and jobs.locked_by is null
        and jobs.attempts < jobs.max_attempts
        and jobs.run_at <= now()`,
    [FLOW_JOB_NAME],
  );
  return Number(rows[0]!.count);
}
