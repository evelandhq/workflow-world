import type { WorkerUtils } from "graphile-worker";
import type { Pool } from "pg";
import { FLOW_JOB_NAME, runQueueName } from "./dispatch-contract.js";
import { readFlowJobRun } from "./job-migration.js";

/**
 * Durable run quarantine: the marker every dispatch surface honours.
 *
 * A quarantined run is excluded from boot recovery, refused by the dispatch
 * handler, and rejected by the deployment-side enqueue — fail closed on all
 * three, because a control-plane fence alone cannot stop a job that is already
 * sitting in the graphile table. Existing jobs are parked (pushed to a
 * far-future `run_at`) rather than deleted: a continuation or hook input that
 * never reached the event log exists only in that payload.
 */

export type RunQuarantine = {
  tenantId: string;
  runId: string;
  operationId: string;
  reason: string;
  createdAt: Date;
  resolvedAt: Date | null;
};

/**
 * Far enough that no claim happens while the marker stands, near enough to
 * stay inside every timestamp range. Resolution reschedules parked jobs.
 */
export const QUARANTINE_PARK_RUN_AT = new Date("9999-01-01T00:00:00.000Z");

export async function quarantineRun(
  pool: Pool,
  workerUtils: WorkerUtils,
  input: { tenantId: string; runId: string; operationId: string; reason: string },
): Promise<void> {
  // Upsert keeps one row per run; re-quarantining an already-marked run under
  // a new operation refreshes the reason and reopens a resolved marker.
  await pool.query(
    `insert into workflow.run_quarantines (tenant_id, run_id, operation_id, reason)
     values ($1, $2, $3, $4)
     on conflict (tenant_id, run_id)
     do update set operation_id = excluded.operation_id,
                   reason = excluded.reason,
                   created_at = now(),
                   resolved_at = null,
                   resolved_by = null`,
    [input.tenantId, input.runId, input.operationId, input.reason],
  );
  await parkRunJobs(pool, workerUtils, input.tenantId, input.runId);
}

/**
 * Park every runnable job addressed at this run without touching its payload,
 * identity or attempt history. Membership is decided by the job's own decoded
 * payload or its exact per-run queue — never by a `wfrun:` prefix, which would
 * miss a delivery parked on some other run's queue. Payloads are decoded in
 * JS: a malformed body must not fail the sweep.
 */
export async function parkRunJobs(
  pool: Pool,
  workerUtils: WorkerUtils,
  tenantId: string,
  runId: string,
): Promise<number> {
  const targets = await selectRunJobs(pool, tenantId, runId, "runnable");
  if (targets.length === 0) return 0;
  // Only run_at moves; payload, identity, priority and attempt history stay.
  await workerUtils.rescheduleJobs(targets, { runAt: QUARANTINE_PARK_RUN_AT });
  return targets.length;
}

/**
 * Close the marker. Parked jobs stay parked deliberately: what happens to them
 * (release, migrate, terminate) is the resolving operation's explicit decision
 * via {@link releaseParkedRunJobs}, not a side effect of closing the marker.
 */
export async function resolveRunQuarantine(
  pool: Pool,
  input: { tenantId: string; runId: string; resolvedBy: string },
): Promise<boolean> {
  const result = await pool.query(
    `update workflow.run_quarantines
        set resolved_at = now(), resolved_by = $3
      where tenant_id = $1 and run_id = $2 and resolved_at is null`,
    [input.tenantId, input.runId, input.resolvedBy],
  );
  return (result.rowCount ?? 0) > 0;
}

/** Make this run's parked jobs claimable again (marker must be resolved first). */
export async function releaseParkedRunJobs(
  pool: Pool,
  workerUtils: WorkerUtils,
  tenantId: string,
  runId: string,
): Promise<number> {
  if (await isRunQuarantined(pool, tenantId, runId)) {
    throw new Error(
      `Run ${runId} of tenant ${tenantId} still has an unresolved quarantine marker; resolve it before releasing its jobs.`,
    );
  }
  const targets = await selectRunJobs(pool, tenantId, runId, "parked");
  if (targets.length === 0) return 0;
  await workerUtils.rescheduleJobs(targets, { runAt: new Date() });
  return targets.length;
}

/**
 * Flow jobs belonging to one run, matched by exact per-run queue or decoded
 * payload. `runnable` selects jobs a worker could still claim; `parked`
 * selects the ones a quarantine pushed to the far-future run_at.
 */
async function selectRunJobs(
  pool: Pool,
  tenantId: string,
  runId: string,
  which: "runnable" | "parked",
): Promise<string[]> {
  const { rows } = await pool.query<{
    id: string;
    payload: unknown;
    queue_name: string | null;
  }>(
    `select jobs.id::text as id, jobs.payload, queues.queue_name
       from graphile_worker._private_jobs as jobs
       join graphile_worker._private_tasks as tasks on tasks.id = jobs.task_id
       left join graphile_worker._private_job_queues as queues on queues.id = jobs.job_queue_id
      where tasks.identifier = $1
        and jobs.locked_by is null
        and ${which === "runnable" ? "jobs.run_at < $2" : "jobs.run_at >= $2"}`,
    [FLOW_JOB_NAME, QUARANTINE_PARK_RUN_AT],
  );
  const exactQueue = runQueueName(tenantId, runId);
  return rows
    .filter((row) => {
      if (row.queue_name === exactQueue) return true;
      const run = readFlowJobRun(row.payload);
      return run !== undefined && run.tenantId === tenantId && run.runId === runId;
    })
    .map((row) => row.id);
}

export async function isRunQuarantined(
  pool: Pool,
  tenantId: string,
  runId: string,
): Promise<boolean> {
  const { rows } = await pool.query(
    `select 1 from workflow.run_quarantines
      where tenant_id = $1 and run_id = $2 and resolved_at is null`,
    [tenantId, runId],
  );
  return rows.length > 0;
}

export async function listUnresolvedRunQuarantines(
  pool: Pool,
  tenantId?: string,
): Promise<RunQuarantine[]> {
  const { rows } = await pool.query<{
    tenant_id: string;
    run_id: string;
    operation_id: string;
    reason: string;
    created_at: Date;
    resolved_at: Date | null;
  }>(
    `select tenant_id, run_id, operation_id, reason, created_at, resolved_at
       from workflow.run_quarantines
      where resolved_at is null
        and ($1::text is null or tenant_id = $1)
      order by tenant_id, run_id`,
    [tenantId ?? null],
  );
  return rows.map((row) => ({
    tenantId: row.tenant_id,
    runId: row.run_id,
    operationId: row.operation_id,
    reason: row.reason,
    createdAt: row.created_at,
    resolvedAt: row.resolved_at,
  }));
}
