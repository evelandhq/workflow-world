import { MessageData } from "../message.js";
import type { WorkerUtils } from "graphile-worker";
import type { Pool } from "pg";
import { FLOW_JOB_NAME } from "./runner.js";
import { runQueueName } from "../dispatch-contract.js";
import { MAX_GRAPHILE_JOB_ATTEMPTS } from "../queue-policy.js";

/**
 * Recovery after a dispatcher that died mid-dispatch.
 *
 * The design called for `forceUnlockWorkers` on the previous generation's
 * worker ids, and an earlier version of this file tried to find them by
 * matching the pg connection's `application_name`. That does not work:
 * graphile's `locked_by` is its own `worker-<18 hex>` id, minted internally
 * (`worker.js`), with no relationship to `application_name` — and graphile
 * refuses an externally supplied `workerId` at concurrency > 1, so the ids
 * cannot be made predictable either. The match found nothing and the "recovery"
 * was a no-op that read as if it worked.
 *
 * What actually recovers a stranded run is the re-enqueue below: it is keyed by
 * run, so a job still locked to a dead worker is superseded rather than waited
 * on. graphile releases the abandoned lock on its own schedule; nothing depends
 * on that having happened first.
 */

/**
 * Re-enqueue every tenant's active runs across the shared database.
 *
 * This is the platform-side counterpart to the world's per-tenant re-enqueue.
 * It is safe to run repeatedly: `jobKey` collapses duplicates, and the workflow
 * handler replays the event log rather than re-executing completed work.
 */
export async function reenqueueActiveRunsForAllTenants(input: {
  pool: Pool;
  workerUtils: WorkerUtils;
  log?: (message: string, meta?: Record<string, unknown>) => void;
}): Promise<number> {
  const { rows } = await input.pool.query<{
    tenant_id: string;
    id: string;
    name: string;
    deployment_id: string;
    queue_namespace: string | null;
  }>(
    `select runs.tenant_id, runs.id, runs.name, runs.deployment_id, runs.queue_namespace
       from workflow.workflow_runs as runs
      where runs.status in ('pending', 'running')
        -- A dead letter is terminal for dispatch, not a workflow-authored
        -- run_failed event. Keep it operator-replayable without recreating the
        -- same terminal delivery on every dispatcher restart.
        and not exists (
          select 1
            from workflow.dispatch_dead_letters as dead
           where dead.tenant_id = runs.tenant_id
             and dead.run_id = runs.id
             and dead.resolved_at is null
        )
      order by runs.tenant_id, runs.created_at`,
  );

  let enqueued = 0;
  let unknownNamespace = 0;
  for (const row of rows) {
    // The namespace the run's own deployment resolved, recorded when the run was
    // created. It cannot be resolved here: this process runs on the host, so
    // `WORKFLOW_QUEUE_NAMESPACE` would be the host's value rather than the
    // tenant's, and it cannot be derived from the run either — it is eve's, not
    // ours, and reimplementing how eve mints it would fork the algorithm.
    //
    // NULL is not "no namespace". It is a row written by code that did not
    // record one — from before the column, or from an older deployment still
    // running mid-upgrade. The default prefix is the only available fallback and
    // it is right for an un-namespaced deployment, but for a namespaced one the
    // dispatch will be refused, so it is reported rather than assumed.
    if (row.queue_namespace === null) {
      unknownNamespace += 1;
      input.log?.("recovering a run whose queue namespace was never recorded", {
        runId: row.id,
        tenantId: row.tenant_id,
        deploymentId: row.deployment_id,
      });
    }
    const messageId = `msg_recover_${row.id}`;
    const message: MessageData = {
      // The BARE sub-queue id, exactly as the World's own enqueue path stores it.
      // `workflow_runs.name` is already unprefixed; the delivery side is what
      // adds `__wkf_<kind>_` back. Storing a prefixed value here produced
      // `__wkf_workflow___wkf_workflow_<name>`, which eve rejects with a 400 —
      // and a 400 is non-retryable, so every recovered run dead-lettered.
      id: row.name,
      data: Buffer.from(JSON.stringify({ runId: row.id })),
      attempt: 1,
      messageId: messageId as MessageData["messageId"],
      tenantId: row.tenant_id,
      deploymentId: row.deployment_id,
      // Absent, not empty, when there is no namespace: that is the wire shape
      // the live enqueue path produces, and `getQueueTopicPrefix` rejects `''`
      // rather than treating it as the default.
      ...(row.queue_namespace ? { queueNamespace: row.queue_namespace } : {}),
    };
    try {
      await input.workerUtils.addJob(FLOW_JOB_NAME, MessageData.encode(message), {
        // Stable per run, so a recovery sweep that overlaps a still-queued job
        // collapses instead of doubling it. It does NOT collapse against the
        // World's own job for that run (whose key is a fresh ULID) — the queue
        // name below is what keeps those two from running concurrently.
        jobKey: messageId,
        queueName: runQueueName(row.tenant_id, row.id),
        maxAttempts: MAX_GRAPHILE_JOB_ATTEMPTS,
        flags: [`project:${row.tenant_id}`],
      });
      enqueued += 1;
    } catch (error) {
      input.log?.("failed to re-enqueue run during boot recovery", {
        runId: row.id,
        tenantId: row.tenant_id,
        error: String(error),
      });
    }
  }

  if (enqueued > 0) {
    input.log?.("re-enqueued active runs on boot", {
      runs: enqueued,
      // Surfaced as a count too, so an upgrade that stranded namespaced runs is
      // visible in one line rather than only in the per-run entries above.
      ...(unknownNamespace > 0 ? { runsWithUnknownQueueNamespace: unknownNamespace } : {}),
    });
  }
  return enqueued;
}
