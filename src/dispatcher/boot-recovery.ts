import { MessageData } from "../message.js";
import type { WorkerUtils } from "graphile-worker";
import type { Pool } from "pg";
import { FLOW_JOB_NAME } from "./runner.js";

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
  }>(
    `select tenant_id, id, name, deployment_id
       from workflow.workflow_runs
      where status in ('pending', 'running')
      order by tenant_id, created_at`,
  );

  let enqueued = 0;
  for (const row of rows) {
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
    };
    try {
      await input.workerUtils.addJob(FLOW_JOB_NAME, MessageData.encode(message), {
        // Stable per run, so a recovery sweep that overlaps a still-queued job
        // collapses instead of doubling it.
        jobKey: messageId,
        maxAttempts: 3,
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
    input.log?.("re-enqueued active runs on boot", { runs: enqueued });
  }
  return enqueued;
}
