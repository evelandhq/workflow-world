import { MessageData } from "../message.js";
import type { WorkerUtils } from "graphile-worker";
import type { Pool } from "pg";
import { FLOW_JOB_NAME } from "./runner.js";
import { runQueueName } from "../dispatch-contract.js";
import { MAX_GRAPHILE_JOB_ATTEMPTS } from "../queue-policy.js";

/**
 * Recovery after a dispatcher that died mid-dispatch.
 *
 * A job key only deduplicates jobs. It does not clear the per-run queue's
 * `locked_by`, so re-enqueueing alone leaves a replacement job unavailable until
 * Graphile's four-hour stale-lock threshold. The service therefore calls the
 * lock-reclaiming entry point only after taking lifecycle ownership and before
 * starting its worker pool. At that point every worker id found on an active run's
 * exact queue belongs to the dead generation and can be passed to Graphile's
 * `forceUnlockWorkers` safely.
 *
 * Standalone callers deliberately default to re-enqueue only. Unlocking while a
 * pool is already running could clear a live worker's queue lock.
 */

/**
 * Re-enqueue every tenant's active runs that have no job left to wake them.
 *
 * This is the platform-side counterpart to the world's per-tenant re-enqueue.
 * It is safe to run repeatedly: `jobKey` collapses duplicates, and the workflow
 * handler replays the event log rather than re-executing completed work.
 *
 * A run whose per-run queue still holds a retryable job is not a candidate.
 * That job is the run's own wake-up — a delayed continuation for a sleep, a
 * pending first delivery, or a delivery the dead dispatcher was holding, which
 * the lock reclaim above makes claimable again. Re-enqueueing beside it added
 * nothing the run needed and cost one cold-start activation per run: with a
 * few hundred active runs across a few dozen deployments, every dispatcher
 * restart became a cold-start storm.
 *
 * A run that holds a hook is not a candidate either, unless it is also parked
 * on a timer. A hook is the run's other wake-up: the party that resolves it
 * enqueues the delivery, through the World's own `queue()`, whenever that
 * happens — an agent session parked on its inbox, a subagent parked on its
 * continuation. Nothing about a dispatcher restart changes that, and the
 * replay a recovery job bought was the same cold start the live-job case paid
 * for nothing: on the incident host every one of the sixty-odd remaining
 * candidates was a session waiting on its inbox, and skipping them left zero
 * deployments to wake.
 *
 * The exception is a run whose wake-up is a `sleep()`: a `waiting` wait with a
 * `resume_at`. Its only driver is the delayed job, and the hooks it holds are
 * its own (from eve 0.57 the durable sleep tool is a child run that carries
 * an abort hook and its callback hook while it sleeps — evelandhq/workflow-world#89).
 * Nobody else resolves those, so losing the job strands the run for good
 * unless boot recovery replays it; the replay re-arms the timer from the
 * wait's own deadline. A run that created a hook, moved on without a timer,
 * and then lost its job still waits for that hook instead of the next boot —
 * the rare shape; parked sessions are the common one.
 */
/** One candidate the sweep found: an active run minus its resolved payloads. */
export type BootRecoveryRun = {
  tenantId: string;
  runId: string;
  workflowName: string;
  deploymentId: string;
  /**
   * As recorded at run creation; `null` for rows written by code that predates
   * the column. See the namespace commentary in the sweep below.
   */
  queueNamespace: string | null;
};

/**
 * How the sweep spreads recovered runs over time, so a restart does not ask the
 * host to cold-start every deployment with an active run at once.
 *
 * Candidates are grouped by deployment in the order the sweep meets them; each
 * group of `deploymentsPerWave` deployments is due `waveIntervalMs` later than
 * the previous one. Runs of one deployment always share a wave — it is the
 * deployment's cold start that costs, not the run's replay.
 */
export type BootRecoveryPacing = {
  deploymentsPerWave: number;
  waveIntervalMs: number;
};

type BootRecoveryInput = {
  pool: Pool;
  workerUtils: WorkerUtils;
  log?: (message: string, meta?: Record<string, unknown>) => void;
  /** Absent means every recovered run is due immediately. */
  pacing?: BootRecoveryPacing;
  /**
   * The host's say over what gets replayed. Called once with every candidate
   * (an active run with no live job on its queue; runs that still have one are
   * never offered, because nothing would be enqueued for them anyway) —
   * one call rather than a per-run predicate, because the question the host
   * answers ("is this run's Deployment still activatable?") is a control-plane
   * lookup it will want to batch. Only the runs returned are re-enqueued;
   * entries not in the candidate list are ignored. Skipped runs stay exactly
   * as they were — still active, still candidates for the next sweep — so a
   * host that wants them gone for good must settle them through
   * `reconcileWorkflowRuns` instead of filtering them forever.
   *
   * Throwing aborts the sweep with nothing partially enqueued beyond what the
   * loop had already reached — which is nothing, since the filter runs first.
   */
  filterRuns?: (runs: BootRecoveryRun[]) => Promise<BootRecoveryRun[]> | BootRecoveryRun[];
};

export function reenqueueActiveRunsForAllTenants(input: BootRecoveryInput): Promise<number> {
  return recoverActiveRunsForAllTenants(input, false);
}

/** Only call while holding dispatcher ownership and before starting its worker pool. */
export function reclaimAndReenqueueActiveRunsForAllTenants(
  input: BootRecoveryInput,
): Promise<number> {
  return recoverActiveRunsForAllTenants(input, true);
}

async function recoverActiveRunsForAllTenants(
  input: BootRecoveryInput,
  reclaimOldWorkerLocks: boolean,
): Promise<number> {
  const sweepStartedAt = Date.now();
  const { rows } = await input.pool.query<{
    tenant_id: string;
    id: string;
    name: string;
    deployment_id: string;
    queue_namespace: string | null;
    locked_by: string | null;
    has_live_job: boolean;
    has_hook: boolean;
    has_timer: boolean;
  }>(
    `select runs.tenant_id, runs.id, runs.name, runs.deployment_id, runs.queue_namespace,
            queues.locked_by,
            -- A job graphile will still deliver: due now, due later, or locked
            -- by a worker the reclaim below is about to release. One that has
            -- spent its attempts is left behind by graphile and delivers
            -- nothing, so it does not count.
            exists (
              select 1
                from graphile_worker._private_jobs as jobs
               where jobs.job_queue_id = queues.id
                 and jobs.attempts < jobs.max_attempts
            ) as has_live_job,
            -- A hook the run still holds: its resolution enqueues the run's
            -- next delivery on its own, so boot needs to write nothing.
            exists (
              select 1
                from workflow.workflow_hooks as hooks
               where hooks.tenant_id = runs.tenant_id
                 and hooks.run_id = runs.id
            ) as has_hook,
            -- A sleep the run is still inside: its wake-up is the delayed job
            -- alone, whatever hooks the run holds meanwhile. The World deletes
            -- a run's waits when it terminates, so a row here belongs to a
            -- live run.
            exists (
              select 1
                from workflow.workflow_waits as waits
               where waits.tenant_id = runs.tenant_id
                 and waits.run_id = runs.id
                 and waits.status = 'waiting'
                 and waits.resume_at is not null
            ) as has_timer
       from workflow.workflow_runs as runs
       left join graphile_worker._private_job_queues as queues
         on queues.queue_name = concat('wfrun:', runs.tenant_id, ':', runs.id)
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

  // Lock reclaim covers ALL candidates, filtered or not. Every worker id found
  // here belongs to the dead generation, and a filtered-out run's queue must
  // not stay locked: the host may settle or re-admit it later, and a stale lock
  // would hold its next legitimate delivery for graphile's four-hour threshold.
  const oldWorkerIds = [...new Set(rows.flatMap((row) => (row.locked_by ? [row.locked_by] : [])))];
  if (reclaimOldWorkerLocks && oldWorkerIds.length > 0) {
    await input.workerUtils.forceUnlockWorkers(oldWorkerIds);
    input.log?.("unlocked old dispatcher workers during boot recovery", {
      workers: oldWorkerIds.length,
    });
  }

  const liveJobs = rows.filter((row) => row.has_live_job).length;
  const parkedOnHook = rows.filter(
    (row) => !row.has_live_job && row.has_hook && !row.has_timer,
  ).length;
  const sleepingWithHook = rows.filter(
    (row) => !row.has_live_job && row.has_hook && row.has_timer,
  ).length;
  let candidates = rows.filter((row) => !row.has_live_job && (!row.has_hook || row.has_timer));
  if (parkedOnHook > 0) {
    input.log?.("skipped runs parked on a hook", { runs: parkedOnHook });
  }
  if (sleepingWithHook > 0) {
    input.log?.("recovering hook-holding runs whose sleep timer was lost", {
      runs: sleepingWithHook,
    });
  }
  if (liveJobs > 0) {
    input.log?.("skipped runs whose own job is still queued", {
      runs: liveJobs,
    });
  }
  if (input.filterRuns) {
    const offered = candidates;
    const kept = await input.filterRuns(
      offered.map((row) => ({
        tenantId: row.tenant_id,
        runId: row.id,
        workflowName: row.name,
        deploymentId: row.deployment_id,
        queueNamespace: row.queue_namespace,
      })),
    );
    const keep = new Set(kept.map((run) => `${run.tenantId}\u0000${run.runId}`));
    candidates = offered.filter((row) => keep.has(`${row.tenant_id}\u0000${row.id}`));
    if (candidates.length < offered.length) {
      input.log?.("host filter excluded runs from boot recovery", {
        runs: offered.length - candidates.length,
      });
    }
  }

  // Wave per deployment, in first-seen order. Only computed when pacing is on;
  // otherwise every run is due now and the map stays empty.
  const waveByDeployment = new Map<string, number>();
  const pacing =
    input.pacing && input.pacing.waveIntervalMs > 0 && input.pacing.deploymentsPerWave > 0
      ? input.pacing
      : undefined;
  if (pacing) {
    for (const row of candidates) {
      const deployment = `${row.tenant_id}\u0000${row.deployment_id}`;
      if (!waveByDeployment.has(deployment)) {
        waveByDeployment.set(
          deployment,
          Math.floor(waveByDeployment.size / pacing.deploymentsPerWave),
        );
      }
    }
    const waves = Math.ceil(waveByDeployment.size / pacing.deploymentsPerWave);
    if (waves > 1) {
      input.log?.("pacing boot recovery across deployment waves", {
        deployments: waveByDeployment.size,
        waves,
        deploymentsPerWave: pacing.deploymentsPerWave,
        waveIntervalMs: pacing.waveIntervalMs,
      });
    }
  }

  let enqueued = 0;
  let unknownNamespace = 0;
  for (const row of candidates) {
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
    // Wave 0 is pinned to the sweep's start rather than left to graphile's
    // `now()`, so every job in a wave carries the same due time.
    const wave = waveByDeployment.get(`${row.tenant_id}\u0000${row.deployment_id}`) ?? 0;
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
        ...(pacing ? { runAt: new Date(sweepStartedAt + wave * pacing.waveIntervalMs) } : {}),
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
