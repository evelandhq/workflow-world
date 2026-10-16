import type { Job, TaskSpec } from "graphile-worker";
import type { Pool } from "pg";
import { z } from "zod/v4";
import { FLOW_JOB_NAME, runQueueName, TIMER_JOB_NAME } from "./dispatch-contract.js";
import { MessageData, readRunId } from "./message.js";
import { MAX_GRAPHILE_JOB_ATTEMPTS } from "./queue-policy.js";

/**
 * Delayed deliveries, held outside their run's queue until they are due
 * (evelandhq/workflow-world#115).
 *
 * graphile 0.16's `get_job` starts by locking every available named queue —
 * `select id from _private_job_queues where is_available for update skip
 * locked` — and only then looks for a due job. Each lock is a WAL record and
 * every claim is a commit, so a claim costs one row lock per queue that has any
 * job at all, due or not. 0.17 and 0.18 claim the same way by default.
 *
 * Per-run serialization gives every run its own queue, and a sleeping run keeps
 * a delayed continuation in it for the whole sleep. eve's session timeout is the
 * extreme case: it re-arms every 23 hours for as long as the session lives
 * (seven days for jiri), so every live session pinned one queue, and with a few
 * hundred of them every claim locked a few hundred rows. The disk was not slow;
 * there was just a great deal of WAL to fsync.
 *
 * A job that is not due has nothing to serialize against, so it does not need
 * the queue yet. It is held as a timer job — {@link TIMER_JOB_NAME}, with no
 * queue — and re-added to the run's queue as an ordinary flow job once it comes
 * due. Serialization still covers the delivery itself, which is the only thing
 * it ever protected.
 *
 * Who creates timers depends on who will claim them:
 *
 *   - `external` mode: the dispatcher alone. A deployment's world is baked at
 *     build time, so it keeps writing flow jobs straight onto the run's queue;
 *     the dispatcher's sweep moves them, for old builds and new alike, and its
 *     own reschedules skip the queue from the start. Deployments never write a
 *     job only a newer dispatcher could claim.
 *   - `embedded` mode: the world itself, under its per-tenant names, because the
 *     process that enqueues is the process that claims.
 */

/**
 * How far out a job must be due before it leaves its run's queue.
 *
 * Below this the extra hop is not worth it: a retry backoff or a near-elapsed
 * wait would pay a second claim to save a few seconds of queue time. Above it
 * the job is a sleep, a session timeout, or a paced recovery wave — the shapes
 * that can hold a queue for hours.
 */
export const TIMER_HORIZON_SECONDS = 60;

/** Default cadence of {@link moveDelayedJobsToTimers} in the dispatcher. */
export const DEFAULT_TIMER_SWEEP_INTERVAL_MS = 30_000;

type AddJob = (identifier: string, payload: unknown, spec: TaskSpec) => Promise<Job>;

/** The target queue travels on the timer, because the job row can no longer hold it. */
const TimerTarget = z.object({ runQueue: z.string().min(1).optional() });

/** Whether a delivery should wait as a timer rather than on its run's queue. */
export function holdsAsTimer(runId: string | undefined, delaySeconds: number | undefined): boolean {
  return runId !== undefined && delaySeconds !== undefined && delaySeconds >= TIMER_HORIZON_SECONDS;
}

/** A timer's payload: the message exactly as enqueued, plus the queue it returns to. */
export function timerPayload(encoded: object, runQueue: string): object {
  return { ...encoded, runQueue };
}

/**
 * Enqueue the dispatcher's own reschedule (a `{ timeoutSeconds }` answer): as a
 * timer when it is far enough out and belongs to a run, otherwise as an
 * ordinary flow job on the run's queue.
 */
export async function addDelayedDelivery(
  addJob: AddJob,
  input: {
    jobName: string;
    message: MessageData;
    delaySeconds: number;
    jobKey: string;
  },
): Promise<void> {
  const { message, delaySeconds } = input;
  const runId = readRunId(message);
  const runAt = new Date(Date.now() + delaySeconds * 1000);
  if (runId && input.jobName === FLOW_JOB_NAME && holdsAsTimer(runId, delaySeconds)) {
    await addJob(
      TIMER_JOB_NAME,
      timerPayload(MessageData.encode(message), runQueueName(message.tenantId, runId)),
      { jobKey: input.jobKey, runAt, maxAttempts: MAX_GRAPHILE_JOB_ATTEMPTS },
    );
    return;
  }
  await addJob(input.jobName, MessageData.encode(message), {
    jobKey: input.jobKey,
    runAt,
    // Same per-run serialization the World applies on the first delivery. A
    // reschedule that omitted it would be concurrently claimable against the
    // run it belongs to.
    ...(runId ? { queueName: runQueueName(message.tenantId, runId) } : {}),
    maxAttempts: MAX_GRAPHILE_JOB_ATTEMPTS,
    flags: [`project:${message.tenantId}`],
  });
}

/**
 * The timer task: re-add the delivery to its run's queue, due now, and finish.
 *
 * It keeps the timer's job key. The timer is locked while this runs, so
 * graphile's `add_jobs` releases the key from it (and marks it spent) before
 * inserting the flow job under the same key — the delivery stays deduplicated
 * against anything else enqueued for it, and the timer is deleted when this
 * returns. A crash between the two leaves the flow job queued and the timer
 * spent, which loses nothing.
 *
 * No fairness accounting: promoting costs the tenant nothing, and the flow job
 * it creates carries the tenant flag the claim loop throttles on.
 */
export function createTimerHandler(input: { addJob: AddJob; flowJobName: string }) {
  return async (payload: unknown, helpers: unknown): Promise<void> => {
    const message = MessageData.parse(payload);
    const runId = readRunId(message);
    const runQueue =
      TimerTarget.parse(payload).runQueue ??
      (runId ? runQueueName(message.tenantId, runId) : undefined);
    const jobKey = readJobKey(helpers);
    await input.addJob(input.flowJobName, MessageData.encode(message), {
      ...(jobKey ? { jobKey } : {}),
      ...(runQueue ? { queueName: runQueue } : {}),
      maxAttempts: MAX_GRAPHILE_JOB_ATTEMPTS,
      flags: [`project:${message.tenantId}`],
    });
  };
}

/**
 * Move every not-yet-due flow job off its `wfrun:` queue and onto the timer
 * task. Returns how many jobs moved.
 *
 * One statement against graphile's private tables, because no public API can
 * re-point an existing job: re-adding it by key would, when the key has since
 * moved to a job that is already running, mark that running job spent. The
 * conditions make it safe against the claim loop without any coordination:
 *
 *   - only jobs due beyond the horizon, which `get_job` (`run_at <= now()`)
 *     cannot be claiming;
 *   - only unlocked jobs that have never been attempted. A retry carries an
 *     attempt count, and a promoted timer starts over at zero — moving one
 *     would quietly lift its retry budget;
 *   - only the given flow task. The dispatcher passes the shared, unsuffixed
 *     name; an embedded runner passes its own tenant's suffixed one;
 *   - only `wfrun:` queues, recorded on the payload so the promotion and boot
 *     recovery can still find the run.
 *
 * A concurrent `add_job` under the same key waits on the row lock and then sees
 * the moved row, which it re-points back to a flow job — exactly what a newer
 * enqueue for that key should do. A missing timer task row (see
 * {@link registerTimerTask}) makes the statement a no-op.
 *
 * The moved jobs' queues are left empty; run `GC_JOB_QUEUES` afterwards, or
 * every claim keeps locking them until the next scheduled sweep.
 */
export async function moveDelayedJobsToTimers(
  pool: Pick<Pool, "query">,
  options: { flowJobName?: string; timerJobName?: string; horizonSeconds?: number } = {},
): Promise<number> {
  const { rowCount } = await pool.query(
    `update graphile_worker._private_jobs as jobs
        set task_id = timer_task.id,
            job_queue_id = null,
            flags = null,
            payload = (jobs.payload::jsonb || jsonb_build_object('runQueue', queues.queue_name))::json,
            revision = jobs.revision + 1,
            updated_at = now()
       from graphile_worker._private_tasks as flow_task,
            graphile_worker._private_tasks as timer_task,
            graphile_worker._private_job_queues as queues
      where flow_task.identifier = $1
        and timer_task.identifier = $2
        and jobs.task_id = flow_task.id
        and queues.id = jobs.job_queue_id
        and queues.queue_name like 'wfrun:%'
        and jobs.locked_at is null
        and jobs.attempts = 0
        and jobs.run_at > now() + make_interval(secs => $3)
        and json_typeof(jobs.payload) = 'object'`,
    [
      options.flowJobName ?? FLOW_JOB_NAME,
      options.timerJobName ?? TIMER_JOB_NAME,
      options.horizonSeconds ?? TIMER_HORIZON_SECONDS,
    ],
  );
  return rowCount ?? 0;
}

/**
 * Make sure the timer task row exists, so the first sweep does not depend on
 * the runner having claimed — which is what registers task names — first.
 * Graphile's own `add_job` creates task rows the same way.
 */
export async function registerTimerTask(
  pool: Pick<Pool, "query">,
  timerJobName: string = TIMER_JOB_NAME,
): Promise<void> {
  await pool.query(
    "insert into graphile_worker._private_tasks (identifier) values ($1) on conflict do nothing",
    [timerJobName],
  );
}

function readJobKey(helpers: unknown): string | undefined {
  const key = (helpers as { job?: { key?: unknown } } | null)?.job?.key;
  return typeof key === "string" && key.length > 0 ? key : undefined;
}
