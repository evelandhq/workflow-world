import { MessageData } from "../message.js";
import { getQueueTopicPrefix } from "@workflow/world";
import { makeWorkerUtils, run, type Runner, type WorkerUtils } from "graphile-worker";
import type { Pool } from "pg";
import {
  createFairness,
  createRunLookup,
  dispatchMessage,
  readRunId,
  type DispatchOutcome,
  type DispatcherDeps,
  type Fairness,
} from "./dispatcher.js";

/**
 * Job names must match what `@eveland/workflow-world` enqueues in external
 * mode. Embedded-mode jobs carry a per-tenant suffix and are deliberately not
 * claimed here — they belong to that deployment's own in-process runner.
 */
import { FLOW_JOB_NAME, runQueueName } from "../dispatch-contract.js";

export { FLOW_JOB_NAME };

export type DispatcherConfig = {
  concurrency: number;
  pollIntervalMs: number;
  maxInFlightPerTenant: number;
  queueGcIntervalMs: number;
};

export type DispatcherRuntime = {
  runner: Runner;
  workerUtils: WorkerUtils;
  fairness: Fairness;
  stop(): Promise<void>;
};

export async function startDispatcher(input: {
  pool: Pool;
  config: DispatcherConfig;
  deps: Omit<DispatcherDeps, "reenqueue" | "onDeadLetter" | "runLookup"> &
    Partial<Pick<DispatcherDeps, "runLookup">>;
}): Promise<DispatcherRuntime> {
  const { pool, config } = input;
  const workerUtils = await makeWorkerUtils({ pgPool: pool });
  const fairness = createFairness({ maxInFlightPerTenant: config.maxInFlightPerTenant });
  const log = input.deps.log ?? (() => {});

  const reenqueue: DispatcherDeps["reenqueue"] = async ({ jobName, message, delaySeconds }) => {
    const runId = readRunId(message);
    await workerUtils.addJob(jobName, MessageData.encode(message), {
      jobKey: message.idempotencyKey ?? message.messageId,
      runAt: new Date(Date.now() + delaySeconds * 1000),
      // Same per-run serialization the World applies on the first delivery. A
      // reschedule that omitted it would be concurrently claimable against the
      // run it belongs to.
      ...(runId ? { queueName: runQueueName(message.tenantId, runId) } : {}),
      maxAttempts: 3,
      flags: [`project:${message.tenantId}`],
    });
  };

  const onDeadLetter: DispatcherDeps["onDeadLetter"] = async ({
    message,
    reason,
    jobName,
    queueName,
  }) => {
    await pool.query(
      `insert into workflow.dispatch_dead_letters
         (tenant_id, deployment_id, run_id, message_id, job_name, queue_name, attempt, reason, payload)
       values ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
      [
        message.tenantId,
        message.deploymentId,
        readRunId(message) ?? null,
        message.messageId,
        jobName ?? null,
        queueName ?? null,
        message.attempt,
        reason,
        JSON.stringify(MessageData.encode(message)),
      ],
    );
    log("dead-lettered workflow message", {
      tenantId: message.tenantId,
      messageId: message.messageId,
      reason,
    });
  };

  const deps: DispatcherDeps = {
    ...input.deps,
    runLookup: input.deps.runLookup ?? createRunLookup(pool),
    reenqueue,
    onDeadLetter,
  };

  const makeHandler = (jobName: string) => async (payload: unknown, helpers: unknown) => {
    const message = MessageData.parse(payload);
    const attempt = readAttempt(helpers) ?? message.attempt;
    const isFinalAttempt = readIsFinalAttempt(helpers);
    // `MessageData.id` is only the sub-queue id: the enqueue path ran it
    // through `parseQueueName`, which strips the `__wkf_<kind>_` prefix. eve's
    // handler rejects a name without that prefix outright ("Unhandled
    // queue", 400), and a 400 is non-retryable — so sending the bare id
    // dead-letters every message. The embedded runner rebuilds the full name
    // for the same reason.
    const queueName = `${getQueueTopicPrefix("workflow")}${message.id}`;

    fairness.acquire(message.tenantId);
    try {
      let outcome: DispatchOutcome;
      try {
        outcome = await dispatchMessage({ message, jobName, queueName, attempt }, deps);
      } catch (error) {
        // dispatchMessage can throw rather than return — a database error in
        // the run lookup, a rejected activation fetch. Treating that as an
        // ordinary retryable outcome keeps the final-attempt dead-letter
        // below on the path; letting it propagate meant the last attempt
        // vanished with no record, which is precisely the silent loss the
        // dead-letter table exists to prevent.
        outcome = { type: "retry", reason: `Dispatch threw: ${String(error)}` };
      }

      if (outcome.type === "completed" || outcome.type === "rescheduled") return;

      if (outcome.type === "dead-letter") {
        await deps.onDeadLetter({ message, reason: outcome.reason, jobName, queueName });
        return; // Terminal: returning stops graphile from retrying.
      }

      // Retryable. On the last attempt graphile would otherwise drop the job
      // silently, so record it before throwing.
      if (isFinalAttempt) {
        await deps.onDeadLetter({
          message,
          reason: `Retries exhausted. Last failure: ${outcome.reason}`,
          jobName,
          queueName,
        });
        return;
      }
      throw new Error(outcome.reason);
    } finally {
      fairness.release(message.tenantId);
    }
  };

  const runner = await run({
    pgPool: pool,
    concurrency: config.concurrency,
    pollInterval: config.pollIntervalMs,
    // graphile otherwise installs its own SIGTERM/SIGINT handlers which end with
    // "killing self via SIGTERM" — the process dies while our own shutdown is
    // still awaiting `runner.stop()`, so the pool is never drained and the
    // telemetry sink never flushes. Owning the signals is the only way the
    // in-flight held dispatches get a chance to finish.
    noHandleSignals: true,
    // Evaluated per claim: a tenant already at its in-flight cap is skipped so
    // the runner picks up someone else's work instead of queueing behind it.
    forbiddenFlags: () => fairness.forbiddenFlags(),
    taskList: {
      [FLOW_JOB_NAME]: makeHandler(FLOW_JOB_NAME),
    },
  });

  // Per-run serialization gives every run its own graphile queue, and graphile
  // does not reclaim the queue row when the last job in it finishes (measured —
  // see run-serialization.integration.test.ts). Left alone that is one permanent
  // `_private_job_queues` row per workflow run, so the sweep is not optional.
  //
  // Cheap and idempotent, so a fixed interval beats trying to be clever about
  // when it is needed. `unref` so it never holds the process open.
  const queueGcTimer = setInterval(() => {
    void workerUtils.cleanup({ tasks: ["GC_JOB_QUEUES"] }).catch((error: unknown) => {
      log("job queue cleanup failed", { error: String(error) });
    });
  }, config.queueGcIntervalMs);
  queueGcTimer.unref();

  return {
    runner,
    workerUtils,
    fairness,
    async stop() {
      clearInterval(queueGcTimer);
      await runner.stop();
      await workerUtils.release();
    },
  };
}

function readAttempt(helpers: unknown): number | undefined {
  const attempts = (helpers as { job?: { attempts?: unknown } } | null)?.job?.attempts;
  return typeof attempts === "number" ? attempts : undefined;
}

function readIsFinalAttempt(helpers: unknown): boolean {
  const job = (helpers as { job?: { attempts?: unknown; max_attempts?: unknown } } | null)?.job;
  return (
    typeof job?.attempts === "number" &&
    typeof job.max_attempts === "number" &&
    job.attempts >= job.max_attempts
  );
}
