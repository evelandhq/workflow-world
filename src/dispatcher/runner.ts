import { MessageData, readBody, withRunId } from "../message.js";
import { getQueueTopicPrefix } from "@workflow/world";
import { makeWorkerUtils, run, type Runner, type WorkerUtils } from "graphile-worker";
import type { Pool } from "pg";
import {
  createExecutorFailureTracker,
  createFairness,
  createLiveFeedGrants,
  createMessageDedup,
  createRunLookup,
  dispatchMessage,
  readRunId,
  type DispatchOutcome,
  type DispatcherDeps,
  type ExecutorFailureTracker,
  type Fairness,
  type LiveFeedGrants,
  type MessageDedup,
} from "./dispatcher.js";
import {
  createRunGate,
  MAX_COALESCED_RERUNS,
  MAX_RERUN_HOLD_MS,
  rerunBody,
  runGateKey,
  setAsideDelaySeconds,
  type PendingWake,
  type RunGate,
} from "./run-gate.js";

/** Job names must match what the World enqueues (`FLOW_JOB_NAME`). */
import {
  deliveryAccounting,
  FLOW_JOB_NAME,
  isCoalescibleWake,
  orchestratorDeliveryRunId,
  setAsideCopyOf,
  tenantFlag,
} from "../dispatch-contract.js";
import { MAX_GRAPHILE_JOB_ATTEMPTS } from "../queue-policy.js";

export { FLOW_JOB_NAME };

export type DispatcherConfig = {
  concurrency: number;
  pollIntervalMs: number;
  maxInFlightPerTenant: number;
  /** Consecutive executor 5xx on one run before it is dead-lettered. */
  executorFailureLimit?: number;
  /** The streak must also span at least this long; see the tracker. */
  executorFailureMinSpanMs?: number;
};

export type DispatcherRuntime = {
  runner: Runner;
  workerUtils: WorkerUtils;
  fairness: Fairness;
  /** Deliveries granted a live event feed; see `createLiveFeedGrants`. */
  liveFeedGrants: LiveFeedGrants;
  /** The per-run gate over orchestrator deliveries; see `run-gate.ts`. */
  runGate: RunGate;
  dedup: MessageDedup;
  executorFailures: ExecutorFailureTracker;
  stop(): Promise<void>;
};

export async function startDispatcher(input: {
  pool: Pool;
  workerUtils?: WorkerUtils;
  config: DispatcherConfig;
  deps: Omit<DispatcherDeps, "reenqueue" | "onDeadLetter" | "runLookup"> &
    Partial<Pick<DispatcherDeps, "runLookup">>;
}): Promise<DispatcherRuntime> {
  const { pool, config } = input;
  const workerUtils = input.workerUtils ?? (await makeWorkerUtils({ pgPool: pool }));
  const fairness = createFairness({ maxInFlightPerTenant: config.maxInFlightPerTenant });
  const liveFeedGrants = createLiveFeedGrants({
    concurrency: config.concurrency,
    maxInFlightPerTenant: config.maxInFlightPerTenant,
  });
  const runGate = createRunGate();
  const dedup = createMessageDedup();
  const log = input.deps.log ?? (() => {});
  // Set when the runtime stops: a delivery then starts no further re-run, and
  // the wake it leaves goes back to the queue for the next dispatcher.
  let stopping = false;

  // Every job the dispatcher writes goes through here: a reschedule or
  // handoff, a message the run gate set aside, a wake it re-adds. No queue
  // name (see `run-gate.ts`), and the run id on the payload, which a job a 1.x
  // World wrote does not carry yet.
  const reenqueue: DispatcherDeps["reenqueue"] = async ({
    jobName,
    message,
    delaySeconds,
    maxAttempts,
  }) => {
    await workerUtils.addJob(jobName, MessageData.encode(withRunId(message)), {
      jobKey: message.idempotencyKey ?? message.messageId,
      runAt: new Date(Date.now() + delaySeconds * 1000),
      maxAttempts: maxAttempts ?? MAX_GRAPHILE_JOB_ATTEMPTS,
      flags: [tenantFlag(message.tenantId)],
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

  const executorFailures =
    input.deps.executorFailures ??
    createExecutorFailureTracker({
      ...(config.executorFailureLimit === undefined ? {} : { limit: config.executorFailureLimit }),
      ...(config.executorFailureMinSpanMs === undefined
        ? {}
        : { minSpanMs: config.executorFailureMinSpanMs }),
    });
  const deps: DispatcherDeps = {
    ...input.deps,
    runLookup: input.deps.runLookup ?? createRunLookup(pool),
    reenqueue,
    onDeadLetter,
    executorFailures,
  };

  const makeHandler = (jobName: string) => async (payload: unknown, helpers: unknown) => {
    const message = MessageData.parse(payload);
    // See `deliveryAccounting`: the attempt the runtime's cap counts, and every
    // delivery of the message so far, reschedules and handoffs included.
    const { attempt, deliveryCount } = deliveryAccounting(message, readAttempt(helpers));
    const isFinalAttempt = readIsFinalAttempt(helpers);
    // `MessageData.id` is only the sub-queue id: the enqueue path ran it
    // through `parseQueueName`, which strips the `__wkf_<kind>_` prefix. eve's
    // handler rejects a name without that prefix outright ("Unhandled
    // queue", 400), and a 400 is non-retryable — so sending the bare id
    // dead-letters every message.
    //
    // The namespace comes off the MESSAGE, not from this process's environment.
    // The dispatcher runs on the host; `WORKFLOW_QUEUE_NAMESPACE` here would be
    // the host's value, and addressing `__wkf_workflow_*` at an executor that
    // registered `__<ns>_wkf_workflow_*` is an unhandled queue every time.
    const queueName = `${getQueueTopicPrefix("workflow", message.queueNamespace)}${message.id}`;

    // The run this is an orchestrator delivery for, read off the body; a step
    // message, a wait's timer hop and a health check have none. Only an
    // orchestrator delivery takes its run's gate, and only one can linger on a
    // live feed: a step message returns when its step does, so a grant on it
    // would be one wasted.
    const body = readBody(message);
    const gateRunId = orchestratorDeliveryRunId(body);

    /**
     * This orchestrator message found its run's gate held. A wake is coalesced
     * into the holder's delivery, and its job completes undelivered. Anything
     * else carries data a later delivery does not cover, and a wake that finds
     * the gate closing would have nobody left to serve it in memory: either
     * goes back to the queue under its own key, a little later, with its counts
     * as they were. It is durable throughout and delivered once the gate is free.
     */
    const setAside = async (key: string, runId: string): Promise<void> => {
      // Only a wake on its first delivery is coalesced. One that was delivered
      // before (a continuation after a reschedule or a handoff, a retry) carries
      // an attempt and a delivery count the runtime caps a run by
      // (`MAX_DELIVERIES_EXCEEDED`); a re-run reports the holder's counts, so
      // absorbing it would let a run that is refused on every pass loop without
      // ever reaching the cap. It is set aside instead and keeps its counts.
      if (isCoalescibleWake(body) && attempt === 1 && deliveryCount === 1) {
        const offer = runGate.offerWake(key, {
          message,
          attempt,
          deliveryCount,
          maxAttempts: readRemainingAttempts(helpers),
          hookResume:
            (body as { hookResumeTiming?: unknown } | undefined)?.hookResumeTiming !== undefined,
        });
        if (offer !== "closing") {
          log("coalesced a wake into its run's delivery", {
            tenantId: message.tenantId,
            runId,
            messageId: message.messageId,
            offer,
          });
          return;
        }
      }
      const setAsideCount = message.setAside ?? 0;
      const delaySeconds = setAsideDelaySeconds(setAsideCount);
      await deps.reenqueue({
        jobName,
        message: {
          ...setAsideCopyOf(message, { attempt, deliveryCount }),
          setAside: setAsideCount + 1,
        },
        delaySeconds,
        maxAttempts: readRemainingAttempts(helpers),
      });
      log("set a message aside while its run's delivery runs", {
        tenantId: message.tenantId,
        runId,
        messageId: message.messageId,
        delaySeconds,
      });
    };

    /** A wake the holder did not serve, back to the queue as a job of its own. */
    const readdWake = async (wake: PendingWake): Promise<void> => {
      await deps.reenqueue({
        jobName,
        message: setAsideCopyOf(wake.message, {
          attempt: wake.attempt,
          deliveryCount: wake.deliveryCount,
        }),
        delaySeconds: 0,
        maxAttempts: wake.maxAttempts,
      });
      log("re-added a coalesced wake its run's delivery did not serve", {
        tenantId: message.tenantId,
        runId: gateRunId ?? "",
        messageId: wake.message.messageId,
      });
    };

    let liveFeed = false;
    let gateKey: string | undefined;
    fairness.acquire(message.tenantId);
    try {
      // A message the dedup would swallow (completed already, or in flight)
      // goes straight to it, as it did before there was a gate: coalescing it
      // would only buy its run a re-run it does not need.
      if (gateRunId !== undefined && !dedup.has(message.idempotencyKey)) {
        const key = runGateKey(message.tenantId, gateRunId);
        if (!runGate.tryEnter(key)) {
          await setAside(key, gateRunId);
          return;
        }
        gateKey = key;
      }

      // The holder's re-run for a wake that arrived while it ran: the same job,
      // grant and lease, the next delivery count, the same attempt.
      const heldKey = gateKey;
      const heldSince = Date.now();
      let reruns = 0;
      const takeRerun = (): Buffer | undefined => {
        if (heldKey === undefined) return undefined;
        if (
          stopping ||
          reruns >= MAX_COALESCED_RERUNS ||
          Date.now() - heldSince >= MAX_RERUN_HOLD_MS
        ) {
          // The holder will not run again in place: no further wake is kept
          // in memory, and the one pending goes back to the queue on the way out.
          runGate.markClosing(heldKey);
          return undefined;
        }
        const wake = runGate.takeWake(heldKey);
        if (!wake) return undefined;
        reruns += 1;
        log("re-running a delivery for a coalesced wake", {
          tenantId: message.tenantId,
          runId: gateRunId ?? "",
          messageId: message.messageId,
          wakeMessageId: wake.message.messageId,
          rerun: reruns,
        });
        return rerunBody(message, wake);
      };

      let outcome: DispatchOutcome;
      try {
        // `undefined` means the dedup swallowed it: already completed, or a
        // concurrent delivery of the same message that we waited on. Either way
        // there is nothing left to do and nothing to report.
        const deduped = await dedup.run(message.idempotencyKey, () => {
          // Taken only by a delivery that is actually sent, and held until it
          // returns, however it returns (released below with its fairness slot).
          liveFeed = gateRunId !== undefined && liveFeedGrants.tryAcquire(message.tenantId);
          return dispatchMessage(
            {
              message,
              jobName,
              queueName,
              attempt,
              deliveryCount,
              liveFeed,
              ...(heldKey === undefined
                ? {}
                : {
                    rerun: takeRerun,
                    // Before a reschedule or handoff writes the continuation:
                    // the pending wake's job is written first, while this job
                    // still owns its key, and no later wake is kept in memory.
                    beforeContinuation: () => runGate.close(heldKey, readdWake),
                  }),
            },
            deps,
          );
        });
        if (!deduped) return;
        outcome = deduped;
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

      // A retry or a dead letter: the holder will not run again in place. The
      // gate closes and its pending wake goes back to the queue before either
      // is recorded; if that add fails, this throws and graphile redelivers the
      // holder's job, which still owns its key.
      if (gateKey !== undefined) await runGate.close(gateKey, readdWake);

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
      try {
        // A reschedule, a handoff, a retry and a dead letter closed the gate
        // already. On a `completed` exit (no wake left to re-run for, the
        // re-run cap, the hold limit, shutdown, a dropped message) this closes
        // it: a wake still pending goes back to the queue before the gate
        // opens. If that add fails this throws, and graphile redelivers the
        // holder's job: a delivery that starts after the wake, so it covers it.
        if (gateKey !== undefined) await runGate.leave(gateKey, readdWake);
      } finally {
        if (liveFeed) liveFeedGrants.release(message.tenantId);
        fairness.release(message.tenantId);
      }
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

  return {
    runner,
    workerUtils,
    fairness,
    liveFeedGrants,
    runGate,
    dedup,
    executorFailures,
    async stop() {
      stopping = true;
      try {
        await runner.stop();
      } finally {
        await workerUtils.release();
      }
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

/** The attempts a job that replaces this one keeps: this one's, this attempt included. */
function readRemainingAttempts(helpers: unknown): number {
  const job = (helpers as { job?: { attempts?: unknown; max_attempts?: unknown } } | null)?.job;
  if (typeof job?.attempts !== "number" || typeof job.max_attempts !== "number") {
    return MAX_GRAPHILE_JOB_ATTEMPTS;
  }
  return Math.max(1, job.max_attempts - job.attempts + 1);
}
