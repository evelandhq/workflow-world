import { MessageData } from "../message.js";
import type { Pool } from "pg";
import type { ActivationClient } from "./activation-client.js";
import { withRenewedLease } from "./lease.js";
import { postVqsMessage, type VqsResult } from "./vqs-client.js";

/**
 * Where a message should execute, and whether it can execute at all.
 *
 * The run row is authoritative: an in-flight run is pinned to the deployment
 * that created it, because that is the only build whose bundle can replay its
 * event log. The message's own `deploymentId` is a hint used when no run row
 * exists yet (the very first delivery of a run).
 */
export type Affinity =
  | { type: "deployment"; deploymentId: string; runId?: string }
  | { type: "unroutable"; kind: "terminal" | "quarantined"; reason: string };

export type RunLookup = (input: { tenantId: string; runId: string }) => Promise<{
  deploymentId: string;
  status: string;
  /**
   * The run has an unresolved dead letter. Boot recovery has always excluded
   * such runs; live dispatch honours the same quarantine so a run that cannot
   * execute stops being redelivered the moment it is dead-lettered.
   */
  quarantined?: boolean;
} | null>;

export type DispatchOutcome =
  | { type: "completed" }
  | { type: "rescheduled"; timeoutSeconds: number }
  | { type: "dead-letter"; reason: string }
  | { type: "retry"; reason: string };

export type DispatcherDeps = {
  activation: ActivationClient;
  runLookup: RunLookup;
  runtimeSecret: string;
  dispatchTimeoutMs: number;
  leaseRenewIntervalMs: number;
  /** The TTL the control plane issues; sets how many missed renewals fit. */
  activationLeaseTtlMs: number;
  reenqueue: (input: {
    jobName: string;
    message: MessageData;
    delaySeconds: number;
  }) => Promise<void>;
  /**
   * `jobName`/`queueName` are optional only because a caller may not have them;
   * the handler does, and passing them is what makes a dead-letter row
   * diagnosable. Recording `message.id` for both — the bare sub-queue id — was
   * the previous behaviour and told you nothing you did not already have.
   */
  onDeadLetter: (input: {
    message: MessageData;
    reason: string;
    jobName?: string;
    queueName?: string;
  }) => Promise<void>;
  log?: (message: string, meta?: Record<string, unknown>) => void;
  /**
   * Per-run accounting of consecutive executor failures. Absent means the
   * containment is off and every executor 5xx retries until graphile gives up.
   */
  executorFailures?: ExecutorFailureTracker;
};

/** Extracts the run id from a vqs payload without interpreting the rest of it. */
export function readRunId(message: MessageData): string | undefined {
  try {
    const body = JSON.parse(Buffer.from(message.data).toString("utf8")) as {
      runId?: unknown;
      workflowRunId?: unknown;
    };
    if (typeof body.runId === "string") return body.runId;
    // Step invocations name it differently.
    if (typeof body.workflowRunId === "string") return body.workflowRunId;
  } catch {
    // A body the dispatcher cannot read is still dispatchable — affinity just
    // falls back to the message's deployment hint.
  }
  return undefined;
}

export async function resolveAffinity(
  message: MessageData,
  runLookup: RunLookup,
): Promise<Affinity> {
  const runId = readRunId(message);
  if (!runId) {
    return { type: "deployment", deploymentId: message.deploymentId };
  }
  const run = await runLookup({ tenantId: message.tenantId, runId });
  if (!run) {
    // First delivery: the run row is created by the executor when it handles
    // this very message, so the enqueuing deployment is the right target.
    return { type: "deployment", deploymentId: message.deploymentId, runId };
  }
  if (run.status === "completed" || run.status === "failed" || run.status === "cancelled") {
    return {
      type: "unroutable",
      kind: "terminal",
      reason: `Run ${runId} is already terminal (${run.status}).`,
    };
  }
  if (run.quarantined) {
    // Every delivery of a quarantined run would reproduce the failure that
    // dead-lettered it, and a run that fails at execution keeps enqueueing its
    // own wake-ups, so redelivering here is what turns one bad run into a
    // storm. The dead letter already holds a replayable copy of the message;
    // resolving it is the operator's explicit retry boundary.
    return {
      type: "unroutable",
      kind: "quarantined",
      reason: `Run ${runId} has an unresolved dead letter; delivery stays parked until an operator resolves it.`,
    };
  }
  return { type: "deployment", deploymentId: run.deploymentId, runId };
}

/**
 * One message, start to finish. Every branch maps onto a row of the design's
 * failure table, and the caller turns the outcome into graphile's vocabulary
 * (return = done, throw = retry).
 */
export async function dispatchMessage(
  input: {
    message: MessageData;
    jobName: string;
    queueName: string;
    attempt: number;
  },
  deps: DispatcherDeps,
): Promise<DispatchOutcome> {
  const { message } = input;
  const affinity = await resolveAffinity(message, deps.runLookup);
  if (affinity.type === "unroutable") {
    // Not an error: a terminal run's straggler message has nowhere to go and
    // nothing to do, and a quarantined run's message is parked behind its dead
    // letter. Dropping either is correct and must not burn retries.
    const runId = readRunId(message);
    deps.log?.(
      affinity.kind === "quarantined"
        ? "dropping message for quarantined run"
        : "dropping message for terminal run",
      {
        tenantId: message.tenantId,
        ...(runId === undefined ? {} : { runId }),
        reason: affinity.reason,
      },
    );
    return { type: "completed" };
  }

  const activation = await deps.activation.activate({
    deploymentId: affinity.deploymentId,
    kind: "workflow_step",
    ownerId: `workflow-dispatcher:${message.messageId}`,
  });

  if (activation.type === "not-activatable") {
    // Archived or failed deployment: no retry can produce an executor. This is
    // the case the retention guard exists to prevent, so it is worth an alarm
    // rather than a silent drop.
    return {
      type: "dead-letter",
      reason: `Deployment ${affinity.deploymentId} is not activatable: ${activation.message}`,
    };
  }
  if (activation.type === "unavailable") {
    return {
      type: "retry",
      reason: `Activation failed (HTTP ${String(activation.status)}): ${activation.message}`,
    };
  }

  const result: VqsResult = await withRenewedLease(
    {
      client: deps.activation,
      leaseId: activation.activation.leaseId,
      renewIntervalMs: deps.leaseRenewIntervalMs,
      leaseTtlMs: deps.activationLeaseTtlMs,
      onRenewFailure: (leaseId, consecutiveFailures) =>
        deps.log?.("activation lease renewal failed", {
          leaseId,
          tenantId: message.tenantId,
          consecutiveFailures,
        }),
    },
    (signal) =>
      postVqsMessage({
        endpointPort: activation.activation.endpointPort,
        queueName: input.queueName,
        messageId: message.messageId,
        attempt: input.attempt,
        body: message.data,
        ...(message.headers ? { headers: message.headers } : {}),
        tenantId: message.tenantId,
        deploymentId: affinity.deploymentId,
        ...(affinity.runId ? { runId: affinity.runId } : {}),
        runtimeSecret: deps.runtimeSecret,
        timeoutMs: deps.dispatchTimeoutMs,
        signal,
      }),
  );

  const failureKey =
    affinity.runId === undefined ? undefined : `${message.tenantId}:${affinity.runId}`;
  if (result.type === "completed") {
    if (failureKey !== undefined) deps.executorFailures?.reset(failureKey);
    return { type: "completed" };
  }

  if (result.type === "reschedule") {
    if (failureKey !== undefined) deps.executorFailures?.reset(failureKey);
    // Enqueued before returning, so a dispatcher crash between the two cannot
    // lose the wake-up. The message id is preserved deliberately: the runtime
    // uses it as the step-ownership lease.
    await deps.reenqueue({
      jobName: input.jobName,
      message: { ...message, attempt: input.attempt + 1 },
      delaySeconds: result.timeoutSeconds,
    });
    return { type: "rescheduled", timeoutSeconds: result.timeoutSeconds };
  }

  if (!result.retryable) {
    return {
      type: "dead-letter",
      reason: `Executor rejected the dispatch with HTTP ${String(result.status)}: ${result.text}`,
    };
  }
  // A 5xx means the executor ran the message and threw. Once is a blip; the
  // same run failing that way delivery after delivery is a run that can never
  // execute — a bundle that cannot replay its log, a row the database refuses —
  // and graphile's 49 attempts with exponential backoff would keep waking its
  // deployment for days while the run enqueues more wake-ups of its own.
  // Transport errors (status 0: the process is dead or restarting) are exactly
  // what retries exist for and never count.
  if (failureKey !== undefined && result.status >= 500 && deps.executorFailures) {
    const streak = deps.executorFailures.record(failureKey);
    if (streak.exhausted) {
      deps.executorFailures.reset(failureKey);
      return {
        type: "dead-letter",
        reason:
          `Executor answered HTTP ${String(result.status)} on ${String(streak.count)} consecutive ` +
          `deliveries of run ${String(affinity.runId)} over ${String(Math.round(streak.spanMs / 1000))}s; ` +
          `last response: ${result.text}`,
      };
    }
  }
  return {
    type: "retry",
    reason: `Dispatch failed (HTTP ${String(result.status)}): ${result.text}`,
  };
}

/** Five 5xx deliveries of one run spread over at least a minute. */
export const DEFAULT_EXECUTOR_FAILURE_LIMIT = 5;
export const DEFAULT_EXECUTOR_FAILURE_MIN_SPAN_MS = 60_000;

/**
 * Consecutive executor-5xx accounting per run, behind the dead-letter decision
 * in `dispatchMessage`.
 *
 * A streak is exhausted when it reaches `limit` failures AND has lasted at least
 * `minSpanMs`. The span guard is what separates a run that cannot execute from a
 * database blip: a run that fails at execution fans out wake-up messages, so a
 * thirty-second outage can produce a burst of 5xx within seconds, and counting
 * alone would quarantine healthy runs behind it. A doomed run keeps failing
 * across graphile's backoff, so it clears the span on its own.
 *
 * In-process and bounded on purpose, like the dedup cache: a restart forgets the
 * streaks, and the consequence is a few more retries rather than a wrong answer.
 */
export function createExecutorFailureTracker(
  input: { limit?: number; minSpanMs?: number; trackedRuns?: number; now?: () => number } = {},
) {
  const limit = input.limit ?? DEFAULT_EXECUTOR_FAILURE_LIMIT;
  const minSpanMs = input.minSpanMs ?? DEFAULT_EXECUTOR_FAILURE_MIN_SPAN_MS;
  const trackedRuns = input.trackedRuns ?? 10_000;
  const now = input.now ?? Date.now;
  if (!Number.isFinite(limit) || limit < 1) {
    throw new Error("Executor failure limit must be a positive number.");
  }
  // Insertion-ordered, so the oldest streak is the first one iteration yields.
  const streaks = new Map<string, { count: number; firstAt: number }>();
  return {
    /** Records one executor 5xx for the run and reports the streak it now has. */
    record(key: string): { count: number; spanMs: number; exhausted: boolean } {
      const at = now();
      const previous = streaks.get(key);
      const streak = previous
        ? { count: previous.count + 1, firstAt: previous.firstAt }
        : { count: 1, firstAt: at };
      // Re-insert so the streak that just advanced is the newest.
      streaks.delete(key);
      streaks.set(key, streak);
      if (streaks.size > trackedRuns) {
        const oldest = streaks.keys().next().value;
        if (oldest !== undefined) streaks.delete(oldest);
      }
      const spanMs = Math.max(0, at - streak.firstAt);
      return {
        count: streak.count,
        spanMs,
        exhausted: streak.count >= limit && spanMs >= minSpanMs,
      };
    },
    /** A delivery that did not end in an executor 5xx ends the streak. */
    reset(key: string): void {
      streaks.delete(key);
    },
    stats(): { tracked: number; limit: number; minSpanMs: number } {
      return { tracked: streaks.size, limit, minSpanMs };
    },
  };
}

export type ExecutorFailureTracker = ReturnType<typeof createExecutorFailureTracker>;

/**
 * Per-tenant in-flight accounting behind graphile's `forbiddenFlags`.
 *
 * graphile can only express a deny-list, which is the right shape here: a
 * tenant at its cap is skipped for this claim so the runner moves on to another
 * tenant's jobs, instead of one busy project monopolising the pool.
 */
export function createFairness(input: { maxInFlightPerTenant: number }) {
  const inFlight = new Map<string, number>();
  return {
    forbiddenFlags(): string[] {
      const forbidden: string[] = [];
      for (const [tenantId, count] of inFlight) {
        if (count >= input.maxInFlightPerTenant) forbidden.push(`project:${tenantId}`);
      }
      return forbidden;
    },
    acquire(tenantId: string): void {
      inFlight.set(tenantId, (inFlight.get(tenantId) ?? 0) + 1);
    },
    release(tenantId: string): void {
      const next = (inFlight.get(tenantId) ?? 1) - 1;
      if (next <= 0) inFlight.delete(tenantId);
      else inFlight.set(tenantId, next);
    },
    snapshot(): Record<string, number> {
      return Object.fromEntries(inFlight);
    },
  };
}

export type Fairness = ReturnType<typeof createFairness>;

/**
 * Suppresses a repeat delivery of a message that already completed, and collapses
 * two concurrent deliveries of the same one.
 *
 * Restores parity with `embedded` mode, where the World's task handler keeps
 * exactly these two structures — a bounded set of completed idempotency keys and a
 * map of in-flight ones — and where they are unreachable once no in-process runner
 * is registered.
 *
 * Only messages carrying an `idempotencyKey` participate, which is upstream's rule
 * too: without one there is nothing stable to key on, and ordering for those is
 * the per-run graphile queue's job instead.
 *
 * Deliberately in-process and bounded, matching embedded mode rather than
 * exceeding it. A durable table would give `external` mode a *stronger* guarantee
 * than `embedded` has ever offered, at the cost of a write per message on the
 * hottest table in a database every tenant shares — and it would still not be a
 * correctness boundary, because the thing that makes redelivery safe is the
 * runtime replaying from the event log, not this cache. Treat it as the waste
 * filter it is: a restart legitimately forgets, and the consequence is a replay
 * rather than a wrong answer.
 */
export function createMessageDedup(input: { limit?: number } = {}) {
  const limit = input.limit ?? 10_000;
  // Insertion-ordered, so the oldest key is the first one iteration yields.
  const completed = new Set<string>();
  const inflight = new Map<string, Promise<void>>();

  function markCompleted(key: string): void {
    // Re-insert so a repeat completion refreshes recency rather than keeping the
    // original position.
    completed.delete(key);
    completed.add(key);
    if (completed.size > limit) {
      const oldest = completed.values().next().value;
      if (oldest !== undefined) completed.delete(oldest);
    }
  }

  return {
    /**
     * Runs `execute` unless this message has already completed or is in flight,
     * in which case it waits for the in-flight one and returns.
     */
    async run(
      idempotencyKey: string | undefined,
      execute: () => Promise<DispatchOutcome>,
    ): Promise<DispatchOutcome | undefined> {
      if (idempotencyKey === undefined) return execute();
      if (completed.has(idempotencyKey)) return undefined;

      const existing = inflight.get(idempotencyKey);
      if (existing) {
        await existing;
        return undefined;
      }

      let outcome: DispatchOutcome | undefined;
      const execution = execute()
        .then((result) => {
          outcome = result;
          // Only a completed dispatch is worth suppressing. A retryable failure
          // must stay deliverable, or one blip would permanently swallow the
          // message.
          if (result.type === "completed") markCompleted(idempotencyKey);
        })
        .finally(() => {
          inflight.delete(idempotencyKey);
        });
      inflight.set(idempotencyKey, execution);
      await execution;
      return outcome;
    },
    stats(): { completed: number; inflight: number } {
      return { completed: completed.size, inflight: inflight.size };
    },
  };
}

export type MessageDedup = ReturnType<typeof createMessageDedup>;

/**
 * Reads a run's deployment and status straight from the shared world schema,
 * plus whether an unresolved dead letter quarantines it — the same anti-join
 * boot recovery applies, served by the partial index on unresolved rows.
 */
export function createRunLookup(pool: Pool): RunLookup {
  return async ({ tenantId, runId }) => {
    const { rows } = await pool.query<{
      deployment_id: string;
      status: string;
      quarantined: boolean;
    }>(
      `select runs.deployment_id, runs.status,
              exists (
                select 1
                  from workflow.dispatch_dead_letters as dead
                 where dead.tenant_id = runs.tenant_id
                   and dead.run_id = runs.id
                   and dead.resolved_at is null
              ) as quarantined
         from workflow.workflow_runs as runs
        where runs.tenant_id = $1 and runs.id = $2
        limit 1`,
      [tenantId, runId],
    );
    const row = rows[0];
    return row
      ? { deploymentId: row.deployment_id, status: row.status, quarantined: row.quarantined }
      : null;
  };
}
