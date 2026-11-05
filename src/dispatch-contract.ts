import {
  isSingleOrchestratorMessage,
  messageRunSpecVersion,
  SPEC_VERSION_SINGLE_ORCHESTRATOR,
  singleOrchestratorRunIdOf,
  type WorkflowInvokePayload,
} from "@workflow/world";

/**
 * The dispatcher→agent contract, shared by both ends so they cannot drift.
 *
 * Versioned explicitly, and actually checked. eve's own stream-version header
 * is sent but never validated; repeating that mistake here would mean a
 * dispatcher change could silently misbehave against an older bundled world
 * instead of failing with a clear error.
 *
 * Compatibility rule: a deployment accepts any version up to the newest it
 * knows. Within a major version the dispatcher may only *add* optional headers,
 * so old bundles keep working against a newer dispatcher — which is the
 * situation for the entire run-out, since a deployment's world is baked at
 * build time and never upgraded in place.
 */
export const DISPATCH_VERSION = 1;

/**
 * The graphile task name the World enqueues to and the dispatcher claims from.
 *
 * Both ends have to agree on it, so it lives here rather than in either half.
 * In `embedded` mode the World suffixes it per tenant (see `getJobQueueName`);
 * in `external` mode the name is shared, because the dispatcher deliberately
 * claims across all tenants.
 *
 * There is one name because `@workflow/world` has one queue kind. Up to
 * 5.0.0-beta.22 there was also a `'step'` kind with its own queue; beta.23
 * removed it, and the runtime now runs steps inline inside the flow handler.
 */
export const FLOW_JOB_NAME = "eveland_wf_flows";

/**
 * The graphile task name for a delivery that is not due yet, held outside its
 * run's queue until it is.
 *
 * graphile 0.16's `get_job` locks every available named queue on every claim
 * (`for update skip locked` over `_private_job_queues`), so its cost grows with
 * the number of queues, not the number of due jobs. A sleeping run's delayed
 * continuation kept its `wfrun:` queue alive for the whole sleep — eve's session
 * timeout re-arms every 23 hours for a week — and with a few hundred sessions
 * every claim locked a few hundred rows (evelandhq/workflow-world#115).
 *
 * Far-future flow jobs move here (see `timers.ts`), and when one comes due it is
 * re-added as a flow job on the run's queue, so per-run serialization still
 * covers the delivery itself. In `external` mode only the dispatcher writes this
 * name: deployments keep enqueueing flow jobs exactly as before, because their
 * world is baked at build time and an older dispatcher would never claim it. An
 * `embedded` runner uses its own per-tenant suffix of it.
 */
export const TIMER_JOB_NAME = "eveland_wf_timers";

/**
 * graphile queue name that serializes the deliveries for one run. A hook wake
 * that arrives while the queue is held is the one exception; see `isHookWake`.
 *
 * graphile executes jobs sharing a `queueName` strictly one at a time, which is
 * exactly the guarantee `external` mode otherwise loses. In `embedded` mode the
 * task handler holds an in-process `inflightWorkflowRuns` map for this, but that
 * map is unreachable when no in-process runner is registered — and it could not
 * work anyway, because a process-local map cannot coordinate N dispatchers.
 *
 * Duplicate deliveries for one run are ordinary, not exotic: the World's own
 * enqueue uses a fresh ULID per send, so two sends for one run are two
 * independently claimable jobs, and boot recovery's `msg_recover_<runId>` key
 * collapses only against another sweep.
 *
 * Every enqueue path must derive the name the same way or the serialization is
 * silently partial, so it has exactly one definition. Tenant-scoped because run
 * ids come from the runtime and two tenants could in principle mint the same one.
 *
 * One caveat, measured rather than assumed: graphile does NOT reclaim the queue
 * row when the last job in it completes. `_private_job_queues` keeps one row per
 * distinct run for ever, so the dispatcher runs graphile's own `GC_JOB_QUEUES`
 * cleanup periodically. Without that sweep this design is a slow table leak.
 *
 * A second caveat: a queue costs every claim one row lock for as long as it has
 * any job, due or not. A delayed job therefore does not wait in this queue — the
 * dispatcher holds it as a {@link TIMER_JOB_NAME} job and puts it back here only
 * once it is due.
 */
export function runQueueName(tenantId: string, runId: string): string {
  return `wfrun:${tenantId}:${runId}`;
}

/**
 * Whether a message wakes its run for new hook input. Such a wake may pass a run
 * queue that a delivery holds, so a cancel reaches a running step; the reasoning
 * is in docs/design.md, "Delivery and failure semantics".
 *
 * `@workflow/core` marks a wake from `resumeHook` with `hookResumeTiming`
 * (`hookInput` from older producers, which core still honours) and keeps both when it re-routes the
 * wake. A step message can carry the resume's timing too, so `stepId` rules it
 * out. A force-claim victim's wake carries neither marker.
 */
export function isHookWake(
  payload: Pick<WorkflowInvokePayload, "stepId" | "hookInput" | "hookResumeTiming">,
): boolean {
  const { stepId, hookInput, hookResumeTiming } = payload;
  return stepId === undefined && (hookInput !== undefined || hookResumeTiming !== undefined);
}

/**
 * graphile job flags the dispatcher's per-tenant caps forbid (`createFairness`).
 * The World stamps one on every flow job, so both ends must spell them the same.
 * A hook wake that bypasses its run's queue carries the wake flag and counts
 * against a cap of its own.
 */
export function tenantFlag(tenantId: string): string {
  return `project:${tenantId}`;
}

export function wakeFlag(tenantId: string): string {
  return `wake:${tenantId}`;
}

export const DISPATCH_VERSION_HEADER = "x-eveland-dispatch-version";
export const RUNTIME_SECRET_HEADER = "x-eveland-runtime-secret";
export const TENANT_HEADER = "x-eveland-project-id";
export const DEPLOYMENT_HEADER = "x-eveland-deployment-id";
export const RUN_HEADER = "x-eveland-run-id";

/** vqs headers, owned by eve. Reproduced verbatim; changing them breaks dispatch. */
export const VQS_QUEUE_NAME_HEADER = "x-vqs-queue-name";
export const VQS_MESSAGE_ID_HEADER = "x-vqs-message-id";
export const VQS_MESSAGE_ATTEMPT_HEADER = "x-vqs-message-attempt";
/**
 * How many times this message has been delivered, this delivery included,
 * across reschedules and retries. An optional addition to the contract: a
 * runtime that does not read it falls back to the attempt.
 */
export const VQS_MESSAGE_DELIVERY_COUNT_HEADER = "x-vqs-message-delivery-count";

/**
 * Route segment under `/.well-known/workflow/v1/`.
 *
 * Only `flow` remains: `@workflow/utils`'s `WorkflowUrlRoute` dropped its
 * `'step'` member alongside the queue kind, so there is nothing to select.
 */
export type DispatchRoute = "flow";

export type DispatchRejection = { status: number; error: string };

/**
 * Checked on the deployment side for every inbound dispatch.
 *
 * Requests carrying no version header at all are accepted: eve's own queue
 * handler is mounted on the same route and an embedded-mode world POSTs to it
 * over loopback without any Eveland headers. The check exists to reject a
 * *newer* dispatcher, not to make the header mandatory.
 */
export function checkDispatchVersion(
  headerValue: string | null | undefined,
  supported: number = DISPATCH_VERSION,
): DispatchRejection | undefined {
  if (headerValue === null || headerValue === undefined || headerValue === "") return undefined;
  const version = Number(headerValue);
  if (!Number.isInteger(version) || version <= 0) {
    return { status: 400, error: `Invalid ${DISPATCH_VERSION_HEADER}: ${headerValue}` };
  }
  if (version > supported) {
    return {
      status: 400,
      error:
        `This deployment's workflow world speaks dispatch version ${String(supported)}, ` +
        `but the dispatcher sent version ${String(version)}. Rebuild the deployment.`,
    };
  }
  return undefined;
}

/**
 * The environment variables carrying the shared dispatch secret, in precedence
 * order. `WORKFLOW_WORLD_RUNTIME_SECRET` is this package's own name;
 * `EVELAND_SCHEDULER_RUNTIME_SECRET` is the name Eveland already sets.
 *
 * This lives in the contract module because BOTH ends must agree. The deployment
 * side reads it in `createQueueHandler` and the host side reads it when building
 * the dispatch headers. A name honoured by only one end produces a dispatcher
 * that starts clean, passes every test, and then 401s every single dispatch —
 * so the list has exactly one home.
 */
export const RUNTIME_SECRET_ENV_NAMES = [
  "WORKFLOW_WORLD_RUNTIME_SECRET",
  "EVELAND_SCHEDULER_RUNTIME_SECRET",
] as const;

/** First non-empty value among {@link RUNTIME_SECRET_ENV_NAMES}. */
export function readRuntimeSecretFromEnv(env: NodeJS.ProcessEnv): string | undefined {
  for (const name of RUNTIME_SECRET_ENV_NAMES) {
    const value = env[name];
    if (value) return value;
  }
  return undefined;
}

/**
 * Constant-time-ish equality for the shared runtime secret. Length is allowed
 * to leak; the value is not.
 */
export function secretMatches(expected: string, received: string | null | undefined): boolean {
  if (!received || expected.length !== received.length) return false;
  let mismatch = 0;
  for (let i = 0; i < expected.length; i += 1) {
    mismatch |= expected.charCodeAt(i) ^ received.charCodeAt(i);
  }
  return mismatch === 0;
}

/*
 * Single-orchestrator runs (World spec 9).
 *
 * A spec 9 run is driven by one orchestrator invocation at a time, and the
 * in-band fence (storage.ts) refuses the writes of a second one that overlaps
 * it. The queue is the half that keeps overlaps rare
 * (`capabilities.maxConcurrency`): a spec 9 run's orchestrator messages wait
 * in its run queue, one delivery at a time, and nothing bypasses it. A hook
 * wake has no reason to: the running orchestrator tails the run's log, so a
 * resume or a cancel reaches it without a second delivery, and a second
 * orchestrator would run an inline step the first is still running again.
 *
 * A spec 9 run's step messages and a long wait's intermediate timer hops are
 * not orchestrator deliveries and wait in no run queue (upstream's
 * `singleOrchestratorRunIdOf`): an orchestrator that waits on a background step
 * stays live for a while, and a step queued behind it would wait out that
 * linger before it could even start.
 *
 * Earlier runs (spec 8 and below, or an unstamped message) keep this World's
 * own scheme: every delivery of the run waits in its run queue, and only a hook
 * wake may pass a delivery that holds it.
 */

/** The run a queue message belongs to, as its body names it. */
function bodyRunId(body: unknown): string | undefined {
  if (!body || typeof body !== "object") return undefined;
  const { runId, workflowRunId } = body as { runId?: unknown; workflowRunId?: unknown };
  if (typeof runId === "string") return runId;
  return typeof workflowRunId === "string" ? workflowRunId : undefined;
}

function isSingleOrchestratorRunMessage(body: unknown): boolean {
  return (messageRunSpecVersion(body) ?? 0) >= SPEC_VERSION_SINGLE_ORCHESTRATOR;
}

/** The run queue a message waits in, or `undefined` when it waits in none. */
export function runQueueFor(tenantId: string, body: unknown): string | undefined {
  if (isSingleOrchestratorRunMessage(body)) {
    const runId = singleOrchestratorRunIdOf(body);
    return runId === undefined ? undefined : runQueueName(tenantId, runId);
  }
  const runId = bodyRunId(body);
  return runId === undefined ? undefined : runQueueName(tenantId, runId);
}

/** Whether a message may pass a delivery that holds its run queue: an earlier run's hook wake. */
export function mayBypassRunQueue(
  payload: Pick<
    WorkflowInvokePayload,
    "stepId" | "hookInput" | "hookResumeTiming" | "runSpecVersion"
  >,
): boolean {
  return isHookWake(payload) && !isSingleOrchestratorRunMessage(payload);
}

/**
 * The graphile job-key mode of a send. A spec 9 run's keyed message (a step's
 * execution message, a wait's timer hop) is sent with a key only to be
 * deduplicated: the runtime sends it again from later deliveries, and a
 * second job would be a second owner of the step's body. `unsafe_dedupe` makes
 * the send a no-op while any job holds the key -- locked, delayed, or in
 * retry backoff -- which covers `QUEUE_MIN_STEP_KEY_DEDUPE_WINDOW_MS` for as
 * long as the step's message lives. Anything else keeps graphile's default.
 */
export function jobKeyModeFor(
  body: unknown,
  idempotencyKey: string | undefined,
): "unsafe_dedupe" | undefined {
  return idempotencyKey !== undefined && isSingleOrchestratorMessage(body)
    ? "unsafe_dedupe"
    : undefined;
}

/** What a stored message carries about the deliveries before its current job. */
export type DeliveryOffsets = {
  attempt: number;
  /** Deliveries of this message before its current job. */
  deliveryOffset?: number;
  /** Attempts a handoff carried over into the current job. */
  attemptOffset?: number;
};

/**
 * One delivery's `attempt` and `deliveryCount`, from the job's own attempt
 * count (graphile's `job.attempts`, 1 on a job's first claim).
 *
 * `deliveryCount` counts every delivery of the message, whatever caused it: a
 * retry of the job, a reschedule, a handoff. A spec 9 orchestrator re-sends a
 * background step's message from any delivery counted above 1, and a queue
 * that reported 1 for a redelivery would break the at-most-once guarantee of
 * a `maxRetries: 0` step. `attempt`, which the runtime's delivery cap counts,
 * starts over on a `{ timeoutSeconds: n }` reschedule (as it always has here,
 * so a run that sleeps by rescheduling is never failed by the cap) and carries
 * over a spec 9 handoff.
 */
export function deliveryAccounting(
  message: DeliveryOffsets,
  jobAttempts: number | undefined,
): { attempt: number; deliveryCount: number } {
  if (jobAttempts === undefined) {
    return { attempt: message.attempt, deliveryCount: message.attempt };
  }
  return {
    attempt: jobAttempts + (message.attemptOffset ?? 0),
    deliveryCount: jobAttempts + (message.deliveryOffset ?? message.attemptOffset ?? 0),
  };
}

/**
 * The stored message that continues one after a `{ timeoutSeconds }` answer.
 * The same message on a fresh job: its id carries over, and so do the
 * deliveries so far. On a spec 9 run `{ timeoutSeconds: 0 }` is a handoff,
 * which the next delivery continues under the same attempt.
 */
export function continuationOf<T extends DeliveryOffsets>(
  message: T,
  delivery: { attempt: number; deliveryCount: number },
  timeoutSeconds: number,
  body: unknown,
): T {
  const handoff = timeoutSeconds === 0 && isSingleOrchestratorMessage(body);
  const { attemptOffset: _previous, ...rest } = message;
  const attemptOffset = handoff ? delivery.attempt - 1 : message.attemptOffset;
  return {
    ...rest,
    attempt: handoff ? delivery.attempt : delivery.attempt + 1,
    ...(attemptOffset ? { attemptOffset } : {}),
    deliveryOffset: delivery.deliveryCount,
  } as T;
}
