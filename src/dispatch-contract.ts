import {
  isSingleOrchestratorMessage,
  messageRunSpecVersion,
  SPEC_VERSION_SINGLE_ORCHESTRATOR,
  singleOrchestratorRunIdOf,
} from "@workflow/world";

/**
 * The dispatcher→agent contract, shared by both ends so they cannot drift.
 *
 * Versioned explicitly, and actually checked. eve's own stream-version header
 * is sent but never validated; repeating that mistake here would mean a
 * dispatcher change could silently misbehave against an older bundled world
 * instead of failing with a clear error.
 *
 * Compatibility rule: a deployment accepts the versions from
 * {@link MIN_DISPATCH_VERSION} to the newest it knows. Within a version the
 * dispatcher may only *add* optional headers, so an older bundle keeps working
 * against a newer dispatcher of the same version. A new version is a break both
 * ways, and every deployment's world is baked at build time.
 *
 * Version 2 moved the serialization of a run's orchestrator deliveries from a
 * graphile queue per run into the dispatcher (`dispatcher/run-gate.ts`). A
 * version 1 dispatcher has no gate, and a version 2 World enqueues without a
 * queue name, so a pairing of the two would deliver a run's orchestrator
 * messages unserialized. The World refuses it instead.
 */
export const DISPATCH_VERSION = 2;

/** The oldest dispatch version this World accepts; see {@link DISPATCH_VERSION}. */
export const MIN_DISPATCH_VERSION = 2;

/**
 * The graphile task name the World enqueues to and the dispatcher claims from.
 *
 * Both ends have to agree on it, so it lives here rather than in either half.
 * The name is shared by every tenant, because the dispatcher deliberately
 * claims across all of them. There is one name because `@workflow/world` has
 * one queue kind.
 */
export const FLOW_JOB_NAME = "eveland_wf_flows";

/**
 * The graphile queue-name prefix a 1.x World gave every orchestrator delivery
 * (`wfrun:<tenant>:<run>`). Nothing writes it any more; boot recovery reads it
 * only to recognise the jobs a 1.x World left behind.
 */
export const LEGACY_RUN_QUEUE_PREFIX = "wfrun:";

/**
 * The graphile job flag the dispatcher's per-tenant cap forbids
 * (`createFairness`). The World stamps it on every flow job, so both ends must
 * spell it the same.
 */
export function tenantFlag(tenantId: string): string {
  return `project:${tenantId}`;
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
 * Checked on the deployment side for every inbound dispatch. Every delivery
 * comes from the dispatcher, so a request without the header is refused like
 * any other unauthenticated one. A version below `minimum` is refused too: an
 * older dispatcher does not serialize a run's orchestrator deliveries for this
 * World, so its deliveries are refused rather than run beside each other.
 */
export function checkDispatchVersion(
  headerValue: string | null | undefined,
  supported: number = DISPATCH_VERSION,
  minimum: number = MIN_DISPATCH_VERSION,
): DispatchRejection | undefined {
  if (headerValue === null || headerValue === undefined || headerValue === "") {
    return { status: 401, error: `Missing ${DISPATCH_VERSION_HEADER}.` };
  }
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
  if (version < minimum) {
    return {
      status: 400,
      error:
        `This deployment's workflow world needs dispatch version ${String(minimum)} or newer, ` +
        `but the dispatcher sent version ${String(version)}. Upgrade the dispatcher.`,
    };
  }
  return undefined;
}

/**
 * The environment variable carrying the shared dispatch secret.
 *
 * This lives in the contract module because BOTH ends must agree. The deployment
 * side reads it in `createQueueHandler` and the host side reads it when building
 * the dispatch headers. A name honoured by only one end produces a dispatcher
 * that starts clean, passes every test, and then 401s every single dispatch.
 */
export const RUNTIME_SECRET_ENV_NAME = "WORKFLOW_WORLD_RUNTIME_SECRET";

/** The shared dispatch secret, or `undefined` when unset or empty. */
export function readRuntimeSecretFromEnv(env: NodeJS.ProcessEnv): string | undefined {
  return env[RUNTIME_SECRET_ENV_NAME] || undefined;
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
 * it. The dispatcher's run gate is the half that keeps overlaps rare
 * (`capabilities.maxConcurrency`, `dispatcher/run-gate.ts`): a spec 9 run's
 * orchestrator deliveries take the run's gate, one delivery at a time, and
 * nothing bypasses it. A hook wake has no reason to: the running orchestrator
 * tails the run's log, so a resume or a cancel reaches it without a second
 * delivery, and a second orchestrator would run an inline step the first is
 * still running again.
 *
 * A spec 9 run's step messages and a long wait's intermediate timer hops are
 * not orchestrator deliveries and never take the gate (upstream's
 * `singleOrchestratorRunIdOf`): an orchestrator runs inline steps for as long
 * as they take, and one the dispatcher granted a live event feed
 * (`LIVE_FEED_HEADER`) stays live up to 30 s more (5 s under Workflow 5.7,
 * eve 1.2+) while it waits on a background step (eve's linger), so a step held behind it would wait all of
 * that out before it could even start. A hop must never be coalesced either:
 * its delivery sends the next hop, and a re-run that finds the wait not yet
 * due would not.
 *
 * This World creates only spec 9 runs, and the runtime stamps every message of
 * one with its version. A message that names a run but carries no stamp (boot
 * recovery rebuilds one from the run row) still takes that run's gate, the
 * conservative reading.
 */

/** The run a queue message belongs to, as its body names it. */
export function messageRunId(body: unknown): string | undefined {
  if (!body || typeof body !== "object") return undefined;
  const { runId, workflowRunId } = body as { runId?: unknown; workflowRunId?: unknown };
  if (typeof runId === "string") return runId;
  return typeof workflowRunId === "string" ? workflowRunId : undefined;
}

function isSingleOrchestratorRunMessage(body: unknown): boolean {
  return (messageRunSpecVersion(body) ?? 0) >= SPEC_VERSION_SINGLE_ORCHESTRATOR;
}

/**
 * The run a message is an orchestrator delivery for, or `undefined` when it is
 * not one: a spec 9 run's step message or timer hop, or a body that names no
 * run. A message that names a run but carries no spec stamp counts as one.
 */
export function orchestratorDeliveryRunId(body: unknown): string | undefined {
  return isSingleOrchestratorRunMessage(body)
    ? singleOrchestratorRunIdOf(body)
    : messageRunId(body);
}

/**
 * Whether a later delivery of the run covers this orchestrator message, so the
 * dispatcher may coalesce it into one while the run's gate is held (upstream
 * world-postgres's `isCoveredWake`, as world-vercel coalesces): its producer
 * committed what it is for (a step outcome, a `hook_received`, a cancellation,
 * a due wait) before sending it, and every delivery reads the run's log. A
 * start message (`runInput`) carries the run's input for a start that may not
 * have committed `run_created` yet, and a legacy hook's input (`hookInput`) or
 * an invocation (`invoke`) carries data of its own: those are never dropped.
 */
export function isCoalescibleWake(body: unknown): boolean {
  return !(
    typeof body === "object" &&
    body !== null &&
    ("runInput" in body || "invoke" in body || "hookInput" in body)
  );
}

/**
 * Sent as `1` on a delivery the dispatcher grants a live event feed: the
 * executor's `events.subscribe` opens one only inside such a delivery
 * (`delivery-context.ts`). With a live feed the runtime holds the delivery
 * open after its work is done (eve's linger: up to 30 s while a background
 * step is outstanding, 5 s under Workflow 5.7 / eve 1.2+, and 5 s while only a
 * hook is open), and the held POST is
 * one of the dispatcher's concurrency slots, so the dispatcher grants a
 * bounded share of them (`createLiveFeedGrants`) and only to orchestrator
 * deliveries.
 *
 * An optional header: a dispatcher that does not send it gets a World that
 * refuses every feed, so no delivery lingers; a World that does not read it
 * ignores it.
 */
export const LIVE_FEED_HEADER = "x-eveland-live-feed";

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

/**
 * The stored message that puts one back without delivering it: a message the
 * dispatcher set aside (a data-bearing message that found its run's gate held,
 * or a wake left pending when the gate's holder ended). Not a delivery, so the
 * new job's first claim reports this one's `attempt` and `deliveryCount` again
 * (upstream world-postgres re-adds a message it left on a run lease the same
 * way).
 */
export function setAsideCopyOf<T extends DeliveryOffsets>(
  message: T,
  delivery: { attempt: number; deliveryCount: number },
): T {
  const { attemptOffset: _attempts, deliveryOffset: _deliveries, ...rest } = message;
  return {
    ...rest,
    attempt: delivery.attempt,
    ...(delivery.attempt > 1 ? { attemptOffset: delivery.attempt - 1 } : {}),
    ...(delivery.deliveryCount > 1 ? { deliveryOffset: delivery.deliveryCount - 1 } : {}),
  } as T;
}
