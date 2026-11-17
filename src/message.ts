import { MessageId } from "@workflow/world";
import { z } from "zod/v4";
import { messageRunId } from "./dispatch-contract.js";
import { Base64Buffer } from "./zod.js";

/**
 * graphile-worker is using JSON under the hood, so we need to base64 encode
 * the body to ensure binary safety
 * maybe later we can have a `blobs` table for larger payloads
 *
 * `tenantId` and `deploymentId` are Eveland additions. The dispatcher reads
 * both straight off the job: the tenant to scope its storage reads, the
 * deployment as the affinity hint for which agent should execute the step. The
 * run row stays authoritative for affinity — these are what let the dispatcher
 * find that row at all, without deserializing the vqs body. Ids only, never
 * state.
 */
export const MessageData = z.object({
  attempt: z.number().describe("The attempt number of the message"),
  /**
   * Deliveries of this message before its current job, and the attempts a
   * spec 9 handoff carried over into it; see `deliveryAccounting` in
   * `dispatch-contract.ts`. Absent on a message's first job, and on every job
   * enqueued before these existed, which then counts from its own attempts.
   */
  deliveryOffset: z.number().int().nonnegative().optional(),
  attemptOffset: z.number().int().nonnegative().optional(),
  messageId: MessageId.describe("The unique ID of the message"),
  idempotencyKey: z.string().optional(),
  headers: z.record(z.string(), z.string()).optional(),
  id: z
    .string()
    .describe(
      "The ID of the sub-queue. For workflows, it's the workflow name. For steps, it's the step name.",
    ),
  data: Base64Buffer.describe("The message that was sent"),
  tenantId: z.string().describe("Eveland project id that owns this message"),
  deploymentId: z
    .string()
    .describe(
      "Deployment the message is addressed to -- the producer's own unless it named another; the dispatcher's affinity hint",
    ),
  /**
   * eve's queue namespace, as resolved by the *enqueuing* deployment.
   *
   * It has to travel on the message. `id` holds only the bare sub-queue id —
   * `parseQueueName` strips the prefix at enqueue — so the delivery side has to
   * rebuild `__<namespace>_wkf_workflow_<id>`, and the dispatcher runs in a
   * different process from the deployment. Resolving `WORKFLOW_QUEUE_NAMESPACE`
   * from the dispatcher's own environment would read the host's value, not the
   * tenant's, and address a queue that executor does not own.
   *
   * Optional so a message enqueued before this field existed still parses.
   *
   * Absent means the default prefix — but only as the reading of a message whose
   * producer is known to have had no namespace. It is not a safe inference about
   * a deployment in general: eve bakes the namespace into the workflow bundle at
   * build time, so a container with no `WORKFLOW_QUEUE_NAMESPACE` in its
   * environment can still be serving a namespaced world, and namespaced
   * deployments are the norm rather than the exception. Anything reconstructing
   * a message rather than receiving one — see `dispatcher/boot-recovery.ts` —
   * has to read the namespace from durable state, not from its own environment
   * and not from this field's absence.
   */
  queueNamespace: z
    .string()
    .optional()
    .describe("eve queue namespace resolved by the enqueuing deployment"),
  /**
   * The run the message belongs to, as its body names it (`messageRunId`),
   * copied out of the body so SQL can find a run's jobs without decoding
   * `data`: boot recovery reads it to tell a run that still has a job from one
   * that has none. The body stays authoritative for everything else; the
   * dispatcher reads the run, and whether a message is an orchestrator
   * delivery, from the body.
   *
   * Absent on a message that names no run (a health check), and on every job a
   * 1.x World wrote, which boot recovery recognises by its `wfrun:` queue
   * instead.
   */
  runId: z.string().optional().describe("The run the message belongs to"),
  /**
   * How many times the dispatcher set this message aside undelivered because
   * its run's gate was held (`dispatcher/run-gate.ts`); sets how long it waits
   * before it is tried again. Absent until the first time.
   */
  setAside: z.number().int().nonnegative().optional(),
});
export type MessageData = z.infer<typeof MessageData>;

/** The message with {@link MessageData.runId} set from its body, for a job written here. */
export function withRunId(message: MessageData): MessageData {
  const { runId: _previous, ...rest } = message;
  const body = readBody(message);
  // A health check may name the run it prepares, but it drives nothing.
  const healthCheck = (body as { __healthCheck?: unknown } | undefined)?.__healthCheck === true;
  const runId = healthCheck ? undefined : messageRunId(body);
  return runId === undefined ? rest : { ...rest, runId };
}

/** The vqs payload, parsed, or `undefined` when it is not JSON. */
export function readBody(message: MessageData): unknown {
  try {
    return JSON.parse(Buffer.from(message.data).toString("utf8")) as unknown;
  } catch {
    return undefined;
  }
}

/**
 * Extracts the run id from a vqs payload without interpreting the rest of it.
 * A body the dispatcher cannot read is still dispatchable: affinity falls back
 * to the message's deployment hint.
 */
export function readRunId(message: MessageData): string | undefined {
  return messageRunId(readBody(message));
}
