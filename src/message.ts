import { MessageId } from "@workflow/world";
import { z } from "zod/v4";
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
    .describe("Deployment that enqueued the message; the dispatcher's affinity hint"),
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
});
export type MessageData = z.infer<typeof MessageData>;
