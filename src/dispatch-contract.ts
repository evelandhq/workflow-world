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
