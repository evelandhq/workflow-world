import { nodeHttpFetch } from "@workflow/world/node-http.js";
import {
  DEPLOYMENT_HEADER,
  DISPATCH_VERSION,
  DISPATCH_VERSION_HEADER,
  RUN_HEADER,
  RUNTIME_SECRET_HEADER,
  TENANT_HEADER,
  VQS_MESSAGE_ATTEMPT_HEADER,
  VQS_MESSAGE_ID_HEADER,
  VQS_QUEUE_NAME_HEADER,
} from "../dispatch-contract.js";

/**
 * The dispatcher's half of the dispatch contract: one held POST per in-flight
 * step, to the deployment's loopback port.
 *
 * The response vocabulary mirrors what the embedded runner does, because the
 * runtime cannot tell the two apart:
 *   * `{ok:true}`      → the job is done;
 *   * `{timeoutSeconds}` → not done; the caller must re-enqueue the *same*
 *     messageId with that delay. eve uses this as its delayed backstop and the
 *     runtime's step-ownership lease keys off the message id, so minting a new
 *     one here would silently degrade crash recovery.
 */
export type VqsResult =
  | { type: "completed" }
  | { type: "reschedule"; timeoutSeconds: number }
  | { type: "error"; status: number; text: string; retryable: boolean };

export type VqsRequest = {
  /**
   * Where the executor listens. A port means loopback, which is every host that
   * runs its executors beside the dispatcher; a URL is an executor somewhere
   * else — a service in front of several replicas of one deployment. When both
   * are given the URL wins, so a control plane can add it without removing the
   * port older dispatchers still read.
   */
  endpointPort?: number;
  endpointUrl?: string;
  queueName: string;
  messageId: string;
  attempt: number;
  body: Uint8Array;
  headers?: Record<string, string>;
  tenantId: string;
  deploymentId: string;
  runId?: string;
  runtimeSecret: string;
  timeoutMs: number;
  signal?: AbortSignal;
};

export const WORKFLOW_ROUTE_BASE = "/.well-known/workflow/v1";

/**
 * The origin of an executor named by URL, or why it cannot be one.
 *
 * Only the origin is kept. The route is this package's to choose — a dispatch
 * carries the runtime secret, and the place it is sent must not be steerable to
 * another path on the host by whoever filled in the URL. Credentials in a URL
 * would end up in logs and error text, so they are refused rather than dropped.
 */
export function parseEndpointUrl(value: string): { origin: string } | { error: string } {
  let url: URL;
  try {
    url = new URL(value);
  } catch {
    return { error: `Executor endpoint ${JSON.stringify(value)} is not a URL.` };
  }
  if (url.protocol !== "http:" && url.protocol !== "https:") {
    return { error: `Executor endpoint ${url.origin} must be http or https.` };
  }
  if (url.username !== "" || url.password !== "") {
    return { error: `Executor endpoint ${url.host} must not carry credentials.` };
  }
  if ((url.pathname !== "/" && url.pathname !== "") || url.search !== "" || url.hash !== "") {
    return { error: `Executor endpoint ${url.origin} must be an origin, with no path or query.` };
  }
  return { origin: url.origin };
}

function flowUrl(request: VqsRequest): string | { error: string } {
  // One route only: `WorkflowUrlRoute` lost its `'step'` member in
  // `@workflow/world` 5.0.0-beta.23, alongside the queue kind.
  if (request.endpointUrl !== undefined) {
    const endpoint = parseEndpointUrl(request.endpointUrl);
    return "error" in endpoint ? endpoint : `${endpoint.origin}${WORKFLOW_ROUTE_BASE}/flow`;
  }
  if (request.endpointPort === undefined) {
    return { error: "Dispatch has neither an executor URL nor a loopback port." };
  }
  return `http://127.0.0.1:${String(request.endpointPort)}${WORKFLOW_ROUTE_BASE}/flow`;
}

export async function postVqsMessage(request: VqsRequest): Promise<VqsResult> {
  const url = flowUrl(request);
  if (typeof url !== "string") {
    // Not retryable: the address came from configuration or the control plane,
    // and sending the same message again resolves to the same address.
    return { type: "error", status: 0, text: url.error, retryable: false };
  }
  const headers: Record<string, string> = {
    ...request.headers,
    "content-type": "application/json",
    // eve's own headers. Names are fixed by the runtime; do not rename.
    [VQS_QUEUE_NAME_HEADER]: request.queueName,
    [VQS_MESSAGE_ID_HEADER]: request.messageId,
    [VQS_MESSAGE_ATTEMPT_HEADER]: String(request.attempt),
    // Eveland's. The runtime secret is what distinguishes platform dispatch
    // from a request that merely reached the same route.
    //
    // Deliberately NOT the internal service token: this port is served by the
    // tenant's own agent process, which can read any header it receives. That
    // token authorizes activating, renewing and releasing leases on *any*
    // deployment, so handing it to tenant code would be a privilege escalation
    // across projects. The deployment id below binds the request to one target,
    // so a captured dispatch cannot be replayed at a different deployment.
    [RUNTIME_SECRET_HEADER]: request.runtimeSecret,
    [DISPATCH_VERSION_HEADER]: String(DISPATCH_VERSION),
    [TENANT_HEADER]: request.tenantId,
    [DEPLOYMENT_HEADER]: request.deploymentId,
    ...(request.runId ? { [RUN_HEADER]: request.runId } : {}),
  };

  const timeout = AbortSignal.timeout(request.timeoutMs);
  const signal = request.signal ? AbortSignal.any([timeout, request.signal]) : timeout;

  // Node's core HTTP client, not the global `fetch`. A delivery executes the
  // workflow body inline, so response headers arrive only once that work is
  // done, and undici gives up on them after a fixed 300 seconds that a caller of
  // the global `fetch` cannot lift. That is well inside `timeoutMs`, so a
  // slow-but-healthy step was declared dead and redelivered while the original
  // was still running. No headers or body deadline is set here: `timeoutMs` is
  // the only one, and liveness is the lease renewal's job.
  let response: Response;
  try {
    response = await nodeHttpFetch(url, {
      method: "POST",
      headers: new Headers(headers),
      body: request.body,
      signal,
    });
  } catch (error) {
    const name = error instanceof Error ? error.name : "";
    const timedOut = name === "TimeoutError" || name === "AbortError";
    return {
      type: "error",
      status: 0,
      text: timedOut
        ? `Dispatch timed out after ${String(request.timeoutMs)}ms.`
        : describeTransportError(error),
      // A dead or restarting executor is exactly what retries exist for.
      retryable: true,
    };
  }

  const text = await response.text();
  if (!response.ok) {
    return {
      type: "error",
      status: response.status,
      text,
      // 4xx means this deployment will never accept the message — a malformed
      // dispatch or a version it refuses. Retrying just burns the budget.
      retryable: response.status >= 500,
    };
  }

  try {
    const timeoutSeconds = Number(
      (JSON.parse(text) as { timeoutSeconds?: unknown }).timeoutSeconds,
    );
    if (Number.isFinite(timeoutSeconds) && timeoutSeconds >= 0) {
      return { type: "reschedule", timeoutSeconds };
    }
  } catch {
    // A non-JSON 2xx body means "done"; upstream treats it the same way.
  }
  return { type: "completed" };
}

/**
 * A transport failure with its cause chain spelled out. `String(error)` alone
 * loses the `code` (`ECONNREFUSED`, `ECONNRESET`, ...) that tells a dead
 * executor apart from a delivery that was cut off mid-flight.
 */
function describeTransportError(error: unknown): string {
  const parts: string[] = [];
  const seen = new Set<unknown>();
  for (let current = error; current !== undefined && current !== null;) {
    if (seen.has(current)) break;
    seen.add(current);
    if (!(current instanceof Error)) {
      parts.push(String(current));
      break;
    }
    const code = (current as NodeJS.ErrnoException).code;
    const text = String(current);
    parts.push(code !== undefined && !text.includes(code) ? `${text} [${code}]` : text);
    current = current.cause;
  }
  return parts.join(" <- ");
}
