import type { ActivationClient } from "./activation-client.js";
import { parseEndpointUrl } from "./vqs-client.js";

/**
 * Activation for executors that are always running: a fixed table from
 * deployment id to the origin its executors are reached at, and nothing to
 * wake.
 *
 * The activation API exists because a platform's executors scale to zero — it
 * starts one, and the lease keeps the idle reaper off it for the length of a
 * step. A service that runs its own replicas has neither problem. Its executors
 * are up, nothing reaps them, and which replica takes a dispatch is decided by
 * whatever stands at the origin (a Kubernetes Service, a load balancer), which
 * also stops choosing a replica once it reports itself unready. So the lease is
 * a formality here: it renews unconditionally and releasing it does nothing.
 *
 * The table is keyed by deployment id, not a single origin, because a run stays
 * pinned to the deployment that created it. During a blue/green cut-over both
 * deployments are listed, each at its own origin, and the old one stays until
 * its runs are over. A deployment that is not in the table is one nobody runs
 * any more: not-activatable, so the message is dead-lettered rather than
 * retried against nothing.
 */
export function createStaticActivationClient(input: {
  endpoints: Readonly<Record<string, string>>;
}): ActivationClient {
  const endpoints = new Map<string, string>();
  for (const [deploymentId, value] of Object.entries(input.endpoints)) {
    const endpoint = parseEndpointUrl(value);
    if ("error" in endpoint) {
      throw new Error(`Static endpoint for deployment ${deploymentId}: ${endpoint.error}`);
    }
    endpoints.set(deploymentId, endpoint.origin);
  }
  let leases = 0;

  return {
    async activate({ deploymentId }) {
      const endpointUrl = endpoints.get(deploymentId);
      if (endpointUrl === undefined) {
        return {
          type: "not-activatable",
          status: 409,
          message: `Deployment ${deploymentId} has no static endpoint configured.`,
        };
      }
      leases += 1;
      return {
        type: "activated",
        activation: { leaseId: `static:${deploymentId}:${String(leases)}`, endpointUrl },
      };
    },
    async renew() {
      return true;
    },
    async release() {},
  };
}

/**
 * Reads `WORKFLOW_DISPATCHER_STATIC_ENDPOINTS`: comma-separated
 * `<deploymentId>=<origin>` pairs. The split is on the first `=`, since an
 * origin never contains one but a deployment id is the host's to choose.
 */
export function parseStaticEndpoints(value: string): Record<string, string> {
  const endpoints: Record<string, string> = {};
  for (const pair of value.split(",")) {
    const entry = pair.trim();
    if (entry === "") continue;
    const separator = entry.indexOf("=");
    const deploymentId = separator < 0 ? "" : entry.slice(0, separator).trim();
    const origin = separator < 0 ? "" : entry.slice(separator + 1).trim();
    if (deploymentId === "" || origin === "") {
      throw new Error(
        `WORKFLOW_DISPATCHER_STATIC_ENDPOINTS entry ${JSON.stringify(entry)} is not <deploymentId>=<origin>.`,
      );
    }
    if (deploymentId in endpoints) {
      throw new Error(
        `WORKFLOW_DISPATCHER_STATIC_ENDPOINTS names deployment ${deploymentId} more than once.`,
      );
    }
    const endpoint = parseEndpointUrl(origin);
    if ("error" in endpoint) {
      throw new Error(
        `WORKFLOW_DISPATCHER_STATIC_ENDPOINTS, deployment ${deploymentId}: ${endpoint.error}`,
      );
    }
    endpoints[deploymentId] = endpoint.origin;
  }
  if (Object.keys(endpoints).length === 0) {
    throw new Error("WORKFLOW_DISPATCHER_STATIC_ENDPOINTS is set but names no deployment.");
  }
  return endpoints;
}
