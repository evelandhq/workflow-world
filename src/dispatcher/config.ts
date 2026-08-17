import os from "node:os";
import { resolveStreamCompaction } from "../config.js";

export type DispatcherConfiguration = {
  worldUrl: string;
  apiUrl: string;
  poolSize: number;
  concurrency: number;
  pollIntervalMs: number;
  maxInFlightPerTenant: number;
  dispatchTimeoutMs: number;
  leaseRenewIntervalMs: number;
  activationLeaseTtlMs: number;
  queueGcIntervalMs: number;
  maintenanceIntervalMs: number;
  maintenanceStreamBatchSize: number;
  maintenanceMaxBatches: number;
  maintenanceMaxStreamsToPack: number;
  maintenanceRunBatchSize: number;
  maintenanceCompactSnapshots: boolean;
};

/**
 * The activation lease TTL the host's control API issues. The dispatcher does
 * not set it; it only has to renew inside it. It used to be a hardcoded mirror
 * of eveland's default, which is exactly the kind of silent coupling a
 * standalone package cannot keep — so the host declares it.
 */
const DEFAULT_ACTIVATION_LEASE_TTL_MS = 180_000;

/**
 * How many tenants one dispatcher may have in flight at once, derived from the
 * machine the way build concurrency is. A held dispatch costs one socket and one
 * PG connection slot, not a core, so this is far more generous than a build cap —
 * the point is a ceiling, not a throttle.
 */
export function deriveMaxInFlightPerTenant(machine: { cpuCoreCount: number }): number {
  return Math.max(2, Math.min(16, machine.cpuCoreCount));
}

/**
 * `WORKFLOW_WORLD_*` and `WORKFLOW_DISPATCHER_*` are this package's namespace.
 *
 * The database URL additionally honours the `EVELAND_*` names, in the same order
 * as the World's own `resolveConnectionString`. This is deliberate and it is the
 * one place naming symmetry actually matters: the dispatcher must claim from the
 * database that deployments write to, and giving the two ends different names for
 * it makes that misconfiguration easy to reach and invisible to any test.
 *
 * The bootstrap override exists because the two ends can need different values
 * for the same database: a containerised deployment reaches Postgres as
 * `host.docker.internal` while the dispatcher on the host reaches it as
 * `localhost`.
 */
export function resolveDispatcherConfig(env: NodeJS.ProcessEnv): DispatcherConfiguration {
  const worldUrl =
    env.WORKFLOW_WORLD_BOOTSTRAP_URL ??
    env.EVELAND_WORKFLOW_WORLD_BOOTSTRAP_URL ??
    env.WORKFLOW_WORLD_URL ??
    env.EVELAND_WORKFLOW_WORLD_URL;
  if (!worldUrl) {
    throw new Error(
      "WORKFLOW_WORLD_URL is required: the dispatcher claims jobs from the shared workflow database. " +
        "It must name the same database deployments write to — see WORKFLOW_WORLD_BOOTSTRAP_URL if the host and the containers reach it by different hostnames.",
    );
  }

  const activationLeaseTtlMs = positiveNumber(
    env.WORKFLOW_DISPATCHER_ACTIVATION_LEASE_TTL_MS,
    DEFAULT_ACTIVATION_LEASE_TTL_MS,
  );
  const leaseRenewIntervalMs = positiveNumber(
    env.WORKFLOW_DISPATCHER_LEASE_RENEW_INTERVAL_MS,
    Math.max(1_000, Math.floor(activationLeaseTtlMs / 3)),
  );
  if (leaseRenewIntervalMs >= activationLeaseTtlMs) {
    throw new Error(
      `WORKFLOW_DISPATCHER_LEASE_RENEW_INTERVAL_MS (${String(leaseRenewIntervalMs)}ms) must be well below the ` +
        `activation lease TTL (${String(activationLeaseTtlMs)}ms), or a long step loses its executor mid-flight.`,
    );
  }

  const apiUrl = env.WORKFLOW_DISPATCHER_ACTIVATION_API_URL;
  if (!apiUrl) {
    throw new Error(
      "WORKFLOW_DISPATCHER_ACTIVATION_API_URL is required: the dispatcher wakes a deployment through the host's activation API.",
    );
  }

  // The pool is the authority, not the concurrency. graphile takes one pooled
  // connection per running job, one held permanently for LISTEN, and one held
  // for the dispatcher's lifecycle advisory lock, so a concurrency above
  // `poolSize - 2` cannot actually be served — upstream's
  // 10-and-50 pairing makes graphile warn on every boot and, worse, puts the
  // shared database's connection count out of the operator's hands. This is the
  // database every tenant now shares, so the bound has to be the explicit one.
  const poolSize = positiveNumber(env.WORKFLOW_DISPATCHER_POOL_SIZE, 10);
  const concurrency = positiveNumber(
    env.WORKFLOW_DISPATCHER_CONCURRENCY,
    Math.max(1, poolSize - 2),
  );
  if (concurrency > poolSize - 2) {
    throw new Error(
      `WORKFLOW_DISPATCHER_CONCURRENCY (${String(concurrency)}) must leave two connections in ` +
        `WORKFLOW_DISPATCHER_POOL_SIZE (${String(poolSize)}): one for dispatcher ownership and one ` +
        `for Graphile LISTEN, on top of one per running job.`,
    );
  }

  return {
    worldUrl,
    apiUrl,
    poolSize,
    concurrency,
    pollIntervalMs: positiveNumber(env.WORKFLOW_DISPATCHER_POLL_INTERVAL_MS, 500),
    maxInFlightPerTenant: positiveNumber(
      env.WORKFLOW_DISPATCHER_MAX_INFLIGHT_PER_TENANT,
      deriveMaxInFlightPerTenant({ cpuCoreCount: os.cpus().length }),
    ),
    dispatchTimeoutMs: positiveNumber(env.WORKFLOW_DISPATCHER_DISPATCH_TIMEOUT_MS, 900_000),
    // Reclaims the per-run graphile queue rows. Five minutes is arbitrary but
    // safe: the sweep only deletes queues with no jobs left, so running it more
    // often costs a query and running it less lets rows sit around.
    queueGcIntervalMs: positiveNumber(env.WORKFLOW_DISPATCHER_QUEUE_GC_INTERVAL_MS, 300_000),
    maintenanceIntervalMs: nonNegativeNumber(
      env.WORKFLOW_DISPATCHER_MAINTENANCE_INTERVAL_MS,
      60_000,
    ),
    maintenanceStreamBatchSize: positiveNumber(
      env.WORKFLOW_DISPATCHER_MAINTENANCE_STREAM_BATCH_SIZE,
      50_000,
    ),
    maintenanceMaxBatches: positiveNumber(env.WORKFLOW_DISPATCHER_MAINTENANCE_MAX_BATCHES, 20),
    maintenanceMaxStreamsToPack: positiveNumber(
      env.WORKFLOW_DISPATCHER_MAINTENANCE_MAX_STREAMS_TO_PACK,
      100,
    ),
    maintenanceRunBatchSize: positiveNumber(
      env.WORKFLOW_DISPATCHER_MAINTENANCE_RUN_BATCH_SIZE,
      1_000,
    ),
    maintenanceCompactSnapshots: resolveStreamCompaction(
      env.WORKFLOW_WORLD_STREAM_COMPACTION ?? env.EVELAND_WORKFLOW_STREAM_COMPACTION,
    ),
    leaseRenewIntervalMs,
    activationLeaseTtlMs,
  };
}

function positiveNumber(value: string | undefined, fallback: number): number {
  const parsed = Number.parseInt(value ?? "", 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
}

function nonNegativeNumber(value: string | undefined, fallback: number): number {
  const parsed = Number.parseInt(value ?? "", 10);
  return Number.isFinite(parsed) && parsed >= 0 ? parsed : fallback;
}
