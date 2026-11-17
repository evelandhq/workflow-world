import type { Pool } from "pg";

type PgConnectionConfig =
  | { connectionString: string; maxPoolSize?: number; pool?: undefined }
  | { pool: Pool; connectionString?: undefined; maxPoolSize?: undefined };

export type EvelandWorldConfig = PgConnectionConfig & {
  /** The tenant this deployment writes as. Falls back to `WORKFLOW_WORLD_TENANT_ID`. */
  tenantId?: string;
  /** This deployment's id. Falls back to `WORKFLOW_WORLD_DEPLOYMENT_ID`. */
  deploymentId?: string;
  /**
   * eve's queue namespace. Falls back to `WORKFLOW_QUEUE_NAMESPACE`.
   *
   * Nothing to do with tenancy — tenancy is the `tenant_id` column, and
   * prefix-based isolation is deliberately never used for it. This is eve's own
   * queue naming: when a namespace is set, the runtime registers handlers for
   * `__<namespace>_wkf_workflow_*`, and a queue name we build without it
   * addresses a queue that executor does not own.
   */
  queueNamespace?: string;
  /**
   * Override the flush interval (in ms) for buffered stream writes.
   * Default is 10ms. Set to 0 for immediate flushing.
   */
  streamFlushIntervalMs?: number;
  /**
   * Strip Eve's accumulated stream snapshots before persistence (default true).
   *
   * @deprecated Will be removed in 2.0. eve 1.0 only writes delta appends, so the
   * stripping it switches is a no-op guard.
   */
  compactStreamSnapshots?: boolean;
};

/**
 * Resolved, fully-defaulted configuration. Everything downstream reads this
 * rather than `process.env`, so tests can construct a world without touching
 * the ambient environment.
 */
export type ResolvedWorldConfig = {
  tenantId: string;
  deploymentId: string;
  /** Resolved once at construction; `undefined` means the default prefix. */
  queueNamespace?: string;
  streamFlushIntervalMs?: number;
  /**
   * @deprecated Will be removed in 2.0. eve 1.0 only writes delta appends, so
   * write-side snapshot stripping is a no-op guard.
   */
  compactStreamSnapshots: boolean;
};

/**
 * Resolve the emergency switch for write-side stream snapshot compaction.
 *
 * @deprecated Will be removed in 2.0 with write-side snapshot stripping, which
 * is a no-op now that eve 1.0 only writes delta appends.
 */
export function resolveStreamCompaction(value: string | undefined): boolean {
  if (value === undefined || value === "" || value === "on" || value === "true" || value === "1") {
    return true;
  }
  if (value === "off" || value === "false" || value === "0") return false;
  throw new Error(
    `Invalid stream compaction setting "${value}": expected "on"/"true"/"1" or "off"/"false"/"0".`,
  );
}

/**
 * The shared world database, required explicitly.
 *
 * There is deliberately no fallback chain. Upstream falls back
 * `WORKFLOW_POSTGRES_URL -> DATABASE_URL -> a hardcoded localhost URL`, which is
 * harmless for a single-tenant world but actively dangerous here: those are
 * single-tenant databases with no `tenant_id` column and no partitions, so a
 * misconfigured deployment would connect to one and fail in confusing ways at
 * the first write — or worse, appear to work against a database that is not the
 * one holding its runs. Failing at startup with the missing variable named is
 * strictly better than either.
 *
 * Both ends of the system, the World inside a deployment and the dispatcher on
 * the host, read the same name, so a host cannot end up with a dispatcher
 * claiming from a database nothing writes to.
 */
export function resolveConnectionString(env: NodeJS.ProcessEnv): string {
  const url = env.WORKFLOW_WORLD_URL;
  if (!url) {
    throw new Error(
      "WORKFLOW_WORLD_URL is required by @evelandhq/workflow-world. The host injects it into every deployment; a missing value means this process was configured for a different world.",
    );
  }
  return url;
}
