import type { Pool } from "pg";

/**
 * `embedded` keeps today's topology: the world runs its own graphile runner
 * in-process and POSTs vqs messages to the executor over loopback, exactly as
 * world-postgres does. It stays supported forever because it is the local
 * development story.
 *
 * `external` starts no runner at all — the platform dispatcher claims this
 * tenant's jobs out of the shared database and POSTs them back in. That is what
 * makes a durable timer fire on a project whose agent has been idle-reaped.
 */
export type WorkflowRunnerMode = "embedded" | "external";

type PgConnectionConfig =
  | { connectionString: string; maxPoolSize?: number; pool?: undefined }
  | { pool: Pool; connectionString?: undefined; maxPoolSize?: undefined };

export type EvelandWorldConfig = PgConnectionConfig & {
  /** Eveland project id. Falls back to `EVELAND_PROJECT_ID`. */
  tenantId?: string;
  /** Eveland deployment id. Falls back to `EVELAND_DEPLOYMENT_ID`. */
  deploymentId?: string;
  /** Falls back to `EVELAND_WORKFLOW_RUNNER`, then `embedded`. */
  runner?: WorkflowRunnerMode;
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
  /** Port of the local eve executor, for embedded dispatch. */
  port?: number;
  queueConcurrency?: number;
  /**
   * Override the flush interval (in ms) for buffered stream writes.
   * Default is 10ms. Set to 0 for immediate flushing.
   */
  streamFlushIntervalMs?: number;
};

/**
 * Resolved, fully-defaulted configuration. Everything downstream reads this
 * rather than `process.env`, so tests can construct a world without touching
 * the ambient environment.
 */
export type ResolvedWorldConfig = {
  tenantId: string;
  deploymentId: string;
  runner: WorkflowRunnerMode;
  /** Resolved once at construction; `undefined` means the default prefix. */
  queueNamespace?: string;
  port?: number;
  queueConcurrency: number;
  streamFlushIntervalMs?: number;
};

export function resolveRunnerMode(value: string | undefined): WorkflowRunnerMode {
  if (value === "external") return "external";
  if (value === "embedded" || value === undefined || value === "") return "embedded";
  throw new Error(`Invalid workflow runner mode "${value}": expected "embedded" or "external".`);
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
 * `WORKFLOW_WORLD_URL` is this package's own name. `EVELAND_WORKFLOW_WORLD_URL`
 * is accepted because Eveland injects it today. Both ends of the system — the
 * World inside a deployment and the dispatcher on the host — read the same two
 * names in the same order, so a host that sets only one of them cannot end up
 * with a dispatcher claiming from a database nothing writes to.
 */
export function resolveConnectionString(env: NodeJS.ProcessEnv): string {
  const url = env.WORKFLOW_WORLD_URL ?? env.EVELAND_WORKFLOW_WORLD_URL;
  if (!url) {
    throw new Error(
      "WORKFLOW_WORLD_URL is required by @evelandhq/workflow-world (EVELAND_WORKFLOW_WORLD_URL is also accepted). The host injects it into every deployment; a missing value means this process was configured for a different world.",
    );
  }
  return url;
}
