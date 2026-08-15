import type { Storage, World } from "@workflow/world";
import { resolveQueueNamespace, SPEC_VERSION_CURRENT } from "@workflow/world";
import { Pool } from "pg";
import {
  type EvelandWorldConfig,
  type ResolvedWorldConfig,
  resolveConnectionString,
  resolveRunnerMode,
} from "./config.js";
import { createClient, type Drizzle } from "./drizzle/index.js";
import { createQueue } from "./queue.js";
import {
  createEventsStorage,
  createHooksStorage,
  createRunsStorage,
  createStepsStorage,
} from "./storage.js";
import { createStreamer } from "./streamer.js";
import { assertValidTenantId } from "./tenant.js";
import { reenqueueTenantRuns } from "./recovery.js";

export type { EvelandWorldConfig, WorkflowRunnerMode } from "./config.js";
export * from "./dispatch-contract.js";
export { MessageData } from "./message.js";
export {
  dropTenantPartitions,
  ensureTenantPartitions,
  resolveMigrationsDir,
  runMigrations,
  tenantPartitionsExist,
} from "./migrate.js";
export { reenqueueTenantRuns } from "./recovery.js";
export {
  pruneTerminalStreamChunks,
  type StreamRetentionOptions,
  type StreamRetentionResult,
} from "./retention.js";
export { derivePartitionName, tenantStreamChannel } from "./tenant.js";
export * from "./drizzle/schema.js";

/**
 * `queueNamespace` reaches storage because run creation is the only moment the
 * value is knowable and durable at once. The external dispatcher rebuilds
 * messages for active runs from their rows, in a process that has no access to
 * this deployment's environment — so a namespace not recorded here is a
 * namespace that boot recovery cannot honour.
 */
function createStorage(drizzle: Drizzle, tenantId: string, queueNamespace?: string): Storage {
  return {
    runs: createRunsStorage(drizzle, tenantId),
    events: createEventsStorage(drizzle, tenantId, queueNamespace),
    hooks: createHooksStorage(drizzle, tenantId),
    steps: createStepsStorage(drizzle, tenantId),
  };
}

function getDefaultMaxPoolSize(): number | undefined {
  const parsed = parseInt(process.env.WORKFLOW_POSTGRES_MAX_POOL_SIZE || "", 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : undefined;
}

function required(value: string | undefined, name: string): string {
  if (!value) {
    throw new Error(
      `${name} is required by @evelandhq/workflow-world. The host injects it into every deployment; a missing value means the build did not go through the host's release preparation.`,
    );
  }
  return value;
}

function resolveConfig(config: EvelandWorldConfig): ResolvedWorldConfig {
  // Each of these has one canonical `WORKFLOW_WORLD_*` name owned by this
  // package plus the `EVELAND_*` name the host injects today. Keep both lists in
  // step with `resolveConnectionString` and `resolveDispatchRuntimeSecret`: a
  // name that only one end honours is how you get a process that starts clean
  // and then fails every dispatch.
  const tenantId = required(
    config.tenantId ?? process.env.WORKFLOW_WORLD_TENANT_ID ?? process.env.EVELAND_PROJECT_ID,
    "WORKFLOW_WORLD_TENANT_ID",
  );
  assertValidTenantId(tenantId);
  const queueNamespace = resolveQueueNamespace(config.queueNamespace);
  return {
    tenantId,
    deploymentId: required(
      config.deploymentId ??
        process.env.WORKFLOW_WORLD_DEPLOYMENT_ID ??
        process.env.EVELAND_DEPLOYMENT_ID,
      "WORKFLOW_WORLD_DEPLOYMENT_ID",
    ),
    runner:
      config.runner ??
      resolveRunnerMode(process.env.WORKFLOW_WORLD_RUNNER ?? process.env.EVELAND_WORKFLOW_RUNNER),
    // `resolveQueueNamespace` is upstream's own resolver, so the fallback to
    // `WORKFLOW_QUEUE_NAMESPACE` matches exactly what eve's runtime does.
    ...(queueNamespace !== undefined ? { queueNamespace } : {}),
    ...(config.port !== undefined ? { port: config.port } : {}),
    // `??` cannot express this: `parseInt("")` is NaN, which is not nullish, so
    // the fallback was unreachable and an unset variable produced NaN. A
    // `Number.isFinite` guard in `createWorld` rescued it, but only there —
    // `resolveConfig` on its own handed NaN to graphile.
    queueConcurrency:
      config.queueConcurrency ??
      (Number.parseInt(process.env.WORKFLOW_POSTGRES_WORKER_CONCURRENCY ?? "", 10) || 50),
    ...(config.streamFlushIntervalMs !== undefined
      ? { streamFlushIntervalMs: config.streamFlushIntervalMs }
      : {}),
  };
}

export function createWorld(
  config: EvelandWorldConfig = {} as EvelandWorldConfig,
): World & { start(): Promise<void> } {
  const resolved = resolveConfig(config);
  if (!Number.isFinite(resolved.queueConcurrency) || resolved.queueConcurrency <= 0) {
    resolved.queueConcurrency = 50;
  }

  const maxPoolSize = config.maxPoolSize ?? getDefaultMaxPoolSize();
  const pool =
    config.pool ||
    new Pool({
      connectionString: config.connectionString || resolveConnectionString(process.env),
      ...(maxPoolSize !== undefined ? { max: maxPoolSize } : {}),
    });

  let closed = false;
  const drizzle = createClient(pool);
  const queue = createQueue(resolved, pool);
  const storage = createStorage(drizzle, resolved.tenantId, resolved.queueNamespace);
  const streamer = createStreamer(pool, drizzle, resolved.tenantId);

  return {
    /**
     * eve compiles a `world.specVersion` check into every release, so this must
     * track the `@workflow/world` line the package depends on. Through eve
     * 0.33.1 (`@workflow/core` beta.40) that check is literal equality against
     * the runtime's own `SPEC_VERSION_CURRENT`; beta.41 widened it to the range
     * `[SPEC_VERSION_CURRENT, SPEC_VERSION_MAX_SUPPORTED]`. Beta.42 then raised
     * both the current version and the required floor to v6.
     *
     * New runs therefore use dense slot event ids. The per-run marker still
     * keeps pre-upgrade v5 runs on ULIDs for older Eve releases in the supported
     * rolling window. The contract test asserts the installed Eve agrees.
     */
    specVersion: SPEC_VERSION_CURRENT,
    /**
     * Declared so the runtime knows hook token retention is honoured here. A
     * World that stays silent is treated as not supporting it, which was the
     * previous state — and worse than that, the field was accepted and dropped.
     */
    capabilities: { hookRetention: { active: true } },
    ...storage,
    ...streamer,
    ...queue,
    ...(resolved.streamFlushIntervalMs !== undefined && {
      streamFlushIntervalMs: resolved.streamFlushIntervalMs,
    }),
    /**
     * Called by eve when a run is started with `deploymentId: 'latest'`.
     *
     * KNOWN LIMITATION. This returns the deployment this process *is*, not the
     * project's currently promoted one. For ordinary traffic they coincide,
     * because the gateway routes new sessions to the promoted deployment — but
     * they diverge in exactly the case this package introduces: a superseded
     * deployment woken by the dispatcher to finish a pinned run would start any
     * new `'latest'` run on itself rather than on the newest code.
     *
     * Resolving it properly needs promotion state, which lives in the control
     * plane rather than the workflow database, and reaching for it from inside
     * a tenant process would break the rule that agents and the platform
     * rendezvous only in Postgres. Left as-is deliberately, and recorded as the
     * one known limitation in `docs/design.md`.
     */
    async resolveLatestDeploymentId() {
      return resolved.deploymentId;
    },
    async start() {
      await queue.start();
      // Upstream calls `reenqueueActiveRuns`, which lists runs unfiltered and
      // would re-enqueue every project's active runs from any agent's boot.
      // This is the tenant-scoped equivalent, and it is the root fix for the
      // class of bug that per-project databases were papering over.
      await reenqueueTenantRuns({
        runs: storage.runs,
        enqueue: queue.queue,
        tenantId: resolved.tenantId,
      });
    },
    async close() {
      // Idempotent, and it has to be: a shutdown path and an error path can both
      // reach here, and a supervisor may signal twice. node-postgres is
      // unforgiving about the repeat — `pool.end()` throws "Called end on pool
      // more than once", and the streamer's LISTEN client throws "Client was
      // closed and is not queryable" — so a second close used to fail loudly
      // after having done its job correctly the first time.
      if (closed) return;
      closed = true;
      await streamer.close();
      await queue.close();
      // Only a pool this world created. A caller-supplied pool belongs to the
      // caller, who is very likely still serving that project's other traffic
      // through it.
      if (pool !== config.pool) {
        await pool.end();
      }
    },
  };
}
