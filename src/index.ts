import type { Storage, World } from "@workflow/world";
import { resolveQueueNamespace } from "@workflow/world";
import { Pool } from "pg";
import {
  type EvelandWorldConfig,
  type ResolvedWorldConfig,
  resolveConnectionString,
  resolveMaxLiveEventFeeds,
  resolveStreamCompaction,
} from "./config.js";
import { createClient, type Drizzle } from "./drizzle/index.js";
import { createListener } from "./listener.js";
import { createQueue } from "./queue.js";
import {
  createRunEventsFeed,
  DEFAULT_MAX_LIVE_EVENT_FEEDS,
  type RunEventsFeed,
} from "./run-events.js";
import {
  worldSpecVersion,
  createEventsStorage,
  createHooksStorage,
  createRunsStorage,
  createStepsStorage,
} from "./storage.js";
import { createStreamer } from "./streamer.js";
import { assertValidTenantId } from "./tenant.js";

export type { EvelandWorldConfig } from "./config.js";
export * from "./dispatch-contract.js";
export { MessageData } from "./message.js";
export {
  dropTenantPartitions,
  ensureTenantPartitions,
  resolveMigrationsDir,
  runMigrations,
  tenantPartitionsExist,
} from "./migrate.js";
export { inflightDeliveries } from "./inflight.js";
export { liveEventFeeds, stopLiveEventFeeds } from "./run-events.js";
export {
  reconcileWorkflowRuns,
  type ReconcileWorkflowRunsOptions,
  type ReconcileWorkflowRunsResult,
  type ReconciledWorkflowRun,
  type WorkflowRunReconciliationDisposition,
} from "./reconciliation.js";
export {
  packTerminalStreamBlocks,
  type PackTerminalStreamBlocksOptions,
  type PackTerminalStreamBlocksResult,
} from "./stream-block-maintenance.js";
export {
  pruneExpiredStreamChunks,
  pruneExpiredWorkflowRuns,
  pruneTerminalStreamChunks,
  setWorkflowRunRetentionClass,
  type DeadlineRetentionOptions,
  type SetWorkflowRunRetentionClassOptions,
  type StreamRetentionOptions,
  type StreamRetentionResult,
  type WorkflowRunRetentionResult,
} from "./retention.js";
export { RUN_RETENTION_ATTRIBUTE, type RunRetentionClass } from "./run-retention-policy.js";
export { derivePartitionName, tenantEventsChannel, tenantStreamChannel } from "./tenant.js";
export * from "./drizzle/schema.js";

/**
 * `queueNamespace` reaches storage because run creation is the only moment the
 * value is knowable and durable at once. The external dispatcher rebuilds
 * messages for active runs from their rows, in a process that has no access to
 * this deployment's environment — so a namespace not recorded here is a
 * namespace that boot recovery cannot honour.
 */
function createStorage(
  drizzle: Drizzle,
  tenantId: string,
  queueNamespace: string | undefined,
  specVersion: number,
  feed: RunEventsFeed,
): Storage {
  return {
    runs: createRunsStorage(drizzle, tenantId),
    events: createEventsStorage(drizzle, tenantId, queueNamespace, specVersion, feed),
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
  const tenantId = required(
    config.tenantId ?? process.env.WORKFLOW_WORLD_TENANT_ID,
    "WORKFLOW_WORLD_TENANT_ID",
  );
  assertValidTenantId(tenantId);
  const queueNamespace = resolveQueueNamespace(config.queueNamespace);
  return {
    tenantId,
    deploymentId: required(
      config.deploymentId ?? process.env.WORKFLOW_WORLD_DEPLOYMENT_ID,
      "WORKFLOW_WORLD_DEPLOYMENT_ID",
    ),
    // `resolveQueueNamespace` is upstream's own resolver, so the fallback to
    // `WORKFLOW_QUEUE_NAMESPACE` matches exactly what eve's runtime does.
    ...(queueNamespace !== undefined ? { queueNamespace } : {}),
    ...(config.streamFlushIntervalMs !== undefined
      ? { streamFlushIntervalMs: config.streamFlushIntervalMs }
      : {}),
    compactStreamSnapshots:
      config.compactStreamSnapshots ??
      resolveStreamCompaction(process.env.WORKFLOW_WORLD_STREAM_COMPACTION),
    maxLiveEventFeeds:
      config.maxLiveEventFeeds !== undefined
        ? resolveMaxLiveEventFeeds(String(config.maxLiveEventFeeds), DEFAULT_MAX_LIVE_EVENT_FEEDS)
        : resolveMaxLiveEventFeeds(
            process.env.WORKFLOW_WORLD_MAX_LIVE_EVENT_FEEDS,
            DEFAULT_MAX_LIVE_EVENT_FEEDS,
          ),
  };
}

export function createWorld(
  config: EvelandWorldConfig = {} as EvelandWorldConfig,
): World & { start(): Promise<void> } {
  const resolved = resolveConfig(config);
  // Before anything opens a connection; see the note on `specVersion` below.
  const specVersion = worldSpecVersion();

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
  // The World's one LISTEN connection, shared by stream readers and the live
  // event feed: a feed costs no connection of its own.
  const listener = createListener(pool);
  const feed = createRunEventsFeed(listener, {
    tenantId: resolved.tenantId,
    maxLiveSubscriptions: resolved.maxLiveEventFeeds,
  });
  const storage = createStorage(
    drizzle,
    resolved.tenantId,
    resolved.queueNamespace,
    specVersion,
    feed,
  );
  const streamer = createStreamer(pool, drizzle, resolved.tenantId, listener, {
    compactSnapshots: resolved.compactStreamSnapshots,
  });

  return {
    /**
     * Always the single-orchestrator version (9), the one eve 1.0 asks for
     * (`WORKFLOW_SPEC_VERSION=9`); any other requested version is refused at
     * construction (`worldSpecVersion`). The runtime stamps every run it
     * creates with what is declared here (`run_created` carries
     * `world.specVersion`).
     *
     * 9 needs the in-band writer fence (`capabilities.inBandFence` below) and
     * a queue that delivers a run's orchestrator messages one at a time
     * (`capabilities.maxConcurrency`). It includes every reader contract
     * below it: the hook force-claim disposal (8) that lets another run take
     * a token over, and the sealed log (7), whose only obligation is the
     * `noop` filler a backend emits when it pre-assigns event positions. This
     * World allocates each position inside the INSERT that occupies it
     * (`insertEventRow`), so it never has a hole to seal.
     *
     * Read once per `createWorld()`, and the same value is handed to storage
     * so its run-creation fallback agrees with what the runtime was told.
     */
    specVersion,
    capabilities: {
      /**
       * Declared so the runtime knows hook token retention is honoured here. A
       * World that stays silent is treated as not supporting it, which was the
       * previous state — and worse than that, the field was accepted and dropped.
       */
      hookRetention: { active: true },
      /**
       * `start(workflow, args, { experimental_hook })` admits the run together
       * with a Hook on the given token, in one transaction (storage.ts and
       * start-hook.ts): one immutable decision per candidate run, a duplicate
       * start refused with `HookConflictError` naming the owner, and the start
       * Hook resumable from the moment `run_created` returns. eve
       * 1.0 starts every session this way, claiming its channel address,
       * and refuses to start one on a World that does not declare this.
       */
      atomicStartHook: { active: true },
      /**
       * `createHook({ experimental_force: true })` takes a token over from the
       * run that holds it instead of answering `hook_conflict`
       * (storage.ts, hook_created): the victim's `hook_disposed{forceClaimedBy}`,
       * the re-pointed token with `Hook.claimedFrom`, and the claimer's
       * `hook_created{forceClaimedFrom}` commit in one transaction, and a
       * delivery that resolved the victim is refused with
       * `HookForceClaimedError` so `resumeHook()` follows the token.
       *
       * eve moves a session to a fresh workflow run (compaction and
       * deployment handoffs) only when the World declares this AND
       * `hookRetention.active`; without it a session never leaves its first
       * run, and its event log grows until `WORKFLOW_MAX_EVENTS` fails the run.
       * Without the flag the runtime rejects `experimental_force` at
       * `createHook()` time. See docs/design.md.
       */
      hookForceClaim: true,
      /**
       * An Eveland Deployment is one immutable build under a real id, and the
       * dispatcher delivers every run to the deployment on its row. eve reads
       * this flag as "this World routes each run to its own
       * deployment": without it eve assumes one eve version runs at a time,
       * parks or cancels deliveries for runs another eve version created, and
       * ends a session whose owner run was created by another eve version
       * instead of letting that run hand the session over. The runtime's own
       * affinity guard comes with it, and stays a second line behind the
       * dispatcher's routing. See docs/design.md.
       */
      deploymentAffinity: true,
      /**
       * The in-band writer fence spec 9 runs need (storage.ts, above
       * `insertEventRow`): a per-run count of in-band positions beside the slot
       * counter, a `snapshot` on `events.list`, and an in-band write refused
       * with `InBandSupersededError` before it writes anything when its
       * expected count is stale. It keeps a run to one writer when two of its
       * orchestrator invocations overlap. Checked by upstream's own
       * conformance module (`storage.in-band-fence.test.ts`).
       */
      inBandFence: true,
      /**
       * The queue half of a spec 9 run's single writer: its orchestrator
       * messages wait in the run's graphile queue, one delivery at a time,
       * and none bypasses it, while its step messages and a long wait's timer
       * hops keep full parallelism (`runQueueFor` in `dispatch-contract.ts`).
       * The fence above is what makes an overlap the queue still lets through
       * (a delivery that outlived its dispatch timeout) safe.
       */
      maxConcurrency: true,
    },
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
    /**
     * Starts the queue's graphile utilities and nothing else. An agent boot is
     * not a recovery event: jobs are durable in Postgres and the dispatcher
     * owns recovery. A held delivery that dies with this process fails its
     * job and graphile retries it, and a dispatcher restart reclaims dead
     * worker locks itself.
     */
    async start() {
      await queue.start();
    },
    async close() {
      // Idempotent, and it has to be: a shutdown path and an error path can both
      // reach here, and a supervisor may signal twice. node-postgres is
      // unforgiving about the repeat — `pool.end()` throws "Called end on pool
      // more than once", and the streamer's LISTEN client throws "Client was
      // closed and is not queryable" — so an unguarded second close would fail
      // loudly after the first had done its job.
      if (closed) return;
      closed = true;
      // Ends this World's live feeds through `onError`, then the connection.
      await feed.close();
      await streamer.close();
      await listener.close();
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
