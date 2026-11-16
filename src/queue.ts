/**
 * Multi-tenant port of `@workflow/world-postgres`'s queue.
 *
 * The graphile mapping is upstream's: same `jobKey` (except a bypassing hook
 * wake's, see `placeInvoke`) and the same 49-attempt budget. What changes is
 * who claims the work. A deployment registers no runner at all: jobs go to one
 * shared job name that the dispatcher claims across tenants, tagged with a
 * tenant flag (`tenantFlag`, or `wakeFlag` for a hook wake that bypasses its
 * run's queue) so the dispatcher can apply per-tenant fairness caps via
 * graphile's `forbiddenFlags`. The dispatcher then POSTs each job back into
 * the deployment it belongs to, where `createQueueHandler` receives it.
 */
import { trackDelivery } from "./inflight.js";
import {
  MessageId,
  parseQueueName,
  type Queue,
  type WorkflowInvokePayload,
  WorkflowInvokePayloadSchema,
} from "@workflow/world";
import { createWorld } from "@workflow/world-local";
import { Logger, makeWorkerUtils, type WorkerUtils } from "graphile-worker";
import type { Pool } from "pg";
import { monotonicFactory } from "ulid";
import type { ResolvedWorldConfig } from "./config.js";
import { MessageData } from "./message.js";
import {
  checkDispatchVersion,
  DEPLOYMENT_HEADER,
  DISPATCH_VERSION_HEADER,
  FLOW_JOB_NAME,
  jobKeyModeFor,
  mayBypassRunQueue,
  runQueueFor,
  readRuntimeSecretFromEnv,
  runQueueName,
  RUNTIME_SECRET_HEADER,
  secretMatches,
  tenantFlag,
  wakeFlag,
} from "./dispatch-contract.js";
import { MIGRATION_LOCK_KEY } from "./migrate.js";
import { MAX_GRAPHILE_JOB_ATTEMPTS } from "./queue-policy.js";

function createGraphileLogger() {
  const isJsonMode = () => process.env.WORKFLOW_JSON_MODE === "1";
  const isVerbose = () => Boolean(process.env.DEBUG);

  return new Logger(() => (level: string, message: string, meta?: unknown) => {
    if (isJsonMode()) return;
    if ((level === "debug" || level === "info") && !isVerbose()) return;
    const pipe = level === "error" ? process.stderr : process.stdout;
    if (meta) {
      pipe.write(`[Graphile Worker] ${message} ${JSON.stringify(meta, null, 2)}\n`);
    } else {
      pipe.write(`[Graphile Worker] ${message}\n`);
    }
  });
}

const graphileLogger = createGraphileLogger();

/**
 * JSON with `Uint8Array` values preserved through a tagged envelope
 * (`{ __type: 'Uint8Array', data: '<base64>' }`), the encoding eve's queue
 * handler decodes: the resilient start path sends `runInput.input`, a
 * `Uint8Array`, through the queue.
 */
function serializeMessage(value: unknown): Buffer {
  return Buffer.from(
    JSON.stringify(value, (_key, v) =>
      v instanceof Uint8Array
        ? { __type: "Uint8Array", data: Buffer.from(v).toString("base64") }
        : v,
    ),
  );
}

/**
 * The deployment half of the queue. Sending writes a graphile job; the
 * dispatcher claims it and POSTs it back to the deployment, where
 * `createQueueHandler` receives it.
 */
export type PostgresQueue = Queue & {
  start(): Promise<void>;
  close(): Promise<void>;
};

/**
 * `'latest'` is upstream's "whatever is current" alias, which this World
 * resolves to itself (`resolveLatestDeploymentId`), and an empty id addresses
 * nothing; neither names a deployment the dispatcher could route to.
 */
function isExactDeploymentId(deploymentId: string | undefined): deploymentId is string {
  return deploymentId !== undefined && deploymentId.length > 0 && deploymentId !== "latest";
}

export function createQueue(config: ResolvedWorldConfig, pool: Pool): PostgresQueue {
  const tenantId = config.tenantId;
  // Only for its queue handler. `'read'` so this World never writes a queue
  // auth key file or a Local data directory: it declares `queueAuthMode:
  // 'skip'` below, so nothing ever checks that key.
  const localWorld = createWorld({ dataDir: undefined, queueAuthKey: "read" });
  const generateMessageId = monotonicFactory();
  // Resolved once. `undefined` yields upstream's default `__wkf_workflow_`.
  const queueNamespace = config.queueNamespace;

  /**
   * The executor half stays eve's — `@workflow/world-local` owns vqs decoding
   * and hands the message to the runtime. It is wrapped only to enforce the
   * dispatch contract, because this handler is what the platform dispatcher
   * POSTs into and eve's own handler validates nothing beyond header presence.
   */
  const createQueueHandler: Queue["createQueueHandler"] = (prefix, handler) => {
    const inner = localWorld.createQueueHandler(prefix, handler);
    return async (req: Request) => {
      // Every delivery comes from the dispatcher, so every request must carry
      // the dispatch version, the runtime secret and its target deployment.
      // eve's own handler authenticates nothing, and the runtime's check is off
      // (`queueAuthMode: 'skip'`), so this is the route's only gate.
      const rejection = checkDispatchVersion(req.headers.get(DISPATCH_VERSION_HEADER));
      if (rejection) {
        return Response.json({ error: rejection.error }, { status: rejection.status });
      }
      const expected = readRuntimeSecretFromEnv(process.env);
      if (!expected || !secretMatches(expected, req.headers.get(RUNTIME_SECRET_HEADER))) {
        return Response.json({ error: "Unauthorized workflow dispatch." }, { status: 401 });
      }
      // The runtime secret is shared platform-wide, so on its own it does not
      // say *which* deployment a dispatch was meant for. Binding to the target
      // means a captured request cannot be replayed at another deployment on
      // the same host.
      if (req.headers.get(DEPLOYMENT_HEADER) !== config.deploymentId) {
        return Response.json(
          { error: "Workflow dispatch was addressed to a different deployment." },
          { status: 401 },
        );
      }
      // Counted only past the checks above: a rejected request is not a step
      // this process is running, and must not hold up a drain.
      return trackDelivery(() => inner(req));
    };
  };

  /**
   * The real deployment id, not upstream's constant `'postgres'`. Runs record
   * it, and it is what pins an in-flight run to the deployment that can still
   * execute it.
   */
  const getDeploymentId: Queue["getDeploymentId"] = async () => {
    return config.deploymentId;
  };

  let workerUtils: WorkerUtils | null = null;
  let closing = false;
  let startPromise: Promise<void> | null = null;

  /**
   * A hook wake passes its run's queue only while a delivery holds it; on an idle
   * run it queues as usual. Decided once, at send time.
   *
   * Reads graphile 0.16's private queue table, as boot recovery does. A newer
   * graphile schema may rename it, so a failed read queues the wake as usual:
   * every inbox message is a hook wake, and a throw would fail to publish them.
   */
  let warnedLockReadFailure = false;
  async function bypassesRunQueue(invoke: WorkflowInvokePayload): Promise<boolean> {
    // A spec 9 run's hook wake never bypasses: see `mayBypassRunQueue`.
    if (!mayBypassRunQueue(invoke)) return false;
    try {
      const { rows } = await pool.query(
        `select 1
           from graphile_worker._private_job_queues
          where queue_name = $1
            and locked_at is not null`,
        [runQueueName(tenantId, invoke.runId)],
      );
      return rows.length > 0;
    } catch (err) {
      // Once per queue: a lasting failure would otherwise log on every inbox message.
      if (!warnedLockReadFailure) {
        warnedLockReadFailure = true;
        console.warn(
          "[eveland workflow world] Could not read a run queue's lock; hook wakes queue as usual, so a cancel waits for a running step:",
          err,
        );
      }
      return false;
    }
  }

  /**
   * Where a sent message goes: into its run's queue, or past it as a bypassing
   * hook wake (`bypassesRunQueue`).
   *
   * At most one pending bypassing wake per run: graphile replaces a pending job
   * under the same key. A claimed one keeps running, but graphile spends its
   * retries, so if it then fails nothing retries it, no dead letter is written
   * and its failed row stays behind; the new job replays the same log. Safe because `resumeHook` commits
   * the input to the event log before it publishes the wake. A wake that carries
   * its input (`hookInput`) keeps a key of its own, and so does one with a
   * producer key: were this World to declare `hookResumeDedup`, core would key
   * every wake `hook-<resumeId>` and wakes would stop folding.
   */
  async function placeInvoke(
    message: unknown,
    messageId: MessageId,
    opts: { idempotencyKey?: string; delaySeconds?: number } | undefined,
  ): Promise<{
    jobKey: string;
    runQueue?: string;
    bypassingWake: boolean;
    jobKeyMode?: "unsafe_dedupe";
  }> {
    const idempotencyKey = opts?.idempotencyKey;
    const runQueue = runQueueFor(tenantId, message);
    const jobKeyMode = jobKeyModeFor(message, idempotencyKey);
    const placed = {
      jobKey: idempotencyKey ?? messageId,
      ...(runQueue ? { runQueue } : {}),
      ...(jobKeyMode ? { jobKeyMode } : {}),
    };
    const invoke = WorkflowInvokePayloadSchema.safeParse(message);
    if (!invoke.success) return { ...placed, bypassingWake: false };
    const { runId, hookInput } = invoke.data;
    // A delayed wake (core re-routing one) is not urgent, and under the run's
    // wake key it would push a pending cancel back by its delay.
    const delayed = (opts?.delaySeconds ?? 0) > 0;
    if (delayed || !(await bypassesRunQueue(invoke.data))) {
      return { ...placed, bypassingWake: false };
    }
    const wakeKey = hookInput === undefined ? `wfwake:${tenantId}:${runId}` : messageId;
    return { jobKey: idempotencyKey ?? wakeKey, bypassingWake: true };
  }

  async function addGraphileJob({
    queueId,
    body,
    messageId,
    idempotencyKey,
    headers,
    delaySeconds,
    jobKey,
    jobKeyMode,
    runQueue,
    deploymentId,
    bypassingWake = false,
  }: {
    queueId: string;
    body: Buffer;
    messageId: MessageId;
    idempotencyKey?: string;
    headers?: Record<string, string>;
    delaySeconds?: number;
    jobKey: string;
    /** Graphile's job-key mode; its default when absent (see `jobKeyModeFor`). */
    jobKeyMode?: "unsafe_dedupe";
    /** The run queue this job waits in (`runQueueFor`); absent when it waits in none. */
    runQueue?: string;
    /** The deployment this message is addressed to; this one when omitted. */
    deploymentId?: string;
    /** Counted against the tenant's wake cap rather than its delivery cap. */
    bypassingWake?: boolean;
  }) {
    const utils = workerUtils;
    if (!utils) {
      throw new Error("Postgres queue worker utils are not initialized");
    }

    const runAt =
      typeof delaySeconds === "number" && delaySeconds > 0
        ? new Date(Date.now() + delaySeconds * 1000)
        : undefined;

    const encoded = MessageData.encode({
      id: queueId,
      data: body,
      attempt: 1,
      messageId,
      idempotencyKey,
      headers,
      tenantId,
      deploymentId: deploymentId ?? config.deploymentId,
      // Recorded so the delivery side can rebuild the exact prefix; the
      // dispatcher must not re-resolve it from its own environment.
      ...(queueNamespace !== undefined ? { queueNamespace } : {}),
    });

    await utils.addJob(FLOW_JOB_NAME, encoded, {
      jobKey,
      ...(jobKeyMode ? { jobKeyMode } : {}),
      ...(runAt ? { runAt } : {}),
      // Serializes concurrent deliveries for one run. Without it two claimed
      // jobs replay the same run's event log at the same time.
      ...(runQueue ? { queueName: runQueue } : {}),
      maxAttempts: MAX_GRAPHILE_JOB_ATTEMPTS,
      // Read by the dispatcher's `forbiddenFlags` callback to throttle a
      // tenant that is already at its in-flight cap, without starving others.
      flags: [bypassingWake ? wakeFlag(tenantId) : tenantFlag(tenantId)],
    });
  }

  async function start(): Promise<void> {
    if (closing) {
      return;
    }

    startPromise ??= (async () => {
      try {
        workerUtils = await makeWorkerUtils({
          pgPool: pool,
          logger: graphileLogger,
        });
        // graphile's installSchema is not race-safe, and on a shared database
        // every agent boot would race every other. `bin/setup` normally has
        // run already, which makes this a no-op; the advisory lock covers the
        // case where it has not.
        const migrationClient = await pool.connect();
        try {
          await migrationClient.query("select pg_advisory_lock($1)", [MIGRATION_LOCK_KEY]);
          await workerUtils.migrate();
        } finally {
          await migrationClient
            .query("select pg_advisory_unlock($1)", [MIGRATION_LOCK_KEY])
            .catch(() => {});
          migrationClient.release();
        }
      } catch (err) {
        startPromise = null;
        throw err;
      }
    })();
    await startPromise;
  }

  const queue: Queue["queue"] = async (queue, message, opts) => {
    await start();
    // Only the sub-queue id is stored; the delivery side rebuilds the prefix.
    const { id: queueId } = parseQueueName(queue);
    const messageId = MessageId.parse(`msg_${generateMessageId()}`);
    await addGraphileJob({
      queueId,
      body: serializeMessage(message),
      messageId,
      idempotencyKey: opts?.idempotencyKey,
      headers: opts?.headers,
      delaySeconds: opts?.delaySeconds,
      ...(await placeInvoke(message, messageId, opts)),
      // A producer that names a deployment means it: `start()` with an explicit
      // `deploymentId` (eve's deployment handoff) and the capability probe
      // before it address ANOTHER deployment. `start()` writes `run_created`
      // and this message in parallel, and until that row exists the dispatcher
      // routes on this hint, so stamping the enqueuer here would hand the new
      // run's first invocation to the deployment it is moving away from. Once
      // the row exists it is authoritative and the hint is not consulted.
      ...(isExactDeploymentId(opts?.deploymentId) ? { deploymentId: opts.deploymentId } : {}),
    });
    return { messageId };
  };

  return {
    /**
     * The runtime's own flow-route check (a queue auth key shared through a
     * Local data directory) does not apply: every delivery comes from the
     * dispatcher and proves itself with the runtime secret and the target
     * deployment in `createQueueHandler`, before eve's handler runs.
     */
    queueAuthMode: "skip",
    createQueueHandler,
    getDeploymentId,
    queue,
    start,
    async close() {
      closing = true;
      await startPromise?.catch(() => {});
      if (workerUtils) {
        await workerUtils.release();
        workerUtils = null;
      }
      startPromise = null;
      await localWorld.close?.();
    },
  };
}
