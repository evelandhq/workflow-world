/**
 * Multi-tenant port of `@workflow/world-postgres`'s queue.
 *
 * The graphile mapping is upstream's: same `jobKey` and the same 49-attempt
 * budget. What changes is who claims the work. A deployment registers no
 * runner at all: jobs go to one shared job name that the dispatcher claims
 * across tenants, tagged with a tenant flag (`tenantFlag`) so the dispatcher
 * can apply per-tenant fairness caps via graphile's `forbiddenFlags`. The dispatcher then POSTs each job back into
 * the deployment it belongs to, where `createQueueHandler` receives it.
 */
import { runDelivery } from "./delivery-context.js";
import { trackDelivery } from "./inflight.js";
import { MessageId, parseQueueName, type Queue } from "@workflow/world";
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
  LIVE_FEED_HEADER,
  messageRunId,
  readRuntimeSecretFromEnv,
  RUNTIME_SECRET_HEADER,
  secretMatches,
  tenantFlag,
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
      // this process is running, and must not hold up a drain. Run inside a
      // delivery context, which is what `events.subscribe` reads: a live event
      // feed only where the dispatcher granted one, since with a feed the
      // runtime keeps the delivery (one of the dispatcher's slots) open after
      // its work is done.
      const liveFeed = req.headers.get(LIVE_FEED_HEADER) === "1";
      return trackDelivery(() => runDelivery({ liveFeed }, () => inner(req)));
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
   * The job key a sent message is stored under (its producer's key or its own
   * message id), and the run it belongs to, which rides on the payload
   * (`MessageData.runId`) so boot recovery can find the run's jobs in SQL.
   * Whether it is an orchestrator delivery, and so takes its run's gate in the
   * dispatcher, is read from the body there (`orchestratorDeliveryRunId`).
   */
  function placeMessage(
    message: unknown,
    messageId: MessageId,
    idempotencyKey: string | undefined,
  ): { jobKey: string; runId?: string; jobKeyMode?: "unsafe_dedupe" } {
    // A health check may name the run it prepares, but it drives nothing.
    const healthCheck =
      (message as { __healthCheck?: unknown } | null | undefined)?.__healthCheck === true;
    const runId = healthCheck ? undefined : messageRunId(message);
    const jobKeyMode = jobKeyModeFor(message, idempotencyKey);
    return {
      jobKey: idempotencyKey ?? messageId,
      ...(runId ? { runId } : {}),
      ...(jobKeyMode ? { jobKeyMode } : {}),
    };
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
    runId,
    deploymentId,
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
    /** The run the message belongs to; absent when it names none. */
    runId?: string;
    /** The deployment this message is addressed to; this one when omitted. */
    deploymentId?: string;
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
      ...(runId !== undefined ? { runId } : {}),
    });

    await utils.addJob(FLOW_JOB_NAME, encoded, {
      jobKey,
      ...(jobKeyMode ? { jobKeyMode } : {}),
      ...(runAt ? { runAt } : {}),
      // No queue name. A run's orchestrator deliveries are serialized by the
      // dispatcher's run gate (`dispatcher/run-gate.ts`): a graphile named
      // queue per run cost every claim one row lock per run that had any job,
      // due or not, and graphile never deletes queue rows.
      maxAttempts: MAX_GRAPHILE_JOB_ATTEMPTS,
      // Read by the dispatcher's `forbiddenFlags` callback to throttle a
      // tenant that is already at its in-flight cap, without starving others.
      flags: [tenantFlag(tenantId)],
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
      ...placeMessage(message, messageId, opts?.idempotencyKey),
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
