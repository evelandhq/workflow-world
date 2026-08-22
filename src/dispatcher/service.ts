import { randomUUID } from "node:crypto";
import { makeWorkerUtils, type WorkerUtils } from "graphile-worker";
import { Pool, type PoolClient } from "pg";
import { runMigrations } from "../migrate.js";
import { startStorageMaintenanceLoop } from "../storage-maintenance.js";
import { createActivationClient, type ActivationClient } from "./activation-client.js";
import { reclaimAndReenqueueActiveRunsForAllTenants } from "./boot-recovery.js";
import { resolveDispatcherConfig, type DispatcherConfiguration } from "./config.js";
import { consoleTelemetry, type DispatcherTelemetry } from "./observability.js";
import { startDispatcher, type DispatcherRuntime } from "./runner.js";
import { resolveDispatchRuntimeSecret, resolveSecretWithDevFallback } from "./secrets.js";

const DISPATCHER_OWNERSHIP_LOCK_KEY = 0x65_76_64_70; // "evdp"

/**
 * The whole service as a function, so the CLI is a three-line wrapper and a test
 * can start it without a process. What used to be `server.ts`'s top-level
 * statements — the platform's build info banner, the platform observability
 * singleton, the eveland env names — are now the host's business, injected here.
 */
export type DispatcherServiceOptions = {
  env?: NodeJS.ProcessEnv;
  config?: Partial<DispatcherConfiguration>;
  telemetry?: DispatcherTelemetry;
  /** Overridable so a test can drive the loop without a control API. */
  activation?: ActivationClient;
  /**
   * Machine-readable lifecycle callbacks, in order:
   * `ownership_acquired → migrations_applied → boot_recovery_completed →
   * ready → stopped`. A supervisor gates on these — never on stdout text,
   * which proves only that the process printed something.
   */
  lifecycle?: {
    onPhase?: (event: DispatcherLifecycleEvent) => void;
  };
  /**
   * Host preflight, run after ownership and migrations but before boot
   * recovery — e.g. to read the World's schema generation and cluster identity
   * for the host's registration. Throwing aborts startup with ownership
   * released and nothing re-enqueued.
   */
  beforeBootRecovery?: (context: { pool: Pool }) => Promise<void>;
};

export type DispatcherLifecyclePhase =
  | "ownership_acquired"
  | "migrations_applied"
  | "boot_recovery_completed"
  | "ready"
  | "stopped";

export type DispatcherLifecycleEvent = {
  phase: DispatcherLifecyclePhase;
  at: Date;
  attributes?: Record<string, string | number | boolean>;
};

export type DispatcherServicePhase = "ready" | "stopped";

export type DispatcherService = {
  runtime?: DispatcherRuntime;
  config: DispatcherConfiguration;
  /** Current lifecycle state; `ready` is the only state that claims jobs. */
  readonly phase: DispatcherServicePhase;
  stop(): Promise<void>;
};

export async function startDispatcherService(
  options: DispatcherServiceOptions = {},
): Promise<DispatcherService> {
  const env = options.env ?? process.env;
  const telemetry = options.telemetry ?? consoleTelemetry;
  const config = { ...resolveDispatcherConfig(env), ...options.config };

  const runtimeSecret = resolveDispatchRuntimeSecret(env);
  if (!runtimeSecret) {
    throw new Error(
      "WORKFLOW_WORLD_RUNTIME_SECRET is required unless NODE_ENV is explicitly development.",
    );
  }

  const activation =
    options.activation ??
    createActivationClient({
      apiUrl: config.apiUrl,
      serviceToken: requiredServiceToken(env),
    });

  const pool = new Pool({
    connectionString: config.worldUrl,
    max: config.poolSize,
    application_name: `workflow-dispatcher-${randomUUID().slice(0, 8)}`,
  });

  // Session-scoped and held on a checked-out client until shutdown. This must
  // precede migrations and recovery: once a generation reaches either, no
  // other participating dispatcher may still be working against this database.
  let ownershipClient: PoolClient;
  try {
    ownershipClient = await pool.connect();
  } catch (error) {
    await pool.end().catch(() => {});
    throw error;
  }
  let ownership;
  try {
    ownership = await ownershipClient.query<{ locked: boolean }>(
      "select pg_try_advisory_lock($1) as locked",
      [DISPATCHER_OWNERSHIP_LOCK_KEY],
    );
  } catch (error) {
    ownershipClient.release();
    await pool.end().catch(() => {});
    throw error;
  }
  if (ownership.rows[0]?.locked !== true) {
    ownershipClient.release();
    await pool.end().catch(() => {});
    throw new Error("Another workflow dispatcher already owns this database.");
  }

  const emitPhase = (
    phase: DispatcherLifecyclePhase,
    attributes?: Record<string, string | number | boolean>,
  ) => {
    options.lifecycle?.onPhase?.({ phase, at: new Date(), ...(attributes ? { attributes } : {}) });
  };
  emitPhase("ownership_acquired");

  let ownershipReleased = false;
  const releaseOwnership = async () => {
    if (ownershipReleased) return;
    ownershipReleased = true;
    await ownershipClient
      .query("select pg_advisory_unlock($1)", [DISPATCHER_OWNERSHIP_LOCK_KEY])
      .catch(() => {});
    ownershipClient.release();
  };

  let workerUtils: WorkerUtils | undefined;
  let runtime: DispatcherRuntime | undefined;
  try {
    await runMigrations(pool, {
      log: (message) =>
        telemetry.emit({
          severity: "info",
          eventName: "workflow_dispatcher.migrate",
          body: message,
        }),
    });
    emitPhase("migrations_applied");

    workerUtils = await makeWorkerUtils({ pgPool: pool });
    const startedWorkerUtils = workerUtils;

    // The host's preflight. Failing here aborts with ownership released and
    // boot recovery never run — nothing has been re-enqueued yet.
    if (options.beforeBootRecovery) {
      await options.beforeBootRecovery({ pool });
    }

    const reenqueuedRuns = await reclaimAndReenqueueActiveRunsForAllTenants({
      pool,
      workerUtils,
      log: (message, meta) =>
        telemetry.emit({
          severity: "info",
          eventName: "workflow_dispatcher.boot_recovery",
          body: message,
          attributes: (meta ?? {}) as Record<string, string | number | boolean>,
        }),
    });
    emitPhase("boot_recovery_completed", { reenqueuedRuns });

    let phase: DispatcherServicePhase | "starting" = "starting";
    let maintenance: { stop(): Promise<void> } | undefined;

    const startClaiming = async () => {
      const startedRuntime = await startDispatcher({
        pool,
        workerUtils: startedWorkerUtils,
        config: {
          concurrency: config.concurrency,
          pollIntervalMs: config.pollIntervalMs,
          maxInFlightPerTenant: config.maxInFlightPerTenant,
          queueGcIntervalMs: config.queueGcIntervalMs,
        },
        deps: {
          activation,
          runtimeSecret,
          dispatchTimeoutMs: config.dispatchTimeoutMs,
          leaseRenewIntervalMs: config.leaseRenewIntervalMs,
          activationLeaseTtlMs: config.activationLeaseTtlMs,
          log: (message, meta) =>
            telemetry.emit({
              severity: "info",
              eventName: "workflow_dispatcher.event",
              body: message,
              attributes: (meta ?? {}) as Record<string, string | number | boolean>,
            }),
        },
      });
      runtime = startedRuntime;
      service.runtime = startedRuntime;

      maintenance = startStorageMaintenanceLoop(pool, {
        intervalMs: config.maintenanceIntervalMs,
        maintenance: {
          streamBatchSize: config.maintenanceStreamBatchSize,
          maxBatches: config.maintenanceMaxBatches,
          maxStreamsToPack: config.maintenanceMaxStreamsToPack,
          runBatchSize: config.maintenanceRunBatchSize,
          compactSnapshots: config.maintenanceCompactSnapshots,
        },
        onResult: (result) => {
          for (const [role, outcome] of Object.entries(result)) {
            if (outcome.status === "rejected") {
              telemetry.emit({
                severity: "error",
                eventName: "workflow_dispatcher.storage_maintenance",
                body: `${role} maintenance failed`,
                attributes: { role, error: String(outcome.reason) },
              });
              continue;
            }
            telemetry.emit({
              severity: "info",
              eventName: "workflow_dispatcher.storage_maintenance",
              body: `${role} maintenance completed`,
              attributes: {
                role,
                ...numericAndBooleanAttributes(outcome.value),
              },
            });
          }
        },
        onError: (error) => {
          telemetry.emit({
            severity: "error",
            eventName: "workflow_dispatcher.storage_maintenance",
            body: "storage maintenance loop failed",
            attributes: { error: String(error) },
          });
        },
      });
      phase = "ready";
      emitPhase("ready");
    };

    const service: DispatcherService = {
      config,
      get phase() {
        // `starting` is unobservable: the service is only handed out once
        // `startClaiming` has moved it to `ready`.
        return phase === "starting" ? "ready" : phase;
      },
      async stop() {
        const errors: unknown[] = [];
        if (maintenance) await collectCleanupError(errors, () => maintenance!.stop());
        if (runtime) {
          const startedRuntime = runtime;
          await collectCleanupError(errors, () => startedRuntime.stop());
        } else {
          // No runner ever owned the worker utils, so release them here.
          await collectCleanupError(errors, () => Promise.resolve(startedWorkerUtils.release()));
        }
        await collectCleanupError(errors, releaseOwnership);
        await collectCleanupError(errors, () => pool.end());
        await collectCleanupError(errors, () => telemetry.shutdown());
        phase = "stopped";
        emitPhase("stopped");
        if (errors.length === 1) throw errors[0];
        if (errors.length > 1) {
          throw new AggregateError(errors, "Dispatcher shutdown failed.");
        }
      },
    };

    await startClaiming();
    return service;
  } catch (error) {
    if (runtime) {
      await runtime.stop().catch(() => {});
    } else if (workerUtils) {
      await Promise.resolve(workerUtils.release()).catch(() => {});
    }
    await releaseOwnership();
    await pool.end().catch(() => {});
    await telemetry.shutdown().catch(() => {});
    throw error;
  }
}

async function collectCleanupError(
  errors: unknown[],
  operation: () => void | Promise<void>,
): Promise<void> {
  try {
    await operation();
  } catch (error) {
    errors.push(error);
  }
}

function numericAndBooleanAttributes(value: unknown): Record<string, number | boolean> {
  if (typeof value !== "object" || value === null) return {};
  return Object.fromEntries(
    Object.entries(value).filter(
      (entry): entry is [string, number | boolean] =>
        typeof entry[1] === "number" || typeof entry[1] === "boolean",
    ),
  );
}

function requiredServiceToken(env: NodeJS.ProcessEnv): string {
  const token = resolveSecretWithDevFallback(
    env,
    env.WORKFLOW_DISPATCHER_ACTIVATION_TOKEN,
    "eveland-dev-gateway-token",
  );
  if (!token) {
    throw new Error(
      "WORKFLOW_DISPATCHER_ACTIVATION_TOKEN is required unless NODE_ENV is explicitly development.",
    );
  }
  return token;
}
