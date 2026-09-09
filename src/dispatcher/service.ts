import { randomUUID } from "node:crypto";
import { makeWorkerUtils, type WorkerUtils } from "graphile-worker";
import { Pool } from "pg";
import { runMigrations } from "../migrate.js";
import { startStorageMaintenanceLoop } from "../storage-maintenance.js";
import { createActivationClient, type ActivationClient } from "./activation-client.js";
import {
  reclaimAndReenqueueActiveRunsForAllTenants,
  type BootRecoveryRun,
} from "./boot-recovery.js";
import { resolveDispatcherConfig, type DispatcherConfiguration } from "./config.js";
import { consoleTelemetry, type DispatcherTelemetry } from "./observability.js";
import { acquireDispatcherOwnership, type DispatcherOwnership } from "./ownership.js";
import { startDispatcher, type DispatcherRuntime } from "./runner.js";
import { resolveDispatchRuntimeSecret, resolveSecretWithDevFallback } from "./secrets.js";

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
   *
   * `ownership_lost` is out of band: the session holding the ownership lock
   * died under a running dispatcher, which then stops itself and reports
   * `stopped`. The host should treat it as a crash and let its supervisor
   * start a fresh process, which acquires the lock anew.
   */
  lifecycle?: {
    onPhase?: (event: DispatcherLifecycleEvent) => void;
  };
  /**
   * Host preflight, run after ownership and migrations but before boot
   * recovery — e.g. to read the World's schema generation and cluster identity
   * for the host's registration, or to settle orphaned runs through
   * `reconcileWorkflowRuns` so the sweep never sees them. Throwing aborts
   * startup with ownership released and nothing re-enqueued.
   */
  beforeBootRecovery?: (context: { pool: Pool }) => Promise<void>;
  /**
   * Host filter over boot recovery's candidates, called once with the full
   * list; only the runs returned are re-enqueued. This is for runs the host
   * knows cannot be replayed right now — bound to a Deployment that is not
   * activatable — without forcing it to settle them. A skipped run stays
   * active and is offered again on the next boot.
   */
  filterBootRecoveryRuns?: (
    runs: BootRecoveryRun[],
  ) => Promise<BootRecoveryRun[]> | BootRecoveryRun[];
};

export type DispatcherLifecyclePhase =
  | "ownership_acquired"
  | "migrations_applied"
  | "boot_recovery_completed"
  | "ready"
  | "ownership_lost"
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
    // Client-side keepalives, so this process notices a vanished server the
    // way the server is made to notice a vanished dispatcher (ownership.ts).
    keepAlive: true,
  });

  const emitPhase = (
    phase: DispatcherLifecyclePhase,
    attributes?: Record<string, string | number | boolean>,
  ) => {
    options.lifecycle?.onPhase?.({ phase, at: new Date(), ...(attributes ? { attributes } : {}) });
  };

  // Session-scoped and held on a checked-out client until shutdown. This must
  // precede migrations and recovery: once a generation reaches either, no
  // other participating dispatcher may still be working against this database.
  //
  // Losing it later is handled below: the heartbeat on the owning session
  // reports through `onLost`, and whatever this service has started by then is
  // stopped — a dispatcher without the lock must not claim.
  let lostError: unknown;
  let stopOnLoss: (() => Promise<void>) | undefined;
  let ownership: DispatcherOwnership;
  try {
    ownership = await acquireDispatcherOwnership(pool, {
      telemetry,
      livenessMs: config.ownershipLivenessMs,
      retryIntervalMs: config.ownershipRetryIntervalMs,
      waitMs: config.ownershipWaitMs,
      onLost: (error) => {
        lostError = error;
        emitPhase("ownership_lost", { error: String(error) });
        if (stopOnLoss) {
          void stopOnLoss().catch((stopError: unknown) => {
            telemetry.emit({
              severity: "error",
              eventName: "workflow_dispatcher.shutdown_failed",
              body: `stopping after ownership loss failed: ${String(stopError)}`,
            });
          });
        }
      },
    });
  } catch (error) {
    await pool.end().catch(() => {});
    throw error;
  }
  emitPhase("ownership_acquired");

  const releaseOwnership = () => ownership.release();

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

    // Counted here rather than returned by the sweep so its public return type
    // stays a plain count; the wrapper also guards against a filter that
    // returns entries the sweep never offered.
    let filteredRuns = 0;
    const hostFilter = options.filterBootRecoveryRuns;
    const reenqueuedRuns = await reclaimAndReenqueueActiveRunsForAllTenants({
      pool,
      workerUtils,
      ...(hostFilter
        ? {
            filterRuns: async (runs: BootRecoveryRun[]) => {
              const kept = await hostFilter(runs);
              filteredRuns = Math.max(0, runs.length - kept.length);
              return kept;
            },
          }
        : {}),
      log: (message, meta) =>
        telemetry.emit({
          severity: "info",
          eventName: "workflow_dispatcher.boot_recovery",
          body: message,
          attributes: (meta ?? {}) as Record<string, string | number | boolean>,
        }),
    });
    emitPhase("boot_recovery_completed", {
      reenqueuedRuns,
      ...(filteredRuns > 0 ? { filteredRuns } : {}),
    });

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
          executorFailureLimit: config.executorFailureLimit,
          executorFailureMinSpanMs: config.executorFailureMinSpanMs,
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
      stop() {
        // Idempotent: a signal and an ownership loss can both ask for it.
        stopping ??= stopEverything();
        return stopping;
      },
    };

    let stopping: Promise<void> | undefined;
    const stopEverything = async () => {
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
    };

    await startClaiming();
    // From here a loss stops the running service; a loss that landed while we
    // were still starting is a failed start, not a service that stops later.
    stopOnLoss = () => service.stop();
    if (lostError !== undefined) {
      await service.stop().catch(() => {});
      throw lostError;
    }
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
