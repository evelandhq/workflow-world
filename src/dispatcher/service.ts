import { randomUUID } from "node:crypto";
import { Pool } from "pg";
import { runMigrations } from "../migrate.js";
import { createActivationClient, type ActivationClient } from "./activation-client.js";
import { reenqueueActiveRunsForAllTenants } from "./boot-recovery.js";
import { resolveDispatcherConfig, type DispatcherConfiguration } from "./config.js";
import { consoleTelemetry, type DispatcherTelemetry } from "./observability.js";
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
};

export type DispatcherService = {
  runtime: DispatcherRuntime;
  config: DispatcherConfiguration;
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

  await runMigrations(pool, {
    log: (message) =>
      telemetry.emit({
        severity: "info",
        eventName: "workflow_dispatcher.migrate",
        body: message,
      }),
  });

  const runtime = await startDispatcher({
    pool,
    config: {
      concurrency: config.concurrency,
      pollIntervalMs: config.pollIntervalMs,
      maxInFlightPerTenant: config.maxInFlightPerTenant,
    },
    deps: {
      activation,
      runtimeSecret,
      dispatchTimeoutMs: config.dispatchTimeoutMs,
      leaseRenewIntervalMs: config.leaseRenewIntervalMs,
      log: (message, meta) =>
        telemetry.emit({
          severity: "info",
          eventName: "workflow_dispatcher.event",
          body: message,
          attributes: (meta ?? {}) as Record<string, string | number | boolean>,
        }),
    },
  });

  await reenqueueActiveRunsForAllTenants({
    pool,
    workerUtils: runtime.workerUtils,
    log: (message, meta) =>
      telemetry.emit({
        severity: "info",
        eventName: "workflow_dispatcher.boot_recovery",
        body: message,
        attributes: (meta ?? {}) as Record<string, string | number | boolean>,
      }),
  });

  return {
    runtime,
    config,
    async stop() {
      await runtime.stop();
      await pool.end();
      await telemetry.shutdown();
    },
  };
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
