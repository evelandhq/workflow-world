import { createBuildInfoFromEnv, formatBuildInfo } from "./build-info.js";
import { consoleTelemetry, type DispatcherTelemetry } from "./observability.js";
import { startDispatcherService } from "./service.js";

/**
 * The readiness token, printed on stdout once the dispatcher is claiming jobs.
 *
 * A supervisor needs *something* to gate on and this process binds no port, so
 * the line is the contract. It is a stable literal on purpose: tests and systemd
 * both match it, and dressing it up later would break them silently.
 */
export const DISPATCHER_READY_TOKEN = "workflow-dispatcher: ready";

/** Exit code for a shutdown that did not finish inside the grace window. */
const SHUTDOWN_TIMEOUT_EXIT_CODE = 75;

/**
 * How long a shutdown may take before the process gives up and exits anyway.
 *
 * A held dispatch can legitimately run for the whole `dispatchTimeoutMs` (15
 * minutes by default), and a supervisor is not going to wait that long. This is
 * the point at which we stop being graceful — the run stays claimed until
 * graphile's own lock expires and boot recovery picks it up, which is the
 * designed failure path.
 */
const SHUTDOWN_GRACE_MS = 30_000;

/** The `workflow-dispatcher` bin. Everything real lives in `service.ts`. */
export async function main(
  env: NodeJS.ProcessEnv = process.env,
  telemetry: DispatcherTelemetry = consoleTelemetry,
): Promise<void> {
  const service = await startDispatcherService({ env, telemetry });

  telemetry.emit({
    severity: "info",
    eventName: "workflow_dispatcher.ready",
    body: `${formatBuildInfo(createBuildInfoFromEnv(env))} dispatching workflow jobs`,
    attributes: {
      "dispatcher.concurrency": service.config.concurrency,
      "dispatcher.pool_size": service.config.poolSize,
      "dispatcher.max_in_flight_per_tenant": service.config.maxInFlightPerTenant,
    },
  });
  // Deliberately not routed through the telemetry sink: the host may swap that
  // for one that ships elsewhere, and the readiness signal has to stay on stdout.
  console.log(DISPATCHER_READY_TOKEN);

  // graphile's own signal handlers are disabled (`noHandleSignals`), so these are
  // the only ones. Guarded against a second signal: a supervisor that sends
  // SIGTERM twice should not start two concurrent shutdowns.
  let shuttingDown = false;
  const shutdown = (signal: string) => {
    if (shuttingDown) return;
    shuttingDown = true;
    telemetry.emit({
      severity: "info",
      eventName: "workflow_dispatcher.shutdown",
      body: `received ${signal}; draining in-flight dispatches`,
    });

    const timer = setTimeout(() => {
      telemetry.emit({
        severity: "warn",
        eventName: "workflow_dispatcher.shutdown_timeout",
        body: `shutdown did not finish within ${String(SHUTDOWN_GRACE_MS)}ms; exiting anyway`,
      });
      process.exit(SHUTDOWN_TIMEOUT_EXIT_CODE);
    }, SHUTDOWN_GRACE_MS);
    timer.unref();

    void service
      .stop()
      .then(
        () => 0,
        (error: unknown) => {
          telemetry.emit({
            severity: "error",
            eventName: "workflow_dispatcher.shutdown_failed",
            body: String(error),
          });
          return 1;
        },
      )
      .then(async (code) => {
        await telemetry.shutdown().catch(() => {});
        clearTimeout(timer);
        process.exit(code);
      });
  };

  process.once("SIGINT", () => shutdown("SIGINT"));
  process.once("SIGTERM", () => shutdown("SIGTERM"));
}
