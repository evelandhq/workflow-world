/**
 * The one place the conformance harness's environment is defined.
 *
 * Everything downstream — globalSetup, the stub API, the dispatcher boot, and
 * every spawned executor — inherits from `process.env`, so these have to be set
 * before vitest loads anything. `vitest.config.mts` imports this for its side
 * effect, which is the earliest hook available.
 */

/** Pinned so the stub activation API can hand the dispatcher a constant target. */
export const EXECUTOR_PORT = Number(process.env.WORKFLOW_WORLD_CONFORMANCE_EXECUTOR_PORT ?? 41777);

/** The stub control plane. Separate port; nothing else binds it. */
export const STUB_API_PORT = Number(process.env.WORKFLOW_WORLD_CONFORMANCE_STUB_PORT ?? 41778);

export const TENANT_ID = "prj_conformance";
export const DEPLOYMENT_ID = "dep_conformance_1";
export const RUNTIME_SECRET = "conformance-runtime-secret";
export const ACTIVATION_TOKEN = "conformance-activation-token";

export const PACKAGE_NAME = "@evelandhq/workflow-world";

export function resolveConformanceDatabaseUrl(): string {
  const url = process.env.WORKFLOW_WORLD_CONFORMANCE_URL;
  if (!url) {
    throw new Error(
      "WORKFLOW_WORLD_CONFORMANCE_URL is required: the conformance harness needs a Postgres it may migrate and write to.",
    );
  }
  return url;
}

/**
 * Applied to `process.env` at config load.
 *
 * `PORT` is the load-bearing one: `world-testing`'s spawned server binds
 * `Number(process.env.PORT) || 0`, and the harness never passes env of its own,
 * so this is the only way to pin it.
 */
export function applyConformanceEnv(): void {
  const url = resolveConformanceDatabaseUrl();
  Object.assign(process.env, {
    PORT: String(EXECUTOR_PORT),

    // Written back, not just read: the stub API and the dispatcher boot are
    // separate processes that inherit `process.env`, and they need the resolved
    // values rather than re-deriving the defaults.
    WORKFLOW_WORLD_CONFORMANCE_EXECUTOR_PORT: String(EXECUTOR_PORT),
    WORKFLOW_WORLD_CONFORMANCE_STUB_PORT: String(STUB_API_PORT),

    // Deployment side — read by the World inside each spawned executor.
    WORKFLOW_WORLD_URL: url,
    WORKFLOW_WORLD_TENANT_ID: TENANT_ID,
    WORKFLOW_WORLD_DEPLOYMENT_ID: DEPLOYMENT_ID,
    WORKFLOW_WORLD_RUNNER: "external",
    WORKFLOW_WORLD_RUNTIME_SECRET: RUNTIME_SECRET,

    // Host side — read by the dispatcher.
    WORKFLOW_DISPATCHER_ACTIVATION_API_URL: `http://127.0.0.1:${String(STUB_API_PORT)}`,
    WORKFLOW_DISPATCHER_ACTIVATION_TOKEN: ACTIVATION_TOKEN,
    // Poll fast: the suite's assertions wait on run status, and the default
    // 500ms adds up over 12 tests for no benefit here.
    WORKFLOW_DISPATCHER_POLL_INTERVAL_MS: "100",
  });
}
