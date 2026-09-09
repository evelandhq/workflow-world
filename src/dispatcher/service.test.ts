import { beforeEach, describe, expect, it, vi } from "vitest";
import type { ActivationClient } from "./activation-client.js";
import { startDispatcherService } from "./service.js";

const state = vi.hoisted(() => ({
  calls: [] as string[],
  connectError: null as Error | null,
  lockAcquired: true,
  ownershipError: null as Error | null,
  recoveryCandidates: [] as unknown[],
  recoveryError: null as Error | null,
  runtimeStopError: null as Error | null,
  /** The 'error' listener the ownership client registered, so a test can end its session. */
  ownershipClientError: null as ((error: Error) => void) | null,
}));

const workerUtils = vi.hoisted(() => ({
  release: vi.fn(async () => {
    state.calls.push("worker-utils:release");
  }),
}));

vi.mock("pg", () => ({
  Pool: class Pool {
    async connect() {
      state.calls.push("ownership:connect");
      if (state.connectError) throw state.connectError;
      return {
        query: async (sql: string) => {
          if (/pg_try_advisory_lock/i.test(sql)) {
            state.calls.push("ownership:lock");
            if (state.ownershipError) throw state.ownershipError;
            return { rows: [{ locked: state.lockAcquired }] };
          }
          if (/pg_advisory_unlock/i.test(sql)) {
            state.calls.push("ownership:unlock");
            return { rows: [{ unlocked: true }] };
          }
          if (/from pg_locks/i.test(sql)) {
            state.calls.push("ownership:holder");
            return { rows: [] };
          }
          // Session liveness settings and the heartbeat are exercised against
          // a real server in ownership.integration.test.ts.
          if (/^set /i.test(sql) || sql === "select 1") return { rows: [] };
          throw new Error(`unexpected ownership query: ${sql}`);
        },
        release: (destroy?: unknown) => {
          state.calls.push(destroy ? "ownership:destroy" : "ownership:release");
        },
        on: (event: string, listener: (error: Error) => void) => {
          if (event === "error") state.ownershipClientError = listener;
        },
        once: () => {},
        removeListener: () => {},
      };
    }

    async end() {
      state.calls.push("pool:end");
    }
  },
}));

vi.mock("graphile-worker", () => ({
  makeWorkerUtils: vi.fn(async () => {
    state.calls.push("worker-utils:create");
    return workerUtils;
  }),
}));

vi.mock("../migrate.js", () => ({
  runMigrations: vi.fn(async () => {
    state.calls.push("migrate");
  }),
}));

vi.mock("../storage-maintenance.js", () => ({
  startStorageMaintenanceLoop: vi.fn(() => ({
    stop: async () => {
      state.calls.push("maintenance:stop");
    },
  })),
}));

vi.mock("./boot-recovery.js", () => ({
  reclaimAndReenqueueActiveRunsForAllTenants: vi.fn(
    async (input: { filterRuns?: (runs: unknown[]) => Promise<unknown[]> | unknown[] }) => {
      state.calls.push("recover");
      if (state.recoveryError) throw state.recoveryError;
      if (input.filterRuns) {
        const kept = await input.filterRuns(state.recoveryCandidates);
        return kept.length;
      }
      return 1;
    },
  ),
}));

vi.mock("./runner.js", () => ({
  startDispatcher: vi.fn(async () => {
    state.calls.push("worker:start");
    return {
      workerUtils,
      stop: async () => {
        state.calls.push("worker:stop");
        if (state.runtimeStopError) throw state.runtimeStopError;
      },
    };
  }),
}));

describe("dispatcher service lifecycle", () => {
  const activation: ActivationClient = {
    activate: vi.fn(),
    renew: vi.fn(),
    release: vi.fn(),
  };

  beforeEach(() => {
    state.calls.length = 0;
    state.connectError = null;
    state.lockAcquired = true;
    state.ownershipClientError = null;
    state.ownershipError = null;
    state.recoveryCandidates = [];
    state.recoveryError = null;
    state.runtimeStopError = null;
    vi.clearAllMocks();
  });

  it("owns the dispatcher and completes recovery before starting workers", async () => {
    const service = await startDispatcherService({
      env: {
        NODE_ENV: "development",
        WORKFLOW_WORLD_URL: "postgres://workflow.test/world",
        WORKFLOW_DISPATCHER_ACTIVATION_API_URL: "http://activation.test",
      },
      activation,
    });

    expect(state.calls).toEqual([
      "ownership:connect",
      "ownership:lock",
      "migrate",
      "worker-utils:create",
      "recover",
      "worker:start",
    ]);

    await service.stop();
  });

  it("releases ownership and worker utilities when boot recovery fails", async () => {
    state.recoveryError = new Error("recovery failed");
    const telemetry = {
      emit: vi.fn(),
      shutdown: vi.fn(async () => {
        state.calls.push("telemetry:shutdown");
      }),
    };

    await expect(
      startDispatcherService({
        env: {
          NODE_ENV: "development",
          WORKFLOW_WORLD_URL: "postgres://workflow.test/world",
          WORKFLOW_DISPATCHER_ACTIVATION_API_URL: "http://activation.test",
        },
        activation,
        telemetry,
      }),
    ).rejects.toThrow("recovery failed");

    expect(state.calls).toEqual([
      "ownership:connect",
      "ownership:lock",
      "migrate",
      "worker-utils:create",
      "recover",
      "worker-utils:release",
      "ownership:unlock",
      "ownership:release",
      "pool:end",
      "telemetry:shutdown",
    ]);
  });

  it("closes the ownership client and pool when lock acquisition errors", async () => {
    state.ownershipError = new Error("ownership query failed");

    await expect(
      startDispatcherService({
        env: {
          NODE_ENV: "development",
          WORKFLOW_WORLD_URL: "postgres://workflow.test/world",
          WORKFLOW_DISPATCHER_ACTIVATION_API_URL: "http://activation.test",
        },
        activation,
      }),
    ).rejects.toThrow("ownership query failed");

    expect(state.calls).toEqual([
      "ownership:connect",
      "ownership:lock",
      "ownership:release",
      "pool:end",
    ]);
  });

  it("refuses to start a second dispatcher generation when told not to wait", async () => {
    state.lockAcquired = false;

    await expect(
      startDispatcherService({
        env: {
          NODE_ENV: "development",
          WORKFLOW_WORLD_URL: "postgres://workflow.test/world",
          WORKFLOW_DISPATCHER_ACTIVATION_API_URL: "http://activation.test",
          WORKFLOW_DISPATCHER_OWNERSHIP_WAIT_MS: "0",
        },
        activation,
      }),
    ).rejects.toThrow(/already owns/i);

    expect(state.calls).toEqual([
      "ownership:connect",
      "ownership:lock",
      "ownership:holder",
      "ownership:release",
      "pool:end",
    ]);
  });

  it("waits for the lock instead of failing when the holder goes away", async () => {
    state.lockAcquired = false;
    const warnings: string[] = [];
    // The second attempt succeeds: the previous owner's session was reclaimed.
    const started = startDispatcherService({
      env: {
        NODE_ENV: "development",
        WORKFLOW_WORLD_URL: "postgres://workflow.test/world",
        WORKFLOW_DISPATCHER_ACTIVATION_API_URL: "http://activation.test",
        WORKFLOW_DISPATCHER_OWNERSHIP_RETRY_INTERVAL_MS: "10",
      },
      activation,
      telemetry: {
        emit(event) {
          if (event.severity === "warn") warnings.push(event.eventName);
        },
        async shutdown() {},
      },
    });
    await vi.waitFor(() => expect(state.calls).toContain("ownership:holder"));
    state.lockAcquired = true;
    const service = await started;
    expect(service.phase).toBe("ready");
    expect(warnings).toContain("workflow_dispatcher.ownership_wait");
    // At least one miss (lock → holder lookup) before the attempt that won,
    // and nothing past ownership ran until it did.
    const attempts = state.calls.filter((call) => call === "ownership:lock").length;
    expect(attempts).toBeGreaterThanOrEqual(2);
    expect(state.calls.indexOf("migrate")).toBeGreaterThan(
      state.calls.lastIndexOf("ownership:lock"),
    );
    await service.stop();
  });

  it("stops itself when the owning session is terminated under it", async () => {
    const phases: string[] = [];
    const service = await startDispatcherService({
      env: {
        NODE_ENV: "development",
        WORKFLOW_WORLD_URL: "postgres://workflow.test/world",
        WORKFLOW_DISPATCHER_ACTIVATION_API_URL: "http://activation.test",
      },
      activation,
      lifecycle: { onPhase: (event) => phases.push(event.phase) },
    });
    expect(service.phase).toBe("ready");
    expect(state.ownershipClientError).not.toBeNull();

    // What pg emits on a checked-out client when the server ends its session.
    state.ownershipClientError!(new Error("terminating connection due to administrator command"));

    await vi.waitFor(() => expect(service.phase).toBe("stopped"));
    expect(phases).toEqual([
      "ownership_acquired",
      "migrations_applied",
      "boot_recovery_completed",
      "ready",
      "ownership_lost",
      "stopped",
    ]);
    // The dead client is destroyed, never unlocked on; everything else drains.
    expect(state.calls).not.toContain("ownership:unlock");
    expect(state.calls).toContain("ownership:destroy");
    expect(state.calls.at(-1)).toBe("pool:end");
    // A later stop() is the same, already-finished shutdown.
    await service.stop();
    expect(phases.filter((phase) => phase === "stopped")).toHaveLength(1);
  });

  it("closes the pool when the ownership connection cannot be established", async () => {
    state.connectError = new Error("ownership connect failed");

    await expect(
      startDispatcherService({
        env: {
          NODE_ENV: "development",
          WORKFLOW_WORLD_URL: "postgres://workflow.test/world",
          WORKFLOW_DISPATCHER_ACTIVATION_API_URL: "http://activation.test",
        },
        activation,
      }),
    ).rejects.toThrow("ownership connect failed");

    expect(state.calls).toEqual(["ownership:connect", "pool:end"]);
  });

  it("reports the lifecycle machine-readably through the observer", async () => {
    const events: Array<{ phase: string; attributes?: Record<string, unknown> }> = [];
    const service = await startDispatcherService({
      env: {
        NODE_ENV: "development",
        WORKFLOW_WORLD_URL: "postgres://workflow.test/world",
        WORKFLOW_DISPATCHER_ACTIVATION_API_URL: "http://activation.test",
      },
      activation,
      lifecycle: {
        onPhase: (event) => {
          events.push({ phase: event.phase, attributes: event.attributes });
        },
      },
    });
    await service.stop();

    expect(events.map((event) => event.phase)).toEqual([
      "ownership_acquired",
      "migrations_applied",
      "boot_recovery_completed",
      "ready",
      "stopped",
    ]);
    // Recovery reports what it re-enqueued, so a registration can carry it.
    expect(events[2]?.attributes).toMatchObject({ reenqueuedRuns: 1 });
  });

  it("hands the host's boot-recovery filter to the sweep and reports what it excluded", async () => {
    state.recoveryCandidates = [
      { tenantId: "p_a", runId: "wrun_1", deploymentId: "dep_live" },
      { tenantId: "p_a", runId: "wrun_2", deploymentId: "dep_retired" },
    ];
    const seen: unknown[] = [];
    const events: Array<{ phase: string; attributes?: Record<string, unknown> }> = [];

    const service = await startDispatcherService({
      env: {
        NODE_ENV: "development",
        WORKFLOW_WORLD_URL: "postgres://workflow.test/world",
        WORKFLOW_DISPATCHER_ACTIVATION_API_URL: "http://activation.test",
      },
      activation,
      filterBootRecoveryRuns: (runs) => {
        seen.push(...runs);
        return runs.filter((run) => run.deploymentId === "dep_live");
      },
      lifecycle: {
        onPhase: (event) => {
          events.push({ phase: event.phase, attributes: event.attributes });
        },
      },
    });
    await service.stop();

    expect(seen).toEqual(state.recoveryCandidates);
    const recovery = events.find((event) => event.phase === "boot_recovery_completed");
    expect(recovery?.attributes).toMatchObject({ reenqueuedRuns: 1, filteredRuns: 1 });
  });

  it("a failing host preflight keeps boot recovery from ever running", async () => {
    await expect(
      startDispatcherService({
        env: {
          NODE_ENV: "development",
          WORKFLOW_WORLD_URL: "postgres://workflow.test/world",
          WORKFLOW_DISPATCHER_ACTIVATION_API_URL: "http://activation.test",
        },
        activation,
        beforeBootRecovery: async () => {
          state.calls.push("preflight");
          throw new Error("world identity unreadable");
        },
      }),
    ).rejects.toThrow("world identity unreadable");

    expect(state.calls).toEqual([
      "ownership:connect",
      "ownership:lock",
      "migrate",
      "worker-utils:create",
      "preflight",
      "worker-utils:release",
      "ownership:unlock",
      "ownership:release",
      "pool:end",
    ]);
    expect(state.calls).not.toContain("recover");
  });

  it("releases ownership and the pool when worker shutdown fails", async () => {
    const telemetry = {
      emit: vi.fn(),
      shutdown: vi.fn(async () => {
        state.calls.push("telemetry:shutdown");
      }),
    };
    const service = await startDispatcherService({
      env: {
        NODE_ENV: "development",
        WORKFLOW_WORLD_URL: "postgres://workflow.test/world",
        WORKFLOW_DISPATCHER_ACTIVATION_API_URL: "http://activation.test",
      },
      activation,
      telemetry,
    });
    state.calls.length = 0;
    state.runtimeStopError = new Error("worker stop failed");

    await expect(service.stop()).rejects.toThrow("worker stop failed");

    expect(state.calls).toEqual([
      "maintenance:stop",
      "worker:stop",
      "ownership:unlock",
      "ownership:release",
      "pool:end",
      "telemetry:shutdown",
    ]);
  });
});
