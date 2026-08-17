import { beforeEach, describe, expect, it, vi } from "vitest";
import type { ActivationClient } from "./activation-client.js";
import { startDispatcherService } from "./service.js";

const state = vi.hoisted(() => ({
  calls: [] as string[],
  connectError: null as Error | null,
  lockAcquired: true,
  ownershipError: null as Error | null,
  recoveryError: null as Error | null,
  runtimeStopError: null as Error | null,
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
          throw new Error(`unexpected ownership query: ${sql}`);
        },
        release: () => {
          state.calls.push("ownership:release");
        },
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
  reclaimAndReenqueueActiveRunsForAllTenants: vi.fn(async () => {
    state.calls.push("recover");
    if (state.recoveryError) throw state.recoveryError;
    return 1;
  }),
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
    state.ownershipError = null;
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

  it("refuses to start a second dispatcher generation", async () => {
    state.lockAcquired = false;

    await expect(
      startDispatcherService({
        env: {
          NODE_ENV: "development",
          WORKFLOW_WORLD_URL: "postgres://workflow.test/world",
          WORKFLOW_DISPATCHER_ACTIVATION_API_URL: "http://activation.test",
        },
        activation,
      }),
    ).rejects.toThrow(/already owns/i);

    expect(state.calls).toEqual([
      "ownership:connect",
      "ownership:lock",
      "ownership:release",
      "pool:end",
    ]);
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
