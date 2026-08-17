import { makeWorkerUtils, run, type Runner, type WorkerUtils } from "graphile-worker";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { FLOW_JOB_NAME } from "../dispatch-contract.js";
import { MessageData } from "../message.js";
import {
  reclaimAndReenqueueActiveRunsForAllTenants,
  reenqueueActiveRunsForAllTenants,
} from "./boot-recovery.js";
import { startDispatcher } from "./runner.js";

vi.mock("graphile-worker", () => ({
  makeWorkerUtils: vi.fn(),
  run: vi.fn(),
}));

describe("dispatcher queue retry policy", () => {
  const workerUtils = {
    addJob: vi.fn(async () => {}),
    cleanup: vi.fn(async () => {}),
    forceUnlockWorkers: vi.fn(async () => {}),
    release: vi.fn(async () => {}),
  } as unknown as WorkerUtils;
  const runner = {
    stop: vi.fn(async () => {}),
    promise: Promise.resolve(),
  } as unknown as Runner;

  beforeEach(() => {
    vi.clearAllMocks();
    vi.mocked(makeWorkerUtils).mockResolvedValue(workerUtils);
    vi.mocked(run).mockResolvedValue(runner);
  });

  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it("uses 49 attempts when the dispatcher re-enqueues a timed-out delivery", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(async () => Response.json({ timeoutSeconds: 30 })),
    );
    const pool = { query: vi.fn(async () => ({ rows: [] })) } as any;
    const runtime = await startDispatcher({
      pool,
      config: {
        concurrency: 1,
        pollIntervalMs: 100,
        maxInFlightPerTenant: 1,
        queueGcIntervalMs: 3_600_000,
      },
      deps: {
        activation: {
          activate: vi.fn(async () => ({
            type: "activated" as const,
            activation: { leaseId: "lease-1", endpointPort: 12345 },
          })),
          renew: vi.fn(async () => true),
          release: vi.fn(async () => {}),
        },
        runLookup: vi.fn(async () => null),
        runtimeSecret: "test-secret",
        dispatchTimeoutMs: 5_000,
        leaseRenewIntervalMs: 60_000,
        activationLeaseTtlMs: 180_000,
      },
    });

    try {
      const task = vi.mocked(run).mock.calls[0]?.[0]?.taskList?.[FLOW_JOB_NAME];
      expect(task).toBeTypeOf("function");
      const message: MessageData = {
        id: "greet",
        data: Buffer.from(JSON.stringify({ runId: "wrun_1" })),
        attempt: 1,
        messageId: "msg_1" as MessageData["messageId"],
        idempotencyKey: "wrun_1",
        tenantId: "tenant-1",
        deploymentId: "deployment-1",
      };

      await task!(MessageData.encode(message), {
        job: { attempts: 1, max_attempts: 49 },
      } as any);

      expect(workerUtils.addJob).toHaveBeenCalledWith(
        FLOW_JOB_NAME,
        expect.anything(),
        expect.objectContaining({ maxAttempts: 49 }),
      );
    } finally {
      await runtime.stop();
    }
  });

  it("uses 49 attempts for boot-recovery jobs", async () => {
    const pool = {
      query: vi.fn(async () => ({
        rows: [
          {
            tenant_id: "tenant-1",
            id: "wrun_1",
            name: "greet",
            deployment_id: "deployment-1",
            queue_namespace: "",
          },
        ],
      })),
    } as any;

    await reenqueueActiveRunsForAllTenants({ pool, workerUtils });

    expect(workerUtils.addJob).toHaveBeenCalledWith(
      FLOW_JOB_NAME,
      expect.anything(),
      expect.objectContaining({ maxAttempts: 49 }),
    );
  });

  it("unlocks the old workers holding active run queues before re-enqueueing", async () => {
    const pool = {
      query: vi.fn(async () => ({
        rows: [
          {
            tenant_id: "tenant-1",
            id: "wrun_1",
            name: "greet",
            deployment_id: "deployment-1",
            queue_namespace: "",
            locked_by: "worker-old-generation",
          },
        ],
      })),
    } as any;

    await reclaimAndReenqueueActiveRunsForAllTenants({ pool, workerUtils });

    expect(workerUtils.forceUnlockWorkers).toHaveBeenCalledWith(["worker-old-generation"]);
    expect(vi.mocked(workerUtils.forceUnlockWorkers).mock.invocationCallOrder[0]).toBeLessThan(
      vi.mocked(workerUtils.addJob).mock.invocationCallOrder[0]!,
    );
  });

  it("does not unlock live workers when invoked as a standalone re-enqueue", async () => {
    const pool = {
      query: vi.fn(async () => ({
        rows: [
          {
            tenant_id: "tenant-1",
            id: "wrun_1",
            name: "greet",
            deployment_id: "deployment-1",
            queue_namespace: "",
            locked_by: "worker-current-generation",
          },
        ],
      })),
    } as any;

    await reenqueueActiveRunsForAllTenants({ pool, workerUtils });

    expect(workerUtils.forceUnlockWorkers).not.toHaveBeenCalled();
    expect(workerUtils.addJob).toHaveBeenCalledOnce();
  });

  it("does not re-enqueue an active run with an unresolved dead letter", async () => {
    const query = vi.fn(async (_sql: string) => ({ rows: [] }));
    const pool = { query } as any;

    await reenqueueActiveRunsForAllTenants({ pool, workerUtils });

    expect(query).toHaveBeenCalledOnce();
    expect(query.mock.calls[0]![0]).toMatch(/not exists[\s\S]+dispatch_dead_letters/i);
    expect(query.mock.calls[0]![0]).toMatch(/resolved_at is null/i);
  });

  it("releases WorkerUtils even when the Graphile runner fails to stop", async () => {
    vi.mocked(runner.stop).mockRejectedValueOnce(new Error("runner stop failed"));
    const runtime = await startDispatcher({
      pool: { query: vi.fn() } as any,
      config: {
        concurrency: 1,
        pollIntervalMs: 100,
        maxInFlightPerTenant: 1,
        queueGcIntervalMs: 3_600_000,
      },
      deps: {
        activation: {
          activate: vi.fn(),
          renew: vi.fn(),
          release: vi.fn(),
        },
        runLookup: vi.fn(async () => null),
        runtimeSecret: "test-secret",
        dispatchTimeoutMs: 5_000,
        leaseRenewIntervalMs: 60_000,
        activationLeaseTtlMs: 180_000,
      },
    });

    await expect(runtime.stop()).rejects.toThrow("runner stop failed");
    expect(workerUtils.release).toHaveBeenCalledOnce();
  });
});
