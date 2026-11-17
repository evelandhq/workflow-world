import { nodeHttpFetch } from "@workflow/world/node-http.js";
import { makeWorkerUtils, run, type Runner, type WorkerUtils } from "graphile-worker";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { FLOW_JOB_NAME } from "../dispatch-contract.js";
import { MessageData } from "../message.js";
import {
  reclaimAndReenqueueActiveRunsForAllTenants,
  reenqueueActiveRunsForAllTenants,
} from "./boot-recovery.js";
import { startDispatcher } from "./runner.js";

// The dispatcher delivers over `nodeHttpFetch`, not the global `fetch`.
vi.mock("@workflow/world/node-http.js", () => ({ nodeHttpFetch: vi.fn() }));

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
    vi.mocked(nodeHttpFetch).mockResolvedValue(Response.json({ timeoutSeconds: 30 }));
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

  describe("live feed grants", () => {
    async function startWith(concurrency: number, activate?: () => Promise<unknown>) {
      const runtime = await startDispatcher({
        pool: { query: vi.fn(async () => ({ rows: [] })) } as any,
        config: {
          concurrency,
          pollIntervalMs: 100,
          maxInFlightPerTenant: 8,
          queueGcIntervalMs: 3_600_000,
        },
        deps: {
          activation: {
            activate: vi.fn(
              activate ??
                (async () => ({
                  type: "activated" as const,
                  activation: { leaseId: "lease-1", endpointPort: 12345 },
                })),
            ) as any,
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
      const task = vi.mocked(run).mock.calls.at(-1)?.[0]?.taskList?.[FLOW_JOB_NAME];
      const deliver = (body: unknown, tenantId = "tenant-1") =>
        task!(
          MessageData.encode({
            id: "greet",
            data: Buffer.from(JSON.stringify(body)),
            attempt: 1,
            messageId: `msg_${Math.random().toString(36).slice(2)}` as MessageData["messageId"],
            tenantId,
            deploymentId: "deployment-1",
          }),
          { job: { attempts: 1, max_attempts: 49 } } as any,
        );
      return { runtime, deliver };
    }

    /** The grant header of each POST so far. */
    const grantsSent = () =>
      vi
        .mocked(nodeHttpFetch)
        .mock.calls.map(([, init]) => new Headers(init?.headers).get("x-eveland-live-feed"));

    const orchestrator = { runId: "wrun_1", runSpecVersion: 9 };
    const step = { runId: "wrun_1", stepId: "step_1", runSpecVersion: 9 };

    it("grants orchestrator deliveries only, and releases each grant when it returns", async () => {
      vi.mocked(nodeHttpFetch).mockImplementation(async () => Response.json({ ok: true }));
      const { runtime, deliver } = await startWith(4);
      try {
        await deliver(orchestrator);
        await deliver(step);
        // A message that names a run but carries no spec stamp (boot recovery).
        await deliver({ runId: "wrun_2" });
        expect(grantsSent()).toEqual(["1", null, "1"]);
        expect(runtime.liveFeedGrants.snapshot()).toMatchObject({ total: 0, tenants: {} });
      } finally {
        await runtime.stop();
      }
    });

    it("keeps granted deliveries in flight at half the concurrency", async () => {
      const answers: Array<() => void> = [];
      vi.mocked(nodeHttpFetch).mockImplementation(
        () =>
          new Promise<Response>((resolve) => {
            answers.push(() => resolve(Response.json({ ok: true })));
          }),
      );
      const { runtime, deliver } = await startWith(4);
      try {
        const held = [
          deliver(orchestrator, "t1"),
          deliver(orchestrator, "t2"),
          deliver(orchestrator, "t3"),
        ];
        await vi.waitFor(() => expect(answers).toHaveLength(3));
        expect(grantsSent()).toEqual(["1", "1", null]);
        expect(runtime.liveFeedGrants.snapshot()).toMatchObject({ total: 2 });
        for (const answer of answers) answer();
        await Promise.all(held);
        expect(runtime.liveFeedGrants.snapshot()).toMatchObject({ total: 0 });
      } finally {
        await runtime.stop();
      }
    });

    it("releases a grant when the delivery fails or the dispatch throws", async () => {
      vi.mocked(nodeHttpFetch).mockImplementation(async () => {
        throw new Error("connection reset");
      });
      const failing = await startWith(4);
      try {
        await expect(failing.deliver(orchestrator)).rejects.toThrow(/Dispatch failed/);
        expect(grantsSent()).toEqual(["1"]);
        expect(failing.runtime.liveFeedGrants.snapshot()).toMatchObject({ total: 0 });
      } finally {
        await failing.runtime.stop();
      }

      const throwing = await startWith(4, async () => {
        throw new Error("activation API down");
      });
      try {
        await expect(throwing.deliver(orchestrator)).rejects.toThrow(/Dispatch threw/);
        expect(throwing.runtime.liveFeedGrants.snapshot()).toMatchObject({ total: 0 });
      } finally {
        await throwing.runtime.stop();
      }
    });

    it("grants none at a concurrency of 1", async () => {
      vi.mocked(nodeHttpFetch).mockImplementation(async () => Response.json({ ok: true }));
      const { runtime, deliver } = await startWith(1);
      try {
        await deliver(orchestrator);
        expect(grantsSent()).toEqual([null]);
      } finally {
        await runtime.stop();
      }
    });
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
