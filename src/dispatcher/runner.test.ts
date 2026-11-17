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

  it("unlocks the old workers holding flow jobs before re-enqueueing, then collects queue rows", async () => {
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
    // A 1.x World's per-run queue rows, deleted once their jobs are gone.
    expect(workerUtils.cleanup).toHaveBeenCalledWith({ tasks: ["GC_JOB_QUEUES"] });
    expect(vi.mocked(workerUtils.cleanup).mock.invocationCallOrder[0]).toBeGreaterThan(
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
    expect(workerUtils.cleanup).not.toHaveBeenCalled();
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

  describe("run gate", () => {
    const TENANT = "tenant-gate";
    const wake = { runId: "wrun_G", runSpecVersion: 9 };
    const hookWake = { ...wake, hookResumeTiming: { resumeRequestedAtMs: 7 }, traceCarrier: "t" };
    const startMessage = { ...wake, runInput: { specVersion: 9, input: [] } };
    const step = { ...wake, stepId: "step_1" };

    type Posted = {
      messageId: string;
      attempt: string;
      deliveryCount: string;
      body: Record<string, unknown>;
      answer: (response: Response) => void;
    };

    /** Every POST, held until the test answers it. */
    function holdPosts(): Posted[] {
      const posted: Posted[] = [];
      vi.mocked(nodeHttpFetch).mockImplementation(
        (_url, init) =>
          new Promise<Response>((resolve) => {
            const headers = new Headers(init?.headers);
            posted.push({
              messageId: headers.get("x-vqs-message-id")!,
              attempt: headers.get("x-vqs-message-attempt")!,
              deliveryCount: headers.get("x-vqs-message-delivery-count")!,
              body: JSON.parse(Buffer.from(init!.body as Buffer).toString("utf8")),
              answer: resolve,
            });
          }),
      );
      return posted;
    }

    async function startGate() {
      const activate = vi.fn(async () => ({
        type: "activated" as const,
        activation: { leaseId: "lease-gate", endpointPort: 12345 },
      }));
      const query = vi.fn(async () => ({ rows: [] }));
      const runtime = await startDispatcher({
        pool: { query } as any,
        config: { concurrency: 8, pollIntervalMs: 100, maxInFlightPerTenant: 8 },
        deps: {
          activation: { activate, renew: vi.fn(async () => true), release: vi.fn(async () => {}) },
          runLookup: vi.fn(async () => null),
          runtimeSecret: "test-secret",
          dispatchTimeoutMs: 5_000,
          leaseRenewIntervalMs: 60_000,
          activationLeaseTtlMs: 180_000,
        },
      });
      const task = vi.mocked(run).mock.calls.at(-1)?.[0]?.taskList?.[FLOW_JOB_NAME];
      let next = 0;
      /** Runs the job handler as graphile would for a job holding this message. */
      const deliver = (
        body: unknown,
        options: { messageId?: string; attempts?: number; message?: Partial<MessageData> } = {},
      ) =>
        task!(
          MessageData.encode({
            id: "greet",
            data: Buffer.from(JSON.stringify(body)),
            attempt: 1,
            messageId: (options.messageId ??
              `msg_gate_${String((next += 1))}`) as MessageData["messageId"],
            tenantId: TENANT,
            deploymentId: "deployment-1",
            ...options.message,
          }),
          { job: { attempts: options.attempts ?? 1, max_attempts: 49 } } as any,
        );
      /** What the dispatcher re-added through `addJob`, decoded. */
      const added = () =>
        vi.mocked(workerUtils.addJob).mock.calls.map(([, payload, spec]) => ({
          message: MessageData.parse(payload),
          spec: spec as { jobKey?: string; runAt?: Date; maxAttempts?: number },
        }));
      return { runtime, deliver, added, activate, query };
    }

    it("never gates a step message of a run whose delivery is in flight", async () => {
      const posted = holdPosts();
      const { runtime, deliver } = await startGate();
      try {
        const holder = deliver(wake);
        await vi.waitFor(() => expect(posted).toHaveLength(1));
        const stepDelivery = deliver(step);
        await vi.waitFor(() => expect(posted).toHaveLength(2));
        expect(posted[1]!.body).toMatchObject({ stepId: "step_1" });
        posted[1]!.answer(Response.json({ ok: true }));
        await stepDelivery;
        posted[0]!.answer(Response.json({ ok: true }));
        await holder;
        expect(runtime.runGate.snapshot()).toEqual({ held: 0, pendingWakes: 0 });
      } finally {
        await runtime.stop();
      }
    });

    it("collapses wakes that arrive during a delivery into one in-place re-run", async () => {
      const posted = holdPosts();
      const { runtime, deliver, added, activate } = await startGate();
      try {
        const holder = deliver(wake, { messageId: "msg_holder", attempts: 2 });
        await vi.waitFor(() => expect(posted).toHaveLength(1));

        // Each completes at once, undelivered: one wake is kept for the run, and
        // the one carrying a hook resume's timing replaces one without.
        await deliver(wake, { messageId: "msg_wake_1" });
        await deliver(hookWake, { messageId: "msg_wake_2" });
        await deliver(wake, { messageId: "msg_wake_3" });
        expect(posted).toHaveLength(1);
        expect(runtime.runGate.snapshot()).toEqual({ held: 1, pendingWakes: 1 });
        // A coalesced wake held its tenant slot only while it was handled.
        expect(runtime.fairness.snapshot()).toEqual({ [TENANT]: 1 });

        posted[0]!.answer(Response.json({ ok: true }));
        await vi.waitFor(() => expect(posted).toHaveLength(2));
        // The holder's own message again, the next delivery count, the same
        // attempt, and the absorbed wake's resume timing and trace.
        expect(posted[1]).toMatchObject({
          messageId: "msg_holder",
          attempt: "2",
          deliveryCount: "3",
          body: { ...hookWake },
        });
        posted[1]!.answer(Response.json({ ok: true }));
        await holder;

        expect(posted).toHaveLength(2);
        expect(activate).toHaveBeenCalledOnce();
        expect(added()).toEqual([]);
        expect(runtime.runGate.snapshot()).toEqual({ held: 0, pendingWakes: 0 });
        expect(runtime.fairness.snapshot()).toEqual({});
        expect(runtime.liveFeedGrants.snapshot()).toMatchObject({ total: 0 });
      } finally {
        await runtime.stop();
      }
    });

    it("sets a wake past its first delivery aside instead of coalescing it, counts intact", async () => {
      const posted = holdPosts();
      const { runtime, deliver, added } = await startGate();
      try {
        const holder = deliver(wake, { messageId: "msg_holder" });
        await vi.waitFor(() => expect(posted).toHaveLength(1));

        // A continuation: attempt 2, delivery 4. Its counts are what the
        // runtime caps a run by, and a re-run would report the holder's.
        await deliver(wake, {
          messageId: "msg_continuation",
          message: { attemptOffset: 1, deliveryOffset: 3 },
        });
        // A retried wake: graphile's second attempt of the same job.
        await deliver(wake, { messageId: "msg_retried", attempts: 2 });

        expect(posted).toHaveLength(1);
        expect(runtime.runGate.snapshot()).toEqual({ held: 1, pendingWakes: 0 });
        expect(added().map((job) => job.message.messageId)).toEqual([
          "msg_continuation",
          "msg_retried",
        ]);
        for (const job of added()) expect(job.spec.runAt).toBeInstanceOf(Date);

        posted[0]!.answer(Response.json({ ok: true }));
        await holder;
        // Nothing was absorbed, so the holder does not re-run.
        expect(posted).toHaveLength(1);

        // Claimed again with the gate free, the continuation reports its own counts.
        const again = deliver(wake, {
          messageId: "msg_continuation",
          message: added()[0]!.message,
        });
        await vi.waitFor(() => expect(posted).toHaveLength(2));
        expect(posted[1]).toMatchObject({
          messageId: "msg_continuation",
          attempt: "2",
          deliveryCount: "4",
        });
        posted[1]!.answer(Response.json({ ok: true }));
        await again;
      } finally {
        await runtime.stop();
      }
    });

    it("re-runs at most 32 times, then puts the wake left back on the queue", async () => {
      const { runtime, deliver, added } = await startGate();
      let posts = 0;
      // Every delivery of the run is woken again while it runs.
      vi.mocked(nodeHttpFetch).mockImplementation(async () => {
        posts += 1;
        await deliver(wake, { messageId: `msg_storm_${String(posts)}` });
        return Response.json({ ok: true });
      });
      try {
        await deliver(wake, { messageId: "msg_storm_holder" });
        expect(posts).toBe(33);
        // The wake the 33rd POST left goes back as an ordinary job, now.
        expect(added()).toHaveLength(1);
        expect(added()[0]!.message).toMatchObject({ messageId: "msg_storm_33", runId: "wrun_G" });
        expect(added()[0]!.spec.jobKey).toBe("msg_storm_33");
        expect(added()[0]!.spec.runAt!.getTime()).toBeLessThanOrEqual(Date.now());
        expect(runtime.runGate.snapshot()).toEqual({ held: 0, pendingWakes: 0 });
      } finally {
        await runtime.stop();
      }
    });

    it("sets a data-bearing message aside with its counts intact and delivers it once, later", async () => {
      const posted = holdPosts();
      const { runtime, deliver, added } = await startGate();
      try {
        const holder = deliver(wake);
        await vi.waitFor(() => expect(posted).toHaveLength(1));

        // A start message on its third attempt, its fifth delivery.
        await deliver(startMessage, {
          messageId: "msg_start",
          attempts: 3,
          message: { deliveryOffset: 2 },
        });
        expect(posted).toHaveLength(1);
        expect(added()).toHaveLength(1);
        const { message: aside, spec } = added()[0]!;
        expect(aside).toMatchObject({
          messageId: "msg_start",
          attempt: 3,
          attemptOffset: 2,
          deliveryOffset: 4,
          setAside: 1,
          runId: "wrun_G",
        });
        expect(spec.jobKey).toBe("msg_start");
        expect(spec.maxAttempts).toBe(47);
        const delayMs = spec.runAt!.getTime() - Date.now();
        expect(delayMs).toBeGreaterThan(0);
        expect(delayMs).toBeLessThanOrEqual(1_000);

        posted[0]!.answer(Response.json({ ok: true }));
        await holder;
        // Not coalesced, so the holder did not re-run for it.
        expect(posted).toHaveLength(1);

        // Its job comes due: delivered with the counts it had.
        const later = task(aside);
        await vi.waitFor(() => expect(posted).toHaveLength(2));
        expect(posted[1]).toMatchObject({
          messageId: "msg_start",
          attempt: "3",
          deliveryCount: "5",
          body: startMessage,
        });
        posted[1]!.answer(Response.json({ ok: true }));
        await later;
        expect(posted).toHaveLength(2);
      } finally {
        await runtime.stop();
      }

      function task(message: MessageData) {
        const handler = vi.mocked(run).mock.calls.at(-1)?.[0]?.taskList?.[FLOW_JOB_NAME];
        return handler!(MessageData.encode(message), {
          job: { attempts: 1, max_attempts: 47 },
        } as any);
      }
    });

    it("waits longer each time a data-bearing message is set aside, up to 30 s", async () => {
      const posted = holdPosts();
      const { runtime, deliver, added } = await startGate();
      try {
        const holder = deliver(wake);
        await vi.waitFor(() => expect(posted).toHaveLength(1));
        await deliver(startMessage, { messageId: "msg_again", message: { setAside: 3 } });
        await deliver(startMessage, { messageId: "msg_often", message: { setAside: 9 } });
        const delays = added().map(({ spec }) => (spec.runAt!.getTime() - Date.now()) / 1000);
        expect(delays[0]).toBeGreaterThan(7);
        expect(delays[0]).toBeLessThanOrEqual(8);
        expect(delays[1]).toBeGreaterThan(29);
        expect(delays[1]).toBeLessThanOrEqual(30);
        expect(added().map(({ message }) => message.setAside)).toEqual([4, 10]);
        posted[0]!.answer(Response.json({ ok: true }));
        await holder;
      } finally {
        await runtime.stop();
      }
    });

    const exits: Array<{
      name: string;
      holderBody: unknown;
      answer: () => Response;
      settles: "resolves" | "rejects";
      jobs: number;
    }> = [
      {
        name: "a reschedule",
        holderBody: wake,
        answer: () => Response.json({ timeoutSeconds: 30 }),
        settles: "resolves",
        jobs: 2,
      },
      {
        name: "a handoff",
        holderBody: wake,
        answer: () => Response.json({ timeoutSeconds: 0 }),
        settles: "resolves",
        jobs: 2,
      },
      {
        name: "a retry",
        holderBody: wake,
        answer: () => new Response("boom", { status: 503 }),
        settles: "rejects",
        jobs: 1,
      },
      {
        name: "a dead letter",
        holderBody: wake,
        answer: () => new Response("bad request", { status: 400 }),
        settles: "resolves",
        jobs: 1,
      },
    ];
    for (const exit of exits) {
      it(`puts a pending wake back on the queue when the holder ends in ${exit.name}`, async () => {
        const posted = holdPosts();
        const { runtime, deliver, added } = await startGate();
        try {
          const holder = deliver(exit.holderBody, { messageId: "msg_exit_holder" });
          await vi.waitFor(() => expect(posted).toHaveLength(1));
          await deliver(hookWake, { messageId: "msg_exit_wake" });
          posted[0]!.answer(exit.answer());
          if (exit.settles === "rejects") await expect(holder).rejects.toThrow();
          else await holder;

          // Not re-run in place: only `completed` re-runs.
          expect(posted).toHaveLength(1);
          const jobs = added();
          expect(jobs).toHaveLength(exit.jobs);
          const readded = jobs.find(({ message }) => message.messageId === "msg_exit_wake");
          // Its own message, its counts as they were, due now.
          expect(readded?.message).toMatchObject({ attempt: 1 });
          expect(readded?.message.attemptOffset ?? 0).toBe(0);
          expect(readded?.spec).toMatchObject({ jobKey: "msg_exit_wake", maxAttempts: 49 });
          expect(readded!.spec.runAt!.getTime()).toBeLessThanOrEqual(Date.now());
          if (exit.jobs === 2) {
            // A reschedule or handoff: the wake's job is written before the
            // continuation, while the holder's job still owns its key.
            expect(jobs.map(({ message }) => message.messageId)).toEqual([
              "msg_exit_wake",
              "msg_exit_holder",
            ]);
          }
          expect(runtime.runGate.snapshot()).toEqual({ held: 0, pendingWakes: 0 });
        } finally {
          await runtime.stop();
        }
      });
    }

    for (const timeoutSeconds of [30, 0]) {
      it(`writes no continuation and fails the holder when its wake cannot be put back (timeoutSeconds ${String(timeoutSeconds)})`, async () => {
        const posted = holdPosts();
        const { runtime, deliver, added } = await startGate();
        try {
          const holder = deliver(wake, { messageId: "msg_failing_holder" });
          await vi.waitFor(() => expect(posted).toHaveLength(1));
          await deliver(wake, { messageId: "msg_lost_wake" });
          vi.mocked(workerUtils.addJob).mockRejectedValueOnce(new Error("database down"));
          posted[0]!.answer(Response.json({ timeoutSeconds }));
          // graphile redelivers the holder, whose job still owns its key: a
          // delivery that starts after the wake.
          await expect(holder).rejects.toThrow("database down");
          // The one add attempted was the wake's; no continuation was written.
          expect(workerUtils.addJob).toHaveBeenCalledOnce();
          expect(added()[0]!.message.messageId).toBe("msg_lost_wake");
          expect(runtime.runGate.snapshot()).toEqual({ held: 0, pendingWakes: 0 });
        } finally {
          await runtime.stop();
        }
      });
    }

    it("puts a wake claimed while the gate is closing back on the queue, not in memory", async () => {
      const posted = holdPosts();
      const { runtime, deliver, added } = await startGate();
      // The pending wake's re-add hangs, so the gate stays closing.
      let finishReadd!: () => void;
      vi.mocked(workerUtils.addJob).mockImplementationOnce(
        () =>
          new Promise((resolve) => {
            finishReadd = () => resolve({} as any);
          }),
      );
      try {
        const holder = deliver(wake, { messageId: "msg_closing_holder" });
        await vi.waitFor(() => expect(posted).toHaveLength(1));
        await deliver(wake, { messageId: "msg_closing_pending" });
        posted[0]!.answer(Response.json({ timeoutSeconds: 30 }));
        await vi.waitFor(() => expect(workerUtils.addJob).toHaveBeenCalledOnce());

        await deliver(hookWake, { messageId: "msg_closing_late" });
        // Not recorded: put back as a short-delayed job of its own.
        expect(runtime.runGate.snapshot()).toEqual({ held: 1, pendingWakes: 0 });
        const late = added().find(({ message }) => message.messageId === "msg_closing_late");
        expect(late?.message).toMatchObject({ setAside: 1, runId: "wrun_G" });
        expect(late?.spec.jobKey).toBe("msg_closing_late");
        expect(late!.spec.runAt!.getTime()).toBeGreaterThan(Date.now());

        finishReadd();
        await holder;
        expect(added().map(({ message }) => message.messageId)).toEqual([
          "msg_closing_pending",
          "msg_closing_late",
          "msg_closing_holder",
        ]);
        expect(posted).toHaveLength(1);
      } finally {
        await runtime.stop();
      }
    });

    it("stops keeping wakes in memory once the re-run cap is reached", async () => {
      const { runtime, deliver, added } = await startGate();
      let posts = 0;
      vi.mocked(nodeHttpFetch).mockImplementation(async () => {
        posts += 1;
        await deliver(wake, { messageId: `msg_cap_${String(posts)}` });
        return Response.json({ ok: true });
      });
      try {
        await deliver(wake, { messageId: "msg_cap_holder" });
        expect(posts).toBe(33);
        expect(added().map(({ message }) => message.messageId)).toEqual(["msg_cap_33"]);
        expect(runtime.runGate.snapshot()).toEqual({ held: 0, pendingWakes: 0 });
      } finally {
        await runtime.stop();
      }
    });

    it("gates runs per tenant: the same run id in two tenants runs in parallel", async () => {
      const posted = holdPosts();
      const { runtime, deliver } = await startGate();
      try {
        const first = deliver(wake);
        const second = deliver(wake, { message: { tenantId: "tenant-other" } });
        await vi.waitFor(() => expect(posted).toHaveLength(2));
        for (const post of posted) post.answer(Response.json({ ok: true }));
        await Promise.all([first, second]);
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
