/**
 * Ported from `@workflow/world-postgres`'s `queue.test.ts`, for the half of
 * the queue a deployment runs: sending a message writes a graphile job, and
 * `createQueueHandler` receives what the dispatcher POSTs back.
 *
 * What is added on top of the port is the fork's own contract: the shared
 * job name, the per-run graphile `queueName`, the tenant flags, and the
 * dispatch-contract wrapper around eve's queue handler.
 */
import { getQueueTopicPrefix } from "@workflow/world";
import { createWorld } from "@workflow/world-local";
import { makeWorkerUtils, type WorkerUtils } from "graphile-worker";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { ResolvedWorldConfig } from "./config.js";
import {
  DEPLOYMENT_HEADER,
  DISPATCH_VERSION,
  DISPATCH_VERSION_HEADER,
  FLOW_JOB_NAME,
  RUNTIME_SECRET_HEADER,
  runQueueName,
} from "./dispatch-contract.js";
import { inflightDeliveries } from "./inflight.js";
import { createQueue } from "./queue.js";

const TENANT = "prj_port_extra_queue";
const DEPLOYMENT = "dep_port_extra_queue";
const SECRET = "s3cret";

const createdQueues: Array<ReturnType<typeof createQueue>> = [];

vi.mock("graphile-worker", () => ({
  Logger: class Logger {
    constructor(_: unknown) {}
  },
  makeWorkerUtils: vi.fn(),
}));

vi.mock("@workflow/world-local", async (importOriginal) => {
  const actual = await importOriginal<typeof import("@workflow/world-local")>();

  return {
    ...actual,
    createWorld: vi.fn(actual.createWorld),
  };
});

describe("postgres queue", () => {
  const workerUtilsMock = {
    addJob: vi.fn(),
    migrate: vi.fn(),
    release: vi.fn(),
  } as unknown as WorkerUtils;
  const wrappedHandler = vi.fn(async () => Response.json({ ok: true }));
  const localWorldClose = vi.fn();
  const createQueueHandler = vi.fn(() => wrappedHandler);
  // `start()` takes a dedicated client for the advisory lock that guards
  // graphile's non-race-safe installSchema, so the pool stub needs `connect`
  // as well as `query`.
  const pool = {
    // The catch-all "exists: false" the schema probes expect.
    query: vi.fn(async () => ({ rows: [{ exists: false }] })),
    connect: vi.fn(async () => ({
      query: vi.fn(async () => ({ rows: [] })),
      release: vi.fn(),
    })),
  } as any;

  beforeEach(() => {
    vi.clearAllMocks();
    vi.mocked(makeWorkerUtils).mockResolvedValue(workerUtilsMock);
    vi.mocked(createWorld).mockReturnValue({
      createQueueHandler,
      close: localWorldClose,
    } as any);
  });

  afterEach(async () => {
    await Promise.all(createdQueues.splice(0).map((queue) => queue.close()));
    vi.useRealTimers();
    delete process.env.WORKFLOW_WORLD_RUNTIME_SECRET;
  });

  it("queues producer delays and headers in graphile job metadata", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2024-01-01T00:00:00.000Z"));

    try {
      const queue = buildQueue(buildConfig(), pool);
      await queue.start();

      await queue.queue(
        "__wkf_workflow_test-step",
        {
          runId: "run_01ABC",
          stepId: "step_01ABC",
          stepName: "test-step",
        },
        {
          delaySeconds: 5,
          headers: { traceparent: "trace-parent" },
          idempotencyKey: "step_01ABC",
        },
      );

      expect(workerUtilsMock.addJob).toHaveBeenCalledWith(
        FLOW_JOB_NAME,
        expect.objectContaining({
          attempt: 1,
          headers: { traceparent: "trace-parent" },
          id: "test-step",
          idempotencyKey: "step_01ABC",
          // Fork additions. The dispatcher reads both straight off the job, so
          // a message that omits them is undeliverable.
          tenantId: TENANT,
          deploymentId: DEPLOYMENT,
        }),
        expect.objectContaining({
          jobKey: "step_01ABC",
          maxAttempts: 49,
          runAt: new Date("2024-01-01T00:00:05.000Z"),
          // Fork additions: per-run serialization, and the fairness flag the
          // dispatcher's `forbiddenFlags` callback throttles on.
          queueName: runQueueName(TENANT, "run_01ABC"),
          flags: [`project:${TENANT}`],
        }),
      );
    } finally {
      vi.useRealTimers();
    }
  });

  it("keeps a spec 9 run's step messages out of its run queue and deduplicates them by key", async () => {
    const queue = buildQueue(buildConfig(), pool);
    await queue.start();

    await queue.queue(
      "__wkf_workflow_greet",
      { runId: "wrun_SPEC9", stepId: "step_1", runSpecVersion: 9 },
      { idempotencyKey: "step-key" },
    );

    const options = vi.mocked(workerUtilsMock.addJob).mock.calls.at(-1)?.[2];
    expect(options).not.toHaveProperty("queueName");
    expect(options).toEqual(
      expect.objectContaining({ jobKey: "step-key", jobKeyMode: "unsafe_dedupe" }),
    );
  });

  it("serializes a spec 9 run's orchestrator messages, a hook wake included, in its run queue", async () => {
    const queue = buildQueue(buildConfig(), pool);
    await queue.start();

    await queue.queue("__wkf_workflow_greet", {
      runId: "wrun_SPEC9",
      runSpecVersion: 9,
      hookResumeTiming: { resumeRequestedAtMs: 1, queuePublishRequestedAtMs: 2 },
    });

    const options = vi.mocked(workerUtilsMock.addJob).mock.calls.at(-1)?.[2];
    expect(options).toEqual(
      expect.objectContaining({
        queueName: runQueueName(TENANT, "wrun_SPEC9"),
        flags: [`project:${TENANT}`],
      }),
    );
  });

  it("addresses a cross-deployment start to the target, not to the deployment that enqueued it", async () => {
    // `start(workflow, args, { deploymentId })` -- what eve's deployment
    // handoff does -- writes `run_created` and this queue message in parallel.
    // Until the run row exists the dispatcher routes on the message's hint, so
    // a hint naming the enqueuer would deliver the new run's first invocation
    // to the very deployment it is leaving.
    const queue = buildQueue(buildConfig(), pool);
    await queue.start();

    await queue.queue(
      "__wkf_workflow_test-workflow",
      { runId: "wrun_01TARGET" },
      { deploymentId: "dep_handoff_target" },
    );

    expect(workerUtilsMock.addJob).toHaveBeenCalledWith(
      FLOW_JOB_NAME,
      expect.objectContaining({ tenantId: TENANT, deploymentId: "dep_handoff_target" }),
      expect.anything(),
    );
  });

  it("keeps its own deployment as the hint when the producer names none", async () => {
    const queue = buildQueue(buildConfig(), pool);
    await queue.start();

    for (const opts of [undefined, {}, { deploymentId: "" }, { deploymentId: "latest" }]) {
      vi.mocked(workerUtilsMock.addJob).mockClear();
      await queue.queue("__wkf_workflow_test-workflow", { runId: "wrun_01OWN" }, opts);
      expect(workerUtilsMock.addJob).toHaveBeenCalledWith(
        FLOW_JOB_NAME,
        expect.objectContaining({ deploymentId: DEPLOYMENT }),
        expect.anything(),
      );
    }
  });

  it("never writes a timer from a deployment", async () => {
    // Only the dispatcher creates timers: a deployment's world is baked
    // at build time, and a job only a newer dispatcher could claim would wait
    // on that upgrade.
    const queue = buildQueue(buildConfig(), pool);
    await queue.start();

    await queue.queue(
      "__wkf_workflow_test-workflow",
      { runId: "wrun_01SLEEP" },
      { delaySeconds: 82_800 },
    );

    expect(workerUtilsMock.addJob).toHaveBeenCalledWith(
      FLOW_JOB_NAME,
      expect.anything(),
      expect.objectContaining({ queueName: runQueueName(TENANT, "wrun_01SLEEP") }),
    );
  });

  it("leaves the flow route to the dispatch check and never writes a queue auth key", async () => {
    const queue = buildQueue(buildConfig(), pool);

    // The runtime's own delivery check stays off: platform dispatch carries
    // the runtime secret instead, checked by `createQueueHandler`.
    expect(queue.queueAuthMode).toBe("skip");
    // The internal Local World only serves the handler, so it must not
    // create a key file (or the data directory it lives in).
    expect(createWorld).toHaveBeenCalledWith(expect.objectContaining({ queueAuthKey: "read" }));
  });

  it("refuses a request that cannot prove itself as platform dispatch before eve's handler runs", async () => {
    const queue = buildQueue(buildConfig(), pool);
    const handler = queue.createQueueHandler(getQueueTopicPrefix("workflow"), async () => {});

    // Nothing but the dispatcher delivers here, so no header is no dispatch.
    expect((await handler(dispatchRequest({}))).status).toBe(401);

    const newerDispatcher = await handler(
      dispatchRequest({ [DISPATCH_VERSION_HEADER]: String(DISPATCH_VERSION + 1) }),
    );
    expect(newerDispatcher.status).toBe(400);

    // Declares dispatch but the deployment holds no secret to check it against.
    const unsigned = await handler(signedRequest({ [RUNTIME_SECRET_HEADER]: "" }));
    expect(unsigned.status).toBe(401);

    process.env.WORKFLOW_WORLD_RUNTIME_SECRET = SECRET;
    const wrongSecret = await handler(signedRequest({ [RUNTIME_SECRET_HEADER]: "nope" }));
    expect(wrongSecret.status).toBe(401);

    // The secret is shared platform-wide, so a captured request must not be
    // replayable against a different deployment on the same host, and a
    // request that names no target is not bound to this one.
    const otherDeployment = await handler(
      signedRequest({ [DEPLOYMENT_HEADER]: "dep_someone_else" }),
    );
    expect(otherDeployment.status).toBe(401);
    const untargeted = new Headers(signedRequest({}).headers);
    untargeted.delete(DEPLOYMENT_HEADER);
    expect((await handler(dispatchRequest(Object.fromEntries(untargeted)))).status).toBe(401);

    expect(wrappedHandler).not.toHaveBeenCalled();

    expect((await handler(signedRequest({}))).status).toBe(200);
    expect(wrappedHandler).toHaveBeenCalledTimes(1);
  });

  it("counts a delivery as in flight until the step it carries is done", async () => {
    // A host drains an executor by waiting for this to reach zero before it
    // sends the signal; a delivery cut off mid-step is run again, side effects
    // included. A refused request is not a step and must not hold a drain up.
    process.env.WORKFLOW_WORLD_RUNTIME_SECRET = SECRET;
    const queue = buildQueue(buildConfig(), pool);
    const handler = queue.createQueueHandler(getQueueTopicPrefix("workflow"), async () => {});
    let finish: (response: Response) => void = () => {};
    wrappedHandler.mockImplementationOnce(
      () => new Promise<Response>((resolve) => (finish = resolve)),
    );

    expect(inflightDeliveries()).toBe(0);
    const delivery = handler(signedRequest({}));
    await vi.waitFor(() => expect(inflightDeliveries()).toBe(1));

    const refused = await handler(
      signedRequest({ [DISPATCH_VERSION_HEADER]: String(DISPATCH_VERSION + 1) }),
    );
    expect(refused.status).toBe(400);
    expect(inflightDeliveries()).toBe(1);

    finish(Response.json({ ok: true }));
    await delivery;
    expect(inflightDeliveries()).toBe(0);

    wrappedHandler.mockImplementationOnce(() => Promise.reject(new Error("step threw")));
    await expect(handler(signedRequest({}))).rejects.toThrow("step threw");
    expect(inflightDeliveries()).toBe(0);
  });
});

function buildConfig(overrides: Partial<ResolvedWorldConfig> = {}): ResolvedWorldConfig {
  return {
    tenantId: TENANT,
    deploymentId: DEPLOYMENT,
    compactStreamSnapshots: true,
    ...overrides,
  };
}

function buildQueue(config: ResolvedWorldConfig, pgPool: Parameters<typeof createQueue>[1]) {
  const queue = createQueue(config, pgPool);
  createdQueues.push(queue);
  return queue;
}

/** What the dispatcher sends this deployment, with `overrides` applied. */
function signedRequest(overrides: Record<string, string>): Request {
  return dispatchRequest({
    [DISPATCH_VERSION_HEADER]: String(DISPATCH_VERSION),
    [RUNTIME_SECRET_HEADER]: SECRET,
    [DEPLOYMENT_HEADER]: DEPLOYMENT,
    ...overrides,
  });
}

function dispatchRequest(headers: Record<string, string>): Request {
  return new Request("http://localhost/.well-known/workflow/v1/flow", {
    method: "POST",
    headers,
  });
}
