/**
 * Ported from `@workflow/world-postgres`'s `queue.test.ts`.
 *
 * The graphile mapping under test is upstream's, so the assertions are
 * upstream's wherever the fork still behaves the same way. Three groups of
 * upstream tests do not survive the fork and were dropped rather than faked:
 *
 *   * the `applicationManagedShutdown` / `noHandleSignals` pair — the fork has
 *     no such config knob, so the assertions would only be checking that an
 *     option nobody can set is absent;
 *   * the `namespace` variants — the fork always uses the unnamespaced topic
 *     prefix, so a namespaced queue name would not round-trip.
 *
 * What is added on top of the port is the fork's own contract: the per-tenant
 * job name, the per-run graphile `queueName`, the reschedule hop, and the
 * dispatch-contract wrapper around eve's queue handler.
 */
import { createServer, type Server } from "node:http";
import { setWorkflowBasePath } from "@workflow/utils";
import { getWorkflowPort } from "@workflow/utils/get-port";
import { getQueueTopicPrefix, MessageId, parseQueueName, type QueuePayload } from "@workflow/world";
import { nodeHttpFetch } from "@workflow/world/node-http.js";
import { createWorld } from "@workflow/world-local";
import { makeWorkerUtils, type Runner, run, type WorkerUtils } from "graphile-worker";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { ResolvedWorldConfig } from "./config.js";
import {
  DEPLOYMENT_HEADER,
  DISPATCH_VERSION,
  DISPATCH_VERSION_HEADER,
  FLOW_JOB_NAME,
  RUNTIME_SECRET_HEADER,
  runQueueName,
  TIMER_JOB_NAME,
} from "./dispatch-contract.js";
import { inflightDeliveries } from "./inflight.js";
import { MessageData } from "./message.js";
import { createQueue } from "./queue.js";
import { derivePartitionName } from "./tenant.js";

const TENANT = "prj_port_extra_queue";
const DEPLOYMENT = "dep_port_extra_queue";
/** What `getJobQueueName()` derives in embedded mode: shared name + tenant suffix. */
const EMBEDDED_JOB_NAME = derivePartitionName(FLOW_JOB_NAME, TENANT);
const EMBEDDED_TIMER_JOB_NAME = derivePartitionName(TIMER_JOB_NAME, TENANT);

type RecordedRequest = {
  method: string | undefined;
  url: string | undefined;
  headers: Record<string, string | string[] | undefined>;
  body: string;
};

const createdQueues: Array<ReturnType<typeof createQueue>> = [];
const createdServers: Server[] = [];

// Deliveries go over `nodeHttpFetch`, not the global `fetch` (see queue.ts).
// The pool helpers stay real; only the request itself is replaced per test.
vi.mock("@workflow/world/node-http.js", async (importOriginal) => ({
  ...(await importOriginal<typeof import("@workflow/world/node-http.js")>()),
  nodeHttpFetch: vi.fn(),
}));

vi.mock("graphile-worker", () => ({
  Logger: class Logger {
    constructor(_: unknown) {}
  },
  makeWorkerUtils: vi.fn(),
  run: vi.fn(),
}));

vi.mock("@workflow/utils/get-port", () => ({
  getWorkflowPort: vi.fn(),
}));

vi.mock("@workflow/world-local", async (importOriginal) => {
  const actual = await importOriginal<typeof import("@workflow/world-local")>();

  return {
    ...actual,
    createWorld: vi.fn(actual.createWorld),
  };
});

describe("postgres queue http execution", () => {
  const workerUtilsMock = {
    addJob: vi.fn(),
    migrate: vi.fn(),
    release: vi.fn(),
  } as unknown as WorkerUtils;
  const runnerMock = {
    stop: vi.fn(),
    promise: Promise.resolve(),
  };
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

  beforeEach(async () => {
    vi.clearAllMocks();
    // Real transport unless a test replaces it: several tests deliver to a
    // local server.
    const actual = await vi.importActual<typeof import("@workflow/world/node-http.js")>(
      "@workflow/world/node-http.js",
    );
    vi.mocked(nodeHttpFetch).mockImplementation(actual.nodeHttpFetch);

    vi.mocked(makeWorkerUtils).mockResolvedValue(workerUtilsMock);
    vi.mocked(getWorkflowPort).mockResolvedValue(undefined);
    vi.mocked(run).mockResolvedValue(runnerMock as unknown as Runner);
    vi.mocked(createWorld).mockReturnValue({
      createQueueHandler,
      close: localWorldClose,
    } as any);
  });

  afterEach(async () => {
    await Promise.all(createdQueues.splice(0).map((queue) => queue.close()));
    await Promise.all(
      createdServers.splice(0).map(
        (server) =>
          new Promise<void>((resolve, reject) => {
            server.close((err) => (err ? reject(err) : resolve()));
            server.closeAllConnections();
          }),
      ),
    );
    vi.useRealTimers();
    delete process.env.WORKFLOW_LOCAL_BASE_URL;
    delete process.env.PORT;
    delete process.env.WORKFLOW_WORLD_RUNTIME_SECRET;
    setWorkflowBasePath(undefined);
  });

  it("uses a late-detected local port when the queue starts before PORT is available", async () => {
    const requests: RecordedRequest[] = [];
    const port = await getUnusedLoopbackPort();
    vi.mocked(getWorkflowPort).mockResolvedValue(port);

    const queue = buildQueue(buildConfig(), pool);
    await queue.start();

    expect(run).not.toHaveBeenCalled();

    await startWorkflowHttpServer(requests, port);
    await vi.waitFor(() => {
      expect(run).toHaveBeenCalledTimes(1);
    });

    const task = getTaskHandler(EMBEDDED_JOB_NAME);
    const message = {
      runId: "run_01ABC",
      stepId: "step_01ABC",
      stepName: "test-step",
    } satisfies QueuePayload;
    const payload = buildMessageData("__wkf_workflow_test-step", message, {
      headers: { traceparent: "trace-parent" },
      idempotencyKey: "step_01ABC",
    });

    await expect(task(payload, {} as any)).resolves.toBeUndefined();

    expect(getWorkflowPort).toHaveBeenCalled();
    expect(requests).toEqual([
      expect.objectContaining({
        method: "POST",
        url: "/.well-known/workflow/v1/flow",
      }),
    ]);
  });

  it("keeps the base-url error when env vars and local port detection cannot resolve a target", async () => {
    const queue = buildQueue(buildConfig(), pool);
    await queue.start();

    const task = getTaskHandler(EMBEDDED_JOB_NAME);
    const message = {
      runId: "run_01ABC",
      stepId: "step_01ABC",
      stepName: "test-step",
    } satisfies QueuePayload;
    const payload = buildMessageData("__wkf_workflow_test-step", message, {
      idempotencyKey: "step_01ABC",
    });

    await expect(task(payload, {} as any)).rejects.toThrow(
      "Unable to resolve base URL for workflow queue.",
    );

    expect(getWorkflowPort).toHaveBeenCalled();
  });

  it("serializes workflow queue execution for the same runId", async () => {
    let resolveFirstRequestStarted!: () => void;
    const firstRequestStarted = new Promise<void>((resolve) => {
      resolveFirstRequestStarted = resolve;
    });
    let resolveReleaseFirstRequest!: () => void;
    const releaseFirstRequest = new Promise<void>((resolve) => {
      resolveReleaseFirstRequest = resolve;
    });
    let requestCount = 0;
    let activeRequests = 0;
    let maxActiveRequests = 0;
    const fetchMock = vi.fn(async () => {
      requestCount += 1;
      activeRequests += 1;
      maxActiveRequests = Math.max(maxActiveRequests, activeRequests);

      if (requestCount === 1) {
        resolveFirstRequestStarted();
        await releaseFirstRequest;
      }

      activeRequests -= 1;
      return Response.json({ ok: true });
    });
    vi.mocked(nodeHttpFetch).mockImplementation(fetchMock as any);
    process.env.WORKFLOW_LOCAL_BASE_URL = "https://workflow.example.test";

    const queue = buildQueue(buildConfig(), pool);
    try {
      await queue.start();

      const task = getTaskHandler(EMBEDDED_JOB_NAME);
      const payload = {
        runId: "wrun_01ABC",
      };
      const firstExecution = task(
        buildMessageData("__wkf_workflow_test-workflow", payload, {
          messageId: MessageId.parse("msg_01ABC"),
        }),
        {} as any,
      );
      const secondExecution = task(
        buildMessageData("__wkf_workflow_test-workflow", payload, {
          messageId: MessageId.parse("msg_01ABD"),
        }),
        {} as any,
      );

      await firstRequestStarted;
      await Promise.resolve();
      expect(requestCount).toBe(1);
      expect(maxActiveRequests).toBe(1);

      resolveReleaseFirstRequest();
      await Promise.all([firstExecution, secondExecution]);

      expect(requestCount).toBe(2);
      expect(maxActiveRequests).toBe(1);
      expect(fetchMock).toHaveBeenCalledTimes(2);
    } finally {
      vi.unstubAllGlobals();
    }
  });

  it("does not require a runId for workflow health-check payloads", async () => {
    const fetchMock = vi.fn(async () => Response.json({ ok: true }));
    vi.mocked(nodeHttpFetch).mockImplementation(fetchMock as any);
    process.env.WORKFLOW_LOCAL_BASE_URL = "https://workflow.example.test";

    const queue = buildQueue(buildConfig(), pool);
    try {
      await queue.start();

      const task = getTaskHandler(EMBEDDED_JOB_NAME);
      const payload = buildMessageData("__wkf_workflow_health_check", {
        __healthCheck: true,
        correlationId: "hc_01ABC",
      });

      await expect(task(payload, {} as any)).resolves.toBeUndefined();

      expect(fetchMock).toHaveBeenCalledWith(
        "https://workflow.example.test/.well-known/workflow/v1/flow",
        expect.objectContaining({ method: "POST" }),
      );
      const [, init] = fetchMock.mock.calls[0] as unknown as [string, { headers: Headers }];
      expect(init.headers.get("x-vqs-queue-name")).toBe("__wkf_workflow_health_check");
    } finally {
      vi.unstubAllGlobals();
    }
  });

  it("uses basePath for local postgres queue HTTP delivery", async () => {
    const fetchMock = vi.fn(async () => Response.json({ ok: true }));
    vi.mocked(nodeHttpFetch).mockImplementation(fetchMock as any);
    const port = await getUnusedLoopbackPort();
    await startWorkflowHttpServer([], port);
    process.env.PORT = String(port);
    setWorkflowBasePath("/v2");

    const queue = buildQueue(buildConfig(), pool);
    try {
      await queue.start();

      const task = getTaskHandler(EMBEDDED_JOB_NAME);
      const payload = buildMessageData("__wkf_workflow_test-step", {
        runId: "run_01ABC",
        stepId: "step_01ABC",
        stepName: "test-step",
      });

      await expect(task(payload, {} as any)).resolves.toBeUndefined();

      expect(fetchMock).toHaveBeenCalledWith(
        `http://localhost:${String(port)}/v2/.well-known/workflow/v1/flow`,
        expect.objectContaining({ method: "POST" }),
      );
      expect(getWorkflowPort).not.toHaveBeenCalled();
    } finally {
      vi.unstubAllGlobals();
    }
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
        EMBEDDED_JOB_NAME,
        expect.objectContaining({
          attempt: 1,
          headers: { traceparent: "trace-parent" },
          id: "test-step",
          idempotencyKey: "step_01ABC",
          // Fork additions. The dispatcher reads both straight off the job, so
          // a message that omits them is undeliverable in external mode.
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

  it("enqueues a hook wake past its run's queue while a delivery holds it", async () => {
    const queue = buildQueue(buildConfig(), pool);
    await queue.start();
    vi.mocked(pool.query).mockResolvedValueOnce({ rows: [{}] } as never);

    await queue.queue("__wkf_workflow_greet", {
      runId: "wrun_01ABC",
      hookResumeTiming: { resumeRequestedAtMs: 1, queuePublishRequestedAtMs: 2 },
    });

    expect(vi.mocked(pool.query).mock.calls.at(-1)?.[1]).toEqual([
      runQueueName(TENANT, "wrun_01ABC"),
    ]);
    const options = vi.mocked(workerUtilsMock.addJob).mock.calls.at(-1)?.[2];
    expect(options).not.toHaveProperty("queueName");
    // The delivery it interrupts already holds a slot of the tenant's cap.
    expect(options).toEqual(expect.objectContaining({ flags: [`wake:${TENANT}`] }));
    // One pending bypassing wake per run: a later one replaces it.
    expect(options).toEqual(expect.objectContaining({ jobKey: `wfwake:${TENANT}:wrun_01ABC` }));
  });

  it("keys a bypassing wake by its producer's idempotency key or its own input when it has one", async () => {
    const queue = buildQueue(buildConfig(), pool);
    await queue.start();
    const hookResumeTiming = { resumeRequestedAtMs: 1, queuePublishRequestedAtMs: 2 };

    vi.mocked(pool.query).mockResolvedValueOnce({ rows: [{}] } as never);
    await queue.queue(
      "__wkf_workflow_greet",
      { runId: "wrun_01ABC", hookResumeTiming },
      { idempotencyKey: "hook-res_1" },
    );
    expect(vi.mocked(workerUtilsMock.addJob).mock.calls.at(-1)?.[2]).toEqual(
      expect.objectContaining({ jobKey: "hook-res_1" }),
    );

    vi.mocked(pool.query).mockResolvedValueOnce({ rows: [{}] } as never);
    const { messageId } = await queue.queue("__wkf_workflow_greet", {
      runId: "wrun_01ABC",
      hookInput: {
        resumeId: "res_2",
        hookId: "hook_1",
        token: "tok_1",
        payload: null,
        payloadDigest: "d",
      },
    });
    expect(vi.mocked(workerUtilsMock.addJob).mock.calls.at(-1)?.[2]).toEqual(
      expect.objectContaining({ jobKey: messageId }),
    );
  });

  it("queues a hook wake as usual when the run queue's lock cannot be read", async () => {
    const queue = buildQueue(buildConfig(), pool);
    await queue.start();
    vi.mocked(pool.query).mockRejectedValueOnce(new Error("relation does not exist"));
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});

    await queue.queue("__wkf_workflow_greet", {
      runId: "wrun_01ABC",
      hookResumeTiming: { resumeRequestedAtMs: 1, queuePublishRequestedAtMs: 2 },
    });

    expect(vi.mocked(workerUtilsMock.addJob).mock.calls.at(-1)?.[2]).toEqual(
      expect.objectContaining({
        queueName: runQueueName(TENANT, "wrun_01ABC"),
        flags: [`project:${TENANT}`],
      }),
    );
    expect(warn).toHaveBeenCalled();
    warn.mockRestore();
  });

  it("queues a delayed hook wake as usual, so it cannot push back a pending one", async () => {
    const queue = buildQueue(buildConfig(), pool);
    await queue.start();
    const lookups = vi.mocked(pool.query).mock.calls.length;

    await queue.queue(
      "__wkf_workflow_greet",
      {
        runId: "wrun_01ABC",
        hookResumeTiming: { resumeRequestedAtMs: 1, queuePublishRequestedAtMs: 2 },
      },
      { delaySeconds: 5 },
    );

    expect(vi.mocked(pool.query).mock.calls.length).toBe(lookups);
    expect(vi.mocked(workerUtilsMock.addJob).mock.calls.at(-1)?.[2]).toEqual(
      expect.objectContaining({
        queueName: runQueueName(TENANT, "wrun_01ABC"),
        flags: [`project:${TENANT}`],
      }),
    );
  });

  it("does not look up the run queue for a message that is not a hook wake", async () => {
    const queue = buildQueue(buildConfig(), pool);
    await queue.start();
    const lookups = vi.mocked(pool.query).mock.calls.length;

    await queue.queue("__wkf_workflow_greet", { runId: "wrun_01ABC" });

    expect(vi.mocked(pool.query).mock.calls.length).toBe(lookups);
    expect(vi.mocked(workerUtilsMock.addJob).mock.calls.at(-1)?.[2]).toEqual(
      expect.objectContaining({ queueName: runQueueName(TENANT, "wrun_01ABC") }),
    );
  });

  it("delivers a hook wake while a replay of its run is still running in this process", async () => {
    let finishReplay = (): void => {};
    const fetchMock = vi
      .fn()
      .mockImplementationOnce(
        () =>
          new Promise<Response>((resolve) => {
            finishReplay = () => resolve(Response.json({ ok: true }));
          }),
      )
      .mockImplementation(async () => Response.json({ ok: true }));
    vi.mocked(nodeHttpFetch).mockImplementation(fetchMock as any);
    const port = await getUnusedLoopbackPort();
    await startWorkflowHttpServer([], port);
    process.env.PORT = String(port);

    const queue = buildQueue(buildConfig(), pool);
    await queue.start();
    const task = getTaskHandler(EMBEDDED_JOB_NAME);
    const replay = buildMessageData("__wkf_workflow_greet", { runId: "wrun_01ABC" });
    const duplicate = buildMessageData("__wkf_workflow_greet", { runId: "wrun_01ABC" });
    const wake = buildMessageData("__wkf_workflow_greet", {
      runId: "wrun_01ABC",
      hookResumeTiming: { resumeRequestedAtMs: 1, queuePublishRequestedAtMs: 2 },
    });

    const running = task(replay, {} as any);
    await vi.waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(1));
    const waiting = task(duplicate, {} as any);
    await expect(task(wake, {} as any)).resolves.toBeUndefined();
    // The wake got through; the duplicate replay still waits for the running one.
    expect(fetchMock).toHaveBeenCalledTimes(2);

    finishReplay();
    await Promise.all([running, waiting]);
    expect(fetchMock).toHaveBeenCalledTimes(3);
  });

  it("enqueues a hook wake for an idle run in its run's queue", async () => {
    const queue = buildQueue(buildConfig(), pool);
    await queue.start();
    vi.mocked(pool.query).mockResolvedValueOnce({ rows: [] } as never);

    await queue.queue("__wkf_workflow_greet", {
      runId: "wrun_01ABC",
      hookResumeTiming: { resumeRequestedAtMs: 1, queuePublishRequestedAtMs: 2 },
    });

    expect(vi.mocked(workerUtilsMock.addJob).mock.calls.at(-1)?.[2]).toEqual(
      expect.objectContaining({ queueName: runQueueName(TENANT, "wrun_01ABC") }),
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
      EMBEDDED_JOB_NAME,
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
        EMBEDDED_JOB_NAME,
        expect.objectContaining({ deploymentId: DEPLOYMENT }),
        expect.anything(),
      );
    }
  });

  it("does not deliver through the global fetch, whose undici deadlines cannot be lifted", async () => {
    // A delivery runs the workflow body inline, so headers arrive only when the
    // work is done; undici's fixed 300s headers deadline would redeliver a
    // slow-but-healthy step while the original is still running.
    vi.stubGlobal("fetch", () => Promise.reject(new TypeError("fetch failed")));
    const requests: RecordedRequest[] = [];
    const server = createServer((req, res) => {
      const chunks: Buffer[] = [];
      req.on("data", (chunk: Buffer) => chunks.push(chunk));
      req.on("end", () => {
        requests.push({
          method: req.method,
          url: req.url,
          headers: req.headers,
          body: Buffer.concat(chunks).toString("utf8"),
        });
        res.writeHead(200, { "content-type": "application/json" }).end('{"ok":true}');
      });
    });
    createdServers.push(server);
    await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
    const { port } = server.address() as { port: number };
    process.env.WORKFLOW_LOCAL_BASE_URL = `http://127.0.0.1:${String(port)}`;

    const queue = buildQueue(buildConfig(), pool);
    try {
      await queue.start();
      const task = getTaskHandler(EMBEDDED_JOB_NAME);
      await task(buildMessageData("__wkf_workflow_test-workflow", { runId: "wrun_01ABC" }), {
        job: { attempts: 1 },
      });

      expect(requests).toHaveLength(1);
      expect(requests[0]).toMatchObject({ method: "POST", url: "/.well-known/workflow/v1/flow" });
    } finally {
      vi.unstubAllGlobals();
    }
  });

  it("passes graphile's abortSignal to the HTTP delivery", async () => {
    const fetchMock = vi.fn(async () => Response.json({ ok: true }));
    vi.mocked(nodeHttpFetch).mockImplementation(fetchMock as any);
    process.env.WORKFLOW_LOCAL_BASE_URL = "https://workflow.example.test";
    const controller = new AbortController();

    const queue = buildQueue(buildConfig(), pool);
    try {
      await queue.start();
      const task = getTaskHandler(EMBEDDED_JOB_NAME);
      const payload = buildMessageData("__wkf_workflow_test-workflow", {
        runId: "wrun_01ABC",
      });

      await task(payload, { abortSignal: controller.signal, job: { attempts: 1 } });

      expect(fetchMock).toHaveBeenCalledWith(
        "https://workflow.example.test/.well-known/workflow/v1/flow",
        expect.objectContaining({ signal: controller.signal }),
      );
    } finally {
      vi.unstubAllGlobals();
    }
  });

  it("reschedules a timed-out delivery under the same job key before returning", async () => {
    // The executor answering with `timeoutSeconds` means "I am not done, wake me
    // later". The follow-up job must be enqueued before the handler returns, or
    // a crash between the two loses the wake-up entirely.
    const fetchMock = vi.fn(async () => Response.json({ timeoutSeconds: 30 }));
    vi.mocked(nodeHttpFetch).mockImplementation(fetchMock as any);
    process.env.WORKFLOW_LOCAL_BASE_URL = "https://workflow.example.test";
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2024-01-01T00:00:00.000Z"));

    const queue = buildQueue(buildConfig(), pool);
    try {
      await queue.start();

      const task = getTaskHandler(EMBEDDED_JOB_NAME);
      const payload = buildMessageData(
        "__wkf_workflow_test-workflow",
        { runId: "wrun_01ABC" },
        { idempotencyKey: "wrun_01ABC" },
      );

      // graphile's own attempt counter wins over the one in the message body,
      // so the rescheduled job continues that sequence rather than restarting it.
      await expect(task(payload, { job: { attempts: 2 } })).resolves.toBeUndefined();

      expect(workerUtilsMock.addJob).toHaveBeenCalledWith(
        EMBEDDED_JOB_NAME,
        expect.objectContaining({ attempt: 3, idempotencyKey: "wrun_01ABC" }),
        expect.objectContaining({
          jobKey: "wrun_01ABC",
          runAt: new Date("2024-01-01T00:00:30.000Z"),
          queueName: runQueueName(TENANT, "wrun_01ABC"),
        }),
      );
    } finally {
      vi.unstubAllGlobals();
      vi.useRealTimers();
    }
  });

  it("holds a far-off embedded delivery as a timer instead of on the run's queue", async () => {
    // graphile locks every queue that holds a job on every claim, due or not
    // (evelandhq/workflow-world#115), so a sleep must not keep its run's queue.
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2024-01-01T00:00:00.000Z"));
    try {
      const queue = buildQueue(buildConfig(), pool);
      await queue.start();

      await queue.queue(
        "__wkf_workflow_test-workflow",
        { runId: "wrun_01SLEEP" },
        { delaySeconds: 82_800, idempotencyKey: "wait_01:hop-7" },
      );

      expect(workerUtilsMock.addJob).toHaveBeenCalledWith(
        EMBEDDED_TIMER_JOB_NAME,
        expect.objectContaining({
          idempotencyKey: "wait_01:hop-7",
          runQueue: runQueueName(TENANT, "wrun_01SLEEP"),
        }),
        {
          jobKey: "wait_01:hop-7",
          runAt: new Date("2024-01-01T23:00:00.000Z"),
          maxAttempts: 49,
        },
      );
    } finally {
      vi.useRealTimers();
    }
  });

  it("puts a due embedded timer back on its run's queue under the same key", async () => {
    const queue = buildQueue(buildConfig(), pool);
    await queue.start();

    const timer = getTaskHandler(EMBEDDED_TIMER_JOB_NAME);
    await timer(
      {
        ...buildMessageData(
          "__wkf_workflow_test-workflow",
          { runId: "wrun_01SLEEP" },
          { idempotencyKey: "wait_01:hop-7" },
        ),
        runQueue: runQueueName(TENANT, "wrun_01SLEEP"),
      },
      { job: { attempts: 1, key: "wait_01:hop-7" } },
    );

    expect(workerUtilsMock.addJob).toHaveBeenCalledWith(
      EMBEDDED_JOB_NAME,
      expect.not.objectContaining({ runQueue: expect.anything() }),
      {
        jobKey: "wait_01:hop-7",
        queueName: runQueueName(TENANT, "wrun_01SLEEP"),
        maxAttempts: 49,
        flags: [`project:${TENANT}`],
      },
    );
  });

  it("never writes a timer from a deployment in external mode", async () => {
    // Only the dispatcher creates timers there: a deployment's world is baked
    // at build time, and a job only a newer dispatcher could claim would wait
    // on that upgrade.
    const queue = buildQueue(buildConfig({ runner: "external" }), pool);
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

  it("reschedules a hook wake into its run's queue under the tenant's delivery cap", async () => {
    const fetchMock = vi.fn(async () => Response.json({ timeoutSeconds: 30 }));
    vi.mocked(nodeHttpFetch).mockImplementation(fetchMock as any);
    process.env.WORKFLOW_LOCAL_BASE_URL = "https://workflow.example.test";

    const queue = buildQueue(buildConfig(), pool);
    try {
      await queue.start();
      const task = getTaskHandler(EMBEDDED_JOB_NAME);
      const wake = buildMessageData("__wkf_workflow_greet", {
        runId: "wrun_01ABC",
        hookResumeTiming: { resumeRequestedAtMs: 1, queuePublishRequestedAtMs: 2 },
      });

      await expect(task(wake, { job: { attempts: 1 } })).resolves.toBeUndefined();

      expect(vi.mocked(workerUtilsMock.addJob).mock.calls.at(-1)?.[2]).toEqual(
        expect.objectContaining({
          queueName: runQueueName(TENANT, "wrun_01ABC"),
          flags: [`project:${TENANT}`],
        }),
      );
    } finally {
      vi.unstubAllGlobals();
    }
  });

  it("does not re-execute a redelivered message that already completed", async () => {
    // graphile redelivers on its own schedule; the idempotency-key cache is what
    // keeps a completed workflow message from being POSTed to the executor twice.
    const fetchMock = vi.fn(async () => Response.json({ ok: true }));
    vi.mocked(nodeHttpFetch).mockImplementation(fetchMock as any);
    process.env.WORKFLOW_LOCAL_BASE_URL = "https://workflow.example.test";

    const queue = buildQueue(buildConfig(), pool);
    try {
      await queue.start();

      const task = getTaskHandler(EMBEDDED_JOB_NAME);
      const payload = buildMessageData(
        "__wkf_workflow_test-workflow",
        { runId: "wrun_01ABC" },
        { idempotencyKey: "wrun_01ABC" },
      );

      await task(payload, {} as any);
      await task(payload, {} as any);

      expect(fetchMock).toHaveBeenCalledTimes(1);
    } finally {
      vi.unstubAllGlobals();
    }
  });

  it("registers no runner in external mode and enqueues to the shared job name", async () => {
    // The two names must differ: an unsuffixed embedded name on the shared
    // database would let any agent's runner claim any project's jobs, which is
    // the cross-project turn stealing this package exists to stop.
    expect(EMBEDDED_JOB_NAME).not.toBe(FLOW_JOB_NAME);

    const queue = buildQueue(buildConfig({ runner: "external" }), pool);
    await queue.start();

    // No in-process runner: the platform dispatcher claims these jobs, which is
    // what lets a durable timer fire after this process has been idle-reaped.
    expect(run).not.toHaveBeenCalled();

    await queue.queue("__wkf_workflow_test-workflow", { runId: "wrun_01ABC" });

    expect(workerUtilsMock.addJob).toHaveBeenCalledWith(
      FLOW_JOB_NAME,
      expect.objectContaining({ tenantId: TENANT }),
      expect.objectContaining({ flags: [`project:${TENANT}`] }),
    );
  });

  it("rejects platform dispatch that cannot prove itself before eve's handler runs", async () => {
    const queue = buildQueue(buildConfig({ runner: "external" }), pool);
    const handler = queue.createQueueHandler(getQueueTopicPrefix("workflow"), async () => {});

    const newerDispatcher = await handler(
      dispatchRequest({ [DISPATCH_VERSION_HEADER]: String(DISPATCH_VERSION + 1) }),
    );
    expect(newerDispatcher.status).toBe(400);

    // Declares dispatch but the deployment holds no secret to check it against.
    const unsigned = await handler(
      dispatchRequest({ [DISPATCH_VERSION_HEADER]: String(DISPATCH_VERSION) }),
    );
    expect(unsigned.status).toBe(401);

    process.env.WORKFLOW_WORLD_RUNTIME_SECRET = "s3cret";
    const wrongSecret = await handler(
      dispatchRequest({
        [DISPATCH_VERSION_HEADER]: String(DISPATCH_VERSION),
        [RUNTIME_SECRET_HEADER]: "nope",
      }),
    );
    expect(wrongSecret.status).toBe(401);

    // The secret is shared platform-wide, so a captured request must not be
    // replayable against a different deployment on the same host.
    const otherDeployment = await handler(
      dispatchRequest({
        [DISPATCH_VERSION_HEADER]: String(DISPATCH_VERSION),
        [RUNTIME_SECRET_HEADER]: "s3cret",
        [DEPLOYMENT_HEADER]: "dep_someone_else",
      }),
    );
    expect(otherDeployment.status).toBe(401);

    expect(wrappedHandler).not.toHaveBeenCalled();
  });

  it("passes an unheadered loopback request straight to eve's handler", async () => {
    // Embedded mode POSTs to this same route over loopback with none of the
    // Eveland headers, so the check must key off the dispatch header rather than
    // being unconditional.
    const queue = buildQueue(buildConfig(), pool);
    const handler = queue.createQueueHandler(getQueueTopicPrefix("workflow"), async () => {});

    const response = await handler(dispatchRequest({}));

    expect(response.status).toBe(200);
    expect(wrappedHandler).toHaveBeenCalledTimes(1);
  });

  it("counts a delivery as in flight until the step it carries is done", async () => {
    // A host drains an executor by waiting for this to reach zero before it
    // sends the signal; a delivery cut off mid-step is run again, side effects
    // included. A refused request is not a step and must not hold a drain up.
    const queue = buildQueue(buildConfig(), pool);
    const handler = queue.createQueueHandler(getQueueTopicPrefix("workflow"), async () => {});
    let finish: (response: Response) => void = () => {};
    wrappedHandler.mockImplementationOnce(
      () => new Promise<Response>((resolve) => (finish = resolve)),
    );

    expect(inflightDeliveries()).toBe(0);
    const delivery = handler(dispatchRequest({}));
    await vi.waitFor(() => expect(inflightDeliveries()).toBe(1));

    const refused = await handler(
      dispatchRequest({ [DISPATCH_VERSION_HEADER]: String(DISPATCH_VERSION + 1) }),
    );
    expect(refused.status).toBe(400);
    expect(inflightDeliveries()).toBe(1);

    finish(Response.json({ ok: true }));
    await delivery;
    expect(inflightDeliveries()).toBe(0);

    wrappedHandler.mockImplementationOnce(() => Promise.reject(new Error("step threw")));
    await expect(handler(dispatchRequest({}))).rejects.toThrow("step threw");
    expect(inflightDeliveries()).toBe(0);
  });
});

function buildConfig(overrides: Partial<ResolvedWorldConfig> = {}): ResolvedWorldConfig {
  return {
    tenantId: TENANT,
    deploymentId: DEPLOYMENT,
    runner: "embedded",
    queueConcurrency: 50,
    compactStreamSnapshots: true,
    ...overrides,
  };
}

function buildQueue(config: ResolvedWorldConfig, pgPool: Parameters<typeof createQueue>[1]) {
  const queue = createQueue(config, pgPool);
  createdQueues.push(queue);
  return queue;
}

function dispatchRequest(headers: Record<string, string>): Request {
  return new Request("http://localhost/.well-known/workflow/v1/flow", {
    method: "POST",
    headers,
  });
}

function buildMessageData(
  queueName: string,
  payload: QueuePayload,
  opts?: {
    attempt?: number;
    headers?: Record<string, string>;
    idempotencyKey?: string;
    messageId?: MessageId;
  },
) {
  const { id } = parseQueueName(queueName);

  return MessageData.encode({
    id,
    // Upstream builds this with `@vercel/queue`'s `JsonTransport`; the fork
    // carries its own equivalent transport rather than the dependency, and the
    // two agree for payloads with no binary members.
    data: Buffer.from(JSON.stringify(payload)),
    attempt: opts?.attempt ?? 1,
    headers: opts?.headers,
    idempotencyKey: opts?.idempotencyKey,
    messageId: opts?.messageId ?? MessageId.parse("msg_01ABC"),
    tenantId: TENANT,
    deploymentId: DEPLOYMENT,
  });
}

function getTaskHandler(name: string) {
  const taskList = vi.mocked(run).mock.calls[0]?.[0]?.taskList;
  const task = taskList?.[name];
  expect(task).toBeTypeOf("function");
  return task as (payload: unknown, helpers: unknown) => Promise<void>;
}

async function startWorkflowHttpServer(requests: RecordedRequest[], port = 0) {
  const server = createServer(async (req, res) => {
    const body = await new Promise<string>((resolve, reject) => {
      let chunks = "";
      req.setEncoding("utf8");
      req.on("data", (chunk: string) => {
        chunks += chunk;
      });
      req.on("end", () => resolve(chunks));
      req.on("error", reject);
    });

    const request = {
      method: req.method,
      url: req.url,
      headers: req.headers,
      body,
    };
    requests.push(request);

    if (req.method === "POST" && req.url === "/.well-known/workflow/v1/flow") {
      res.writeHead(200, { "content-type": "application/json" });
      res.end(JSON.stringify({ ok: true }));
      return;
    }

    res.writeHead(404, { "content-type": "text/plain" });
    res.end("not found");
  });

  await new Promise<void>((resolve, reject) => {
    server.listen(port, "127.0.0.1", () => resolve());
    server.on("error", reject);
  });

  createdServers.push(server);
  const address = server.address();
  if (!address || typeof address === "string") {
    throw new Error("Failed to determine test server address");
  }

  return {
    baseUrl: `http://127.0.0.1:${String(address.port)}`,
  };
}

async function getUnusedLoopbackPort(): Promise<number> {
  const server = createServer();
  await new Promise<void>((resolve, reject) => {
    server.listen(0, "127.0.0.1", () => resolve());
    server.on("error", reject);
  });
  const address = server.address();
  await new Promise<void>((resolve, reject) => {
    server.close((err) => (err ? reject(err) : resolve()));
  });

  if (!address || typeof address === "string") {
    throw new Error("Failed to reserve a loopback port");
  }

  return address.port;
}
