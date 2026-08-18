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
} from "./dispatch-contract.js";
import { MessageData } from "./message.js";
import { createQueue } from "./queue.js";
import { derivePartitionName } from "./tenant.js";

const TENANT = "prj_port_extra_queue";
const DEPLOYMENT = "dep_port_extra_queue";
/** What `getJobQueueName()` derives in embedded mode: shared name + tenant suffix. */
const EMBEDDED_JOB_NAME = derivePartitionName(FLOW_JOB_NAME, TENANT);

type RecordedRequest = {
  method: string | undefined;
  url: string | undefined;
  headers: Record<string, string | string[] | undefined>;
  body: string;
};

const createdQueues: Array<ReturnType<typeof createQueue>> = [];
const createdServers: Server[] = [];

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
    // No run is quarantined in these tests; everything else keeps the
    // catch-all "exists: false" the schema probes expect.
    query: vi.fn(async (sql: unknown) =>
      /run_quarantines/.test(String(sql)) ? { rows: [] } : { rows: [{ exists: false }] },
    ),
    connect: vi.fn(async () => ({
      query: vi.fn(async () => ({ rows: [] })),
      release: vi.fn(),
    })),
  } as any;

  beforeEach(() => {
    vi.clearAllMocks();

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
    vi.stubGlobal("fetch", fetchMock);
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
    vi.stubGlobal("fetch", fetchMock);
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
        expect.objectContaining({
          method: "POST",
          headers: expect.objectContaining({
            "x-vqs-queue-name": "__wkf_workflow_health_check",
          }),
        }),
      );
    } finally {
      vi.unstubAllGlobals();
    }
  });

  it("uses basePath for local postgres queue HTTP delivery", async () => {
    const fetchMock = vi.fn(async () => Response.json({ ok: true }));
    vi.stubGlobal("fetch", fetchMock);
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

  it("passes graphile's abortSignal to the HTTP delivery", async () => {
    const fetchMock = vi.fn(async () => Response.json({ ok: true }));
    vi.stubGlobal("fetch", fetchMock);
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
    vi.stubGlobal("fetch", fetchMock);
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

  it("does not re-execute a redelivered message that already completed", async () => {
    // graphile redelivers on its own schedule; the idempotency-key cache is what
    // keeps a completed workflow message from being POSTed to the executor twice.
    const fetchMock = vi.fn(async () => Response.json({ ok: true }));
    vi.stubGlobal("fetch", fetchMock);
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
