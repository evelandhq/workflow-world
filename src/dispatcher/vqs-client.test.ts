import { createServer, type IncomingMessage, type Server, type ServerResponse } from "node:http";
import type { AddressInfo } from "node:net";
import { afterEach, describe, expect, it, vi } from "vitest";
import { postVqsMessage, type VqsRequest } from "./vqs-client.js";

const servers: Server[] = [];

async function listen(
  handler: (req: IncomingMessage, res: ServerResponse) => void,
): Promise<number> {
  const server = createServer(handler);
  servers.push(server);
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  return (server.address() as AddressInfo).port;
}

function request(endpointPort: number, overrides: Partial<VqsRequest> = {}): VqsRequest {
  return {
    endpointPort,
    queueName: "__wkf_workflow_demo",
    messageId: "msg_01",
    attempt: 1,
    body: new TextEncoder().encode('{"runId":"wrun_01"}'),
    tenantId: "prj_vqs",
    deploymentId: "dep_vqs",
    runtimeSecret: "secret",
    timeoutMs: 5_000,
    ...overrides,
  };
}

afterEach(async () => {
  vi.unstubAllGlobals();
  for (const server of servers.splice(0)) {
    server.closeAllConnections();
    await new Promise<void>((resolve) => server.close(() => resolve()));
  }
});

describe("postVqsMessage", () => {
  it("does not deliver through the global fetch, whose undici deadlines cannot be lifted", async () => {
    // A delivery executes the workflow body inline, so response headers arrive
    // only when that work is done. The global `fetch` gives up on headers after
    // undici's fixed 300 seconds -- well inside `timeoutMs` (15 minutes by
    // default) -- and the dispatcher would then redeliver a step that is still
    // running. `timeoutMs` has to be the only deadline.
    vi.stubGlobal("fetch", () => Promise.reject(new TypeError("fetch failed")));
    let received: { url: string | undefined; body: string } | undefined;
    const port = await listen((req, res) => {
      const chunks: Buffer[] = [];
      req.on("data", (chunk: Buffer) => chunks.push(chunk));
      req.on("end", () => {
        received = { url: req.url, body: Buffer.concat(chunks).toString("utf8") };
        res.writeHead(200, { "content-type": "application/json" }).end('{"ok":true}');
      });
    });

    await expect(postVqsMessage(request(port))).resolves.toEqual({ type: "completed" });
    expect(received).toEqual({
      url: "/.well-known/workflow/v1/flow",
      body: '{"runId":"wrun_01"}',
    });
  });

  it("reports a reschedule when the executor answers with timeoutSeconds", async () => {
    const port = await listen((req, res) => {
      req.resume();
      res.writeHead(200, { "content-type": "application/json" }).end('{"timeoutSeconds":42}');
    });

    await expect(postVqsMessage(request(port))).resolves.toEqual({
      type: "reschedule",
      timeoutSeconds: 42,
    });
  });

  it("gives up after timeoutMs and says so", async () => {
    const port = await listen((req) => {
      req.resume();
      // Never answers: the held POST outlives the caller's deadline.
    });

    await expect(postVqsMessage(request(port, { timeoutMs: 50 }))).resolves.toEqual({
      type: "error",
      status: 0,
      text: "Dispatch timed out after 50ms.",
      retryable: true,
    });
  });

  it("names the transport failure instead of a bare 'fetch failed'", async () => {
    const port = await listen(() => {});
    const server = servers.pop()!;
    await new Promise<void>((resolve) => server.close(() => resolve()));

    const result = await postVqsMessage(request(port));

    expect(result).toMatchObject({ type: "error", status: 0, retryable: true });
    expect((result as { text: string }).text).toContain("ECONNREFUSED");
  });

  it("treats 4xx as final and 5xx as retryable", async () => {
    let status = 409;
    const port = await listen((req, res) => {
      req.resume();
      res.writeHead(status).end("nope");
    });

    await expect(postVqsMessage(request(port))).resolves.toEqual({
      type: "error",
      status: 409,
      text: "nope",
      retryable: false,
    });
    status = 503;
    await expect(postVqsMessage(request(port))).resolves.toEqual({
      type: "error",
      status: 503,
      text: "nope",
      retryable: true,
    });
  });
});
