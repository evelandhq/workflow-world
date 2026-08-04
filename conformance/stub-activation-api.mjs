/**
 * Stands in for the host's control plane: the three
 * `/internal/runtime/activations` endpoints `activation-client.ts` talks to.
 *
 * It always reports the pinned executor port, which is what lets the dispatcher
 * deliver to whichever executor `world-testing` happens to have spawned.
 *
 * It also asserts the bearer token, so a dispatcher that stops sending it fails
 * here rather than silently working.
 *
 * Two knobs the CI matrix uses:
 *   STUB_RENEW_FAIL=1   every renew returns 500 — exercises the abort path
 *   STUB_ACTIVATE_FAIL_N=k  the first k activations return 503
 */
import http from "node:http";

const PORT = Number(process.env.WORKFLOW_WORLD_CONFORMANCE_STUB_PORT);
const EXECUTOR_PORT = Number(process.env.WORKFLOW_WORLD_CONFORMANCE_EXECUTOR_PORT);
const EXPECTED_TOKEN = process.env.WORKFLOW_DISPATCHER_ACTIVATION_TOKEN;
const RENEW_FAIL = process.env.STUB_RENEW_FAIL === "1";
let activateFailuresLeft = Number(process.env.STUB_ACTIVATE_FAIL_N ?? 0);

const stats = { activate: 0, renew: 0, renewFailed: 0, release: 0, unauthorized: 0, other: 0 };
const liveLeases = new Set();
let leaseSeq = 0;

const json = (res, status, body) => {
  res.writeHead(status, { "content-type": "application/json" });
  res.end(JSON.stringify(body));
};
const log = (message) => console.log(`[stub-activation-api] ${message}`);

const server = http.createServer((req, res) => {
  const path = new URL(req.url, "http://127.0.0.1").pathname;

  if (req.headers.authorization !== `Bearer ${EXPECTED_TOKEN}`) {
    stats.unauthorized += 1;
    log(`REJECTED ${req.method} ${path}: bad or missing bearer token`);
    json(res, 401, { error: "bad service token" });
    return;
  }

  const chunks = [];
  req.on("data", (chunk) => chunks.push(chunk));
  req.on("end", () => {
    if (req.method === "POST" && path === "/internal/runtime/activations") {
      stats.activate += 1;
      if (activateFailuresLeft > 0) {
        activateFailuresLeft -= 1;
        log(`activate #${stats.activate} -> 503 (forced, ${activateFailuresLeft} left)`);
        json(res, 503, { error: "forced activation failure" });
        return;
      }
      let body = {};
      try {
        body = JSON.parse(Buffer.concat(chunks).toString("utf8"));
      } catch {
        // A malformed body is the dispatcher's problem to surface, not ours.
      }
      leaseSeq += 1;
      const leaseId = `lease_${leaseSeq}`;
      liveLeases.add(leaseId);
      log(
        `activate #${stats.activate} deployment=${body.deploymentId} kind=${body.kind} -> ${leaseId} port=${EXECUTOR_PORT}`,
      );
      json(res, 200, {
        lease: { id: leaseId },
        runtimeInstance: { endpointPort: EXECUTOR_PORT },
      });
      return;
    }

    const renew = /^\/internal\/runtime\/activations\/([^/]+)\/renew$/.exec(path);
    if (req.method === "POST" && renew) {
      stats.renew += 1;
      if (RENEW_FAIL) {
        stats.renewFailed += 1;
        log(`renew ${renew[1]} -> 500 (forced)`);
        json(res, 500, { error: "forced renew failure" });
        return;
      }
      json(res, 200, { ok: true });
      return;
    }

    const release = /^\/internal\/runtime\/activations\/([^/]+)$/.exec(path);
    if (req.method === "DELETE" && release) {
      stats.release += 1;
      liveLeases.delete(release[1]);
      json(res, 200, { ok: true });
      return;
    }

    stats.other += 1;
    log(`UNHANDLED ${req.method} ${path}`);
    json(res, 404, { error: "not found" });
  });
});

const report = () => {
  log(`stats ${JSON.stringify(stats)} leaked-leases=${liveLeases.size}`);
};
process.on("SIGTERM", () => {
  report();
  process.exit(0);
});
process.on("SIGINT", () => {
  report();
  process.exit(0);
});

server.listen(PORT, "127.0.0.1", () => {
  log(`ready on 127.0.0.1:${PORT}, executor port ${EXECUTOR_PORT}`);
});
