/**
 * Does a long step actually survive on a renewed lease?
 *
 * The lease machinery exists so a step can outlive the 180s activation TTL, and
 * for a long time nothing tested that: a whole conformance run reports
 * `renew: 0`, because its longest dispatch is far shorter than one interval.
 * The CI variants that do renew get there by shortening the interval, which
 * exercises the mechanism but says nothing about duration. This closes that.
 *
 * Run by hand, not by CI. The whole point is real durations: the default
 * configuration holds one dispatch open for 200 seconds against the production
 * lease settings (180s TTL, 60s renewal interval), which is minutes of wall
 * clock per invocation. Putting that in the matrix would tax every push for a
 * property that changes about once a year.
 *
 *   npm run check:long-step
 *
 * Needs `WORKFLOW_WORLD_LEASE_CHECK_URL` (or `WORKFLOW_WORLD_CONFORMANCE_URL`)
 * pointing at a Postgres it may migrate, and a built `dist/`.
 *
 * ## What it drives
 *
 * Everything except the executor is the real thing: the World's own
 * `queue()` enqueue, the shipped dispatcher via `main()`, and the conformance
 * stub standing in for the host's control plane.
 *
 * The executor is a stub that holds the vqs POST open and then answers 200,
 * because no real one can do this. Upstream's fixtures have no long-blocking
 * step body, and `sleepWorkflow` is the opposite of what is needed — a durable
 * `sleep` answers `{timeoutSeconds}` so the dispatch is *released* and
 * re-enqueued, which is exactly how the design avoids holding a lease across an
 * idle wait. What holds a lease is a step body that blocks, so that is what the
 * stub reproduces.
 *
 * So this proves the dispatcher half at real durations. It does not run a
 * workflow runtime, and does not need to: the claim under test is about who
 * keeps the activation alive while a POST is held, not about what the executor
 * computes.
 *
 * ## Why there is a control
 *
 * A long dispatch that completes proves nothing on its own — it might have
 * completed because nothing was ever at risk. The control runs the identical
 * hold with every renewal refused, and requires that it is aborted. That is
 * what makes the gate mean "the renewals kept it alive" rather than "nothing
 * went wrong".
 */
import cp from "node:child_process";
import http from "node:http";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { getQueueTopicPrefix } from "@workflow/world";
import { Pool } from "pg";
import { createWorld, ensureTenantPartitions, runMigrations } from "../dist/index.js";
import { DISPATCHER_READY_TOKEN } from "../dist/dispatcher/main.js";

const here = path.dirname(fileURLToPath(import.meta.url));
const repoRoot = path.resolve(here, "..");

// Deliberately not the conformance ports: this is meant to be runnable while a
// conformance run is open in another terminal.
const EXECUTOR_PORT = Number(process.env.LEASE_CHECK_EXECUTOR_PORT ?? 41877);
const STUB_PORT = Number(process.env.LEASE_CHECK_STUB_PORT ?? 41878);

const TENANT_ID = "prj_lease_check";
const DEPLOYMENT_ID = "dep_lease_check_1";
const RUNTIME_SECRET = "lease-check-runtime-secret";
const ACTIVATION_TOKEN = "lease-check-activation-token";

/** Production defaults, on purpose — the gap is that nothing runs at these. */
const LEASE_TTL_MS = Number(process.env.LEASE_CHECK_TTL_MS ?? 180_000);
const RENEW_INTERVAL_MS = Number(process.env.LEASE_CHECK_RENEW_INTERVAL_MS ?? 60_000);
/** Long enough for three renewals, and comfortably past one whole TTL. */
const HOLD_MS = Number(process.env.LEASE_CHECK_HOLD_MS ?? 200_000);

const DATABASE_URL =
  process.env.WORKFLOW_WORLD_LEASE_CHECK_URL ?? process.env.WORKFLOW_WORLD_CONFORMANCE_URL;

if (!DATABASE_URL) {
  console.error(
    "WORKFLOW_WORLD_LEASE_CHECK_URL is required: this check migrates and writes to a real Postgres.\n" +
      "  WORKFLOW_WORLD_LEASE_CHECK_URL=postgres://user:pass@127.0.0.1:5432/wfw_lease npm run check:long-step",
  );
  process.exit(2);
}

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const seconds = (ms) => `${(ms / 1000).toFixed(1)}s`;

/* ────────────────────────────── the slow executor ───────────────────────── */

/**
 * Stands in for a deployment running one very slow step.
 *
 * Records, for each held POST, how long it was held and whether the dispatcher
 * hung up before it answered — the abort is the signal the control asserts on.
 */
function startSlowExecutor() {
  const dispatches = [];

  const server = http.createServer((req, res) => {
    if (!req.url.endsWith("/flow")) {
      res.writeHead(404).end();
      return;
    }
    const record = { startedAt: Date.now(), heldMs: 0, aborted: false, answered: false };
    dispatches.push(record);
    console.log(`[slow-executor] holding dispatch #${dispatches.length} for ${seconds(HOLD_MS)}`);

    // The client going away is the whole signal: `withRenewedLease` aborts the
    // fetch, which lands here as the socket closing before we answer.
    res.on("close", () => {
      if (record.answered) return;
      record.aborted = true;
      record.heldMs = Date.now() - record.startedAt;
      console.log(
        `[slow-executor] dispatch #${dispatches.length} ABORTED by the dispatcher after ${seconds(record.heldMs)}`,
      );
    });

    req.resume();
    const timer = setTimeout(() => {
      if (record.aborted) return;
      record.answered = true;
      record.heldMs = Date.now() - record.startedAt;
      console.log(`[slow-executor] answering after ${seconds(record.heldMs)}`);
      res.writeHead(200, { "content-type": "application/json" }).end("{}");
    }, HOLD_MS);
    timer.unref();
  });

  return new Promise((resolve) => {
    server.listen(EXECUTOR_PORT, "127.0.0.1", () => {
      console.log(`[slow-executor] ready on 127.0.0.1:${EXECUTOR_PORT}`);
      resolve({ dispatches, close: () => new Promise((done) => server.close(done)) });
    });
  });
}

/* ────────────────────────────── child services ──────────────────────────── */

function spawnService(name, script, readyToken, extraEnv) {
  const proc = cp.spawn(process.execPath, [script], {
    cwd: path.join(repoRoot, "conformance"),
    stdio: ["ignore", "pipe", "pipe"],
    env: { ...process.env, ...extraEnv },
  });

  let settle;
  const ready = new Promise((resolve, reject) => {
    settle = (error) => (error ? reject(error) : resolve());
  });
  const onChunk = (chunk) => {
    const text = chunk.toString();
    process.stdout.write(text);
    if (text.includes(readyToken)) settle();
  };
  proc.stdout.on("data", onChunk);
  proc.stderr.on("data", onChunk);
  proc.once("exit", (code, signal) =>
    settle(new Error(`${name} exited before ready (code=${code} signal=${signal})`)),
  );
  setTimeout(() => settle(new Error(`${name} never became ready within 60s`)), 60_000).unref();

  return { name, proc, ready };
}

async function stopService(service) {
  if (service.proc.exitCode !== null) return;
  service.proc.kill("SIGTERM");
  await new Promise((resolve) => {
    service.proc.once("exit", resolve);
    setTimeout(() => {
      service.proc.kill("SIGKILL");
      resolve();
    }, 5_000).unref();
  });
}

async function readStubStats() {
  const response = await fetch(`http://127.0.0.1:${STUB_PORT}/internal/stats`, {
    headers: { authorization: `Bearer ${ACTIVATION_TOKEN}` },
  });
  return response.json();
}

/* ─────────────────────────────── one scenario ───────────────────────────── */

async function runScenario({ label, refuseRenewals }) {
  console.log(`\n${"═".repeat(76)}\n${label}\n${"═".repeat(76)}`);

  const env = {
    WORKFLOW_WORLD_CONFORMANCE_EXECUTOR_PORT: String(EXECUTOR_PORT),
    WORKFLOW_WORLD_CONFORMANCE_STUB_PORT: String(STUB_PORT),
    WORKFLOW_WORLD_URL: DATABASE_URL,
    WORKFLOW_WORLD_TENANT_ID: TENANT_ID,
    WORKFLOW_WORLD_DEPLOYMENT_ID: DEPLOYMENT_ID,
    WORKFLOW_WORLD_RUNNER: "external",
    WORKFLOW_WORLD_RUNTIME_SECRET: RUNTIME_SECRET,
    WORKFLOW_DISPATCHER_ACTIVATION_API_URL: `http://127.0.0.1:${STUB_PORT}`,
    WORKFLOW_DISPATCHER_ACTIVATION_TOKEN: ACTIVATION_TOKEN,
    WORKFLOW_DISPATCHER_ACTIVATION_LEASE_TTL_MS: String(LEASE_TTL_MS),
    WORKFLOW_DISPATCHER_LEASE_RENEW_INTERVAL_MS: String(RENEW_INTERVAL_MS),
    WORKFLOW_DISPATCHER_POLL_INTERVAL_MS: "200",
    STUB_RENEW_FAIL: refuseRenewals ? "1" : "",
  };

  const pool = new Pool({ connectionString: DATABASE_URL, max: 4 });

  // Strictly before the dispatcher boots. An aborted dispatch is left retrying,
  // so the control scenario always ends with a live job; a dispatcher that is
  // already polling claims it within one 200ms tick, and then no amount of
  // deleting afterwards un-dispatches it. Scoped by tenant rather than
  // truncating the table, because the URL may well be a database something else
  // is also using.
  await pool.query("delete from workflow.dispatch_dead_letters where tenant_id = $1", [TENANT_ID]);
  const { rowCount: leftover } = await pool.query(
    "delete from graphile_worker._private_jobs where payload->>'tenantId' = $1",
    [TENANT_ID],
  );
  if (leftover > 0) console.log(`[check] cleared ${leftover} leftover job(s) from a previous run`);

  const executor = await startSlowExecutor();
  const stub = spawnService("stub", "stub-activation-api.mjs", "[stub-activation-api] ready", env);
  await stub.ready;
  const dispatcher = spawnService("dispatcher", "dispatcher-boot.mjs", DISPATCHER_READY_TOKEN, env);
  await dispatcher.ready;

  const world = createWorld({
    connectionString: DATABASE_URL,
    tenantId: TENANT_ID,
    deploymentId: DEPLOYMENT_ID,
    runner: "external",
  });

  try {
    // The real enqueue path, not a hand-built job: same `addJob` the World uses
    // for every workflow message, so the job name, the `project:` flag and the
    // per-run queue are whatever production would have written.
    const queueName = `${getQueueTopicPrefix("workflow", undefined)}lease-check`;
    const { messageId } = await world.queue(queueName, { probe: "long-step-lease" }, {});
    console.log(`[check] enqueued ${messageId} on ${queueName}`);

    const deadline = Date.now() + HOLD_MS + LEASE_TTL_MS + 60_000;
    while (Date.now() < deadline) {
      const settled = executor.dispatches.find((d) => d.answered || d.aborted);
      if (settled) break;
      await sleep(1_000);
    }

    // Stop the dispatcher BEFORE reading the tally. An aborted dispatch is
    // retried, so in the control a fresh lease is legitimately open the whole
    // time — sampling live leases while the dispatcher still runs measures that
    // retry, not a leak. SIGTERM drains the in-flight dispatches and every
    // `withRenewedLease` releases in its `finally`, so the tally is only final
    // once the drain has finished.
    await sleep(2_000);
    await stopService(dispatcher);

    const stats = await readStubStats();
    const { rows: dead } = await pool.query(
      "select count(*)::int as n from workflow.dispatch_dead_letters where tenant_id = $1",
      [TENANT_ID],
    );

    return { dispatches: executor.dispatches, stats, deadLetters: dead[0].n };
  } finally {
    await world.close().catch(() => {});
    await pool.end().catch(() => {});
    await stopService(dispatcher);
    await stopService(stub);
    await executor.close();
  }
}

/* ──────────────────────────────── assertions ────────────────────────────── */

const failures = [];
function check(condition, message) {
  console.log(`  ${condition ? "✓" : "✗"} ${message}`);
  if (!condition) failures.push(message);
}

/* ───────────────────────────────── the run ──────────────────────────────── */

console.log(
  `long-step lease check\n` +
    `  hold           ${seconds(HOLD_MS)}\n` +
    `  lease TTL      ${seconds(LEASE_TTL_MS)}\n` +
    `  renew interval ${seconds(RENEW_INTERVAL_MS)}\n` +
    `  expect         ~${Math.floor(HOLD_MS / RENEW_INTERVAL_MS)} renewals across the held POST\n` +
    `  runtime        about ${seconds(2 * HOLD_MS + 30_000)} total, two scenarios\n`,
);

{
  const admin = new Pool({ connectionString: DATABASE_URL, max: 2 });
  try {
    await runMigrations(admin, { log: (m) => console.log(`[check] migrate: ${m}`) });
    await ensureTenantPartitions(admin, TENANT_ID);
  } finally {
    await admin.end();
  }
}

const gate = await runScenario({
  label: "GATE — renewals succeed: the held POST must survive its own lease TTL",
  refuseRenewals: false,
});

console.log("\nGATE assertions");
const held = gate.dispatches[0];
const expectedRenewals = Math.floor(HOLD_MS / RENEW_INTERVAL_MS) - 1;
check(
  gate.dispatches.length === 1,
  `exactly one dispatch was held (got ${gate.dispatches.length})`,
);
check(held?.answered === true, "the executor answered rather than being cut off");
check(held?.aborted === false, "the dispatcher never aborted the held POST");
check(
  held !== undefined && held.heldMs > LEASE_TTL_MS,
  `the POST outlived one whole lease TTL (held ${seconds(held?.heldMs ?? 0)} > ${seconds(LEASE_TTL_MS)})`,
);
check(
  gate.stats.renew >= expectedRenewals,
  `at least ${expectedRenewals} renewals fired (got ${gate.stats.renew})`,
);
check(gate.stats.renewFailed === 0, `no renewal failed (got ${gate.stats.renewFailed})`);
check(gate.stats.liveLeases === 0, `the lease was released (live leases ${gate.stats.liveLeases})`);
check(gate.deadLetters === 0, `nothing dead-lettered (got ${gate.deadLetters})`);

const control = await runScenario({
  label: "CONTROL — renewals refused: the same hold must be aborted, not survive",
  refuseRenewals: true,
});

console.log("\nCONTROL assertions");
const cut = control.dispatches[0];
check(cut !== undefined, "a dispatch was held");
check(cut?.aborted === true, "the dispatcher aborted it once the lease could no longer be renewed");
check(
  cut !== undefined && cut.heldMs < HOLD_MS,
  `it was cut short rather than running to completion (held ${seconds(cut?.heldMs ?? 0)} < ${seconds(HOLD_MS)})`,
);
check(control.stats.renewFailed > 0, `renewals were refused (got ${control.stats.renewFailed})`);
check(
  control.stats.liveLeases === 0,
  `the lease was still released on the way out (live leases ${control.stats.liveLeases})`,
);

console.log(`\n${"═".repeat(76)}`);
if (failures.length > 0) {
  console.error(`FAILED — ${failures.length} assertion(s):`);
  for (const failure of failures) console.error(`  ✗ ${failure}`);
  process.exit(1);
}
console.log("PASSED — a long step survives on a renewed lease, and dies without one.");
process.exit(0);
