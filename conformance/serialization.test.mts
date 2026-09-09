import { hydrateWorkflowReturnValue } from "@workflow/core/serialization";
import { getQueueTopicPrefix, type ValidQueueName } from "@workflow/world";
import { createFetcher, startServer } from "@workflow/world-testing/dist/src/util.mjs";
import { makeWorkerUtils } from "graphile-worker";
import { Pool } from "pg";
import { expect, test } from "vitest";
import { reenqueueActiveRunsForAllTenants } from "../src/dispatcher/boot-recovery.js";
import { createWorld } from "../src/index.js";
import { DEPLOYMENT_ID, PACKAGE_NAME, resolveConformanceDatabaseUrl, TENANT_ID } from "./env.mts";

/**
 * Per-run serialization under concurrent delivery — the gap upstream's
 * conformance suite structurally cannot see, because every test in it is a single
 * sequential invoke.
 *
 * In `external` mode the World starts no in-process runner, so the embedded task
 * handler's `inflightWorkflowRuns` guard is unreachable — and a process-local map
 * could not serve N dispatchers anyway. The replacement is a per-run graphile
 * queue (`runQueueName`), applied by all three enqueue paths: the World's own
 * send, the dispatcher's reschedule, and boot recovery.
 *
 * `workflows/noop.ts`'s `brokenWf` is the detector. `noop` is a `'use step'`
 * function returning a module-level counter, so the output is one number per
 * step and a value above the step count means a step body ran an extra time — a
 * duplicate side effect, not merely a duplicated event.
 *
 * Deliberately NOT asserted through the event log: the correlated-event unique
 * index absorbs the losing insert, so the log looks perfect while bodies run
 * twice. An event-shaped assertion would pass and hide the bug.
 *
 * ── History, because it explains the shape of these two tests ────────────────
 * Before per-run serialization, the GATE below overshot by 3 body executions in
 * 2 of 3 runs — and so did the CONTROL, occasionally, because eve's replay can
 * re-execute an uncommitted step body on its own. That made overshoot alone an
 * unusable signal, so the CONTROL is not decoration: it is what distinguishes
 * "duplicate delivery" from "ordinary replay". Both are now clean, and
 * `flowInvocations` for the duplicated case fell from 34–63 to 16–18, because
 * serializing the deliveries removed the redundant replays entirely.
 */

type BrokenWfOutput = { numbers: number[] };

async function runBrokenWf(options: {
  /** Called while the run is in flight, before we start waiting for it. */
  duringFlight?: (runId: string) => Promise<void>;
}): Promise<{ status: string; sorted: number[]; overshoot: number[]; invocations: number }> {
  const server = await startServer({ world: PACKAGE_NAME }).then(createFetcher);
  const { runId } = await server.invoke("workflows/noop.ts", "brokenWf", []);

  await options.duringFlight?.(runId);

  let status = "";
  for (let attempt = 0; attempt < 400; attempt += 1) {
    const run = await server.getRun(runId).catch(() => null);
    status = (run?.status as string) ?? "";
    if (status === "completed" || status === "failed") break;
    await new Promise((resolve) => setTimeout(resolve, 200));
  }
  expect(status).toBe("completed");

  const run = await server.getRun(runId);
  const output = (await hydrateWorkflowReturnValue(
    run.output!,
    runId,
    undefined,
  )) as BrokenWfOutput;

  const sorted = [...output.numbers].sort((a, b) => a - b);
  return {
    status,
    sorted,
    overshoot: sorted.filter((value) => value > output.numbers.length),
    invocations: await server.getFlowInvocationCount(runId),
  };
}

/**
 * The control, and it is load-bearing: overshoot only implicates duplicate
 * delivery if a clean run does not overshoot on its own. Without this, a runtime
 * change that made `brokenWf` overshoot for an unrelated reason would silently
 * invalidate the gate below.
 */
test(
  "CONTROL: a clean external run executes each step body exactly once",
  { timeout: 120_000 },
  async () => {
    const result = await runBrokenWf({});

    console.log(
      `[control] flowInvocations=${String(result.invocations)} steps=${String(result.sorted.length)} values=${JSON.stringify(result.sorted)}`,
    );
    expect(result.overshoot).toEqual([]);
  },
);

/**
 * The gate. Nothing here is hand-built: the competing delivery goes through the
 * World's own `queue()`, the production path a resolved hook takes to wake a
 * run, and it lands as a genuine second job for the run — its jobKey is a fresh
 * ULID per send, so nothing collapses it — while the first is in flight. The
 * per-run queue name is the only thing keeping the two from running at once.
 *
 * The dispatcher's own boot sweep used to be the injector here. It no longer
 * can be: a run in flight always has a live job on its queue (the delivery
 * being held, or the continuation the runtime enqueues before acking), and the
 * sweep skips exactly those runs. So it is asserted the other way round — the
 * sweep, run repeatedly during flight, never so much as offers this run to the
 * host filter.
 */
test(
  "a second delivery during a live run does not duplicate step bodies, and the boot sweep leaves the run alone",
  { timeout: 120_000 },
  async () => {
    const pool = new Pool({ connectionString: resolveConformanceDatabaseUrl(), max: 6 });
    const workerUtils = await makeWorkerUtils({ pgPool: pool });
    // The same tenant and deployment the spawned executor runs as, so the send
    // is addressed exactly as the executor's own continuations are.
    const world = createWorld({
      connectionString: resolveConformanceDatabaseUrl(),
      tenantId: TENANT_ID,
      deploymentId: DEPLOYMENT_ID,
      runner: "external",
    });
    try {
      const offered: string[] = [];
      const result = await runBrokenWf({
        duringFlight: async (runId) => {
          const { rows } = await pool.query<{ name: string }>(
            "select name from workflow.workflow_runs where tenant_id = $1 and id = $2",
            [TENANT_ID, runId],
          );
          const workflowName = rows[0]?.name;
          expect(workflowName).toBeDefined();
          const queueName = `${getQueueTopicPrefix("workflow")}${workflowName}` as ValidQueueName;

          let sent = 0;
          let enqueued = 0;
          for (let round = 0; round < 12; round += 1) {
            await world.queue(queueName, { runId });
            sent += 1;
            enqueued += await reenqueueActiveRunsForAllTenants({
              pool,
              workerUtils,
              filterRuns: (runs) => {
                offered.push(...runs.filter((run) => run.runId === runId).map((run) => run.runId));
                return runs;
              },
            });
            await new Promise((resolve) => setTimeout(resolve, 15));
          }
          console.log(
            `[gate] ${String(sent)} competing send(s) through the World; boot sweeps enqueued ${String(enqueued)} recovery message(s) for other runs`,
          );
        },
      });

      console.log(
        `[gate] flowInvocations=${String(result.invocations)} steps=${String(result.sorted.length)} overshoot=${JSON.stringify(result.overshoot)}`,
      );
      expect(result.overshoot).toEqual([]);
      expect(offered).toEqual([]);
    } finally {
      await world.close?.();
      await workerUtils.release();
      await pool.end();
    }
  },
);
