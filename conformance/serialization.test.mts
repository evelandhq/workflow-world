import { hydrateWorkflowReturnValue } from "@workflow/core/serialization";
import { createFetcher, startServer } from "@workflow/world-testing/dist/src/util.mjs";
import { makeWorkerUtils } from "graphile-worker";
import { Pool } from "pg";
import { expect, test } from "vitest";
import { reenqueueActiveRunsForAllTenants } from "../src/dispatcher/boot-recovery.js";
import { PACKAGE_NAME, resolveConformanceDatabaseUrl } from "./env.mts";

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
  duringFlight?: () => Promise<void>;
}): Promise<{ status: string; sorted: number[]; overshoot: number[]; invocations: number }> {
  const server = await startServer({ world: PACKAGE_NAME }).then(createFetcher);
  const { runId } = await server.invoke("workflows/noop.ts", "brokenWf", []);

  await options.duringFlight?.();

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
 * The gate. Nothing here is hand-built. `reenqueueActiveRunsForAllTenants` IS the
 * dispatcher's own boot sweep; its jobKey is `msg_recover_<runId>`, which
 * collapses only against another sweep, never against the World's own flow job
 * (whose jobKey is a fresh ULID per send). So a sweep overlapping a live run adds
 * a genuine second concurrent delivery through production code — the ordinary
 * case of a dispatcher restarting while work is in flight.
 */
test(
  "a boot sweep during a live run does not duplicate step bodies",
  { timeout: 120_000 },
  async () => {
    const pool = new Pool({ connectionString: resolveConformanceDatabaseUrl(), max: 6 });
    const workerUtils = await makeWorkerUtils({ pgPool: pool });
    try {
      const result = await runBrokenWf({
        duringFlight: async () => {
          let enqueued = 0;
          for (let sweep = 0; sweep < 12; sweep += 1) {
            enqueued += await reenqueueActiveRunsForAllTenants({ pool, workerUtils });
            await new Promise((resolve) => setTimeout(resolve, 15));
          }
          console.log(
            `[gate] production boot sweeps enqueued ${String(enqueued)} recovery message(s)`,
          );
        },
      });

      console.log(
        `[gate] flowInvocations=${String(result.invocations)} steps=${String(result.sorted.length)} overshoot=${JSON.stringify(result.overshoot)}`,
      );
      expect(result.overshoot).toEqual([]);
    } finally {
      await workerUtils.release();
      await pool.end();
    }
  },
);
