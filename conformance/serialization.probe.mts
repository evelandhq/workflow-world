import { hydrateWorkflowReturnValue } from "@workflow/core/serialization";
import { createFetcher, startServer } from "@workflow/world-testing/dist/src/util.mjs";
import { makeWorkerUtils } from "graphile-worker";
import { Pool } from "pg";
import { expect, test } from "vitest";
import { reenqueueActiveRunsForAllTenants } from "../src/dispatcher/boot-recovery.js";
import { PACKAGE_NAME, resolveConformanceDatabaseUrl } from "./env.mts";

/**
 * A MANUAL PROBE, not a gate. Named `.probe.mts` so the conformance project's
 * `*.test.mts` glob does not pick it up.
 *
 * ─────────────────────────────────────────────────────────────────────────────
 * WHY IT IS NOT A GATE: the detector below does not isolate what it claims to.
 *
 * Measured over three runs of the CONTROL — a clean run with zero injected
 * duplicates — on this configuration:
 *
 *   run 1  no overshoot
 *   run 2  no overshoot
 *   run 3  values [1,2,3,4,5,9,10,…,23] for 20 steps → overshoot [21,22,23]
 *
 * So an ordinary external-mode run already re-executes step bodies during
 * replay, without any duplicate delivery: the module-level counter advances for
 * a body whose result is then discarded. The overshoot signal therefore conflates
 * ordinary replay with the duplicate-concurrent-delivery bug, and the reproduction
 * that was thought to demonstrate R1 quantitatively ("20 steps, 23 body
 * executions") is not distinguishable from the control.
 *
 * The GATE case was correspondingly flaky: overshoot in 2 of 3 runs.
 *
 * R1 itself is still real — that rests on the code paths, not on this detector.
 * See KNOWN-GAPS.md. What is missing is a *deterministic* observation of it, and
 * that needs a detector keyed on committed side effects rather than on a
 * process-local counter.
 * ─────────────────────────────────────────────────────────────────────────────
 *
 * Every test in `createTestSuite` is one sequential `invoke`, so none of them
 * ever produces two concurrent deliveries for a single run. In `external` mode
 * that is exactly the hole: the embedded task handler's `inflightWorkflowRuns`
 * guard (and its `completedMessages` / `inflightMessages` siblings) live inside
 * `createTaskHandler`, which is only registered by `setupListeners`, which is
 * only reachable through `startRunnerWhenExecutorIsReady` — and that returns
 * immediately when `runner === "external"`. The dispatcher replaces none of
 * them: its `addJob` passes no graphile `queueName`, and `fairness.acquire` runs
 * inside the handler *after* the claim.
 *
 * `workflows/noop.ts`'s `brokenWf` is the detector. `noop` is a `'use step'`
 * function returning a module-level counter, so the run's output is one number
 * per step. A value above the step count can only come from a step body that ran
 * an extra time — a duplicate side effect, not merely a duplicated event.
 *
 * Deliberately NOT asserted through the event log: the correlated-event unique
 * index absorbs the losing insert, so the log looks perfect while bodies run
 * twice. An event-shaped assertion here would pass and hide the bug.
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
 * The control, and it is load-bearing: the overshoot detector only means
 * something if a clean external run records exactly `1..N`. Without this, a
 * future runtime change that made `brokenWf` overshoot for an unrelated reason
 * would silently invalidate the gate below.
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
 * KNOWN FAILURE — `test.fails` asserts the body throws.
 *
 * R1 is open, so this reproduction is expected to detect duplicate execution.
 * When per-run serialization lands, `test.fails` itself starts failing, which
 * forces this to be flipped to a plain `test` — the gap cannot be quietly fixed
 * or quietly forgotten.
 *
 * Nothing here is hand-built. `reenqueueActiveRunsForAllTenants` IS the
 * dispatcher's own boot sweep; its jobKey is `msg_recover_<runId>`, which
 * collapses only against another sweep, never against the World's own flow job
 * (whose jobKey is a fresh ULID per send). So a sweep overlapping a live run adds
 * a genuine second concurrent delivery through production code — the ordinary
 * case of a dispatcher restarting while work is in flight.
 */
test.fails(
  "GATE (expected to fail until R1 is fixed): a boot sweep during a live run must not duplicate step bodies",
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
