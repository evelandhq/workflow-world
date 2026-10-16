import { readFileSync } from "node:fs";
import path from "node:path";
import { pathToFileURL } from "node:url";
import { describe, expect, test } from "vitest";

/**
 * What a hook wake that bypasses its run's queue (`isHookWake`) relies on in the
 * installed `@workflow/core`, checked against its dist so that a core bump that
 * changes either fails here instead of running a step body twice in production.
 *
 * The wake's replay runs beside the delivery that holds the queue, and the
 * argument that no step body runs twice is that a replay executes inline only a
 * step it won the `step_started` claim for. Optimistic inline start breaks that:
 * it begins a new step's body before the claim resolves, so two racing replays
 * can both run it ("can execute a step more than once when handlers race",
 * runtime/constants.js). It is reached two ways, and both must stay away from a
 * run that can receive a hook wake:
 *
 *  - opted into per deployment with `WORKFLOW_OPTIMISTIC_INLINE_START=1`, which
 *    must stay off by default; and
 *  - forced by turbo on a run's first delivery, which core turns off once the
 *    run has a hook or a wait. A run that receives a hook wake has a hook, so its
 *    wake never races a forced optimistic start.
 *
 * The second is internal to the runtime's invocation loop, with no export to
 * call, so it is read from the source of the expression itself.
 */
const coreDist = path.resolve(
  import.meta.dirname,
  "..",
  "node_modules",
  "@workflow",
  "core",
  "dist",
);

describe("@workflow/core concurrency the hook-wake bypass relies on", () => {
  test("optimistic inline start is off unless a deployment opts in", async () => {
    const constants = (await import(
      pathToFileURL(path.join(coreDist, "runtime", "constants.js")).href
    )) as { isOptimisticInlineStartEnabled: () => boolean };
    const previous = process.env.WORKFLOW_OPTIMISTIC_INLINE_START;
    delete process.env.WORKFLOW_OPTIMISTIC_INLINE_START;
    try {
      expect(constants.isOptimisticInlineStartEnabled()).toBe(false);
    } finally {
      if (previous !== undefined) process.env.WORKFLOW_OPTIMISTIC_INLINE_START = previous;
    }
  });

  test("turbo forces optimistic start only while the run has no hook and no wait", () => {
    const source = readFileSync(path.join(coreDist, "runtime.js"), "utf8");
    const definitions = [...source.matchAll(/const forceOptimisticStart\s*=\s*([^;]+);/g)];
    expect(definitions).toHaveLength(1);
    const clauses = definitions[0]![1]!.split("&&").map((clause) => clause.trim());

    expect(clauses[0]).toBe("turbo");
    expect(clauses).toEqual(
      expect.arrayContaining([
        "!suspensionResult.hasHookEvents",
        "!suspensionResult.hasAwaitedHookCreation",
        "!openHookWaitState.openHook",
        "!openHookWaitState.openWait",
        "!suspensionResult.waitTimeout",
      ]),
    );
    // Every clause only narrows it: an `||` would let it on past a hook.
    expect(definitions[0]![1]).not.toContain("||");
  });
});
