import { describe, expect, test } from "vitest";
import {
  checkDispatchVersion,
  continuationOf,
  deliveryAccounting,
  DISPATCH_VERSION,
  jobKeyModeFor,
  runQueueFor,
  runQueueName,
  secretMatches,
} from "./dispatch-contract.js";

describe("dispatch contract", () => {
  test("accepts the current version", () => {
    expect(checkDispatchVersion(String(DISPATCH_VERSION))).toBeUndefined();
  });

  test("accepts an older dispatcher", () => {
    // Deployments outlive dispatcher releases; a newer bundle must keep serving
    // an older dispatcher for the whole run-out.
    expect(checkDispatchVersion("1", 2)).toBeUndefined();
  });

  test("rejects a newer dispatcher with an actionable error", () => {
    const rejection = checkDispatchVersion("2", 1);
    expect(rejection?.status).toBe(400);
    expect(rejection?.error).toContain("Rebuild the deployment");
  });

  test("refuses a request with no version header", () => {
    // Every delivery comes from the dispatcher, which always sends it.
    for (const value of [null, undefined, ""]) {
      expect(checkDispatchVersion(value)?.status, String(value)).toBe(401);
    }
  });

  test("rejects a malformed version rather than coercing it", () => {
    for (const value of ["abc", "0", "-1", "1.5"]) {
      expect(checkDispatchVersion(value)?.status, value).toBe(400);
    }
  });

  test("secret comparison rejects mismatches and missing values", () => {
    expect(secretMatches("s3cret", "s3cret")).toBe(true);
    expect(secretMatches("s3cret", "s3creT")).toBe(false);
    expect(secretMatches("s3cret", "")).toBe(false);
    expect(secretMatches("s3cret", null)).toBe(false);
    expect(secretMatches("s3cret", undefined)).toBe(false);
    expect(secretMatches("s3cret", "s3cret-longer")).toBe(false);
  });
});

describe("single-orchestrator (spec 9) delivery", () => {
  const TENANT = "prj_spec9";
  const orchestrator = { runId: "wrun_A", runSpecVersion: 9 };
  const step = { runId: "wrun_A", stepId: "step_1", runSpecVersion: 9 };
  const hop = {
    runId: "wrun_A",
    runSpecVersion: 9,
    waitContinuation: { resumeAtMs: Date.now() + 60_000 },
  };
  const unstamped = { runId: "wrun_C" };

  test("only a spec 9 run's orchestrator messages wait in its run queue", () => {
    expect(runQueueFor(TENANT, orchestrator)).toBe(runQueueName(TENANT, "wrun_A"));
    // Step messages and a long wait's intermediate hops keep full parallelism.
    expect(runQueueFor(TENANT, step)).toBeUndefined();
    expect(runQueueFor(TENANT, hop)).toBeUndefined();
    // A message naming a run without a stamp (boot recovery's) still waits
    // in that run's queue.
    expect(runQueueFor(TENANT, unstamped)).toBe(runQueueName(TENANT, "wrun_C"));
    expect(runQueueFor(TENANT, { __healthCheck: true })).toBeUndefined();
  });

  test("a spec 9 keyed send is deduplicated while its job lives", () => {
    expect(jobKeyModeFor(step, "step-key")).toBe("unsafe_dedupe");
    expect(jobKeyModeFor(step, undefined)).toBeUndefined();
    expect(jobKeyModeFor(unstamped, "step-key")).toBeUndefined();
  });

  test("deliveryCount counts every delivery of a message; attempt starts over on a reschedule", () => {
    const first = { attempt: 1 };
    expect(deliveryAccounting(first, 1)).toEqual({ attempt: 1, deliveryCount: 1 });
    // A retry of the same job (the handler threw) is the next delivery and attempt.
    expect(deliveryAccounting(first, 2)).toEqual({ attempt: 2, deliveryCount: 2 });

    // `{ timeoutSeconds: 5 }`: a counted redelivery on a fresh job.
    const later = continuationOf(first, { attempt: 2, deliveryCount: 2 }, 5, orchestrator);
    expect(deliveryAccounting(later, 1)).toEqual({ attempt: 1, deliveryCount: 3 });

    // `{ timeoutSeconds: 0 }` on a spec 9 run is a handoff: same attempt, next delivery.
    const handedOff = continuationOf(first, { attempt: 2, deliveryCount: 2 }, 0, orchestrator);
    expect(deliveryAccounting(handedOff, 1)).toEqual({ attempt: 2, deliveryCount: 3 });
  });
});
