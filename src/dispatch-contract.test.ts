import { describe, expect, test } from "vitest";
import {
  checkDispatchVersion,
  continuationOf,
  deliveryAccounting,
  DISPATCH_VERSION,
  isCoalescibleWake,
  jobKeyModeFor,
  LIVE_FEED_HEADER,
  MIN_DISPATCH_VERSION,
  orchestratorDeliveryRunId,
  secretMatches,
  setAsideCopyOf,
} from "./dispatch-contract.js";

describe("dispatch contract", () => {
  test("is version 2, and a 2.x World accepts only version 2", () => {
    expect(DISPATCH_VERSION).toBe(2);
    expect(MIN_DISPATCH_VERSION).toBe(2);
    expect(checkDispatchVersion("2")).toBeUndefined();
  });

  test("refuses a version 1 dispatcher, which does not gate a run's deliveries", () => {
    // A 1.x dispatcher relies on a per-run graphile queue that a 2.x World no
    // longer writes, so its deliveries would run unserialized.
    const rejection = checkDispatchVersion("1");
    expect(rejection?.status).toBe(400);
    expect(rejection?.error).toContain("Upgrade the dispatcher");
  });

  test("rejects a newer dispatcher with an actionable error", () => {
    // What a 1.x World (which knows only version 1) answers this dispatcher.
    const rejection = checkDispatchVersion("2", 1, 1);
    expect(rejection?.status).toBe(400);
    expect(rejection?.error).toContain("Rebuild the deployment");
    expect(checkDispatchVersion("3")?.error).toContain("Rebuild the deployment");
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

  test("the live feed grant is an optional header", () => {
    // A World that predates the grant accepts a dispatch that carries it (an
    // unknown header changes nothing it checks), and a dispatcher that does
    // not send it gets a World that reads its absence as "no feed".
    expect(LIVE_FEED_HEADER).toBe("x-eveland-live-feed");
    expect(checkDispatchVersion(String(DISPATCH_VERSION))).toBeUndefined();
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
  const orchestrator = { runId: "wrun_A", runSpecVersion: 9 };
  const step = { runId: "wrun_A", stepId: "step_1", runSpecVersion: 9 };
  const hop = {
    runId: "wrun_A",
    runSpecVersion: 9,
    waitContinuation: { resumeAtMs: Date.now() + 60_000 },
  };
  const unstamped = { runId: "wrun_C" };

  test("only a spec 9 run's orchestrator messages take its gate", () => {
    expect(orchestratorDeliveryRunId(orchestrator)).toBe("wrun_A");
    // A message naming a run without a stamp (boot recovery's) still takes
    // that run's gate.
    expect(orchestratorDeliveryRunId(unstamped)).toBe("wrun_C");
    // Step messages and a long wait's intermediate hops keep full parallelism.
    expect(orchestratorDeliveryRunId(step)).toBeUndefined();
    expect(orchestratorDeliveryRunId(hop)).toBeUndefined();
    expect(orchestratorDeliveryRunId({ __healthCheck: true, runSpecVersion: 9 })).toBeUndefined();
    expect(orchestratorDeliveryRunId(undefined)).toBeUndefined();
  });

  test("a wake is coalescible; a message that carries data is not", () => {
    expect(isCoalescibleWake(orchestrator)).toBe(true);
    expect(isCoalescibleWake(unstamped)).toBe(true);
    expect(
      isCoalescibleWake({ ...orchestrator, hookResumeTiming: { resumeRequestedAtMs: 1 } }),
    ).toBe(true);
    expect(isCoalescibleWake({ ...orchestrator, runInput: { specVersion: 9 } })).toBe(false);
    expect(isCoalescibleWake({ ...orchestrator, hookInput: { token: "t" } })).toBe(false);
    expect(isCoalescibleWake({ ...orchestrator, invoke: {} })).toBe(false);
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

  test("a message set aside is not a delivery: its next job reports the same counts", () => {
    // Found its run's gate held on its first delivery.
    const first = setAsideCopyOf({ attempt: 1 }, { attempt: 1, deliveryCount: 1 });
    expect(first).toEqual({ attempt: 1 });
    expect(deliveryAccounting(first, 1)).toEqual({ attempt: 1, deliveryCount: 1 });

    // The third attempt of a handed-off message, its fourth delivery.
    const handedOff = continuationOf({ attempt: 1 }, { attempt: 2, deliveryCount: 2 }, 0, {
      ...orchestrator,
    });
    const counts = deliveryAccounting(handedOff, 2);
    expect(counts).toEqual({ attempt: 3, deliveryCount: 4 });
    expect(deliveryAccounting(setAsideCopyOf(handedOff, counts), 1)).toEqual(counts);
  });
});
