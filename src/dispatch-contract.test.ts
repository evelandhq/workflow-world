import { WorkflowInvokePayloadSchema } from "@workflow/world";
import { describe, expect, test } from "vitest";
import {
  checkDispatchVersion,
  DISPATCH_VERSION,
  secretMatches,
  isHookWake,
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

  test("accepts a request with no version header", () => {
    // Embedded mode POSTs to the same route over loopback carrying none of the
    // Eveland headers; requiring the header would break it.
    expect(checkDispatchVersion(null)).toBeUndefined();
    expect(checkDispatchVersion(undefined)).toBeUndefined();
    expect(checkDispatchVersion("")).toBeUndefined();
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

  test("a wake from a hook resume is a hook wake", () => {
    const timing = { resumeRequestedAtMs: 1, queuePublishRequestedAtMs: 2 };
    const hookInput = {
      resumeId: "res_1",
      hookId: "hook_1",
      token: "tok_1",
      payload: null,
      payloadDigest: "d",
    };
    expect(isHookWake(invoke({ runId: "wrun_1", hookResumeTiming: timing }))).toBe(true);
    expect(isHookWake(invoke({ runId: "wrun_1", hookInput }))).toBe(true);
  });

  test("a plain replay or a step message is not a hook wake", () => {
    const timing = { resumeRequestedAtMs: 1, queuePublishRequestedAtMs: 2 };
    expect(isHookWake(invoke({ runId: "wrun_1" }))).toBe(false);
    expect(
      isHookWake(invoke({ runId: "wrun_1", stepId: "step_1", hookResumeTiming: timing })),
    ).toBe(false);
  });
});

/** Parsed the way the World parses a message, so a renamed upstream field fails here. */
function invoke(payload: Record<string, unknown>) {
  return WorkflowInvokePayloadSchema.parse(payload);
}
