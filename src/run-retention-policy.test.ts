import { describe, expect, test } from "vitest";
import {
  RUN_RETENTION_ATTRIBUTE,
  resolveRunRetentionClass,
  retentionDeadlines,
} from "./run-retention-policy.js";

describe("run retention policy", () => {
  test.each([
    ["scheduled", "scheduled"],
    ["ephemeral", "scheduled"],
    ["interactive", "interactive"],
    ["persistent", "persistent"],
    [undefined, "interactive"],
  ] as const)("resolves %j to %s", (value, expected) => {
    expect(resolveRunRetentionClass(value)).toBe(expected);
  });

  test("rejects an unknown class", () => {
    expect(() => resolveRunRetentionClass("forever-ish")).toThrow(/retention class/);
  });

  test("uses an explicit run field before the attributes convention", () => {
    expect(resolveRunRetentionClass("scheduled", { [RUN_RETENTION_ATTRIBUTE]: "persistent" })).toBe(
      "scheduled",
    );
    expect(resolveRunRetentionClass(undefined, { [RUN_RETENTION_ATTRIBUTE]: "ephemeral" })).toBe(
      "scheduled",
    );
  });

  test("successful scheduled runs keep stream data for 15 minutes and details for 24 hours", () => {
    const completedAt = new Date("2026-08-17T00:00:00.000Z");
    const deadlines = retentionDeadlines("scheduled", "completed", completedAt);

    expect(deadlines.compactAfter?.toISOString()).toBe("2026-08-17T00:01:00.000Z");
    expect(deadlines.expireAfter?.toISOString()).toBe("2026-08-17T00:15:00.000Z");
    expect(deadlines.detailExpireAfter?.toISOString()).toBe("2026-08-18T00:00:00.000Z");
  });

  test("failed scheduled runs keep streams for 1 hour and details for 7 days", () => {
    const completedAt = new Date("2026-08-17T00:00:00.000Z");
    const deadlines = retentionDeadlines("scheduled", "failed", completedAt);

    expect(deadlines.compactAfter?.toISOString()).toBe("2026-08-17T00:01:00.000Z");
    expect(deadlines.expireAfter?.toISOString()).toBe("2026-08-17T01:00:00.000Z");
    expect(deadlines.detailExpireAfter?.toISOString()).toBe("2026-08-24T00:00:00.000Z");
  });

  test("cancelled scheduled runs keep streams for 1 hour and details for 3 days", () => {
    const completedAt = new Date("2026-08-17T00:00:00.000Z");
    const deadlines = retentionDeadlines("scheduled", "cancelled", completedAt);

    expect(deadlines.compactAfter?.toISOString()).toBe("2026-08-17T00:01:00.000Z");
    expect(deadlines.expireAfter?.toISOString()).toBe("2026-08-17T01:00:00.000Z");
    expect(deadlines.detailExpireAfter?.toISOString()).toBe("2026-08-20T00:00:00.000Z");
  });

  test("interactive runs keep streams for 24 hours and details for 30 days", () => {
    const completedAt = new Date("2026-08-17T00:00:00.000Z");
    const deadlines = retentionDeadlines("interactive", "failed", completedAt);

    expect(deadlines.compactAfter?.toISOString()).toBe("2026-08-17T00:05:00.000Z");
    expect(deadlines.expireAfter?.toISOString()).toBe("2026-08-18T00:00:00.000Z");
    expect(deadlines.detailExpireAfter?.toISOString()).toBe("2026-09-16T00:00:00.000Z");
  });

  test("persistent runs do not receive cleanup deadlines", () => {
    expect(retentionDeadlines("persistent", "cancelled", new Date())).toEqual({
      compactAfter: null,
      expireAfter: null,
      detailExpireAfter: null,
    });
  });
});
