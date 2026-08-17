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

  test("scheduled runs keep stream data for 15 minutes and details for 7 days", () => {
    const completedAt = new Date("2026-08-17T00:00:00.000Z");
    const deadlines = retentionDeadlines("scheduled", completedAt);

    expect(deadlines.compactAfter?.toISOString()).toBe("2026-08-17T00:01:00.000Z");
    expect(deadlines.expireAfter?.toISOString()).toBe("2026-08-17T00:15:00.000Z");
    expect(deadlines.detailExpireAfter?.toISOString()).toBe("2026-08-24T00:00:00.000Z");
  });

  test("interactive runs keep streams for 24 hours and details for 30 days", () => {
    const completedAt = new Date("2026-08-17T00:00:00.000Z");
    const deadlines = retentionDeadlines("interactive", completedAt);

    expect(deadlines.compactAfter?.toISOString()).toBe("2026-08-17T00:05:00.000Z");
    expect(deadlines.expireAfter?.toISOString()).toBe("2026-08-18T00:00:00.000Z");
    expect(deadlines.detailExpireAfter?.toISOString()).toBe("2026-09-16T00:00:00.000Z");
  });

  test("persistent runs do not receive cleanup deadlines", () => {
    expect(retentionDeadlines("persistent", new Date())).toEqual({
      compactAfter: null,
      expireAfter: null,
      detailExpireAfter: null,
    });
  });
});
