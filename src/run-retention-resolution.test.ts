import { describe, expect, test } from "vitest";
import {
  resolveRunRetentionForCreation,
  resolveRunRetentionClassForCreation,
  withRunRetentionIntent,
} from "./run-retention-resolution.js";
import { RUN_RETENTION_ATTRIBUTE } from "./run-retention-policy.js";

describe("run creation retention resolution", () => {
  test("resolves a parent-only child to its parent's stored retention root", async () => {
    await expect(
      resolveRunRetentionForCreation({
        runId: "wrun_child",
        attributes: { $parentRunId: "wrun_parent" },
        getAncestorRetention: async (runId) =>
          runId === "wrun_parent"
            ? { retentionClass: "scheduled", retentionRootRunId: "wrun_root" }
            : undefined,
      }),
    ).resolves.toEqual({
      retentionClass: "scheduled",
      retentionRootRunId: "wrun_root",
    });
  });

  test("makes an unrelated root its own retention root", async () => {
    await expect(resolveRunRetentionForCreation({ runId: "wrun_root" })).resolves.toEqual({
      retentionClass: "interactive",
      retentionRootRunId: "wrun_root",
    });
  });

  test("classifies a root created inside the platform scheduler context as scheduled", async () => {
    const retentionClass = await withRunRetentionIntent("scheduled", () =>
      resolveRunRetentionClassForCreation({}),
    );

    expect(retentionClass).toBe("scheduled");
  });

  test.each(["scheduled", "interactive", "persistent"] as const)(
    "inherits %s from the SDK root lineage without knowing the workflow name",
    async (rootClass) => {
      const retentionClass = await resolveRunRetentionClassForCreation({
        attributes: {
          $parentRunId: "wrun_parent",
          $rootRunId: "wrun_root",
        },
        getAncestorRetentionClass: async (runId) => (runId === "wrun_root" ? rootClass : undefined),
      });

      expect(retentionClass).toBe(rootClass);
    },
  );

  test("uses the parent edge when an older SDK did not write a root edge", async () => {
    const retentionClass = await resolveRunRetentionClassForCreation({
      attributes: { $parentRunId: "wrun_parent" },
      getAncestorRetentionClass: async (runId) =>
        runId === "wrun_parent" ? "scheduled" : undefined,
    });

    expect(retentionClass).toBe("scheduled");
  });

  test("keeps explicit input and the public attribute above inherited or ambient policy", async () => {
    await expect(
      withRunRetentionIntent("scheduled", () =>
        resolveRunRetentionClassForCreation({
          retentionClass: "persistent",
          attributes: {
            [RUN_RETENTION_ATTRIBUTE]: "interactive",
            $rootRunId: "wrun_root",
          },
          getAncestorRetentionClass: async () => "scheduled",
        }),
      ),
    ).resolves.toBe("persistent");

    await expect(
      withRunRetentionIntent("scheduled", () =>
        resolveRunRetentionClassForCreation({
          attributes: {
            [RUN_RETENTION_ATTRIBUTE]: "ephemeral",
            $rootRunId: "wrun_root",
          },
          getAncestorRetentionClass: async () => "persistent",
        }),
      ),
    ).resolves.toBe("scheduled");
  });

  test("does not permanently infer policy from an Eveland-specific trigger", async () => {
    await expect(
      resolveRunRetentionClassForCreation({
        attributes: { "$eve.trigger": "channel:eveland-scheduler" },
      }),
    ).resolves.toBe("interactive");
  });

  test("defaults an unrelated root to interactive", async () => {
    await expect(
      resolveRunRetentionClassForCreation({
        attributes: { "$eve.trigger": "channel:slack" },
      }),
    ).resolves.toBe("interactive");
  });

  test("rejects lineage that cannot be resolved instead of silently changing policy", async () => {
    await expect(
      resolveRunRetentionClassForCreation({
        attributes: { $rootRunId: "wrun_missing" },
        getAncestorRetentionClass: async () => undefined,
      }),
    ).rejects.toThrow(/retention lineage/i);
  });
});
