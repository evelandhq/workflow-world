import { describe, expect, it } from "vitest";
import type { Pool } from "pg";
import * as workflowWorld from "./index.js";
import { pruneTerminalStreamChunks } from "./retention.js";

describe("pruneTerminalStreamChunks", () => {
  it("is available from the package root", () => {
    const prune = (workflowWorld as Record<string, unknown>).pruneTerminalStreamChunks;

    expect(prune).toBeTypeOf("function");
  });

  it.each([
    [{ retentionMs: -1, batchSize: 1, maxBatches: 1 }, "retentionMs"],
    [{ retentionMs: 1.5, batchSize: 1, maxBatches: 1 }, "retentionMs"],
    [{ retentionMs: 1, batchSize: 0, maxBatches: 1 }, "batchSize"],
    [{ retentionMs: 1, batchSize: 1.5, maxBatches: 1 }, "batchSize"],
    [{ retentionMs: 1, batchSize: 1, maxBatches: 0 }, "maxBatches"],
    [{ retentionMs: 1, batchSize: 1, maxBatches: Number.POSITIVE_INFINITY }, "maxBatches"],
  ])("rejects invalid options before connecting: %j", async (options, expectedName) => {
    await expect(pruneTerminalStreamChunks({} as Pool, options)).rejects.toThrow(expectedName);
  });
});
