import type { Pool } from "pg";
import { describe, expect, test, vi } from "vitest";
import { runStorageMaintenanceOnce, startStorageMaintenanceLoop } from "./storage-maintenance.js";

describe("runStorageMaintenanceOnce", () => {
  test("runs block packing, stream expiry, and detail expiry", async () => {
    const calls: string[] = [];
    const result = await runStorageMaintenanceOnce(
      {} as Pool,
      { streamBatchSize: 50, maxBatches: 2, maxStreamsToPack: 10, runBatchSize: 5 },
      {
        packBlocks: async () => {
          calls.push("pack");
          return { streamsCompacted: 1 };
        },
        pruneStreams: async () => {
          calls.push("streams");
          return { deletedRows: 2 };
        },
        pruneRuns: async () => {
          calls.push("runs");
          return { deletedRuns: 3 };
        },
      },
    );

    expect(calls).toEqual(["pack", "streams", "runs"]);
    expect(result).toEqual({
      blocks: { status: "fulfilled", value: { streamsCompacted: 1 } },
      streams: { status: "fulfilled", value: { deletedRows: 2 } },
      runs: { status: "fulfilled", value: { deletedRuns: 3 } },
    });
  });

  test("one maintenance failure does not suppress the remaining work", async () => {
    const pruneStreams = vi.fn(async () => ({ deletedRows: 2 }));
    const pruneRuns = vi.fn(async () => ({ deletedRuns: 3 }));
    const result = await runStorageMaintenanceOnce(
      {} as Pool,
      { streamBatchSize: 50, maxBatches: 2, maxStreamsToPack: 10, runBatchSize: 5 },
      {
        packBlocks: async () => {
          throw new Error("pack failed");
        },
        pruneStreams,
        pruneRuns,
      },
    );

    expect(result.blocks.status).toBe("rejected");
    expect(result.streams.status).toBe("fulfilled");
    expect(result.runs.status).toBe("fulfilled");
    expect(pruneStreams).toHaveBeenCalledOnce();
    expect(pruneRuns).toHaveBeenCalledOnce();
  });
});

describe("startStorageMaintenanceLoop", () => {
  test("runs at startup and on the configured interval, then stops cleanly", async () => {
    vi.useFakeTimers();
    try {
      const runOnce = vi.fn(async () => ({ ok: true }));
      const loop = startStorageMaintenanceLoop(
        {} as Pool,
        {
          intervalMs: 60_000,
          maintenance: {
            streamBatchSize: 50,
            maxBatches: 2,
            maxStreamsToPack: 10,
            runBatchSize: 5,
          },
        },
        runOnce,
      );
      await vi.waitFor(() => expect(runOnce).toHaveBeenCalledTimes(1));
      await vi.advanceTimersByTimeAsync(60_000);
      expect(runOnce).toHaveBeenCalledTimes(2);

      await loop.stop();
      await vi.advanceTimersByTimeAsync(60_000);
      expect(runOnce).toHaveBeenCalledTimes(2);
    } finally {
      vi.useRealTimers();
    }
  });

  test("zero disables startup and recurring maintenance", async () => {
    const runOnce = vi.fn(async () => ({ ok: true }));
    const loop = startStorageMaintenanceLoop(
      {} as Pool,
      {
        intervalMs: 0,
        maintenance: {
          streamBatchSize: 50,
          maxBatches: 2,
          maxStreamsToPack: 10,
          runBatchSize: 5,
        },
      },
      runOnce,
    );

    expect(runOnce).not.toHaveBeenCalled();
    await loop.stop();
  });
});
