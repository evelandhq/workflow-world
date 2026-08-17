import { describe, expect, test } from "vitest";
import type { Pool } from "pg";
import * as workflowWorld from "./index.js";
import { packTerminalStreamBlocks } from "./stream-block-maintenance.js";

describe("packTerminalStreamBlocks", () => {
  test("is exported from the package root", () => {
    expect((workflowWorld as Record<string, unknown>).packTerminalStreamBlocks).toBeTypeOf(
      "function",
    );
  });

  test.each([
    [{ maxStreams: 0 }, "maxStreams"],
    [{ maxStreams: 1.5 }, "maxStreams"],
    [{ maxStreams: 1, maxChunksPerBlock: 0 }, "maxChunksPerBlock"],
    [{ maxStreams: 1, maxBlockBytes: Number.POSITIVE_INFINITY }, "maxBlockBytes"],
  ])("validates options before connecting: %j", async (options, field) => {
    await expect(packTerminalStreamBlocks({} as Pool, options)).rejects.toThrow(field);
  });
});
