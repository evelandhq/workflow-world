import { describe, expect, test } from "vitest";
import { createCheckpointingRehydrator } from "./stream-checkpoints.js";

describe("stream rehydration checkpoint policy", () => {
  test("offers a checkpoint after the logical chunk interval", () => {
    const progress = createCheckpointingRehydrator({ chunkInterval: 3, byteInterval: 1_000 });

    expect(progress.feed("chnk_1", Buffer.from("a"), 0).checkpoint).toBeNull();
    expect(progress.feed("chnk_2", Buffer.from("b"), 1).checkpoint).toBeNull();
    const third = progress.feed("chnk_3", Buffer.from("c"), 2).checkpoint;

    expect(third).toMatchObject({ chunkId: "chnk_3", nextIndex: 3 });
    expect(progress.feed("chnk_4", Buffer.from("d"), 3).checkpoint).toBeNull();
  });

  test("offers a checkpoint after the compacted byte interval", () => {
    const progress = createCheckpointingRehydrator({ chunkInterval: 100, byteInterval: 5 });

    expect(progress.feed("chnk_1", Buffer.from("abc"), 0).checkpoint).toBeNull();
    expect(progress.feed("chnk_2", Buffer.from("de"), 1).checkpoint).toMatchObject({
      chunkId: "chnk_2",
      nextIndex: 2,
    });
  });

  test("resumes indices and state from the most recent database checkpoint", () => {
    const first = createCheckpointingRehydrator({ chunkInterval: 1, byteInterval: 1_000 });
    const saved = first.feed("chnk_1", Buffer.from("opaque"), 40).checkpoint!;
    const resumed = createCheckpointingRehydrator({ chunkInterval: 2, byteInterval: 1_000 }, saved);

    expect(resumed.nextIndex).toBe(41);
    expect(resumed.feed("chnk_2", Buffer.from("x"), 41).checkpoint).toBeNull();
    expect(resumed.feed("chnk_3", Buffer.from("y"), 42).checkpoint).toMatchObject({
      nextIndex: 43,
    });
  });
});
