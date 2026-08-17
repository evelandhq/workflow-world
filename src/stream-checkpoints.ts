import { createStreamRehydrator, type StreamRehydrationCheckpoint } from "./stream-compaction.js";

export const STREAM_CHECKPOINT_CHUNK_INTERVAL = 128;
export const STREAM_CHECKPOINT_BYTE_INTERVAL = 64 * 1024;

export type StoredStreamCheckpoint = {
  chunkId: `chnk_${string}` | string;
  nextIndex: number;
  state: StreamRehydrationCheckpoint;
};

export type CheckpointingRehydrator = {
  readonly nextIndex: number;
  feed(
    chunkId: `chnk_${string}` | string,
    chunk: Buffer,
    index: number,
  ): { data: Buffer; checkpoint: StoredStreamCheckpoint | null };
};

export function createCheckpointingRehydrator(
  options: { chunkInterval?: number; byteInterval?: number } = {},
  checkpoint?: StoredStreamCheckpoint,
): CheckpointingRehydrator {
  const chunkInterval = options.chunkInterval ?? STREAM_CHECKPOINT_CHUNK_INTERVAL;
  const byteInterval = options.byteInterval ?? STREAM_CHECKPOINT_BYTE_INTERVAL;
  assertPositiveInteger(chunkInterval, "chunkInterval");
  assertPositiveInteger(byteInterval, "byteInterval");

  const rehydrator = createStreamRehydrator(checkpoint?.state);
  let nextIndex = checkpoint?.nextIndex ?? 0;
  let chunksSinceCheckpoint = 0;
  let bytesSinceCheckpoint = 0;

  return {
    get nextIndex() {
      return nextIndex;
    },

    feed(chunkId, chunk, index) {
      const data = rehydrator.rehydrate(chunk);
      nextIndex = index + 1;
      chunksSinceCheckpoint += 1;
      bytesSinceCheckpoint += chunk.byteLength;
      if (chunksSinceCheckpoint < chunkInterval && bytesSinceCheckpoint < byteInterval) {
        return { data, checkpoint: null };
      }

      chunksSinceCheckpoint = 0;
      bytesSinceCheckpoint = 0;
      return {
        data,
        checkpoint: {
          chunkId,
          nextIndex,
          state: rehydrator.checkpoint(),
        },
      };
    },
  };
}

function assertPositiveInteger(value: number, name: string): void {
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new TypeError(`${name} must be a positive safe integer`);
  }
}
