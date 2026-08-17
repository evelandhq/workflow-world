import type { Pool } from "pg";
import {
  packTerminalStreamBlocks,
  type PackTerminalStreamBlocksResult,
} from "./stream-block-maintenance.js";
import {
  pruneExpiredStreamChunks,
  pruneExpiredWorkflowRuns,
  type StreamRetentionResult,
  type WorkflowRunRetentionResult,
} from "./retention.js";

export type StorageMaintenanceOptions = {
  streamBatchSize: number;
  maxBatches: number;
  maxStreamsToPack: number;
  runBatchSize: number;
  compactSnapshots?: boolean;
};

type StorageMaintenanceDependencies = {
  packBlocks: (
    pool: Pool,
    options: { maxStreams: number; compactSnapshots?: boolean },
  ) => Promise<PackTerminalStreamBlocksResult | { streamsCompacted: number }>;
  pruneStreams: (
    pool: Pool,
    options: { batchSize: number; maxBatches: number },
  ) => Promise<StreamRetentionResult | { deletedRows: number }>;
  pruneRuns: (
    pool: Pool,
    options: { batchSize: number; maxBatches: number },
  ) => Promise<WorkflowRunRetentionResult | { deletedRuns: number }>;
};

const defaultDependencies: StorageMaintenanceDependencies = {
  packBlocks: packTerminalStreamBlocks,
  pruneStreams: pruneExpiredStreamChunks,
  pruneRuns: pruneExpiredWorkflowRuns,
};

/** Run every storage task in order while isolating individual failures. */
export async function runStorageMaintenanceOnce(
  pool: Pool,
  options: StorageMaintenanceOptions,
  dependencies: StorageMaintenanceDependencies = defaultDependencies,
) {
  const blocks = await settle(() =>
    dependencies.packBlocks(pool, {
      maxStreams: options.maxStreamsToPack,
      compactSnapshots: options.compactSnapshots ?? true,
    }),
  );
  const streams = await settle(() =>
    dependencies.pruneStreams(pool, {
      batchSize: options.streamBatchSize,
      maxBatches: options.maxBatches,
    }),
  );
  const runs = await settle(() =>
    dependencies.pruneRuns(pool, {
      batchSize: options.runBatchSize,
      maxBatches: options.maxBatches,
    }),
  );
  return { blocks, streams, runs };
}

export function startStorageMaintenanceLoop(
  pool: Pool,
  options: {
    intervalMs: number;
    maintenance: StorageMaintenanceOptions;
    onResult?: (result: Awaited<ReturnType<typeof runStorageMaintenanceOnce>>) => void;
    onError?: (error: unknown) => void;
  },
  runOnce: (
    pool: Pool,
    options: StorageMaintenanceOptions,
  ) => Promise<unknown> = runStorageMaintenanceOnce,
): { stop(): Promise<void> } {
  if (options.intervalMs === 0) return { stop: async () => {} };

  let stopped = false;
  let active: Promise<void> | null = null;
  const tick = () => {
    if (stopped || active) return;
    active = runOnce(pool, options.maintenance)
      .then((result) => {
        options.onResult?.(result as Awaited<ReturnType<typeof runStorageMaintenanceOnce>>);
      })
      .catch((error: unknown) => options.onError?.(error))
      .finally(() => {
        active = null;
      });
  };

  tick();
  const timer = setInterval(tick, options.intervalMs);
  timer.unref();

  return {
    async stop() {
      stopped = true;
      clearInterval(timer);
      await active;
    },
  };
}

async function settle<T>(operation: () => Promise<T>): Promise<PromiseSettledResult<T>> {
  try {
    return { status: "fulfilled", value: await operation() };
  } catch (reason) {
    return { status: "rejected", reason };
  }
}
