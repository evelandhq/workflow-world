import { defineConfig } from "vitest/config";
import { applyConformanceEnv } from "./env.mts";

// Applied at config load: `world-testing` spawns each executor with
// `{...process.env}` and passes nothing of its own, so anything set later than
// this never reaches the executor.
applyConformanceEnv();

export default defineConfig({
  test: {
    root: import.meta.dirname,
    globalSetup: ["./global-setup.mts"],
    include: ["*.test.mts"],
    testTimeout: 90_000,
    hookTimeout: 120_000,
    // Every spawned executor binds the same pinned PORT, so two of them must
    // never be alive at once. One file at a time, one fork.
    fileParallelism: false,
    pool: "forks",
    poolOptions: { forks: { singleFork: true } },
  },
});
