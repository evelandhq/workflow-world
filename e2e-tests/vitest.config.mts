import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    root: import.meta.dirname,
    include: ["*.test.mts"],
    // An npm install plus a full `eve build` per eve version, then a real agent
    // boot. Minutes, not seconds.
    testTimeout: 600_000,
    hookTimeout: 900_000,
    // Each version binds its own port and database, but agent builds are heavy
    // and eve keeps a shared build cache under node_modules/eve/.eve — run them
    // one at a time rather than fighting over it.
    fileParallelism: false,
    pool: "forks",
    // Vitest 4 moved these to the top level of `test`.
    forks: { singleFork: true },
  },
});
