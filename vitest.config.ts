import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    // `src` only. The conformance project has its own config: it needs a pinned
    // executor port, a running dispatcher and a stub control plane, so it must
    // never be swept up by the default run.
    include: ["src/**/*.test.ts"],
    // Several suites start real graphile runners against one database. Run the
    // files one at a time: concurrent runners sweep each other's locked jobs and
    // the resulting failures look like product bugs rather than interference.
    // The whole suite is about a second, so this costs nothing.
    fileParallelism: false,
  },
});
