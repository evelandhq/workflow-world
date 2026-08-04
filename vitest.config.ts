import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    // `src` only. The conformance project has its own config: it needs a pinned
    // executor port, a running dispatcher and a stub control plane, so it must
    // never be swept up by the default run.
    include: ["src/**/*.test.ts"],
  },
});
