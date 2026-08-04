import { defineAgent } from "eve";

export default defineAgent({
  model: "anthropic/claude-sonnet-5",
  experimental: {
    workflow: {
      // The package name, exactly as Eveland's build-time injector writes it
      // (`injectWorkflowWorld` in apps/worker/src/runtime/workflow-world.ts).
      // The harness installs this from a packed tarball, so the suite exercises
      // the published resolution rather than the workspace source.
      world: "@evelandhq/workflow-world",
    },
  },
});
