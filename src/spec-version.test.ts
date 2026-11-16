import { SPEC_VERSION_SINGLE_ORCHESTRATOR } from "@workflow/world";
import { afterEach, describe, expect, test, vi } from "vitest";
import { createWorld } from "./index.js";

const options = {
  connectionString: "postgres://127.0.0.1:1/unused",
  tenantId: "prj_spec_version",
  deploymentId: "dep_spec_version",
};

describe("the spec version a deployment creates runs with", () => {
  afterEach(() => {
    vi.unstubAllEnvs();
  });

  test.each([undefined, "9", " 9 "])("WORKFLOW_SPEC_VERSION=%j declares 9", async (value) => {
    vi.stubEnv("WORKFLOW_SPEC_VERSION", value);
    const world = createWorld(options);
    try {
      expect(world.specVersion).toBe(SPEC_VERSION_SINGLE_ORCHESTRATOR);
    } finally {
      await world.close?.();
    }
  });

  test.each(["6", "8", "10", "nine"])("WORKFLOW_SPEC_VERSION=%s is refused", (value) => {
    // Ignoring it would leave the deployment believing it creates runs of a
    // version it does not.
    vi.stubEnv("WORKFLOW_SPEC_VERSION", value);
    expect(() => createWorld(options)).toThrow(/WORKFLOW_SPEC_VERSION/);
  });
});
