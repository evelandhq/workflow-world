import { afterEach, describe, expect, it, vi } from "vitest";
import { createActivationClient } from "./activation-client.js";

function controlApi(runtimeInstance: unknown) {
  vi.stubGlobal("fetch", () =>
    Promise.resolve(Response.json({ lease: { id: "lease_1" }, runtimeInstance })),
  );
  return createActivationClient({ apiUrl: "http://control.internal", serviceToken: "t" });
}

const activate = (client: ReturnType<typeof createActivationClient>) =>
  client.activate({ deploymentId: "dep_1", kind: "workflow_step", ownerId: "o" });

afterEach(() => vi.unstubAllGlobals());

describe("activation result", () => {
  it("still reads the loopback port every existing control plane sends", async () => {
    await expect(activate(controlApi({ endpointPort: 4100 }))).resolves.toEqual({
      type: "activated",
      activation: { leaseId: "lease_1", endpointPort: 4100 },
    });
  });

  it("reads an executor URL, beside the port or on its own", async () => {
    await expect(
      activate(controlApi({ endpointPort: 4100, endpointUrl: "http://jiri.svc:3000" })),
    ).resolves.toEqual({
      type: "activated",
      activation: { leaseId: "lease_1", endpointPort: 4100, endpointUrl: "http://jiri.svc:3000" },
    });
    await expect(activate(controlApi({ endpointUrl: "http://jiri.svc:3000" }))).resolves.toEqual({
      type: "activated",
      activation: { leaseId: "lease_1", endpointUrl: "http://jiri.svc:3000" },
    });
  });

  it("treats an activation that names no executor as unavailable", async () => {
    await expect(activate(controlApi({}))).resolves.toMatchObject({ type: "unavailable" });
  });
});
