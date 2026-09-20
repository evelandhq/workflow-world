import { describe, expect, it } from "vitest";
import { resolveDispatcherConfig } from "./config.js";
import { createStaticActivationClient, parseStaticEndpoints } from "./static-activation.js";

describe("static activation", () => {
  it("sends each deployment to its own origin, so two can run side by side during a cut-over", async () => {
    // A run stays pinned to the deployment that created it. While the old
    // deployment still has runs, both are listed and each gets its own messages.
    const client = createStaticActivationClient({
      endpoints: { "jiri-v3": "http://jiri-v3.svc:3000", "jiri-v4": "http://jiri-v4.svc:3000/" },
    });

    const old = await client.activate({
      deploymentId: "jiri-v3",
      kind: "workflow_step",
      ownerId: "o",
    });
    const current = await client.activate({
      deploymentId: "jiri-v4",
      kind: "workflow_step",
      ownerId: "o",
    });

    expect(old).toMatchObject({
      type: "activated",
      activation: { endpointUrl: "http://jiri-v3.svc:3000" },
    });
    expect(current).toMatchObject({
      type: "activated",
      activation: { endpointUrl: "http://jiri-v4.svc:3000" },
    });
  });

  it("dead-letters a deployment nobody runs any more instead of retrying against nothing", async () => {
    const client = createStaticActivationClient({
      endpoints: { "jiri-v4": "http://jiri-v4.svc:3000" },
    });

    await expect(
      client.activate({ deploymentId: "jiri-v2", kind: "workflow_step", ownerId: "o" }),
    ).resolves.toMatchObject({ type: "not-activatable" });
  });

  it("holds a lease that never lapses, because nothing reaps an executor that is always running", async () => {
    const client = createStaticActivationClient({ endpoints: { d: "http://d.svc" } });
    const first = await client.activate({ deploymentId: "d", kind: "workflow_step", ownerId: "o" });
    const second = await client.activate({
      deploymentId: "d",
      kind: "workflow_step",
      ownerId: "o",
    });
    if (first.type !== "activated" || second.type !== "activated") throw new Error("not activated");

    expect(first.activation.leaseId).not.toBe(second.activation.leaseId);
    await expect(client.renew(first.activation.leaseId)).resolves.toBe(true);
    await expect(client.release(first.activation.leaseId)).resolves.toBeUndefined();
  });

  it("refuses an endpoint that is not an origin at startup, not at the first dispatch", () => {
    expect(() => createStaticActivationClient({ endpoints: { d: "http://d.svc/path" } })).toThrow(
      /origin/,
    );
    expect(() => parseStaticEndpoints("d=ftp://d.svc")).toThrow(/http or https/);
    expect(() => parseStaticEndpoints("just-a-name")).toThrow(/<deploymentId>=<origin>/);
    expect(() => parseStaticEndpoints("d=http://a.svc,d=http://b.svc")).toThrow(/more than once/);
    expect(() => parseStaticEndpoints(" , ")).toThrow(/names no deployment/);
  });

  it("reads the table from the environment", () => {
    expect(
      parseStaticEndpoints("jiri-v3=http://jiri-v3.svc:3000, jiri-v4=https://jiri-v4.svc/"),
    ).toEqual({
      "jiri-v3": "http://jiri-v3.svc:3000",
      "jiri-v4": "https://jiri-v4.svc",
    });
  });
});

describe("dispatcher configuration with static endpoints", () => {
  const base = { WORKFLOW_WORLD_URL: "postgres://host/shared" };

  it("needs no activation API when the executors are always running", () => {
    const config = resolveDispatcherConfig({
      ...base,
      WORKFLOW_DISPATCHER_STATIC_ENDPOINTS: "jiri=http://jiri.svc:3000",
    });

    expect(config.staticEndpoints).toEqual({ jiri: "http://jiri.svc:3000" });
    expect(config.apiUrl).toBeUndefined();
  });

  it("refuses two answers to where a deployment runs", () => {
    expect(() =>
      resolveDispatcherConfig({
        ...base,
        WORKFLOW_DISPATCHER_ACTIVATION_API_URL: "http://127.0.0.1:4000",
        WORKFLOW_DISPATCHER_STATIC_ENDPOINTS: "jiri=http://jiri.svc:3000",
      }),
    ).toThrow(/configure one/);
  });

  it("still requires one of them", () => {
    expect(() => resolveDispatcherConfig(base)).toThrow(
      /WORKFLOW_DISPATCHER_ACTIVATION_API_URL is required/,
    );
  });
});
