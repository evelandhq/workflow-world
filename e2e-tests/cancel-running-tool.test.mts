import { createRequire } from "node:module";
import path from "node:path";
import { pathToFileURL } from "node:url";
import { Pool } from "pg";
import { afterAll, beforeAll, describe, expect, test } from "vitest";
import { startDispatcherService, type DispatcherService } from "../src/dispatcher/service.js";
import { ensureTenantPartitions, runMigrations } from "../src/index.js";
import { ENABLED_EVE_VERSIONS } from "./eve-versions.mts";
import {
  buildAgent,
  databaseFor,
  deploymentFor,
  E2E_RUNTIME_SECRET,
  packWorld,
  startAgent,
  tenantFor,
  type StartedAgent,
} from "./harness.mts";

/**
 * A cancel stops a tool that is running, through a real eve agent in the
 * topology a platform ships: `external` runner, the dispatcher in the loop.
 *
 * eve runs the tool inside the delivery that claimed its step, and reaches it
 * only through the replay that the cancel's hook wake starts. With every
 * delivery for a run in one queue, that replay waited behind the tool and the
 * abort arrived after the side effect (workflow-world#112).
 *
 * The fixture's model is scripted to call `send_newsletter`, an eight-second
 * tool that marks `started`, `aborted` or `sent` on the agent's output.
 */
const baseUrl = process.env.WORKFLOW_WORLD_E2E_URL;
const adminUrl = process.env.WORKFLOW_WORLD_E2E_ADMIN_URL ?? baseUrl;

describe.skipIf(!baseUrl)("cancelling a running tool through a real eve agent", () => {
  let worldTarball: string;

  beforeAll(() => {
    worldTarball = packWorld();
  });

  for (const [index, entry] of ENABLED_EVE_VERSIONS.entries()) {
    describe(`eve ${entry.version}`, () => {
      const tenantId = `${tenantFor(entry.version)}_cancel`;
      const deploymentId = `${deploymentFor(entry.version)}_cancel`;
      const database = `${databaseFor(entry.version)}_cancel`;
      const port = 41950 + index;

      let pool: Pool;
      let agent: StartedAgent | undefined;
      let dispatcher: DispatcherService | undefined;
      let dir: string;

      beforeAll(async () => {
        const admin = new Pool({ connectionString: adminUrl, max: 1 });
        try {
          await admin.query(`drop database if exists ${database}`);
          await admin.query(`create database ${database}`);
        } finally {
          await admin.end();
        }

        const url = new URL(baseUrl!);
        url.pathname = `/${database}`;
        const databaseUrl = url.toString();
        pool = new Pool({ connectionString: databaseUrl, max: 4 });
        await runMigrations(pool);
        await ensureTenantPartitions(pool, tenantId);

        dir = buildAgent({
          eveVersion: entry.version,
          worldTarball,
          fixture: "fixture-cancel",
          dependencies: { zod: "4.6.5" },
        });
        agent = await startAgent({
          dir,
          port,
          databaseUrl,
          tenantId,
          deploymentId,
          runner: "external",
          // vitest sets NODE_ENV=test, under which eve swaps the agent's own
          // model for its mock and the scripted tool call never happens.
          env: { NODE_ENV: "production" },
        });
        dispatcher = await startDispatcherService({
          env: {
            NODE_ENV: "development",
            WORKFLOW_WORLD_URL: databaseUrl,
            WORKFLOW_WORLD_RUNTIME_SECRET: E2E_RUNTIME_SECRET,
            WORKFLOW_DISPATCHER_STATIC_ENDPOINTS: `${deploymentId}=http://127.0.0.1:${String(port)}`,
            WORKFLOW_DISPATCHER_POLL_INTERVAL_MS: "100",
            WORKFLOW_DISPATCHER_MAINTENANCE_INTERVAL_MS: "0",
          },
          telemetry: { emit() {}, async shutdown() {} },
        });
      });

      afterAll(async () => {
        await dispatcher?.stop().catch(() => {});
        await agent?.stop();
        await pool?.end().catch(() => {});
        const admin = new Pool({ connectionString: adminUrl, max: 1 });
        await admin.query(`drop database if exists ${database}`).catch(() => {});
        await admin.end();
      });

      test("the tool sees the abort and never sends", async () => {
        const { EveAgentStore, defaultMessageReducer } = (await import(
          pathToFileURL(createRequire(path.join(dir, "package.json")).resolve("eve/client")).href
        )) as typeof import("eve/client");
        const store = new EveAgentStore({
          host: `http://127.0.0.1:${String(port)}`,
          reducer: defaultMessageReducer(),
        });
        const marks = () =>
          [...agent!.log().matchAll(/\[e2e-cancel\] (\w+)/g)].map((match) => match[1]);

        await store.prewarm();
        const turn = store.send({ message: "Send the newsletter." });
        await waitFor(() => marks().includes("started"), 60_000, "the tool to start").catch(
          (error: Error) => {
            const events = store.snapshot.events.map((event) => event.type).join(", ");
            throw new Error(
              `${error.message}. Session events: ${events}. Agent output:\n${agent!.log().slice(-4000)}`,
            );
          },
        );

        await new Promise((resolve) => setTimeout(resolve, 1_500));
        expect((await store.cancel()).status).toBe("accepted");
        await waitFor(() => marks().includes("aborted"), 5_000, "the abort to reach the tool");
        await turn;

        // Past the point the tool would have sent, had it run on.
        await new Promise((resolve) => setTimeout(resolve, 8_000));
        expect(marks()).toEqual(["started", "aborted"]);
      }, 180_000);
    });
  }
});

async function waitFor(predicate: () => boolean, timeoutMs: number, what: string): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!predicate()) {
    if (Date.now() > deadline) throw new Error(`timed out waiting for ${what}`);
    await new Promise((resolve) => setTimeout(resolve, 100));
  }
}
