import { createRequire } from "node:module";
import path from "node:path";
import { pathToFileURL } from "node:url";
import { Pool } from "pg";
import { afterAll, beforeAll, describe, expect, test } from "vitest";
import { createWorld, ensureTenantPartitions, runMigrations } from "../src/index.js";
import { dropTenantPartitions } from "../src/migrate.js";
import { ENABLED_EVE_VERSIONS } from "./eve-versions.mts";
import {
  buildAgent,
  databaseFor,
  deploymentFor,
  packWorld,
  startAgent,
  tenantFor,
  type StartedAgent,
} from "./harness.mts";

/**
 * A real eve session moves to a fresh workflow run through this World.
 *
 * eve hands a session over -- after a compaction, or onto a newer
 * deployment -- by starting a successor `workflowEntry` run that takes every
 * session hook token over with `createHook({ experimental_force: true })`. It
 * does so only when the World declares `capabilities.hookForceClaim` and
 * `capabilities.hookRetention`; otherwise it logs `session handoff unavailable`
 * and the session stays in its first run for good, where its event log grows
 * until `WORKFLOW_MAX_EVENTS` fails it. So this is the end-to-end proof of the
 * force-claim takeover (`docs/design.md`, Hook force-claim): eve's own
 * compaction, eve's own successor, eve's own messages, this World in between.
 *
 * Compaction is triggered by hand (the client's `compact()`), which runs the
 * same compaction turn the context-window threshold would and arms the same
 * handoff. The fixture runs on eve's mock model (vitest's
 * `NODE_ENV=test`, made explicit below), whose summaries are never empty, so no
 * credentials and no threshold tuning are needed.
 *
 * Two handoffs, because of what each one leaves behind. The session's FIRST
 * run is its anchor: once it has handed over it parks on the
 * `<sessionId>:anchor` hook until the session ends, so it stays `running`. A
 * successor that hands over in turn has nothing left to hold and completes. The
 * second compaction is what shows an owner run actually finishing.
 */
const baseUrl = process.env.WORKFLOW_WORLD_E2E_URL;
const adminUrl = process.env.WORKFLOW_WORLD_E2E_ADMIN_URL ?? baseUrl;

/** The session's command inbox: the hook every message is delivered through. */
function inboxToken(sessionId: string): string {
  return `eve:inbox:v1:eve:session:${sessionId}:inbox`;
}

type RunRow = { id: string; status: string; spec_version: number | null };

describe.skipIf(!baseUrl)("an eve session moves to a fresh run through hook force-claim", () => {
  let worldTarball: string;

  beforeAll(() => {
    worldTarball = packWorld();
  });

  for (const [index, entry] of ENABLED_EVE_VERSIONS.entries()) {
    describe(`eve ${entry.version}`, () => {
      const tenantId = `${tenantFor(entry.version)}_handoff`;
      const deploymentId = `${deploymentFor(entry.version)}_handoff`;
      const database = `${databaseFor(entry.version)}_handoff`;
      const port = 41980 + index;

      let pool: Pool;
      let agent: StartedAgent | undefined;
      let world: ReturnType<typeof createWorld> | undefined;
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
        // Read-only use: decoding CBOR event payloads the SQL below cannot.
        world = createWorld({
          connectionString: databaseUrl,
          tenantId,
          deploymentId,
          runner: "external",
        });

        dir = buildAgent({ eveVersion: entry.version, worldTarball });
        agent = await startAgent({
          dir,
          port,
          databaseUrl,
          tenantId,
          deploymentId,
          env: { NODE_ENV: "test", EVE_MOCK_AUTHORED_MODELS: "1" },
        });
      });

      afterAll(async () => {
        await agent?.stop();
        await world?.close?.();
        await dropTenantPartitions(pool, tenantId).catch(() => {});
        await pool?.end().catch(() => {});
      });

      async function waitFor<T>(
        what: string,
        probe: () => Promise<T | undefined>,
        timeoutMs = 60_000,
      ): Promise<T> {
        const deadline = Date.now() + timeoutMs;
        for (;;) {
          const value = await probe();
          if (value !== undefined) return value;
          if (Date.now() > deadline) {
            throw new Error(
              `timed out waiting for ${what}. Agent log:\n${agent!.log().slice(-4000)}`,
            );
          }
          await new Promise((resolve) => setTimeout(resolve, 250));
        }
      }

      type Delivery = import("eve/client").Delivery;

      /** A client store for one session, from the eve the agent was built with. */
      async function openStore() {
        const { SessionStore } = (await import(
          pathToFileURL(createRequire(path.join(dir, "package.json")).resolve("eve/client")).href
        )) as typeof import("eve/client");
        return new SessionStore({ baseUrl: `http://127.0.0.1:${String(port)}` });
      }

      /**
       * Waits for an accepted command to settle and asserts it completed. A
       * delivery belongs to the session, not to a run, so this keeps working
       * across handoffs.
       */
      async function settled(delivery: Delivery, what: string, timeoutMs = 90_000) {
        let timer: NodeJS.Timeout | undefined;
        const timeout = new Promise<never>((_, reject) => {
          timer = setTimeout(
            () =>
              reject(new Error(`${what} never settled. Agent log:\n${agent!.log().slice(-4000)}`)),
            timeoutMs,
          );
        });
        try {
          const result = await Promise.race([delivery.result(), timeout]);
          expect(result.status, `${what}: ${JSON.stringify(result.error)}`).toBe("completed");
          return result;
        } finally {
          clearTimeout(timer);
        }
      }

      /** The session's owner runs other than its first, oldest first. */
      async function successors(sessionId: string): Promise<RunRow[]> {
        const { rows } = await pool.query<RunRow>(
          `select id, status, spec_version from workflow.workflow_runs
            where tenant_id = $1 and attributes ->> '$eve.session' = $2 and id <> $2
            order by id`,
          [tenantId, sessionId],
        );
        return rows;
      }

      async function runStatus(runId: string): Promise<string> {
        const { rows } = await pool.query<{ status: string }>(
          "select status from workflow.workflow_runs where tenant_id = $1 and id = $2",
          [tenantId, runId],
        );
        return rows[0]!.status;
      }

      /** Who holds the session inbox, once it is `owner`. */
      async function inboxHeldBy(sessionId: string, owner: string) {
        return waitFor(`the session inbox to move to ${owner}`, async () => {
          const hook = await world!.hooks.getByToken(inboxToken(sessionId)).catch(() => undefined);
          return hook?.runId === owner ? hook : undefined;
        });
      }

      /** The forced disposals this World wrote into `runId`'s log. */
      async function forcedDisposals(runId: string) {
        const page = await world!.events.list({ runId, pagination: { limit: 1000 } });
        return page.data.filter(
          (event) =>
            event.eventType === "hook_disposed" &&
            (event as { eventData?: { forceClaimedBy?: unknown } }).eventData?.forceClaimedBy !==
              undefined,
        );
      }

      /**
       * Deliveries to the inbox hook in `runId`. Not every one is a message: the
       * previous owner forwards what it accepted mid-handoff through the same
       * inbox, so callers compare counts across a message rather than reading
       * any delivery as one.
       */
      async function inboxDeliveries(runId: string, hookId: string): Promise<number> {
        const { rows } = await pool.query<{ count: string }>(
          `select count(*) from workflow.workflow_events
            where tenant_id = $1 and run_id = $2 and correlation_id = $3 and type = 'hook_received'`,
          [tenantId, runId, hookId],
        );
        return Number(rows[0]!.count);
      }

      /** Sends one message and waits for its turn to settle. */
      async function converse(
        store: Awaited<ReturnType<typeof openStore>>,
        message: string,
      ): Promise<void> {
        await settled(await store.send({ message }), `a reply to "${message}"`);
      }

      test("compaction hands the session to a new run, twice, and messages keep flowing", async () => {
        const store = await openStore();
        const stopObserving = store.observe();
        try {
          const opening = await store.send({ message: "hello from the e2e harness" });
          const { sessionId } = opening;
          await settled(opening, "the first turn");
          expect(await successors(sessionId)).toEqual([]);

          // First compaction: the first run hands over and stays as the anchor.
          const firstCompaction = await store.compact();
          const [first] = await waitFor("a successor run", async () => {
            const rows = await successors(sessionId);
            return rows.length >= 1 ? rows : undefined;
          });
          // eve 1.0 asks for single-orchestrator runs, which this World creates.
          expect(first!.spec_version).toBe(9);
          const firstInbox = await inboxHeldBy(sessionId, first!.id);
          expect(firstInbox.claimedFrom).toMatchObject({
            runId: sessionId,
            workflowName: "workflow//eve//workflowEntry",
            deploymentId,
            runSpecVersion: 9,
          });
          const takenFromAnchor = await forcedDisposals(sessionId);
          expect(takenFromAnchor.length).toBeGreaterThan(0);
          for (const disposal of takenFromAnchor) {
            expect(
              (disposal as { eventData?: { forceClaimedBy?: { runId: string } } }).eventData
                ?.forceClaimedBy?.runId,
            ).toBe(first!.id);
          }
          await settled(firstCompaction, "the first compaction");
          expect(await runStatus(sessionId)).toBe("running");

          // The same session id keeps working, and the new owner answers.
          const beforeFirst = await inboxDeliveries(first!.id, firstInbox.hookId);
          await converse(store, "message for the first successor");
          expect(await inboxDeliveries(first!.id, firstInbox.hookId)).toBeGreaterThan(beforeFirst);

          // Second compaction: the first successor hands over and finishes.
          const secondCompaction = await store.compact();
          const second = await waitFor("a second successor run", async () => {
            const rows = await successors(sessionId);
            return rows.length >= 2 ? rows[1] : undefined;
          });
          const secondInbox = await inboxHeldBy(sessionId, second.id);
          expect(secondInbox.claimedFrom).toMatchObject({ runId: first!.id });
          expect((await forcedDisposals(first!.id)).length).toBeGreaterThan(0);
          await waitFor(`${first!.id} to complete`, async () =>
            (await runStatus(first!.id)) === "completed" ? true : undefined,
          );
          await settled(secondCompaction, "the second compaction");
          const beforeSecond = await inboxDeliveries(second.id, secondInbox.hookId);
          await converse(store, "message for the second successor");
          expect(await inboxDeliveries(second.id, secondInbox.hookId)).toBeGreaterThan(
            beforeSecond,
          );

          // No message was ever journaled behind the disposal that retired its hook.
          for (const runId of [sessionId, first!.id]) {
            const page = await world!.events.list({ runId, pagination: { limit: 1000 } });
            const disposedAt = new Map<string, number>();
            page.data.forEach((event, position) => {
              if (event.eventType === "hook_disposed") {
                disposedAt.set(event.correlationId!, position);
              }
              if (event.eventType === "hook_received") {
                expect(disposedAt.has(event.correlationId!), `${runId} ${event.eventId}`).toBe(
                  false,
                );
              }
            });
          }

          const log = agent!.log();
          expect(log).not.toContain("session handoff unavailable");
          expect(log).not.toContain("session handoff failed");
        } finally {
          stopObserving();
        }
      });
    });
  }
});
