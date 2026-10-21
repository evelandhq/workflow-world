import { Pool } from "pg";
import { afterAll, beforeAll, describe, expect, test } from "vitest";
import { createWorld, ensureTenantPartitions, runMigrations } from "../src/index.js";
import { dropTenantPartitions } from "../src/migrate.js";
import { ENABLED_EVE_VERSIONS, handsOffByForceClaim } from "./eve-versions.mts";
import {
  buildAgent,
  databaseFor,
  deploymentFor,
  packWorld,
  startAgent,
  startSession,
  tenantFor,
  type StartedAgent,
} from "./harness.mts";

/**
 * A real eve 0.81 session moves to a fresh workflow run through this World.
 *
 * eve 0.81.1 hands a session over -- after a compaction, or onto a newer
 * deployment -- by starting a successor `workflowEntry` run that takes every
 * session hook token over with `createHook({ experimental_force: true })`. It
 * does so only when the World declares `capabilities.hookForceClaim` and
 * `capabilities.hookRetention`; otherwise it logs `session handoff unavailable`
 * and the session stays in its first run for good, where its event log grows
 * until `WORKFLOW_MAX_EVENTS` fails it. So this is the end-to-end proof of the
 * force-claim takeover (`docs/design.md`, Hook force-claim): eve's own
 * compaction, eve's own successor, eve's own messages, this World in between.
 *
 * Compaction is triggered by hand (`POST /eve/v1/session/:id/compact`), which
 * runs the same compaction turn the context-window threshold would and arms
 * the same handoff. The fixture runs on eve's mock model (vitest's
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
    describe.skipIf(!handsOffByForceClaim(entry.version))(`eve ${entry.version}`, () => {
      const tenantId = `${tenantFor(entry.version)}_handoff`;
      const deploymentId = `${deploymentFor(entry.version)}_handoff`;
      const database = `${databaseFor(entry.version)}_handoff`;
      const port = 41980 + index;

      let pool: Pool;
      let agent: StartedAgent | undefined;
      let world: ReturnType<typeof createWorld> | undefined;

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

        const dir = buildAgent({ eveVersion: entry.version, worldTarball });
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

      type StreamEvent = { type?: string; data?: Record<string, unknown> };

      /**
       * Reads the session's NDJSON stream from its start until `done` holds for
       * everything seen so far. The stream belongs to the session, not to a run:
       * every owner writes to the first run's stream, so this keeps working
       * across handoffs.
       */
      async function waitForStream(
        sessionId: string,
        what: string,
        done: (events: StreamEvent[]) => boolean,
        timeoutMs = 90_000,
      ): Promise<StreamEvent[]> {
        const controller = new AbortController();
        const timer = setTimeout(() => controller.abort(), timeoutMs);
        const seen: StreamEvent[] = [];
        let buffered = "";
        try {
          const response = await fetch(
            `http://127.0.0.1:${String(port)}/eve/v1/session/${sessionId}/stream`,
            { signal: controller.signal },
          );
          expect(response.status).toBe(200);
          const reader = response.body!.getReader();
          const decoder = new TextDecoder();
          for (;;) {
            const { value, done: ended } = await reader.read();
            if (ended) break;
            buffered += decoder.decode(value, { stream: true });
            const lines = buffered.split("\n");
            buffered = lines.pop() ?? "";
            for (const line of lines.filter(Boolean)) {
              seen.push(JSON.parse(line) as StreamEvent);
            }
            if (done(seen)) {
              controller.abort();
              return seen;
            }
          }
        } catch (error) {
          if (!controller.signal.aborted) throw error;
        } finally {
          clearTimeout(timer);
        }
        throw new Error(
          `stream never showed ${what}; saw ${seen.map((e) => e.type).join(", ")}. Agent log:\n${agent!.log().slice(-4000)}`,
        );
      }

      const count = (events: StreamEvent[], type: string) =>
        events.filter((event) => event.type === type).length;

      /** Waits until the session has settled `compactions` compaction turns. */
      async function compacted(sessionId: string, compactions: number): Promise<void> {
        await waitForStream(sessionId, `${String(compactions)} settled compaction(s)`, (events) => {
          const completed = events.findLastIndex((e) => e.type === "compaction.completed");
          return (
            count(events, "compaction.completed") >= compactions &&
            events.slice(completed).some((e) => e.type === "session.waiting")
          );
        });
      }

      async function post(path: string, body: unknown): Promise<void> {
        const response = await fetch(`http://127.0.0.1:${String(port)}${path}`, {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify(body),
        });
        expect(response.status, await response.clone().text()).toBeLessThan(300);
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

      /** Sends one message and waits for the mock model's reply to it on the stream. */
      async function converse(sessionId: string, message: string): Promise<void> {
        await post(`/eve/v1/session/${sessionId}`, { message });
        await waitForStream(sessionId, `a reply to "${message}"`, (events) =>
          events.some(
            (event) =>
              event.type === "message.completed" &&
              String(event.data?.message ?? "").includes(message),
          ),
        );
      }

      test("compaction hands the session to a new run, twice, and messages keep flowing", async () => {
        const { sessionId } = await startSession(port);
        await waitForStream(sessionId, "the first turn parked", (events) =>
          events.some((event) => event.type === "session.waiting"),
        );
        expect(await successors(sessionId)).toEqual([]);

        // First compaction: the first run hands over and stays as the anchor.
        await post(`/eve/v1/session/${sessionId}/compact`, {});
        const [first] = await waitFor("a successor run", async () => {
          const rows = await successors(sessionId);
          return rows.length >= 1 ? rows : undefined;
        });
        expect(first!.spec_version).toBe(8);
        const firstInbox = await inboxHeldBy(sessionId, first!.id);
        expect(firstInbox.claimedFrom).toMatchObject({
          runId: sessionId,
          workflowName: "workflow//eve//workflowEntry",
          deploymentId,
          runSpecVersion: 8,
        });
        const takenFromAnchor = await forcedDisposals(sessionId);
        expect(takenFromAnchor.length).toBeGreaterThan(0);
        for (const disposal of takenFromAnchor) {
          expect(
            (disposal as { eventData?: { forceClaimedBy?: { runId: string } } }).eventData
              ?.forceClaimedBy?.runId,
          ).toBe(first!.id);
        }
        await compacted(sessionId, 1);
        expect(await runStatus(sessionId)).toBe("running");

        // The same session id keeps working, and the new owner answers.
        const beforeFirst = await inboxDeliveries(first!.id, firstInbox.hookId);
        await converse(sessionId, "message for the first successor");
        expect(await inboxDeliveries(first!.id, firstInbox.hookId)).toBeGreaterThan(beforeFirst);

        // Second compaction: the first successor hands over and finishes.
        await post(`/eve/v1/session/${sessionId}/compact`, {});
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
        await compacted(sessionId, 2);
        const beforeSecond = await inboxDeliveries(second.id, secondInbox.hookId);
        await converse(sessionId, "message for the second successor");
        expect(await inboxDeliveries(second.id, secondInbox.hookId)).toBeGreaterThan(beforeSecond);

        // No message was ever journaled behind the disposal that retired its hook.
        for (const runId of [sessionId, first!.id]) {
          const page = await world!.events.list({ runId, pagination: { limit: 1000 } });
          const disposedAt = new Map<string, number>();
          page.data.forEach((event, position) => {
            if (event.eventType === "hook_disposed") {
              disposedAt.set(event.correlationId!, position);
            }
            if (event.eventType === "hook_received") {
              expect(disposedAt.has(event.correlationId!), `${runId} ${event.eventId}`).toBe(false);
            }
          });
        }

        const log = agent!.log();
        expect(log).not.toContain("session handoff unavailable");
        expect(log).not.toContain("session handoff failed");
      });
    });
  }
});
