import type { Event } from "@workflow/world";
import { eventIdToSlot } from "@workflow/world";
import { Client, Pool } from "pg";
import { monotonicFactory } from "ulid";
import { afterAll, beforeAll, describe, expect, test } from "vitest";
// The runtime's own feed client, from the installed `@workflow/core` (not an
// exported path): what eve's delivery runs while it waits or lingers.
import { LiveLogFeed } from "../node_modules/@workflow/core/dist/runtime/spec9/live-feed.js";
import {
  DEPLOYMENT_HEADER,
  DISPATCH_VERSION,
  DISPATCH_VERSION_HEADER,
  LIVE_FEED_HEADER,
  RUNTIME_SECRET_ENV_NAME,
  RUNTIME_SECRET_HEADER,
  VQS_MESSAGE_ATTEMPT_HEADER,
  VQS_MESSAGE_ID_HEADER,
  VQS_QUEUE_NAME_HEADER,
} from "./dispatch-contract.js";
import { runDelivery } from "./delivery-context.js";
import { createWorld } from "./index.js";
import {
  dropTenantPartitions,
  ensureTenantPartitions,
  resolveMigrationsDir,
  runMigrations,
} from "./migrate.js";
import { tenantEventsChannel, tenantStreamChannel } from "./tenant.js";

/**
 * The live event feed (`events.subscribe`) against a real database: writes
 * announce on the tenant's channel and reach a granted subscriber in slot
 * order, another tenant's writes reach no one, the grant travels from the
 * dispatch header through the real `createQueueHandler` to the runtime's own
 * `LiveLogFeed`, and a terminated `LISTEN` connection fails the feeds and is
 * replaced, with stream readers carrying on. The feed's slot rules are in
 * `run-events.test.ts`.
 */
const testUrl = process.env.WORKFLOW_WORLD_TEST_URL;
const suffix = `${String(process.pid)}${Date.now().toString(36)}`;
const ALPHA = `p_feed_a_${suffix}`;
const BETA = `p_feed_b_${suffix}`;
const SECRET = "feed-secret";
const ulid = monotonicFactory();

type World = ReturnType<typeof createWorld>;

async function waitFor(check: () => boolean | Promise<boolean>, timeoutMs = 10_000) {
  const deadline = Date.now() + timeoutMs;
  while (!(await check())) {
    if (Date.now() > deadline) throw new Error("timed out waiting");
    await new Promise((resolve) => setTimeout(resolve, 25));
  }
}

async function createRun(world: World): Promise<string> {
  const result = await world.events.create(null, {
    eventType: "run_created",
    specVersion: 9,
    eventData: { deploymentId: "dep_feed", workflowName: "feed", input: new Uint8Array([1]) },
  });
  return (result.run?.runId ?? result.event?.runId)!;
}

/** An out-of-band write, as a background step or `resumeHook()` makes. */
async function appendWait(world: World, runId: string): Promise<number> {
  const result = await world.events.create(runId, {
    eventType: "wait_created",
    correlationId: `wait_${ulid()}`,
    specVersion: 9,
    eventData: { resumeAt: new Date(Date.now() + 60_000) },
  });
  return eventIdToSlot(result.event!.eventId)!;
}

/** A granted subscription inside a delivery that stays open until `end()`. */
function subscribeGranted(world: World, runId: string, afterSlot = 1) {
  const slots: number[] = [];
  const errors: unknown[] = [];
  let release!: () => void;
  const delivery = runDelivery({ liveFeed: true }, async () => {
    world.events.subscribe!(
      runId,
      afterSlot,
      (event: Event) => slots.push(eventIdToSlot(event.eventId)!),
      { onError: (error) => errors.push(error) },
    );
    await new Promise<void>((resolve) => {
      release = resolve;
    });
  });
  return {
    slots,
    errors,
    end: async () => {
      release();
      await delivery;
    },
  };
}

describe.skipIf(!testUrl)("live event feed", () => {
  let pool: Pool;
  let alpha: World;
  let beta: World;

  beforeAll(async () => {
    pool = new Pool({ connectionString: testUrl, max: 8 });
    await runMigrations(pool, { migrationsDir: resolveMigrationsDir() });
    await ensureTenantPartitions(pool, ALPHA);
    await ensureTenantPartitions(pool, BETA);
    alpha = createWorld({ pool, tenantId: ALPHA, deploymentId: "dep_feed_a" });
    beta = createWorld({ pool, tenantId: BETA, deploymentId: "dep_feed_b" });
  }, 60_000);

  afterAll(async () => {
    await alpha?.close?.();
    await beta?.close?.();
    await dropTenantPartitions(pool, ALPHA).catch(() => {});
    await dropTenantPartitions(pool, BETA).catch(() => {});
    await pool?.end().catch(() => {});
  });

  test("committed writes reach a granted subscriber in slot order, each once", async () => {
    const runId = await createRun(alpha);
    const before = await appendWait(alpha, runId);
    const sub = subscribeGranted(alpha, runId);
    try {
      const written = [before];
      for (let i = 0; i < 5; i++) written.push(await appendWait(alpha, runId));
      await waitFor(() => sub.slots.length >= written.length);
      // A short wait for anything delivered twice.
      await new Promise((resolve) => setTimeout(resolve, 200));
      expect(sub.slots).toEqual(written);
      expect(sub.errors).toEqual([]);
    } finally {
      await sub.end();
    }
  });

  test("another tenant's writes never reach a subscriber", async () => {
    const alphaChannel = new Client({ connectionString: testUrl });
    const betaChannel = new Client({ connectionString: testUrl });
    const heard: Record<string, string[]> = { alpha: [], beta: [] };
    await alphaChannel.connect();
    await betaChannel.connect();
    try {
      alphaChannel.on("notification", (msg) => heard.alpha!.push(msg.payload ?? ""));
      betaChannel.on("notification", (msg) => heard.beta!.push(msg.payload ?? ""));
      await alphaChannel.query(`LISTEN "${tenantEventsChannel(ALPHA)}"`);
      await betaChannel.query(`LISTEN "${tenantEventsChannel(BETA)}"`);
      expect(tenantEventsChannel(ALPHA)).not.toBe(tenantEventsChannel(BETA));

      const betaRun = await createRun(beta);
      // Alpha subscribes to beta's run id: nothing it can be told or read.
      const sub = subscribeGranted(alpha, betaRun, 0);
      try {
        await appendWait(beta, betaRun);
        await appendWait(beta, betaRun);
        await waitFor(() => heard.beta!.length >= 3);
        await new Promise((resolve) => setTimeout(resolve, 200));
        expect(heard.beta).toContain(betaRun);
        expect(heard.alpha).toEqual([]);
        expect(sub.slots).toEqual([]);
        expect(sub.errors).toEqual([]);
      } finally {
        await sub.end();
      }

      const alphaRun = await createRun(alpha);
      await waitFor(() => heard.alpha!.includes(alphaRun));
      expect(heard.beta).not.toContain(alphaRun);
    } finally {
      await alphaChannel.end();
      await betaChannel.end();
    }
  });

  describe("through the real createQueueHandler", () => {
    const previousSecret = process.env[RUNTIME_SECRET_ENV_NAME];
    beforeAll(() => {
      process.env[RUNTIME_SECRET_ENV_NAME] = SECRET;
    });
    afterAll(() => {
      if (previousSecret === undefined) delete process.env[RUNTIME_SECRET_ENV_NAME];
      else process.env[RUNTIME_SECRET_ENV_NAME] = previousSecret;
    });

    /**
     * Delivers one orchestrator message to `world`'s flow handler, where the
     * runtime's `LiveLogFeed` starts, as eve's delivery starts it, and an
     * out-of-band write lands while it is open.
     */
    async function deliver(world: World, headers: Record<string, string>) {
      const runId = await createRun(world);
      const seen: { live?: boolean; pushed: number[] } = { pushed: [] };
      const handler = world.createQueueHandler("__wkf_workflow_", async () => {
        const feed = new LiveLogFeed(world, runId, {
          afterSlot: 1,
          cursor: null,
          // The poll stays out of the way: what arrives is the push.
          pollIntervalMs: 60_000,
          feedPollIntervalMs: 60_000,
          onEvents: (events) => seen.pushed.push(...events.map((e) => eventIdToSlot(e.eventId)!)),
        });
        feed.start();
        try {
          // A refusal arrives in a microtask; give it every chance.
          await new Promise((resolve) => setTimeout(resolve, 50));
          seen.live = feed.isLive;
          if (seen.live) {
            const slot = await appendWait(world, runId);
            await waitFor(() => seen.pushed.includes(slot), 5_000);
          }
        } finally {
          feed.stop();
        }
        return undefined;
      });
      const response = await handler(
        new Request("http://localhost/.well-known/workflow/v1/flow", {
          method: "POST",
          headers: {
            "content-type": "application/json",
            [VQS_QUEUE_NAME_HEADER]: "__wkf_workflow_feed",
            [VQS_MESSAGE_ID_HEADER]: `msg_${ulid()}`,
            [VQS_MESSAGE_ATTEMPT_HEADER]: "1",
            [DISPATCH_VERSION_HEADER]: String(DISPATCH_VERSION),
            [RUNTIME_SECRET_HEADER]: SECRET,
            [DEPLOYMENT_HEADER]: "dep_feed_a",
            ...headers,
          },
          body: JSON.stringify({ runId, runSpecVersion: 9 }),
        }),
      );
      expect(response.status).toBe(200);
      return seen;
    }

    test("a granted delivery's context reaches subscribe, and the runtime's feed goes live", async () => {
      const seen = await deliver(alpha, { [LIVE_FEED_HEADER]: "1" });
      expect(seen.live).toBe(true);
      expect(seen.pushed).toHaveLength(1);
    });

    test("a delivery from a dispatcher that sends no grant gets no feed, as before", async () => {
      const seen = await deliver(alpha, {});
      expect(seen.live).toBe(false);
      expect(seen.pushed).toEqual([]);
    });

    test("any other header value is no grant", async () => {
      const seen = await deliver(alpha, { [LIVE_FEED_HEADER]: "true" });
      expect(seen.live).toBe(false);
    });
  });

  test("a terminated LISTEN connection fails the feeds and is replaced; stream readers carry on", async () => {
    const runId = await createRun(alpha);
    const stream = `strm_${ulid()}`;
    const reader = (await alpha.streams.get(runId, stream)).getReader();
    const text = (value: string) => new TextEncoder().encode(value);
    const decode = (value: Uint8Array | undefined) => new TextDecoder().decode(value);

    await alpha.streams.write(runId, stream, text("before"));
    expect(decode((await reader.read()).value)).toBe("before");

    const sub = subscribeGranted(alpha, runId);
    try {
      await waitFor(async () => (await listenerPids()).length === 1);
      const [pid] = await listenerPids();
      await pool.query("select pg_terminate_backend($1)", [pid]);

      // Every live subscription ends through onError.
      await waitFor(() => sub.errors.length === 1);
      expect(String(sub.errors[0])).toMatch(/LISTEN connection/);

      // Written while the connection is down: its NOTIFY reaches no one, and
      // the reader is woken by the reconnect instead.
      await alpha.streams.write(runId, stream, text("during"));
      const during = await reader.read();
      expect(decode(during.value)).toBe("during");

      // A fresh connection, listening on both channels again.
      await waitFor(async () => {
        const pids = await listenerPids();
        return pids.length === 1 && pids[0] !== pid;
      });
      await alpha.streams.write(runId, stream, text("after"));
      expect(decode((await reader.read()).value)).toBe("after");

      const again = subscribeGranted(alpha, runId);
      try {
        const slot = await appendWait(alpha, runId);
        await waitFor(() => again.slots.includes(slot));
        expect(again.errors).toEqual([]);
      } finally {
        await again.end();
      }
    } finally {
      await sub.end();
      await reader.cancel();
    }

    /** The backend of alpha's LISTEN connection; its last query is a LISTEN. */
    async function listenerPids(): Promise<number[]> {
      const { rows } = await pool.query<{ pid: number }>(
        `select pid from pg_stat_activity where query = any($1::text[])`,
        [[`LISTEN "${tenantStreamChannel(ALPHA)}"`, `LISTEN "${tenantEventsChannel(ALPHA)}"`]],
      );
      return rows.map((row) => row.pid);
    }
  }, 30_000);

  test("close ends the World's feeds through onError", async () => {
    const world = createWorld({ pool, tenantId: ALPHA, deploymentId: "dep_feed_a" });
    const runId = await createRun(world);
    const sub = subscribeGranted(world, runId);
    try {
      await new Promise((resolve) => setTimeout(resolve, 100));
      await world.close?.();
      expect(sub.errors).toHaveLength(1);
      expect(String(sub.errors[0])).toMatch(/closed/);
    } finally {
      await sub.end();
    }
  });
});
