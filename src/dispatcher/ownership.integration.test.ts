import { Client, Pool } from "pg";
import { afterAll, afterEach, beforeAll, describe, expect, it, vi } from "vitest";
import type { DispatcherEvent, DispatcherTelemetry } from "./observability.js";
import {
  acquireDispatcherOwnership,
  DISPATCHER_OWNERSHIP_LOCK_KEY,
  OwnershipHeldElsewhereError,
  readDispatcherOwnershipHolder,
  terminateDispatcherOwnershipHolder,
  type DispatcherOwnership,
} from "./ownership.js";

/**
 * Dispatcher ownership against a real server.
 *
 * The lock itself was always correct; what these prove is the liveness around
 * it — that the owning session is configured to be found dead, that a
 * terminated owner is reported as lost rather than crashing the process, and
 * that a waiting dispatcher gets the lock the moment the holder is gone. A
 * mocked client cannot say any of that.
 */
const testUrl = process.env.EVELAND_WORKFLOW_WORLD_TEST_URL;

function collectingTelemetry(): DispatcherTelemetry & { events: DispatcherEvent[] } {
  const events: DispatcherEvent[] = [];
  return {
    events,
    emit(event) {
      events.push(event);
    },
    async shutdown() {},
  };
}

async function lockIsFree(pool: Pool): Promise<boolean> {
  return (await readDispatcherOwnershipHolder(pool)) === null;
}

describe.skipIf(!testUrl)("dispatcher ownership liveness", () => {
  let pool: Pool;
  const openOwnerships: DispatcherOwnership[] = [];
  const openClients: Client[] = [];

  beforeAll(() => {
    pool = new Pool({ connectionString: testUrl, max: 4 });
  });

  afterEach(async () => {
    for (const ownership of openOwnerships.splice(0)) await ownership.release();
    for (const client of openClients.splice(0)) await client.end().catch(() => {});
    await vi.waitFor(async () => expect(await lockIsFree(pool)).toBe(true));
  });

  afterAll(async () => {
    await pool.end();
  });

  it("configures the owning session so the server can find it dead", async () => {
    const telemetry = collectingTelemetry();
    const ownership = await acquireDispatcherOwnership(pool, {
      telemetry,
      livenessMs: 9_000,
      retryIntervalMs: 100,
      waitMs: 0,
      onLost: () => {},
    });
    openOwnerships.push(ownership);

    const show = async (name: string) =>
      (await ownership.client.query<Record<string, string>>(`show ${name}`)).rows[0]?.[name];
    // Derived from the 9s budget: idle L/3, interval L/9, three probes,
    // idle_session_timeout L. The server reports the timeout in its own unit.
    expect(await show("tcp_keepalives_idle")).toBe("3");
    expect(await show("tcp_keepalives_interval")).toBe("1");
    expect(await show("tcp_keepalives_count")).toBe("3");
    expect(await show("idle_session_timeout")).toBe("9s");
    expect(telemetry.events.map((event) => event.eventName)).not.toContain(
      "workflow_dispatcher.ownership_liveness_unavailable",
    );

    const holder = await readDispatcherOwnershipHolder(pool);
    const { rows } = await ownership.client.query<{ pid: number }>(
      "select pg_backend_pid() as pid",
    );
    expect(holder?.pid).toBe(rows[0]?.pid);
  });

  it("releases cleanly and leaves the lock free", async () => {
    const ownership = await acquireDispatcherOwnership(pool, {
      telemetry: collectingTelemetry(),
      livenessMs: 9_000,
      retryIntervalMs: 100,
      waitMs: 0,
      onLost: () => {},
    });
    expect(await lockIsFree(pool)).toBe(false);
    await ownership.release();
    expect(ownership.lost).toBe(false);
    expect(await lockIsFree(pool)).toBe(true);
    // Idempotent, and the pool still has its full complement of clients.
    await ownership.release();
    expect(pool.totalCount).toBeLessThanOrEqual(4);
  });

  it("waits for a holder to go away and names it while waiting", async () => {
    const holder = new Client({ connectionString: testUrl, application_name: "old-dispatcher" });
    openClients.push(holder);
    await holder.connect();
    await holder.query("select pg_try_advisory_lock($1)", [DISPATCHER_OWNERSHIP_LOCK_KEY]);
    const holderPid = (await holder.query<{ pid: number }>("select pg_backend_pid() as pid"))
      .rows[0]?.pid;

    const telemetry = collectingTelemetry();
    const acquiring = acquireDispatcherOwnership(pool, {
      telemetry,
      livenessMs: 9_000,
      retryIntervalMs: 100,
      waitMs: null,
      onLost: () => {},
    });

    await vi.waitFor(() => {
      expect(telemetry.events.map((event) => event.eventName)).toContain(
        "workflow_dispatcher.ownership_wait",
      );
    });
    const wait = telemetry.events.find(
      (event) => event.eventName === "workflow_dispatcher.ownership_wait",
    );
    expect(wait?.attributes?.["ownership.holder.pid"]).toBe(holderPid);
    expect(wait?.attributes?.["ownership.holder.application_name"]).toBe("old-dispatcher");
    expect(wait?.body).toContain(`pid ${String(holderPid)}`);

    // The holder's session ends — the waiting dispatcher gets the lock.
    await holder.end();
    const ownership = await acquiring;
    openOwnerships.push(ownership);
    expect(ownership.lost).toBe(false);
    expect((await readDispatcherOwnershipHolder(pool))?.pid).not.toBe(holderPid);
  });

  it("fails fast with the holder attached when told not to wait", async () => {
    const holder = new Client({ connectionString: testUrl });
    openClients.push(holder);
    await holder.connect();
    await holder.query("select pg_try_advisory_lock($1)", [DISPATCHER_OWNERSHIP_LOCK_KEY]);
    const holderPid = (await holder.query<{ pid: number }>("select pg_backend_pid() as pid"))
      .rows[0]?.pid;

    const attempt = acquireDispatcherOwnership(pool, {
      telemetry: collectingTelemetry(),
      livenessMs: 9_000,
      retryIntervalMs: 100,
      waitMs: 0,
      onLost: () => {},
    });
    await expect(attempt).rejects.toBeInstanceOf(OwnershipHeldElsewhereError);
    await expect(attempt).rejects.toThrow(`pid ${String(holderPid)}`);
    await attempt.catch((error: OwnershipHeldElsewhereError) => {
      expect(error.holder?.pid).toBe(holderPid);
    });
  });

  it("reports the loss instead of crashing when the owning session is terminated", async () => {
    const telemetry = collectingTelemetry();
    const onLost = vi.fn();
    const ownership = await acquireDispatcherOwnership(pool, {
      telemetry,
      livenessMs: 3_000,
      retryIntervalMs: 100,
      waitMs: 0,
      onLost,
    });
    const ownerPid = (await readDispatcherOwnershipHolder(pool))?.pid;
    expect(ownerPid).toBeDefined();

    // What an operator, or the server's own idle_session_timeout, does to a
    // holder it believes dead. Without a listener on the checked-out client
    // this is an unhandled 'error' event on the process.
    expect(await terminateDispatcherOwnershipHolder(pool, ownerPid!)).toBe(true);

    await vi.waitFor(() => expect(onLost).toHaveBeenCalledTimes(1), { timeout: 5_000 });
    expect(ownership.lost).toBe(true);
    expect(telemetry.events.map((event) => event.eventName)).toContain(
      "workflow_dispatcher.ownership_lost",
    );
    expect(await lockIsFree(pool)).toBe(true);

    // Release after a loss destroys the dead client rather than trying to
    // unlock on it, and never throws.
    await ownership.release();
    expect(onLost).toHaveBeenCalledTimes(1);
  });

  it("lets the server reclaim the lock from an owner that goes silent", async () => {
    // The server-side layer on its own: an idle session past
    // idle_session_timeout is terminated and its session-level advisory lock
    // released, with no cooperation from the (wedged) client.
    const silent = new Client({ connectionString: testUrl });
    openClients.push(silent);
    await silent.connect();
    silent.on("error", () => {});
    await silent.query("set idle_session_timeout = 1000");
    await silent.query("select pg_try_advisory_lock($1)", [DISPATCHER_OWNERSHIP_LOCK_KEY]);
    expect(await lockIsFree(pool)).toBe(false);

    await vi.waitFor(async () => expect(await lockIsFree(pool)).toBe(true), { timeout: 5_000 });

    const next = await acquireDispatcherOwnership(pool, {
      telemetry: collectingTelemetry(),
      livenessMs: 9_000,
      retryIntervalMs: 100,
      waitMs: 0,
      onLost: () => {},
    });
    openOwnerships.push(next);
  });
});
