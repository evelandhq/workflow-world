import { setTimeout as delay } from "node:timers/promises";
import type { Pool, PoolClient } from "pg";
import type { DispatcherTelemetry } from "./observability.js";

/**
 * The session-level advisory lock that makes the dispatcher a singleton per
 * database. Held on one checked-out client for the whole process lifetime.
 */
export const DISPATCHER_OWNERSHIP_LOCK_KEY = 0x65_76_64_70; // "evdp"

/** The floor below which the derived keepalive/heartbeat cadence stops making sense. */
export const MIN_OWNERSHIP_LIVENESS_MS = 3_000;

/**
 * A session-level advisory lock is released when its *session* ends, and
 * PostgreSQL only learns that a session has ended when the socket closes. A
 * dispatcher that crashes on the same host closes its socket at once; one whose
 * host lost power, or whose connection crosses a NAT, VPN, or port forward that
 * swallows the close, leaves an idle backend holding the lock until the
 * kernel's TCP keepalive gives up — two hours and more at Linux defaults. Every
 * restart in that window fails to acquire ownership.
 *
 * So the owning session is made to prove it is alive, in two independent
 * layers, both derived from one liveness budget `L`:
 *
 * - **Server-side TCP keepalives** (`tcp_keepalives_*`, per-session GUCs that
 *   apply to the backend's own socket): idle `L/3`, interval `L/9`, three
 *   probes. A peer that is gone is noticed within `L/3 + 3·L/9 = 2L/3`. This is
 *   the layer that catches the dead-host and swallowed-close cases.
 * - **`idle_session_timeout = L`** (PostgreSQL 14+) plus a `select 1` heartbeat
 *   every `L/3` on the owning client. A session that stops heartbeating — the
 *   process is wedged, or the network black-holes — is terminated by the
 *   server itself and its lock freed at `L`. The heartbeat is the same signal
 *   from the client's side: when it fails or does not answer inside `L/3`, the
 *   owner has lost its claim and must stop rather than keep dispatching.
 *
 * Neither layer is a heartbeat table or a lease of our own: the lock is still
 * the single source of truth, the layers only make the session that holds it
 * honest about being alive.
 */
export type OwnershipLivenessSettings = {
  keepalivesIdleSeconds: number;
  keepalivesIntervalSeconds: number;
  keepalivesCount: number;
  idleSessionTimeoutMs: number;
  heartbeatIntervalMs: number;
};

export function deriveOwnershipLiveness(livenessMs: number): OwnershipLivenessSettings {
  const budget = Math.max(MIN_OWNERSHIP_LIVENESS_MS, Math.floor(livenessMs));
  const third = Math.floor(budget / 3);
  return {
    keepalivesIdleSeconds: Math.max(1, Math.floor(third / 1_000)),
    keepalivesIntervalSeconds: Math.max(1, Math.floor(third / 3_000)),
    keepalivesCount: 3,
    idleSessionTimeoutMs: budget,
    heartbeatIntervalMs: third,
  };
}

export type OwnershipHolder = {
  pid: number;
  applicationName: string | null;
  clientAddr: string | null;
  backendStart: string | null;
  state: string | null;
  stateChange: string | null;
};

type Queryable = Pick<Pool, "query"> | Pick<PoolClient, "query">;

/**
 * Who holds the dispatcher's ownership lock right now, from `pg_locks` joined to
 * `pg_stat_activity`. Only rows visible to the connected role carry details;
 * the pid is always there. `null` when nobody holds it.
 */
export async function readDispatcherOwnershipHolder(
  db: Queryable,
  lockKey: number = DISPATCHER_OWNERSHIP_LOCK_KEY,
): Promise<OwnershipHolder | null> {
  // A bigint advisory key shows up in pg_locks split into two 32-bit halves.
  const key = BigInt(lockKey);
  const classid = Number(key >> 32n);
  const objid = Number(key & 0xff_ff_ff_ffn);
  const { rows } = await (db as Pick<Pool, "query">).query<{
    pid: number;
    application_name: string | null;
    client_addr: string | null;
    backend_start: Date | string | null;
    state: string | null;
    state_change: Date | string | null;
  }>(
    `select l.pid, a.application_name, host(a.client_addr) as client_addr,
            a.backend_start, a.state, a.state_change
       from pg_locks l
       left join pg_stat_activity a on a.pid = l.pid
      where l.locktype = 'advisory'
        and l.database = (select oid from pg_database where datname = current_database())
        and l.classid = $1 and l.objid = $2 and l.objsubid = 1
        and l.granted
      order by l.pid
      limit 1`,
    [classid, objid],
  );
  const row = rows[0];
  if (!row) return null;
  return {
    pid: row.pid,
    applicationName: row.application_name,
    clientAddr: row.client_addr,
    backendStart: isoOrNull(row.backend_start),
    state: row.state,
    stateChange: isoOrNull(row.state_change),
  };
}

/**
 * Terminate the backend holding the ownership lock. This is the operator's
 * escape hatch for a holder the liveness layers cannot reach (an older
 * dispatcher that never set them, or a server that ignores keepalives): it
 * needs the same role as the holder, or superuser. Returns whether a backend
 * with that pid was signalled.
 */
export async function terminateDispatcherOwnershipHolder(
  db: Queryable,
  pid: number,
): Promise<boolean> {
  const { rows } = await (db as Pick<Pool, "query">).query<{ terminated: boolean }>(
    "select pg_terminate_backend($1) as terminated",
    [pid],
  );
  return rows[0]?.terminated === true;
}

export function describeOwnershipHolder(holder: OwnershipHolder | null): string {
  if (!holder) return "holder not visible";
  const parts = [`pid ${String(holder.pid)}`];
  if (holder.applicationName) parts.push(holder.applicationName);
  if (holder.clientAddr) parts.push(`from ${holder.clientAddr}`);
  if (holder.backendStart) parts.push(`connected ${holder.backendStart}`);
  if (holder.state) {
    parts.push(holder.stateChange ? `${holder.state} since ${holder.stateChange}` : holder.state);
  }
  return parts.join(", ");
}

export type AcquireOwnershipOptions = {
  telemetry: DispatcherTelemetry;
  /** The liveness budget `L` above. */
  livenessMs: number;
  /** How long to wait between attempts while another session holds the lock. */
  retryIntervalMs: number;
  /** Give up after this long; `null` waits until the lock is free. `0` fails on the first miss. */
  waitMs: number | null;
  /**
   * Called once, when the owning session is found dead: the heartbeat failed
   * or did not answer within its interval. The lock is no longer ours.
   */
  onLost: (error: unknown) => void;
  lockKey?: number;
};

export type DispatcherOwnership = {
  /** The client the lock lives on. Stays checked out until `release()`. */
  client: PoolClient;
  readonly lost: boolean;
  /** Unlock and return the client. After a loss the client is destroyed instead. */
  release(): Promise<void>;
};

export class OwnershipHeldElsewhereError extends Error {
  readonly holder: OwnershipHolder | null;
  constructor(holder: OwnershipHolder | null) {
    super(
      `Another workflow dispatcher already owns this database (${describeOwnershipHolder(holder)}).`,
    );
    this.name = "OwnershipHeldElsewhereError";
    this.holder = holder;
  }
}

export async function acquireDispatcherOwnership(
  pool: Pool,
  options: AcquireOwnershipOptions,
): Promise<DispatcherOwnership> {
  const lockKey = options.lockKey ?? DISPATCHER_OWNERSHIP_LOCK_KEY;
  const liveness = deriveOwnershipLiveness(options.livenessMs);
  const client = await pool.connect();

  try {
    const startedAt = Date.now();
    for (let attempt = 1; ; attempt += 1) {
      const { rows } = await client.query<{ locked: boolean }>(
        "select pg_try_advisory_lock($1) as locked",
        [lockKey],
      );
      if (rows[0]?.locked === true) break;

      const holder = await readDispatcherOwnershipHolder(client, lockKey).catch(() => null);
      const waitedMs = Date.now() - startedAt;
      if (options.waitMs !== null && waitedMs >= options.waitMs) {
        throw new OwnershipHeldElsewhereError(holder);
      }
      options.telemetry.emit({
        severity: "warn",
        eventName: "workflow_dispatcher.ownership_wait",
        body: `another workflow dispatcher owns this database (${describeOwnershipHolder(holder)}); retrying in ${String(options.retryIntervalMs)}ms`,
        attributes: {
          attempt,
          "ownership.waited_ms": waitedMs,
          ...(holder ? holderAttributes(holder) : {}),
        },
      });
      await delay(options.retryIntervalMs);
    }
  } catch (error) {
    client.release();
    throw error;
  }

  await configureSessionLiveness(client, liveness, options.telemetry);

  let lost = false;
  let released = false;
  let heartbeatTimer: NodeJS.Timeout | undefined;

  const markLost = (error: unknown) => {
    if (lost || released) return;
    lost = true;
    if (heartbeatTimer) clearTimeout(heartbeatTimer);
    options.telemetry.emit({
      severity: "error",
      eventName: "workflow_dispatcher.ownership_lost",
      body: `the session holding dispatcher ownership is gone; this dispatcher no longer owns the database: ${String(error)}`,
    });
    try {
      options.onLost(error);
    } catch {
      // The host's reaction must not mask the loss itself.
    }
  };

  // pg-pool detaches its own error listener while a client is checked out, so
  // the server ending this session (idle_session_timeout, pg_terminate_backend,
  // a restart) would surface as an unhandled 'error' event and crash the
  // process. Route it into the loss path instead; `release()` sets `released`
  // first, so our own teardown never reads as a loss.
  const onClientError = (error: Error) => markLost(error);
  const onClientEnd = () => markLost(new Error("the ownership session ended"));
  client.on("error", onClientError);
  client.once("end", onClientEnd);

  const heartbeat = async () => {
    heartbeatTimer = undefined;
    if (lost || released) return;
    try {
      await withTimeout(
        client.query("select 1"),
        liveness.heartbeatIntervalMs,
        "ownership heartbeat did not answer",
      );
    } catch (error) {
      markLost(error);
      return;
    }
    scheduleHeartbeat();
  };
  const scheduleHeartbeat = () => {
    if (lost || released) return;
    heartbeatTimer = setTimeout(() => void heartbeat(), liveness.heartbeatIntervalMs);
    heartbeatTimer.unref();
  };
  scheduleHeartbeat();

  return {
    client,
    get lost() {
      return lost;
    },
    async release() {
      if (released) return;
      released = true;
      if (heartbeatTimer) clearTimeout(heartbeatTimer);
      client.removeListener("end", onClientEnd);
      let destroy = lost;
      if (!lost) {
        // A hung unlock must not hang shutdown: if the session does not answer,
        // destroying the connection releases the lock just as well.
        await withTimeout(
          client.query("select pg_advisory_unlock($1)", [lockKey]),
          liveness.heartbeatIntervalMs,
          "ownership unlock did not answer",
        ).catch(() => {
          destroy = true;
        });
      }
      client.release(destroy ? true : undefined);
    },
  };
}

async function configureSessionLiveness(
  client: PoolClient,
  liveness: OwnershipLivenessSettings,
  telemetry: DispatcherTelemetry,
): Promise<void> {
  // SET takes no bind parameters; every value here is an integer we derived.
  const statements: Array<[name: string, sql: string]> = [
    ["tcp_keepalives_idle", `set tcp_keepalives_idle = ${String(liveness.keepalivesIdleSeconds)}`],
    [
      "tcp_keepalives_interval",
      `set tcp_keepalives_interval = ${String(liveness.keepalivesIntervalSeconds)}`,
    ],
    ["tcp_keepalives_count", `set tcp_keepalives_count = ${String(liveness.keepalivesCount)}`],
    // PostgreSQL 14+. Older servers reject the parameter; the keepalive layer
    // still stands on its own there.
    ["idle_session_timeout", `set idle_session_timeout = ${String(liveness.idleSessionTimeoutMs)}`],
  ];
  const unavailable: string[] = [];
  for (const [name, sql] of statements) {
    try {
      await client.query(sql);
    } catch (error) {
      unavailable.push(`${name}: ${String(error)}`);
    }
  }
  if (unavailable.length > 0) {
    telemetry.emit({
      severity: "warn",
      eventName: "workflow_dispatcher.ownership_liveness_unavailable",
      body: `the server refused some session liveness settings; a dead owner may hold the lock longer than ${String(liveness.idleSessionTimeoutMs)}ms: ${unavailable.join("; ")}`,
    });
  }
}

function holderAttributes(holder: OwnershipHolder): Record<string, string | number> {
  const attributes: Record<string, string | number> = { "ownership.holder.pid": holder.pid };
  if (holder.applicationName) {
    attributes["ownership.holder.application_name"] = holder.applicationName;
  }
  if (holder.clientAddr) attributes["ownership.holder.client_addr"] = holder.clientAddr;
  if (holder.backendStart) attributes["ownership.holder.backend_start"] = holder.backendStart;
  if (holder.state) attributes["ownership.holder.state"] = holder.state;
  return attributes;
}

function isoOrNull(value: Date | string | null | undefined): string | null {
  if (value === null || value === undefined) return null;
  return value instanceof Date ? value.toISOString() : String(value);
}

function withTimeout<T>(promise: Promise<T>, ms: number, message: string): Promise<T> {
  let timer: NodeJS.Timeout | undefined;
  const timeout = new Promise<never>((_, reject) => {
    timer = setTimeout(() => reject(new Error(`${message} within ${String(ms)}ms`)), ms);
    timer.unref();
  });
  return Promise.race([promise, timeout]).finally(() => {
    if (timer) clearTimeout(timer);
  });
}
