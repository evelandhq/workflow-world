/**
 * The one `LISTEN` connection a World holds.
 *
 * Every NOTIFY channel a World listens on (the stream channel, the live event
 * feed's) is a channel on this one `pg.Client`, so the feed costs no second
 * held connection: connection budget is what runs out first on a shared
 * database.
 *
 * A dropped connection (database restart, failover, `pg_terminate_backend`)
 * is noticed through the client's `error`/`end` events, every user is told
 * (`onDisconnect`), and the connection is replaced with backoff until `LISTEN`
 * succeeds again on every channel, when every user is told again
 * (`onReconnect`). What was notified while it was down is lost; each user
 * decides what that means for it. Ported from upstream `listenChannel`, with
 * the connection shared rather than one per channel.
 */
import { Client, type Pool } from "pg";

/** The first delay before replacing a dropped connection; doubles per failure. */
export const LISTEN_RECONNECT_DELAY_MS = 1_000;
const LISTEN_RECONNECT_MAX_DELAY_MS = 10_000;

/**
 * Upper bound on one connection attempt when the pool sets none. `pg` defaults
 * to no timeout, so a connect into a network partition would otherwise stall
 * the reconnect loop for ever.
 */
const LISTEN_CONNECT_TIMEOUT_MS = 10_000;

export type ChannelHandlers = {
  onPayload(payload: string): void;
  /** The connection dropped: notifications may have been lost from now on. */
  onDisconnect?(): void;
  /** `LISTEN` is back on a new connection; what was sent meanwhile is lost. */
  onReconnect?(): void;
};

export type SharedListener = {
  /**
   * Listens on `channel` (a trusted identifier). Resolves once `LISTEN` is in
   * effect, and rejects when the connection is down or cannot be opened; the
   * channel stays registered either way and is listened on again after every
   * reconnect.
   */
  listen(channel: string, handlers: ChannelHandlers): Promise<void>;
  /** Whether the connection is up and listening. */
  isUp(): boolean;
  close(): Promise<void>;
};

export function createListener(
  pool: Pool,
  options: { reconnectDelayMs?: number; log?: (message: string) => void } = {},
): SharedListener {
  const baseDelayMs = options.reconnectDelayMs ?? LISTEN_RECONNECT_DELAY_MS;
  const log = options.log ?? ((message: string) => console.warn(message));
  const channels = new Map<string, ChannelHandlers>();
  /** The connection that is up, and the channels `LISTEN` is in effect for on it. */
  let current: { client: Client; listening: Set<string> } | undefined;
  let connecting: Promise<void> | undefined;
  let reconnectTimer: NodeJS.Timeout | undefined;
  let delayMs = baseDelayMs;
  /** Set from a drop until the next connection is up, so each outage logs once. */
  let degraded = false;
  let closed = false;

  const each = (fn: (handlers: ChannelHandlers) => void) => {
    for (const handlers of channels.values()) {
      try {
        fn(handlers);
      } catch {
        // One user's callback must not stop the others, or the reconnect.
      }
    }
  };

  const scheduleReconnect = () => {
    if (closed || reconnectTimer || channels.size === 0) return;
    const wait = delayMs;
    delayMs = Math.min(delayMs * 2, Math.max(baseDelayMs, LISTEN_RECONNECT_MAX_DELAY_MS));
    reconnectTimer = setTimeout(() => {
      reconnectTimer = undefined;
      connect().catch(() => {});
    }, wait);
    reconnectTimer.unref();
  };

  const dropped = (client: Client) => {
    // `pg` emits both `error` and `end` for one drop, and a retired client can
    // still emit while it unwinds; only the live client's first signal counts.
    if (current?.client !== client) return;
    current = undefined;
    void client.end().catch(() => {});
    if (closed) return;
    if (!degraded) {
      degraded = true;
      log("[workflow-world] LISTEN connection lost; reconnecting");
      each((handlers) => handlers.onDisconnect?.());
    }
    scheduleReconnect();
  };

  /** One connection attempt, which every caller arriving meanwhile shares. */
  const connect = (): Promise<void> => {
    if (current) return Promise.resolve();
    if (closed) return Promise.reject(new Error("The LISTEN connection is closed"));
    connecting ??= (async () => {
      const client = new Client({
        ...pool.options,
        connectionTimeoutMillis: pool.options.connectionTimeoutMillis || LISTEN_CONNECT_TIMEOUT_MS,
      });
      // Without an `error` listener a server-side disconnect is rethrown by
      // the EventEmitter and ends the process. Kept on a retired client too.
      client.on("error", () => dropped(client));
      client.on("end", () => dropped(client));
      client.on("notification", (message) => {
        const handlers = channels.get(message.channel);
        if (!handlers) return;
        try {
          handlers.onPayload(message.payload ?? "");
        } catch {
          // A user's failure is its own.
        }
      });
      const listening = new Set<string>();
      try {
        await client.connect();
        for (const channel of channels.keys()) {
          await client.query(`LISTEN "${channel}"`);
          listening.add(channel);
        }
      } catch (error) {
        await client.end().catch(() => {});
        throw error;
      }
      if (closed) {
        await client.end().catch(() => {});
        throw new Error("The LISTEN connection is closed");
      }
      current = { client, listening };
      delayMs = baseDelayMs;
      if (degraded) {
        degraded = false;
        log("[workflow-world] LISTEN connection restored");
        each((handlers) => handlers.onReconnect?.());
      }
    })().then(
      () => {
        connecting = undefined;
      },
      (error: unknown) => {
        connecting = undefined;
        // A first connection that failed counts as an outage too: whoever
        // registered meanwhile hears `onReconnect` when it comes up.
        if (!closed && !degraded) {
          degraded = true;
          log("[workflow-world] LISTEN connection failed; retrying");
        }
        scheduleReconnect();
        throw error;
      },
    );
    return connecting;
  };

  return {
    async listen(channel, handlers) {
      channels.set(channel, handlers);
      // Waiting out a backoff: the channel is listened on with the rest when
      // the next attempt succeeds, and until then it is down.
      if (reconnectTimer) throw new Error("The LISTEN connection is down");
      await connect();
      const up = current;
      if (!up) throw new Error("The LISTEN connection is down");
      if (up.listening.has(channel)) return;
      await up.client.query(`LISTEN "${channel}"`);
      up.listening.add(channel);
    },
    isUp() {
      return current !== undefined;
    },
    async close() {
      if (closed) return;
      closed = true;
      clearTimeout(reconnectTimer);
      reconnectTimer = undefined;
      await connecting?.catch(() => {});
      const up = current;
      current = undefined;
      // Ending the connection drops every `LISTEN` with it.
      await up?.client.end().catch(() => {});
    },
  };
}
