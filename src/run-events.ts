/**
 * The live event feed of a run (`events.subscribe`), on Postgres
 * `LISTEN`/`NOTIFY`. Ported from upstream `@workflow/world-postgres`'s
 * `run-events.ts`, with three changes: the channel is per tenant, it is a
 * channel on the World's one `LISTEN` connection (`listener.ts`), and a
 * subscription is opened only inside a delivery the dispatcher granted one
 * (`delivery-context.ts`).
 *
 * Every committed event write of a run announces the run on its tenant's
 * channel ({@link announceRunEvents}). A subscription, for each announcement
 * of its run, reads the log from the slot after the last one it delivered and
 * delivers what continues the sequence, in slot order, each slot once. The
 * notification is a signal only: what is delivered is always read from the
 * table, so a duplicated, merged or reordered `NOTIFY` changes nothing, and a
 * position not committed yet (a hole) stops delivery until a later
 * announcement fills it.
 *
 * The feed is best effort, as the World contract allows: a dropped `LISTEN`
 * connection, a failed read, the end of the delivery that opened it, or a
 * drain ({@link stopLiveEventFeeds}) ends a subscription through `onError`,
 * and the runtime falls back to its log-tail poll. A lost announcement (the
 * writer died between its commit and its `NOTIFY`) is found by the next
 * announcement of the run, or by that poll.
 *
 * The grant is what makes the feed safe to offer. With a live feed the
 * runtime holds a delivery open after its work is done (eve's linger: 30 s
 * while a background step is outstanding, 5 s while only a hook is open), and
 * a held delivery is one of the dispatcher's concurrency slots and one of its
 * tenant's in-flight slots. The dispatcher grants a bounded share of those
 * (see `createLiveFeedGrants`), and everything else is refused here.
 */
import type {
  AnyEventRequest,
  CreateEventParams,
  Event,
  EventResult,
  EventsSubscribeOptions,
  HookClaimedFrom,
  ListEventsParams,
  Storage,
} from "@workflow/world";
import { eventIdToSlot, slotToEventId, SPEC_VERSION_SINGLE_ORCHESTRATOR } from "@workflow/world";
import { sql } from "drizzle-orm";
import { currentDelivery } from "./delivery-context.js";
import type { Drizzle } from "./drizzle/index.js";
import type { SharedListener } from "./listener.js";
import { tenantEventsChannel } from "./tenant.js";

/** Events one read of a subscription asks for. */
const READ_PAGE_SIZE = 200;

/** Live subscriptions one process holds at most, by default. */
export const DEFAULT_MAX_LIVE_EVENT_FEEDS = 32;

/**
 * Whether an event is a hook resume another writer made with `resumeHook()`
 * (or a webhook): a `hook_received` with a `resumeId`. The workflow's own
 * `hook_received` (an `AbortController` abort) carries none.
 *
 * The feed does not deliver one: it ends the subscription there instead
 * (`onError`). The resume's wake carries its producer's timing, which is where
 * the runtime starts its hook-resume latency measurement; a delivery that
 * picked the resume up from the feed would consume it with no timing, and the
 * wake that follows would find nothing left to run. Ending the feed lets a
 * delivery that is only waiting stop lingering and acknowledge; the wake then
 * consumes the resume and measures it, as without a feed. Upstream's rule.
 */
function isHookResume(event: Event): boolean {
  return (
    event.eventType === "hook_received" &&
    typeof (event as { resumeId?: unknown }).resumeId === "string"
  );
}

/**
 * Announces a committed event write of a run to its live feed, in any process
 * of the tenant.
 *
 * Best effort and never awaited by the write: a lost announcement costs a
 * subscriber the runtime's poll interval and nothing else. Called after the
 * write's transaction committed and outside it, since a `NOTIFY` inside the
 * write's transaction could fail the write itself.
 */
export function announceRunEvents(drizzle: Drizzle, tenantId: string, runId: string): void {
  const channel = tenantEventsChannel(tenantId);
  void Promise.resolve()
    .then(() => drizzle.execute(sql`select pg_notify(${channel}, ${runId})`))
    .catch(() => {
      // Intentionally ignored. See above.
    });
}

/** `events.create` with its overloads collapsed: the wrapper forwards what it received. */
type CreateEvent = (
  runId: string | null,
  data: AnyEventRequest,
  params?: CreateEventParams,
) => Promise<EventResult>;

/**
 * The spec 9 run a forced `hook_created` took its token from, when the same
 * transaction committed `hook_disposed{forceClaimedBy}` to that run's log: the
 * victim was still running, which is exactly when `claimedFrom` carries its
 * `runSpecVersion`. Its feed is announced too, so the victim sees the takeover
 * at once rather than at its next poll. Upstream's rule.
 */
function forceClaimVictimRunId(result: EventResult): string | undefined {
  const claimedFrom =
    result.hook?.claimedFrom ??
    (result.event?.eventData as { forceClaimedFrom?: HookClaimedFrom } | undefined)
      ?.forceClaimedFrom;
  return claimedFrom?.runId !== undefined &&
    (claimedFrom.runSpecVersion ?? 0) >= SPEC_VERSION_SINGLE_ORCHESTRATOR
    ? claimedFrom.runId
    : undefined;
}

/**
 * The events storage with the live feed on it: every committed `create`
 * announces its run (and a force-claim's victim run), and `subscribe` reads
 * with `read`, the unfenced `list`, when this World has a feed.
 *
 * Around the outermost `create`, so the announcement follows whichever
 * commit the write ended in, fenced or not, and a refused or failed write
 * announces nothing.
 */
export function withRunEventFeed(
  events: Storage["events"],
  input: { drizzle: Drizzle; tenantId: string; read: Read; feed?: RunEventsFeed },
): Storage["events"] {
  const create = events.create as CreateEvent;
  const { drizzle, tenantId, read, feed } = input;
  const announced: CreateEvent = async (runId, data, params) => {
    const result = await create(runId, data, params);
    const announcedRunId = result.event?.runId ?? runId;
    if (announcedRunId && result.event) announceRunEvents(drizzle, tenantId, announcedRunId);
    const victimRunId = forceClaimVictimRunId(result);
    if (victimRunId !== undefined && victimRunId !== announcedRunId) {
      announceRunEvents(drizzle, tenantId, victimRunId);
    }
    return result;
  };
  return {
    ...events,
    create: announced as Storage["events"]["create"],
    ...(feed
      ? {
          subscribe: (runId, afterSlot, onEvent, options) =>
            feed.subscribe(runId, afterSlot, onEvent, options, read),
        }
      : {}),
  };
}

type Read = (params: ListEventsParams) => Promise<{ data: Event[]; hasMore: boolean }>;

type Subscription = {
  runId: string;
  lastSlot: number;
  reading: boolean;
  /** An announcement arrived while a read was in flight. */
  dirty: boolean;
  closed: boolean;
  onEvent: (event: Event) => void;
  onError: ((error: unknown) => void) | undefined;
  read: Read;
  /** Removes it from its delivery's end list. */
  detach: () => void;
};

/**
 * Process-wide state: one count and one stop switch for every World in the
 * process, on `globalThis` for the same reason as `inflightDeliveries`.
 */
type Registry = {
  stopped: boolean;
  live: number;
  feeds: Set<{ failAll(error: Error): void }>;
};

const REGISTRY_KEY = Symbol.for("@evelandhq/workflow-world.live-event-feeds");

function registry(): Registry {
  const holder = globalThis as Record<symbol, unknown>;
  return (holder[REGISTRY_KEY] ??= { stopped: false, live: 0, feeds: new Set() }) as Registry;
}

/**
 * Ends every live event feed in this process through `onError` and refuses
 * new ones, for good.
 *
 * Part of a drain. A delivery lingering on a live feed is a held request with
 * no work left in it; ending its feed ends the linger, so the delivery
 * acknowledges and `inflightDeliveries()` reaches zero without waiting the
 * linger out. Call it when the host stops routing to the process, before it
 * waits on `inflightDeliveries()`.
 */
export function stopLiveEventFeeds(): void {
  const shared = registry();
  shared.stopped = true;
  for (const feed of shared.feeds) {
    feed.failAll(new Error("Live event feeds were stopped in this process"));
  }
}

/** Live event feed subscriptions open in this process. */
export function liveEventFeeds(): number {
  return registry().live;
}

export type RunEventsFeed = {
  subscribe(
    runId: string,
    afterSlot: number,
    onEvent: (event: Event) => void,
    options: EventsSubscribeOptions | undefined,
    read: Read,
  ): () => void;
  close(): Promise<void>;
};

export function createRunEventsFeed(
  listener: SharedListener,
  options: { tenantId: string; maxLiveSubscriptions: number },
): RunEventsFeed {
  const channel = tenantEventsChannel(options.tenantId);
  const shared = registry();
  const byRun = new Map<string, Set<Subscription>>();
  let listening: Promise<void> | undefined;
  let closed = false;

  const remove = (subscription: Subscription): void => {
    if (subscription.closed) return;
    subscription.closed = true;
    shared.live--;
    subscription.detach();
    const set = byRun.get(subscription.runId);
    set?.delete(subscription);
    if (set?.size === 0) byRun.delete(subscription.runId);
  };

  /** Ends a subscription and tells its caller. */
  const fail = (subscription: Subscription, error: unknown): void => {
    if (subscription.closed) return;
    remove(subscription);
    try {
      subscription.onError?.(error);
    } catch {
      // The caller's callback must not break the feed.
    }
  };

  const failAll = (error: Error): void => {
    // A snapshot: an `onError` that subscribes again must not be failed here.
    for (const subscription of Array.from(byRun.values()).flatMap((set) => Array.from(set))) {
      fail(subscription, error);
    }
  };

  const feed = { failAll };
  shared.feeds.add(feed);

  /** Reads and delivers what continues the subscription's slot sequence. */
  const catchUp = async (subscription: Subscription): Promise<void> => {
    if (subscription.closed) return;
    if (subscription.reading) {
      subscription.dirty = true;
      return;
    }
    subscription.reading = true;
    try {
      let more = true;
      while (more && !subscription.closed) {
        subscription.dirty = false;
        const page = await subscription.read({
          runId: subscription.runId,
          pagination: {
            sortOrder: "asc",
            limit: READ_PAGE_SIZE,
            ...(subscription.lastSlot >= 1 ? { cursor: slotToEventId(subscription.lastSlot) } : {}),
          },
          resolveData: "all",
        });
        let contiguous = true;
        for (const event of page.data) {
          if (subscription.closed) return;
          const slot = eventIdToSlot(event.eventId);
          if (slot === null || slot <= subscription.lastSlot) continue;
          // A position not committed yet: wait for the announcement that fills
          // it rather than deliver past it.
          if (slot !== subscription.lastSlot + 1) {
            contiguous = false;
            break;
          }
          if (isHookResume(event)) {
            fail(
              subscription,
              new Error(
                `Live event feed of ${subscription.runId} stopped at a hook resume (slot ${String(slot)}), which its wake delivers`,
              ),
            );
            return;
          }
          subscription.lastSlot = slot;
          subscription.onEvent(event);
        }
        more = subscription.dirty || (contiguous && page.hasMore);
      }
    } catch (error) {
      fail(subscription, error);
    } finally {
      subscription.reading = false;
    }
  };

  const ensureListening = (): Promise<void> => {
    listening ??= listener
      .listen(channel, {
        onPayload(runId) {
          const set = byRun.get(runId);
          if (!set) return;
          for (const subscription of set) void catchUp(subscription);
        },
        // What was announced while the connection was down is lost, so every
        // subscription ends; the runtime polls instead.
        onDisconnect() {
          failAll(new Error("The LISTEN connection of the live event feed was lost"));
        },
      })
      .catch((error: unknown) => {
        // Asked again by the next subscription.
        listening = undefined;
        throw error;
      });
    return listening;
  };

  return {
    subscribe(runId, afterSlot, onEvent, subscribeOptions, read) {
      const refuse = (reason: string): (() => void) => {
        // Never synchronously: the caller has not received its unsubscribe
        // function yet.
        queueMicrotask(() => {
          try {
            subscribeOptions?.onError?.(new Error(reason));
          } catch {
            // See `fail`.
          }
        });
        return () => {};
      };
      if (closed) return refuse("The World is closed");
      if (shared.stopped) return refuse("Live event feeds were stopped in this process");
      const delivery = currentDelivery();
      if (!delivery) return refuse("Not inside a dispatched delivery");
      if (!delivery.active) return refuse("The delivery that asked has ended");
      if (!delivery.liveFeed) {
        return refuse("The dispatcher did not grant this delivery a live event feed");
      }
      if (shared.live >= options.maxLiveSubscriptions) {
        return refuse(
          `This process already holds ${String(options.maxLiveSubscriptions)} live event feeds`,
        );
      }

      const end = () => fail(subscription, new Error("The delivery that opened it has ended"));
      const subscription: Subscription = {
        runId,
        lastSlot: afterSlot,
        reading: false,
        dirty: false,
        closed: false,
        onEvent,
        onError: subscribeOptions?.onError,
        read,
        detach: () => delivery.onEnd.delete(end),
      };
      shared.live++;
      delivery.onEnd.add(end);
      let set = byRun.get(runId);
      if (!set) {
        set = new Set();
        byRun.set(runId, set);
      }
      set.add(subscription);
      // Read once the channel listens: anything committed before then was
      // announced to no one.
      ensureListening().then(
        () => {
          if (subscription.closed) return;
          if (!listener.isUp()) {
            fail(subscription, new Error("The LISTEN connection of the live event feed is down"));
            return;
          }
          void catchUp(subscription);
        },
        (error: unknown) => fail(subscription, error),
      );
      return () => remove(subscription);
    },

    async close() {
      if (closed) return;
      closed = true;
      shared.feeds.delete(feed);
      failAll(new Error("The World is closed"));
    },
  };
}
