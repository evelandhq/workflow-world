import type { Event, ListEventsParams } from "@workflow/world";
import { eventIdToSlot, slotToEventId } from "@workflow/world";
import { afterEach, describe, expect, test, vi } from "vitest";
import { resolveMaxLiveEventFeeds } from "./config.js";
import { runDelivery } from "./delivery-context.js";
import type { ChannelHandlers, SharedListener } from "./listener.js";
import {
  createRunEventsFeed,
  DEFAULT_MAX_LIVE_EVENT_FEEDS,
  liveEventFeeds,
  stopLiveEventFeeds,
} from "./run-events.js";

/**
 * The live event feed's own rules, against a listener and a log held in
 * memory: slot order, each slot once, a hole stops delivery, a lost
 * announcement is caught up by the next, a hook resume ends the feed, and
 * every refusal is an asynchronous `onError` with a no-op unsubscribe. The
 * NOTIFY plumbing, tenancy and connection loss against a real database are in
 * `run-events.integration.test.ts`.
 */
const RUN = "wrun_feed";
const TENANT = "prj_feed_unit";

function fakeListener() {
  const channels = new Map<string, ChannelHandlers>();
  let up = true;
  const listener: SharedListener = {
    async listen(channel, handlers) {
      channels.set(channel, handlers);
      if (!up) throw new Error("down");
    },
    isUp: () => up,
    async close() {},
  };
  return {
    listener,
    announce(runId: string) {
      for (const handlers of channels.values()) handlers.onPayload(runId);
    },
    drop() {
      up = false;
      for (const handlers of channels.values()) handlers.onDisconnect?.();
    },
  };
}

/** A run's log: slots present in `committed` are visible to reads. */
function fakeLog() {
  const committed = new Map<number, Event>();
  const read = vi.fn(async (params: ListEventsParams) => {
    const after = params.pagination?.cursor ? eventIdToSlot(params.pagination.cursor)! : 0;
    const limit = params.pagination?.limit ?? 100;
    const slots = [...committed.keys()].filter((slot) => slot > after).sort((a, b) => a - b);
    return {
      data: slots.slice(0, limit).map((slot) => committed.get(slot)!),
      hasMore: slots.length > limit,
    };
  });
  return {
    read,
    commit(slot: number, extra: Partial<Event> = {}) {
      committed.set(slot, {
        runId: RUN,
        eventId: slotToEventId(slot),
        eventType: "wait_created",
        correlationId: `wait_${String(slot)}`,
        eventData: { resumeAt: new Date() },
        createdAt: new Date(),
        ...extra,
      } as Event);
    },
  };
}

/** Lets reads and microtasks run. */
const settle = () => new Promise((resolve) => setTimeout(resolve, 10));

type Feed = ReturnType<typeof createRunEventsFeed>;

/** Subscribes inside a delivery; the delivery stays open until `end()`. */
function subscribeIn(
  feed: Feed,
  read: ReturnType<typeof fakeLog>["read"],
  options: { liveFeed?: boolean; afterSlot?: number } = {},
) {
  const events: number[] = [];
  const errors: unknown[] = [];
  let unsubscribe!: () => void;
  let release!: () => void;
  const delivery = runDelivery({ liveFeed: options.liveFeed ?? true }, async () => {
    unsubscribe = feed.subscribe(
      RUN,
      options.afterSlot ?? 0,
      (event) => events.push(eventIdToSlot(event.eventId)!),
      { onError: (error) => errors.push(error) },
      read,
    );
    await new Promise<void>((resolve) => {
      release = resolve;
    });
  });
  return {
    events,
    errors,
    unsubscribe: () => unsubscribe(),
    end: async () => {
      release();
      await delivery;
    },
  };
}

const open: Array<{ end(): Promise<void> }> = [];
const feeds: Feed[] = [];

function newFeed(listener: SharedListener, maxLiveSubscriptions = 32): Feed {
  const feed = createRunEventsFeed(listener, { tenantId: TENANT, maxLiveSubscriptions });
  feeds.push(feed);
  return feed;
}

afterEach(async () => {
  await Promise.all(open.splice(0).map((subscription) => subscription.end()));
  await Promise.all(feeds.splice(0).map((feed) => feed.close()));
});

describe("live event feed", () => {
  test("delivers slots in order, each once, however announcements repeat", async () => {
    const { listener, announce } = fakeListener();
    const log = fakeLog();
    const feed = newFeed(listener);
    log.commit(1);
    log.commit(2);
    const sub = subscribeIn(feed, log.read, { afterSlot: 1 });
    open.push(sub);
    await settle();
    // The first read after LISTEN picks up what was committed before.
    expect(sub.events).toEqual([2]);

    log.commit(3);
    log.commit(4);
    announce(RUN);
    announce(RUN);
    announce(RUN);
    await settle();
    log.commit(5);
    announce(RUN);
    await settle();
    expect(sub.events).toEqual([2, 3, 4, 5]);
    expect(sub.errors).toEqual([]);
  });

  test("a hole stops delivery until it is filled", async () => {
    const { listener, announce } = fakeListener();
    const log = fakeLog();
    const feed = newFeed(listener);
    const sub = subscribeIn(feed, log.read);
    open.push(sub);
    await settle();

    log.commit(1);
    log.commit(3);
    announce(RUN);
    await settle();
    expect(sub.events).toEqual([1]);

    log.commit(2);
    announce(RUN);
    await settle();
    expect(sub.events).toEqual([1, 2, 3]);
  });

  test("a lost announcement is caught up by the next one", async () => {
    const { listener, announce } = fakeListener();
    const log = fakeLog();
    const feed = newFeed(listener);
    const sub = subscribeIn(feed, log.read);
    open.push(sub);
    await settle();

    log.commit(1); // its NOTIFY never arrives
    await settle();
    expect(sub.events).toEqual([]);
    log.commit(2);
    announce(RUN);
    await settle();
    expect(sub.events).toEqual([1, 2]);
  });

  test("reads on past a full page", async () => {
    const { listener } = fakeListener();
    const log = fakeLog();
    for (let slot = 1; slot <= 450; slot++) log.commit(slot);
    const feed = newFeed(listener);
    const sub = subscribeIn(feed, log.read);
    open.push(sub);
    await settle();
    expect(sub.events).toHaveLength(450);
    expect(sub.events.at(-1)).toBe(450);
  });

  test("ends at a hook resume made by resumeHook(), without delivering it", async () => {
    const { listener, announce } = fakeListener();
    const log = fakeLog();
    const feed = newFeed(listener);
    const sub = subscribeIn(feed, log.read);
    open.push(sub);
    await settle();

    // The workflow's own `hook_received` (no resumeId) is delivered.
    log.commit(1, { eventType: "hook_received", correlationId: "hook_1" } as Partial<Event>);
    log.commit(2, {
      eventType: "hook_received",
      correlationId: "hook_1",
      resumeId: "resume_1",
    } as Partial<Event>);
    log.commit(3);
    announce(RUN);
    await settle();
    expect(sub.events).toEqual([1]);
    expect(sub.errors).toHaveLength(1);
    expect(String(sub.errors[0])).toMatch(/hook resume/);

    // Ended: nothing more arrives.
    announce(RUN);
    await settle();
    expect(sub.events).toEqual([1]);
  });

  test("unsubscribe stops delivery at once and is idempotent", async () => {
    const { listener, announce } = fakeListener();
    const log = fakeLog();
    const feed = newFeed(listener);
    const sub = subscribeIn(feed, log.read);
    open.push(sub);
    await settle();
    const before = liveEventFeeds();
    sub.unsubscribe();
    sub.unsubscribe();
    expect(liveEventFeeds()).toBe(before - 1);
    log.commit(1);
    announce(RUN);
    await settle();
    expect(sub.events).toEqual([]);
    expect(sub.errors).toEqual([]);
  });

  test("a dropped connection fails every subscription, and new ones while it is down", async () => {
    const { listener, drop } = fakeListener();
    const log = fakeLog();
    const feed = newFeed(listener);
    const first = subscribeIn(feed, log.read);
    const second = subscribeIn(feed, log.read);
    open.push(first, second);
    await settle();

    drop();
    expect(first.errors).toHaveLength(1);
    expect(second.errors).toHaveLength(1);

    const later = subscribeIn(feed, log.read);
    open.push(later);
    await settle();
    expect(later.errors).toHaveLength(1);
    expect(String(later.errors[0])).toMatch(/LISTEN connection/);
  });

  test("the end of its delivery ends a subscription", async () => {
    const { listener } = fakeListener();
    const log = fakeLog();
    const feed = newFeed(listener);
    const sub = subscribeIn(feed, log.read);
    await settle();
    const before = liveEventFeeds();
    await sub.end();
    expect(sub.errors).toHaveLength(1);
    expect(liveEventFeeds()).toBe(before - 1);
  });

  describe("refusals are an asynchronous onError and a no-op unsubscribe", () => {
    async function expectRefused(subscribe: (onError: (error: unknown) => void) => () => void) {
      const errors: unknown[] = [];
      const before = liveEventFeeds();
      const unsubscribe = subscribe((error) => errors.push(error));
      // Never synchronously: the caller has not got its unsubscribe yet.
      expect(errors).toEqual([]);
      expect(typeof unsubscribe).toBe("function");
      await Promise.resolve();
      expect(errors).toHaveLength(1);
      expect(liveEventFeeds()).toBe(before);
      expect(() => unsubscribe()).not.toThrow();
      return String(errors[0]);
    }

    test("outside any delivery", async () => {
      const feed = newFeed(fakeListener().listener);
      const reason = await expectRefused((onError) =>
        feed.subscribe(RUN, 0, () => {}, { onError }, fakeLog().read),
      );
      expect(reason).toMatch(/Not inside a dispatched delivery/);
    });

    test("in a delivery the dispatcher did not grant one", async () => {
      const feed = newFeed(fakeListener().listener);
      await runDelivery({ liveFeed: false }, async () => {
        const reason = await expectRefused((onError) =>
          feed.subscribe(RUN, 0, () => {}, { onError }, fakeLog().read),
        );
        expect(reason).toMatch(/did not grant/);
      });
    });

    test("from work a delivery left behind", async () => {
      const feed = newFeed(fakeListener().listener);
      let leftBehind!: Promise<string>;
      await runDelivery({ liveFeed: true }, async () => {
        // A timer keeps the delivery's context, and fires after it ended.
        leftBehind = new Promise((resolve) => {
          setTimeout(() => {
            resolve(
              expectRefused((onError) =>
                feed.subscribe(RUN, 0, () => {}, { onError }, fakeLog().read),
              ),
            );
          }, 20);
        });
      });
      expect(await leftBehind).toMatch(/has ended/);
    });

    test("at the per-process cap", async () => {
      const { listener } = fakeListener();
      const log = fakeLog();
      const feed = newFeed(listener, 1);
      const held = subscribeIn(feed, log.read);
      open.push(held);
      await settle();
      await runDelivery({ liveFeed: true }, async () => {
        const reason = await expectRefused((onError) =>
          feed.subscribe(RUN, 0, () => {}, { onError }, log.read),
        );
        expect(reason).toMatch(/already holds 1/);
      });
      expect(held.errors).toEqual([]);
    });

    test("when the cap is 0", async () => {
      const feed = newFeed(fakeListener().listener, 0);
      await runDelivery({ liveFeed: true }, async () => {
        await expectRefused((onError) =>
          feed.subscribe(RUN, 0, () => {}, { onError }, fakeLog().read),
        );
      });
    });

    test("after the World closed", async () => {
      const feed = newFeed(fakeListener().listener);
      await feed.close();
      await runDelivery({ liveFeed: true }, async () => {
        const reason = await expectRefused((onError) =>
          feed.subscribe(RUN, 0, () => {}, { onError }, fakeLog().read),
        );
        expect(reason).toMatch(/closed/);
      });
    });
  });

  test("the per-process cap reads WORKFLOW_WORLD_MAX_LIVE_EVENT_FEEDS, 32 when unset", () => {
    expect(DEFAULT_MAX_LIVE_EVENT_FEEDS).toBe(32);
    expect(resolveMaxLiveEventFeeds(undefined, 32)).toBe(32);
    expect(resolveMaxLiveEventFeeds("", 32)).toBe(32);
    expect(resolveMaxLiveEventFeeds("0", 32)).toBe(0);
    expect(resolveMaxLiveEventFeeds("8", 32)).toBe(8);
    for (const bad of ["-1", "1.5", "lots"]) {
      expect(() => resolveMaxLiveEventFeeds(bad, 32), bad).toThrow(
        /WORKFLOW_WORLD_MAX_LIVE_EVENT_FEEDS/,
      );
    }
  });

  // Last: the stop is permanent for the process, so it is undone by hand.
  test("stopLiveEventFeeds ends every feed in the process and refuses new ones", async () => {
    const log = fakeLog();
    const first = newFeed(fakeListener().listener);
    const second = newFeed(fakeListener().listener);
    const a = subscribeIn(first, log.read);
    const b = subscribeIn(second, log.read);
    open.push(a, b);
    await settle();
    try {
      stopLiveEventFeeds();
      expect(a.errors).toHaveLength(1);
      expect(b.errors).toHaveLength(1);
      expect(liveEventFeeds()).toBe(0);

      const refused = subscribeIn(first, log.read);
      open.push(refused);
      await settle();
      expect(String(refused.errors[0])).toMatch(/stopped/);
    } finally {
      delete (globalThis as Record<symbol, unknown>)[
        Symbol.for("@evelandhq/workflow-world.live-event-feeds")
      ];
    }
  });
});
