/**
 * What the World knows about the delivery it is running inside.
 *
 * `createQueueHandler` runs each dispatched delivery inside one of these, and
 * `events.subscribe` reads it: a live event feed is opened only inside a
 * delivery the dispatcher granted one (`LIVE_FEED_HEADER`), because with a
 * live feed the runtime keeps the delivery open after its work is done (eve's
 * linger), and that held POST is one of the dispatcher's concurrency slots.
 *
 * The `AsyncLocalStorage` lives on `globalThis`, like the in-flight counter
 * (`inflight.ts`): the runtime loads the World from its package name while
 * the host's route may be bundled with a second copy, and a context set by one
 * copy has to be readable by the other.
 */
import { AsyncLocalStorage } from "node:async_hooks";

export type DeliveryContext = {
  /** The dispatcher granted this delivery a live event feed. */
  readonly liveFeed: boolean;
  /** False once the delivery has settled; work it left behind holds no grant. */
  active: boolean;
  /** Run once when the delivery settles: ends what it still holds. */
  readonly onEnd: Set<() => void>;
};

const STORAGE_KEY = Symbol.for("@evelandhq/workflow-world.delivery-context");

function storage(): AsyncLocalStorage<DeliveryContext> {
  const holder = globalThis as Record<symbol, unknown>;
  return (holder[STORAGE_KEY] ??=
    new AsyncLocalStorage<DeliveryContext>()) as AsyncLocalStorage<DeliveryContext>;
}

/** The delivery the caller runs inside, if any. */
export function currentDelivery(): DeliveryContext | undefined {
  return storage().getStore();
}

/** Runs `deliver` as one delivery, and ends what it still holds when it settles. */
export async function runDelivery<T>(
  input: { liveFeed: boolean },
  deliver: () => Promise<T>,
): Promise<T> {
  const context: DeliveryContext = { liveFeed: input.liveFeed, active: true, onEnd: new Set() };
  try {
    return await storage().run(context, deliver);
  } finally {
    context.active = false;
    for (const end of context.onEnd) {
      try {
        end();
      } catch {
        // Ending one holding must not keep the others.
      }
    }
    context.onEnd.clear();
  }
}
