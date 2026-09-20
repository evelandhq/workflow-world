/**
 * How many workflow deliveries this process is executing right now.
 *
 * A delivery is a held request: it returns when the step it carries is done,
 * which for a model call or a long command is minutes. Stopping the process
 * while one is open does not lose the step — the dispatcher sees the request
 * fail and delivers it again — but it does run the step a second time, side
 * effects included. A host that wants to replace an executor without that stops
 * routing new deliveries to it first (fail its readiness check), waits for this
 * to reach zero, and only then sends the signal.
 *
 * The count lives on `globalThis` rather than in the module. The World is
 * loaded by the runtime from its package name while the host's own code —
 * the route that reports this number to a preStop hook — is usually bundled
 * separately, and two copies of this module would otherwise each count their
 * own nothing.
 */
const COUNTER_KEY = Symbol.for("@evelandhq/workflow-world.inflight-deliveries");

type Counter = { count: number };

function counter(): Counter {
  const holder = globalThis as Record<symbol, unknown>;
  return (holder[COUNTER_KEY] ??= { count: 0 }) as Counter;
}

/** Deliveries currently executing in this process. */
export function inflightDeliveries(): number {
  return counter().count;
}

/** Counts `deliver` as in flight until it settles, however it settles. */
export async function trackDelivery<T>(deliver: () => Promise<T>): Promise<T> {
  const current = counter();
  current.count += 1;
  try {
    return await deliver();
  } finally {
    current.count -= 1;
  }
}
