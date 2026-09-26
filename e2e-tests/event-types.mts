export const REQUIRED_TURN_EVENT_TYPES = [
  "run_created",
  "run_started",
  "run_completed",
  "step_created",
  "step_started",
  "step_completed",
  "hook_created",
  "hook_disposed",
  "wait_created",
] as const;

export type TurnEventType = (typeof REQUIRED_TURN_EVENT_TYPES)[number];

/**
 * The event types one first turn has to leave behind, by eve line.
 *
 * Through 0.66 the fixture's `task` sessions end as soon as their turn settles,
 * completing the session run and disposing its address hook on the way out.
 * eve 0.67.0 removed the `conversation` / `task` run mode: every session parks
 * after each turn and only its timeout ends it, so nothing in this suite's
 * window completes a run or disposes a hook. Those two are asserted by the
 * conformance suite against `@workflow/world-testing` instead.
 */
export function requiredTurnEventTypes(eveVersion: string): readonly TurnEventType[] {
  const minor = Number(/^0\.(\d+)\./.exec(eveVersion)?.[1]);
  if (!Number.isInteger(minor)) throw new Error(`unrecognized eve version ${eveVersion}`);
  return minor >= 67
    ? REQUIRED_TURN_EVENT_TYPES.filter(
        (type) => type !== "run_completed" && type !== "hook_disposed",
      )
    : REQUIRED_TURN_EVENT_TYPES;
}

type EventCount = { type: string; count: string };
type EventCountQuery = () => Promise<readonly EventCount[]>;

export type WaitForRequiredEventTypesOptions = {
  timeoutMs?: number;
  intervalMs?: number;
  /** Defaults to every type in `REQUIRED_TURN_EVENT_TYPES`. */
  required?: readonly TurnEventType[];
};

/** Assert the event lifecycle produced by one Eve turn. */
export async function waitForRequiredEventTypes(
  query: EventCountQuery,
  options: WaitForRequiredEventTypesOptions = {},
): Promise<void> {
  const timeoutMs = options.timeoutMs ?? 30_000;
  const intervalMs = options.intervalMs ?? 250;
  const required = options.required ?? REQUIRED_TURN_EVENT_TYPES;
  const deadline = Date.now() + timeoutMs;

  for (;;) {
    const rows = await query();
    const byType = Object.fromEntries(rows.map((row) => [row.type, Number(row.count)]));
    const missing = required.filter((eventType) => !byType[eventType]);
    if (missing.length === 0) return;
    if (Date.now() >= deadline) {
      throw new Error(`timed out waiting for required event types: ${missing.join(", ")}`);
    }
    if (intervalMs > 0) {
      await new Promise((resolve) => setTimeout(resolve, intervalMs));
    }
  }
}
