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

type EventCount = { type: string; count: string };
type EventCountQuery = () => Promise<readonly EventCount[]>;

export type WaitForRequiredEventTypesOptions = {
  timeoutMs?: number;
  intervalMs?: number;
};

/** Assert the event lifecycle produced by one Eve turn. */
export async function waitForRequiredEventTypes(
  query: EventCountQuery,
  options: WaitForRequiredEventTypesOptions = {},
): Promise<void> {
  const timeoutMs = options.timeoutMs ?? 30_000;
  const intervalMs = options.intervalMs ?? 250;
  const deadline = Date.now() + timeoutMs;

  for (;;) {
    const rows = await query();
    const byType = Object.fromEntries(rows.map((row) => [row.type, Number(row.count)]));
    const missing = REQUIRED_TURN_EVENT_TYPES.filter((eventType) => !byType[eventType]);
    if (missing.length === 0) return;
    if (Date.now() >= deadline) {
      throw new Error(`timed out waiting for required event types: ${missing.join(", ")}`);
    }
    if (intervalMs > 0) {
      await new Promise((resolve) => setTimeout(resolve, intervalMs));
    }
  }
}
