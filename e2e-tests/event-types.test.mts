import { expect, test } from "vitest";
import { REQUIRED_TURN_EVENT_TYPES, waitForRequiredEventTypes } from "./event-types.mts";

test("waits for asynchronous turn events to finish arriving", async () => {
  let attempts = 0;

  await waitForRequiredEventTypes(
    async () => {
      attempts += 1;
      const visibleTypes =
        attempts === 1 ? REQUIRED_TURN_EVENT_TYPES.slice(0, -1) : REQUIRED_TURN_EVENT_TYPES;
      return visibleTypes.map((type) => ({ type, count: "1" }));
    },
    { timeoutMs: 100, intervalMs: 0 },
  );

  expect(attempts).toBe(2);
});
