import { describe, expect, test } from "vitest";
import type { MessageData } from "../message.js";
import {
  createRunGate,
  rerunBody,
  runGateKey,
  setAsideDelaySeconds,
  type PendingWake,
} from "./run-gate.js";

function wake(messageId: string, body: Record<string, unknown> = { runId: "wrun_1" }): PendingWake {
  return {
    message: {
      id: "greet",
      data: Buffer.from(JSON.stringify(body)),
      attempt: 1,
      messageId: messageId as MessageData["messageId"],
      tenantId: "t",
      deploymentId: "d",
    },
    attempt: 1,
    deliveryCount: 1,
    maxAttempts: 49,
    hookResume: body.hookResumeTiming !== undefined,
  };
}

describe("run gate", () => {
  const key = runGateKey("t", "wrun_1");

  test("one holder per run; other runs and tenants are independent", () => {
    const gate = createRunGate();
    expect(gate.tryEnter(key)).toBe(true);
    expect(gate.tryEnter(key)).toBe(false);
    expect(gate.tryEnter(runGateKey("t", "wrun_2"))).toBe(true);
    expect(gate.tryEnter(runGateKey("u", "wrun_1"))).toBe(true);
    expect(gate.snapshot()).toEqual({ held: 3, pendingWakes: 0 });
  });

  test("keeps one wake per run; a hook resume's replaces one without, never the reverse", () => {
    const gate = createRunGate();
    gate.tryEnter(key);
    expect(gate.offerWake(key, wake("a"))).toBe("pending");
    expect(gate.offerWake(key, wake("b"))).toBe("covered");
    expect(gate.offerWake(key, wake("c", { runId: "wrun_1", hookResumeTiming: 1 }))).toBe(
      "replaced",
    );
    expect(gate.offerWake(key, wake("d", { runId: "wrun_1", hookResumeTiming: 2 }))).toBe(
      "covered",
    );
    expect(gate.offerWake(key, wake("e"))).toBe("covered");
    expect(gate.takeWake(key)?.message.messageId).toBe("c");
    expect(gate.takeWake(key)).toBeUndefined();
  });

  test("closing re-adds the pending wake and refuses any offered after", async () => {
    const gate = createRunGate();
    gate.tryEnter(key);
    gate.offerWake(key, wake("first"));
    const readded: string[] = [];
    await gate.close(key, async (pending) => {
      readded.push(pending.message.messageId);
      await Promise.resolve();
      // Claimed while the re-add is in flight: the caller must put it back.
      expect(gate.offerWake(key, wake("meanwhile"))).toBe("closing");
    });
    expect(readded).toEqual(["first"]);
    // Still held until the holder leaves; nothing in memory.
    expect(gate.snapshot()).toEqual({ held: 1, pendingWakes: 0 });
    expect(gate.offerWake(key, wake("later"))).toBe("closing");
    await gate.leave(key, async () => {
      throw new Error("nothing left to re-add");
    });
    expect(gate.tryEnter(key)).toBe(true);
    // A new holder's gate is open again.
    expect(gate.offerWake(key, wake("next"))).toBe("pending");
  });

  test("markClosing refuses new wakes and leave re-adds the one pending", async () => {
    const gate = createRunGate();
    gate.tryEnter(key);
    gate.offerWake(key, wake("kept"));
    gate.markClosing(key);
    expect(gate.offerWake(key, wake("refused"))).toBe("closing");
    const readded: string[] = [];
    await gate.leave(key, async (pending) => {
      readded.push(pending.message.messageId);
    });
    expect(readded).toEqual(["kept"]);
    expect(gate.snapshot()).toEqual({ held: 0, pendingWakes: 0 });
  });

  test("a failed re-add still releases the gate, and throws", async () => {
    const gate = createRunGate();
    gate.tryEnter(key);
    gate.offerWake(key, wake("lost"));
    await expect(
      gate.leave(key, async () => {
        throw new Error("database down");
      }),
    ).rejects.toThrow("database down");
    expect(gate.snapshot()).toEqual({ held: 0, pendingWakes: 0 });
  });

  test("a re-run delivers the holder's message with the wake's resume timing and trace", () => {
    const holder = wake("holder", {
      runId: "wrun_1",
      runSpecVersion: 9,
      hookResumeTiming: "served already",
      traceCarrier: "holder-trace",
    }).message;
    const withResume = rerunBody(
      holder,
      wake("w", { runId: "wrun_1", hookResumeTiming: "new", traceCarrier: "wake-trace" }),
    );
    expect(JSON.parse(withResume.toString("utf8"))).toEqual({
      runId: "wrun_1",
      runSpecVersion: 9,
      hookResumeTiming: "new",
      traceCarrier: "wake-trace",
    });
    const plain = rerunBody(holder, wake("w"));
    expect(JSON.parse(plain.toString("utf8"))).toEqual({
      runId: "wrun_1",
      runSpecVersion: 9,
      traceCarrier: "holder-trace",
    });
  });

  test("a set-aside message waits 1 s, doubling, at most 30 s", () => {
    expect([0, 1, 2, 3, 4, 5, 6, 20].map(setAsideDelaySeconds)).toEqual([
      1, 2, 4, 8, 16, 30, 30, 30,
    ]);
  });
});
