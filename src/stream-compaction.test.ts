import { describe, expect, test } from "vitest";
import { resolveStreamCompaction } from "./config.js";
import { compactStreamChunk, createStreamRehydrator } from "./stream-compaction.js";

/**
 * Encode an event exactly the way it arrives from eve: one newline-terminated
 * JSON line, wrapped in `@workflow/serde`'s devalue-flat Uint8Array envelope,
 * with the 4-byte big-endian length + `devl` tag framing. Verified against
 * 5,515 production chunks — every one round-tripped byte-identically through
 * this encoding.
 */
function encodeEveChunk(event: unknown): Buffer {
  const inner = Buffer.from(`${JSON.stringify(event)}\n`, "utf8");
  const payload = Buffer.from(
    JSON.stringify([["Uint8Array", 1], inner.toString("base64")]),
    "utf8",
  );
  const frame = Buffer.alloc(8 + payload.length);
  frame.writeUInt32BE(4 + payload.length, 0);
  frame.write("devl", 4, "latin1");
  payload.copy(frame, 8);
  return frame;
}

function decodeEveChunk(chunk: Buffer): unknown[] {
  const events: unknown[] = [];
  let offset = 0;
  while (offset < chunk.length) {
    const length = chunk.readUInt32BE(offset);
    const payload = JSON.parse(chunk.subarray(offset + 8, offset + 4 + length).toString("utf8"));
    events.push(JSON.parse(Buffer.from(payload[1], "base64").toString("utf8")));
    offset += 4 + length;
  }
  return events;
}

const meta = (n: number) => ({ at: `2026-08-13T00:00:0${n}.000Z`, id: `evt_${n}` });

function messageAppended(delta: string, soFar: string, coords = {}) {
  return {
    data: {
      messageDelta: delta,
      messageSoFar: soFar,
      sequence: 0,
      stepIndex: 0,
      turnId: "turn_0",
      ...coords,
    },
    type: "message.appended",
    meta: meta(1),
  };
}

function reasoningAppended(delta: string, soFar: string, coords = {}) {
  return {
    data: {
      reasoningDelta: delta,
      reasoningSoFar: soFar,
      sequence: 0,
      stepIndex: 0,
      turnId: "turn_0",
      ...coords,
    },
    type: "reasoning.appended",
    meta: meta(2),
  };
}

describe("compactStreamChunk", () => {
  test("strips messageSoFar and reasoningSoFar, leaving everything else in place", () => {
    for (const [event, soFarKey] of [
      [messageAppended("世界", "你好世界"), "messageSoFar"],
      [reasoningAppended(" world", "hello world"), "reasoningSoFar"],
    ] as const) {
      const compacted = compactStreamChunk(encodeEveChunk(event));
      const [decoded] = decodeEveChunk(compacted) as [{ data: Record<string, unknown> }];
      expect(decoded.data).not.toHaveProperty(soFarKey);
      const { data, ...rest } = event;
      const { [soFarKey]: _dropped, ...keptData } = data as Record<string, unknown>;
      expect(decoded).toEqual({ ...rest, data: keptData });
    }
  });

  test("compacts every frame of a multi-frame chunk", () => {
    const chunk = Buffer.concat([
      encodeEveChunk(messageAppended("a", "a")),
      encodeEveChunk(messageAppended("b", "ab")),
    ]);
    const events = decodeEveChunk(compactStreamChunk(chunk)) as { data: object }[];
    expect(events).toHaveLength(2);
    for (const event of events) expect(event.data).not.toHaveProperty("messageSoFar");
  });

  test.each([
    ["plain text a conformance suite writes", Buffer.from("hello, not a frame")],
    ["an empty buffer", Buffer.alloc(0)],
    [
      "an unknown serde tag",
      (() => {
        const chunk = encodeEveChunk(messageAppended("x", "x"));
        chunk.write("cbor", 4, "latin1");
        return chunk;
      })(),
    ],
    ["a truncated frame", encodeEveChunk(messageAppended("x", "x")).subarray(0, 20)],
    [
      "json that is not an event line",
      (() => {
        const inner = Buffer.from("[1,2,3]\n", "utf8");
        const payload = Buffer.from(
          JSON.stringify([["Uint8Array", 1], inner.toString("base64")]),
          "utf8",
        );
        const frame = Buffer.alloc(8 + payload.length);
        frame.writeUInt32BE(4 + payload.length, 0);
        frame.write("devl", 4, "latin1");
        payload.copy(frame, 8);
        return frame;
      })(),
    ],
    [
      "an appended event without its snapshot field",
      encodeEveChunk({
        data: { messageDelta: "x", sequence: 0, stepIndex: 0, turnId: "turn_0" },
        type: "message.appended",
        meta: meta(3),
      }),
    ],
    [
      "a non-appended event",
      encodeEveChunk({
        data: { sequence: 0, turnId: "turn_0" },
        type: "turn.started",
        meta: meta(4),
      }),
    ],
  ])("passes %s through as the same instance", (_name, chunk) => {
    expect(compactStreamChunk(chunk)).toBe(chunk);
  });
});

describe("createStreamRehydrator", () => {
  test("strip → rehydrate reproduces the original wire bytes", () => {
    // Interleaved accumulations across coordinates, multi-byte text included:
    // exactly the shape eve writes for a turn with reasoning then a message.
    const wire = [
      encodeEveChunk({
        data: { sequence: 0, turnId: "turn_0" },
        type: "turn.started",
        meta: meta(0),
      }),
      encodeEveChunk(reasoningAppended("Th", "Th")),
      encodeEveChunk(reasoningAppended("inking", "Thinking")),
      encodeEveChunk({
        data: { reasoning: "Thinking", sequence: 0, stepIndex: 0, turnId: "turn_0" },
        type: "reasoning.completed",
        meta: meta(3),
      }),
      encodeEveChunk(messageAppended("你好", "你好", { sequence: 1 })),
      encodeEveChunk(messageAppended("，世界", "你好，世界", { sequence: 1 })),
      encodeEveChunk({
        data: {
          finishReason: "stop",
          message: "你好，世界",
          sequence: 1,
          stepIndex: 0,
          turnId: "turn_0",
        },
        type: "message.completed",
        meta: meta(6),
      }),
      // A fresh accumulation after the reset, same coordinates as the first.
      encodeEveChunk(reasoningAppended("Again", "Again")),
    ];
    const rehydrate = createStreamRehydrator();
    for (const original of wire) {
      const rehydrated = rehydrate(compactStreamChunk(original));
      expect(rehydrated.equals(original)).toBe(true);
    }
  });

  test("keeps concurrent accumulations apart by turn, step, and sequence", () => {
    const a = messageAppended("a", "a", { turnId: "turn_1" });
    const b = messageAppended("b", "b", { turnId: "turn_2" });
    const a2 = messageAppended("x", "ax", { turnId: "turn_1" });
    const rehydrate = createStreamRehydrator();
    for (const original of [a, b, a2].map(encodeEveChunk)) {
      expect(rehydrate(compactStreamChunk(original)).equals(original)).toBe(true);
    }
  });

  test("adopts the snapshot of a legacy uncompacted chunk mid-stream", () => {
    // A stream written partly before compaction shipped: the legacy chunk
    // carries its snapshot; later compacted chunks must continue from it.
    const legacy = encodeEveChunk(messageAppended("Hello", "Hello"));
    const later = encodeEveChunk(messageAppended(" world", "Hello world"));
    const rehydrate = createStreamRehydrator();
    expect(rehydrate(legacy)).toBe(legacy);
    expect(rehydrate(compactStreamChunk(later)).equals(later)).toBe(true);
  });

  test("passes foreign chunks through untouched without disturbing state", () => {
    const rehydrate = createStreamRehydrator();
    const first = encodeEveChunk(messageAppended("a", "a"));
    expect(rehydrate(compactStreamChunk(first)).equals(first)).toBe(true);
    const foreign = Buffer.from("opaque bytes");
    expect(rehydrate(foreign)).toBe(foreign);
    const second = encodeEveChunk(messageAppended("b", "ab"));
    expect(rehydrate(compactStreamChunk(second)).equals(second)).toBe(true);
  });
});

describe("resolveStreamCompaction", () => {
  test.each([undefined, "", "on", "true", "1"])("%j means on", (value) => {
    expect(resolveStreamCompaction(value)).toBe(true);
  });
  test.each(["off", "false", "0"])("%j means off", (value) => {
    expect(resolveStreamCompaction(value)).toBe(false);
  });
  test("rejects a typo instead of silently staying on", () => {
    expect(() => resolveStreamCompaction("offf")).toThrow(/Invalid stream compaction/);
  });
});
