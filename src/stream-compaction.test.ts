import { describe, expect, test } from "vitest";
import { resolveStreamCompaction } from "./config.js";
import { compactStreamChunk } from "./stream-compaction.js";

function encodeEveChunk(event: unknown): Buffer {
  const inner = Buffer.from(`${JSON.stringify(event)}\n`, "utf8");
  return encodeEveInner(inner);
}

function encodeEveInner(inner: Buffer, payloadText?: string): Buffer {
  const payload = Buffer.from(
    payloadText ?? JSON.stringify([["Uint8Array", 1], inner.toString("base64")]),
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

const messageAppended = (delta: string, soFar: string, coords = {}) => ({
  data: {
    messageDelta: delta,
    messageSoFar: soFar,
    sequence: 0,
    stepIndex: 0,
    turnId: "turn_0",
    ...coords,
  },
  type: "message.appended",
  meta: { at: "2026-08-13T00:00:00.000Z", id: "evt_x" },
});

const reasoningAppended = (delta: string, soFar: string) => ({
  data: {
    reasoningDelta: delta,
    reasoningSoFar: soFar,
    sequence: 0,
    stepIndex: 0,
    turnId: "turn_0",
  },
  type: "reasoning.appended",
  meta: { at: "2026-08-13T00:00:00.000Z", id: "evt_y" },
});

describe("compactStreamChunk", () => {
  test("strips only Eve accumulated snapshot fields", () => {
    for (const [event, field] of [
      [messageAppended("世界", "你好世界"), "messageSoFar"],
      [reasoningAppended(" world", "hello world"), "reasoningSoFar"],
    ] as const) {
      const [decoded] = decodeEveChunk(compactStreamChunk(encodeEveChunk(event))) as [
        { data: Record<string, unknown> },
      ];
      expect(decoded.data).not.toHaveProperty(field);
    }
  });

  test.each([
    Buffer.from("opaque bytes"),
    Buffer.alloc(0),
    (() => {
      const chunk = encodeEveChunk(messageAppended("x", "x"));
      chunk.write("cbor", 4, "latin1");
      return chunk;
    })(),
    encodeEveChunk({ data: { turnId: "turn_0" }, type: "turn.started" }),
    (() => {
      const event = messageAppended("x", "x");
      return encodeEveInner(Buffer.from(` ${JSON.stringify(event)}\n`));
    })(),
    (() => {
      const inner = Buffer.from(`${JSON.stringify(messageAppended("x", "x"))}\n`);
      return encodeEveInner(
        inner,
        `[ ["Uint8Array", 1], ${JSON.stringify(inner.toString("base64"))} ]`,
      );
    })(),
  ])("passes unknown input through by identity", (chunk) => {
    expect(compactStreamChunk(chunk)).toBe(chunk);
  });
});

describe("compactStreamChunk on message stream v25", () => {
  test("delta-only appends pass through by identity", () => {
    // Eve 0.50+ writes appends without `messageSoFar`; nothing is stripped and
    // nothing is rebuilt on read, so the stored bytes are the wire bytes.
    const deltaOnly = (delta: string) => ({
      data: { messageDelta: delta, sequence: 0, stepIndex: 0, turnId: "turn_0" },
      type: "message.appended",
      meta: { at: "2026-09-03T00:00:00.000Z", id: "evt_v25" },
    });
    for (const chunk of ["你好", "世界", "!"].map((delta) => encodeEveChunk(deltaOnly(delta)))) {
      expect(compactStreamChunk(chunk)).toBe(chunk);
    }
  });

  test("a v24-shaped append is reduced to the same bytes a v25 writer produces", () => {
    const stripped = compactStreamChunk(encodeEveChunk(messageAppended("，世界", "你好，世界")));
    const [decoded] = decodeEveChunk(stripped) as [{ data: Record<string, unknown> }];
    expect(decoded.data).toEqual({
      messageDelta: "，世界",
      sequence: 0,
      stepIndex: 0,
      turnId: "turn_0",
    });
    expect(compactStreamChunk(stripped)).toBe(stripped);
  });
});

describe("resolveStreamCompaction", () => {
  test.each([undefined, "", "on", "true", "1"])("%j means on", (value) => {
    expect(resolveStreamCompaction(value)).toBe(true);
  });

  test.each(["off", "false", "0"])("%j means off", (value) => {
    expect(resolveStreamCompaction(value)).toBe(false);
  });

  test("rejects typos", () => {
    expect(() => resolveStreamCompaction("offf")).toThrow(/Invalid stream compaction/);
  });
});
