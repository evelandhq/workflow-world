import { describe, expect, test } from "vitest";
import { resolveStreamCompaction } from "./config.js";
import {
  compactStreamChunk,
  createStreamRehydrator,
  type StreamRehydrationCheckpoint,
} from "./stream-compaction.js";

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

describe("createStreamRehydrator", () => {
  test("strip then rehydrate reproduces Eve's original wire bytes", () => {
    const wire = [
      encodeEveChunk(reasoningAppended("思", "思")),
      encodeEveChunk(reasoningAppended("考", "思考")),
      encodeEveChunk(messageAppended("你好", "你好", { sequence: 1 })),
      encodeEveChunk(messageAppended("，世界", "你好，世界", { sequence: 1 })),
    ];
    const rehydrator = createStreamRehydrator();
    for (const original of wire) {
      expect(rehydrator.rehydrate(compactStreamChunk(original)).equals(original)).toBe(true);
    }
  });

  test("adopts legacy snapshots in a mixed-format stream", () => {
    const legacy = encodeEveChunk(messageAppended("Hello", "Hello"));
    const compacted = encodeEveChunk(messageAppended(" world", "Hello world"));
    const rehydrator = createStreamRehydrator();

    expect(rehydrator.rehydrate(legacy)).toBe(legacy);
    expect(rehydrator.rehydrate(compactStreamChunk(compacted)).equals(compacted)).toBe(true);
  });

  test("can resume from a database-safe checkpoint without replaying the prefix", () => {
    const first = encodeEveChunk(messageAppended("checkpoint ", "checkpoint "));
    const second = encodeEveChunk(messageAppended("resume", "checkpoint resume"));
    const writer = createStreamRehydrator();
    writer.rehydrate(compactStreamChunk(first));

    const checkpoint: StreamRehydrationCheckpoint = writer.checkpoint();
    expect(JSON.parse(JSON.stringify(checkpoint))).toEqual(checkpoint);

    const resumed = createStreamRehydrator(checkpoint);
    expect(resumed.rehydrate(compactStreamChunk(second)).equals(second)).toBe(true);
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
