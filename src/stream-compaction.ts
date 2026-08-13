/**
 * Write-side compaction and read-side rehydration for eve's durable stream
 * snapshots.
 *
 * eve's harness emits one `message.appended` / `reasoning.appended` event per
 * token delta, and every one of them carries the full accumulated text in
 * `messageSoFar` / `reasoningSoFar` (vercel/eve#1441, verified unchanged
 * through eve 0.34). Each event is persisted as one stream chunk, so a
 * message's stored footprint is O(n²) in its delta count — measured 99.56%
 * redundant bytes in production, and a single busy tenant took the shared
 * world database to 10 GB in four days.
 *
 * Stripping the snapshot at write time is not enough on its own: eve's client
 * reducer renders `data.messageSoFar` directly (`message-reducer.js` never
 * reads `messageDelta`, verified on 0.32/0.33/0.34), so a delta-only wire
 * would blank live streaming in every consumer. Hence the pair:
 *
 * - {@link compactStreamChunk} removes the snapshot field before the row is
 *   written, turning storage O(n²) → O(n).
 * - {@link createStreamRehydrator} re-accumulates deltas on the read path and
 *   injects the snapshot back, byte-identically, so the wire format consumers
 *   see is exactly what eve wrote.
 *
 * The chunk bytes are layered: `[4-byte BE length]["devl"][devalue-serialized
 * Uint8Array]` framing from `@workflow/serde`, and inside that one
 * newline-terminated JSON event from eve's NDJSON stream. Both transforms
 * parse all layers strictly and PASS THE ORIGINAL BYTES THROUGH UNTOUCHED on
 * any deviation — an unknown serde tag, a multi-line payload, an unfamiliar
 * event shape. Chunks that are not eve appended-events (conformance suites
 * write arbitrary strings; eve writes a dozen other event types) cross both
 * functions unchanged, so the pair is safe to leave always-on for readers and
 * degrades to a no-op if eve changes its serialization. Round-trip
 * (strip → rehydrate ≡ original) was verified byte-for-byte on 5,515
 * production chunks before this shipped.
 */

/** `@workflow/serde`'s 4-character format tag for devalue-flat payloads. */
const FORMAT_TAG = "devl";

type ParsedFrame = {
  /** The NDJSON payload carried inside the serde envelope. */
  inner: Buffer;
};

type AppendedEvent = {
  type: string;
  data: Record<string, unknown>;
  [key: string]: unknown;
};

/**
 * Snapshot field to strip and delta field to re-accumulate, per event type.
 * The delta key doubles as the insertion anchor: eve serializes the snapshot
 * immediately after the delta, and rehydration re-inserts it there so the
 * reconstructed event is byte-identical to what eve wrote.
 */
const SNAPSHOT_FIELDS: Record<string, { deltaKey: string; soFarKey: string }> = {
  "message.appended": { deltaKey: "messageDelta", soFarKey: "messageSoFar" },
  "reasoning.appended": { deltaKey: "reasoningDelta", soFarKey: "reasoningSoFar" },
};

/**
 * A terminal event resets its family's accumulator, mirroring eve's emission:
 * the harness zeroes its running text when it emits `reasoning.completed`
 * (and starts a fresh accumulation per message), so a later appended event
 * with the same coordinates starts from empty again.
 */
const RESET_FIELDS: Record<string, string> = {
  "message.completed": "message.appended",
  "reasoning.completed": "reasoning.appended",
};

/**
 * Parse the serde framing. Returns null — meaning "leave the chunk alone" —
 * unless every frame is a well-formed `devl` devalue-flat Uint8Array.
 */
function parseFrames(chunk: Buffer): ParsedFrame[] | null {
  const frames: ParsedFrame[] = [];
  let offset = 0;
  while (offset < chunk.length) {
    if (chunk.length - offset < 8) return null;
    const length = chunk.readUInt32BE(offset);
    if (length < 4 || offset + 4 + length > chunk.length) return null;
    if (chunk.subarray(offset + 4, offset + 8).toString("latin1") !== FORMAT_TAG) return null;
    const payload = chunk.subarray(offset + 8, offset + 4 + length).toString("utf8");
    let parsed: unknown;
    try {
      parsed = JSON.parse(payload);
    } catch {
      return null;
    }
    // devalue's flat encoding of a single Uint8Array: [["Uint8Array",1],"<base64>"]
    if (
      !Array.isArray(parsed) ||
      parsed.length !== 2 ||
      !Array.isArray(parsed[0]) ||
      parsed[0].length !== 2 ||
      parsed[0][0] !== "Uint8Array" ||
      parsed[0][1] !== 1 ||
      typeof parsed[1] !== "string"
    ) {
      return null;
    }
    frames.push({ inner: Buffer.from(parsed[1], "base64") });
    offset += 4 + length;
  }
  return frames.length > 0 ? frames : null;
}

function encodeFrame(inner: Buffer): Buffer {
  const payload = Buffer.from(
    JSON.stringify([["Uint8Array", 1], inner.toString("base64")]),
    "utf8",
  );
  const frame = Buffer.alloc(8 + payload.length);
  frame.writeUInt32BE(4 + payload.length, 0);
  frame.write(FORMAT_TAG, 4, "latin1");
  payload.copy(frame, 8);
  return frame;
}

/** One newline-terminated JSON event, or null to leave the frame alone. */
function parseEventLine(inner: Buffer): AppendedEvent | null {
  const text = inner.toString("utf8");
  if (!text.endsWith("\n")) return null;
  const body = text.slice(0, -1);
  if (body.includes("\n")) return null;
  let event: unknown;
  try {
    event = JSON.parse(body);
  } catch {
    return null;
  }
  if (typeof event !== "object" || event === null) return null;
  const candidate = event as AppendedEvent;
  if (typeof candidate.type !== "string") return null;
  if (typeof candidate.data !== "object" || candidate.data === null) return null;
  return candidate;
}

function encodeEventLine(event: AppendedEvent): Buffer {
  return Buffer.from(`${JSON.stringify(event)}\n`, "utf8");
}

/**
 * Rebuild `data` without the snapshot key, or with it re-inserted directly
 * after the delta key. Iteration order preserves eve's serialization order,
 * which is what makes rehydration byte-identical.
 */
function rebuildData(
  data: Record<string, unknown>,
  deltaKey: string,
  soFarKey: string,
  soFarValue?: string,
): Record<string, unknown> {
  const rebuilt: Record<string, unknown> = {};
  for (const [key, value] of Object.entries(data)) {
    if (key === soFarKey) continue;
    rebuilt[key] = value;
    if (key === deltaKey && soFarValue !== undefined) rebuilt[soFarKey] = soFarValue;
  }
  return rebuilt;
}

/**
 * Accumulators are keyed by the coordinates eve stamps on every appended and
 * completed event. `sequence` is a turn-scoped counter (a turn interleaving
 * reasoning and text advances it), so all three are needed to keep parallel
 * accumulations apart.
 */
function accumulatorKey(family: string, data: Record<string, unknown>): string {
  return `${family}:${String(data.turnId)}:${String(data.stepIndex)}:${String(data.sequence)}`;
}

/**
 * Write-side: drop the accumulated-snapshot field from appended events.
 * Anything that is not exactly an eve appended event in serde framing is
 * returned unchanged (same Buffer instance, so callers can cheaply detect
 * pass-through by identity if they care).
 */
export function compactStreamChunk(chunk: Buffer): Buffer {
  const frames = parseFrames(chunk);
  if (!frames) return chunk;
  let changed = false;
  const rebuilt = frames.map(({ inner }) => {
    const event = parseEventLine(inner);
    const fields = event && SNAPSHOT_FIELDS[event.type];
    if (!event || !fields) return inner;
    const { deltaKey, soFarKey } = fields;
    if (typeof event.data[deltaKey] !== "string" || typeof event.data[soFarKey] !== "string") {
      return inner;
    }
    changed = true;
    return encodeEventLine({ ...event, data: rebuildData(event.data, deltaKey, soFarKey) });
  });
  if (!changed) return chunk;
  return Buffer.concat(rebuilt.map(encodeFrame));
}

/**
 * Read-side: a stateful transform that re-injects snapshots into compacted
 * appended events. One instance per stream read, fed every chunk exactly once
 * in write order — skipped-offset chunks included, because their deltas are
 * part of the accumulated text even when the chunk itself is not delivered.
 *
 * Legacy chunks that still carry their snapshot pass through unchanged and
 * re-seed the accumulator from it, so a stream written partly before and
 * partly after compaction rehydrates correctly across the boundary.
 */
export function createStreamRehydrator(): (chunk: Buffer) => Buffer {
  const accumulators = new Map<string, string>();
  return (chunk) => {
    const frames = parseFrames(chunk);
    if (!frames) return chunk;
    let changed = false;
    const rebuilt = frames.map(({ inner }) => {
      const event = parseEventLine(inner);
      if (!event) return inner;
      const resetFamily = RESET_FIELDS[event.type];
      if (resetFamily) {
        accumulators.delete(accumulatorKey(resetFamily, event.data));
        return inner;
      }
      const fields = SNAPSHOT_FIELDS[event.type];
      if (!fields) return inner;
      const { deltaKey, soFarKey } = fields;
      const delta = event.data[deltaKey];
      if (typeof delta !== "string") return inner;
      const key = accumulatorKey(event.type, event.data);
      const existing = event.data[soFarKey];
      if (typeof existing === "string") {
        accumulators.set(key, existing);
        return inner;
      }
      const soFar = (accumulators.get(key) ?? "") + delta;
      accumulators.set(key, soFar);
      changed = true;
      return encodeEventLine({
        ...event,
        data: rebuildData(event.data, deltaKey, soFarKey, soFar),
      });
    });
    if (!changed) return chunk;
    return Buffer.concat(rebuilt.map(encodeFrame));
  };
}
