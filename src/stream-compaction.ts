/** `@workflow/serde`'s devalue-flat format tag. */
const FORMAT_TAG = "devl";

type ParsedFrame = { inner: Buffer };
type AppendedEvent = {
  type: string;
  data: Record<string, unknown>;
  [key: string]: unknown;
};

const SNAPSHOT_FIELDS: Record<string, { deltaKey: string; soFarKey: string }> = {
  "message.appended": { deltaKey: "messageDelta", soFarKey: "messageSoFar" },
  "reasoning.appended": { deltaKey: "reasoningDelta", soFarKey: "reasoningSoFar" },
};

function parseFrames(chunk: Buffer): ParsedFrame[] | null {
  const frames: ParsedFrame[] = [];
  let offset = 0;
  while (offset < chunk.length) {
    if (chunk.length - offset < 8) return null;
    const length = chunk.readUInt32BE(offset);
    if (length < 4 || offset + 4 + length > chunk.length) return null;
    if (chunk.subarray(offset + 4, offset + 8).toString("latin1") !== FORMAT_TAG) return null;

    let parsed: unknown;
    try {
      parsed = JSON.parse(chunk.subarray(offset + 8, offset + 4 + length).toString("utf8"));
    } catch {
      return null;
    }
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
    const inner = Buffer.from(parsed[1], "base64");
    const frameEnd = offset + 4 + length;
    if (!encodeFrame(inner).equals(chunk.subarray(offset, frameEnd))) return null;
    frames.push({ inner });
    offset = frameEnd;
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
  if (!encodeEventLine(candidate).equals(inner)) return null;
  return candidate;
}

function encodeEventLine(event: AppendedEvent): Buffer {
  return Buffer.from(`${JSON.stringify(event)}\n`, "utf8");
}

function withoutSnapshot(data: Record<string, unknown>, soFarKey: string): Record<string, unknown> {
  const rebuilt: Record<string, unknown> = {};
  for (const [key, value] of Object.entries(data)) {
    if (key !== soFarKey) rebuilt[key] = value;
  }
  return rebuilt;
}

/**
 * Strip Eve's cumulative snapshots while preserving unknown bytes exactly.
 *
 * Every supported Eve line speaks message stream v25 (0.50.0 and later), whose
 * appends carry the delta alone, so on a current run this is a no-op: the
 * frame is parsed, nothing is found, and the original buffer is returned. It
 * stays on as a cheap write-side guard so that a v24-shaped append -- a stale
 * build, a hand-written chunk -- can never reintroduce O(n²) storage.
 *
 * Nothing rebuilds the snapshot on read. The rehydrator that used to do so was
 * removed once the last v24 line left the supported window: it re-inflated
 * every read back to O(n²) bytes that a v25 runtime immediately normalized away
 * again, and it persisted accumulator checkpoints for the same non-purpose.
 * Stored bytes are what readers get.
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
    return encodeEventLine({ ...event, data: withoutSnapshot(event.data, soFarKey) });
  });
  return changed ? Buffer.concat(rebuilt.map(encodeFrame)) : chunk;
}
