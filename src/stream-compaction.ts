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

const RESET_FIELDS: Record<string, string> = {
  "message.completed": "message.appended",
  "reasoning.completed": "reasoning.appended",
};

export type StreamRehydrationCheckpoint = {
  version: 1;
  accumulators: [key: string, value: string][];
};

export type StreamRehydrator = {
  rehydrate(chunk: Buffer): Buffer;
  checkpoint(): StreamRehydrationCheckpoint;
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

function accumulatorKey(family: string, data: Record<string, unknown>): string {
  return `${family}:${String(data.turnId)}:${String(data.stepIndex)}:${String(data.sequence)}`;
}

/** Strip Eve's cumulative snapshots while preserving unknown bytes exactly. */
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
  return changed ? Buffer.concat(rebuilt.map(encodeFrame)) : chunk;
}

/** Rehydrate compacted rows, optionally resuming from a persisted checkpoint. */
export function createStreamRehydrator(checkpoint?: StreamRehydrationCheckpoint): StreamRehydrator {
  const accumulators = new Map<string, string>();
  if (checkpoint?.version === 1 && Array.isArray(checkpoint.accumulators)) {
    for (const entry of checkpoint.accumulators) {
      if (
        Array.isArray(entry) &&
        entry.length === 2 &&
        typeof entry[0] === "string" &&
        typeof entry[1] === "string"
      ) {
        accumulators.set(entry[0], entry[1]);
      }
    }
  }

  return {
    rehydrate(chunk) {
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
      return changed ? Buffer.concat(rebuilt.map(encodeFrame)) : chunk;
    },

    checkpoint() {
      return { version: 1, accumulators: [...accumulators.entries()] };
    },
  };
}
