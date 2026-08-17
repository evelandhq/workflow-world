export const PACKED_STREAM_CODEC_VERSION = 2;
export const DEFAULT_STREAM_BLOCK_CHUNKS = 64;
export const DEFAULT_STREAM_BLOCK_BYTES = 256 * 1024;

export type LogicalStreamChunk = {
  id: `chnk_${string}`;
  data: Buffer;
};

export type PackedStreamBlock = {
  firstChunkId: `chnk_${string}`;
  lastChunkId: `chnk_${string}`;
  chunkCount: number;
  codecVersion: typeof PACKED_STREAM_CODEC_VERSION;
  data: Buffer;
};

type PhysicalStreamRow = {
  chunkId: `chnk_${string}` | string;
  lastChunkId: `chnk_${string}` | string | null;
  chunkCount: number | null;
  codecVersion: number | null;
  data: Buffer;
  eof: boolean;
};

const ENTRY_HEADER_BYTES = 6;

export function packStreamChunks(
  chunks: LogicalStreamChunk[],
  options: { maxChunks?: number; maxBytes?: number } = {},
): PackedStreamBlock[] {
  const maxChunks = options.maxChunks ?? DEFAULT_STREAM_BLOCK_CHUNKS;
  const maxBytes = options.maxBytes ?? DEFAULT_STREAM_BLOCK_BYTES;
  assertPositiveInteger(maxChunks, "maxChunks");
  assertPositiveInteger(maxBytes, "maxBytes");

  const blocks: PackedStreamBlock[] = [];
  let pending: LogicalStreamChunk[] = [];
  let pendingBytes = 0;

  const flush = () => {
    if (pending.length === 0) return;
    blocks.push(encodeBlock(pending, pendingBytes));
    pending = [];
    pendingBytes = 0;
  };

  for (const chunk of chunks) {
    const idBytes = Buffer.byteLength(chunk.id, "utf8");
    if (idBytes > 0xffff) throw new TypeError("stream chunk id is too long to pack");
    const encodedBytes = ENTRY_HEADER_BYTES + idBytes + chunk.data.byteLength;
    if (
      pending.length > 0 &&
      (pending.length >= maxChunks || pendingBytes + encodedBytes > maxBytes)
    ) {
      flush();
    }
    pending.push(chunk);
    pendingBytes += encodedBytes;
  }
  flush();
  return blocks;
}

export function unpackStreamRow(row: PhysicalStreamRow): LogicalStreamChunk[] {
  if (row.eof) return [];
  if (row.codecVersion === null || row.codecVersion === 1) {
    return [{ id: row.chunkId as `chnk_${string}`, data: row.data }];
  }
  if (row.codecVersion !== PACKED_STREAM_CODEC_VERSION) {
    throw new Error(`Unsupported packed stream codec version ${String(row.codecVersion)}`);
  }

  const expectedCount = row.chunkCount;
  if (!Number.isSafeInteger(expectedCount) || expectedCount! <= 0) malformed();
  const chunks: LogicalStreamChunk[] = [];
  let offset = 0;
  while (offset < row.data.byteLength) {
    if (row.data.byteLength - offset < ENTRY_HEADER_BYTES) malformed();
    const idLength = row.data.readUInt16BE(offset);
    const dataLength = row.data.readUInt32BE(offset + 2);
    const end = offset + ENTRY_HEADER_BYTES + idLength + dataLength;
    if (end > row.data.byteLength) malformed();
    const id = row.data
      .subarray(offset + ENTRY_HEADER_BYTES, offset + ENTRY_HEADER_BYTES + idLength)
      .toString("utf8");
    if (!id.startsWith("chnk_")) malformed();
    chunks.push({
      id: id as `chnk_${string}`,
      data: row.data.subarray(offset + ENTRY_HEADER_BYTES + idLength, end),
    });
    offset = end;
  }

  if (
    chunks.length !== expectedCount ||
    chunks[0]?.id !== row.chunkId ||
    chunks.at(-1)?.id !== row.lastChunkId
  ) {
    malformed();
  }
  return chunks;
}

function encodeBlock(chunks: LogicalStreamChunk[], byteLength: number): PackedStreamBlock {
  const data = Buffer.allocUnsafe(byteLength);
  let offset = 0;
  for (const chunk of chunks) {
    const id = Buffer.from(chunk.id, "utf8");
    data.writeUInt16BE(id.byteLength, offset);
    data.writeUInt32BE(chunk.data.byteLength, offset + 2);
    id.copy(data, offset + ENTRY_HEADER_BYTES);
    chunk.data.copy(data, offset + ENTRY_HEADER_BYTES + id.byteLength);
    offset += ENTRY_HEADER_BYTES + id.byteLength + chunk.data.byteLength;
  }
  return {
    firstChunkId: chunks[0]!.id,
    lastChunkId: chunks.at(-1)!.id,
    chunkCount: chunks.length,
    codecVersion: PACKED_STREAM_CODEC_VERSION,
    data,
  };
}

function malformed(): never {
  throw new Error("Malformed packed stream block");
}

function assertPositiveInteger(value: number, name: string): void {
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new TypeError(`${name} must be a positive safe integer`);
  }
}
