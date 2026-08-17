import { describe, expect, test } from "vitest";
import { PACKED_STREAM_CODEC_VERSION, packStreamChunks, unpackStreamRow } from "./stream-blocks.js";

const chunks = (count: number, size = 8) =>
  Array.from({ length: count }, (_, index) => ({
    id: `chnk_${String(index).padStart(4, "0")}` as const,
    data: Buffer.alloc(size, index),
  }));

describe("stream block codec", () => {
  test("packs logical chunks into 64-chunk physical blocks and round-trips", () => {
    const logical = chunks(130);
    const blocks = packStreamChunks(logical);

    expect(blocks.map((block) => block.chunkCount)).toEqual([64, 64, 2]);
    expect(blocks.every((block) => block.codecVersion === PACKED_STREAM_CODEC_VERSION)).toBe(true);
    expect(blocks.map((block) => block.firstChunkId)).toEqual([
      logical[0]!.id,
      logical[64]!.id,
      logical[128]!.id,
    ]);
    expect(blocks.map((block) => block.lastChunkId)).toEqual([
      logical[63]!.id,
      logical[127]!.id,
      logical[129]!.id,
    ]);

    expect(
      blocks.flatMap((block) =>
        unpackStreamRow({
          chunkId: block.firstChunkId,
          lastChunkId: block.lastChunkId,
          chunkCount: block.chunkCount,
          codecVersion: block.codecVersion,
          data: block.data,
          eof: false,
        }),
      ),
    ).toEqual(logical);
  });

  test("starts a new block before the encoded block would exceed its byte limit", () => {
    const logical = chunks(5, 100);
    const blocks = packStreamChunks(logical, { maxChunks: 64, maxBytes: 250 });

    expect(blocks.map((block) => block.chunkCount)).toEqual([2, 2, 1]);
    expect(blocks.every((block) => block.data.byteLength <= 250)).toBe(true);
  });

  test("allows one oversized logical chunk without splitting its bytes", () => {
    const [block] = packStreamChunks(chunks(1, 1024), { maxChunks: 64, maxBytes: 64 });

    expect(block?.chunkCount).toBe(1);
    expect(
      unpackStreamRow({
        chunkId: block!.firstChunkId,
        lastChunkId: block!.lastChunkId,
        chunkCount: block!.chunkCount,
        codecVersion: block!.codecVersion,
        data: block!.data,
        eof: false,
      }),
    ).toEqual(chunks(1, 1024));
  });

  test("reads legacy rows as one logical chunk", () => {
    const data = Buffer.from("legacy");

    expect(
      unpackStreamRow({
        chunkId: "chnk_legacy",
        lastChunkId: null,
        chunkCount: null,
        codecVersion: null,
        data,
        eof: false,
      }),
    ).toEqual([{ id: "chnk_legacy", data }]);
  });

  test("rejects malformed packed data instead of returning partial chunks", () => {
    const [block] = packStreamChunks(chunks(2));
    expect(() =>
      unpackStreamRow({
        chunkId: block!.firstChunkId,
        lastChunkId: block!.lastChunkId,
        chunkCount: block!.chunkCount,
        codecVersion: block!.codecVersion,
        data: block!.data.subarray(0, block!.data.length - 1),
        eof: false,
      }),
    ).toThrow(/Malformed packed stream block/);
  });
});
