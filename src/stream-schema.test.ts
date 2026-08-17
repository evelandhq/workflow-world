import { describe, expect, test } from "vitest";
import { getTableConfig } from "drizzle-orm/pg-core";
import { Schema } from "./drizzle/index.js";

describe("stream storage v2 schema", () => {
  test("stream rows expose block metadata", () => {
    expect(Schema.streams.codecVersion.name).toBe("codec_version");
    expect(Schema.streams.chunkCount.name).toBe("chunk_count");
    expect(Schema.streams.lastChunkId.name).toBe("last_chunk_id");
  });

  test("maintenance has a small pending-EOF candidate index", () => {
    expect(
      getTableConfig(Schema.streams).indexes.map((candidate) => candidate.config.name),
    ).toContain("workflow_stream_chunks_pending_pack_index");
  });

  test("rehydration checkpoints are internal database rows", () => {
    expect(Schema.streamCheckpoints.tenantId.name).toBe("tenant_id");
    expect(Schema.streamCheckpoints.streamId.name).toBe("stream_id");
    expect(Schema.streamCheckpoints.chunkId.name).toBe("chunk_id");
    expect(Schema.streamCheckpoints.nextIndex.name).toBe("next_index");
    expect(Schema.streamCheckpoints.state.name).toBe("state");
  });

  test("runs carry explicit maintenance deadlines", () => {
    expect(Schema.runs.retentionClass.name).toBe("retention_class");
    expect(Schema.runs.compactAfter.name).toBe("compact_after");
    expect(Schema.runs.expireAfter.name).toBe("expire_after");
    expect(Schema.runs.detailExpireAfter.name).toBe("detail_expire_after");
  });
});
