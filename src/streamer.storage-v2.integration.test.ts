import { Pool } from "pg";
import { afterAll, beforeAll, describe, expect, test } from "vitest";
import { createWorld } from "./index.js";
import {
  dropTenantPartitions,
  ensureTenantPartitions,
  resolveMigrationsDir,
  runMigrations,
} from "./migrate.js";
import { compactStreamChunk } from "./stream-compaction.js";

const testUrl = process.env.EVELAND_WORKFLOW_WORLD_TEST_URL;
const suffix = `${String(process.pid)}${Date.now().toString(36)}`;
const TENANT = `p_stream_v2_${suffix}`;

function encodeMessage(delta: string, soFar: string): Buffer {
  const event = {
    data: {
      messageDelta: delta,
      messageSoFar: soFar,
      sequence: 0,
      stepIndex: 0,
      turnId: "turn_0",
    },
    type: "message.appended",
    meta: { at: "2026-08-17T00:00:00.000Z", id: `evt_${soFar.length}` },
  };
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

describe.skipIf(!testUrl)("stream storage v2", () => {
  let pool: Pool;
  let world: ReturnType<typeof createWorld>;
  let uncompacted: ReturnType<typeof createWorld>;

  beforeAll(async () => {
    pool = new Pool({ connectionString: testUrl, max: 3 });
    await runMigrations(pool, { migrationsDir: resolveMigrationsDir() });
    await ensureTenantPartitions(pool, TENANT);
    world = createWorld({
      pool,
      tenantId: TENANT,
      deploymentId: "dep_stream_v2",
      runner: "external",
    });
    uncompacted = createWorld({
      pool,
      tenantId: TENANT,
      deploymentId: "dep_stream_v2",
      runner: "external",
      compactStreamSnapshots: false,
    });
  }, 60_000);

  afterAll(async () => {
    await world?.close?.();
    await uncompacted?.close?.();
    await dropTenantPartitions(pool, TENANT).catch(() => {});
    await pool?.end().catch(() => {});
  });

  test("single writes strip snapshots and every read path serves the stored delta-only bytes", async () => {
    const streamId = `strm_snapshots_${suffix}`;
    let soFar = "";
    const wire = ["one ", "two ", "three ", "four"].map((delta) => {
      soFar += delta;
      return encodeMessage(delta, soFar);
    });
    for (const chunk of wire) await world.streams.write("wrun_snapshots", streamId, chunk);
    await world.streams.close("wrun_snapshots", streamId);

    const stored = await pool.query<{ data: Buffer }>(
      `select data from workflow.workflow_stream_chunks
        where tenant_id = $1 and stream_id = $2 and eof = false order by id`,
      [TENANT, streamId],
    );
    expect(stored.rows.map((row) => row.data)).not.toEqual(wire);
    expect(stored.rows.reduce((total, row) => total + row.data.length, 0)).toBeLessThan(
      wire.reduce((total, chunk) => total + chunk.length, 0),
    );

    const stored_bytes = stored.rows.map((row) => row.data);
    expect(stored_bytes).toEqual(wire.map(compactStreamChunk));
    const page = await world.streams.getChunks("wrun_snapshots", streamId, { limit: 10 });
    expect(page.data.map(({ data }) => Buffer.from(data))).toEqual(stored_bytes);
    expect(await collect(await world.streams.get("wrun_snapshots", streamId, 2))).toEqual(
      stored_bytes.slice(2),
    );
  });

  test("live readers expand a notified block into its stored logical chunks", async () => {
    const streamId = `strm_live_${suffix}`;
    const first = encodeMessage("live ", "live ");
    const rest = [encodeMessage("block ", "live block "), encodeMessage("tail", "live block tail")];
    await world.streams.write("wrun_live", streamId, first);
    const reading = collect(await world.streams.get("wrun_live", streamId));
    await new Promise((resolve) => setTimeout(resolve, 300));
    await world.streams.writeMulti!("wrun_live", streamId, rest);
    await world.streams.close("wrun_live", streamId);

    expect(await reading).toEqual([first, ...rest].map(compactStreamChunk));
  }, 20_000);

  test("the kill switch writes snapshots unchanged and readers serve them unchanged", async () => {
    const streamId = `strm_kill_${suffix}`;
    const wire = [encodeMessage("fat", "fat"), encodeMessage(" row", "fat row")];
    await uncompacted.streams.writeMulti!("wrun_kill", streamId, wire);
    await uncompacted.streams.close("wrun_kill", streamId);

    const page = await world.streams.getChunks("wrun_kill", streamId, { limit: 10 });
    expect(page.data.map(({ data }) => Buffer.from(data))).toEqual(wire);
  });

  test("writeMulti stores 130 logical chunks in three physical blocks", async () => {
    const streamId = `strm_blocks_${suffix}`;
    const wire = Array.from({ length: 130 }, (_, index) => Buffer.from(`chunk-${index}`));

    await world.streams.writeMulti!("wrun_blocks", streamId, wire);
    await world.streams.close("wrun_blocks", streamId);

    const rows = await pool.query<{
      codec_version: number;
      chunk_count: number;
      last_chunk_id: string;
    }>(
      `select codec_version, chunk_count, last_chunk_id
         from workflow.workflow_stream_chunks
        where tenant_id = $1 and stream_id = $2 and eof = false
        order by id`,
      [TENANT, streamId],
    );
    expect(rows.rows.map((row) => row.chunk_count)).toEqual([64, 64, 2]);
    expect(rows.rows.every((row) => row.codec_version === 2 && row.last_chunk_id)).toBe(true);

    const read = await world.streams.getChunks("wrun_blocks", streamId, { limit: 200 });
    expect(read.data.map(({ data }) => Buffer.from(data))).toEqual(wire);
    expect(await world.streams.getInfo("wrun_blocks", streamId)).toEqual({
      tailIndex: 129,
      done: true,
    });
  });

  test("cursor reads preserve stored bytes and write no checkpoints", async () => {
    const streamId = `strm_cursor_${suffix}`;
    let soFar = "";
    const wire = Array.from({ length: 140 }, () => {
      soFar += "x";
      return encodeMessage("x", soFar);
    });
    await world.streams.writeMulti!("wrun_cursor", streamId, wire);
    await world.streams.close("wrun_cursor", streamId);

    const first = await world.streams.getChunks("wrun_cursor", streamId, { limit: 64 });
    const second = await world.streams.getChunks("wrun_cursor", streamId, {
      limit: 64,
      cursor: first.cursor!,
    });
    expect([...first.data, ...second.data].map(({ data }) => Buffer.from(data))).toEqual(
      wire.slice(0, 128).map(compactStreamChunk),
    );
    expect(second.data.map(({ index }) => index)).toEqual(
      Array.from({ length: 64 }, (_, offset) => 64 + offset),
    );

    const checkpoints = await pool.query(
      `select 1 from workflow.workflow_stream_checkpoints where tenant_id = $1 and stream_id = $2`,
      [TENANT, streamId],
    );
    expect(checkpoints.rowCount).toBe(0);
  });

  test("new readers continue to read legacy one-row chunks", async () => {
    const streamId = `strm_legacy_${suffix}`;
    await pool.query(
      `insert into workflow.workflow_stream_chunks
         (tenant_id, id, stream_id, run_id, data, eof)
       values ($1, 'chnk_legacy_1', $2, 'wrun_legacy', $3, false),
              ($1, 'chnk_legacy_2', $2, 'wrun_legacy', $4, false),
              ($1, 'chnk_legacy_eof', $2, 'wrun_legacy', ''::bytea, true)`,
      [TENANT, streamId, Buffer.from("one"), Buffer.from("two")],
    );

    const read = await world.streams.getChunks("wrun_legacy", streamId, { limit: 10 });
    expect(read.data.map(({ data }) => Buffer.from(data).toString())).toEqual(["one", "two"]);
    expect(read.done).toBe(true);
  });
});

async function collect(stream: ReadableStream<Uint8Array>): Promise<Buffer[]> {
  const chunks: Buffer[] = [];
  const reader = stream.getReader();
  for (;;) {
    const { value, done } = await reader.read();
    if (done) return chunks;
    chunks.push(Buffer.from(value));
  }
}
