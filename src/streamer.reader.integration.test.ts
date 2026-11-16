import { EventEmitter } from "node:events";
import { Pool } from "pg";
import { afterAll, afterEach, beforeAll, describe, expect, test, vi } from "vitest";
import { createWorld } from "./index.js";
import {
  dropTenantPartitions,
  ensureTenantPartitions,
  resolveMigrationsDir,
  runMigrations,
} from "./migrate.js";
import { packStreamChunks } from "./stream-blocks.js";

const testUrl = process.env.WORKFLOW_WORLD_TEST_URL;
const suffix = `${String(process.pid)}${Date.now().toString(36)}`;
const TENANT = `p_stream_reader_${suffix}`;
const RUN = "wrun_reader";

const text = (value: string) => new TextEncoder().encode(value);
const settle = (ms = 300) => new Promise((resolve) => setTimeout(resolve, ms));

async function collect(stream: ReadableStream<Uint8Array>): Promise<string[]> {
  const seen: string[] = [];
  const reader = stream.getReader();
  for (;;) {
    const { value, done } = await reader.read();
    if (done) return seen;
    seen.push(new TextDecoder().decode(value));
  }
}

// A long-lived process serves many stream reads, and a consumer may stall or go
// away at any point. What a reader holds on to must not grow with the stream.
describe.skipIf(!testUrl)("stream readers", () => {
  let pool: Pool;
  let world: ReturnType<typeof createWorld>;

  beforeAll(async () => {
    pool = new Pool({ connectionString: testUrl, max: 3 });
    await runMigrations(pool, { migrationsDir: resolveMigrationsDir() });
    await ensureTenantPartitions(pool, TENANT);
    world = createWorld({
      pool,
      tenantId: TENANT,
      deploymentId: "dep_stream_reader",
      compactStreamSnapshots: false,
    });
  }, 60_000);

  afterEach(() => {
    vi.restoreAllMocks();
  });

  afterAll(async () => {
    await world?.close?.();
    await dropTenantPartitions(pool, TENANT).catch(() => {});
    await pool?.end().catch(() => {});
  });

  /** Reads of the chunk table from here on. */
  function chunkReads(): () => number {
    const query = vi.spyOn(pool, "query");
    return () =>
      query.mock.calls.filter(([statement]) => {
        const sql =
          typeof statement === "string" ? statement : (statement as { text: string }).text;
        return /^[\s(]*select/i.test(sql) && sql.includes("workflow_stream_chunks");
      }).length;
  }

  /** Stream listeners registered and not yet removed, from here on. */
  function streamListeners(): () => number {
    const live = new Set<unknown>();
    const on = EventEmitter.prototype.on;
    const off = EventEmitter.prototype.off;
    vi.spyOn(EventEmitter.prototype, "on").mockImplementation(
      function (this: EventEmitter, name, fn) {
        if (typeof name === "string" && name.startsWith("strm:")) live.add(fn);
        return on.call(this, name, fn);
      },
    );
    vi.spyOn(EventEmitter.prototype, "off").mockImplementation(
      function (this: EventEmitter, name, fn) {
        live.delete(fn);
        return off.call(this, name, fn);
      },
    );
    return () => live.size;
  }

  test("a reader nobody reads from takes in no more than its first page", async () => {
    const streamId = `strm_idle_${suffix}`;
    const written = Array.from({ length: 80 }, (_, index) => `chunk-${index}`);
    for (const chunk of written.slice(0, 40)) await world.streams.write(RUN, streamId, text(chunk));

    const reads = chunkReads();
    const idle = await world.streams.get(RUN, streamId);
    for (const chunk of written.slice(40)) await world.streams.write(RUN, streamId, text(chunk));
    await world.streams.close(RUN, streamId);
    await settle();

    // One page to fill the queue; the 41 notifications that followed read nothing.
    expect(reads()).toBeLessThanOrEqual(1);

    // Everything is still there, in order, once somebody reads.
    expect(await collect(idle)).toEqual(written);
  }, 30_000);

  test("a reader that reached the end no longer listens for its stream", async () => {
    const streamId = `strm_ended_${suffix}`;
    await world.streams.write(RUN, streamId, text("one"));
    await world.streams.close(RUN, streamId);

    const listening = streamListeners();
    expect(await collect(await world.streams.get(RUN, streamId))).toEqual(["one"]);
    expect(listening()).toBe(0);
  });

  test("a cancelled reader no longer listens for its stream", async () => {
    const streamId = `strm_cancelled_${suffix}`;
    await world.streams.write(RUN, streamId, text("one"));

    const listening = streamListeners();
    const reader = (await world.streams.get(RUN, streamId)).getReader();
    expect(new TextDecoder().decode((await reader.read()).value)).toBe("one");
    expect(listening()).toBe(1);

    // Cancelled while it waits for a chunk that has not been written yet.
    const pending = reader.read();
    await reader.cancel();
    expect((await pending).done).toBe(true);
    expect(listening()).toBe(0);
  });

  test("a reader that caught up picks up what is written afterwards", async () => {
    const streamId = `strm_follow_${suffix}`;
    await world.streams.write(RUN, streamId, text("one"));
    const reading = collect(await world.streams.get(RUN, streamId));
    await settle();
    await world.streams.writeMulti!(RUN, streamId, [text("two"), text("three")]);
    await settle();
    await world.streams.write(RUN, streamId, text("four"));
    await world.streams.close(RUN, streamId);

    expect(await reading).toEqual(["one", "two", "three", "four"]);
  }, 20_000);

  test("a reader that is caught up reads once for each write", async () => {
    const streamId = `strm_live_reads_${suffix}`;
    await world.streams.write(RUN, streamId, text("one"));
    const reader = (await world.streams.get(RUN, streamId)).getReader();
    expect(new TextDecoder().decode((await reader.read()).value)).toBe("one");
    await settle();

    const reads = chunkReads();
    for (const chunk of ["two", "three", "four"]) {
      await world.streams.write(RUN, streamId, text(chunk));
      expect(new TextDecoder().decode((await reader.read()).value)).toBe(chunk);
    }
    await settle();
    expect(reads()).toBe(3);
    await reader.cancel();
  }, 20_000);

  test("a reader part-way through rows that maintenance then packs loses and repeats nothing", async () => {
    // Closed streams are rewritten into blocks while readers may still be on
    // them, which leaves a reader's position inside a block rather than
    // between two rows. The stream is longer than a reader takes in at once,
    // and the blocks are sized so that no block ends where the reader stopped.
    const streamId = `strm_packed_under_${suffix}`;
    const written = Array.from({ length: 300 }, (_, index) => `chunk-${index}`);
    for (const chunk of written) await world.streams.write(RUN, streamId, text(chunk));
    await world.streams.close(RUN, streamId);

    const reader = (await world.streams.get(RUN, streamId)).getReader();
    const seen: string[] = [];
    for (let index = 0; index < 10; index++) {
      seen.push(new TextDecoder().decode((await reader.read()).value));
    }

    const stored = await pool.query<{ id: `chnk_${string}`; data: Buffer }>(
      `delete from workflow.workflow_stream_chunks
        where tenant_id = $1 and stream_id = $2 and eof = false
        returning id, data`,
      [TENANT, streamId],
    );
    const blocks = packStreamChunks(
      stored.rows.sort((a, b) => (a.id < b.id ? -1 : 1)),
      { maxChunks: 37 },
    );
    for (const block of blocks) {
      await pool.query(
        `insert into workflow.workflow_stream_chunks
           (tenant_id, id, stream_id, run_id, data, eof, codec_version, chunk_count, last_chunk_id)
         values ($1, $2, $3, $4, $5, false, $6, $7, $8)`,
        [
          TENANT,
          block.firstChunkId,
          streamId,
          RUN,
          block.data,
          block.codecVersion,
          block.chunkCount,
          block.lastChunkId,
        ],
      );
    }

    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      seen.push(new TextDecoder().decode(value));
    }
    expect(seen).toEqual(written);
  }, 30_000);

  test("a start index skips that many chunks, and a negative one counts from the end", async () => {
    const streamId = `strm_offset_${suffix}`;
    const written = Array.from({ length: 30 }, (_, index) => `chunk-${index}`);
    for (const chunk of written) await world.streams.write(RUN, streamId, text(chunk));
    await world.streams.close(RUN, streamId);

    expect(await collect(await world.streams.get(RUN, streamId, 25))).toEqual(written.slice(25));
    expect(await collect(await world.streams.get(RUN, streamId, -3))).toEqual(written.slice(-3));
    expect(await collect(await world.streams.get(RUN, streamId, 30))).toEqual([]);
  }, 20_000);

  test("a start index inside a block skips the chunks before it in that block", async () => {
    const streamId = `strm_offset_block_${suffix}`;
    const written = Array.from({ length: 130 }, (_, index) => `chunk-${index}`);
    await world.streams.writeMulti!(RUN, streamId, written.map(text));
    await world.streams.close(RUN, streamId);

    expect(await collect(await world.streams.get(RUN, streamId, 70))).toEqual(written.slice(70));
    expect(await collect(await world.streams.get(RUN, streamId, -5))).toEqual(written.slice(-5));
  }, 20_000);

  test("a start index past what is written skips the chunks that arrive later", async () => {
    const streamId = `strm_offset_future_${suffix}`;
    await world.streams.write(RUN, streamId, text("one"));
    const reading = collect(await world.streams.get(RUN, streamId, 2));
    await settle();
    await world.streams.write(RUN, streamId, text("two"));
    await world.streams.write(RUN, streamId, text("three"));
    await world.streams.close(RUN, streamId);

    expect(await reading).toEqual(["three"]);
  }, 20_000);
});
