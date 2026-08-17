/**
 * Multi-tenant port of `@workflow/world-postgres`'s streamer.
 *
 * Two things change from upstream: every chunk row is scoped to the ambient
 * tenant, and the LISTEN/NOTIFY channel is derived per tenant rather than being
 * the single global `workflow_event_chunk` topic.
 */
import { EventEmitter } from "node:events";
import type {
  GetChunksOptions,
  StreamChunksResponse,
  Streamer,
  StreamInfoResponse,
} from "@workflow/world";
import { and, asc, desc, eq, lte, sql } from "drizzle-orm";
import { Client, type Pool } from "pg";
import { monotonicFactory } from "ulid";
import * as z from "zod";
import { type Drizzle, Schema } from "./drizzle/index.js";
import { packStreamChunks, unpackStreamRow } from "./stream-blocks.js";
import {
  createCheckpointingRehydrator,
  type StoredStreamCheckpoint,
} from "./stream-checkpoints.js";
import { compactStreamChunk } from "./stream-compaction.js";
import { tenantStreamChannel } from "./tenant.js";
import { Mutex } from "./util.js";

const StreamPublishMessage = z.object({
  streamId: z.string(),
  chunkId: z.templateLiteral(["chnk_", z.string()]),
});

interface StreamChunkEvent {
  id: `chnk_${string}`;
  data: Uint8Array;
  eof: boolean;
}

class Rc<T extends { drop(): void }> {
  private refCount = 0;
  constructor(private resource: T) {}
  acquire() {
    this.refCount++;
    return {
      ...this.resource,
      [Symbol.dispose]: () => {
        this.release();
      },
    };
  }
  release() {
    this.refCount--;
    if (this.refCount <= 0) {
      this.resource.drop();
    }
  }
}

/**
 * Subscribe to a PostgreSQL NOTIFY channel using a dedicated client created
 * from the pool's connection options. `channel` must be a trusted identifier.
 */
export const listenChannel = async (
  pool: Pool,
  channel: string,
  onPayload: (payload: string) => Promise<void>,
): Promise<{ close: () => Promise<void> }> => {
  const client = new Client(pool.options);

  try {
    await client.connect();
    await client.query(`LISTEN "${channel}"`);
  } catch (err) {
    await client.end().catch(() => {});
    throw err;
  }

  const onNotification = (msg: { payload?: string | undefined }) => {
    onPayload(msg.payload ?? "").catch(() => {});
  };

  client.on("notification", onNotification);

  // `close` has to be idempotent. `createWorld().close()` is reachable more than
  // once in practice — a shutdown path and an error path can both call it, and a
  // supervisor may signal twice — and the second call used to throw "Client was
  // closed and is not queryable" from the `UNLISTEN`, because the client had
  // already ended.
  let closed = false;

  return {
    close: async () => {
      if (closed) return;
      closed = true;
      client.removeListener("notification", onNotification);
      try {
        await client.query(`UNLISTEN "${channel}"`);
      } catch {
        // Best-effort: `client.end()` below stops delivery regardless, and a
        // connection that has already gone away cannot be un-listened. Failing
        // here would turn a successful teardown into a thrown error.
      } finally {
        await client.end().catch(() => {});
      }
    },
  };
};

export type PostgresStreamer = Streamer & {
  /** Unlisten from the LISTEN subscription and release resources. */
  close(): Promise<void>;
};

export type StreamerOptions = {
  /** Strip Eve's cumulative snapshot fields before persistence. */
  compactSnapshots?: boolean;
};

export function createStreamer(
  pool: Pool,
  drizzle: Drizzle,
  tenantId: string,
  options: StreamerOptions = {},
): PostgresStreamer {
  const compactSnapshots = options.compactSnapshots ?? true;
  const compact = (chunk: Buffer): Buffer => (compactSnapshots ? compactStreamChunk(chunk) : chunk);
  const ulid = monotonicFactory();
  const events = new EventEmitter<{
    [key: `strm:${string}`]: [StreamChunkEvent];
  }>();
  const { streams } = Schema;
  const genChunkId = () => `chnk_${ulid()}` as const;
  const mutexes = new Map<string, Rc<{ drop(): void; mutex: Mutex }>>();
  const getMutex = (key: string) => {
    let mutex = mutexes.get(key);
    if (!mutex) {
      mutex = new Rc({
        mutex: new Mutex(),
        drop: () => mutexes.delete(key),
      });
      mutexes.set(key, mutex);
    }
    return mutex.acquire();
  };

  // One channel per tenant. Upstream uses a single global
  // `workflow_event_chunk` topic, which was harmless when each project had
  // its own database; here it would broadcast every chunk of every run to
  // every agent on the platform.
  const STREAM_TOPIC = tenantStreamChannel(tenantId);

  const listenSubscription = listenChannel(pool, STREAM_TOPIC, async (msg) => {
    const parsed = StreamPublishMessage.parse(JSON.parse(msg));

    const key = `strm:${parsed.streamId}` as const;
    if (!events.listenerCount(key)) {
      return;
    }

    const resource = getMutex(key);
    await resource.mutex.andThen(async () => {
      const [value] = await drizzle
        .select({
          chunkId: streams.chunkId,
          lastChunkId: streams.lastChunkId,
          chunkCount: streams.chunkCount,
          codecVersion: streams.codecVersion,
          eof: streams.eof,
          data: streams.chunkData,
        })
        .from(streams)
        .where(
          and(
            eq(Schema.streams.tenantId, tenantId),
            eq(streams.streamId, parsed.streamId),
            eq(streams.chunkId, parsed.chunkId),
          ),
        )
        .limit(1);
      if (!value) return;
      if (value.eof) {
        events.emit(key, { id: parsed.chunkId, data: value.data, eof: true });
        return;
      }
      for (const chunk of unpackStreamRow(value)) {
        events.emit(key, { id: chunk.id, data: chunk.data, eof: false });
      }
    });
  });

  const notifyStream = async (payload: string) => {
    await pool.query("SELECT pg_notify($1, $2)", [STREAM_TOPIC, payload]);
  };

  // Helper to convert chunk to Buffer
  const toBuffer = (chunk: string | Uint8Array): Buffer =>
    !Buffer.isBuffer(chunk) ? Buffer.from(chunk) : chunk;

  return {
    streams: {
      async write(_runId: string | Promise<string>, name: string, chunk: string | Uint8Array) {
        // Await runId if it's a promise to ensure proper flushing
        const runId = await _runId;

        const chunkId = genChunkId();
        await drizzle.insert(streams).values({
          tenantId,
          chunkId,
          streamId: name,
          runId,
          chunkData: compact(toBuffer(chunk)),
          eof: false,
          codecVersion: 1,
          chunkCount: 1,
          lastChunkId: chunkId,
        });
        await notifyStream(
          JSON.stringify(
            StreamPublishMessage.encode({
              chunkId,
              streamId: name,
            }),
          ),
        );
      },

      async writeMulti(
        _runId: string | Promise<string>,
        name: string,
        chunks: (string | Uint8Array)[],
      ) {
        if (chunks.length === 0) return;

        // Generate all chunk IDs up front to preserve ordering
        const chunkIds = chunks.map(() => genChunkId());

        // Await runId if it's a promise to ensure proper flushing
        const runId = await _runId;

        const blocks = packStreamChunks(
          chunks.map((chunk, index) => ({
            id: chunkIds[index]!,
            data: compact(toBuffer(chunk)),
          })),
        );

        // One physical row and one NOTIFY per block, while the reader expands
        // the original logical chunks at the public boundary.
        await drizzle.insert(streams).values(
          blocks.map((block) => ({
            tenantId,
            chunkId: block.firstChunkId,
            streamId: name,
            runId,
            chunkData: block.data,
            eof: false,
            codecVersion: block.codecVersion,
            chunkCount: block.chunkCount,
            lastChunkId: block.lastChunkId,
          })),
        );

        for (const block of blocks) {
          await notifyStream(
            JSON.stringify(
              StreamPublishMessage.encode({
                chunkId: block.firstChunkId,
                streamId: name,
              }),
            ),
          );
        }
      },

      async close(_runId: string | Promise<string>, name: string): Promise<void> {
        // Await runId if it's a promise to ensure proper flushing
        const runId = await _runId;

        const chunkId = genChunkId();
        await drizzle.insert(streams).values({
          tenantId,
          chunkId,
          streamId: name,
          runId,
          chunkData: Buffer.from([]),
          eof: true,
        });
        await notifyStream(
          JSON.stringify(
            StreamPublishMessage.encode({
              streamId: name,
              chunkId,
            }),
          ),
        );
      },

      async getChunks(
        runId: string,
        name: string,
        options?: GetChunksOptions,
      ): Promise<StreamChunksResponse> {
        const limit = options?.limit ?? 100;

        let cursorChunkId: string | null = null;
        let baseIndex = 0;
        if (options?.cursor) {
          try {
            const decoded = JSON.parse(Buffer.from(options.cursor, "base64").toString("utf8"));
            if (typeof decoded.c === "string" && decoded.c.startsWith("chnk_")) {
              cursorChunkId = decoded.c;
            }
            if (Number.isSafeInteger(decoded.i) && decoded.i >= 0) baseIndex = decoded.i;
          } catch {
            // Invalid cursor, start from beginning
          }
        }

        const physicalSelection = {
          chunkId: streams.chunkId,
          lastChunkId: streams.lastChunkId,
          chunkCount: streams.chunkCount,
          codecVersion: streams.codecVersion,
          data: streams.chunkData,
          eof: streams.eof,
        };

        let savedCheckpoint: StoredStreamCheckpoint | undefined;
        if (cursorChunkId) {
          const [row] = await drizzle
            .select({
              chunkId: Schema.streamCheckpoints.chunkId,
              nextIndex: Schema.streamCheckpoints.nextIndex,
              state: Schema.streamCheckpoints.state,
            })
            .from(Schema.streamCheckpoints)
            .where(
              and(
                eq(Schema.streamCheckpoints.tenantId, tenantId),
                eq(Schema.streamCheckpoints.streamId, name),
                lte(Schema.streamCheckpoints.chunkId, cursorChunkId as `chnk_${string}`),
              ),
            )
            .orderBy(desc(Schema.streamCheckpoints.chunkId))
            .limit(1);
          savedCheckpoint = row;
        }

        const progress = createCheckpointingRehydrator({}, savedCheckpoint);
        const checkpoints: StoredStreamCheckpoint[] = [];
        if (cursorChunkId) {
          const checkpointChunkId = savedCheckpoint?.chunkId ?? null;
          const prefixRows = await drizzle
            .select(physicalSelection)
            .from(streams)
            .where(
              and(
                eq(streams.tenantId, tenantId),
                eq(streams.streamId, name),
                eq(streams.eof, false),
                lte(streams.chunkId, cursorChunkId as `chnk_${string}`),
                ...(checkpointChunkId
                  ? [
                      sql`coalesce(${streams.lastChunkId}, ${streams.chunkId}) > ${checkpointChunkId}`,
                    ]
                  : []),
              ),
            )
            .orderBy(asc(streams.chunkId));

          let prefixIndex = savedCheckpoint?.nextIndex ?? 0;
          for (const chunk of prefixRows.flatMap(unpackStreamRow)) {
            if (checkpointChunkId && chunk.id <= checkpointChunkId) continue;
            if (chunk.id > cursorChunkId) continue;
            const fed = progress.feed(chunk.id, chunk.data, prefixIndex);
            prefixIndex += 1;
            if (fed.checkpoint) checkpoints.push(fed.checkpoint);
          }
        }

        // One physical row yields at least one logical chunk, so `limit + 1`
        // rows are sufficient even when the cursor starts inside a block.
        const rows = await drizzle
          .select(physicalSelection)
          .from(streams)
          .where(
            and(
              eq(streams.tenantId, tenantId),
              eq(streams.streamId, name),
              eq(streams.eof, false),
              ...(cursorChunkId
                ? [sql`coalesce(${streams.lastChunkId}, ${streams.chunkId}) > ${cursorChunkId}`]
                : []),
            ),
          )
          .orderBy(asc(streams.chunkId))
          .limit(limit + 1);

        const logicalRows = rows
          .flatMap(unpackStreamRow)
          .filter((chunk) => !cursorChunkId || chunk.id > cursorChunkId);
        const hasMore = logicalRows.length > limit;
        const pageRows = logicalRows.slice(0, limit);
        const chunks = pageRows.map((row, index) => {
          const fed = progress.feed(row.id, row.data, baseIndex + index);
          if (fed.checkpoint) checkpoints.push(fed.checkpoint);
          return { index: baseIndex + index, data: new Uint8Array(fed.data) };
        });

        if (checkpoints.length > 0) {
          await drizzle
            .insert(Schema.streamCheckpoints)
            .values(
              checkpoints.map((checkpoint) => ({
                tenantId,
                streamId: name,
                runId,
                chunkId: checkpoint.chunkId as `chnk_${string}`,
                nextIndex: checkpoint.nextIndex,
                state: checkpoint.state,
              })),
            )
            .onConflictDoNothing();
        }

        const [eofRow] = await drizzle
          .select({ eof: streams.eof })
          .from(streams)
          .where(
            and(
              eq(Schema.streams.tenantId, tenantId),
              and(eq(streams.streamId, name), eq(streams.eof, true)),
            ),
          )
          .limit(1);

        const nextCursor =
          hasMore && pageRows.length > 0
            ? Buffer.from(
                JSON.stringify({
                  c: pageRows.at(-1)!.id,
                  i: baseIndex + pageRows.length,
                }),
              ).toString("base64")
            : null;

        return {
          data: chunks,
          cursor: nextCursor,
          hasMore,
          done: !!eofRow,
        };
      },

      async getInfo(_runId: string, name: string): Promise<StreamInfoResponse> {
        const [countResult] = await drizzle
          .select({
            count: sql<number>`coalesce(sum(coalesce(${streams.chunkCount}, 1)), 0)`,
          })
          .from(streams)
          .where(
            and(
              eq(Schema.streams.tenantId, tenantId),
              and(eq(streams.streamId, name), eq(streams.eof, false)),
            ),
          );

        const dataCount = Number(countResult?.count ?? 0);

        // Check for EOF
        const [eofRow] = await drizzle
          .select({ eof: streams.eof })
          .from(streams)
          .where(
            and(
              eq(Schema.streams.tenantId, tenantId),
              and(eq(streams.streamId, name), eq(streams.eof, true)),
            ),
          )
          .limit(1);

        return {
          tailIndex: dataCount - 1,
          done: !!eofRow,
        };
      },

      async get(
        runId: string,
        name: string,
        startIndex?: number,
      ): Promise<ReadableStream<Uint8Array>> {
        const cleanups: (() => void)[] = [];

        return new ReadableStream<Uint8Array>({
          async start(controller) {
            // an empty string is always < than any string,
            // so `'' < ulid()` and `ulid() < ulid()` (maintaining order)
            let lastChunkId = "";
            let offset = startIndex ?? 0;
            let buffer = [] as StreamChunkEvent[] | null;
            let logicalIndex = 0;
            const progress = createCheckpointingRehydrator();
            const checkpoints: StoredStreamCheckpoint[] = [];

            function enqueue(msg: { id: string; data: Uint8Array; eof: boolean }) {
              if (lastChunkId >= msg.id) {
                // already sent or out of order
                return;
              }

              lastChunkId = msg.id;
              if (msg.eof) {
                controller.close();
                return;
              }

              const fed = progress.feed(
                msg.id,
                Buffer.isBuffer(msg.data) ? msg.data : Buffer.from(msg.data),
                logicalIndex,
              );
              logicalIndex += 1;
              if (fed.checkpoint) checkpoints.push(fed.checkpoint);

              if (offset > 0) {
                offset--;
                return;
              }

              if (fed.data.byteLength) controller.enqueue(new Uint8Array(fed.data));
            }

            function onData(data: StreamChunkEvent) {
              if (buffer) {
                buffer.push(data);
                return;
              }
              enqueue(data);
            }
            events.on(`strm:${name}`, onData);
            cleanups.push(() => {
              events.off(`strm:${name}`, onData);
            });

            const physicalRows = await drizzle
              .select({
                chunkId: streams.chunkId,
                lastChunkId: streams.lastChunkId,
                chunkCount: streams.chunkCount,
                codecVersion: streams.codecVersion,
                eof: streams.eof,
                data: streams.chunkData,
              })
              .from(streams)
              .where(and(eq(Schema.streams.tenantId, tenantId), and(eq(streams.streamId, name))))
              .orderBy(streams.chunkId);

            const chunks = physicalRows.flatMap<StreamChunkEvent>((row) =>
              row.eof
                ? [{ id: row.chunkId, data: row.data, eof: true }]
                : unpackStreamRow(row).map((chunk) => ({
                    id: chunk.id,
                    data: chunk.data,
                    eof: false,
                  })),
            );

            if (typeof offset === "number" && offset < 0) {
              const dataCount = chunks.filter((chunk) => !chunk.eof).length;
              offset = Math.max(0, dataCount + offset);
            }

            for (const chunk of [...chunks, ...(buffer ?? [])]) {
              enqueue(chunk);
            }
            buffer = null;

            if (checkpoints.length > 0) {
              await drizzle
                .insert(Schema.streamCheckpoints)
                .values(
                  checkpoints.map((checkpoint) => ({
                    tenantId,
                    streamId: name,
                    runId,
                    chunkId: checkpoint.chunkId as `chnk_${string}`,
                    nextIndex: checkpoint.nextIndex,
                    state: checkpoint.state,
                  })),
                )
                .onConflictDoNothing();
            }
          },
          cancel() {
            cleanups.forEach((fn) => void fn());
          },
        });
      },

      async list(runId: string): Promise<string[]> {
        // Query distinct stream IDs associated with the runId
        const results = await drizzle
          .selectDistinct({ streamId: streams.streamId })
          .from(streams)
          .where(and(eq(Schema.streams.tenantId, tenantId), eq(streams.runId, runId)));

        return results.map((r) => r.streamId);
      },
    },

    async close() {
      const sub = await listenSubscription.catch(() => undefined);
      if (sub) await sub.close();
    },
  };
}
