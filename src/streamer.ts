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
import { and, asc, eq, gt, lt, lte, sql } from "drizzle-orm";
import { unionAll } from "drizzle-orm/pg-core";
import { Client, type Pool } from "pg";
import { monotonicFactory } from "ulid";
import * as z from "zod";
import { type Drizzle, Schema } from "./drizzle/index.js";
import { packStreamChunks, unpackStreamRow } from "./stream-blocks.js";
import { compactStreamChunk } from "./stream-compaction.js";
import { tenantStreamChannel } from "./tenant.js";

const StreamPublishMessage = z.object({
  streamId: z.string(),
  chunkId: z.templateLiteral(["chnk_", z.string()]),
});

/**
 * Physical rows a reader takes in at a time. This is what a reader nobody reads
 * from holds, however long its stream grows: a page of single-chunk rows is a
 * few kilobytes, and one of full blocks (`DEFAULT_STREAM_BLOCK_BYTES` each) is
 * the ceiling.
 */
const READ_PAGE_ROWS = 128;

/** Rows a reader steps over at a time on its way to a start index; no data is read. */
const SKIP_PAGE_ROWS = 5000;

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
  // supervisor may signal twice — and a second `UNLISTEN` on the ended client
  // would throw "Client was closed and is not queryable".
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
  // A notification carries no data: it wakes the readers of that stream, and
  // each reads on from its own position when its consumer asks for more.
  const events = new EventEmitter<{
    [key: `strm:${string}`]: [];
  }>();
  const { streams } = Schema;
  const genChunkId = () => `chnk_${ulid()}` as const;

  // One channel per tenant. Upstream uses a single global
  // `workflow_event_chunk` topic, which was harmless when each project had
  // its own database; here it would broadcast every chunk of every run to
  // every agent on the platform.
  const STREAM_TOPIC = tenantStreamChannel(tenantId);

  const listenSubscription = listenChannel(pool, STREAM_TOPIC, async (msg) => {
    const parsed = StreamPublishMessage.parse(JSON.parse(msg));
    events.emit(`strm:${parsed.streamId}`);
  });

  const notifyStream = async (payload: string) => {
    await pool.query("SELECT pg_notify($1, $2)", [STREAM_TOPIC, payload]);
  };

  // Helper to convert chunk to Buffer
  const toBuffer = (chunk: string | Uint8Array): Buffer =>
    !Buffer.isBuffer(chunk) ? Buffer.from(chunk) : chunk;

  /**
   * The chunk id of the first EOF row, if one has been written. A producer that
   * retries a terminal write (lost ACK, overlapping attempts) can append data
   * and EOF rows after it; every read path stops at the first EOF so those rows
   * never surface as live data or inflate a count.
   */
  const findFirstEofChunkId = async (name: string): Promise<`chnk_${string}` | null> => {
    const [row] = await drizzle
      .select({ chunkId: streams.chunkId })
      .from(streams)
      .where(and(eq(streams.tenantId, tenantId), eq(streams.streamId, name), eq(streams.eof, true)))
      .orderBy(asc(streams.chunkId))
      .limit(1);
    return row?.chunkId ?? null;
  };

  const readInfo = async (name: string): Promise<StreamInfoResponse> => {
    const firstEofChunkId = await findFirstEofChunkId(name);
    const [countResult] = await drizzle
      .select({
        count: sql<number>`coalesce(sum(coalesce(${streams.chunkCount}, 1)), 0)`,
      })
      .from(streams)
      .where(
        and(
          eq(Schema.streams.tenantId, tenantId),
          and(eq(streams.streamId, name), eq(streams.eof, false)),
          ...(firstEofChunkId ? [lt(streams.chunkId, firstEofChunkId)] : []),
        ),
      );

    const dataCount = Number(countResult?.count ?? 0);

    return {
      tailIndex: dataCount - 1,
      done: firstEofChunkId !== null,
    };
  };

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
        _runId: string,
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

        const firstEofChunkId = await findFirstEofChunkId(name);

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
              ...(firstEofChunkId ? [lt(streams.chunkId, firstEofChunkId)] : []),
              ...(cursorChunkId
                ? [sql`coalesce(${streams.lastChunkId}, ${streams.chunkId}) > ${cursorChunkId}`]
                : []),
            ),
          )
          .orderBy(asc(streams.chunkId))
          .limit(limit + 1);

        // Stored bytes are handed through verbatim: eve writes delta-only
        // appends, so there is no snapshot to rebuild and no accumulator state
        // to carry across pages.
        const logicalRows = rows
          .flatMap(unpackStreamRow)
          .filter((chunk) => !cursorChunkId || chunk.id > cursorChunkId);
        const hasMore = logicalRows.length > limit;
        const pageRows = logicalRows.slice(0, limit);
        const chunks = pageRows.map((row, index) => ({
          index: baseIndex + index,
          data: new Uint8Array(row.data),
        }));

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
          done: firstEofChunkId !== null,
        };
      },

      async getInfo(_runId: string, name: string): Promise<StreamInfoResponse> {
        return readInfo(name);
      },

      async get(
        _runId: string,
        name: string,
        startIndex?: number,
      ): Promise<ReadableStream<Uint8Array>> {
        const key = `strm:${name}` as const;
        // The reader's position: the id of the last logical chunk it has seen.
        let lastChunkId: "" | `chnk_${string}` = "";
        let offset = startIndex ?? 0;
        let closed = false;
        // The last read came up short of a page, so there is nothing more to
        // read until a notification says so.
        let drained = false;
        // A notification that arrives while a page is being read must not be
        // lost: it is checked before the reader waits.
        let notified = false;
        let wake: (() => void) | undefined;

        const onNotify = () => {
          notified = true;
          wake?.();
        };
        // The listener holds this reader and whatever is still queued on it, so
        // it goes as soon as the reader is finished: at the first EOF, on
        // cancel, and on a failed read.
        const stop = () => {
          closed = true;
          events.off(key, onNotify);
          wake?.();
        };

        const head = {
          chunkId: streams.chunkId,
          lastChunkId: streams.lastChunkId,
          chunkCount: streams.chunkCount,
          codecVersion: streams.codecVersion,
          eof: streams.eof,
        };
        const scope = and(eq(streams.tenantId, tenantId), eq(streams.streamId, name));

        /**
         * The rows holding the chunks after the reader's position, in order.
         *
         * Both branches are index lookups, so a read costs the same far into a
         * long stream as at its start. The first is the block the position
         * falls inside, which exists once maintenance has packed the rows the
         * reader was part-way through; the second is every row after.
         */
        const rowsAfter = async (limit: number, withData: boolean) => {
          const columns = {
            ...head,
            data: withData ? sql<Buffer>`${streams.chunkData}` : sql<Buffer>`''::bytea`,
          };
          const position = lastChunkId;
          const following = drizzle
            .select(columns)
            .from(streams)
            .where(and(scope, ...(position ? [gt(streams.chunkId, position)] : [])))
            .orderBy(asc(streams.chunkId))
            .limit(limit);
          if (!position) return following;
          // The one row that can hold the position is the first whose last id
          // is past it. Naming that row first keeps this a point lookup: left
          // to a filter, the scan runs on through every later block when no
          // row holds the position, which is the usual case.
          const candidate = sql`(
            select ${streams.chunkId} from ${streams}
             where ${scope} and ${streams.eof} = false and ${streams.lastChunkId} > ${position}
             order by ${streams.lastChunkId} limit 1)`;
          const holding = drizzle
            .select(columns)
            .from(streams)
            .where(
              and(scope, sql`${streams.chunkId} = ${candidate}`, lte(streams.chunkId, position)),
            );
          const rows = await unionAll(holding, following);
          return rows.sort((a, b) => (a.chunkId < b.chunkId ? -1 : 1));
        };

        return new ReadableStream<Uint8Array>({
          async start() {
            events.on(key, onNotify);
            if (offset >= 0) return;
            try {
              // Counted up to the first EOF only, matching `getInfo`.
              const { tailIndex } = await readInfo(name);
              offset = Math.max(0, tailIndex + 1 + offset);
            } catch (error) {
              stop();
              throw error;
            }
          },
          // Chunks are read when the consumer has room for them, one page at a
          // time. Pushing every notified chunk into the queue instead keeps the
          // rest of the stream in memory for a consumer that is slow or gone.
          async pull(controller) {
            try {
              while (!closed) {
                if (drained && !notified) {
                  await new Promise<void>((resolve) => {
                    wake = resolve;
                  });
                  wake = undefined;
                  continue;
                }
                notified = false;

                // Whole rows before the start index are stepped over by their
                // counts, so resuming far into a stream does not read it all.
                while (offset > 0) {
                  const heads = await rowsAfter(SKIP_PAGE_ROWS, false);
                  if (closed) return;
                  let skipped = 0;
                  for (const row of heads) {
                    const count = row.chunkCount ?? 1;
                    // Left to the read below: it closes at an EOF, and counts
                    // chunk by chunk in a block the position or the start
                    // index falls inside.
                    if (row.eof || row.chunkId <= lastChunkId || count > offset) break;
                    offset -= count;
                    lastChunkId = row.lastChunkId ?? row.chunkId;
                    skipped++;
                  }
                  if (skipped < SKIP_PAGE_ROWS) break;
                }

                const rows = await rowsAfter(READ_PAGE_ROWS, true);
                if (closed) return;
                drained = rows.length < READ_PAGE_ROWS;

                let delivered = false;
                for (const row of rows) {
                  const chunks = row.eof
                    ? [{ id: row.chunkId, data: row.data, eof: true }]
                    : unpackStreamRow(row).map((chunk) => ({ ...chunk, eof: false }));
                  for (const chunk of chunks) {
                    // already sent or out of order
                    if (lastChunkId >= chunk.id) continue;
                    lastChunkId = chunk.id;
                    // Nothing after the first EOF is delivered: a retried
                    // terminal write can append data and EOF rows after it.
                    if (chunk.eof) {
                      stop();
                      controller.close();
                      return;
                    }
                    if (offset > 0) {
                      offset--;
                      continue;
                    }
                    if (chunk.data.byteLength) {
                      controller.enqueue(new Uint8Array(chunk.data));
                      delivered = true;
                    }
                  }
                }
                if (delivered) return;
              }
            } catch (error) {
              stop();
              throw error;
            }
          },
          cancel() {
            stop();
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
