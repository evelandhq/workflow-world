import type { Pool, PoolClient } from "pg";
import {
  DEFAULT_STREAM_BLOCK_BYTES,
  DEFAULT_STREAM_BLOCK_CHUNKS,
  packStreamChunks,
  unpackStreamRow,
} from "./stream-blocks.js";
import { compactStreamChunk } from "./stream-compaction.js";

const STREAM_BLOCK_PACK_LOCK_KEY = 0x65_76_62_70; // "evbp"

export type PackTerminalStreamBlocksOptions = {
  /** Maximum completed streams rewritten by one invocation. */
  maxStreams: number;
  maxChunksPerBlock?: number;
  maxBlockBytes?: number;
  /** Strip cumulative Eve snapshots while rewriting (default true). */
  compactSnapshots?: boolean;
};

export type PackTerminalStreamBlocksResult = {
  streamsCompacted: number;
  logicalChunks: number;
  physicalRowsBefore: number;
  physicalRowsAfter: number;
  hitStreamLimit: boolean;
  lockAcquired: boolean;
};

type Candidate = {
  tenant_id: string;
  stream_id: string;
  run_id: string | null;
};

/**
 * Rewrite closed streams of terminal runs into v2 blocks. Logical chunk ids
 * stay in the block payload, so public cursors survive the physical rewrite.
 */
export async function packTerminalStreamBlocks(
  pool: Pool,
  options: PackTerminalStreamBlocksOptions,
): Promise<PackTerminalStreamBlocksResult> {
  assertPositiveInteger(options.maxStreams, "maxStreams");
  const maxChunks = options.maxChunksPerBlock ?? DEFAULT_STREAM_BLOCK_CHUNKS;
  const maxBytes = options.maxBlockBytes ?? DEFAULT_STREAM_BLOCK_BYTES;
  assertPositiveInteger(maxChunks, "maxChunksPerBlock");
  assertPositiveInteger(maxBytes, "maxBlockBytes");

  const client = await pool.connect();
  let lockAcquired = false;
  try {
    const lock = await client.query<{ locked: boolean }>(
      "select pg_try_advisory_lock($1) as locked",
      [STREAM_BLOCK_PACK_LOCK_KEY],
    );
    lockAcquired = lock.rows[0]?.locked === true;
    if (!lockAcquired) return emptyResult(false);

    const candidates = await client.query<Candidate>(
      `select candidates.tenant_id, candidates.stream_id, candidates.run_id
         from (
           select distinct on (eof.tenant_id, eof.stream_id)
                  eof.tenant_id, eof.stream_id, eof.run_id, runs.compact_after
             from workflow.workflow_stream_chunks as eof
             join workflow.workflow_runs as runs
               on runs.tenant_id = eof.tenant_id
              and runs.id = eof.run_id
            where eof.eof = true
              and eof.codec_version is distinct from 2
              and runs.status in ('completed', 'failed', 'cancelled')
              and runs.compact_after <= now()
            order by eof.tenant_id, eof.stream_id, eof.id
         ) as candidates
        order by candidates.compact_after, candidates.tenant_id, candidates.stream_id
        limit $1`,
      [options.maxStreams],
    );

    const result = emptyResult(true);
    for (const candidate of candidates.rows) {
      const compacted = await compactOneStream(
        client,
        candidate,
        maxChunks,
        maxBytes,
        options.compactSnapshots ?? true,
      );
      result.streamsCompacted += 1;
      result.logicalChunks += compacted.logicalChunks;
      result.physicalRowsBefore += compacted.physicalRowsBefore;
      result.physicalRowsAfter += compacted.physicalRowsAfter;
    }
    result.hitStreamLimit = candidates.rows.length === options.maxStreams;
    return result;
  } finally {
    if (lockAcquired) {
      await client
        .query("select pg_advisory_unlock($1)", [STREAM_BLOCK_PACK_LOCK_KEY])
        .catch(() => {});
    }
    client.release();
  }
}

async function compactOneStream(
  client: PoolClient,
  candidate: Candidate,
  maxChunks: number,
  maxBytes: number,
  compactSnapshots: boolean,
): Promise<{
  logicalChunks: number;
  physicalRowsBefore: number;
  physicalRowsAfter: number;
}> {
  await client.query("begin");
  try {
    const rows = await client.query<{
      id: `chnk_${string}`;
      last_chunk_id: `chnk_${string}` | null;
      chunk_count: number | null;
      codec_version: number | null;
      data: Buffer;
      eof: boolean;
    }>(
      `delete from workflow.workflow_stream_chunks
        where tenant_id = $1 and stream_id = $2 and eof = false
        returning id, last_chunk_id, chunk_count, codec_version, data, eof`,
      [candidate.tenant_id, candidate.stream_id],
    );
    const logical = rows.rows
      .sort((left, right) => left.id.localeCompare(right.id))
      .flatMap((row) =>
        unpackStreamRow({
          chunkId: row.id,
          lastChunkId: row.last_chunk_id,
          chunkCount: row.chunk_count,
          codecVersion: row.codec_version,
          data: row.data,
          eof: row.eof,
        }),
      )
      .map((chunk) => ({
        ...chunk,
        data: compactSnapshots ? compactStreamChunk(chunk.data) : chunk.data,
      }));
    const blocks = packStreamChunks(logical, { maxChunks, maxBytes });
    for (const block of blocks) {
      await client.query(
        `insert into workflow.workflow_stream_chunks
           (tenant_id, id, stream_id, run_id, data, eof,
            codec_version, chunk_count, last_chunk_id)
         values ($1, $2, $3, $4, $5, false, $6, $7, $8)`,
        [
          candidate.tenant_id,
          block.firstChunkId,
          candidate.stream_id,
          candidate.run_id,
          block.data,
          block.codecVersion,
          block.chunkCount,
          block.lastChunkId,
        ],
      );
    }
    await client.query(
      `update workflow.workflow_stream_chunks
          set codec_version = 2,
              chunk_count = 0,
              last_chunk_id = $3
        where tenant_id = $1 and stream_id = $2 and eof = true`,
      [candidate.tenant_id, candidate.stream_id, logical.at(-1)?.id ?? null],
    );
    await client.query("commit");
    return {
      logicalChunks: logical.length,
      physicalRowsBefore: rows.rows.length,
      physicalRowsAfter: blocks.length,
    };
  } catch (error) {
    await client.query("rollback").catch(() => {});
    throw error;
  }
}

function emptyResult(lockAcquired: boolean): PackTerminalStreamBlocksResult {
  return {
    streamsCompacted: 0,
    logicalChunks: 0,
    physicalRowsBefore: 0,
    physicalRowsAfter: 0,
    hitStreamLimit: false,
    lockAcquired,
  };
}

function assertPositiveInteger(value: number, name: string): void {
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new TypeError(`${name} must be a positive safe integer`);
  }
}
