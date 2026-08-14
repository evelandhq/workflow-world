import type { Pool } from "pg";

/** A session-level lock keeps independently scheduled sweepers from piling up. */
const STREAM_RETENTION_LOCK_KEY = 0x65_76_72_74; // "evrt"

export type StreamRetentionOptions = {
  /** Age after terminal completion before non-EOF chunks become eligible. */
  retentionMs: number;
  /** Maximum rows deleted by one statement. */
  batchSize: number;
  /** Maximum DELETE statements issued by one invocation. */
  maxBatches: number;
};

export type StreamRetentionResult = {
  deletedRows: number;
  batches: number;
  hitBatchLimit: boolean;
  lockAcquired: boolean;
};

/**
 * Irreversibly expires stream replay data for old terminal runs. The caller
 * owns scheduling and policy; this function only bounds and serializes one
 * database invocation.
 */
export async function pruneTerminalStreamChunks(
  pool: Pool,
  options: StreamRetentionOptions,
): Promise<StreamRetentionResult> {
  assertNonNegativeInteger(options.retentionMs, "retentionMs");
  assertPositiveInteger(options.batchSize, "batchSize");
  assertPositiveInteger(options.maxBatches, "maxBatches");

  const client = await pool.connect();
  let lockAcquired = false;
  try {
    const lockResult = await client.query<{ locked: boolean }>(
      "select pg_try_advisory_lock($1) as locked",
      [STREAM_RETENTION_LOCK_KEY],
    );
    lockAcquired = lockResult.rows[0]?.locked === true;
    if (!lockAcquired) {
      return {
        deletedRows: 0,
        batches: 0,
        hitBatchLimit: false,
        lockAcquired: false,
      };
    }

    let deletedRows = 0;
    for (let batches = 1; batches <= options.maxBatches; batches += 1) {
      const result = await client.query(
        `with victims as (
           select chunks.tableoid, chunks.ctid
             from workflow.workflow_stream_chunks as chunks
             join workflow.workflow_runs as runs
               on runs.tenant_id = chunks.tenant_id
              and runs.id = chunks.run_id
            where chunks.eof = false
              and runs.status in ('completed', 'failed', 'cancelled')
              and coalesce(runs.completed_at, runs.updated_at)
                    < now() - ($1::bigint * interval '1 millisecond')
            order by coalesce(runs.completed_at, runs.updated_at),
                     chunks.tenant_id,
                     chunks.stream_id,
                     chunks.id
            limit $2
         )
         delete from workflow.workflow_stream_chunks as chunks
          using victims
          where chunks.tableoid = victims.tableoid
            and chunks.ctid = victims.ctid
         returning 1`,
        [options.retentionMs, options.batchSize],
      );
      const batchDeletedRows = result.rows.length;
      deletedRows += batchDeletedRows;

      if (batchDeletedRows < options.batchSize) {
        return {
          deletedRows,
          batches,
          hitBatchLimit: false,
          lockAcquired: true,
        };
      }
    }

    return {
      deletedRows,
      batches: options.maxBatches,
      hitBatchLimit: true,
      lockAcquired: true,
    };
  } finally {
    if (lockAcquired) {
      await client
        .query("select pg_advisory_unlock($1)", [STREAM_RETENTION_LOCK_KEY])
        .catch(() => {});
    }
    client.release();
  }
}

function assertNonNegativeInteger(value: number, name: string): void {
  if (!Number.isSafeInteger(value) || value < 0) {
    throw new TypeError(`${name} must be a non-negative safe integer`);
  }
}

function assertPositiveInteger(value: number, name: string): void {
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new TypeError(`${name} must be a positive safe integer`);
  }
}
