import { Pool } from "pg";
import { afterAll, beforeAll, beforeEach, describe, expect, test } from "vitest";
import { createWorld } from "./index.js";
import { pruneTerminalStreamChunks } from "./retention.js";
import {
  dropTenantPartitions,
  ensureTenantPartitions,
  resolveMigrationsDir,
  runMigrations,
} from "./migrate.js";

const testUrl = process.env.EVELAND_WORKFLOW_WORLD_TEST_URL;
const suffix = `${String(process.pid)}${Date.now().toString(36)}`;
const ALPHA = `p_retention_alpha_${suffix}`;
const BETA = `p_retention_beta_${suffix}`;
const STREAM_RETENTION_LOCK_KEY = 0x65_76_72_74; // "evrt"

describe.skipIf(!testUrl)("terminal stream retention", () => {
  let pool: Pool;

  beforeAll(async () => {
    pool = new Pool({ connectionString: testUrl, max: 4 });
    await runMigrations(pool, { migrationsDir: resolveMigrationsDir() });
    await ensureTenantPartitions(pool, ALPHA);
    await ensureTenantPartitions(pool, BETA);
  }, 60_000);

  beforeEach(async () => {
    await pool.query("delete from workflow.workflow_stream_chunks where tenant_id = any($1)", [
      [ALPHA, BETA],
    ]);
    await pool.query("delete from workflow.workflow_runs where tenant_id = any($1)", [
      [ALPHA, BETA],
    ]);
  });

  afterAll(async () => {
    await dropTenantPartitions(pool, ALPHA).catch(() => {});
    await dropTenantPartitions(pool, BETA).catch(() => {});
    await pool
      ?.query("delete from workflow.workflow_runs where tenant_id = any($1)", [[ALPHA, BETA]])
      .catch(() => {});
    await pool?.end().catch(() => {});
  });

  test("deletes only non-EOF chunks belonging to old terminal runs", async () => {
    await insertRun(ALPHA, "same_run", "completed", "2 days");
    await insertRun(ALPHA, "failed_run", "failed", "2 days");
    await insertRun(ALPHA, "cancelled_run", "cancelled", "2 days");
    await insertRun(ALPHA, "recent_run", "completed", "1 hour");
    await insertRun(ALPHA, "active_run", "running", "2 days");
    // The same run id in a different tenant must join to this active row, not
    // ALPHA's terminal row.
    await insertRun(BETA, "same_run", "running", "2 days");

    await insertChunk(ALPHA, "same_run", "alpha-old", false);
    await insertChunk(ALPHA, "same_run", "alpha-eof", true);
    await insertChunk(ALPHA, "failed_run", "alpha-failed", false);
    await insertChunk(ALPHA, "cancelled_run", "alpha-cancelled", false);
    await insertChunk(ALPHA, "recent_run", "alpha-recent", false);
    await insertChunk(ALPHA, "active_run", "alpha-active", false);
    await insertChunk(BETA, "same_run", "beta-active", false);
    await insertChunk(ALPHA, null, "alpha-unowned", false);

    const eligible = await pool.query(
      `select 1
         from workflow.workflow_stream_chunks as chunks
         join workflow.workflow_runs as runs
           on runs.tenant_id = chunks.tenant_id and runs.id = chunks.run_id
        where chunks.eof = false
          and runs.status in ('completed', 'failed', 'cancelled')
          and coalesce(runs.completed_at, runs.updated_at) < now() - interval '1 day'`,
    );
    expect(eligible.rowCount).toBe(3);

    const result = await pruneTerminalStreamChunks(pool, {
      retentionMs: 24 * 60 * 60 * 1_000,
      batchSize: 100,
      maxBatches: 10,
    });

    expect(result).toEqual({
      deletedRows: 3,
      batches: 1,
      hitBatchLimit: false,
      lockAcquired: true,
    });
    expect(await remainingChunkIds()).toEqual([
      "alpha-active",
      "alpha-eof",
      "alpha-recent",
      "alpha-unowned",
      "beta-active",
    ]);

    const alphaWorld = createWorld({
      pool,
      tenantId: ALPHA,
      deploymentId: "dep_retention",
      runner: "external",
    });
    try {
      const expiredStream = await alphaWorld.streams.getChunks("same_run", "stream-same_run");
      expect(expiredStream.data).toEqual([]);
      expect(expiredStream.done).toBe(true);
    } finally {
      await alphaWorld.close?.();
    }
  });

  test("bounds each invocation by batch size and maximum batches", async () => {
    await insertRun(ALPHA, "bounded_run", "completed", "2 days");
    for (let index = 0; index < 5; index += 1) {
      await insertChunk(ALPHA, "bounded_run", `bounded-${String(index)}`, false);
    }

    await expect(
      pruneTerminalStreamChunks(pool, {
        retentionMs: 24 * 60 * 60 * 1_000,
        batchSize: 2,
        maxBatches: 2,
      }),
    ).resolves.toEqual({
      deletedRows: 4,
      batches: 2,
      hitBatchLimit: true,
      lockAcquired: true,
    });

    await expect(
      pruneTerminalStreamChunks(pool, {
        retentionMs: 24 * 60 * 60 * 1_000,
        batchSize: 2,
        maxBatches: 2,
      }),
    ).resolves.toEqual({
      deletedRows: 1,
      batches: 1,
      hitBatchLimit: false,
      lockAcquired: true,
    });
  });

  test("returns immediately when another session owns the pruning lock", async () => {
    const lockOwner = await pool.connect();
    try {
      await lockOwner.query("select pg_advisory_lock($1)", [STREAM_RETENTION_LOCK_KEY]);

      await expect(
        pruneTerminalStreamChunks(pool, {
          retentionMs: 24 * 60 * 60 * 1_000,
          batchSize: 10,
          maxBatches: 1,
        }),
      ).resolves.toEqual({
        deletedRows: 0,
        batches: 0,
        hitBatchLimit: false,
        lockAcquired: false,
      });
    } finally {
      await lockOwner.query("select pg_advisory_unlock($1)", [STREAM_RETENTION_LOCK_KEY]);
      lockOwner.release();
    }
  });

  test("migration installs the terminal-run retention index", async () => {
    const { rows } = await pool.query<{ indexdef: string }>(
      `select indexdef
         from pg_indexes
        where schemaname = 'workflow'
          and indexname = 'workflow_runs_terminal_retention_index'`,
    );

    expect(rows).toHaveLength(1);
    expect(rows[0]?.indexdef).toContain("COALESCE(completed_at, updated_at)");
    expect(rows[0]?.indexdef).toContain("status = ANY");
  });

  async function insertRun(
    tenantId: string,
    runId: string,
    status: "pending" | "running" | "completed" | "failed" | "cancelled",
    age: string,
  ): Promise<void> {
    await pool.query(
      `insert into workflow.workflow_runs
         (tenant_id, id, deployment_id, status, name, spec_version,
          created_at, updated_at, completed_at)
       values ($1, $2, 'dep_retention', $3::workflow.status, 'retention-test', 5,
               now() - $4::interval, now() - $4::interval,
               case when $3::workflow.status in ('completed', 'failed', 'cancelled')
                    then now() - $4::interval end)`,
      [tenantId, runId, status, age],
    );
  }

  async function insertChunk(
    tenantId: string,
    runId: string | null,
    chunkId: string,
    eof: boolean,
  ): Promise<void> {
    const streamId = `stream-${runId ?? "unowned"}`;
    await pool.query(
      `insert into workflow.workflow_stream_chunks
         (tenant_id, id, stream_id, run_id, data, created_at, eof)
       values ($1, $2, $3, $4, $5, now() - interval '2 days', $6)`,
      [tenantId, chunkId, streamId, runId, Buffer.from(chunkId), eof],
    );
  }

  async function remainingChunkIds(): Promise<string[]> {
    const { rows } = await pool.query<{ id: string }>(
      `select id
         from workflow.workflow_stream_chunks
        where tenant_id = any($1)
        order by id`,
      [[ALPHA, BETA]],
    );
    return rows.map(({ id }) => id);
  }
});
