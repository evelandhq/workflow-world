import { Pool } from "pg";
import { afterAll, beforeAll, beforeEach, describe, expect, test } from "vitest";
import {
  dropTenantPartitions,
  ensureTenantPartitions,
  resolveMigrationsDir,
  runMigrations,
} from "./migrate.js";
import {
  pruneExpiredStreamChunks,
  pruneExpiredWorkflowRuns,
  setWorkflowRunRetentionClass,
} from "./retention.js";

const testUrl = process.env.EVELAND_WORKFLOW_WORLD_TEST_URL;
const suffix = `${String(process.pid)}${Date.now().toString(36)}`;
const TENANT = `p_deadline_retention_${suffix}`;

describe.skipIf(!testUrl)("deadline-driven retention", () => {
  let pool: Pool;

  beforeAll(async () => {
    pool = new Pool({ connectionString: testUrl, max: 3 });
    await runMigrations(pool, { migrationsDir: resolveMigrationsDir() });
    await ensureTenantPartitions(pool, TENANT);
  }, 60_000);

  beforeEach(async () => {
    await clearTenant();
  });

  afterAll(async () => {
    await clearTenant().catch(() => {});
    await dropTenantPartitions(pool, TENANT).catch(() => {});
    await pool?.end().catch(() => {});
  });

  test("scheduled expiry deletes data and checkpoints while preserving EOF", async () => {
    await insertRun("old", "scheduled", "completed", "20 minutes");
    await insertRun("recent", "scheduled", "completed", "5 minutes");
    await insertRun("persistent", "persistent", "completed", "2 days");
    await insertRun("active", "scheduled", "running", "2 days");
    for (const runId of ["old", "recent", "persistent", "active"]) {
      await insertChunk(runId, false);
      await insertChunk(runId, true);
      await insertCheckpoint(runId);
    }

    await expect(
      pruneExpiredStreamChunks(pool, { batchSize: 100, maxBatches: 2 }),
    ).resolves.toEqual({
      deletedRows: 1,
      batches: 1,
      hitBatchLimit: false,
      lockAcquired: true,
    });

    expect(await chunkKinds("old")).toEqual([true]);
    expect(await checkpointCount("old")).toBe(0);
    for (const runId of ["recent", "persistent", "active"]) {
      expect(await chunkKinds(runId)).toEqual([false, true]);
      expect(await checkpointCount(runId)).toBe(1);
    }
  });

  test("reclassifying a terminal run recomputes its deadlines", async () => {
    await insertRun("reclassify", "interactive", "completed", "2 hours");
    await insertChunk("reclassify", false);

    await expect(
      setWorkflowRunRetentionClass(pool, {
        tenantId: TENANT,
        runId: "reclassify",
        retentionClass: "ephemeral",
      }),
    ).resolves.toEqual({ updated: true, retentionClass: "scheduled" });

    const row = await pool.query<{ retention_class: string; expired: boolean }>(
      `select retention_class, expire_after <= now() as expired
         from workflow.workflow_runs where tenant_id = $1 and id = 'reclassify'`,
      [TENANT],
    );
    expect(row.rows[0]).toEqual({ retention_class: "scheduled", expired: true });
    expect(await pruneExpiredStreamChunks(pool, { batchSize: 10, maxBatches: 1 })).toMatchObject({
      deletedRows: 1,
    });
  });

  test("detail expiry removes the workflow graph while preserving EOF", async () => {
    await insertRun("details", "scheduled", "completed", "8 days");
    await insertRun("keep", "interactive", "running", "40 days");
    await insertGraph("details");
    await insertGraph("keep");

    await expect(pruneExpiredWorkflowRuns(pool, { batchSize: 10, maxBatches: 2 })).resolves.toEqual(
      {
        deletedRuns: 1,
        batches: 1,
        hitBatchLimit: false,
        lockAcquired: true,
      },
    );

    for (const table of [
      "workflow_stream_checkpoints",
      "workflow_events",
      "workflow_event_slots",
      "workflow_steps",
      "workflow_hooks",
      "workflow_waits",
    ]) {
      expect(await graphRowCount(table, "details")).toBe(0);
      expect(await graphRowCount(table, "keep")).toBeGreaterThan(0);
    }
    expect(await chunkKinds("details")).toEqual([true]);
    expect(await chunkKinds("keep")).toEqual([false, true]);
    expect(await runCount("details")).toBe(0);
    expect(await runCount("keep")).toBe(1);
  });

  test("detail expiry honors a hook token's longer reservation", async () => {
    await insertRun("retained-hook", "scheduled", "completed", "8 days");
    await insertGraph("retained-hook");
    await pool.query(
      `update workflow.workflow_hooks
          set token_retention_until = now() + interval '1 hour'
        where tenant_id = $1 and run_id = 'retained-hook'`,
      [TENANT],
    );

    await pruneExpiredWorkflowRuns(pool, { batchSize: 10, maxBatches: 1 });
    expect(await graphRowCount("workflow_hooks", "retained-hook")).toBe(1);
    expect(await runCount("retained-hook")).toBe(0);

    await pool.query(
      `update workflow.workflow_hooks
          set token_retention_until = now() - interval '1 second'
        where tenant_id = $1 and run_id = 'retained-hook'`,
      [TENANT],
    );
    await pruneExpiredWorkflowRuns(pool, { batchSize: 10, maxBatches: 1 });
    expect(await graphRowCount("workflow_hooks", "retained-hook")).toBe(0);
  });

  async function insertRun(
    runId: string,
    retentionClass: "scheduled" | "interactive" | "persistent",
    status: "running" | "completed",
    age: string,
  ) {
    await pool.query(
      `insert into workflow.workflow_runs
         (tenant_id, id, deployment_id, status, name, spec_version,
          retention_class, created_at, updated_at, completed_at)
       values ($1, $2, 'dep_retention', $3::workflow.status, 'retention-test', 6,
               $4, now() - $5::interval, now() - $5::interval,
               case when $3 = 'completed' then now() - $5::interval end)`,
      [TENANT, runId, status, retentionClass, age],
    );
  }

  async function insertChunk(runId: string, eof: boolean) {
    await pool.query(
      `insert into workflow.workflow_stream_chunks
         (tenant_id, id, stream_id, run_id, data, eof)
       values ($1, $2, $3, $4, $5, $6)`,
      [
        TENANT,
        `chnk_${runId}_${eof ? "eof" : "data"}`,
        `stream_${runId}`,
        runId,
        Buffer.from(runId),
        eof,
      ],
    );
  }

  async function insertCheckpoint(runId: string) {
    await pool.query(
      `insert into workflow.workflow_stream_checkpoints
         (tenant_id, stream_id, run_id, chunk_id, next_index, state)
       values ($1, $2, $3, $4, 1, '{"version":1,"accumulators":[]}'::jsonb)`,
      [TENANT, `stream_${runId}`, runId, `chnk_${runId}_data`],
    );
  }

  async function insertGraph(runId: string) {
    await insertChunk(runId, false);
    await insertChunk(runId, true);
    await insertCheckpoint(runId);
    await Promise.all([
      pool.query("insert into workflow.workflow_event_slots (tenant_id, run_id) values ($1, $2)", [
        TENANT,
        runId,
      ]),
      pool.query(
        `insert into workflow.workflow_events (tenant_id, run_id, id, type, spec_version)
         values ($1, $2, 'evnt_000000000001', 'run_created', 6)`,
        [TENANT, runId],
      ),
      pool.query(
        `insert into workflow.workflow_steps
           (tenant_id, run_id, step_id, step_name, status, attempt, spec_version)
         values ($1, $2, $3, 'step', 'pending', 0, 6)`,
        [TENANT, runId, `step_${runId}`],
      ),
      pool.query(
        `insert into workflow.workflow_hooks
           (tenant_id, run_id, hook_id, token, owner_id, project_id, environment, spec_version)
         values ($1, $2, $3, $4, 'owner', '', '', 6)`,
        [TENANT, runId, `hook_${runId}`, `token_${runId}`],
      ),
      pool.query(
        `insert into workflow.workflow_waits
           (tenant_id, run_id, wait_id, status, spec_version)
         values ($1, $2, $3, 'waiting', 6)`,
        [TENANT, runId, `wait_${runId}`],
      ),
    ]);
  }

  async function chunkKinds(runId: string): Promise<boolean[]> {
    const { rows } = await pool.query<{ eof: boolean }>(
      `select eof from workflow.workflow_stream_chunks
        where tenant_id = $1 and run_id = $2 order by eof`,
      [TENANT, runId],
    );
    return rows.map((row) => row.eof);
  }

  async function checkpointCount(runId: string) {
    return graphRowCount("workflow_stream_checkpoints", runId);
  }

  async function graphRowCount(table: string, runId: string): Promise<number> {
    const result = await pool.query(
      `select 1 from workflow.${table} where tenant_id = $1 and run_id = $2`,
      [TENANT, runId],
    );
    return result.rows.length;
  }

  async function runCount(runId: string): Promise<number> {
    const result = await pool.query(
      "select 1 from workflow.workflow_runs where tenant_id = $1 and id = $2",
      [TENANT, runId],
    );
    return result.rows.length;
  }

  async function clearTenant() {
    for (const table of [
      "workflow_stream_checkpoints",
      "workflow_stream_chunks",
      "workflow_events",
      "workflow_event_slots",
      "workflow_steps",
      "workflow_hooks",
      "workflow_waits",
      "workflow_runs",
    ]) {
      await pool.query(`delete from workflow.${table} where tenant_id = $1`, [TENANT]);
    }
  }
});
