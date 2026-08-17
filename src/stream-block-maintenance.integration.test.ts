import { Pool } from "pg";
import { afterAll, beforeAll, describe, expect, test } from "vitest";
import { createWorld } from "./index.js";
import {
  dropTenantPartitions,
  ensureTenantPartitions,
  resolveMigrationsDir,
  runMigrations,
} from "./migrate.js";
import { packTerminalStreamBlocks } from "./stream-block-maintenance.js";

const testUrl = process.env.EVELAND_WORKFLOW_WORLD_TEST_URL;
const suffix = `${String(process.pid)}${Date.now().toString(36)}`;
const TENANT = `p_pack_terminal_${suffix}`;

describe.skipIf(!testUrl)("terminal stream block maintenance", () => {
  let pool: Pool;
  let world: ReturnType<typeof createWorld>;

  beforeAll(async () => {
    pool = new Pool({ connectionString: testUrl, max: 3 });
    await runMigrations(pool, { migrationsDir: resolveMigrationsDir() });
    await ensureTenantPartitions(pool, TENANT);
    world = createWorld({
      pool,
      tenantId: TENANT,
      deploymentId: "dep_pack_terminal",
      runner: "external",
    });
  }, 60_000);

  afterAll(async () => {
    await world?.close?.();
    await pool
      ?.query("delete from workflow.workflow_runs where tenant_id = $1", [TENANT])
      .catch(() => {});
    await dropTenantPartitions(pool, TENANT).catch(() => {});
    await pool?.end().catch(() => {});
  });

  test("rewrites closed terminal single rows into blocks without changing logical reads", async () => {
    const runId = `wrun_pack_${suffix}`;
    const streamId = `strm_pack_${suffix}`;
    const wire = Array.from({ length: 130 }, (_, index) => Buffer.from(`delta-${index}`));
    await insertRun(runId, "completed");
    for (const chunk of wire) await world.streams.write(runId, streamId, chunk);
    await world.streams.close(runId, streamId);

    await expect(packTerminalStreamBlocks(pool, { maxStreams: 10 })).resolves.toMatchObject({
      streamsCompacted: 1,
      logicalChunks: 130,
      physicalRowsBefore: 130,
      physicalRowsAfter: 3,
      lockAcquired: true,
      hitStreamLimit: false,
    });

    const rows = await pool.query<{ eof: boolean; codec_version: number; chunk_count: number }>(
      `select eof, codec_version, chunk_count
         from workflow.workflow_stream_chunks
        where tenant_id = $1 and stream_id = $2
        order by id`,
      [TENANT, streamId],
    );
    expect(rows.rows.filter((row) => !row.eof).map((row) => row.chunk_count)).toEqual([64, 64, 2]);
    expect(rows.rows.find((row) => row.eof)?.codec_version).toBe(2);

    const read = await world.streams.getChunks(runId, streamId, { limit: 200 });
    expect(read.data.map(({ data }) => Buffer.from(data))).toEqual(wire);
  });

  test("does not compact active or persistent runs", async () => {
    const runId = `wrun_active_${suffix}`;
    const streamId = `strm_active_${suffix}`;
    const persistentRunId = `wrun_persistent_${suffix}`;
    const persistentStreamId = `strm_persistent_${suffix}`;
    await insertRun(runId, "running");
    await world.streams.write(runId, streamId, "active");
    await world.streams.close(runId, streamId);
    await pool.query(
      `insert into workflow.workflow_runs
         (tenant_id, id, deployment_id, status, name, spec_version,
          retention_class, completed_at)
       values ($1, $2, 'dep_pack_terminal', 'completed', 'pack-test', 6,
               'persistent', now() - interval '1 day')`,
      [TENANT, persistentRunId],
    );
    await world.streams.write(persistentRunId, persistentStreamId, "persistent");
    await world.streams.close(persistentRunId, persistentStreamId);

    await packTerminalStreamBlocks(pool, { maxStreams: 10 });
    const rows = await pool.query<{ stream_id: string; codec_version: number }>(
      `select codec_version from workflow.workflow_stream_chunks
        where tenant_id = $1 and stream_id in ($2, $3) and eof = false
        order by stream_id`,
      [TENANT, streamId, persistentStreamId],
    );
    expect(rows.rows.map((row) => row.codec_version)).toEqual([1, 1]);
  });

  test("terminal rewriting also strips legacy cumulative snapshots", async () => {
    const runId = `wrun_legacy_fat_${suffix}`;
    const streamId = `strm_legacy_fat_${suffix}`;
    await insertRun(runId, "completed");
    let soFar = "";
    const wire = ["legacy ", "snapshot ", "bytes"].map((delta, index) => {
      soFar += delta;
      return encodeMessage(delta, soFar, index);
    });
    for (const [index, data] of wire.entries()) {
      await pool.query(
        `insert into workflow.workflow_stream_chunks
           (tenant_id, id, stream_id, run_id, data, eof)
         values ($1, $2, $3, $4, $5, false)`,
        [TENANT, `chnk_legacy_${String(index).padStart(4, "0")}`, streamId, runId, data],
      );
    }
    await pool.query(
      `insert into workflow.workflow_stream_chunks
         (tenant_id, id, stream_id, run_id, data, eof)
       values ($1, 'chnk_legacy_eof', $2, $3, ''::bytea, true)`,
      [TENANT, streamId, runId],
    );

    await packTerminalStreamBlocks(pool, { maxStreams: 10 });
    const stored = await pool.query<{ data: Buffer }>(
      `select data from workflow.workflow_stream_chunks
        where tenant_id = $1 and stream_id = $2 and eof = false`,
      [TENANT, streamId],
    );
    expect(stored.rows.reduce((total, row) => total + row.data.length, 0)).toBeLessThan(
      wire.reduce((total, row) => total + row.length, 0),
    );
    const read = await world.streams.getChunks(runId, streamId, { limit: 10 });
    expect(read.data.map(({ data }) => Buffer.from(data))).toEqual(wire);
  });

  test("includes a chunk committed between candidate selection and rewriting", async () => {
    const runId = `wrun_race_${suffix}`;
    const streamId = `strm_race_${suffix}`;
    await insertCompletedRunAt(runId, "10 minutes");
    await pool.query(
      `insert into workflow.workflow_stream_chunks
         (tenant_id, id, stream_id, run_id, data, eof)
       values ($1, 'chnk_race_0001', $2, $3, 'first'::bytea, false),
              ($1, 'chnk_race_eof', $2, $3, ''::bytea, true)`,
      [TENANT, streamId, runId],
    );

    const actualClient = await pool.connect();
    let injected = false;
    const injectLateChunk = async () => {
      if (injected) return;
      injected = true;
      await pool.query(
        `insert into workflow.workflow_stream_chunks
           (tenant_id, id, stream_id, run_id, data, eof)
         values ($1, 'chnk_race_0002', $2, $3, 'second'::bytea, false)`,
        [TENANT, streamId, runId],
      );
    };
    const racingPool = {
      connect: async () => ({
        query: async (query: string, values?: unknown[]) => {
          if (query.includes("select id, last_chunk_id")) {
            const result = await actualClient.query(query, values);
            await injectLateChunk();
            return result;
          }
          if (
            query.includes("delete from workflow.workflow_stream_chunks") &&
            query.includes("returning id")
          ) {
            await injectLateChunk();
          }
          return actualClient.query(query, values);
        },
        release: () => actualClient.release(),
      }),
    } as unknown as Pool;

    await expect(packTerminalStreamBlocks(racingPool, { maxStreams: 1 })).resolves.toMatchObject({
      streamsCompacted: 1,
      logicalChunks: 2,
    });
    const read = await world.streams.getChunks(runId, streamId, { limit: 10 });
    expect(read.data.map(({ data }) => Buffer.from(data).toString())).toEqual(["first", "second"]);
  });

  test("packs the oldest eligible stream first when a pass is bounded", async () => {
    const newerRunId = `wrun_fair_newer_${suffix}`;
    const olderRunId = `wrun_fair_older_${suffix}`;
    const newerStreamId = `strm_fair_a_${suffix}`;
    const olderStreamId = `strm_fair_z_${suffix}`;
    await insertCompletedRunAt(newerRunId, "10 minutes");
    await insertCompletedRunAt(olderRunId, "20 minutes");
    await world.streams.write(newerRunId, newerStreamId, "newer");
    await world.streams.close(newerRunId, newerStreamId);
    await world.streams.write(olderRunId, olderStreamId, "older");
    await world.streams.close(olderRunId, olderStreamId);

    await expect(packTerminalStreamBlocks(pool, { maxStreams: 1 })).resolves.toMatchObject({
      streamsCompacted: 1,
      hitStreamLimit: true,
    });

    const codecs = await pool.query<{ stream_id: string; codec_version: number | null }>(
      `select stream_id, codec_version
         from workflow.workflow_stream_chunks
        where tenant_id = $1 and stream_id in ($2, $3) and eof = true
        order by stream_id`,
      [TENANT, newerStreamId, olderStreamId],
    );
    expect(codecs.rows).toEqual([
      { stream_id: newerStreamId, codec_version: null },
      { stream_id: olderStreamId, codec_version: 2 },
    ]);
  });

  async function insertRun(runId: string, status: "running" | "completed") {
    await pool.query(
      `insert into workflow.workflow_runs
         (tenant_id, id, deployment_id, status, name, spec_version, completed_at)
       values ($1, $2, 'dep_pack_terminal', $3::workflow.status, 'pack-test', 6,
               case when $3 = 'completed' then now() - interval '10 minutes' end)`,
      [TENANT, runId, status],
    );
  }

  async function insertCompletedRunAt(runId: string, age: string) {
    await pool.query(
      `insert into workflow.workflow_runs
         (tenant_id, id, deployment_id, status, name, spec_version, completed_at)
       values ($1, $2, 'dep_pack_terminal', 'completed', 'pack-test', 6,
               now() - $3::interval)`,
      [TENANT, runId, age],
    );
  }
});

function encodeMessage(delta: string, soFar: string, index: number): Buffer {
  const inner = Buffer.from(
    `${JSON.stringify({
      data: {
        messageDelta: delta,
        messageSoFar: soFar,
        sequence: 0,
        stepIndex: 0,
        turnId: "turn_0",
      },
      type: "message.appended",
      meta: { id: `evt_${index}` },
    })}\n`,
  );
  const payload = Buffer.from(JSON.stringify([["Uint8Array", 1], inner.toString("base64")]));
  const frame = Buffer.alloc(8 + payload.length);
  frame.writeUInt32BE(payload.length + 4, 0);
  frame.write("devl", 4, "latin1");
  payload.copy(frame, 8);
  return frame;
}
