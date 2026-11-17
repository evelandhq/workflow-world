import type { AnyEventRequest, CreateEventParams, EventResult, World } from "@workflow/world";
import { Pool } from "pg";
import { monotonicFactory } from "ulid";
import { afterAll, afterEach, beforeAll, describe, expect, test, vi } from "vitest";
import { createWorld } from "./index.js";
import {
  dropTenantPartitions,
  ensureTenantPartitions,
  resolveMigrationsDir,
  runMigrations,
} from "./migrate.js";
import { reconcileWorkflowRuns } from "./reconciliation.js";
import { pruneExpiredWorkflowRuns } from "./retention.js";

/**
 * `$retention: 0`: a run's user data is deleted when it reaches a terminal
 * state (`run-data-purge.ts`). Runs are driven through the real event path and
 * streamer so the purge sees the rows production writes.
 *
 * Set `WORKFLOW_WORLD_TEST_URL` to a scratch database to run it.
 */
const testUrl = process.env.WORKFLOW_WORLD_TEST_URL;
const suffix = `${String(process.pid)}${Date.now().toString(36)}`;
const TENANT = `p_zero_ret_${suffix}`;
const OTHER = `p_zero_ret_other_${suffix}`;
const TENANTS = [TENANT, OTHER];
const ulid = monotonicFactory();

const bytes = (...values: number[]) => new Uint8Array(values);
const text = (value: string) => new TextEncoder().encode(value);

async function collect(stream: ReadableStream<Uint8Array>): Promise<string[]> {
  const seen: string[] = [];
  const reader = stream.getReader();
  for (;;) {
    const { value, done } = await reader.read();
    if (done) return seen;
    seen.push(new TextDecoder().decode(value));
  }
}

/** `events.create` without its per-event-type overloads. */
function emit(
  target: World,
  runId: string,
  data: AnyEventRequest | Record<string, unknown>,
  params?: CreateEventParams,
): Promise<EventResult> {
  const create = target.events.create as (
    runId: string,
    data: unknown,
    params?: CreateEventParams,
  ) => Promise<EventResult>;
  return create(runId, data, params);
}

type TerminalEvent = "run_completed" | "run_failed" | "run_cancelled";

function terminalEvent(eventType: TerminalEvent): AnyEventRequest {
  if (eventType === "run_completed") {
    return { eventType, eventData: { output: bytes(9, 9) } } as AnyEventRequest;
  }
  if (eventType === "run_failed") {
    return {
      eventType,
      eventData: { error: bytes(7, 7), errorCode: "USER_ERROR" },
    } as AnyEventRequest;
  }
  return { eventType } as AnyEventRequest;
}

describe.skipIf(!testUrl)("$retention: 0", () => {
  let pool: Pool;
  let world: World;
  let other: World;

  beforeAll(async () => {
    pool = new Pool({ connectionString: testUrl, max: 4 });
    await runMigrations(pool, { migrationsDir: resolveMigrationsDir() });
    for (const tenant of TENANTS) await ensureTenantPartitions(pool, tenant);
    world = createWorld({ pool, tenantId: TENANT, deploymentId: "dep_zero" });
    other = createWorld({ pool, tenantId: OTHER, deploymentId: "dep_zero" });
  }, 60_000);

  afterEach(() => {
    vi.restoreAllMocks();
  });

  afterAll(async () => {
    await world?.close?.();
    await other?.close?.();
    for (const table of [
      "workflow_event_slots",
      "workflow_steps",
      "workflow_hooks",
      "workflow_waits",
      "workflow_runs",
      "dispatch_dead_letters",
    ]) {
      await pool
        ?.query(`delete from workflow.${table} where tenant_id = any($1)`, [TENANTS])
        .catch(() => {});
    }
    for (const tenant of TENANTS) await dropTenantPartitions(pool, tenant).catch(() => {});
    await pool?.end().catch(() => {});
  });

  /** A started run with a step, a retained hook and a closed two-row stream. */
  async function seedRun(
    target: World,
    attributes: Record<string, string> | undefined,
    options: { runId?: string; stream?: string; closeStream?: boolean } = {},
  ): Promise<{ runId: string; stepId: string; hookId: string; stream: string }> {
    const runId = options.runId ?? `wrun_${ulid()}`;
    await emit(target, runId, {
      eventType: "run_created",
      eventData: {
        deploymentId: "dep_zero",
        workflowName: "zero",
        input: bytes(1, 2, 3),
        ...(attributes ? { attributes, allowReservedAttributes: true } : {}),
      },
    });
    await emit(target, runId, { eventType: "run_started" });
    const stepId = `step_${ulid()}`;
    await emit(target, runId, {
      eventType: "step_created",
      correlationId: stepId,
      eventData: { stepName: "s", input: bytes(4) },
    });
    await emit(target, runId, { eventType: "step_started", correlationId: stepId });
    await emit(target, runId, {
      eventType: "step_completed",
      correlationId: stepId,
      eventData: { result: bytes(5) },
    });
    const hookId = `whk_${ulid()}`;
    await emit(target, runId, {
      eventType: "hook_created",
      correlationId: hookId,
      eventData: {
        token: `tok_${ulid()}`,
        metadata: bytes(6),
        tokenRetentionUntil: new Date(Date.now() + 60_000),
      },
    });
    // A disposed hook: its `hook_disposed` payload is routing, kept by the purge.
    const disposedId = `whk_${ulid()}`;
    const disposedToken = `tok_${ulid()}`;
    await emit(target, runId, {
      eventType: "hook_created",
      correlationId: disposedId,
      eventData: { token: disposedToken, metadata: bytes(8) },
    });
    await emit(target, runId, {
      eventType: "hook_disposed",
      correlationId: disposedId,
      eventData: { token: disposedToken },
    });
    const stream = options.stream ?? `strm_${ulid()}`;
    await target.streams.write(runId, stream, text("one"));
    // A packed (v2) block, which cannot be blanked in place.
    await target.streams.writeMulti!(runId, stream, [text("two"), text("three")]);
    if (options.closeStream !== false) await target.streams.close(runId, stream);
    return { runId, stepId, hookId, stream };
  }

  async function payloads(tenantId: string, runId: string) {
    const run = await pool.query<{
      expired_at: Date | null;
      input: boolean;
      output: boolean;
      error: boolean;
      error_code: string | null;
      attributes: Record<string, string>;
    }>(
      `select expired_at, input_cbor is not null as input, output_cbor is not null as output,
              error_cbor is not null as error, error_code, attributes
         from workflow.workflow_runs where tenant_id = $1 and id = $2`,
      [tenantId, runId],
    );
    const steps = await pool.query<{ n: number; data: number }>(
      `select count(*)::int as n,
              count(*) filter (where input_cbor is not null or output_cbor is not null
                                  or error_cbor is not null)::int as data
         from workflow.workflow_steps where tenant_id = $1 and run_id = $2`,
      [tenantId, runId],
    );
    const events = await pool.query<{ n: number; data: number }>(
      `select count(*)::int as n,
              count(*) filter (where payload_cbor is not null)::int as data
         from workflow.workflow_events where tenant_id = $1 and run_id = $2`,
      [tenantId, runId],
    );
    const hooks = await pool.query<{ n: number; data: number }>(
      `select count(*)::int as n,
              count(*) filter (where metadata_cbor is not null)::int as data
         from workflow.workflow_hooks where tenant_id = $1 and run_id = $2`,
      [tenantId, runId],
    );
    const chunks = await pool.query<{ eof: boolean }>(
      `select eof from workflow.workflow_stream_chunks
        where tenant_id = $1 and run_id = $2 order by id`,
      [tenantId, runId],
    );
    return {
      run: run.rows[0]!,
      steps: steps.rows[0]!,
      events: events.rows[0]!,
      hooks: hooks.rows[0]!,
      chunks: chunks.rows.map((row) => row.eof),
    };
  }

  async function expectPurged(target: World, tenantId: string, runId: string, stream: string) {
    const after = await payloads(tenantId, runId);
    expect(after.run.expired_at).toBeInstanceOf(Date);
    expect(after.run).toMatchObject({ input: false, output: false, error: false });
    expect(after.run.attributes.$retention).toBe("0");
    expect(after.steps).toEqual({ n: 1, data: 0 });
    expect(after.events.n).toBeGreaterThan(5);
    expect(after.events.data).toBe(1);
    expect(after.hooks).toEqual({ n: 1, data: 0 });
    expect(after.chunks).toEqual([true]);

    // Every read still answers, with the data gone.
    const run = await target.runs.get(runId);
    expect(run.expiredAt).toBeInstanceOf(Date);
    expect(run.input).toBeUndefined();
    expect(run.output).toBeUndefined();
    expect((await target.runs.get(runId, { resolveData: "none" })).expiredAt).toBeInstanceOf(Date);
    const listed = await target.runs.list({ pagination: { limit: 100 } });
    expect(listed.data.find((r) => r.runId === runId)?.expiredAt).toBeInstanceOf(Date);

    const events = await target.events.list({ runId, pagination: { limit: 100 } });
    expect(events.data.length).toBe(after.events.n);
    const withData = events.data.filter((e) => "eventData" in e && e.eventData !== undefined);
    expect(withData.map((e) => e.eventType)).toEqual(["hook_disposed"]);
    expect(withData[0]).toMatchObject({ eventData: { token: expect.stringMatching(/^tok_/) } });
    const none = await target.events.list({ runId, resolveData: "none" });
    expect(none.data.length).toBe(after.events.n);
    const first = events.data[0]!;
    expect((await target.events.get(runId, first.eventId)).eventType).toBe("run_created");
    const steps = await target.steps.list({ runId });
    expect(steps.data).toHaveLength(1);
    expect(steps.data[0]!.output).toBeUndefined();
    const step = await target.steps.get(runId, steps.data[0]!.stepId);
    expect(step.input).toBeUndefined();
    const byCorrelation = await target.events.listByCorrelationId({
      runId,
      correlationId: step.stepId,
    });
    expect(byCorrelation.data.map((e) => e.eventType)).toEqual([
      "step_created",
      "step_started",
      "step_completed",
    ]);

    expect(await collect(await target.streams.get(runId, stream))).toEqual([]);
    expect(await target.streams.getInfo!(runId, stream)).toEqual({ tailIndex: -1, done: true });
    expect(await target.streams.getChunks!(runId, stream)).toMatchObject({
      data: [],
      done: true,
    });
    expect(await target.streams.list(runId)).toEqual([stream]);
  }

  test.each(["run_completed", "run_failed", "run_cancelled"] as const)(
    "%s purges the run's user data and keeps its rows",
    async (eventType) => {
      const { runId, stream, hookId } = await seedRun(world, { $retention: "0" });
      const before = await payloads(TENANT, runId);
      expect(before.run).toMatchObject({ expired_at: null, input: true });
      expect(before.chunks).toEqual([false, false, true]);

      const result = await emit(world, runId, terminalEvent(eventType));
      // The winner's response is a pre-purge snapshot.
      expect(result.run?.status).toBe(eventType.replace("run_", ""));
      expect(result.run?.expiredAt).toBeUndefined();

      await expectPurged(world, TENANT, runId, stream);
      // The retained hook keeps its row and token, not its metadata.
      const hook = await world.hooks.get(hookId);
      expect(hook.metadata).toBeUndefined();
      expect(hook.token).toMatch(/^tok_/);
      const status = (await world.runs.get(runId)).status;
      expect(status).toBe(eventType.replace("run_", ""));
      if (eventType === "run_failed") {
        expect((await payloads(TENANT, runId)).run.error_code).toBe("USER_ERROR");
      }
      // A write to the finished run keeps its usual refusal.
      await expect(emit(world, runId, terminalEvent("run_completed"))).rejects.toThrow(
        /terminal state/,
      );
    },
  );

  test("an in-band terminal write purges inside its fenced transaction", async () => {
    const { runId, stream } = await seedRun(world, { $retention: "0" });
    await emit(world, runId, terminalEvent("run_completed"), {
      inBand: true,
      expectedSeqInBand: 1,
    });
    await expectPurged(world, TENANT, runId, stream);
  });

  test.each([
    ["absent", undefined],
    ["default", { $retention: "default" }],
    ["a duration this World cannot scale", { $retention: "60" }],
    ["unrecognized", { $retention: "forever" }],
  ] as const)("%s retention keeps the data", async (_label, attributes) => {
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    const { runId } = await seedRun(world, attributes as Record<string, string> | undefined);
    await emit(world, runId, terminalEvent("run_completed"));

    const after = await payloads(TENANT, runId);
    expect(after.run).toMatchObject({ expired_at: null, input: true, output: true });
    expect(after.steps.data).toBe(1);
    expect(after.hooks.data).toBe(1);
    expect(after.events.data).toBeGreaterThan(0);
    expect(after.chunks).toEqual([false, false, true]);
    const unsupported = attributes !== undefined && attributes.$retention !== "default";
    expect(warn).toHaveBeenCalledTimes(unsupported ? 1 : 0);
  });

  test("an idempotent second run_cancelled does not purge again", async () => {
    const { runId } = await seedRun(world, { $retention: "0" });
    await emit(world, runId, terminalEvent("run_cancelled"));
    const first = (await payloads(TENANT, runId)).run.expired_at;

    // Data written after the purge (a late stream chunk) would only be cleared
    // by a second purge; the idempotent path must not run one.
    await pool.query(
      `update workflow.workflow_runs set input_cbor = '\\x01' where tenant_id = $1 and id = $2`,
      [TENANT, runId],
    );
    const again = await emit(world, runId, terminalEvent("run_cancelled"));
    expect(again.event?.eventType).toBe("run_cancelled");

    const after = await payloads(TENANT, runId);
    expect(after.run.expired_at).toEqual(first);
    expect(after.run.input).toBe(true);
  });

  test("a failed purge is logged and the run still finishes", async () => {
    const error = vi.spyOn(console, "error").mockImplementation(() => {});
    const { runId } = await seedRun(world, { $retention: "0" });
    // Make the purge's first statement fail.
    await pool.query(`
      create or replace function workflow.test_refuse_expiry() returns trigger
      language plpgsql as $$ begin
        if new.expired_at is not null then raise exception 'refused'; end if;
        return new;
      end $$`);
    await pool.query(`
      create trigger test_refuse_expiry before update of expired_at on workflow.workflow_runs
      for each row execute function workflow.test_refuse_expiry()`);
    try {
      const result = await emit(world, runId, terminalEvent("run_completed"));
      expect(result.run?.status).toBe("completed");
    } finally {
      await pool.query("drop trigger test_refuse_expiry on workflow.workflow_runs");
      await pool.query("drop function workflow.test_refuse_expiry()");
    }
    expect(error).toHaveBeenCalledTimes(1);
    const after = await payloads(TENANT, runId);
    expect(after.run).toMatchObject({ expired_at: null, input: true, output: true });
    expect((await world.runs.get(runId)).status).toBe("completed");
  });

  test("another tenant's run with the same id is untouched", async () => {
    const runId = `wrun_${ulid()}`;
    const mine = await seedRun(world, { $retention: "0" }, { runId });
    const theirs = await seedRun(other, { $retention: "0" }, { runId, stream: mine.stream });

    await emit(world, runId, terminalEvent("run_completed"));

    await expectPurged(world, TENANT, runId, mine.stream);
    const untouched = await payloads(OTHER, runId);
    expect(untouched.run).toMatchObject({ expired_at: null, input: true });
    expect(untouched.steps.data).toBe(1);
    expect(untouched.hooks.data).toBe(1);
    expect(untouched.chunks).toEqual([false, false, true]);
    expect(await collect(await other.streams.get(runId, theirs.stream))).toEqual([
      "one",
      "two",
      "three",
    ]);
  });

  test("a session's successor run ends without touching the session stream", async () => {
    // Run A owns the session stream and stays running; B is its handoff
    // successor and writes into A's stream under A's run id.
    const a = await seedRun(world, { $retention: "0" }, { closeStream: false });
    const bId = `wrun_${ulid()}`;
    const b = await seedRun(
      world,
      { $retention: "0", $rootRunId: a.runId, $parentRunId: a.runId },
      { runId: bId },
    );
    await world.streams.write(a.runId, a.stream, text("from-b"));

    await emit(world, bId, terminalEvent("run_completed"));
    await expectPurged(world, TENANT, bId, b.stream);

    const aRows = await payloads(TENANT, a.runId);
    expect(aRows.run).toMatchObject({ expired_at: null, input: true });
    expect(aRows.chunks).toEqual([false, false, false]);
    await world.streams.close(a.runId, a.stream);
    expect(await collect(await world.streams.get(a.runId, a.stream, 0))).toEqual([
      "one",
      "two",
      "three",
      "from-b",
    ]);

    await emit(world, a.runId, terminalEvent("run_completed"));
    await expectPurged(world, TENANT, a.runId, a.stream);
  });

  test.each([
    ["fail", "failed"],
    ["cancel", "cancelled"],
  ] as const)("reconciliation's %s purges a zero-retention run", async (disposition, status) => {
    const zero = await seedRun(world, { $retention: "0" });
    const kept = await seedRun(world, undefined);
    // A dead letter preserves the message body, which can carry run input.
    const body = Buffer.from(JSON.stringify({ runId: zero.runId, runInput: { secret: 1 } }));
    await pool.query(
      `insert into workflow.dispatch_dead_letters
         (tenant_id, deployment_id, run_id, message_id, job_name, queue_name, attempt, reason, payload)
       values ($1, 'dep_zero', $2, 'msg_dl', 'job', 'queue', 3, 'exhausted', $3)`,
      [TENANT, zero.runId, JSON.stringify({ messageId: "msg_dl", data: body.toString("base64") })],
    );

    const result = await reconcileWorkflowRuns(pool, {
      tenantId: TENANT,
      runIds: [zero.runId, kept.runId],
      disposition,
      reason: "executor gone",
    });
    expect(result.reconciled).toHaveLength(2);

    await expectPurged(world, TENANT, zero.runId, zero.stream);
    expect((await world.runs.get(zero.runId)).status).toBe(status);
    const letter = await pool.query<{ payload: { messageId: string; data: string } }>(
      `select payload from workflow.dispatch_dead_letters where tenant_id = $1 and run_id = $2`,
      [TENANT, zero.runId],
    );
    expect(letter.rows[0]!.payload).toEqual({ messageId: "msg_dl", data: "" });

    const untouched = await payloads(TENANT, kept.runId);
    expect(untouched.run).toMatchObject({ expired_at: null, input: true });
    expect(untouched.chunks).toEqual([false, false, true]);
  });

  test("reconciliation's quarantine does not purge", async () => {
    const zero = await seedRun(world, { $retention: "0" });
    await reconcileWorkflowRuns(pool, {
      tenantId: TENANT,
      runIds: [zero.runId],
      disposition: "quarantine",
      reason: "hold",
    });
    const after = await payloads(TENANT, zero.runId);
    expect(after.run).toMatchObject({ expired_at: null, input: true });
    expect(after.chunks).toEqual([false, false, true]);
  });

  test("a purged run is still removed by its retention class", async () => {
    const runId = `wrun_${ulid()}`;
    await emit(world, runId, {
      eventType: "run_created",
      eventData: {
        deploymentId: "dep_zero",
        workflowName: "zero",
        input: bytes(1),
        attributes: { $retention: "0" },
        allowReservedAttributes: true,
      },
    });
    await emit(world, runId, { eventType: "run_started" });
    await world.streams.write(runId, "strm_prune", text("x"));
    await world.streams.close(runId, "strm_prune");
    await emit(world, runId, terminalEvent("run_completed"));
    expect((await payloads(TENANT, runId)).run.expired_at).toBeInstanceOf(Date);

    await pool.query(
      `update workflow.workflow_runs set detail_expire_after = now() - interval '1 minute'
        where tenant_id = $1 and id = $2`,
      [TENANT, runId],
    );
    await pruneExpiredWorkflowRuns(pool, { batchSize: 100, maxBatches: 10 });

    const left = await pool.query(
      `select 1 from workflow.workflow_runs where tenant_id = $1 and id = $2
        union all
       select 1 from workflow.workflow_events where tenant_id = $1 and run_id = $2`,
      [TENANT, runId],
    );
    expect(left.rows).toHaveLength(0);
    expect((await payloads(TENANT, runId)).chunks).toEqual([true]);
  });
});
