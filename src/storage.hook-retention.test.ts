import { Pool } from "pg";
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it } from "vitest";
import { and, eq } from "drizzle-orm";
import { createClient, Schema } from "./drizzle/index.js";
import {
  createWorld,
  dropTenantPartitions,
  ensureTenantPartitions,
  runMigrations,
} from "./index.js";
import { createEventsStorage, createHooksStorage } from "./storage.js";
import { ulid } from "ulid";

/**
 * Hook token retention: `tokenRetentionUntil` keeps a token reserved past the end
 * of the run that created it.
 *
 * The field used to be accepted and silently discarded — there was no column for
 * it — and run termination deleted the hook regardless, so a caller that asked for
 * retention got a token that was immediately reusable. Silently dropping a field
 * someone set is the dangerous half: failing loudly would at least be actionable.
 */
const testUrl = process.env.EVELAND_WORKFLOW_WORLD_TEST_URL;
const TENANT = "prj_hook_retention";

type EventsStorage = ReturnType<typeof createEventsStorage>;

describe.skipIf(!testUrl)("hook token retention", () => {
  let pool: Pool;
  let drizzle: ReturnType<typeof createClient>;
  let events: EventsStorage;
  let hooks: ReturnType<typeof createHooksStorage>;
  const previousLimit = process.env.WORKFLOW_POSTGRES_HOOK_RETENTION_LIMIT_DAYS;

  beforeAll(async () => {
    pool = new Pool({ connectionString: testUrl, max: 2 });
    await runMigrations(pool);
    await ensureTenantPartitions(pool, TENANT);
    drizzle = createClient(pool);
    events = createEventsStorage(drizzle, TENANT);
    hooks = createHooksStorage(drizzle, TENANT);
  }, 60_000);

  beforeEach(async () => {
    for (const table of ["workflow_events", "workflow_steps", "workflow_hooks", "workflow_waits"]) {
      await pool.query(`delete from workflow.${table} where tenant_id = $1`, [TENANT]);
    }
    await pool.query("delete from workflow.workflow_runs where tenant_id = $1", [TENANT]);
  });

  afterEach(() => {
    if (previousLimit === undefined) delete process.env.WORKFLOW_POSTGRES_HOOK_RETENTION_LIMIT_DAYS;
    else process.env.WORKFLOW_POSTGRES_HOOK_RETENTION_LIMIT_DAYS = previousLimit;
  });

  afterAll(async () => {
    await dropTenantPartitions(pool, TENANT).catch(() => {});
    await pool?.end().catch(() => {});
  });

  const runData = {
    deploymentId: "deployment-retention",
    workflowName: "retention-workflow",
    input: new Uint8Array([1]),
  };

  async function startRun(): Promise<string> {
    const runId = `wrun_${ulid()}`;
    await events.create(runId, { eventType: "run_created", eventData: runData });
    await events.create(runId, { eventType: "run_started", eventData: runData });
    return runId;
  }

  async function createHook(
    runId: string,
    token: string,
    tokenRetentionUntil?: Date,
  ): Promise<string> {
    const hookId = `whk_${ulid()}`;
    await events.create(runId, {
      eventType: "hook_created",
      correlationId: hookId,
      eventData: {
        token,
        ...(tokenRetentionUntil ? { tokenRetentionUntil } : {}),
      },
    } as Parameters<EventsStorage["create"]>[1]);
    return hookId;
  }

  async function completeRun(runId: string): Promise<void> {
    await events.create(runId, {
      eventType: "run_completed",
      eventData: { output: new Uint8Array([2]) },
    } as Parameters<EventsStorage["create"]>[1]);
  }

  async function hookRowCount(hookId: string): Promise<number> {
    const rows = await drizzle
      .select({ hookId: Schema.hooks.hookId })
      .from(Schema.hooks)
      .where(and(eq(Schema.hooks.tenantId, TENANT), eq(Schema.hooks.hookId, hookId)));
    return rows.length;
  }

  it("persists the requested retention instead of discarding it", async () => {
    const runId = await startRun();
    const until = new Date(Date.now() + 60_000);
    const hookId = await createHook(runId, `tok_${ulid()}`, until);

    const [row] = await drizzle
      .select({ tokenRetentionUntil: Schema.hooks.tokenRetentionUntil })
      .from(Schema.hooks)
      .where(and(eq(Schema.hooks.tenantId, TENANT), eq(Schema.hooks.hookId, hookId)));

    expect(row?.tokenRetentionUntil).toBeInstanceOf(Date);
    expect(row?.tokenRetentionUntil?.getTime()).toBe(until.getTime());
  });

  it("keeps a retained hook alive when its run completes", async () => {
    const runId = await startRun();
    const hookId = await createHook(runId, `tok_${ulid()}`, new Date(Date.now() + 60_000));

    await completeRun(runId);

    // This is the whole feature: the row outlives the run.
    expect(await hookRowCount(hookId)).toBe(1);
  });

  it("deletes a hook with no retention when its run completes", async () => {
    // The default, and the behaviour retention is an exception to.
    const runId = await startRun();
    const hookId = await createHook(runId, `tok_${ulid()}`);

    await completeRun(runId);

    expect(await hookRowCount(hookId)).toBe(0);
  });

  it("deletes a hook whose retention has already lapsed", async () => {
    const runId = await startRun();
    const hookId = await createHook(runId, `tok_${ulid()}`, new Date(Date.now() + 60_000));
    // Move the retention into the past rather than sleeping through it.
    await drizzle
      .update(Schema.hooks)
      .set({ tokenRetentionUntil: new Date(Date.now() - 1_000) })
      .where(and(eq(Schema.hooks.tenantId, TENANT), eq(Schema.hooks.hookId, hookId)));

    await completeRun(runId);

    expect(await hookRowCount(hookId)).toBe(0);
  });

  it("still resolves a retained token by lookup after the run ended", async () => {
    const runId = await startRun();
    const token = `tok_${ulid()}`;
    const hookId = await createHook(runId, token, new Date(Date.now() + 60_000));
    await completeRun(runId);

    const found = await hooks.getByToken(token);
    expect(found.hookId).toBe(hookId);
  });

  it("refuses a retention beyond the configured limit rather than clamping it", async () => {
    // Clamping silently would leave the caller believing its token was reserved
    // for far longer than it is.
    process.env.WORKFLOW_POSTGRES_HOOK_RETENTION_LIMIT_DAYS = "1";
    const scopedEvents = createEventsStorage(createClient(pool), TENANT);
    const runId = `wrun_${ulid()}`;
    await scopedEvents.create(runId, { eventType: "run_created", eventData: runData });
    await scopedEvents.create(runId, { eventType: "run_started", eventData: runData });

    await expect(
      scopedEvents.create(runId, {
        eventType: "hook_created",
        correlationId: `whk_${ulid()}`,
        eventData: {
          token: `tok_${ulid()}`,
          tokenRetentionUntil: new Date(Date.now() + 3 * 24 * 60 * 60 * 1000),
        },
      } as Parameters<EventsStorage["create"]>[1]),
    ).rejects.toThrow(/cannot exceed 1 days/i);
  });

  it("rejects a non-positive retention limit at construction", async () => {
    process.env.WORKFLOW_POSTGRES_HOOK_RETENTION_LIMIT_DAYS = "0";
    expect(() => createEventsStorage(createClient(pool), TENANT)).toThrow(
      /must be a positive number/i,
    );
  });

  it("declares the capability, so the runtime knows retention is honoured", () => {
    const world = createWorld({
      pool,
      tenantId: TENANT,
      deploymentId: "dep_retention",
      runner: "external",
    });
    // A World that stays silent is treated as not supporting retention.
    expect(world.capabilities?.hookRetention?.active).toBe(true);
  });
});
