import { SPEC_VERSION_CURRENT } from "@workflow/world";
import { Pool } from "pg";
import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";
import { createClient } from "./drizzle/index.js";
import { dropTenantPartitions, ensureTenantPartitions, runMigrations } from "./index.js";
import { Schema } from "./drizzle/index.js";
import { and, eq } from "drizzle-orm";
import { createEventsStorage } from "./storage.js";
import { ulid } from "ulid";

/**
 * `hook_received` must not be appended to a run that has already finished.
 *
 * It is the one event type with no branch in the terminal-run guard — it neither
 * transitions the run nor creates an entity — so it used to fall straight through
 * to the generic event INSERT and land on a completed, failed or cancelled run.
 *
 * Two paths need the guard, and the legacy one is the more surprising of the two:
 * legacy runs (specVersion <= 1) are routed to `handleLegacyEventPostgres` BEFORE
 * the caller's terminal-run validation block runs, so nothing else was checking
 * them at all. Upstream's own suite does not cover that path.
 */
const testUrl = process.env.EVELAND_WORKFLOW_WORLD_TEST_URL;
const TENANT = "prj_hook_terminal";

type EventsStorage = ReturnType<typeof createEventsStorage>;

describe.skipIf(!testUrl)("hook_received against a terminal run", () => {
  let pool: Pool;
  let events: EventsStorage;
  let drizzle: ReturnType<typeof createClient>;

  beforeAll(async () => {
    pool = new Pool({ connectionString: testUrl, max: 2 });
    await runMigrations(pool);
    await ensureTenantPartitions(pool, TENANT);
    drizzle = createClient(pool);
    events = createEventsStorage(drizzle, TENANT);
  }, 60_000);

  beforeEach(async () => {
    // Tenant-scoped, never TRUNCATE: the parents are partitioned and shared.
    for (const table of ["workflow_events", "workflow_steps", "workflow_hooks", "workflow_waits"]) {
      await pool.query(`delete from workflow.${table} where tenant_id = $1`, [TENANT]);
    }
    await pool.query("delete from workflow.workflow_runs where tenant_id = $1", [TENANT]);
  });

  afterAll(async () => {
    await dropTenantPartitions(pool, TENANT).catch(() => {});
    await pool?.end().catch(() => {});
  });

  async function createRunInStatus(
    status: "completed" | "failed" | "cancelled",
    whileRunning?: (runId: string) => Promise<void>,
  ): Promise<string> {
    const runId = `wrun_${ulid()}`;
    await events.create(runId, {
      eventType: "run_created",
      eventData: {
        deploymentId: "deployment-hook",
        workflowName: "hook-workflow",
        input: new Uint8Array([1]),
      },
    });
    await events.create(runId, {
      eventType: "run_started",
      eventData: {
        deploymentId: "deployment-hook",
        workflowName: "hook-workflow",
        input: new Uint8Array([1]),
      },
    });
    await whileRunning?.(runId);
    const terminal =
      status === "completed"
        ? { eventType: "run_completed" as const, eventData: { output: new Uint8Array([2]) } }
        : status === "failed"
          ? { eventType: "run_failed" as const, eventData: { error: { message: "nope" } } }
          : { eventType: "run_cancelled" as const, eventData: {} };
    await events.create(runId, terminal as Parameters<EventsStorage["create"]>[1]);
    return runId;
  }

  /** Creates the hook entity the way a running workflow does. */
  async function createHook(runId: string, hookId: string, token: string): Promise<void> {
    await events.create(runId, {
      eventType: "hook_created",
      correlationId: hookId,
      eventData: { token },
    } as Parameters<EventsStorage["create"]>[1]);
  }

  /**
   * Puts the hook row back after the run went terminal.
   *
   * Run termination deletes every hook for the run, so a *sequential*
   * `hook_received` afterwards fails the hook-existence check before the terminal
   * guard is ever reached. The state this reconstructs is the one the guard
   * actually exists for: the hook lookup succeeded because the terminating
   * transaction had not committed its delete yet, and the append is about to land
   * on a run that is now finished. Reconstructing it directly is deterministic
   * where racing the two writers is not.
   */
  async function restoreHookRow(runId: string, hookId: string, token: string): Promise<void> {
    await drizzle.insert(Schema.hooks).values({
      tenantId: TENANT,
      hookId,
      runId,
      token,
      ownerId: "",
      // Upstream's column, not tenancy; world-postgres writes the empty string.
      projectId: "",
      environment: "",
    });
  }

  function hookReceived(runId: string, hookId: string) {
    return events.create(runId, {
      eventType: "hook_received",
      correlationId: hookId,
      eventData: { payload: new Uint8Array([9]) },
    } as Parameters<EventsStorage["create"]>[1]);
  }

  for (const status of ["completed", "failed", "cancelled"] as const) {
    it(`rejects hook_received on a ${status} run`, async () => {
      const hookId = `whk_${ulid()}`;
      const token = `tok_${ulid()}`;
      const runId = await createRunInStatus(status, async (id) => {
        await createHook(id, hookId, token);
      });
      await restoreHookRow(runId, hookId, token);

      await expect(hookReceived(runId, hookId)).rejects.toThrow(/terminal state/i);

      // And nothing was written: the guard has to prevent the append, not merely
      // report it. A recorded hook_received would be replayed.
      const listed = await events.list({ runId, pagination: { sortOrder: "asc" } });
      expect(listed.data.filter((event) => event.eventType === "hook_received")).toHaveLength(0);
    });
  }

  it("still accepts hook_received while the run is running", async () => {
    // Guards against over-tightening: the point is to reject terminal runs, not
    // to reject hooks.
    const runId = `wrun_${ulid()}`;
    const runData = {
      deploymentId: "deployment-hook",
      workflowName: "hook-workflow",
      input: new Uint8Array([1]),
    };
    await events.create(runId, { eventType: "run_created", eventData: runData });
    await events.create(runId, { eventType: "run_started", eventData: runData });
    const hookId = `whk_${ulid()}`;
    await createHook(runId, hookId, `tok_${ulid()}`);

    await expect(hookReceived(runId, hookId)).resolves.toBeDefined();

    const listed = await events.list({ runId, pagination: { sortOrder: "asc" } });
    expect(listed.data.filter((event) => event.eventType === "hook_received")).toHaveLength(1);
  });

  /**
   * The legacy path, which nothing else covers.
   *
   * A run whose `specVersion` is <= 1 is routed to the legacy handler before the
   * terminal-run validation block is reached, so the guard inside that handler is
   * the only thing standing between a cancelled legacy run and an accepted hook.
   */
  it("rejects hook_received on a terminal LEGACY run, which bypasses the main guard", async () => {
    const runId = await createRunInStatus("cancelled");

    // Demote the run to a legacy spec version. Writing it directly is the point:
    // the current-spec path cannot produce one, and this is exactly the shape a
    // run created by an older eve has on disk.
    await drizzle
      .update(Schema.runs)
      .set({ specVersion: 1 })
      .where(and(eq(Schema.runs.tenantId, TENANT), eq(Schema.runs.runId, runId)));

    await expect(hookReceived(runId, `whk_${ulid()}`)).rejects.toThrow(/terminal state/i);

    const listed = await events.list({ runId, pagination: { sortOrder: "asc" } });
    expect(listed.data.filter((event) => event.eventType === "hook_received")).toHaveLength(0);
  });

  it("accepts hook_received on a running LEGACY run", async () => {
    const runId = `wrun_${ulid()}`;
    const runData = {
      deploymentId: "deployment-hook",
      workflowName: "hook-workflow",
      input: new Uint8Array([1]),
    };
    await events.create(runId, { eventType: "run_created", eventData: runData });
    await events.create(runId, { eventType: "run_started", eventData: runData });
    const hookId = `whk_${ulid()}`;
    await createHook(runId, hookId, `tok_${ulid()}`);
    await drizzle
      .update(Schema.runs)
      .set({ specVersion: 1 })
      .where(and(eq(Schema.runs.tenantId, TENANT), eq(Schema.runs.runId, runId)));

    await expect(hookReceived(runId, hookId)).resolves.toBeDefined();
  });

  it("reports a missing run rather than silently accepting the hook", async () => {
    // The guard reads the run row under lock; no row means there is nothing to
    // attach the hook to.
    await expect(hookReceived(`wrun_${ulid()}`, `whk_${ulid()}`)).rejects.toThrow();
  });

  it("keeps the spec version the run was created with", () => {
    // Cheap tripwire: if the current spec version moves past the legacy threshold
    // the demotion above stops exercising the legacy path, and these tests would
    // start passing for the wrong reason.
    expect(SPEC_VERSION_CURRENT).toBeGreaterThan(1);
  });
});
