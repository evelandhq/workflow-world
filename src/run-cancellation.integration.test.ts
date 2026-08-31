/**
 * Host-driven run reconciliation (issue #57): `runs.cancelMany` and the
 * cross-tenant `cancelWorkflowRuns` / `listActiveWorkflowRuns` helpers.
 *
 * The scenario behind these paths: an idle-reaped agent, or a Deployment that
 * can never activate again, leaves runs `running` forever. The host settles
 * them — and the settle has to go through the World's own termination path so
 * the event log, the hook/wait cleanup and the terminal guard all hold.
 *
 * Set `EVELAND_WORKFLOW_WORLD_TEST_URL` to a scratch database to run it.
 */
import { and, eq } from "drizzle-orm";
import { Pool } from "pg";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { createClient } from "./drizzle/index.js";
import * as DrizzleSchema from "./drizzle/schema.js";
import { ensureTenantPartitions, runMigrations } from "./index.js";
import { dropTenantPartitions } from "./migrate.js";
import { cancelWorkflowRuns, listActiveWorkflowRuns } from "./reconciliation.js";
import { createRunsCancellation } from "./run-cancellation.js";
import { createEventsStorage } from "./storage.js";

const testUrl = process.env.EVELAND_WORKFLOW_WORLD_TEST_URL;

const suffix = `${String(process.pid)}${Date.now().toString(36)}`;
const TENANT = `p_cancel_a_${suffix}`;
const OTHER = `p_cancel_b_${suffix}`;

type EventsStorage = ReturnType<typeof createEventsStorage>;

async function createRun(
  events: EventsStorage,
  input: { deploymentId?: string; start?: boolean } = {},
): Promise<string> {
  const created = await events.create(null, {
    eventType: "run_created",
    eventData: {
      deploymentId: input.deploymentId ?? "dep_cancel_test",
      workflowName: "cancel-test-workflow",
      input: new Uint8Array(),
    },
  });
  const runId = created.run!.runId;
  if (input.start) {
    await events.create(runId, { eventType: "run_started" });
  }
  return runId;
}

describe.skipIf(!testUrl)("host-driven run cancellation (Postgres integration)", () => {
  let pool: Pool;
  let drizzle: ReturnType<typeof createClient>;
  let events: EventsStorage;
  let cancelMany: ReturnType<typeof createRunsCancellation>;

  beforeAll(async () => {
    pool = new Pool({ connectionString: testUrl, max: 4 });
    await runMigrations(pool);
    await ensureTenantPartitions(pool, TENANT);
    await ensureTenantPartitions(pool, OTHER);
    drizzle = createClient(pool);
    events = createEventsStorage(drizzle, TENANT);
    cancelMany = createRunsCancellation(drizzle, TENANT, events);
  }, 60_000);

  afterAll(async () => {
    // `workflow_runs` is not partitioned, so dropping partitions leaves the
    // rows behind — and active leftovers are exactly what another file's boot
    // sweep would re-enqueue.
    await pool
      .query("delete from workflow.workflow_runs where tenant_id = any($1)", [[TENANT, OTHER]])
      .catch(() => {});
    for (const tenant of [TENANT, OTHER]) {
      await dropTenantPartitions(pool, tenant).catch(() => {});
    }
    await pool?.end().catch(() => {});
  });

  it("cancels a running run through the full termination path", async () => {
    const runId = await createRun(events, { start: true });
    await events.create(runId, {
      eventType: "hook_created",
      correlationId: "hook_cancel_cleanup",
      eventData: { token: `tok-cancel-${suffix}` },
    });

    const result = await cancelMany({
      runIds: [runId],
      cancelReason: "RuntimeInstance ri_test stopped before the run reached a terminal boundary.",
    });

    expect(result.results).toEqual([{ runId, outcome: "cancelled" }]);
    expect(result.summary).toMatchObject({ requested: 1, cancelled: 1 });

    const [run] = await drizzle
      .select()
      .from(DrizzleSchema.runs)
      .where(and(eq(DrizzleSchema.runs.tenantId, TENANT), eq(DrizzleSchema.runs.runId, runId)));
    expect(run?.status).toBe("cancelled");
    expect(run?.completedAt).not.toBeNull();

    // The event log names the actor's reason, so an operator reading the run
    // later can tell a host reconciliation from a user cancellation.
    const cancelEvents = await drizzle
      .select()
      .from(DrizzleSchema.events)
      .where(
        and(
          eq(DrizzleSchema.events.tenantId, TENANT),
          eq(DrizzleSchema.events.runId, runId),
          eq(DrizzleSchema.events.eventType, "run_cancelled"),
        ),
      );
    expect(cancelEvents).toHaveLength(1);
    expect(cancelEvents[0]?.eventData).toMatchObject({
      cancelReason: "RuntimeInstance ri_test stopped before the run reached a terminal boundary.",
    });

    // Hook cleanup is the terminal path's job; a bare status write would leak it.
    const hooks = await drizzle
      .select()
      .from(DrizzleSchema.hooks)
      .where(and(eq(DrizzleSchema.hooks.tenantId, TENANT), eq(DrizzleSchema.hooks.runId, runId)));
    expect(hooks).toHaveLength(0);
  });

  it("reports the outcome matrix per run, order preserved", async () => {
    const pending = await createRun(events);
    const completed = await createRun(events, { start: true });
    await events.create(completed, {
      eventType: "run_completed",
      eventData: { output: new Uint8Array([1]) },
    });
    const cancelled = await createRun(events, { start: true });
    await events.create(cancelled, { eventType: "run_cancelled" });

    const result = await cancelMany({
      runIds: [pending, completed, cancelled, "wrun_missing"],
    });

    expect(result.results).toEqual([
      { runId: pending, outcome: "cancelled" },
      { runId: completed, outcome: "not_cancellable", status: "completed" },
      { runId: cancelled, outcome: "already_cancelled" },
      { runId: "wrun_missing", outcome: "not_found" },
    ]);
    expect(result.summary).toEqual({
      requested: 4,
      cancelled: 1,
      alreadyCancelled: 1,
      notCancellable: 1,
      notFound: 1,
      failed: 0,
    });
  });

  it("a repeat sweep does not grow the event log of a settled run", async () => {
    const runId = await createRun(events, { start: true });
    await cancelMany({ runIds: [runId], cancelReason: "sweep 1" });
    const again = await cancelMany({ runIds: [runId], cancelReason: "sweep 2" });
    expect(again.results).toEqual([{ runId, outcome: "already_cancelled" }]);

    const cancelEvents = await drizzle
      .select()
      .from(DrizzleSchema.events)
      .where(
        and(
          eq(DrizzleSchema.events.tenantId, TENANT),
          eq(DrizzleSchema.events.runId, runId),
          eq(DrizzleSchema.events.eventType, "run_cancelled"),
        ),
      );
    expect(cancelEvents).toHaveLength(1);
  });

  it("refuses an oversized request instead of partially applying it", async () => {
    const runIds = Array.from({ length: 501 }, (_, i) => `wrun_bulk_${String(i)}`);
    await expect(cancelMany({ runIds })).rejects.toThrow();
  });

  it("cancelWorkflowRuns is tenant-scoped: one tenant cannot settle another's run", async () => {
    const otherEvents = createEventsStorage(drizzle, OTHER);
    const foreignRun = await createRun(otherEvents, { start: true });

    const result = await cancelWorkflowRuns(pool, {
      tenantId: TENANT,
      runIds: [foreignRun],
      cancelReason: "cross-tenant attempt",
    });
    expect(result.results).toEqual([{ runId: foreignRun, outcome: "not_found" }]);

    const [run] = await drizzle
      .select()
      .from(DrizzleSchema.runs)
      .where(and(eq(DrizzleSchema.runs.tenantId, OTHER), eq(DrizzleSchema.runs.runId, foreignRun)));
    expect(run?.status).toBe("running");
  });

  it("listActiveWorkflowRuns scopes by tenant and deployment and excludes terminal runs", async () => {
    const onDeploymentA = await createRun(events, { deploymentId: "dep_list_a", start: true });
    const onDeploymentB = await createRun(events, { deploymentId: "dep_list_b" });
    const settled = await createRun(events, { deploymentId: "dep_list_a", start: true });
    await events.create(settled, { eventType: "run_cancelled" });

    const forTenant = await listActiveWorkflowRuns(pool, { tenantId: TENANT });
    const ids = forTenant.map((run) => run.runId);
    expect(ids).toContain(onDeploymentA);
    expect(ids).toContain(onDeploymentB);
    expect(ids).not.toContain(settled);
    for (const run of forTenant) {
      expect(run.tenantId).toBe(TENANT);
    }

    const forDeployment = await listActiveWorkflowRuns(pool, {
      tenantId: TENANT,
      deploymentId: "dep_list_a",
    });
    expect(forDeployment.map((run) => run.runId)).toEqual([onDeploymentA]);
    expect(forDeployment[0]).toMatchObject({
      tenantId: TENANT,
      deploymentId: "dep_list_a",
      status: "running",
    });
  });
});
