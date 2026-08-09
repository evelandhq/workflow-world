import type { Event, Hook, HookCreatedEventRequest, Step, WorkflowRun } from "@workflow/world";
import { Pool } from "pg";
import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";
import { createClient } from "./drizzle/index.js";
import { dropTenantPartitions, ensureTenantPartitions, runMigrations } from "./index.js";
import { createEventsStorage } from "./storage.js";

/**
 * Upstream `@workflow/world-postgres` covers this storage layer with a suite the
 * fork inherited none of. This file is its `describe('events')` group (upstream
 * test/storage.test.ts lines 1088-1678), re-pointed at the tenant-scoped
 * factory: the assertions are upstream's, only the wiring is ours (a tenant
 * threaded into the factory, and this repo's migrations instead of a
 * testcontainer plus `db:push`).
 *
 * All 16 upstream tests in the group are present; none were dropped.
 *
 * Set `EVELAND_WORKFLOW_WORLD_TEST_URL` to a scratch database to run it.
 */
const testUrl = process.env.EVELAND_WORKFLOW_WORLD_TEST_URL;

/**
 * One tenant per ported file. Files share a database and only *files* are
 * serialized, so a tenant shared with another file would leak rows into the
 * assertions below — `list` counts every event a run owns.
 */
const TENANT = "prj_port_events";

type EventsStorage = ReturnType<typeof createEventsStorage>;

/**
 * `hook_conflict`'s `conflictingRunId` is not declared on the `Event` union in
 * the pinned @workflow/world 5.0.0-beta.25 (upstream reaches it through `any`).
 * One narrowing helper keeps the cast out of the assertions.
 */
function conflictData(event: Event | undefined): { token?: string; conflictingRunId?: string } {
  return (
    (event as { eventData?: { token?: string; conflictingRunId?: string } } | undefined)
      ?.eventData ?? {}
  );
}

/** Entities are only reachable through the event log; there is no direct create. */
async function createRun(
  events: EventsStorage,
  data: {
    deploymentId: string;
    workflowName: string;
    input: Uint8Array;
  },
): Promise<WorkflowRun> {
  const result = await events.create(null, {
    eventType: "run_created",
    eventData: data,
  });
  if (!result.run) {
    throw new Error("Expected run to be created");
  }
  return result.run;
}

async function createStep(
  events: EventsStorage,
  runId: string,
  data: { stepId: string; stepName: string; input: Uint8Array },
): Promise<Step> {
  const result = await events.create(runId, {
    eventType: "step_created",
    correlationId: data.stepId,
    eventData: { stepName: data.stepName, input: data.input },
  });
  if (!result.step) {
    throw new Error("Expected step to be created");
  }
  return result.step;
}

async function createHook(
  events: EventsStorage,
  runId: string,
  data: HookCreatedEventRequest["eventData"] & { hookId: string },
): Promise<Hook> {
  const { hookId, ...eventData } = data;
  const result = await events.create(runId, {
    eventType: "hook_created",
    correlationId: hookId,
    eventData,
  });
  if (!result.hook) {
    throw new Error("Expected hook to be created");
  }
  return result.hook;
}

describe.skipIf(!testUrl)("events storage (postgres)", () => {
  let pool: Pool;
  let events: EventsStorage;
  let testRunId: string;

  /**
   * Upstream truncates the tables outright. Here the delete is tenant-scoped:
   * truncating the partitioned parents would wipe every other tenant in the
   * database, including whatever a concurrently-running suite provisioned.
   *
   * The reset is load-bearing, not hygiene. Every `list` assertion below counts
   * exact rows, and the tests reuse fixed correlation ids (`corr_123`,
   * `step-abc123`, `hook_test123`). Those are step and hook ids, and
   * `workflow_steps` / `workflow_hooks` are keyed `(tenant_id, step_id)` /
   * `(tenant_id, hook_id)` — unique per *tenant*, not per run — so without the
   * reset a second run of this file would collide with the previous run's rows
   * on creation. `listByCorrelationId` itself is run-scoped since
   * `@workflow/world` 5.0.0-beta.25 made `runId` a required param, so it is the
   * creation path, not the read path, that needs the clean slate.
   */
  async function deleteTenantRows() {
    for (const table of ["workflow_events", "workflow_steps", "workflow_hooks", "workflow_waits"]) {
      await pool.query(`delete from workflow.${table} where tenant_id = $1`, [TENANT]);
    }
    await pool.query("delete from workflow.workflow_runs where tenant_id = $1", [TENANT]);
  }

  beforeAll(async () => {
    pool = new Pool({ connectionString: testUrl, max: 1 });
    await runMigrations(pool);
    // No DEFAULT partition exists, so an unprovisioned tenant cannot write at
    // all: this call is a hard prerequisite, not a convenience.
    await ensureTenantPartitions(pool, TENANT);
    const drizzle = createClient(pool);
    events = createEventsStorage(drizzle, TENANT);
  }, 60_000);

  beforeEach(async () => {
    await deleteTenantRows();
    const run = await createRun(events, {
      deploymentId: "deployment-123",
      workflowName: "test-workflow",
      input: new Uint8Array(),
    });
    testRunId = run.runId;
  });

  afterAll(async () => {
    await deleteTenantRows().catch(() => {});
    await dropTenantPartitions(pool, TENANT).catch(() => {});
    await pool?.end().catch(() => {});
  });

  describe("create", () => {
    it("should create a new event", async () => {
      // Create step before step_started event
      await createStep(events, testRunId, {
        stepId: "corr_123",
        stepName: "test-step",
        input: new Uint8Array(),
      });

      const result = await events.create(testRunId, {
        eventType: "step_started",
        correlationId: "corr_123",
      });

      expect(result.event?.runId).toBe(testRunId);
      expect(result.event?.eventId).toMatch(/^wevt_/);
      expect(result.event?.eventType).toBe("step_started");
      expect(result.event?.correlationId).toBe("corr_123");
      expect(result.event?.createdAt).toBeInstanceOf(Date);
    });

    it("should create a new event with null byte in payload", async () => {
      // Create step before step_failed event
      await createStep(events, testRunId, {
        stepId: "corr_123_null",
        stepName: "test-step-null",
        input: new Uint8Array(),
      });
      await events.create(testRunId, {
        eventType: "step_started",
        correlationId: "corr_123_null",
      });

      // A literal NUL is illegal in a Postgres `text` value, so this only works
      // because the payload round-trips through a bytea/CBOR column. It is the
      // regression test for that encoding choice.
      const result = await events.create(testRunId, {
        eventType: "step_failed",
        correlationId: "corr_123_null",
        eventData: { error: "Error with null byte \u0000 in message" },
      });

      expect(result.event?.runId).toBe(testRunId);
      expect(result.event?.eventId).toMatch(/^wevt_/);
      expect(result.event?.eventType).toBe("step_failed");
      expect(result.event?.correlationId).toBe("corr_123_null");
      expect(result.event?.createdAt).toBeInstanceOf(Date);
    });

    it("should handle run completed events", async () => {
      const result = await events.create(testRunId, {
        eventType: "run_completed",
        eventData: { output: new Uint8Array([1]) },
      });

      expect(result.event?.eventType).toBe("run_completed");
      expect(result.event?.correlationId).toBeUndefined();
    });
  });

  describe("list", () => {
    it("should list all events for a run", async () => {
      const result1 = await events.create(testRunId, {
        eventType: "run_started",
      });

      // Small delay to ensure different timestamps in event IDs
      await new Promise((resolve) => setTimeout(resolve, 2));

      // Create step before step_started event
      await createStep(events, testRunId, {
        stepId: "corr-step-1",
        stepName: "test-step",
        input: new Uint8Array(),
      });

      const result2 = await events.create(testRunId, {
        eventType: "step_started",
        correlationId: "corr-step-1",
      });

      const result = await events.list({
        runId: testRunId,
        pagination: { sortOrder: "asc" }, // Explicitly request ascending order
      });

      // 4 events: run_created (from createRun), run_started, step_created, step_started
      expect(result.data).toHaveLength(4);
      // Should be in chronological order (oldest first)
      expect(result.data[0]?.eventType).toBe("run_created");
      expect(result.data[1]?.eventId).toBe(result1.event?.eventId);
      expect(result.data[3]?.eventId).toBe(result2.event?.eventId);
      expect(result.data[3]?.createdAt.getTime()).toBeGreaterThanOrEqual(
        result.data[1]!.createdAt.getTime(),
      );
    });

    it("should list events in descending order when explicitly requested (newest first)", async () => {
      const result1 = await events.create(testRunId, {
        eventType: "run_started",
      });

      // Small delay to ensure different timestamps in event IDs
      await new Promise((resolve) => setTimeout(resolve, 2));

      // Create step before step_started event
      await createStep(events, testRunId, {
        stepId: "corr-step-1",
        stepName: "test-step",
        input: new Uint8Array(),
      });

      const result2 = await events.create(testRunId, {
        eventType: "step_started",
        correlationId: "corr-step-1",
      });

      const result = await events.list({
        runId: testRunId,
        pagination: { sortOrder: "desc" },
      });

      // 4 events: run_created (from createRun), run_started, step_created, step_started
      expect(result.data).toHaveLength(4);
      // Should be in reverse chronological order (newest first)
      expect(result.data[0]?.eventId).toBe(result2.event?.eventId);
      expect(result.data[1]?.eventType).toBe("step_created");
      expect(result.data[2]?.eventId).toBe(result1.event?.eventId);
      expect(result.data[3]?.eventType).toBe("run_created");
      expect(result.data[0]?.createdAt.getTime()).toBeGreaterThanOrEqual(
        result.data[2]!.createdAt.getTime(),
      );
    });

    it("should support pagination", async () => {
      // Create multiple events - must create steps first
      for (let i = 0; i < 5; i++) {
        await createStep(events, testRunId, {
          stepId: `corr_${String(i)}`,
          stepName: `test-step-${String(i)}`,
          input: new Uint8Array(),
        });
        // Start the step before completing
        await events.create(testRunId, {
          eventType: "step_started",
          correlationId: `corr_${String(i)}`,
        });
        await events.create(testRunId, {
          eventType: "step_completed",
          correlationId: `corr_${String(i)}`,
          eventData: { result: new Uint8Array([i]) },
        });
      }

      const page1 = await events.list({
        runId: testRunId,
        pagination: { limit: 2 },
      });

      expect(page1.data).toHaveLength(2);
      expect(page1.cursor).not.toBeNull();

      const page2 = await events.list({
        runId: testRunId,
        pagination: { limit: 2, cursor: page1.cursor || undefined },
      });

      expect(page2.data).toHaveLength(2);
      expect(page2.data[0]?.eventId).not.toBe(page1.data[0]?.eventId);
    });
  });

  describe("listByCorrelationId", () => {
    it("should list all events with a specific correlation ID", async () => {
      const correlationId = "step-abc123";

      // Create step before step events
      await createStep(events, testRunId, {
        stepId: correlationId,
        stepName: "test-step",
        input: new Uint8Array(),
      });

      // Create events with the target correlation ID
      const result1 = await events.create(testRunId, {
        eventType: "step_started",
        correlationId,
      });

      await new Promise((resolve) => setTimeout(resolve, 2));

      const result2 = await events.create(testRunId, {
        eventType: "step_completed",
        correlationId,
        eventData: { result: new Uint8Array([1]) },
      });

      // Create events with different correlation IDs (should be filtered out)
      await createStep(events, testRunId, {
        stepId: "different-step",
        stepName: "different-step",
        input: new Uint8Array(),
      });
      await events.create(testRunId, {
        eventType: "step_started",
        correlationId: "different-step",
      });
      await events.create(testRunId, {
        eventType: "run_completed",
        eventData: { output: new Uint8Array([1]) },
      });

      const result = await events.listByCorrelationId({
        runId: testRunId,
        correlationId,
        pagination: {},
      });

      // 3 events: step_created, step_started, step_completed
      expect(result.data).toHaveLength(3);
      expect(result.data[0]?.eventType).toBe("step_created");
      expect(result.data[1]?.eventId).toBe(result1.event?.eventId);
      expect(result.data[1]?.correlationId).toBe(correlationId);
      expect(result.data[2]?.eventId).toBe(result2.event?.eventId);
      expect(result.data[2]?.correlationId).toBe(correlationId);
    });

    /**
     * One correlationId, events owned by two runs — and each run sees only its
     * own since `@workflow/world` 5.0.0-beta.25 made `runId` required here.
     *
     * Upstream's version of this asserted the opposite: that all three events
     * came back from one call. That was the only reachable shape when the
     * predicate was `(tenant, correlation)`, and it is what the required param
     * deliberately ends. The cross-run write itself still happens, so the
     * coverage is kept and split across the two runs rather than deleted.
     */
    it("scopes to the requested run when one correlation id spans runs", async () => {
      const correlationId = "hook-xyz789";

      // Create another run
      const run2 = await createRun(events, {
        deploymentId: "deployment-456",
        workflowName: "test-workflow-2",
        input: new Uint8Array(),
      });

      // Create events in both runs with same correlation ID
      const result1 = await events.create(testRunId, {
        eventType: "hook_created",
        correlationId,
        eventData: { token: "test-token-1" },
      });

      await new Promise((resolve) => setTimeout(resolve, 2));

      // The hook-existence lookup ahead of hook_received is keyed on
      // (tenantId, hookId) with no run in the predicate, which is exactly why a
      // hook created under `testRunId` can be received under `run2`.
      const result2 = await events.create(run2.runId, {
        eventType: "hook_received",
        correlationId,
        eventData: { payload: new Uint8Array([1, 2, 3]) },
      });

      await new Promise((resolve) => setTimeout(resolve, 2));

      const result3 = await events.create(testRunId, {
        eventType: "hook_disposed",
        correlationId,
      });

      const owning = await events.listByCorrelationId({
        runId: testRunId,
        correlationId,
        pagination: {},
      });

      // The creating run sees its own two events, not the receive under run2.
      expect(owning.data).toHaveLength(2);
      expect(owning.data[0]?.eventId).toBe(result1.event?.eventId);
      expect(owning.data[0]?.runId).toBe(testRunId);
      expect(owning.data[1]?.eventId).toBe(result3.event?.eventId);
      expect(owning.data[1]?.runId).toBe(testRunId);

      // And the receiving run sees exactly the event it owns.
      const receiving = await events.listByCorrelationId({
        runId: run2.runId,
        correlationId,
        pagination: {},
      });

      expect(receiving.data).toHaveLength(1);
      expect(receiving.data[0]?.eventId).toBe(result2.event?.eventId);
      expect(receiving.data[0]?.runId).toBe(run2.runId);
    });

    it("should return empty list for non-existent correlation ID", async () => {
      // Create a step and start it
      await createStep(events, testRunId, {
        stepId: "existing-step",
        stepName: "existing-step",
        input: new Uint8Array(),
      });
      await events.create(testRunId, {
        eventType: "step_started",
        correlationId: "existing-step",
      });

      const result = await events.listByCorrelationId({
        runId: testRunId,
        correlationId: "non-existent-correlation-id",
        pagination: {},
      });

      expect(result.data).toHaveLength(0);
      expect(result.hasMore).toBe(false);
      expect(result.cursor).toBeNull();
    });

    it("should respect pagination parameters", async () => {
      const correlationId = "step_paginated";

      // Create step first
      await createStep(events, testRunId, {
        stepId: correlationId,
        stepName: "test-step",
        input: new Uint8Array(),
      });

      // Create multiple events
      await events.create(testRunId, {
        eventType: "step_started",
        correlationId,
      });

      await new Promise((resolve) => setTimeout(resolve, 2));

      await events.create(testRunId, {
        eventType: "step_retrying",
        correlationId,
        eventData: { error: "retry error" },
      });

      await new Promise((resolve) => setTimeout(resolve, 2));

      // Start again after retry
      await events.create(testRunId, {
        eventType: "step_started",
        correlationId,
      });

      await new Promise((resolve) => setTimeout(resolve, 2));

      await events.create(testRunId, {
        eventType: "step_completed",
        correlationId,
        eventData: { result: new Uint8Array([1]) },
      });

      // Get first page (step_created, step_started, step_retrying)
      const page1 = await events.listByCorrelationId({
        runId: testRunId,
        correlationId,
        pagination: { limit: 3 },
      });

      expect(page1.data).toHaveLength(3);
      expect(page1.hasMore).toBe(true);
      expect(page1.cursor).toBeDefined();

      // Get second page (step_started, step_completed)
      const page2 = await events.listByCorrelationId({
        runId: testRunId,
        correlationId,
        pagination: { limit: 3, cursor: page1.cursor || undefined },
      });

      expect(page2.data).toHaveLength(2);
      expect(page2.hasMore).toBe(false);
    });

    it("should always return full event data", async () => {
      // Create step first
      await createStep(events, testRunId, {
        stepId: "step-with-data",
        stepName: "step-with-data",
        input: new Uint8Array(),
      });
      // Start the step before completing
      await events.create(testRunId, {
        eventType: "step_started",
        correlationId: "step-with-data",
      });
      await events.create(testRunId, {
        eventType: "step_completed",
        correlationId: "step-with-data",
        eventData: { result: new Uint8Array([1]) },
      });

      // Note: resolveData parameter is ignored by the PG World storage implementation
      const result = await events.listByCorrelationId({
        runId: testRunId,
        correlationId: "step-with-data",
        pagination: {},
      });

      // 3 events: step_created, step_started, step_completed
      expect(result.data).toHaveLength(3);
      expect(result.data[2]?.correlationId).toBe("step-with-data");
    });

    it("should return events in ascending order by default", async () => {
      const correlationId = "step-ordering";

      // Create step first
      await createStep(events, testRunId, {
        stepId: correlationId,
        stepName: "test-step",
        input: new Uint8Array(),
      });

      // Create events with slight delays to ensure different timestamps
      const result1 = await events.create(testRunId, {
        eventType: "step_started",
        correlationId,
      });

      await new Promise((resolve) => setTimeout(resolve, 2));

      const result2 = await events.create(testRunId, {
        eventType: "step_completed",
        correlationId,
        eventData: { result: new Uint8Array([1]) },
      });

      const result = await events.listByCorrelationId({
        runId: testRunId,
        correlationId,
        pagination: {},
      });

      // 3 events: step_created, step_started, step_completed
      expect(result.data).toHaveLength(3);
      expect(result.data[1]?.eventId).toBe(result1.event?.eventId);
      expect(result.data[2]?.eventId).toBe(result2.event?.eventId);
      expect(result.data[1]!.createdAt.getTime()).toBeLessThanOrEqual(
        result.data[2]!.createdAt.getTime(),
      );
    });

    it("should support descending order", async () => {
      const correlationId = "step-desc-order";

      // Create step first
      await createStep(events, testRunId, {
        stepId: correlationId,
        stepName: "test-step",
        input: new Uint8Array(),
      });

      const result1 = await events.create(testRunId, {
        eventType: "step_started",
        correlationId,
      });

      await new Promise((resolve) => setTimeout(resolve, 2));

      const result2 = await events.create(testRunId, {
        eventType: "step_completed",
        correlationId,
        eventData: { result: new Uint8Array([1]) },
      });

      const result = await events.listByCorrelationId({
        runId: testRunId,
        correlationId,
        pagination: { sortOrder: "desc" },
      });

      // 3 events in descending order: step_completed, step_started, step_created
      expect(result.data).toHaveLength(3);
      expect(result.data[0]?.eventId).toBe(result2.event?.eventId);
      expect(result.data[1]?.eventId).toBe(result1.event?.eventId);
      expect(result.data[0]!.createdAt.getTime()).toBeGreaterThanOrEqual(
        result.data[1]!.createdAt.getTime(),
      );
    });

    it("should handle hook lifecycle events", async () => {
      const hookId = "hook_test123";

      // Create a typical hook lifecycle
      const createdResult = await events.create(testRunId, {
        eventType: "hook_created",
        correlationId: hookId,
        eventData: { token: "lifecycle-test-token" },
      });

      await new Promise((resolve) => setTimeout(resolve, 2));

      const received1Result = await events.create(testRunId, {
        eventType: "hook_received",
        correlationId: hookId,
        eventData: { payload: new Uint8Array([1]) },
      });

      await new Promise((resolve) => setTimeout(resolve, 2));

      const received2Result = await events.create(testRunId, {
        eventType: "hook_received",
        correlationId: hookId,
        eventData: { payload: new Uint8Array([2]) },
      });

      await new Promise((resolve) => setTimeout(resolve, 2));

      const disposedResult = await events.create(testRunId, {
        eventType: "hook_disposed",
        correlationId: hookId,
      });

      const result = await events.listByCorrelationId({
        runId: testRunId,
        correlationId: hookId,
        pagination: {},
      });

      expect(result.data).toHaveLength(4);
      expect(result.data[0]?.eventId).toBe(createdResult.event?.eventId);
      expect(result.data[0]?.eventType).toBe("hook_created");
      expect(result.data[1]?.eventId).toBe(received1Result.event?.eventId);
      expect(result.data[1]?.eventType).toBe("hook_received");
      expect(result.data[2]?.eventId).toBe(received2Result.event?.eventId);
      expect(result.data[2]?.eventType).toBe("hook_received");
      expect(result.data[3]?.eventId).toBe(disposedResult.event?.eventId);
      expect(result.data[3]?.eventType).toBe("hook_disposed");
    });

    it("should enforce token uniqueness across different runs", async () => {
      const token = "unique-token-test";

      // Create first hook with the token
      await createHook(events, testRunId, { hookId: "hook_1", token });

      // Create another run
      const run2 = await createRun(events, {
        deploymentId: "deployment-456",
        workflowName: "test-workflow-2",
        input: new Uint8Array(),
      });

      // Try to create another hook with the same token - should return hook_conflict event
      const result = await events.create(run2.runId, {
        eventType: "hook_created",
        correlationId: "hook_2",
        eventData: { token },
      });

      // Should return a hook_conflict event instead of throwing
      expect(result.event?.eventType).toBe("hook_conflict");
      expect(result.event?.correlationId).toBe("hook_2");
      expect(conflictData(result.event).token).toBe(token);
      expect(conflictData(result.event).conflictingRunId).toBe(testRunId);
      // No hook entity should be created
      expect(result.hook).toBeUndefined();
    });

    it("should allow token reuse after hook is disposed", async () => {
      const token = "reusable-token-test";

      // Create first hook with the token
      await createHook(events, testRunId, { hookId: "hook_reuse_1", token });

      // Dispose the first hook
      await events.create(testRunId, {
        eventType: "hook_disposed",
        correlationId: "hook_reuse_1",
      });

      // Create another run
      const run2 = await createRun(events, {
        deploymentId: "deployment-789",
        workflowName: "test-workflow-3",
        input: new Uint8Array(),
      });

      // Now creating a hook with the same token should succeed
      const result = await events.create(run2.runId, {
        eventType: "hook_created",
        correlationId: "hook_reuse_2",
        eventData: { token },
      });

      expect(result.hook).toBeDefined();
      expect(result.hook?.token).toBe(token);
    });
  });
});
