import type { Step, Storage, WorkflowRun } from "@workflow/world";
import { SPEC_VERSION_CURRENT } from "@workflow/world";
import { Pool } from "pg";
import { decodeTime } from "ulid";
import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";
import { createClient } from "./drizzle/index.js";
import { dropTenantPartitions, ensureTenantPartitions, runMigrations } from "./migrate.js";
import { createEventsStorage, createStepsStorage } from "./storage.js";

/**
 * Upstream `@workflow/world-postgres`'s step-storage coverage, ported onto the
 * tenant-scoped factories. The storage logic is inherited verbatim, so these are
 * the assertions that catch a tenancy edit breaking it — most of them exercise
 * paths (lazy step start, the event-id-after-lock ordering) that no other suite
 * in this repo touches.
 *
 * Set `EVELAND_WORKFLOW_WORLD_TEST_URL` to a scratch database to run it.
 */
const testUrl = process.env.EVELAND_WORKFLOW_WORLD_TEST_URL;

/**
 * One tenant per test file. Files run serially (`fileParallelism: false`) but
 * share the database, and steps are keyed by `(tenantId, stepId)` — these tests
 * reuse fixed ids like `step-123`, so a tenant shared with another suite would
 * turn an unrelated leftover row into a primary-key violation here.
 */
const TENANT = "prj_port_steps";

type EventsStorage = Storage["events"];

async function createRun(
  events: EventsStorage,
  data: {
    deploymentId: string;
    workflowName: string;
    input: Uint8Array;
    executionContext?: Record<string, unknown>;
    attributes?: Record<string, string>;
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

async function updateRun(
  events: EventsStorage,
  runId: string,
  eventType: "run_started" | "run_completed" | "run_failed",
  eventData?: Record<string, unknown>,
): Promise<WorkflowRun> {
  // The cast is the price of the shared helper: `eventType` is a union here, so
  // TS cannot pick a single member of the `CreateEventRequest` discriminated
  // union and match `eventData` against it.
  const result = await events.create(runId, {
    eventType,
    eventData,
  } as Parameters<EventsStorage["create"]>[1]);
  if (!result.run) {
    throw new Error("Expected run to be updated");
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

async function updateStep(
  events: EventsStorage,
  runId: string,
  stepId: string,
  eventType: "step_started" | "step_completed" | "step_failed",
  eventData?: Record<string, unknown>,
): Promise<Step> {
  // Cast for the same reason as `updateRun` above.
  const result = await events.create(runId, {
    eventType,
    correlationId: stepId,
    eventData,
  } as Parameters<EventsStorage["create"]>[1]);
  if (!result.step) {
    throw new Error("Expected step to be updated");
  }
  return result.step;
}

describe.skipIf(!testUrl)("steps storage", () => {
  let pool: Pool;
  let steps: Storage["steps"];
  let events: EventsStorage;
  let testRunId: string;

  /**
   * Upstream truncates the whole schema between tests; scope the reset to this
   * file's tenant instead so a suite that owns another partition is untouched.
   * The reset is what lets the fixed step ids below be reused test to test, and
   * what makes a re-run against a database this file has already used repeatable.
   */
  async function resetTenantRows(): Promise<void> {
    for (const table of ["workflow_events", "workflow_steps", "workflow_hooks", "workflow_runs"]) {
      await pool.query(`delete from workflow.${table} where tenant_id = $1`, [TENANT]);
    }
  }

  beforeAll(async () => {
    pool = new Pool({ connectionString: testUrl, max: 5 });
    await runMigrations(pool);
    // No DEFAULT partition exists, so the tenant must be provisioned before the
    // first write or every insert below fails on a missing partition.
    await ensureTenantPartitions(pool, TENANT);

    const drizzle = createClient(pool);
    steps = createStepsStorage(drizzle, TENANT);
    events = createEventsStorage(drizzle, TENANT);
  }, 60_000);

  afterAll(async () => {
    await resetTenantRows().catch(() => {});
    await dropTenantPartitions(pool, TENANT).catch(() => {});
    await pool?.end().catch(() => {});
  });

  beforeEach(async () => {
    await resetTenantRows();
    const run = await createRun(events, {
      deploymentId: "deployment-123",
      workflowName: "test-workflow",
      input: new Uint8Array(),
    });
    testRunId = run.runId;
  });

  describe("create", () => {
    it("should create a new step", async () => {
      const stepData = {
        stepId: "step-123",
        stepName: "test-step",
        input: new Uint8Array([1, 2]),
      };

      const step = await createStep(events, testRunId, stepData);

      expect(step).toMatchObject({
        runId: testRunId,
        stepId: "step-123",
        stepName: "test-step",
        status: "pending",
        input: new Uint8Array([1, 2]),
        output: undefined,
        error: undefined,
        attempt: 0, // steps are created with attempt 0
        startedAt: undefined,
        completedAt: undefined,
        createdAt: expect.any(Date),
        updatedAt: expect.any(Date),
        specVersion: SPEC_VERSION_CURRENT,
      });
    });
  });

  describe("get", () => {
    it("should retrieve a step with runId and stepId", async () => {
      const created = await createStep(events, testRunId, {
        stepId: "step-123",
        stepName: "test-step",
        input: new Uint8Array([1]),
      });

      const retrieved = await steps.get(testRunId, "step-123");

      expect(retrieved.stepId).toBe(created.stepId);
    });

    it("should throw error for non-existent step", async () => {
      await expect(steps.get(testRunId, "missing-step")).rejects.toThrow("Step not found");
    });
  });

  describe("update via events", () => {
    it("should update step status to running via step_started event", async () => {
      await createStep(events, testRunId, {
        stepId: "step-123",
        stepName: "test-step",
        input: new Uint8Array([1]),
      });

      const updated = await updateStep(
        events,
        testRunId,
        "step-123",
        "step_started",
        {}, // step_started no longer needs attempt in eventData - World increments it
      );

      expect(updated.status).toBe("running");
      expect(updated.startedAt).toBeInstanceOf(Date);
      expect(updated.attempt).toBe(1); // Incremented by step_started
    });

    it("allocates the step_started event id after the guarded step update", async () => {
      const stepId = "step-start-lock";
      await createStep(events, testRunId, {
        stepId,
        stepName: "test-step",
        input: new Uint8Array([1]),
      });

      // Hold the step row from outside, then check the event id's ULID timestamp
      // was drawn after the lock was released. Allocating it before the guarded
      // UPDATE would let a writer that waited on the row insert an event id
      // older than one a concurrent terminal event already wrote, and replay
      // orders by event id.
      const lockPool = new Pool({ connectionString: testUrl, max: 1 });
      const client = await lockPool.connect();

      try {
        await client.query("BEGIN");
        await client.query(
          "SELECT 1 FROM workflow.workflow_steps WHERE tenant_id = $1 AND run_id = $2 AND step_id = $3 FOR UPDATE",
          [TENANT, testRunId, stepId],
        );

        const started = events.create(testRunId, {
          eventType: "step_started",
          correlationId: stepId,
        });

        await new Promise((resolve) => setTimeout(resolve, 50));
        const releasedAt = Date.now();
        await client.query("COMMIT");

        const result = await started;
        if (!result.event) {
          throw new Error("Expected step_started event");
        }
        expect(decodeTime(result.event.eventId.slice("wevt_".length))).toBeGreaterThanOrEqual(
          releasedAt,
        );
      } finally {
        client.release();
        await lockPool.end();
      }
    });

    it("should update step status to completed via step_completed event", async () => {
      await createStep(events, testRunId, {
        stepId: "step-123",
        stepName: "test-step",
        input: new Uint8Array([1]),
      });

      const updated = await updateStep(events, testRunId, "step-123", "step_completed", {
        result: new Uint8Array([1]),
      });

      expect(updated.status).toBe("completed");
      expect(updated.completedAt).toBeInstanceOf(Date);
      expect(updated.output).toEqual(new Uint8Array([1]));
    });

    it("should update step status to failed via step_failed event", async () => {
      await createStep(events, testRunId, {
        stepId: "step-123",
        stepName: "test-step",
        input: new Uint8Array([1]),
      });

      // The `error` field is opaque SerializedData (Uint8Array) produced by
      // dehydrateStepError. The storage layer persists it verbatim.
      const serializedError = new Uint8Array([1, 2, 3]);
      const updated = await updateStep(events, testRunId, "step-123", "step_failed", {
        error: serializedError,
      });

      expect(updated.status).toBe("failed");
      expect(updated.error).toEqual(serializedError);
      expect(updated.completedAt).toBeInstanceOf(Date);
    });
  });

  describe("lazy step start", () => {
    it("creates the step on the fly when step_started carries input", async () => {
      const result = await events.create(testRunId, {
        eventType: "step_started",
        correlationId: "lazy-step-1",
        eventData: {
          stepName: "lazy-step",
          input: new Uint8Array([7, 8, 9]),
        },
      });

      // Created + started in one call: running, attempt 1, ownership signal.
      expect(result.step?.stepId).toBe("lazy-step-1");
      expect(result.step?.stepName).toBe("lazy-step");
      expect(result.step?.status).toBe("running");
      expect(result.step?.attempt).toBe(1);
      expect(result.step?.input).toEqual(new Uint8Array([7, 8, 9]));
      expect(result.stepCreated).toBe(true);

      const persisted = await steps.get(testRunId, "lazy-step-1");
      expect(persisted.status).toBe("running");
      expect(persisted.input).toEqual(new Uint8Array([7, 8, 9]));
    });

    it("writes a synthetic step_created event (input there, not on step_started)", async () => {
      await events.create(testRunId, {
        eventType: "step_started",
        correlationId: "lazy-step-2",
        eventData: { stepName: "lazy-step", input: new Uint8Array([1]) },
      });

      const evts = await events.listByCorrelationId({ correlationId: "lazy-step-2" });
      const created = evts.data.find((e) => e.eventType === "step_created");
      const started = evts.data.find((e) => e.eventType === "step_started");
      expect(created).toBeDefined();
      expect(started).toBeDefined();
      expect((created?.eventData as { input?: unknown } | undefined)?.input).toBeDefined();
      expect((started?.eventData as { input?: unknown } | undefined)?.input).toBeUndefined();
    });

    it("still rejects a bare step_started (no input) on a missing step", async () => {
      await expect(
        events.create(testRunId, {
          eventType: "step_started",
          correlationId: "never-created",
          eventData: { stepName: "legacy-step" },
        }),
      ).rejects.toThrow("not found");
    });

    it("rejects a second lazy step_started for an existing step (concurrent loser)", async () => {
      const first = await events.create(testRunId, {
        eventType: "step_started",
        correlationId: "lazy-step-3",
        eventData: { stepName: "lazy-step", input: new Uint8Array([1]) },
      });
      expect(first.step?.attempt).toBe(1);
      expect(first.stepCreated).toBe(true);

      // The step exists → this caller lost the create race → must not start
      // or run the body. EntityConflictError → executeStep `skipped`.
      await expect(
        events.create(testRunId, {
          eventType: "step_started",
          correlationId: "lazy-step-3",
          eventData: { stepName: "lazy-step", input: new Uint8Array([1]) },
        }),
      ).rejects.toMatchObject({ name: "EntityConflictError" });
    });

    it("crash recovery re-starts via a non-lazy step_started on the existing step", async () => {
      // Owner creates + starts lazily (attempt 1). On recovery the step
      // already exists, so it is re-run via a NON-lazy step_started (no
      // input), which re-starts the step (attempt 2) — at-least-once.
      await events.create(testRunId, {
        eventType: "step_started",
        correlationId: "lazy-step-4",
        eventData: { stepName: "lazy-step", input: new Uint8Array([1]) },
      });

      const rerun = await updateStep(events, testRunId, "lazy-step-4", "step_started", {});
      expect(rerun.status).toBe("running");
      expect(rerun.attempt).toBe(2);
    });

    it("rejects a lazy step_started on a terminal run", async () => {
      await updateRun(events, testRunId, "run_started");
      await updateRun(events, testRunId, "run_completed", { output: new Uint8Array([1]) });

      await expect(
        events.create(testRunId, {
          eventType: "step_started",
          correlationId: "lazy-on-terminal",
          eventData: { stepName: "lazy-step", input: new Uint8Array([1]) },
        }),
      ).rejects.toThrow("terminal state");
    });

    it("a lazy step_started followed by step_failed marks the step failed", async () => {
      // Regression guard for the unregistered-step path on the lazy inline
      // route: executeStep sends the lazy step_started to materialize the
      // deferred step, then writes step_failed. Failing a never-created step
      // would hit the "step must exist" ordering guard and wedge the run.
      await events.create(testRunId, {
        eventType: "step_started",
        correlationId: "lazy-step-fail",
        eventData: { stepName: "ghost-step", input: new Uint8Array([1]) },
      });

      const failed = await updateStep(events, testRunId, "lazy-step-fail", "step_failed", {
        error: new Uint8Array([2, 3]),
      });
      expect(failed.status).toBe("failed");
      expect(failed.attempt).toBe(1);
    });
  });

  describe("list", () => {
    it("should list all steps for a run", async () => {
      const step1 = await createStep(events, testRunId, {
        stepId: "step-1",
        stepName: "first-step",
        input: new Uint8Array(),
      });
      const step2 = await createStep(events, testRunId, {
        stepId: "step-2",
        stepName: "second-step",
        input: new Uint8Array(),
      });

      const result = await steps.list({ runId: testRunId });

      expect(result.data).toHaveLength(2);
      // Should be in descending order
      const [first, second] = result.data;
      expect(first?.stepId).toBe(step2.stepId);
      expect(second?.stepId).toBe(step1.stepId);
      expect(first?.createdAt.getTime()).toBeGreaterThanOrEqual(
        second?.createdAt.getTime() ?? Number.NaN,
      );
    });

    it("should support pagination", async () => {
      // Create multiple steps
      for (let i = 0; i < 5; i++) {
        await createStep(events, testRunId, {
          stepId: `step-${i}`,
          stepName: `step-name-${i}`,
          input: new Uint8Array(),
        });
      }

      const page1 = await steps.list({
        runId: testRunId,
        pagination: { limit: 2 },
      });

      expect(page1.data).toHaveLength(2);
      expect(page1.cursor).not.toBeNull();

      const page2 = await steps.list({
        runId: testRunId,
        pagination: { limit: 2, cursor: page1.cursor || undefined },
      });

      expect(page2.data).toHaveLength(2);
      expect(page2.data[0]?.stepId).not.toBe(page1.data[0]?.stepId);
    });
  });
});
