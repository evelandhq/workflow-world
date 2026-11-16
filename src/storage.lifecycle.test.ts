import type { CreateEventRequest, Step, Storage, WorkflowRun } from "@workflow/world";
import { Pool } from "pg";
import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";
import { createClient } from "./drizzle/index.js";
import {
  dropTenantPartitions,
  ensureTenantPartitions,
  resolveMigrationsDir,
  runMigrations,
} from "./migrate.js";
import { createEventsStorage } from "./storage.js";

/**
 * Lifecycle coverage ported from `@workflow/world-postgres`'s
 * `test/storage.test.ts`: the "idempotent operations", "step_retrying event
 * handling", "run cancellation with in-flight entities", "event ordering
 * validation" and "spec versions" groups.
 *
 * These are upstream's assertions rather than new ones. `storage.ts` is a
 * tenant-scoped fork that deliberately keeps upstream's event-sourcing logic
 * line-for-line so upstream fixes stay easy to follow; keeping upstream's tests
 * line-for-line is the other half of that bargain — it is what tells us whether
 * a tenancy change broke the state machine underneath it.
 *
 * Two adaptations run through the whole file:
 *   * every storage factory takes `(drizzle, tenantId)`, so the tenant is
 *     threaded through each construction site;
 *   * schema setup uses this package's own `runMigrations` +
 *     `ensureTenantPartitions` instead of upstream's testcontainer + `db:push`,
 *     and every hand-written INSERT carries `tenant_id`.
 *
 * Set `WORKFLOW_WORLD_TEST_URL` to a scratch database to run it.
 */
const testUrl = process.env.WORKFLOW_WORLD_TEST_URL;

/**
 * One tenant per test FILE, not per test. `vitest.config.ts` sets
 * `fileParallelism: false`, but the ported suites all share one database, and
 * several of the tests below pin fixed run ids (`wrun_future`, ...). A tenant
 * unique to this file is what keeps those ids from colliding with another file's.
 */
const TENANT = "prj_port_lifecycle";

type EventsStorage = Storage["events"];

// Helper functions to create entities through events.create
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

async function createStep(
  events: EventsStorage,
  runId: string,
  data: {
    stepId: string;
    stepName: string;
    input: Uint8Array;
  },
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
  const result = await events.create(runId, {
    eventType,
    correlationId: stepId,
    eventData,
  } as CreateEventRequest);
  if (!result.step) {
    throw new Error("Expected step to be updated");
  }
  return result.step;
}

describe.skipIf(!testUrl)("storage lifecycle (Postgres)", () => {
  let pool: Pool;
  let events: Storage["events"];

  /**
   * Upstream TRUNCATEs the four tables; here the tables are shared by every
   * tenant, so the reset is scoped by `tenant_id`. It also makes the file
   * repeatable against a database it has already used, which the fixed run ids
   * in the spec versions group need.
   */
  async function clearTenant() {
    for (const table of [
      "workflow_events",
      "workflow_steps",
      "workflow_hooks",
      "workflow_waits",
      "workflow_runs",
    ]) {
      await pool.query(`delete from workflow.${table} where tenant_id = $1`, [TENANT]);
    }
  }

  beforeAll(async () => {
    pool = new Pool({ connectionString: testUrl, max: 2 });
    await runMigrations(pool, { migrationsDir: resolveMigrationsDir() });
    // There is deliberately no DEFAULT partition, so the tenant must be
    // provisioned before its first write or every insert below fails with
    // "no partition of relation ... found for row".
    await ensureTenantPartitions(pool, TENANT);

    const drizzle = createClient(pool);
    events = createEventsStorage(drizzle, TENANT);
  }, 60_000);

  beforeEach(async () => {
    await clearTenant();
  });

  afterAll(async () => {
    await clearTenant().catch(() => {});
    await dropTenantPartitions(pool, TENANT).catch(() => {});
    await pool.end().catch(() => {});
  });

  describe("idempotent operations", () => {
    it("should allow run_cancelled on already cancelled run (idempotent)", async () => {
      const run = await createRun(events, {
        deploymentId: "deployment-123",
        workflowName: "test-workflow",
        input: new Uint8Array(),
      });
      await events.create(run.runId, { eventType: "run_cancelled" });

      // Should succeed - idempotent operation
      const result = await events.create(run.runId, {
        eventType: "run_cancelled",
      });
      expect(result.run?.status).toBe("cancelled");
    });
  });

  describe("step_retrying event handling", () => {
    let testRunId: string;

    beforeEach(async () => {
      const run = await createRun(events, {
        deploymentId: "deployment-123",
        workflowName: "test-workflow",
        input: new Uint8Array(),
      });
      testRunId = run.runId;
    });

    it("should set step status to pending and record error", async () => {
      await createStep(events, testRunId, {
        stepId: "step_retry_1",
        stepName: "test-step",
        input: new Uint8Array(),
      });
      await updateStep(events, testRunId, "step_retry_1", "step_started");

      // The `error` field is opaque SerializedData (Uint8Array) produced by
      // dehydrateStepError. The storage layer persists it verbatim.
      const serializedError = new Uint8Array([9, 9, 9]);
      const result = await events.create(testRunId, {
        eventType: "step_retrying",
        correlationId: "step_retry_1",
        eventData: {
          error: serializedError,
          retryAfter: new Date(Date.now() + 5000),
        },
      });

      expect(result.step?.status).toBe("pending");
      expect(result.step?.error).toEqual(serializedError);
      expect(result.step?.retryAfter).toBeInstanceOf(Date);
    });

    it("should increment attempt when step_started is called after step_retrying", async () => {
      await createStep(events, testRunId, {
        stepId: "step_retry_2",
        stepName: "test-step",
        input: new Uint8Array(),
      });

      // First attempt
      const started1 = await updateStep(events, testRunId, "step_retry_2", "step_started");
      expect(started1.attempt).toBe(1);

      // Retry
      await events.create(testRunId, {
        eventType: "step_retrying",
        correlationId: "step_retry_2",
        eventData: { error: "Temporary failure" },
      });

      // Second attempt
      const started2 = await updateStep(events, testRunId, "step_retry_2", "step_started");
      expect(started2.attempt).toBe(2);
    });

    it("should reject step_retrying on completed step", async () => {
      await createStep(events, testRunId, {
        stepId: "step_retry_completed",
        stepName: "test-step",
        input: new Uint8Array(),
      });
      await updateStep(events, testRunId, "step_retry_completed", "step_completed", {
        result: new Uint8Array([1]),
      });

      await expect(
        events.create(testRunId, {
          eventType: "step_retrying",
          correlationId: "step_retry_completed",
          eventData: { error: "Should not work" },
        }),
      ).rejects.toThrow(/terminal/i);
    });

    it("should reject step_retrying on failed step", async () => {
      await createStep(events, testRunId, {
        stepId: "step_retry_failed",
        stepName: "test-step",
        input: new Uint8Array(),
      });
      await updateStep(events, testRunId, "step_retry_failed", "step_failed", {
        error: "Permanent failure",
      });

      await expect(
        events.create(testRunId, {
          eventType: "step_retrying",
          correlationId: "step_retry_failed",
          eventData: { error: "Should not work" },
        }),
      ).rejects.toThrow(/terminal/i);
    });
  });

  describe("run cancellation with in-flight entities", () => {
    it("should allow in-progress step to complete after run cancelled", async () => {
      const run = await createRun(events, {
        deploymentId: "deployment-123",
        workflowName: "test-workflow",
        input: new Uint8Array(),
      });

      // Create and start a step
      await createStep(events, run.runId, {
        stepId: "step_in_flight",
        stepName: "test-step",
        input: new Uint8Array(),
      });
      await updateStep(events, run.runId, "step_in_flight", "step_started");

      // Cancel the run
      await events.create(run.runId, { eventType: "run_cancelled" });

      // Should succeed - completing an in-progress step is allowed
      const result = await updateStep(events, run.runId, "step_in_flight", "step_completed", {
        result: new Uint8Array([1]),
      });
      expect(result.status).toBe("completed");
    });

    it("should reject step_created after run cancelled", async () => {
      const run = await createRun(events, {
        deploymentId: "deployment-123",
        workflowName: "test-workflow",
        input: new Uint8Array(),
      });
      await events.create(run.runId, { eventType: "run_cancelled" });

      await expect(
        createStep(events, run.runId, {
          stepId: "new_step_after_cancel",
          stepName: "test-step",
          input: new Uint8Array(),
        }),
      ).rejects.toThrow(/terminal/i);
    });

    it("should reject step_started for pending step after run cancelled", async () => {
      const run = await createRun(events, {
        deploymentId: "deployment-123",
        workflowName: "test-workflow",
        input: new Uint8Array(),
      });

      // Create a step but don't start it
      await createStep(events, run.runId, {
        stepId: "pending_after_cancel",
        stepName: "test-step",
        input: new Uint8Array(),
      });

      // Cancel the run
      await events.create(run.runId, { eventType: "run_cancelled" });

      // Should reject - cannot start a pending step on a cancelled run
      await expect(
        updateStep(events, run.runId, "pending_after_cancel", "step_started"),
      ).rejects.toThrow(/terminal/i);
    });
  });

  describe("event ordering validation", () => {
    let testRunId: string;

    beforeEach(async () => {
      const run = await createRun(events, {
        deploymentId: "deployment-123",
        workflowName: "test-workflow",
        input: new Uint8Array(),
      });
      testRunId = run.runId;
    });

    it("should reject step_completed before step_created", async () => {
      await expect(
        events.create(testRunId, {
          eventType: "step_completed",
          correlationId: "nonexistent_step",
          eventData: { result: new Uint8Array([1]) },
        }),
      ).rejects.toThrow(/not found/i);
    });

    it("should reject step_started before step_created", async () => {
      await expect(
        events.create(testRunId, {
          eventType: "step_started",
          correlationId: "nonexistent_step_started",
        }),
      ).rejects.toThrow(/not found/i);
    });

    it("should reject step_failed before step_created", async () => {
      await expect(
        events.create(testRunId, {
          eventType: "step_failed",
          correlationId: "nonexistent_step_failed",
          eventData: { error: "Failed" },
        }),
      ).rejects.toThrow(/not found/i);
    });

    it("should allow step_completed without step_started (instant completion)", async () => {
      await createStep(events, testRunId, {
        stepId: "instant_complete",
        stepName: "test-step",
        input: new Uint8Array(),
      });

      // Should succeed - instant completion without starting
      const result = await updateStep(events, testRunId, "instant_complete", "step_completed", {
        result: new Uint8Array([1]),
      });
      expect(result.status).toBe("completed");
    });

    it("should reject hook_disposed before hook_created", async () => {
      await expect(
        events.create(testRunId, {
          eventType: "hook_disposed",
          correlationId: "nonexistent_hook",
        }),
      ).rejects.toThrow(/not found/i);
    });

    it("should reject hook_received before hook_created", async () => {
      await expect(
        events.create(testRunId, {
          eventType: "hook_received",
          correlationId: "nonexistent_hook_received",
          eventData: { payload: new Uint8Array() },
        }),
      ).rejects.toThrow(/not found/i);
    });
  });

  describe("spec versions", () => {
    describe("newer runs (specVersion > current)", () => {
      it("should reject events on runs with newer specVersion", async () => {
        const runId = "wrun_future";
        // Create a run with a future spec version (higher than current)
        await pool.query(
          `INSERT INTO workflow.workflow_runs (tenant_id, id, deployment_id, name, spec_version, status, created_at, updated_at)
          VALUES ($1, $2, 'future-deployment', 'future-workflow', 999, 'running', NOW(), NOW())`,
          [TENANT, runId],
        );

        await expect(events.create(runId, { eventType: "run_started" })).rejects.toThrow(
          /requires spec version 999/i,
        );
      });
    });

    describe("current version runs", () => {
      it("should process events normally for current specVersion runs", async () => {
        // Create run via events.create (gets current specVersion)
        const run = await createRun(events, {
          deploymentId: "current-deployment",
          workflowName: "current-workflow",
          input: new Uint8Array(),
        });

        // Should work normally
        const result = await events.create(run.runId, {
          eventType: "run_started",
        });

        expect(result.run?.status).toBe("running");
        expect(result.event?.eventType).toBe("run_started");
      });
    });
  });
});
