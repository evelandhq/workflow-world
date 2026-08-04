import type { CreateEventRequest, Step, Storage, WorkflowRun } from "@workflow/world";
import { encode } from "cbor-x";
import { Pool } from "pg";
import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";
import { createClient } from "./drizzle/index.js";
import {
  dropTenantPartitions,
  ensureTenantPartitions,
  resolveMigrationsDir,
  runMigrations,
} from "./migrate.js";
import { createEventsStorage, createRunsStorage, createStepsStorage } from "./storage.js";

/**
 * Lifecycle coverage ported from `@workflow/world-postgres`'s
 * `test/storage.test.ts`: the "idempotent operations", "step_retrying event
 * handling", "run cancellation with in-flight entities", "event ordering
 * validation" and "legacy/backwards compatibility" groups.
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
 * Set `EVELAND_WORKFLOW_WORLD_TEST_URL` to a scratch database to run it.
 */
const testUrl = process.env.EVELAND_WORKFLOW_WORLD_TEST_URL;

/**
 * One tenant per test FILE, not per test. `vitest.config.ts` sets
 * `fileParallelism: false`, but the ported suites all share one database, and
 * several of the tests below pin fixed run ids (`wrun_legacy_v1`, ...). A tenant
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
  let runs: Storage["runs"];
  let steps: Storage["steps"];
  let events: Storage["events"];

  /**
   * Upstream TRUNCATEs the four tables; here the tables are shared by every
   * tenant, so the reset is scoped by `tenant_id`. It also makes the file
   * repeatable against a database it has already used, which the fixed run ids
   * in the legacy group need.
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
    runs = createRunsStorage(drizzle, TENANT);
    steps = createStepsStorage(drizzle, TENANT);
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

  describe("legacy/backwards compatibility", () => {
    // Helper to create a legacy run directly in the database (bypassing events.create)
    // Column mapping: id (runId), deployment_id, name (workflowName), spec_version, status, input
    async function createLegacyRun(runId: string, specVersion: number | null) {
      await pool.query(
        `INSERT INTO workflow.workflow_runs (tenant_id, id, deployment_id, name, spec_version, status, input, created_at, updated_at)
        VALUES ($1, $2, 'legacy-deployment', 'legacy-workflow', $3, 'running', '[]'::jsonb, NOW(), NOW())`,
        [TENANT, runId, specVersion],
      );
    }

    describe("legacy runs (specVersion < 2 or null)", () => {
      it("should handle run_cancelled on legacy run with specVersion=1", async () => {
        const runId = "wrun_legacy_v1";
        await createLegacyRun(runId, 1);

        const result = await events.create(runId, {
          eventType: "run_cancelled",
        });

        // Legacy behavior: run is updated but event is not stored
        expect(result.run?.status).toBe("cancelled");
        expect(result.event).toBeUndefined();
      });

      it("should handle run_cancelled on legacy run with specVersion=null", async () => {
        const runId = "wrun_legacy_null";
        await createLegacyRun(runId, null);

        const result = await events.create(runId, {
          eventType: "run_cancelled",
        });

        // Legacy behavior: run is updated but event is not stored
        expect(result.run?.status).toBe("cancelled");
        expect(result.event).toBeUndefined();
      });

      it("should handle wait_completed on legacy run", async () => {
        const runId = "wrun_legacy_wait";
        await createLegacyRun(runId, 1);

        const result = await events.create(runId, {
          eventType: "wait_completed",
          correlationId: "wait_123",
          eventData: { result: new Uint8Array([1]) },
        } as unknown as CreateEventRequest);

        // Legacy behavior: event is stored but no entity mutation
        expect(result.event).toBeDefined();
        expect(result.event?.eventType).toBe("wait_completed");
        expect(result.run).toBeUndefined();
      });

      it("should handle hook_received on legacy run", async () => {
        const runId = "wrun_legacy_hook_received";
        await createLegacyRun(runId, 1);

        const result = await events.create(runId, {
          eventType: "hook_received",
          correlationId: "hook_123",
          eventData: { payload: new Uint8Array([1, 2, 3]) },
        });

        // Legacy behavior: event is stored but no entity mutation
        // (hooks exist via old system, not via events)
        expect(result.event).toBeDefined();
        expect(result.event?.eventType).toBe("hook_received");
        expect(result.event?.correlationId).toBe("hook_123");
        expect(result.hook).toBeUndefined();
      });

      it("should reject unsupported events on legacy runs", async () => {
        const runId = "wrun_legacy_unsupported";
        await createLegacyRun(runId, 1);

        // run_started is not supported for legacy runs
        await expect(events.create(runId, { eventType: "run_started" })).rejects.toThrow(
          /not supported for legacy runs/i,
        );

        // run_completed is not supported for legacy runs
        await expect(
          events.create(runId, {
            eventType: "run_completed",
            eventData: { output: new Uint8Array([1]) },
          }),
        ).rejects.toThrow(/not supported for legacy runs/i);

        // run_failed is not supported for legacy runs
        await expect(
          events.create(runId, {
            eventType: "run_failed",
            eventData: { error: "failed" },
          }),
        ).rejects.toThrow(/not supported for legacy runs/i);
      });

      it("should delete hooks when legacy run is cancelled", async () => {
        const runId = "wrun_legacy_hooks";
        await createLegacyRun(runId, 1);

        // Create a hook directly in the database for this run
        await pool.query(
          `INSERT INTO workflow.workflow_hooks (tenant_id, hook_id, run_id, token, owner_id, project_id, environment, created_at)
          VALUES ($1, 'hook_legacy', $2, 'legacy-token', 'owner', 'project', 'test', NOW())`,
          [TENANT, runId],
        );

        // Verify hook exists
        const hookBefore = await pool.query(
          `SELECT hook_id FROM workflow.workflow_hooks WHERE tenant_id = $1 AND hook_id = 'hook_legacy'`,
          [TENANT],
        );
        expect(hookBefore.rows[0]).toBeDefined();

        // Cancel the legacy run
        await events.create(runId, { eventType: "run_cancelled" });

        // Hook should be deleted
        const hookAfter = await pool.query(
          `SELECT hook_id FROM workflow.workflow_hooks WHERE tenant_id = $1 AND hook_id = 'hook_legacy'`,
          [TENANT],
        );
        expect(hookAfter.rows[0]).toBeUndefined();
      });
    });

    describe("newer runs (specVersion > current)", () => {
      it("should reject events on runs with newer specVersion", async () => {
        const runId = "wrun_future";
        // Create a run with a future spec version (higher than current)
        await pool.query(
          `INSERT INTO workflow.workflow_runs (tenant_id, id, deployment_id, name, spec_version, status, input, created_at, updated_at)
          VALUES ($1, $2, 'future-deployment', 'future-workflow', 999, 'running', '[]'::jsonb, NOW(), NOW())`,
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

    describe("legacy error column handling", () => {
      // In the current event-sourced model, the `error` field on runs/steps
      // is SerializedData (Uint8Array) produced by dehydrate*Error, stored in
      // the `error_cbor` column. Legacy records written pre-serialization-
      // pipeline (to the `error` text column) cannot be hydrated into the
      // original thrown value and are surfaced as `undefined` on read.
      it("should surface legacy errorJson field on runs as undefined", async () => {
        const runId = "wrun_legacy_error";
        const inputCbor = encode(new Uint8Array());
        await pool.query(
          `INSERT INTO workflow.workflow_runs (tenant_id, id, deployment_id, name, spec_version, status, input_cbor, error, created_at, updated_at, completed_at)
          VALUES ($1, $2, 'deployment', 'workflow', 2, 'failed', $3, $4, NOW(), NOW(), NOW())`,
          [TENANT, runId, inputCbor, '{"message":"Legacy error","stack":"at foo()"}'],
        );

        const run = await runs.get(runId);
        expect(run.status).toBe("failed");
        expect(run.error).toBeUndefined();
      });

      it("should surface legacy errorJson on steps as undefined", async () => {
        const run = await createRun(events, {
          deploymentId: "deployment",
          workflowName: "workflow",
          input: new Uint8Array(),
        });

        const inputCbor = encode(new Uint8Array());
        await pool.query(
          `INSERT INTO workflow.workflow_steps (tenant_id, run_id, step_id, step_name, status, input_cbor, error, attempt, created_at, updated_at, completed_at)
          VALUES ($1, $2, 'step_legacy_err', 'test-step', 'failed', $3, $4, 1, NOW(), NOW(), NOW())`,
          [TENANT, run.runId, inputCbor, '{"message":"Step error","stack":"at bar()"}'],
        );

        const step = await steps.get(run.runId, "step_legacy_err");
        expect(step.status).toBe("failed");
        expect(step.error).toBeUndefined();
      });
    });
  });
});
