/**
 * Terminal-state coverage ported from upstream `@workflow/world-postgres`'s
 * `test/storage.test.ts` — the four describes covering step terminal states, run
 * terminal states, and which operations survive a terminal run. The assertions
 * are upstream's; what changes is tenancy (every storage factory takes a
 * `tenantId`) and schema setup (this repo's own migrations instead of
 * testcontainers + `db:push`).
 *
 * Six upstream tests in this range are deliberately absent because the
 * behaviour they assert does not exist at our pins (`@workflow/world`
 * 5.0.0-beta.24): the four `tokenRetentionUntil` retention tests (no retention
 * column, and `hook_created` drops the field), and the two that expect
 * `hook_received` to raise `RunExpiredError` on a run that reached a terminal
 * state (there is no such guard on either the current-spec or legacy path).
 * Faking them would assert our fork's absence of a feature as if it were the
 * feature.
 *
 * Set `EVELAND_WORKFLOW_WORLD_TEST_URL` to a scratch database to run it.
 */
import type { Hook, HookCreatedEventRequest, Step, WorkflowRun } from "@workflow/world";
import { and, eq } from "drizzle-orm";
import { Pool } from "pg";
import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";
import { createClient } from "./drizzle/index.js";
import * as DrizzleSchema from "./drizzle/schema.js";
import { ensureTenantPartitions, runMigrations } from "./index.js";
import { createEventsStorage } from "./storage.js";

const testUrl = process.env.EVELAND_WORKFLOW_WORLD_TEST_URL;

/**
 * One tenant per ported file. `vitest.config.ts` serialises test *files*, but
 * nothing scopes their rows apart — two files sharing a tenant would see each
 * other's runs, steps and hook tokens.
 */
const TENANT = "prj_port_terminal";

type EventsStorage = ReturnType<typeof createEventsStorage>;

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

async function updateRun(
  events: EventsStorage,
  runId: string,
  eventType: "run_started" | "run_completed" | "run_failed",
  eventData?: Record<string, unknown>,
): Promise<WorkflowRun> {
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
  eventType: "step_started" | "step_completed" | "step_failed" | "step_retrying",
  eventData?: Record<string, unknown>,
): Promise<Step> {
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

describe.skipIf(!testUrl)("Storage terminal states (Postgres integration)", () => {
  let pool: Pool;
  let drizzle: ReturnType<typeof createClient>;
  let events: EventsStorage;

  /**
   * Upstream truncates the whole `workflow` schema between tests. Here the
   * tables are shared with every other tenant, so only this tenant's rows may
   * go — and they must go at least once, because `workflow_steps` is keyed
   * `(tenant_id, step_id)` with no run in the key: a second run of this file
   * against a database it has already used would collide on the fixed step ids
   * below and surface as "Expected step to be created".
   */
  async function clearTenantRows() {
    await drizzle.delete(DrizzleSchema.events).where(eq(DrizzleSchema.events.tenantId, TENANT));
    await drizzle.delete(DrizzleSchema.steps).where(eq(DrizzleSchema.steps.tenantId, TENANT));
    await drizzle.delete(DrizzleSchema.hooks).where(eq(DrizzleSchema.hooks.tenantId, TENANT));
    await drizzle.delete(DrizzleSchema.waits).where(eq(DrizzleSchema.waits.tenantId, TENANT));
    await drizzle.delete(DrizzleSchema.runs).where(eq(DrizzleSchema.runs.tenantId, TENANT));
  }

  beforeAll(async () => {
    pool = new Pool({ connectionString: testUrl, max: 2 });
    await runMigrations(pool);
    // There is deliberately no DEFAULT partition, so the tenant has to be
    // provisioned before the first write or every insert below fails with "no
    // partition of relation found for row".
    await ensureTenantPartitions(pool, TENANT);
    drizzle = createClient(pool);
    events = createEventsStorage(drizzle, TENANT);
    await clearTenantRows();
  }, 60_000);

  afterAll(async () => {
    await clearTenantRows().catch(() => {});
    await pool?.end().catch(() => {});
  });

  describe("step terminal state validation", () => {
    let testRunId: string;

    beforeEach(async () => {
      const run = await createRun(events, {
        deploymentId: "deployment-123",
        workflowName: "test-workflow",
        input: new Uint8Array(),
      });
      testRunId = run.runId;
    });

    describe("completed step", () => {
      it("should reject step_started on completed step", async () => {
        await createStep(events, testRunId, {
          stepId: "step_terminal_1",
          stepName: "test-step",
          input: new Uint8Array(),
        });
        await updateStep(events, testRunId, "step_terminal_1", "step_completed", {
          result: new Uint8Array([1]),
        });

        await expect(
          updateStep(events, testRunId, "step_terminal_1", "step_started"),
        ).rejects.toThrow(/terminal/i);
      });

      it("should reject step_completed on already completed step", async () => {
        await createStep(events, testRunId, {
          stepId: "step_terminal_2",
          stepName: "test-step",
          input: new Uint8Array(),
        });
        await updateStep(events, testRunId, "step_terminal_2", "step_completed", {
          result: new Uint8Array([1]),
        });

        await expect(
          updateStep(events, testRunId, "step_terminal_2", "step_completed", {
            result: new Uint8Array([2]),
          }),
        ).rejects.toThrow(/terminal/i);
      });

      it("should reject step_failed on completed step", async () => {
        await createStep(events, testRunId, {
          stepId: "step_terminal_3",
          stepName: "test-step",
          input: new Uint8Array(),
        });
        await updateStep(events, testRunId, "step_terminal_3", "step_completed", {
          result: new Uint8Array([1]),
        });

        await expect(
          updateStep(events, testRunId, "step_terminal_3", "step_failed", {
            error: "Should not work",
          }),
        ).rejects.toThrow(/terminal/i);
      });
    });

    describe("failed step", () => {
      it("should reject step_started on failed step", async () => {
        await createStep(events, testRunId, {
          stepId: "step_failed_1",
          stepName: "test-step",
          input: new Uint8Array(),
        });
        await updateStep(events, testRunId, "step_failed_1", "step_failed", {
          error: "Failed permanently",
        });

        await expect(
          updateStep(events, testRunId, "step_failed_1", "step_started"),
        ).rejects.toThrow(/terminal/i);
      });

      it("should reject step_completed on failed step", async () => {
        await createStep(events, testRunId, {
          stepId: "step_failed_2",
          stepName: "test-step",
          input: new Uint8Array(),
        });
        await updateStep(events, testRunId, "step_failed_2", "step_failed", {
          error: "Failed permanently",
        });

        await expect(
          updateStep(events, testRunId, "step_failed_2", "step_completed", {
            result: new Uint8Array([3]),
          }),
        ).rejects.toThrow(/terminal/i);
      });

      it("should reject step_failed on already failed step", async () => {
        await createStep(events, testRunId, {
          stepId: "step_failed_3",
          stepName: "test-step",
          input: new Uint8Array(),
        });
        await updateStep(events, testRunId, "step_failed_3", "step_failed", {
          error: "Failed once",
        });

        await expect(
          updateStep(events, testRunId, "step_failed_3", "step_failed", {
            error: "Failed again",
          }),
        ).rejects.toThrow(/terminal/i);
      });

      it("should reject step_retrying on failed step", async () => {
        await createStep(events, testRunId, {
          stepId: "step_failed_retry",
          stepName: "test-step",
          input: new Uint8Array(),
        });
        await updateStep(events, testRunId, "step_failed_retry", "step_failed", {
          error: "Failed permanently",
        });

        await expect(
          updateStep(events, testRunId, "step_failed_retry", "step_retrying", {
            error: "Retry attempt",
          }),
        ).rejects.toThrow(/terminal/i);
      });
    });

    describe("step_retrying validation", () => {
      it("should reject step_retrying on completed step", async () => {
        await createStep(events, testRunId, {
          stepId: "step_completed_retry",
          stepName: "test-step",
          input: new Uint8Array(),
        });
        await updateStep(events, testRunId, "step_completed_retry", "step_completed", {
          result: new Uint8Array([1]),
        });

        await expect(
          updateStep(events, testRunId, "step_completed_retry", "step_retrying", {
            error: "Retry attempt",
          }),
        ).rejects.toThrow(/terminal/i);
      });
    });
  });

  describe("run terminal state validation", () => {
    describe("completed run", () => {
      it("should reject run_started on completed run", async () => {
        const run = await createRun(events, {
          deploymentId: "deployment-123",
          workflowName: "test-workflow",
          input: new Uint8Array(),
        });
        await updateRun(events, run.runId, "run_completed", {
          output: new Uint8Array([1]),
        });

        await expect(updateRun(events, run.runId, "run_started")).rejects.toThrow(/terminal/i);
      });

      it("should reject run_failed on completed run", async () => {
        const run = await createRun(events, {
          deploymentId: "deployment-123",
          workflowName: "test-workflow",
          input: new Uint8Array(),
        });
        await updateRun(events, run.runId, "run_completed", {
          output: new Uint8Array([1]),
        });

        await expect(
          updateRun(events, run.runId, "run_failed", {
            error: "Should not work",
          }),
        ).rejects.toThrow(/terminal/i);
      });

      it("should reject run_cancelled on completed run", async () => {
        const run = await createRun(events, {
          deploymentId: "deployment-123",
          workflowName: "test-workflow",
          input: new Uint8Array(),
        });
        await updateRun(events, run.runId, "run_completed", {
          output: new Uint8Array([1]),
        });

        await expect(events.create(run.runId, { eventType: "run_cancelled" })).rejects.toThrow(
          /terminal/i,
        );
      });
    });

    describe("failed run", () => {
      it("should reject run_started on failed run", async () => {
        const run = await createRun(events, {
          deploymentId: "deployment-123",
          workflowName: "test-workflow",
          input: new Uint8Array(),
        });
        await updateRun(events, run.runId, "run_failed", { error: "Failed" });

        await expect(updateRun(events, run.runId, "run_started")).rejects.toThrow(/terminal/i);
      });

      it("should reject run_completed on failed run", async () => {
        const run = await createRun(events, {
          deploymentId: "deployment-123",
          workflowName: "test-workflow",
          input: new Uint8Array(),
        });
        await updateRun(events, run.runId, "run_failed", { error: "Failed" });

        await expect(
          updateRun(events, run.runId, "run_completed", {
            output: new Uint8Array([2]),
          }),
        ).rejects.toThrow(/terminal/i);
      });

      it("should reject run_cancelled on failed run", async () => {
        const run = await createRun(events, {
          deploymentId: "deployment-123",
          workflowName: "test-workflow",
          input: new Uint8Array(),
        });
        await updateRun(events, run.runId, "run_failed", { error: "Failed" });

        await expect(events.create(run.runId, { eventType: "run_cancelled" })).rejects.toThrow(
          /terminal/i,
        );
      });
    });

    describe("cancelled run", () => {
      it("should reject run_started on cancelled run", async () => {
        const run = await createRun(events, {
          deploymentId: "deployment-123",
          workflowName: "test-workflow",
          input: new Uint8Array(),
        });
        await events.create(run.runId, { eventType: "run_cancelled" });

        await expect(updateRun(events, run.runId, "run_started")).rejects.toThrow(/terminal/i);
      });

      it("should reject run_completed on cancelled run", async () => {
        const run = await createRun(events, {
          deploymentId: "deployment-123",
          workflowName: "test-workflow",
          input: new Uint8Array(),
        });
        await events.create(run.runId, { eventType: "run_cancelled" });

        await expect(
          updateRun(events, run.runId, "run_completed", {
            output: new Uint8Array([2]),
          }),
        ).rejects.toThrow(/terminal/i);
      });

      it("should reject run_failed on cancelled run", async () => {
        const run = await createRun(events, {
          deploymentId: "deployment-123",
          workflowName: "test-workflow",
          input: new Uint8Array(),
        });
        await events.create(run.runId, { eventType: "run_cancelled" });

        await expect(
          updateRun(events, run.runId, "run_failed", {
            error: "Should not work",
          }),
        ).rejects.toThrow(/terminal/i);
      });
    });
  });

  describe("allowed operations on terminal runs", () => {
    it("should allow step_completed on completed run for in-progress step", async () => {
      const run = await createRun(events, {
        deploymentId: "deployment-123",
        workflowName: "test-workflow",
        input: new Uint8Array(),
      });

      // Create and start a step (making it in-progress)
      await createStep(events, run.runId, {
        stepId: "step_in_progress",
        stepName: "test-step",
        input: new Uint8Array(),
      });
      await updateStep(events, run.runId, "step_in_progress", "step_started");

      // Complete the run while step is still running
      await updateRun(events, run.runId, "run_completed", {
        output: new Uint8Array([1]),
      });

      // Should succeed - completing an in-progress step on a terminal run is allowed
      const result = await updateStep(events, run.runId, "step_in_progress", "step_completed", {
        result: new Uint8Array([1]),
      });
      expect(result.status).toBe("completed");
    });

    it("should allow step_failed on completed run for in-progress step", async () => {
      const run = await createRun(events, {
        deploymentId: "deployment-123",
        workflowName: "test-workflow",
        input: new Uint8Array(),
      });

      // Create and start a step
      await createStep(events, run.runId, {
        stepId: "step_in_progress_fail",
        stepName: "test-step",
        input: new Uint8Array(),
      });
      await updateStep(events, run.runId, "step_in_progress_fail", "step_started");

      // Complete the run
      await updateRun(events, run.runId, "run_completed", {
        output: new Uint8Array([1]),
      });

      // Should succeed - failing an in-progress step on a terminal run is allowed
      const result = await updateStep(events, run.runId, "step_in_progress_fail", "step_failed", {
        error: "step failed",
      });
      expect(result.status).toBe("failed");
    });

    it("should auto-delete hooks when run completes (postgres-specific behavior)", async () => {
      const run = await createRun(events, {
        deploymentId: "deployment-123",
        workflowName: "test-workflow",
        input: new Uint8Array(),
      });

      // Create a hook
      await createHook(events, run.runId, {
        hookId: "hook_auto_deleted",
        token: "test-token-dispose",
      });

      // Complete the run - this auto-deletes the hook
      await updateRun(events, run.runId, "run_completed", {
        output: new Uint8Array([1]),
      });

      // The hook should no longer exist because run completion auto-deletes hooks
      // This is intentional behavior to allow token reuse across runs
      await expect(
        events.create(run.runId, {
          eventType: "hook_disposed",
          correlationId: "hook_auto_deleted",
        }),
      ).rejects.toThrow(/not found/i);
    });
  });

  describe("disallowed operations on terminal runs", () => {
    it("should reject step_created on completed run", async () => {
      const run = await createRun(events, {
        deploymentId: "deployment-123",
        workflowName: "test-workflow",
        input: new Uint8Array(),
      });
      await updateRun(events, run.runId, "run_completed", {
        output: new Uint8Array([1]),
      });

      await expect(
        createStep(events, run.runId, {
          stepId: "new_step",
          stepName: "test-step",
          input: new Uint8Array(),
        }),
      ).rejects.toThrow(/terminal/i);
    });

    it("should reject step_started on completed run for pending step", async () => {
      const run = await createRun(events, {
        deploymentId: "deployment-123",
        workflowName: "test-workflow",
        input: new Uint8Array(),
      });

      // Create a step but don't start it
      await createStep(events, run.runId, {
        stepId: "pending_step",
        stepName: "test-step",
        input: new Uint8Array(),
      });

      // Complete the run
      await updateRun(events, run.runId, "run_completed", {
        output: new Uint8Array([1]),
      });

      // Should reject - cannot start a pending step on a terminal run
      await expect(updateStep(events, run.runId, "pending_step", "step_started")).rejects.toThrow(
        /terminal/i,
      );
    });

    it("should reject hook_created on completed run", async () => {
      const run = await createRun(events, {
        deploymentId: "deployment-123",
        workflowName: "test-workflow",
        input: new Uint8Array(),
      });
      await updateRun(events, run.runId, "run_completed", {
        output: new Uint8Array([1]),
      });

      await expect(
        createHook(events, run.runId, {
          hookId: "new_hook",
          token: "new-token",
        }),
      ).rejects.toThrow(/terminal/i);
    });

    it("should reject attr_set on completed run", async () => {
      const run = await createRun(events, {
        deploymentId: "deployment-123",
        workflowName: "test-workflow",
        input: new Uint8Array(),
      });
      await updateRun(events, run.runId, "run_completed", {
        output: new Uint8Array([1]),
      });

      await expect(
        events.create(run.runId, {
          eventType: "attr_set",
          correlationId: "attr_after_complete",
          eventData: {
            changes: [{ key: "phase", value: "too-late" }],
            writer: { type: "workflow" },
          },
        }),
      ).rejects.toThrow(/terminal/i);
    });

    it("should reject step_created on failed run", async () => {
      const run = await createRun(events, {
        deploymentId: "deployment-123",
        workflowName: "test-workflow",
        input: new Uint8Array(),
      });
      await updateRun(events, run.runId, "run_failed", { error: "Failed" });

      await expect(
        createStep(events, run.runId, {
          stepId: "new_step_failed",
          stepName: "test-step",
          input: new Uint8Array(),
        }),
      ).rejects.toThrow(/terminal/i);
    });

    it("should reject step_created on cancelled run", async () => {
      const run = await createRun(events, {
        deploymentId: "deployment-123",
        workflowName: "test-workflow",
        input: new Uint8Array(),
      });
      await events.create(run.runId, { eventType: "run_cancelled" });

      await expect(
        createStep(events, run.runId, {
          stepId: "new_step_cancelled",
          stepName: "test-step",
          input: new Uint8Array(),
        }),
      ).rejects.toThrow(/terminal/i);
    });

    it("should reject hook_created on failed run", async () => {
      const run = await createRun(events, {
        deploymentId: "deployment-123",
        workflowName: "test-workflow",
        input: new Uint8Array(),
      });
      await updateRun(events, run.runId, "run_failed", { error: "Failed" });

      await expect(
        createHook(events, run.runId, {
          hookId: "new_hook_failed",
          token: "new-token-failed",
        }),
      ).rejects.toThrow(/terminal/i);
    });

    it("should reject hook_created on cancelled run", async () => {
      const run = await createRun(events, {
        deploymentId: "deployment-123",
        workflowName: "test-workflow",
        input: new Uint8Array(),
      });
      await events.create(run.runId, { eventType: "run_cancelled" });

      await expect(
        createHook(events, run.runId, {
          hookId: "new_hook_cancelled",
          token: "new-token-cancelled",
        }),
      ).rejects.toThrow(/terminal/i);
    });

    it("should reject hook_received on a completed run", async () => {
      const run = await createRun(events, {
        deploymentId: "deployment-123",
        workflowName: "test-workflow",
        input: new Uint8Array(),
      });
      const hook = await createHook(events, run.runId, {
        hookId: "hook_before_complete",
        token: "token-before-complete",
      });
      await updateRun(events, run.runId, "run_completed", {
        output: new Uint8Array([1]),
      });

      // run_completed's hook/wait cleanup runs before hook_received is
      // attempted here, so this sequential case surfaces as the hook no
      // longer existing rather than as a terminal-run rejection.
      await expect(
        events.create(run.runId, {
          eventType: "hook_received",
          correlationId: hook.hookId,
          eventData: { payload: {} },
        }),
      ).rejects.toMatchObject({ name: "HookNotFoundError" });
    });

    it("accepts hook_received on a live legacy run", async () => {
      // Legacy runs (specVersion <= 1) are routed to
      // handleLegacyEventPostgres, which stores the event without touching
      // entities. Simulated by downgrading a real run's persisted specVersion,
      // since nothing here can mint a legacy run any more.
      const run = await createRun(events, {
        deploymentId: "deployment-123",
        workflowName: "legacy-workflow",
        input: new Uint8Array(),
      });
      await drizzle
        .update(DrizzleSchema.runs)
        .set({ specVersion: 1 })
        .where(
          and(eq(DrizzleSchema.runs.tenantId, TENANT), eq(DrizzleSchema.runs.runId, run.runId)),
        );

      const result = await events.create(run.runId, {
        eventType: "hook_received",
        correlationId: "hook_legacy_live",
        eventData: { payload: {} },
      });
      expect(result.event?.eventType).toBe("hook_received");
    });
  });
});
