import { SPEC_VERSION_CURRENT, type WorkflowRun } from "@workflow/world";
import { Pool } from "pg";
import { ulid } from "ulid";
import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";
import { createClient } from "./drizzle/index.js";
import { dropTenantPartitions, ensureTenantPartitions, runMigrations } from "./index.js";
import { createEventsStorage, createRunsStorage } from "./storage.js";

/**
 * Upstream `@workflow/world-postgres` covers this storage layer with a suite the
 * fork inherited none of. This file is its `describe('runs')` group, re-pointed
 * at the tenant-scoped factories: the assertions are upstream's, only the wiring
 * is ours (a tenant threaded into every factory, and this repo's migrations
 * instead of a testcontainer plus `db:push`).
 *
 * Set `EVELAND_WORKFLOW_WORLD_TEST_URL` to a scratch database to run it.
 */
const testUrl = process.env.EVELAND_WORKFLOW_WORLD_TEST_URL;

/**
 * One tenant per ported file. Files share a database and only *files* are
 * serialized, so a tenant shared with another file would let its rows into the
 * `list` assertions below — those count every run the tenant owns.
 */
const TENANT = "prj_port_runs";

type EventsStorage = ReturnType<typeof createEventsStorage>;
type RunsStorage = ReturnType<typeof createRunsStorage>;

/** Entities are only reachable through the event log; there is no direct create. */
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

describe.skipIf(!testUrl)("runs storage (postgres)", () => {
  let pool: Pool;
  let runs: RunsStorage;
  let events: EventsStorage;

  /**
   * Upstream truncates the tables outright. Here the delete is tenant-scoped:
   * truncating the partitioned parents would wipe every other tenant in the
   * database, including whatever a concurrently-developed suite provisioned.
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
    runs = createRunsStorage(drizzle, TENANT);
    events = createEventsStorage(drizzle, TENANT);
  }, 60_000);

  beforeEach(async () => {
    await deleteTenantRows();
  });

  afterAll(async () => {
    await dropTenantPartitions(pool, TENANT).catch(() => {});
    await pool?.end().catch(() => {});
  });

  describe("create", () => {
    it("should create a new workflow run", async () => {
      const runData = {
        deploymentId: "deployment-123",
        workflowName: "test-workflow",
        executionContext: { userId: "user-1" },
        input: new Uint8Array([1, 2]),
      };

      const run = await createRun(events, runData);

      expect(run.runId).toMatch(/^wrun_/);
      expect(run.deploymentId).toBe("deployment-123");
      expect(run.status).toBe("pending");
      expect(run.workflowName).toBe("test-workflow");
      expect(run.executionContext).toEqual({ userId: "user-1" });
      expect(run.input).toEqual(new Uint8Array([1, 2]));
      expect(run.output).toBeUndefined();
      expect(run.error).toBeUndefined();
      expect(run.startedAt).toBeUndefined();
      expect(run.completedAt).toBeUndefined();
      expect(run.createdAt).toBeInstanceOf(Date);
      expect(run.updatedAt).toBeInstanceOf(Date);
    });

    it("should handle minimal run data", async () => {
      const runData = {
        deploymentId: "deployment-123",
        workflowName: "minimal-workflow",
        input: new Uint8Array(),
      };

      const run = await createRun(events, runData);

      expect(run.executionContext).toBeUndefined();
      expect(run.input).toEqual(new Uint8Array());
    });

    it("should seed initial attributes from run_created", async () => {
      const run = await createRun(events, {
        deploymentId: "deployment-123",
        workflowName: "attributed-workflow",
        input: new Uint8Array(),
        attributes: { tenant: "t1", phase: "created" },
      });

      expect(run.attributes).toEqual({ tenant: "t1", phase: "created" });
    });

    it("treats SQL-looking initial attribute keys as literal JSON keys", async () => {
      const key = "tenant'); DROP TABLE workflow_runs; --";
      const run = await createRun(events, {
        deploymentId: "deployment-123",
        workflowName: "attributed-workflow",
        input: new Uint8Array(),
        attributes: { [key]: "literal" },
      });

      expect(run.attributes).toEqual({ [key]: "literal" });
    });

    /**
     * Restored: the fork used to swallow this. `.onConflictDoNothing()` returned
     * no row, so the code left `run` undefined and fell through to the generic
     * event INSERT — a duplicate `run_created` landed in the log, and eve's
     * `start()`, which asserts `result.run`, got `undefined` with no typed error
     * to branch on.
     */
    it("rejects a duplicate run_created with EntityConflictError", async () => {
      const runId = `wrun_${ulid()}`;
      const runData = {
        deploymentId: "deployment-123",
        workflowName: "test-workflow",
        input: new Uint8Array([1, 2]),
      };
      await events.create(runId, { eventType: "run_created", eventData: runData });

      await expect(
        events.create(runId, { eventType: "run_created", eventData: runData }),
      ).rejects.toMatchObject({ name: "EntityConflictError" });
    });

    it("rejects run_created when resilient start already created the run", async () => {
      // `start()` races `events.create(run_created)` against `world.queue()`. When
      // the worker dequeues first, `run_started` on a not-yet-existent run takes
      // the resilient start path and creates the run itself. The late
      // `run_created` must lose loudly: `start()` treats EntityConflictError as
      // benign, while a silent no-op both fails its `run` assertion and appends a
      // duplicate `run_created` to the log.
      const runId = `wrun_${ulid()}`;
      const runData = {
        deploymentId: "deployment-123",
        workflowName: "test-workflow",
        input: new Uint8Array([1, 2]),
      };
      await events.create(runId, { eventType: "run_started", eventData: runData });

      await expect(
        events.create(runId, { eventType: "run_created", eventData: runData }),
      ).rejects.toMatchObject({ name: "EntityConflictError" });

      // Exactly one, which is what falling through broke.
      const result = await events.list({ runId, pagination: { sortOrder: "asc" } });
      expect(result.data.filter((event) => event.eventType === "run_created")).toHaveLength(1);
    });
  });

  describe("get", () => {
    it("should retrieve an existing run", async () => {
      const created = await createRun(events, {
        deploymentId: "deployment-123",
        workflowName: "test-workflow",
        input: new Uint8Array([1]),
      });

      const retrieved = await runs.get(created.runId);
      expect(retrieved.runId).toBe(created.runId);
      expect(retrieved.workflowName).toBe("test-workflow");
      expect(retrieved.input).toEqual(new Uint8Array([1]));
    });

    it("should throw error for non-existent run", async () => {
      await expect(runs.get("missing")).rejects.toMatchObject({
        name: "WorkflowRunNotFoundError",
      });
    });
  });

  describe("update via events", () => {
    it("should update run status to running via run_started event", async () => {
      const created = await createRun(events, {
        deploymentId: "deployment-123",
        workflowName: "test-workflow",
        input: new Uint8Array(),
      });

      const updated = await updateRun(events, created.runId, "run_started");
      expect(updated.status).toBe("running");
      expect(updated.startedAt).toBeInstanceOf(Date);
    });

    it("should update run status to completed via run_completed event", async () => {
      const created = await createRun(events, {
        deploymentId: "deployment-123",
        workflowName: "test-workflow",
        input: new Uint8Array(),
      });

      const updated = await updateRun(events, created.runId, "run_completed", {
        output: new Uint8Array([42]),
      });
      expect(updated.status).toBe("completed");
      expect(updated.completedAt).toBeInstanceOf(Date);
      expect(updated.output).toEqual(new Uint8Array([42]));
    });

    it("should update run status to failed via run_failed event", async () => {
      const created = await createRun(events, {
        deploymentId: "deployment-123",
        workflowName: "test-workflow",
        input: new Uint8Array(),
      });

      // The `error` field is opaque SerializedData (Uint8Array) produced by
      // dehydrateRunError. The storage layer persists it verbatim.
      const serializedError = new Uint8Array([1, 2, 3]);
      const updated = await updateRun(events, created.runId, "run_failed", {
        error: serializedError,
      });

      expect(updated.status).toBe("failed");
      expect(updated.error).toEqual(serializedError);
      expect(updated.completedAt).toBeInstanceOf(Date);
    });
  });

  describe("list", () => {
    it("should list all runs", async () => {
      const run1 = await createRun(events, {
        deploymentId: "deployment-1",
        workflowName: "workflow-1",
        input: new Uint8Array(),
      });

      // Small delay to ensure different timestamps in createdAt
      await new Promise((resolve) => setTimeout(resolve, 2));

      const run2 = await createRun(events, {
        deploymentId: "deployment-2",
        workflowName: "workflow-2",
        input: new Uint8Array(),
      });

      const result = await runs.list();

      expect(result.data).toHaveLength(2);
      // Should be in descending order (most recent first)
      expect(result.data[0]?.runId).toBe(run2.runId);
      expect(result.data[1]?.runId).toBe(run1.runId);
      expect(result.data[0]!.createdAt.getTime()).toBeGreaterThan(
        result.data[1]!.createdAt.getTime(),
      );
    });

    it("should filter runs by workflowName", async () => {
      await createRun(events, {
        deploymentId: "deployment-1",
        workflowName: "workflow-1",
        input: new Uint8Array(),
      });
      const run2 = await createRun(events, {
        deploymentId: "deployment-2",
        workflowName: "workflow-2",
        input: new Uint8Array(),
      });

      const result = await runs.list({ workflowName: "workflow-2" });

      expect(result.data).toHaveLength(1);
      expect(result.data[0]?.runId).toBe(run2.runId);
    });

    it("should support pagination", async () => {
      // Create multiple runs
      for (let i = 0; i < 5; i++) {
        await createRun(events, {
          deploymentId: `deployment-${i}`,
          workflowName: `workflow-${i}`,
          input: new Uint8Array(),
        });
      }

      const page1 = await runs.list({
        pagination: { limit: 2 },
      });

      expect(page1.data).toHaveLength(2);
      expect(page1.cursor).not.toBeNull();

      const page2 = await runs.list({
        pagination: { limit: 2, cursor: page1.cursor || undefined },
      });

      expect(page2.data).toHaveLength(2);
      expect(page2.data[0]?.runId).not.toBe(page1.data[0]?.runId);
    });
  });

  describe("experimentalSetAttributes", () => {
    it("upserts new keys", async () => {
      const run = await createRun(events, {
        deploymentId: "d",
        workflowName: "w",
        input: new Uint8Array(),
      });

      const result = await runs.experimentalSetAttributes!(run.runId, [
        { key: "phase", value: "init" },
        { key: "tenant", value: "t1" },
      ]);
      expect(result.attributes).toEqual({ phase: "init", tenant: "t1" });

      const fresh = await runs.get(run.runId);
      expect(fresh.attributes).toEqual({ phase: "init", tenant: "t1" });
    });

    it("merges across calls without clobbering prior keys", async () => {
      const run = await createRun(events, {
        deploymentId: "d",
        workflowName: "w",
        input: new Uint8Array(),
      });

      await runs.experimentalSetAttributes!(run.runId, [{ key: "a", value: "1" }]);
      const result = await runs.experimentalSetAttributes!(run.runId, [{ key: "b", value: "2" }]);
      expect(result.attributes).toEqual({ a: "1", b: "2" });
    });

    it("removes keys when value is null", async () => {
      const run = await createRun(events, {
        deploymentId: "d",
        workflowName: "w",
        input: new Uint8Array(),
      });
      await runs.experimentalSetAttributes!(run.runId, [
        { key: "a", value: "1" },
        { key: "b", value: "2" },
      ]);
      const result = await runs.experimentalSetAttributes!(run.runId, [{ key: "a", value: null }]);
      expect(result.attributes).toEqual({ b: "2" });
    });
  });

  describe("native attr_set events", () => {
    it("materializes writes and removals on the run", async () => {
      const run = await createRun(events, {
        deploymentId: "d",
        workflowName: "w",
        input: new Uint8Array(),
        attributes: { stale: "remove" },
      });
      const result = await events.create(run.runId, {
        eventType: "attr_set",
        specVersion: SPEC_VERSION_CURRENT,
        correlationId: "attr_1",
        eventData: {
          changes: [
            { key: "phase", value: "ready" },
            { key: "stale", value: null },
          ],
          writer: { type: "workflow" },
        },
      });

      expect(result.event?.eventType).toBe("attr_set");
      expect(result.run?.attributes).toEqual({ phase: "ready" });
      expect((await runs.get(run.runId)).attributes).toEqual({
        phase: "ready",
      });
    });

    it("requires reserved-key opt-in on native events", async () => {
      const run = await createRun(events, {
        deploymentId: "d",
        workflowName: "w",
        input: new Uint8Array(),
      });
      await expect(
        events.create(run.runId, {
          eventType: "attr_set",
          specVersion: SPEC_VERSION_CURRENT,
          eventData: {
            changes: [{ key: "$system", value: "nope" }],
            writer: { type: "workflow" },
          },
        }),
      ).rejects.toThrow(/reserved prefix/);

      const result = await events.create(run.runId, {
        eventType: "attr_set",
        specVersion: SPEC_VERSION_CURRENT,
        eventData: {
          changes: [{ key: "$system", value: "ok" }],
          writer: { type: "workflow" },
          allowReservedAttributes: true,
        },
      });
      expect(result.run?.attributes).toEqual({ $system: "ok" });
    });

    it("treats SQL-looking attribute keys as literal JSON keys", async () => {
      const run = await createRun(events, {
        deploymentId: "d",
        workflowName: "w",
        input: new Uint8Array(),
      });
      const key = "phase'); DROP TABLE workflow_runs; --";

      const written = await events.create(run.runId, {
        eventType: "attr_set",
        specVersion: SPEC_VERSION_CURRENT,
        eventData: {
          changes: [{ key, value: "literal" }],
          writer: { type: "workflow" },
        },
      });
      expect(written.run?.attributes).toEqual({ [key]: "literal" });

      const removed = await events.create(run.runId, {
        eventType: "attr_set",
        specVersion: SPEC_VERSION_CURRENT,
        eventData: {
          changes: [{ key, value: null }],
          writer: { type: "workflow" },
        },
      });
      expect(removed.run?.attributes).toEqual({});
    });

    it("enforces the per-run cap against existing attributes", async () => {
      const initial: Record<string, string> = {};
      for (let i = 0; i < 63; i++) initial[`a${i}`] = "v";
      const run = await createRun(events, {
        deploymentId: "d",
        workflowName: "w",
        input: new Uint8Array(),
        attributes: initial,
      });

      // 64th attribute fits exactly at the cap.
      const atCap = await events.create(run.runId, {
        eventType: "attr_set",
        specVersion: SPEC_VERSION_CURRENT,
        eventData: {
          changes: [{ key: "a63", value: "v" }],
          writer: { type: "workflow" },
        },
      });
      expect(Object.keys(atCap.run?.attributes ?? {})).toHaveLength(64);

      // A 65th attribute exceeds the cap with a clear error.
      await expect(
        events.create(run.runId, {
          eventType: "attr_set",
          specVersion: SPEC_VERSION_CURRENT,
          eventData: {
            changes: [{ key: "a64", value: "v" }],
            writer: { type: "workflow" },
          },
        }),
      ).rejects.toThrow(/exceed limit 64/);

      // Upserting an existing key at the cap is a zero-net change.
      const upserted = await events.create(run.runId, {
        eventType: "attr_set",
        specVersion: SPEC_VERSION_CURRENT,
        eventData: {
          changes: [{ key: "a0", value: "updated" }],
          writer: { type: "step", stepId: "step_1", attempt: 1 },
        },
      });
      expect(upserted.run?.attributes?.a0).toBe("updated");

      // Removing a key frees room for a new one in the same batch.
      const swapped = await events.create(run.runId, {
        eventType: "attr_set",
        specVersion: SPEC_VERSION_CURRENT,
        eventData: {
          changes: [
            { key: "a1", value: null },
            { key: "replacement", value: "v" },
          ],
          writer: { type: "workflow" },
        },
      });
      expect(swapped.run?.attributes?.replacement).toBe("v");
      expect(swapped.run?.attributes).not.toHaveProperty("a1");
      expect(Object.keys(swapped.run?.attributes ?? {})).toHaveLength(64);
    });

    it("rejects oversized attribute values on attr_set", async () => {
      const run = await createRun(events, {
        deploymentId: "d",
        workflowName: "w",
        input: new Uint8Array(),
      });
      await expect(
        events.create(run.runId, {
          eventType: "attr_set",
          specVersion: SPEC_VERSION_CURRENT,
          eventData: {
            changes: [{ key: "note", value: "v".repeat(257) }],
            writer: { type: "workflow" },
          },
        }),
      ).rejects.toThrow(/byte length 257 exceeds limit 256/);
    });

    it("rejects invalid initial attributes on run_created", async () => {
      const overCap: Record<string, string> = {};
      for (let i = 0; i <= 64; i++) overCap[`a${i}`] = "v";
      await expect(
        createRun(events, {
          deploymentId: "d",
          workflowName: "w",
          input: new Uint8Array(),
          attributes: overCap,
        }),
      ).rejects.toThrow(/exceed limit 64/);

      await expect(
        createRun(events, {
          deploymentId: "d",
          workflowName: "w",
          input: new Uint8Array(),
          attributes: { $reserved: "nope" },
        }),
      ).rejects.toThrow(/reserved prefix/);
    });
  });
});
