import type { Hook, HookCreatedEventRequest, Step, WorkflowRun } from "@workflow/world";
import { SPEC_VERSION_CURRENT } from "@workflow/world";
import { encode } from "cbor-x";
import { Pool } from "pg";
import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";
import * as DrizzleSchema from "./drizzle/schema.js";
import { createClient } from "./drizzle/index.js";
import {
  dropTenantPartitions,
  ensureTenantPartitions,
  resolveMigrationsDir,
  runMigrations,
} from "./migrate.js";
import {
  createEventsStorage,
  createHooksStorage,
  createRunsStorage,
  createStepsStorage,
} from "./storage.js";

/**
 * Upstream `@workflow/world-postgres`'s `concurrent entity-creation races`
 * group, ported onto the tenant-scoped factories.
 *
 * These are the tests that hold the dedup contract the eve runtime is written
 * against: a second writer of a correlated creation event must surface as
 * `EntityConflictError`, because that is the only error the runtime's
 * concurrent-replay catch path swallows. Anything else — a raw driver error, or
 * worse, a silent second insert — turns two concurrent replays of one
 * resumption into two executions of the same step or hook.
 *
 * The tenancy fork puts more weight on them than upstream does. `tenant_id`
 * leads the dedup index and `workflow_events` is LIST-partitioned, so Postgres
 * reports a per-tenant *child* index name on conflict; the translation in
 * `isCorrelatedEventUniqueViolation` has to recognise that name or every
 * assertion below degrades to an unhandled 23505.
 *
 * Set `EVELAND_WORKFLOW_WORLD_TEST_URL` to a scratch database to run it.
 */
const testUrl = process.env.EVELAND_WORKFLOW_WORLD_TEST_URL;

/**
 * Fixed rather than per-run-unique, because `vitest.config.ts` sets
 * `fileParallelism: false` and this id belongs to this file alone — no other
 * suite writes rows under it, so `clearTenantRows` below is enough to make the
 * file repeatable against a database it has already used.
 */
const TENANT = "prj_port_races";

type EventsStorage = ReturnType<typeof createEventsStorage>;

// Entities are only creatable through the event log, so the helpers below are
// how upstream's fixtures build them. Duplicated from upstream's shared block
// (adapted for the tenant-scoped factories) so this file stands alone.
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

/**
 * Upstream's `updateRun` takes the lifecycle event type as a parameter; this
 * group only ever starts a run, and a union-typed discriminant does not narrow
 * against `CreateEventRequest`, so it is pinned to `run_started` rather than
 * cast past the type.
 */
async function startRun(events: EventsStorage, runId: string): Promise<WorkflowRun> {
  const result = await events.create(runId, { eventType: "run_started" });
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

/**
 * `hook_conflict` carries a `conflictingRunId` that the `Event` union does not
 * expose (upstream reaches it through `any`), and `EventResult.event` is
 * optional in our pinned `@workflow/world`. Reading it through one narrowing
 * helper keeps the optionality honest: an absent event fails the comparison
 * instead of throwing a TypeError that would read as a different bug.
 */
function asConflictEvent(
  event: unknown,
): { eventType: string; eventData: { conflictingRunId: string } } | undefined {
  return event as { eventType: string; eventData: { conflictingRunId: string } } | undefined;
}

describe.skipIf(!testUrl)("concurrent entity-creation races", () => {
  let admin: Pool;
  let pool: Pool;
  let runs: ReturnType<typeof createRunsStorage>;
  let steps: ReturnType<typeof createStepsStorage>;
  let events: ReturnType<typeof createEventsStorage>;
  let hooks: ReturnType<typeof createHooksStorage>;
  let testRunId: string;

  /**
   * Upstream truncates the whole schema between tests. Scoped to this file's
   * tenant instead — the row set is identical here, and a truncate would be a
   * loaded gun in a database that other suites may also be migrated into.
   *
   * It is not merely hygiene: hook tokens are unique per *tenant*, not per run,
   * so without this a second run of the file would find the previous run's
   * `idempotent-token` hook under a different hookId and get a legitimate
   * `hook_conflict` where the test expects a fresh creation.
   */
  async function clearTenantRows() {
    for (const table of [
      "workflow_events",
      "workflow_steps",
      "workflow_hooks",
      "workflow_waits",
      "workflow_runs",
    ]) {
      await admin.query(`delete from workflow.${table} where tenant_id = $1`, [TENANT]);
    }
  }

  beforeAll(async () => {
    admin = new Pool({ connectionString: testUrl, max: 2 });
    await runMigrations(admin, { migrationsDir: resolveMigrationsDir() });
    // No DEFAULT partition exists by design, so the tenant must be provisioned
    // before the first write or every insert below fails on a missing partition.
    await ensureTenantPartitions(admin, TENANT);

    // `max: 1` mirrors upstream, and the concurrency tests depend on it: with a
    // single connection the two racing `events.create` calls interleave at their
    // await points in a fixed order, so "one wins, one conflicts" is
    // deterministic instead of a coin flip between two server backends.
    pool = new Pool({ connectionString: testUrl, max: 1 });
    const drizzle = createClient(pool);
    runs = createRunsStorage(drizzle, TENANT);
    steps = createStepsStorage(drizzle, TENANT);
    events = createEventsStorage(drizzle, TENANT);
    hooks = createHooksStorage(drizzle, TENANT);
  }, 60_000);

  beforeEach(async () => {
    await clearTenantRows();
    const run = await createRun(events, {
      deploymentId: "deployment-123",
      workflowName: "test-workflow",
      input: new Uint8Array(),
    });
    testRunId = run.runId;
    await startRun(events, testRunId);
  });

  afterAll(async () => {
    await clearTenantRows().catch(() => {});
    await pool?.end().catch(() => {});
    await dropTenantPartitions(admin, TENANT).catch(() => {});
    await admin?.end().catch(() => {});
  });

  it("should reject concurrent step_created with the same correlationId", async () => {
    // Two concurrent step_created calls with identical correlationIds
    // (as produced by the snapshot runtime's deterministic ULIDs across
    // concurrent VM invocations of the same resumption) must produce
    // exactly one step_created event in the log. The unique partial
    // index on workflow_events ensures the loser's INSERT raises a
    // unique-violation, which storage translates to EntityConflictError
    // for the runtime's existing dedup catch path.
    const results = await Promise.allSettled([
      createStep(events, testRunId, {
        stepId: "step_dup_1",
        stepName: "test-step",
        input: new Uint8Array([1]),
      }),
      createStep(events, testRunId, {
        stepId: "step_dup_1",
        stepName: "test-step",
        input: new Uint8Array([2]),
      }),
    ]);

    const fulfilled = results.filter((r) => r.status === "fulfilled");
    const rejected = results.filter((r) => r.status === "rejected");
    expect(fulfilled).toHaveLength(1);
    expect(rejected).toHaveLength(1);
    expect((rejected[0] as PromiseRejectedResult).reason).toMatchObject({
      name: "EntityConflictError",
    });

    // Verify only one step_created event exists in the log.
    const evts = await events.list({
      runId: testRunId,
      pagination: {},
    });
    const stepCreated = evts.data.filter(
      (e) => e.eventType === "step_created" && e.correlationId === "step_dup_1",
    );
    expect(stepCreated).toHaveLength(1);

    // The winner's step is the one that materialized: a loser that had also
    // written its own step row would leave the entity disagreeing with the log.
    const persistedSteps = await steps.list({ runId: testRunId });
    expect(persistedSteps.data).toHaveLength(1);
  });

  it("should reject sequential duplicate step_created with EntityConflictError", async () => {
    await createStep(events, testRunId, {
      stepId: "step_seq_dup",
      stepName: "test-step",
      input: new Uint8Array(),
    });
    await expect(
      createStep(events, testRunId, {
        stepId: "step_seq_dup",
        stepName: "test-step",
        input: new Uint8Array(),
      }),
    ).rejects.toMatchObject({ name: "EntityConflictError" });
  });

  it("should reject duplicate correlated workflow attr_set events", async () => {
    await events.create(testRunId, {
      eventType: "attr_set",
      correlationId: "attr_dup_1",
      eventData: {
        changes: [{ key: "phase", value: "running" }],
        writer: { type: "workflow" },
      },
    });
    await expect(
      events.create(testRunId, {
        eventType: "attr_set",
        correlationId: "attr_dup_1",
        eventData: {
          changes: [{ key: "phase", value: "running" }],
          writer: { type: "workflow" },
        },
      }),
    ).rejects.toMatchObject({ name: "EntityConflictError" });

    // A duplicate carrying *different* changes for the same correlationId
    // must be rejected before touching the run snapshot — otherwise the
    // materialized attributes would diverge from the event log.
    await expect(
      events.create(testRunId, {
        eventType: "attr_set",
        correlationId: "attr_dup_1",
        eventData: {
          changes: [{ key: "phase", value: "DIVERGED" }],
          writer: { type: "workflow" },
        },
      }),
    ).rejects.toMatchObject({ name: "EntityConflictError" });
    expect((await runs.get(testRunId)).attributes?.phase).toBe("running");

    const evts = await events.list({
      runId: testRunId,
      pagination: {},
    });
    expect(
      evts.data.filter(
        (event) => event.eventType === "attr_set" && event.correlationId === "attr_dup_1",
      ),
    ).toHaveLength(1);
  });

  it("should reject duplicate wait_created with EntityConflictError", async () => {
    // Sequential duplicate wait_created — the wait_created insert path
    // uses `INSERT ... onConflictDoNothing()` plus an existence check, so
    // the second insert is silently dropped at the SQL level. The unique
    // partial index on workflow_events still provides a stronger
    // concurrent guarantee here, and the storage layer translates the
    // resulting unique-violation into an EntityConflictError matching the
    // step_created behavior.
    await events.create(testRunId, {
      eventType: "wait_created",
      correlationId: "wait_seq_dup",
      eventData: { resumeAt: new Date("2099-01-01") },
    });
    await expect(
      events.create(testRunId, {
        eventType: "wait_created",
        correlationId: "wait_seq_dup",
        eventData: { resumeAt: new Date("2099-01-02") },
      }),
    ).rejects.toMatchObject({ name: "EntityConflictError" });

    // Mirror the step_created test: assert exactly one wait_created
    // event landed in the log, so a regression that allowed both
    // inserts through would fail this test even if the second
    // insert's translation to EntityConflictError still worked.
    const evts = await events.list({
      runId: testRunId,
      pagination: {},
    });
    const waitCreated = evts.data.filter(
      (e) => e.eventType === "wait_created" && e.correlationId === "wait_seq_dup",
    );
    expect(waitCreated).toHaveLength(1);
  });

  it("should reject duplicate same-hook hook_created with EntityConflictError, not hook_conflict", async () => {
    // Regression test for https://github.com/vercel/workflow/issues/2283
    //
    // Duplicate processing of the *same* (runId, hookId, token) — e.g.
    // queue redelivery or cross-process replay — must be idempotent.
    // It must throw EntityConflictError (mirroring the step_created
    // duplicate path) so the runtime's existing concurrent-replay catch
    // path swallows it, and must NOT append a hook_conflict event that
    // would later replay as a self-conflict HookConflictError.
    const token = "idempotent-token";
    const hookId = "hook_idem_1";

    await createHook(events, testRunId, { hookId, token });

    // Same runId, same hookId, same token — must be idempotent.
    await expect(
      events.create(testRunId, {
        eventType: "hook_created",
        correlationId: hookId,
        eventData: { token },
      }),
    ).rejects.toMatchObject({ name: "EntityConflictError" });

    // No hook_conflict event should have been written to the log.
    const evts = await events.list({
      runId: testRunId,
      pagination: {},
    });
    const hookCreatedEvents = evts.data.filter(
      (e) => e.eventType === "hook_created" && e.correlationId === hookId,
    );
    const hookConflictEvents = evts.data.filter((e) => e.eventType === "hook_conflict");
    expect(hookCreatedEvents).toHaveLength(1);
    expect(hookConflictEvents).toHaveLength(0);
  });

  it("should still emit hook_conflict for a different hookId reusing the same token in the same run", async () => {
    // The idempotency guard must NOT mask genuine token conflicts — a
    // different hookId reusing the same token (even in the same run)
    // is still a real conflict.
    const token = "same-run-different-hook-token";

    await createHook(events, testRunId, { hookId: "hook_a", token });

    const result = await events.create(testRunId, {
      eventType: "hook_created",
      correlationId: "hook_b",
      eventData: { token },
    });

    const conflict = asConflictEvent(result.event);
    expect(conflict?.eventType).toBe("hook_conflict");
    expect(conflict?.eventData.conflictingRunId).toBe(testRunId);
    expect(result.hook).toBeUndefined();
  });

  it("should still emit hook_conflict for the same hookId in a different run reusing the same token", async () => {
    // The idempotency guard checks (runId, hookId) together — a
    // different run reusing the same hookId (highly unlikely in
    // practice, but a worthwhile boundary) must still produce a real
    // hook_conflict.
    const token = "cross-run-same-hookid-token";
    const hookId = "hook_shared_id";

    await createHook(events, testRunId, { hookId, token });

    const otherRun = await createRun(events, {
      deploymentId: "deployment-other",
      workflowName: "other-workflow",
      input: new Uint8Array(),
    });

    const result = await events.create(otherRun.runId, {
      eventType: "hook_created",
      correlationId: hookId,
      eventData: { token },
    });

    const conflict = asConflictEvent(result.event);
    expect(conflict?.eventType).toBe("hook_conflict");
    expect(conflict?.eventData.conflictingRunId).toBe(testRunId);
    expect(result.hook).toBeUndefined();
  });

  it("should recover an orphaned hook row that lacks a hook_created event", async () => {
    // Crash-recovery regression: in `events.create`, the hook INSERT and the
    // events INSERT are not wrapped in a single transaction. If a process / DB
    // interruption lands between them, the hook row exists but no
    // `hook_created` event is in the log. The same-`(runId, hookId)`
    // retry must not be treated as a "real duplicate" — that would
    // throw EntityConflictError, which the runtime's concurrent-
    // replay catch path would swallow, permanently leaving the run
    // with a hook entity but no `hook_created` event in the log.
    //
    // The recovery path detects the missing event and completes the
    // partial write: it skips re-inserting the hook row and lets the
    // outer code path emit the `hook_created` event.
    const token = "orphaned-hook-row-token";
    const hookId = "hook_orphan_pg_1";

    // Pre-seed an orphaned hook row that has no corresponding
    // `hook_created` event in the events table.
    const drizzle = createClient(pool);
    await drizzle.insert(DrizzleSchema.hooks).values({
      tenantId: TENANT,
      runId: testRunId,
      hookId,
      token,
      ownerId: "",
      projectId: "",
      environment: "",
      specVersion: SPEC_VERSION_CURRENT,
      isWebhook: false,
      isSystem: false,
    });

    // Sanity: the hook row exists but no hook_created event is in
    // the log yet.
    const preEvents = await events.list({
      runId: testRunId,
      pagination: {},
    });
    expect(preEvents.data.filter((e) => e.eventType === "hook_created").length).toBe(0);

    // Retry: must succeed and emit a hook_created event, NOT a
    // hook_conflict event, and NOT throw EntityConflictError.
    const result = await events.create(testRunId, {
      eventType: "hook_created",
      correlationId: hookId,
      eventData: { token },
    });

    expect(result.event?.eventType).toBe("hook_created");
    expect(result.hook?.hookId).toBe(hookId);

    const postEvents = await events.list({
      runId: testRunId,
      pagination: {},
    });
    const created = postEvents.data.filter(
      (e) => e.eventType === "hook_created" && e.correlationId === hookId,
    );
    const conflicts = postEvents.data.filter((e) => e.eventType === "hook_conflict");
    expect(created).toHaveLength(1);
    expect(conflicts).toHaveLength(0);
  });

  it("does not mutate an already-committed hook entity when a duplicate hook_created retry collides", async () => {
    // Parallel to the world-local regression for karthikscale3's
    // review on PR #2295. world-postgres uses
    // `.insert(Schema.hooks).onConflictDoNothing()` so a duplicate
    // hook_created retry's hook INSERT is a no-op against an
    // already-committed row — but this test guards against a
    // future regression that adds an UPDATE/UPSERT or otherwise
    // mutates the existing entity in the dedup path.
    const token = "no-mutate-on-duplicate-token-pg";
    const hookId = "hook_no_mutate_on_duplicate_pg";
    const originalMetadata = encode({ v: "a" }) as Uint8Array;
    const retryMetadata = encode({ v: "b" }) as Uint8Array;

    // First write: original metadata + isWebhook: true.
    const first = await events.create(testRunId, {
      eventType: "hook_created",
      correlationId: hookId,
      eventData: {
        token,
        metadata: originalMetadata,
        isWebhook: true,
      },
    });
    expect(first.event?.eventType).toBe("hook_created");
    expect(first.hook?.isWebhook).toBe(true);

    // Retry with DIFFERENT metadata and isWebhook.
    await expect(
      events.create(testRunId, {
        eventType: "hook_created",
        correlationId: hookId,
        eventData: {
          token,
          metadata: retryMetadata,
          isWebhook: false,
        },
      }),
    ).rejects.toMatchObject({ name: "EntityConflictError" });

    // The hook entity still has the ORIGINAL metadata and
    // isWebhook — the retry's payload did NOT overwrite the
    // already-committed entity.
    const persisted = await hooks.get(hookId);
    expect(persisted.isWebhook).toBe(true);
    // Compare metadata as bytes since cbor round-trips through
    // Buffer / Uint8Array.
    expect(Buffer.from(persisted.metadata as Uint8Array)).toEqual(Buffer.from(originalMetadata));

    // Exactly one hook_created event in the log.
    const evts = await events.list({
      runId: testRunId,
      pagination: { limit: 100 },
    });
    const hookCreated = evts.data.filter(
      (e) => e.eventType === "hook_created" && e.correlationId === hookId,
    );
    expect(hookCreated).toHaveLength(1);
  });

  it("converges same-hook creation across concurrent calls to one event", async () => {
    // Cross-worker convergence regression. The events table's
    // partial unique index
    // (workflow_events_entity_creation_unique on
    // tenantId+runId+correlationId+eventType for hook_created/
    // step_created/wait_created) makes the events INSERT the durable
    // convergence point — at most one `hook_created` event with
    // the same `(runId, correlationId)` can land. The dedup branch
    // can race with the original INSERT (both probe getHookByToken
    // before the loser sees the event), but the outer events
    // INSERT then raises 23505 (unique-violation) which is
    // translated to EntityConflictError that the runtime's
    // existing concurrent-replay catch path swallows. Net result:
    // exactly one `hook_created` event per logical creation.
    //
    // This test is the world-postgres counterpart to the
    // `converges same-hook creation across workers to one event`
    // test in world-local, exercising true in-process concurrency
    // since world-postgres has no per-process tag isolation.
    const attempts = 25;
    for (let i = 0; i < attempts; i++) {
      const correlationId = `hook_pg_converge_${i}`;
      const token = `token-pg-converge-${i}`;
      await Promise.allSettled([
        events.create(testRunId, {
          eventType: "hook_created",
          correlationId,
          eventData: { token },
        }),
        events.create(testRunId, {
          eventType: "hook_created",
          correlationId,
          eventData: { token },
        }),
      ]);
    }

    const evts = await events.list({
      runId: testRunId,
      pagination: { limit: 1000 },
    });
    const created = evts.data.filter((e) => e.eventType === "hook_created");
    const conflicts = evts.data.filter((e) => e.eventType === "hook_conflict");
    expect(created).toHaveLength(attempts);
    expect(conflicts).toHaveLength(0);
  });
});
