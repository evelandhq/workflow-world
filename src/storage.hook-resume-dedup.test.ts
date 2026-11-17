import { createHash } from "node:crypto";
import { readFile, readdir } from "node:fs/promises";
import path from "node:path";
import {
  HookForceClaimedError,
  HookNotFoundError,
  RunExpiredError,
  WorkflowRunNotFoundError,
  WorkflowWorldError,
} from "@workflow/errors";
import type { CreateEventParams, Event, EventResult } from "@workflow/world";
import { RETENTION_ATTRIBUTE, RETENTION_ZERO } from "@workflow/world";
import { Pool } from "pg";
import { monotonicFactory } from "ulid";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { createClient } from "./drizzle/index.js";
import {
  dropTenantPartitions,
  ensureTenantPartitions,
  resolveMigrationsDir,
  runMigrations,
} from "./migrate.js";
import { createEventsStorage } from "./storage.js";
import { dedupIndexName, derivePartitionName, resumeDedupIndexName } from "./tenant.js";

/**
 * Hook resume deduplication (`capabilities.hookResumeDedup`): a `hook_received`
 * carrying `resumeHook()`'s `resumeId` is written once per `(run, resumeId)`,
 * and its retry is answered with the committed event, as upstream
 * world-postgres 5.2.1's `createHookResume` does. Plus the schema it needs
 * (migration 0002): the partial unique index on every tenant partition, under
 * the name provisioning derives.
 *
 * Set `WORKFLOW_WORLD_TEST_URL` to a scratch database to run it.
 */
const testUrl = process.env.WORKFLOW_WORLD_TEST_URL;
const suffix = `${String(process.pid)}${Date.now().toString(36)}`;
const TENANT = `p_resume_${suffix}`;
const OTHER = `p_resume_other_${suffix}`;
const ulid = monotonicFactory();

type EventsStorage = ReturnType<typeof createEventsStorage>;
type CreateRequest = Parameters<EventsStorage["create"]>[1];

const sha256 = (bytes: Uint8Array) => createHash("sha256").update(bytes).digest("hex");

async function waitForLockWaiters(admin: Pool, count: number): Promise<void> {
  const deadline = Date.now() + 10_000;
  for (;;) {
    const { rows } = await admin.query<{ waiting: number }>(
      `select count(*)::int as waiting from pg_stat_activity
        where datname = current_database() and wait_event_type = 'Lock'`,
    );
    if (rows[0]!.waiting >= count) return;
    if (Date.now() > deadline) {
      throw new Error(`expected ${String(count)} lock waiters, saw ${String(rows[0]!.waiting)}`);
    }
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
}

/** The name of the child index of `parent` on a `workflow_events` partition. */
async function childIndexName(
  admin: Pool,
  partition: string,
  parent: string,
): Promise<string | undefined> {
  const { rows } = await admin.query<{ relname: string }>(
    `select ci.relname
       from pg_inherits inh
       join pg_class ci on ci.oid = inh.inhrelid
       join pg_index i on i.indexrelid = ci.oid
       join pg_class ct on ct.oid = i.indrelid
      where ct.relname = $1 and inh.inhparent = to_regclass($2)`,
    [partition, `workflow.${parent}`],
  );
  return rows[0]?.relname;
}

function isWorldError(error: unknown, status: number, code?: string): boolean {
  return (
    WorkflowWorldError.is(error) &&
    error.status === status &&
    (code === undefined || error.code === code)
  );
}

async function rejectionOf(promise: Promise<unknown>): Promise<unknown> {
  return promise.then(
    () => {
      throw new Error("expected a rejection");
    },
    (error: unknown) => error,
  );
}

describe.skipIf(!testUrl)("hook resume dedup", () => {
  let pool: Pool;
  let admin: Pool;
  let events: EventsStorage;
  let otherEvents: EventsStorage;

  beforeAll(async () => {
    pool = new Pool({ connectionString: testUrl, max: 8 });
    admin = new Pool({ connectionString: testUrl, max: 3 });
    await runMigrations(admin, { migrationsDir: resolveMigrationsDir() });
    await ensureTenantPartitions(admin, TENANT);
    await ensureTenantPartitions(admin, OTHER);
    const drizzle = createClient(pool);
    events = createEventsStorage(drizzle, TENANT);
    otherEvents = createEventsStorage(drizzle, OTHER);
  }, 60_000);

  afterAll(async () => {
    for (const tenant of [TENANT, OTHER]) {
      for (const table of [
        "workflow_event_slots",
        "workflow_steps",
        "workflow_hooks",
        "workflow_waits",
        "workflow_runs",
      ]) {
        await admin
          ?.query(`delete from workflow.${table} where tenant_id = $1`, [tenant])
          .catch(() => {});
      }
      await dropTenantPartitions(admin, tenant).catch(() => {});
    }
    await pool?.end().catch(() => {});
    await admin?.end().catch(() => {});
  });

  async function startedRun(
    storage: EventsStorage = events,
    options: { runId?: string; attributes?: Record<string, string> } = {},
  ): Promise<string> {
    const runId = options.runId ?? `wrun_${ulid()}`;
    await storage.create(runId, {
      eventType: "run_created",
      eventData: {
        deploymentId: "dpl_resume",
        workflowName: "wf//resume",
        input: new Uint8Array([1]),
        ...(options.attributes
          ? { attributes: options.attributes, allowReservedAttributes: true }
          : {}),
      },
    } as unknown as CreateRequest);
    await storage.create(runId, { eventType: "run_started" } as CreateRequest);
    return runId;
  }

  async function createHook(
    runId: string,
    hookId: string,
    token: string,
    extra: { force?: boolean } = {},
    storage: EventsStorage = events,
  ): Promise<void> {
    await storage.create(runId, {
      eventType: "hook_created",
      correlationId: hookId,
      eventData: { token, ...extra },
    } as CreateRequest);
  }

  /** A hooked running run. */
  async function hookedRun(storage: EventsStorage = events, runId?: string) {
    const id = await startedRun(storage, runId ? { runId } : {});
    const hookId = `whk_${ulid()}`;
    const token = `tok_${ulid()}`;
    await createHook(id, hookId, token, {}, storage);
    return { runId: id, hookId, token };
  }

  function resume(
    target: { runId: string; hookId: string; token: string },
    resumeId: string | undefined,
    payload: Uint8Array = new Uint8Array([7, 7]),
    options: { storage?: EventsStorage; params?: CreateEventParams; digest?: string } = {},
  ): Promise<EventResult> {
    const storage = options.storage ?? events;
    return storage.create(
      target.runId,
      {
        eventType: "hook_received",
        correlationId: target.hookId,
        eventData: { token: target.token, payload },
      } as CreateRequest,
      {
        ...(resumeId === undefined
          ? {}
          : { resumeId, resumePayloadDigest: options.digest ?? sha256(payload) }),
        ...options.params,
      },
    );
  }

  async function received(runId: string, storage: EventsStorage = events): Promise<Event[]> {
    const page = await storage.list({ runId, pagination: { limit: 1000 } });
    return page.data.filter((event) => event.eventType === "hook_received");
  }

  it("answers a retry with the same id and payload with the one committed event", async () => {
    const target = await hookedRun();
    const resumeId = ulid();

    const first = await resume(target, resumeId);
    const retry = await resume(target, resumeId);

    expect(first.event?.resumeId).toBe(resumeId);
    expect(retry.event).toEqual(first.event);
    const log = await received(target.runId);
    expect(log).toHaveLength(1);
    // Read back with its key: the legacy consumer and the live feed match on it.
    expect(log[0]).toMatchObject({ eventId: first.event!.eventId, resumeId });
    expect(await events.get(target.runId, first.event!.eventId)).toMatchObject({ resumeId });
    expect((log[0] as { eventData?: { payload?: unknown } }).eventData?.payload).toEqual(
      new Uint8Array([7, 7]),
    );
  });

  it("refuses the same id with a different payload, hook or token, and writes nothing", async () => {
    const target = await hookedRun();
    const resumeId = ulid();
    const first = await resume(target, resumeId);

    const otherPayload = await rejectionOf(resume(target, resumeId, new Uint8Array([8])));
    expect(isWorldError(otherPayload, 422)).toBe(true);
    expect((otherPayload as Error).message).toMatch(/reused with different contents/);

    const second = `whk_${ulid()}`;
    await createHook(target.runId, second, `tok_${ulid()}`);
    const otherHook = await rejectionOf(resume({ ...target, hookId: second }, resumeId));
    expect(isWorldError(otherHook, 422)).toBe(true);

    expect(await received(target.runId)).toEqual([first.event]);
  });

  it("refuses a digest that does not match the payload before writing", async () => {
    const target = await hookedRun();
    const error = await rejectionOf(
      resume(target, ulid(), new Uint8Array([1]), { digest: sha256(new Uint8Array([2])) }),
    );
    expect(isWorldError(error, 422)).toBe(true);
    expect((error as Error).message).toMatch(/digest does not match/);
    expect(await received(target.runId)).toEqual([]);
  });

  it("computes the digest itself when the write carries none", async () => {
    const target = await hookedRun();
    const resumeId = ulid();
    const created = await events.create(
      target.runId,
      {
        eventType: "hook_received",
        correlationId: target.hookId,
        eventData: { token: target.token, payload: new Uint8Array([3]) },
      } as CreateRequest,
      { resumeId },
    );
    // A retry that does send the digest converges on the same event.
    expect((await resume(target, resumeId, new Uint8Array([3]))).event).toEqual(created.event);
    const { rows } = await admin.query<{ resume_payload_digest: string }>(
      `select resume_payload_digest from workflow.workflow_events
        where tenant_id = $1 and run_id = $2 and type = 'hook_received'`,
      [TENANT, target.runId],
    );
    expect(rows).toEqual([{ resume_payload_digest: sha256(new Uint8Array([3])) }]);
  });

  it("answers a retry after the hook was disposed, and refuses a new resume", async () => {
    const target = await hookedRun();
    const resumeId = ulid();
    const first = await resume(target, resumeId);
    await events.create(target.runId, {
      eventType: "hook_disposed",
      correlationId: target.hookId,
      eventData: { token: target.token },
    } as CreateRequest);

    expect((await resume(target, resumeId)).event).toEqual(first.event);
    expect(HookNotFoundError.is(await rejectionOf(resume(target, ulid())))).toBe(true);
    expect(await received(target.runId)).toHaveLength(1);
  });

  it("answers a retry after the run completed, and refuses a new resume", async () => {
    const target = await hookedRun();
    const resumeId = ulid();
    const first = await resume(target, resumeId);
    await events.create(target.runId, {
      eventType: "run_completed",
      eventData: { output: new Uint8Array([2]) },
    } as CreateRequest);

    expect((await resume(target, resumeId)).event).toEqual(first.event);
    expect(RunExpiredError.is(await rejectionOf(resume(target, ulid())))).toBe(true);
    expect(await received(target.runId)).toHaveLength(1);
  });

  it("collapses concurrent writes of one id onto one event", async () => {
    const target = await hookedRun();
    const resumeId = ulid();
    // Hold the run row so both writers queue on its lock, then let them race.
    const lock = await admin.connect();
    try {
      await lock.query("begin");
      await lock.query(
        "select 1 from workflow.workflow_runs where tenant_id = $1 and id = $2 for update",
        [TENANT, target.runId],
      );
      const writes = [resume(target, resumeId), resume(target, resumeId)];
      await waitForLockWaiters(admin, 2);
      await lock.query("commit");
      const [a, b] = await Promise.all(writes);
      expect(a!.event).toEqual(b!.event);
    } finally {
      lock.release();
    }
    // And without a held lock, several at once.
    const burstId = ulid();
    const burst = await Promise.all(Array.from({ length: 6 }, () => resume(target, burstId)));
    expect(new Set(burst.map((result) => result.event!.eventId)).size).toBe(1);

    const log = await received(target.runId);
    expect(log.map((event) => event.resumeId).sort()).toEqual([resumeId, burstId].sort());
  });

  it("leaves a write without a resumeId as it was: every write appends", async () => {
    const target = await hookedRun();
    const a = await resume(target, undefined);
    const b = await resume(target, undefined);
    expect(a.event!.eventId).not.toBe(b.event!.eventId);
    const log = await received(target.runId);
    expect(log).toHaveLength(2);
    expect(log.every((event) => event.resumeId === undefined)).toBe(true);
    const { rows } = await admin.query<{ n: number }>(
      `select count(*)::int as n from workflow.workflow_events
        where tenant_id = $1 and run_id = $2
          and (resume_id is not null or resume_payload_digest is not null)`,
      [TENANT, target.runId],
    );
    expect(rows[0]!.n).toBe(0);
  });

  it("returns the log delta after sinceCursor with the event", async () => {
    const target = await hookedRun();
    const before = await events.list({ runId: target.runId, pagination: { limit: 1000 } });
    const resumeId = ulid();
    const result = await resume(target, resumeId, undefined, {
      params: { sinceCursor: before.cursor! },
    });
    expect(result.events?.map((event) => event.eventId)).toEqual([result.event!.eventId]);
    expect(result.hasMore).toBe(false);
    const retry = await resume(target, resumeId, undefined, {
      params: { sinceCursor: before.cursor! },
    });
    expect(retry.events).toEqual(result.events);
  });

  it("refuses a hook of another run or under another token, as upstream does", async () => {
    const target = await hookedRun();
    const elsewhere = await startedRun();
    const wrongRun = await rejectionOf(resume({ ...target, runId: elsewhere }, ulid()));
    expect(HookNotFoundError.is(wrongRun)).toBe(true);
    const wrongToken = await rejectionOf(resume({ ...target, token: `tok_${ulid()}` }, ulid()));
    expect(HookNotFoundError.is(wrongToken)).toBe(true);
    expect(await received(target.runId)).toEqual([]);
    expect(await received(elsewhere)).toEqual([]);
  });

  it("redirects a resume of a force-claimed hook to the token's new owner", async () => {
    const victim = await hookedRun();
    const claimer = await startedRun();
    await createHook(claimer, "hook_claimer", victim.token, { force: true });
    const error = await rejectionOf(resume(victim, ulid()));
    expect(HookForceClaimedError.is(error)).toBe(true);
    expect(error).toMatchObject({ claimedByRunId: claimer });
    expect(await received(victim.runId)).toEqual([]);
  });

  it("keeps resume ids per tenant", async () => {
    const runId = `wrun_${ulid()}`;
    const mine = await hookedRun(events, runId);
    const theirs = await hookedRun(otherEvents, runId);
    const resumeId = ulid();

    const a = await resume(mine, resumeId);
    const b = await resume({ ...theirs }, resumeId, undefined, { storage: otherEvents });
    expect(a.event!.runId).toBe(b.event!.runId);
    expect((await received(runId)).map((event) => event.resumeId)).toEqual([resumeId]);
    expect((await received(runId, otherEvents)).map((event) => event.resumeId)).toEqual([resumeId]);

    // A tenant cannot answer, or write, a resume on another tenant's run.
    const crossRun = await startedRun();
    const crossHook = `whk_${ulid()}`;
    await createHook(crossRun, crossHook, `tok_${ulid()}`);
    const cross = await rejectionOf(
      resume({ runId: crossRun, hookId: crossHook, token: "x" }, resumeId, undefined, {
        storage: otherEvents,
      }),
    );
    expect(WorkflowRunNotFoundError.is(cross)).toBe(true);
  });

  it("answers 410 on a $retention: 0 run, whose purge cleared the digest", async () => {
    const runId = await startedRun(events, {
      attributes: { [RETENTION_ATTRIBUTE]: RETENTION_ZERO },
    });
    const hookId = `whk_${ulid()}`;
    const token = `tok_${ulid()}`;
    await createHook(runId, hookId, token);
    const target = { runId, hookId, token };
    const resumeId = ulid();
    const first = await resume(target, resumeId);
    await events.create(runId, {
      eventType: "run_completed",
      eventData: { output: new Uint8Array([2]) },
    } as CreateRequest);

    const { rows } = await admin.query<{
      resume_id: string | null;
      digest: string | null;
      payload: boolean;
    }>(
      `select resume_id, resume_payload_digest as digest, payload_cbor is not null as payload
         from workflow.workflow_events
        where tenant_id = $1 and run_id = $2 and type = 'hook_received'`,
      [TENANT, runId],
    );
    expect(rows).toEqual([{ resume_id: resumeId, digest: null, payload: false }]);

    for (const id of [resumeId, ulid()]) {
      const error = await rejectionOf(resume(target, id));
      expect(isWorldError(error, 410, "INVOCATION_DATA_EXPIRED")).toBe(true);
    }
    // The purged event still reads back as its envelope, key included.
    const log = await received(runId);
    expect(log).toHaveLength(1);
    expect(log[0]).toMatchObject({ eventId: first.event!.eventId, resumeId });
    expect((log[0] as { eventData?: unknown }).eventData).toBeUndefined();
  });
});

describe.skipIf(!testUrl)("the hook resume index", () => {
  let admin: Pool;
  const SEEDED = `p_resume_seed_${suffix}`;
  const FRESH = `p_resume_fresh_${suffix}`;
  const PARENT = "workflow_events_hook_resume_unique";

  beforeAll(async () => {
    admin = new Pool({ connectionString: testUrl, max: 2 });
    await runMigrations(admin, { migrationsDir: resolveMigrationsDir() });
  }, 60_000);

  afterAll(async () => {
    for (const tenant of [SEEDED, FRESH]) {
      for (const table of ["workflow_event_slots", "workflow_hooks", "workflow_runs"]) {
        await admin
          ?.query(`delete from workflow.${table} where tenant_id = $1`, [tenant])
          .catch(() => {});
      }
      await dropTenantPartitions(admin, tenant).catch(() => {});
    }
    await admin?.end().catch(() => {});
  });

  it("migration 0002 applies to a database with tenants and events, naming every child", async () => {
    await ensureTenantPartitions(admin, SEEDED);
    const seeded = createEventsStorage(createClient(admin), SEEDED);
    const runId = `wrun_${ulid()}`;
    await seeded.create(runId, {
      eventType: "run_created",
      eventData: { deploymentId: "dpl", workflowName: "wf", input: new Uint8Array([1]) },
    } as unknown as CreateRequest);
    await seeded.create(runId, {
      eventType: "hook_created",
      correlationId: "whk_seed",
      eventData: { token: `tok_${ulid()}` },
    } as CreateRequest);

    const dir = resolveMigrationsDir();
    const file = (await readdir(dir)).find((name) => name.startsWith("0002_"))!;
    const migration = await readFile(path.join(dir, file), "utf8");

    // Back to the schema before 0002, apply it, check, and roll all of it back.
    const client = await admin.connect();
    try {
      await client.query("begin");
      await client.query(`drop index workflow.${PARENT}`);
      await client.query(
        "alter table workflow.workflow_events drop column resume_id, drop column resume_payload_digest",
      );
      await client.query(migration);

      const { rows: children } = await client.query<{ relname: string; bound: string }>(
        `select ci.relname, pg_get_expr(ct.relpartbound, ct.oid) as bound
           from pg_inherits inh
           join pg_class ci on ci.oid = inh.inhrelid
           join pg_index i on i.indexrelid = ci.oid
           join pg_class ct on ct.oid = i.indrelid
          where inh.inhparent = to_regclass($1)`,
        [`workflow.${PARENT}`],
      );
      const { rows: partitions } = await client.query<{ n: number }>(
        `select count(*)::int as n from pg_inherits
          where inhparent = 'workflow.workflow_events'::regclass`,
      );
      expect(children.length).toBe(partitions[0]!.n);
      expect(children.length).toBeGreaterThan(0);
      for (const child of children) {
        const tenant = /^FOR VALUES IN \('(.+)'\)$/.exec(child.bound)![1]!;
        expect(child.relname, tenant).toBe(resumeDedupIndexName(tenant));
      }

      const { rows: kept } = await client.query<{ type: string; resume_id: string | null }>(
        `select type, resume_id from workflow.workflow_events
          where tenant_id = $1 and run_id = $2 order by id`,
        [SEEDED, runId],
      );
      expect(kept).toEqual([
        { type: "run_created", resume_id: null },
        { type: "hook_created", resume_id: null },
      ]);

      const violation = await rejectionOf(
        client.query(
          `insert into workflow.workflow_events (tenant_id, id, type, run_id, resume_id)
           values ($1, 'evnt_x1', 'hook_received', $2, 'r1'),
                  ($1, 'evnt_x2', 'hook_received', $2, 'r1')`,
          [SEEDED, runId],
        ),
      );
      expect(violation).toMatchObject({ code: "23505", constraint: resumeDedupIndexName(SEEDED) });
    } finally {
      await client.query("rollback").catch(() => {});
      client.release();
    }
    expect(
      await childIndexName(admin, derivePartitionName("workflow_events", SEEDED), PARENT),
    ).toBe(resumeDedupIndexName(SEEDED));
  });

  it("gives a newly provisioned tenant both child indexes under their derived names", async () => {
    await ensureTenantPartitions(admin, FRESH);
    const partition = derivePartitionName("workflow_events", FRESH);
    expect(await childIndexName(admin, partition, PARENT)).toBe(resumeDedupIndexName(FRESH));
    expect(await childIndexName(admin, partition, "workflow_events_entity_creation_unique")).toBe(
      dedupIndexName(FRESH),
    );

    // Provisioning older than 0002 could have given the resume child the dedup
    // name; provisioning again repairs both.
    await admin.query(
      `alter index workflow."${dedupIndexName(FRESH)}" rename to "${FRESH}_tmp_dedup"`,
    );
    await admin.query(
      `alter index workflow."${resumeDedupIndexName(FRESH)}" rename to "${dedupIndexName(FRESH)}"`,
    );
    await ensureTenantPartitions(admin, FRESH);
    expect(await childIndexName(admin, partition, PARENT)).toBe(resumeDedupIndexName(FRESH));
    expect(await childIndexName(admin, partition, "workflow_events_entity_creation_unique")).toBe(
      dedupIndexName(FRESH),
    );
  });
});
