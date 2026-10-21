import { EntityConflictError, HookForceClaimedError } from "@workflow/errors";
import type { Event, EventResult, Hook } from "@workflow/world";
import { SPEC_VERSION_SUPPORTS_HOOK_FORCE_CLAIM } from "@workflow/world";
import { Pool, type PoolClient } from "pg";
import { ulid } from "ulid";
import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";
import { createClient } from "./drizzle/index.js";
import { dropTenantPartitions, ensureTenantPartitions, runMigrations } from "./index.js";
import { createEventsStorage, createHooksStorage } from "./storage.js";

/**
 * Hook force-claim: `createHook({ experimental_force: true })` takes a token
 * over from the run that holds it (`capabilities.hookForceClaim`, World spec 8).
 *
 * What these pin is the contract in `WorldCapabilities.hookForceClaim`: the
 * victim's `hook_disposed{forceClaimedBy}` lands before the token moves, the
 * claimer's hook records `claimedFrom`, its `hook_created` carries
 * `forceClaimedFrom`, a delivery that resolved the victim is answered with
 * `HookForceClaimedError` naming the token's current owner, and -- the one
 * order that corrupts a run -- no `hook_received` is ever journaled behind the
 * disposal that retired its hook. Plus the refusals: a running victim stamped
 * below 8 is not taken from, an unforced creation is the plain conflict it
 * always was, and a token is never taken across tenants.
 *
 * The race cases make their interleaving deterministic by holding a row lock
 * from a separate connection and releasing it only once the writers under test
 * are queued behind it (`waitForLockWaiters`). `vitest.config.ts` runs files
 * one at a time, so the lock waiters counted are this file's own.
 */
const testUrl = process.env.EVELAND_WORKFLOW_WORLD_TEST_URL;
const TENANT = "prj_hook_force_claim";
const OTHER_TENANT = "prj_hook_force_claim_other";

type EventsStorage = ReturnType<typeof createEventsStorage>;
type HooksStorage = ReturnType<typeof createHooksStorage>;
type CreateRequest = Parameters<EventsStorage["create"]>[1];

describe.skipIf(!testUrl)("hook force-claim", () => {
  let pool: Pool;
  let admin: Pool;
  let events: EventsStorage;
  let hooks: HooksStorage;
  let otherEvents: EventsStorage;
  let otherHooks: HooksStorage;

  beforeAll(async () => {
    // Several writers must be in flight at once for the race cases.
    pool = new Pool({ connectionString: testUrl, max: 8 });
    admin = new Pool({ connectionString: testUrl, max: 3 });
    await runMigrations(admin);
    await ensureTenantPartitions(admin, TENANT);
    await ensureTenantPartitions(admin, OTHER_TENANT);
    const drizzle = createClient(pool);
    events = createEventsStorage(drizzle, TENANT);
    hooks = createHooksStorage(drizzle, TENANT);
    otherEvents = createEventsStorage(drizzle, OTHER_TENANT);
    otherHooks = createHooksStorage(drizzle, OTHER_TENANT);
  }, 60_000);

  beforeEach(async () => {
    // Tenant-scoped, never TRUNCATE: the parents are partitioned and shared.
    for (const tenant of [TENANT, OTHER_TENANT]) {
      for (const table of [
        "workflow_events",
        "workflow_steps",
        "workflow_hooks",
        "workflow_waits",
        "workflow_runs",
      ]) {
        await admin.query(`delete from workflow.${table} where tenant_id = $1`, [tenant]);
      }
    }
  });

  afterAll(async () => {
    await dropTenantPartitions(admin, TENANT).catch(() => {});
    await dropTenantPartitions(admin, OTHER_TENANT).catch(() => {});
    await pool?.end().catch(() => {});
    await admin?.end().catch(() => {});
  });

  /** A started run, stamped `specVersion` (what the runtime sends on run_created). */
  async function startedRun(
    specVersion: number = SPEC_VERSION_SUPPORTS_HOOK_FORCE_CLAIM,
    storage: EventsStorage = events,
  ): Promise<string> {
    const runId = `wrun_${ulid()}`;
    const input = {
      deploymentId: "dpl_victim",
      workflowName: "wf//victim",
      input: new Uint8Array([1]),
    };
    await storage.create(runId, { eventType: "run_created", specVersion, eventData: input });
    await storage.create(runId, { eventType: "run_started", specVersion });
    return runId;
  }

  async function completeRun(runId: string): Promise<void> {
    await events.create(runId, {
      eventType: "run_completed",
      eventData: { output: new Uint8Array([2]) },
    } as CreateRequest);
  }

  function createHook(
    runId: string,
    hookId: string,
    token: string,
    extra: { force?: boolean; tokenRetentionUntil?: Date } = {},
    storage: EventsStorage = events,
  ): Promise<EventResult> {
    return storage.create(runId, {
      eventType: "hook_created",
      correlationId: hookId,
      eventData: { token, ...extra },
    } as CreateRequest);
  }

  function hookReceived(
    runId: string,
    hookId: string,
    token: string,
    storage: EventsStorage = events,
  ): Promise<EventResult> {
    return storage.create(runId, {
      eventType: "hook_received",
      correlationId: hookId,
      eventData: { token, payload: new Uint8Array([7]) },
    } as CreateRequest);
  }

  async function log(runId: string, storage: EventsStorage = events): Promise<Event[]> {
    const page = await storage.list({ runId, pagination: { limit: 1000 } });
    return page.data;
  }

  function data(event: Event | undefined): Record<string, any> {
    return ((event as { eventData?: unknown } | undefined)?.eventData ?? {}) as Record<string, any>;
  }

  /** The `hook_received` rows journaled after their hook's `hook_disposed`: must be empty. */
  function receivedAfterDisposal(entries: Event[]): Event[] {
    const disposedAt = new Map<string, number>();
    const late: Event[] = [];
    entries.forEach((event, index) => {
      if (event.eventType === "hook_disposed") disposedAt.set(event.correlationId!, index);
      if (event.eventType === "hook_received" && disposedAt.has(event.correlationId!)) {
        late.push(event);
      }
    });
    return late;
  }

  /** Holds a row lock from a separate connection until `release()` is called. */
  async function holdLock(sql: string, params: unknown[]): Promise<{ release(): Promise<void> }> {
    const client: PoolClient = await admin.connect();
    await client.query("begin");
    const { rowCount } = await client.query(sql, params);
    expect(rowCount).toBe(1);
    return {
      async release() {
        await client.query("commit");
        client.release();
      },
    };
  }

  function holdHookRow(tenantId: string, hookId: string) {
    return holdLock(
      "select 1 from workflow.workflow_hooks where tenant_id = $1 and hook_id = $2 for update",
      [tenantId, hookId],
    );
  }

  /** Resolves once at least `count` backends in this database are waiting on a lock. */
  async function waitForLockWaiters(count: number): Promise<void> {
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

  it("takes the token from a running spec-8 victim: disposal first, then the token, then the claim", async () => {
    const victim = await startedRun();
    const claimer = await startedRun();
    const token = `tok_${ulid()}`;
    await createHook(victim, "hook_victim", token);
    const before = await log(victim);

    const result = await createHook(claimer, "hook_claimer", token, { force: true });

    const claimedFrom = {
      runId: victim,
      hookId: "hook_victim",
      workflowName: "wf//victim",
      deploymentId: "dpl_victim",
      runSpecVersion: SPEC_VERSION_SUPPORTS_HOOK_FORCE_CLAIM,
    };
    expect(result.event?.eventType).toBe("hook_created");
    expect(data(result.event)).toMatchObject({ token, force: true, forceClaimedFrom: claimedFrom });
    expect(result.hook).toMatchObject({
      runId: claimer,
      hookId: "hook_claimer",
      token,
      claimedFrom,
    });

    // The victim's log gained exactly the involuntary disposal.
    const after = await log(victim);
    expect(after).toHaveLength(before.length + 1);
    const disposal = after.at(-1)!;
    expect(disposal).toMatchObject({ eventType: "hook_disposed", correlationId: "hook_victim" });
    expect(data(disposal)).toEqual({
      token,
      forceClaimedBy: { runId: claimer, hookId: "hook_claimer" },
    });

    // The claimer's log journals the forced creation with its provenance.
    const created = (await log(claimer)).filter((event) => event.eventType === "hook_created");
    expect(created).toHaveLength(1);
    expect(data(created[0])).toMatchObject({ force: true, forceClaimedFrom: claimedFrom });

    // The token is re-pointed, and every hook read carries `claimedFrom`.
    const byToken = await hooks.getByToken(token);
    expect(byToken).toMatchObject({ runId: claimer, hookId: "hook_claimer", claimedFrom });
    await expect(hooks.get("hook_claimer")).resolves.toMatchObject({ claimedFrom });
    const listed = await hooks.list({ runId: claimer });
    expect(listed.data.map((hook: Hook) => hook.claimedFrom)).toEqual([claimedFrom]);
    await expect(hooks.get("hook_victim")).rejects.toMatchObject({ name: "HookNotFoundError" });
    expect((await hooks.list({ runId: victim })).data).toEqual([]);
  });

  it("refuses a running victim stamped below 8 with a marked conflict, writing nothing", async () => {
    const victim = await startedRun(SPEC_VERSION_SUPPORTS_HOOK_FORCE_CLAIM - 1);
    const claimer = await startedRun();
    const token = `tok_${ulid()}`;
    await createHook(victim, "hook_victim", token);
    const before = await log(victim);

    const result = await createHook(claimer, "hook_claimer", token, { force: true });

    expect(result.event?.eventType).toBe("hook_conflict");
    expect(data(result.event)).toEqual({
      token,
      conflictingRunId: victim,
      forceRefusedReason: "victim-spec-version",
    });
    expect(result.hook).toBeUndefined();
    expect(await log(victim)).toEqual(before);
    await expect(hooks.getByToken(token)).resolves.toMatchObject({
      runId: victim,
      hookId: "hook_victim",
    });
    expect((await hooks.getByToken(token)).claimedFrom).toBeUndefined();
    await expect(hooks.get("hook_claimer")).rejects.toMatchObject({ name: "HookNotFoundError" });
    // The victim is untouched: it still receives.
    await expect(hookReceived(victim, "hook_victim", token)).resolves.toMatchObject({
      event: { eventType: "hook_received" },
    });
  });

  it("takes a finished victim's retained token at any spec version, with no disposal row", async () => {
    // Stamped 6 on purpose: a finished run has no reader to strand.
    const victim = await startedRun(6);
    const claimer = await startedRun();
    const token = `tok_${ulid()}`;
    await createHook(victim, "hook_victim", token, {
      tokenRetentionUntil: new Date(Date.now() + 60 * 60 * 1000),
    });
    await completeRun(victim);
    const before = await log(victim);

    // Unforced, a retained token is still held.
    await expect(createHook(claimer, "hook_plain", token)).resolves.toMatchObject({
      event: { eventType: "hook_conflict" },
    });

    const result = await createHook(claimer, "hook_claimer", token, { force: true });

    // No wake-targeting fields: there is nothing to wake.
    const claimedFrom = { runId: victim, hookId: "hook_victim" };
    expect(result.hook).toMatchObject({ runId: claimer, claimedFrom });
    expect(result.hook!.claimedFrom).toEqual(claimedFrom);
    expect(data(result.event).forceClaimedFrom).toEqual(claimedFrom);
    expect(await log(victim)).toEqual(before);
    await expect(hooks.getByToken(token)).resolves.toMatchObject({
      runId: claimer,
      hookId: "hook_claimer",
      claimedFrom,
    });
  });

  it("treats a finished owner whose retention lapsed as a free token, not a takeover", async () => {
    const victim = await startedRun();
    const claimer = await startedRun();
    const token = `tok_${ulid()}`;
    await createHook(victim, "hook_victim", token);
    await completeRun(victim);

    const result = await createHook(claimer, "hook_claimer", token, { force: true });

    expect(result.event?.eventType).toBe("hook_created");
    expect(result.hook?.claimedFrom).toBeUndefined();
    expect(data(result.event).forceClaimedFrom).toBeUndefined();
    expect((await log(victim)).some((event) => event.eventType === "hook_disposed")).toBe(false);
  });

  it("answers a delivery to a taken-over hook with HookForceClaimedError naming the current owner", async () => {
    const victim = await startedRun();
    const first = await startedRun();
    const second = await startedRun();
    const token = `tok_${ulid()}`;
    await createHook(victim, "hook_victim", token);
    await createHook(first, "hook_first", token, { force: true });

    const refused = await hookReceived(victim, "hook_victim", token).catch((error) => error);
    expect(HookForceClaimedError.is(refused)).toBe(true);
    expect(refused).toMatchObject({
      token,
      claimedByRunId: first,
      claimedByHookId: "hook_first",
    });

    // Along a chain v -> c1 -> c2, the victim's refusal names the END of it.
    await createHook(second, "hook_second", token, { force: true });
    const chained = await hookReceived(victim, "hook_victim", token).catch((error) => error);
    expect(HookForceClaimedError.is(chained)).toBe(true);
    expect(chained).toMatchObject({ claimedByRunId: second, claimedByHookId: "hook_second" });
    const middle = await hookReceived(first, "hook_first", token).catch((error) => error);
    expect(HookForceClaimedError.is(middle)).toBe(true);
    expect(middle).toMatchObject({ claimedByRunId: second, claimedByHookId: "hook_second" });

    // The redirect works without the request naming its token: the disposal row has it.
    const tokenless = await events
      .create(victim, {
        eventType: "hook_received",
        correlationId: "hook_victim",
        eventData: { payload: new Uint8Array([7]) },
      } as CreateRequest)
      .catch((error) => error);
    expect(tokenless).toMatchObject({ claimedByRunId: second });

    // The second claimer's provenance is the first claimer, which is still running.
    await expect(hooks.getByToken(token)).resolves.toMatchObject({
      runId: second,
      claimedFrom: { runId: first, hookId: "hook_first", workflowName: "wf//victim" },
    });
    // Each claimer's own disposal comes after its own creation.
    const firstLog = await log(first);
    const createdAt = firstLog.findIndex((event) => event.eventType === "hook_created");
    const disposedAt = firstLog.findIndex((event) => event.eventType === "hook_disposed");
    expect(createdAt).toBeGreaterThanOrEqual(0);
    expect(disposedAt).toBeGreaterThan(createdAt);
    // The new owner receives.
    await expect(hookReceived(second, "hook_second", token)).resolves.toMatchObject({
      event: { eventType: "hook_received" },
    });
    for (const runId of [victim, first]) {
      expect((await log(runId)).some((event) => event.eventType === "hook_received")).toBe(false);
    }
  });

  it("keeps a run's own disposal a plain not-found, not a redirect", async () => {
    const owner = await startedRun();
    const token = `tok_${ulid()}`;
    await createHook(owner, "hook_own", token);
    await events.create(owner, {
      eventType: "hook_disposed",
      correlationId: "hook_own",
    } as CreateRequest);

    const refused = await hookReceived(owner, "hook_own", token).catch((error) => error);
    expect(refused).toMatchObject({ name: "HookNotFoundError" });
    expect(HookForceClaimedError.is(refused)).toBe(false);
    // A later disposal finds nothing to dispose, and writes nothing.
    await expect(
      events.create(owner, {
        eventType: "hook_disposed",
        correlationId: "hook_own",
      } as CreateRequest),
    ).rejects.toMatchObject({ name: "HookNotFoundError" });
    expect((await log(owner)).filter((event) => event.eventType === "hook_disposed")).toHaveLength(
      1,
    );
  });

  it("orders a delivery racing the takeover after it, refused rather than journaled behind the disposal", async () => {
    const victim = await startedRun();
    const claimer = await startedRun();
    const token = `tok_${ulid()}`;
    await createHook(victim, "hook_victim", token);

    // The takeover locks the victim's run row and queues on the hook row. The
    // delivery then passes its unlocked existence check (the row is still
    // there) and queues on the run row behind the takeover. Releasing the hook
    // row lets the takeover commit first; the delivery's locked re-check must
    // then see the hook gone.
    const lock = await holdHookRow(TENANT, "hook_victim");
    const takeover = createHook(claimer, "hook_claimer", token, { force: true });
    await waitForLockWaiters(1);
    const delivery = hookReceived(victim, "hook_victim", token).then(
      (value) => ({ ok: true as const, value }),
      (error: unknown) => ({ ok: false as const, error }),
    );
    await waitForLockWaiters(2);
    await lock.release();

    await expect(takeover).resolves.toMatchObject({ event: { eventType: "hook_created" } });
    const outcome = await delivery;
    expect(outcome.ok).toBe(false);
    expect(HookForceClaimedError.is((outcome as { error: unknown }).error)).toBe(true);
    expect((outcome as { error: unknown }).error).toMatchObject({ claimedByRunId: claimer });
    const victimLog = await log(victim);
    expect(victimLog.some((event) => event.eventType === "hook_received")).toBe(false);
    expect(victimLog.at(-1)?.eventType).toBe("hook_disposed");
  });

  it("never journals a hook_received behind its disposal under free-running races", async () => {
    for (let round = 0; round < 6; round++) {
      const victim = await startedRun();
      const claimer = await startedRun();
      const token = `tok_${ulid()}`;
      // Hook ids are unique per tenant, as the runtime's are.
      const victimHook = `hook_victim_${String(round)}`;
      await createHook(victim, victimHook, token);

      const deliveries = Array.from({ length: 4 }, () =>
        hookReceived(victim, victimHook, token).then(
          () => "delivered" as const,
          (error: unknown) => {
            if (HookForceClaimedError.is(error)) return "redirected" as const;
            throw error;
          },
        ),
      );
      const takeover = createHook(claimer, `hook_claimer_${String(round)}`, token, { force: true });
      const outcomes = await Promise.all(deliveries);
      await expect(takeover).resolves.toMatchObject({ event: { eventType: "hook_created" } });

      const victimLog = await log(victim);
      expect(receivedAfterDisposal(victimLog)).toEqual([]);
      expect(victimLog.filter((event) => event.eventType === "hook_received")).toHaveLength(
        outcomes.filter((outcome) => outcome === "delivered").length,
      );
      expect(victimLog.at(-1)?.eventType).toBe("hook_disposed");
    }
  });

  it("chains two concurrent claimers instead of losing one", async () => {
    const victim = await startedRun();
    const first = await startedRun();
    const second = await startedRun();
    const token = `tok_${ulid()}`;
    await createHook(victim, "hook_victim", token);

    // Both read the victim as the owner. The first locks the victim's run and
    // queues on its hook row; the second queues on the victim's run behind it.
    // Once the first commits, the second must find the first's row under the
    // token and take it from THAT run.
    const lock = await holdHookRow(TENANT, "hook_victim");
    const firstClaim = createHook(first, "hook_first", token, { force: true });
    await waitForLockWaiters(1);
    const secondClaim = createHook(second, "hook_second", token, { force: true });
    await waitForLockWaiters(2);
    await lock.release();

    const [firstResult, secondResult] = await Promise.all([firstClaim, secondClaim]);
    expect(firstResult.hook?.claimedFrom).toMatchObject({ runId: victim, hookId: "hook_victim" });
    expect(secondResult.hook?.claimedFrom).toMatchObject({ runId: first, hookId: "hook_first" });
    await expect(hooks.getByToken(token)).resolves.toMatchObject({
      runId: second,
      hookId: "hook_second",
    });

    const victimDisposals = (await log(victim)).filter((e) => e.eventType === "hook_disposed");
    expect(victimDisposals.map((event) => data(event).forceClaimedBy)).toEqual([
      { runId: first, hookId: "hook_first" },
    ]);
    const firstLog = await log(first);
    expect(firstLog.map((event) => event.eventType).slice(-2)).toEqual([
      "hook_created",
      "hook_disposed",
    ]);
    expect(data(firstLog.at(-1)).forceClaimedBy).toEqual({ runId: second, hookId: "hook_second" });
    // Only one hook row holds the token.
    const { rows } = await admin.query(
      "select run_id from workflow.workflow_hooks where tenant_id = $1 and token = $2",
      [TENANT, token],
    );
    expect(rows).toEqual([{ run_id: second }]);
  });

  it("is idempotent across a replayed forced creation", async () => {
    const victim = await startedRun();
    const claimer = await startedRun();
    const token = `tok_${ulid()}`;
    await createHook(victim, "hook_victim", token);

    // Concurrent duplicates of one creation: one takes the token, the other is
    // the dedup conflict the runtime's replay path swallows.
    const settled = await Promise.allSettled([
      createHook(claimer, "hook_claimer", token, { force: true }),
      createHook(claimer, "hook_claimer", token, { force: true }),
    ]);
    expect(settled.filter((outcome) => outcome.status === "fulfilled")).toHaveLength(1);
    const rejected = settled.filter((outcome) => outcome.status === "rejected");
    expect(rejected).toHaveLength(1);
    expect((rejected[0] as PromiseRejectedResult).reason).toBeInstanceOf(EntityConflictError);

    // And a sequential replay after both settled.
    await expect(
      createHook(claimer, "hook_claimer", token, { force: true }),
    ).rejects.toBeInstanceOf(EntityConflictError);

    const victimLog = await log(victim);
    expect(victimLog.filter((event) => event.eventType === "hook_disposed")).toHaveLength(1);
    const claimerLog = await log(claimer);
    expect(claimerLog.filter((event) => event.eventType === "hook_created")).toHaveLength(1);
    expect(claimerLog.some((event) => event.eventType === "hook_conflict")).toBe(false);
    await expect(hooks.getByToken(token)).resolves.toMatchObject({ runId: claimer });
  });

  it("answers an unforced creation with the plain conflict it always was", async () => {
    const victim = await startedRun();
    const claimer = await startedRun();
    const token = `tok_${ulid()}`;
    await createHook(victim, "hook_victim", token);
    const before = await log(victim);

    const result = await createHook(claimer, "hook_claimer", token);

    expect(result.event?.eventType).toBe("hook_conflict");
    expect(data(result.event)).toEqual({ token, conflictingRunId: victim });
    expect(await log(victim)).toEqual(before);
    await expect(hooks.getByToken(token)).resolves.toMatchObject({ runId: victim });
  });

  it("lets a run stamped 6 claim from a spec-8 victim", async () => {
    // WORKFLOW_SEALED_LOG=0 runs can claim; they just cannot be claimed from.
    const victim = await startedRun();
    const claimer = await startedRun(6);
    const token = `tok_${ulid()}`;
    await createHook(victim, "hook_victim", token);

    const result = await createHook(claimer, "hook_claimer", token, { force: true });

    expect(result.hook).toMatchObject({ runId: claimer, claimedFrom: { runId: victim } });
  });

  it("never takes a token across tenants", async () => {
    const victim = await startedRun();
    const token = `tok_${ulid()}`;
    await createHook(victim, "hook_victim", token);
    const before = await log(victim);

    // Another tenant holding the same string is an ordinary, unrelated creation.
    const outsider = await startedRun(SPEC_VERSION_SUPPORTS_HOOK_FORCE_CLAIM, otherEvents);
    const result = await createHook(outsider, "hook_outsider", token, { force: true }, otherEvents);
    expect(result.event?.eventType).toBe("hook_created");
    expect(result.hook?.claimedFrom).toBeUndefined();
    await expect(otherHooks.getByToken(token)).resolves.toMatchObject({ runId: outsider });

    // Tenant A is untouched, and still receives.
    expect(await log(victim)).toEqual(before);
    await expect(hooks.getByToken(token)).resolves.toMatchObject({
      runId: victim,
      hookId: "hook_victim",
    });
    await expect(hookReceived(victim, "hook_victim", token)).resolves.toMatchObject({
      event: { eventType: "hook_received" },
    });

    // And the other direction: tenant B's later takeover inside B leaves A alone.
    const otherClaimer = await startedRun(SPEC_VERSION_SUPPORTS_HOOK_FORCE_CLAIM, otherEvents);
    const claimed = await createHook(
      otherClaimer,
      "hook_other_claimer",
      token,
      { force: true },
      otherEvents,
    );
    expect(claimed.hook?.claimedFrom).toMatchObject({ runId: outsider });
    await expect(hooks.getByToken(token)).resolves.toMatchObject({ runId: victim });
    // A redirect in B names B's owner, never A's.
    const refused = await hookReceived(outsider, "hook_outsider", token, otherEvents).catch(
      (error) => error,
    );
    expect(refused).toMatchObject({ claimedByRunId: otherClaimer });
  });
});
