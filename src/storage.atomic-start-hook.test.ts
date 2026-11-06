import { HookConflictError } from "@workflow/errors";
import {
  type AnyEventRequest,
  type EventResult,
  SPEC_VERSION_CURRENT,
  startHookId,
} from "@workflow/world";
import { atomicStartHookConformance } from "@workflow/world/test-support/atomic-start-hook-conformance.js";
import { Pool } from "pg";
import { ulid } from "ulid";
import { afterAll, beforeAll, describe, expect, test, vi } from "vitest";
import {
  createWorld,
  dropTenantPartitions,
  ensureTenantPartitions,
  runMigrations,
} from "./index.js";

/**
 * Atomic start Hook admission (`capabilities.atomicStartHook`), which eve
 * 1.0.0-rc.6 requires to start a session: checked with upstream's own
 * conformance module, plus what is particular to this World -- a tenant owns
 * its tokens, and a token of several kilobytes (eveland#521) is admitted and
 * refused like any other.
 *
 * Two worlds over one database play the direct `run_created` and the queued
 * `run_started` of a start, as two Deployment processes would. The orphan
 * bound is crossed in virtual time: admission and the run id check read the
 * JS clock, which the suite moves (only `Date` is faked, and it keeps running
 * with real time). Hook retention is compared in SQL, so those cases wait real
 * seconds, which is why the timeout is raised.
 */
const testUrl = process.env.EVELAND_WORKFLOW_WORLD_TEST_URL;
const TENANT = "prj_atomic_start_hook";
const OTHER_TENANT = "prj_atomic_start_hook_other";

type World = ReturnType<typeof createWorld>;

let pool: Pool | undefined;
let world: World | undefined;
let secondWorld: World | undefined;
let otherTenantWorld: World | undefined;

vi.setConfig({ testTimeout: 60_000 });
const realNow = () => performance.timeOrigin + performance.now();

describe.skipIf(!testUrl)("atomic start Hook", () => {
  beforeAll(async () => {
    pool = new Pool({ connectionString: testUrl, max: 16 });
    await runMigrations(pool);
    await ensureTenantPartitions(pool, TENANT);
    await ensureTenantPartitions(pool, OTHER_TENANT);
    const options = {
      pool,
      tenantId: TENANT,
      deploymentId: "dep_start_hook",
      runner: "external" as const,
    };
    world = createWorld(options);
    secondWorld = createWorld(options);
    otherTenantWorld = createWorld({ ...options, tenantId: OTHER_TENANT });
    vi.useFakeTimers({ toFake: ["Date"], shouldAdvanceTime: true });
  }, 60_000);

  afterAll(async () => {
    vi.useRealTimers();
    await world?.close?.();
    await secondWorld?.close?.();
    await otherTenantWorld?.close?.();
    if (pool) {
      await dropTenantPartitions(pool, TENANT).catch(() => {});
      await dropTenantPartitions(pool, OTHER_TENANT).catch(() => {});
    }
    await pool?.end();
  });

  atomicStartHookConformance({
    name: "@evelandhq/workflow-world",
    storage: () => world!,
    capabilities: () => world!.capabilities,
    newRunId: () => `wrun_${ulid()}`,
    secondEvents: () => secondWorld!.events,
    advanceClock: (ms) => {
      vi.setSystemTime(ms === 0 ? realNow() : Date.now() + ms);
    },
  });

  const admit = (target: World, token: string) => {
    const runId = `wrun_${ulid()}`;
    // `create` without its overloads, as the conformance module calls it.
    const create = target.events.create as (
      runId: string,
      data: AnyEventRequest,
    ) => Promise<EventResult>;
    return create(runId, {
      eventType: "run_created",
      specVersion: SPEC_VERSION_CURRENT,
      eventData: {
        deploymentId: "dep_start_hook",
        workflowName: "startHookTenancy",
        input: new Uint8Array([1]),
        startHook: { token },
      },
    } as AnyEventRequest).then((result) => ({ runId, result }));
  };

  test("a token is owned per tenant", async () => {
    const token = `tenancy:${ulid()}`;
    const mine = await admit(world!, token);
    const theirs = await admit(otherTenantWorld!, token);
    expect(mine.result.run?.startHook).toMatchObject({ token, hookId: startHookId(mine.runId) });
    expect(theirs.result.run?.startHook).toMatchObject({
      token,
      hookId: startHookId(theirs.runId),
    });
    expect((await world!.hooks.getByToken(token)).runId).toBe(mine.runId);
    expect((await otherTenantWorld!.hooks.getByToken(token)).runId).toBe(theirs.runId);
  });

  test("a token of several kilobytes is admitted once and refused after", async () => {
    // eve builds some tokens from model-issued tool-call ids, which a provider
    // may pad with a multi-kilobyte reasoning signature (eveland#521); the
    // claim index keys on a digest so such a token still fits.
    const token = `long:${"x".repeat(6_000)}:${ulid()}`;
    const owner = await admit(world!, token);
    expect(owner.result.run?.runId).toBe(owner.runId);
    const refused = await admit(world!, token).then(
      () => undefined,
      (error: unknown) => error,
    );
    expect(HookConflictError.is(refused)).toBe(true);
    expect(refused).toMatchObject({ conflictingRunId: owner.runId });
  });
});
