import { randomBytes } from "node:crypto";
import { Pool } from "pg";
import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";
import { createClient } from "./drizzle/index.js";
import { dropTenantPartitions, ensureTenantPartitions, runMigrations } from "./index.js";
import { createEventsStorage, createHooksStorage } from "./storage.js";
import { ulid } from "ulid";

/**
 * Hook tokens of arbitrary length (eveland#521).
 *
 * eve mints a subagent's continuation token as `subagent:<session>:<callId>`,
 * and the call id is whatever the model provider returned. Providers that encode
 * a reasoning signature into the id hand back several kilobytes, which the
 * original `(tenant_id, token)` btree refused outright: "index row size 3704
 * exceeds btree version 4 maximum 2704". The child run then failed at hook
 * registration on every delivery, the parent saw "Hook not found" after 30 s, and
 * the dispatcher kept retrying a run that could never start.
 *
 * The btree limit applies to the entry after compression, so a repetitive token
 * would pass and prove nothing; the tokens here are random bytes.
 */
const testUrl = process.env.EVELAND_WORKFLOW_WORLD_TEST_URL;
const TENANT = "prj_long_hook_token";

type EventsStorage = ReturnType<typeof createEventsStorage>;

function longToken(bytes = 3000): string {
  return `subagent:wrun_${ulid()}:call_${ulid()}__thought__${randomBytes(bytes).toString("base64")}`;
}

describe.skipIf(!testUrl)("hook tokens longer than a btree entry", () => {
  let pool: Pool;
  let events: EventsStorage;
  let hooks: ReturnType<typeof createHooksStorage>;

  beforeAll(async () => {
    pool = new Pool({ connectionString: testUrl, max: 2 });
    await runMigrations(pool);
    await ensureTenantPartitions(pool, TENANT);
    const drizzle = createClient(pool);
    events = createEventsStorage(drizzle, TENANT);
    hooks = createHooksStorage(drizzle, TENANT);
  }, 60_000);

  beforeEach(async () => {
    for (const table of ["workflow_events", "workflow_steps", "workflow_hooks", "workflow_waits"]) {
      await pool.query(`delete from workflow.${table} where tenant_id = $1`, [TENANT]);
    }
    await pool.query("delete from workflow.workflow_runs where tenant_id = $1", [TENANT]);
  });

  afterAll(async () => {
    await dropTenantPartitions(pool, TENANT).catch(() => {});
    await pool?.end().catch(() => {});
  });

  const runData = {
    deploymentId: "deployment-long-token",
    workflowName: "long-token-workflow",
    input: new Uint8Array([1]),
  };

  async function startRun(): Promise<string> {
    const runId = `wrun_${ulid()}`;
    await events.create(runId, { eventType: "run_created", eventData: runData });
    await events.create(runId, { eventType: "run_started", eventData: runData });
    return runId;
  }

  async function createHook(runId: string, token: string): Promise<string> {
    const hookId = `whk_${ulid()}`;
    await events.create(runId, {
      eventType: "hook_created",
      correlationId: hookId,
      eventData: { token },
    } as Parameters<EventsStorage["create"]>[1]);
    return hookId;
  }

  it("registers and resolves a token several times the btree row limit", async () => {
    const runId = await startRun();
    const token = longToken(4500);
    expect(token.length).toBeGreaterThan(2704 * 2);

    const hookId = await createHook(runId, token);

    await expect(hooks.getByToken(token)).resolves.toMatchObject({ hookId, runId });
  });

  it("matches on the whole token, not only its digest", async () => {
    const runId = await startRun();
    const base = longToken(3000);
    const sibling = `${base.slice(0, -1)}${base.endsWith("A") ? "B" : "A"}`;
    const hookId = await createHook(runId, base);
    const siblingId = await createHook(await startRun(), sibling);

    await expect(hooks.getByToken(base)).resolves.toMatchObject({ hookId });
    await expect(hooks.getByToken(sibling)).resolves.toMatchObject({ hookId: siblingId });
    await expect(hooks.getByToken(`${base}x`)).rejects.toMatchObject({ name: "HookNotFoundError" });
  });

  it("reports a same-token conflict for a long token instead of a database error", async () => {
    const first = await startRun();
    const token = longToken(3000);
    await createHook(first, token);

    const second = await startRun();
    const result = await events.create(second, {
      eventType: "hook_created",
      correlationId: `whk_${ulid()}`,
      eventData: { token },
    } as Parameters<EventsStorage["create"]>[1]);

    // The conflict path reads the existing row back by token; it must go through
    // the same digest-aware lookup or the long token surfaces as a failed query.
    expect(result.hook).toBeUndefined();
    expect(result.event?.eventType).toBe("hook_conflict");
    const conflict = result.event as { eventData?: { conflictingRunId?: string } } | undefined;
    expect(conflict?.eventData?.conflictingRunId).toBe(first);
  });

  it("replaced the plain token index with the md5 expression index", async () => {
    const { rows } = await pool.query<{ indexname: string; indexdef: string }>(
      `select indexname, indexdef from pg_indexes
        where schemaname = 'workflow' and tablename = 'workflow_hooks'
        order by indexname`,
    );
    const names = rows.map((row) => row.indexname);
    expect(names).not.toContain("workflow_hooks_tenant_token_index");
    const md5Index = rows.find((row) => row.indexname === "workflow_hooks_tenant_token_md5_index");
    expect(md5Index?.indexdef).toMatch(/\(tenant_id, md5\(\(token\)::text\)\)/);
  });
});
