import type { AnyEventRequest, Storage } from "@workflow/world";
import { Pool } from "pg";
import { afterAll, beforeAll, describe, expect, test } from "vitest";
import { createClient } from "./drizzle/index.js";
import {
  dropTenantPartitions,
  ensureTenantPartitions,
  resolveMigrationsDir,
  runMigrations,
} from "./migrate.js";
import { createEventsStorage } from "./storage.js";

const testUrl = process.env.EVELAND_WORKFLOW_WORLD_TEST_URL;
const suffix = `${String(process.pid)}${Date.now().toString(36)}`;
const TENANT = `p_run_retention_${suffix}`;

describe.skipIf(!testUrl)("run retention deadlines", () => {
  let pool: Pool;
  let events: Storage["events"];

  beforeAll(async () => {
    pool = new Pool({ connectionString: testUrl, max: 2 });
    await runMigrations(pool, { migrationsDir: resolveMigrationsDir() });
    await ensureTenantPartitions(pool, TENANT);
    events = createEventsStorage(createClient(pool), TENANT);
  }, 60_000);

  afterAll(async () => {
    for (const table of [
      "workflow_stream_checkpoints",
      "workflow_event_slots",
      "workflow_events",
      "workflow_steps",
      "workflow_hooks",
      "workflow_waits",
      "workflow_runs",
    ]) {
      await pool
        ?.query(`delete from workflow.${table} where tenant_id = $1`, [TENANT])
        .catch(() => {});
    }
    await dropTenantPartitions(pool, TENANT).catch(() => {});
    await pool?.end().catch(() => {});
  });

  test.each([
    ["scheduled", 60_000, 15 * 60_000, 7 * 24 * 60 * 60_000],
    ["interactive", 5 * 60_000, 24 * 60 * 60_000, 30 * 24 * 60 * 60_000],
  ] as const)(
    "terminal %s runs receive class-specific deadlines",
    async (retentionClass, compactDelay, expireDelay, detailDelay) => {
      const runId = await createRun(retentionClass);
      await events.create(runId, { eventType: "run_started" });
      await events.create(runId, { eventType: "run_completed", eventData: { output: [] } });

      const row = await getRetention(runId);
      expect(row.retention_class).toBe(retentionClass);
      expect(row.compact_after!.getTime() - row.completed_at!.getTime()).toBe(compactDelay);
      expect(row.expire_after!.getTime() - row.completed_at!.getTime()).toBe(expireDelay);
      expect(row.detail_expire_after!.getTime() - row.completed_at!.getTime()).toBe(detailDelay);
    },
  );

  test("persistent runs never receive cleanup deadlines", async () => {
    const runId = await createRun("persistent");
    await events.create(runId, { eventType: "run_started" });
    await events.create(runId, { eventType: "run_cancelled" });

    const row = await getRetention(runId);
    expect(row.retention_class).toBe("persistent");
    expect(row.compact_after).toBeNull();
    expect(row.expire_after).toBeNull();
    expect(row.detail_expire_after).toBeNull();
  });

  async function createRun(retentionClass: string): Promise<string> {
    const request = {
      eventType: "run_created",
      eventData: {
        deploymentId: "dep_retention",
        workflowName: "retention-test",
        input: [],
        retentionClass,
      },
    } as unknown as Extract<AnyEventRequest, { eventType: "run_created" }>;
    const result = await events.create(null, request);
    return result.run!.runId;
  }

  async function getRetention(runId: string) {
    const { rows } = await pool.query<{
      retention_class: string;
      completed_at: Date | null;
      compact_after: Date | null;
      expire_after: Date | null;
      detail_expire_after: Date | null;
    }>(
      `select retention_class, completed_at, compact_after, expire_after, detail_expire_after
         from workflow.workflow_runs where tenant_id = $1 and id = $2`,
      [TENANT, runId],
    );
    return rows[0]!;
  }
});
