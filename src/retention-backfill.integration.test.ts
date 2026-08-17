import { Pool } from "pg";
import { afterAll, beforeAll, beforeEach, describe, expect, test } from "vitest";
import {
  backfillWorkflowRunRetentionClass,
  inspectWorkflowRunRetentionMismatches,
  previewWorkflowRunRetentionBackfill,
} from "./retention-backfill.js";
import {
  dropTenantPartitions,
  ensureTenantPartitions,
  resolveMigrationsDir,
  runMigrations,
} from "./migrate.js";

const testUrl = process.env.EVELAND_WORKFLOW_WORLD_TEST_URL;
const TENANT = "prj_retention_backfill";
const OTHER_TENANT = "prj_retention_backfill_other";
const selector = {
  tenantId: TENANT,
  rootAttribute: "$eve.trigger",
  rootValue: "channel:eveland-scheduler",
  retentionClass: "scheduled" as const,
};

describe.skipIf(!testUrl)("workflow graph retention backfill", () => {
  let pool: Pool;

  beforeAll(async () => {
    pool = new Pool({ connectionString: testUrl, max: 2 });
    await runMigrations(pool, { migrationsDir: resolveMigrationsDir() });
    await ensureTenantPartitions(pool, TENANT);
    await ensureTenantPartitions(pool, OTHER_TENANT);
  }, 60_000);

  beforeEach(async () => {
    await pool.query(`delete from workflow.workflow_runs where tenant_id = any($1::text[])`, [
      [TENANT, OTHER_TENANT],
    ]);
    await seedGraph(TENANT);
    await seedGraph(OTHER_TENANT);
  });

  afterAll(async () => {
    await dropTenantPartitions(pool, TENANT).catch(() => {});
    await dropTenantPartitions(pool, OTHER_TENANT).catch(() => {});
    await pool?.end().catch(() => {});
  });

  test("previews a provable graph without mutating it", async () => {
    const preview = await previewWorkflowRunRetentionBackfill(pool, selector);

    expect(preview).toMatchObject({
      matchedRoots: 1,
      eligibleRuns: 4,
      excludedPersistentRuns: 1,
    });
    expect(preview.groups).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          tenantId: TENANT,
          rootTrigger: "channel:eveland-scheduler",
          workflowName: "workflow//eve//workflowEntry",
          retentionClass: "interactive",
          status: "running",
          runs: 1,
        }),
        expect.objectContaining({
          workflowName: "workflow//eve//turnWorkflow",
          retentionClass: "interactive",
          status: "completed",
          runs: 1,
        }),
      ]),
    );
    expect(await classes(TENANT)).toContainEqual({ id: "root", retention_class: "interactive" });
  });

  test("reports bounded root and child mismatches without flagging persistent overrides", async () => {
    await expect(
      inspectWorkflowRunRetentionMismatches(pool, { ...selector, limit: 1 }),
    ).resolves.toMatchObject({
      mismatches: [expect.objectContaining({ runId: "root", kind: "root-class" })],
      hitLimit: false,
    });

    await backfillWorkflowRunRetentionClass(pool, { ...selector, batchSize: 2 });
    await expect(
      inspectWorkflowRunRetentionMismatches(pool, { ...selector, limit: 1 }),
    ).resolves.toMatchObject({ hitLimit: true });
    const result = await inspectWorkflowRunRetentionMismatches(pool, {
      ...selector,
      limit: 10,
    });
    expect(result.hitLimit).toBe(false);
    expect(result.mismatches).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          runId: "terminal-child",
          rootRunId: "root",
          kind: "child-root-class",
        }),
        expect.objectContaining({
          runId: "eve-lineage-child",
          rootRunId: "root",
          kind: "child-root-class",
        }),
      ]),
    );
    expect(result.mismatches.map((mismatch) => mismatch.runId)).not.toContain("persistent-child");
  });

  test("covers every stable Eve workflow name through generic mismatch lineage", async () => {
    await pool.query(
      `update workflow.workflow_runs
          set retention_class = case when id = 'root' then 'scheduled' else 'interactive' end
        where tenant_id = $1 and id <> 'unrelated'`,
      [TENANT],
    );

    const result = await inspectWorkflowRunRetentionMismatches(pool, {
      ...selector,
      limit: 10,
    });
    expect(new Set(result.mismatches.map((mismatch) => mismatch.workflowName))).toEqual(
      new Set([
        "workflow//eve//workflowEntry",
        "workflow//eve//turnWorkflow",
        "workflow//eve//sessionTimeoutWorkflow",
        "workflow//eve//taskRunWorkflow",
      ]),
    );
  });

  test("updates active runs first in bounded repeatable batches and recomputes deadlines", async () => {
    await expect(
      backfillWorkflowRunRetentionClass(pool, { ...selector, batchSize: 2 }),
    ).resolves.toEqual({ updatedRuns: 2, remainingRuns: 2, hitBatchLimit: true });

    expect(await classes(TENANT)).toEqual(
      expect.arrayContaining([
        { id: "root", retention_class: "scheduled" },
        { id: "active-child", retention_class: "scheduled" },
        { id: "terminal-child", retention_class: "interactive" },
      ]),
    );

    await expect(
      backfillWorkflowRunRetentionClass(pool, { ...selector, batchSize: 2 }),
    ).resolves.toEqual({ updatedRuns: 2, remainingRuns: 0, hitBatchLimit: false });
    await expect(
      backfillWorkflowRunRetentionClass(pool, { ...selector, batchSize: 2 }),
    ).resolves.toEqual({ updatedRuns: 0, remainingRuns: 0, hitBatchLimit: false });

    const { rows } = await pool.query<{
      retention_class: string;
      completed_at: Date;
      compact_after: Date;
      expire_after: Date;
      detail_expire_after: Date;
    }>(
      `select retention_class, completed_at, compact_after, expire_after, detail_expire_after
         from workflow.workflow_runs
        where tenant_id = $1 and id = 'terminal-child'`,
      [TENANT],
    );
    const terminal = rows[0]!;
    expect(terminal.retention_class).toBe("scheduled");
    expect(terminal.compact_after.getTime() - terminal.completed_at.getTime()).toBe(60_000);
    expect(terminal.expire_after.getTime() - terminal.completed_at.getTime()).toBe(15 * 60_000);
    expect(terminal.detail_expire_after.getTime() - terminal.completed_at.getTime()).toBe(
      24 * 60 * 60_000,
    );

    expect(await classes(TENANT)).toContainEqual({
      id: "persistent-child",
      retention_class: "persistent",
    });
    expect(await classes(TENANT)).toContainEqual({
      id: "unrelated",
      retention_class: "interactive",
    });
    expect(await classes(OTHER_TENANT)).toContainEqual({
      id: "root",
      retention_class: "interactive",
    });
  });

  async function seedGraph(tenantId: string) {
    await Promise.all([
      insertRun(tenantId, "root", "workflow//eve//workflowEntry", "running", "interactive", {
        "$eve.trigger": "channel:eveland-scheduler",
      }),
      insertRun(
        tenantId,
        "active-child",
        "workflow//eve//sessionTimeoutWorkflow",
        "running",
        "interactive",
        { $parentRunId: "root", $rootRunId: "root" },
      ),
      insertRun(
        tenantId,
        "terminal-child",
        "workflow//eve//turnWorkflow",
        "completed",
        "interactive",
        { $parentRunId: "root", $rootRunId: "root" },
      ),
      insertRun(
        tenantId,
        "eve-lineage-child",
        "workflow//eve//workflowEntry",
        "completed",
        "interactive",
        { "$eve.parent": "root", "$eve.root": "root" },
      ),
      insertRun(
        tenantId,
        "persistent-child",
        "workflow//eve//taskRunWorkflow",
        "completed",
        "persistent",
        { $parentRunId: "root", $rootRunId: "root" },
      ),
      insertRun(
        tenantId,
        "unrelated",
        "workflow//eve//turnWorkflow",
        "completed",
        "interactive",
        {},
      ),
    ]);
  }

  async function insertRun(
    tenantId: string,
    runId: string,
    workflowName: string,
    status: "running" | "completed",
    retentionClass: "scheduled" | "interactive" | "persistent",
    attributes: Record<string, string>,
  ) {
    await pool.query(
      `insert into workflow.workflow_runs
         (tenant_id, id, deployment_id, status, name, spec_version, attributes,
          retention_class, created_at, updated_at, completed_at)
       values ($1, $2, 'dep_retention', $3::workflow.status, $4, 6, $5::jsonb,
               $6, now() - interval '2 hours', now() - interval '2 hours',
               case when $3 = 'completed' then now() - interval '2 hours' end)`,
      [tenantId, runId, status, workflowName, JSON.stringify(attributes), retentionClass],
    );
  }

  async function classes(tenantId: string) {
    const { rows } = await pool.query<{ id: string; retention_class: string }>(
      `select id, retention_class
         from workflow.workflow_runs
        where tenant_id = $1
        order by id`,
      [tenantId],
    );
    return rows;
  }
});
