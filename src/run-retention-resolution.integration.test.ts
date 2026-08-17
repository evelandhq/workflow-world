import type { AnyEventRequest, Storage } from "@workflow/world";
import { Pool } from "pg";
import { ulid } from "ulid";
import { afterAll, beforeAll, beforeEach, describe, expect, test } from "vitest";
import { createClient } from "./drizzle/index.js";
import {
  dropTenantPartitions,
  ensureTenantPartitions,
  resolveMigrationsDir,
  runMigrations,
} from "./migrate.js";
import { withRunRetentionIntent } from "./run-retention-resolution.js";
import { createEventsStorage } from "./storage.js";

const testUrl = process.env.EVELAND_WORKFLOW_WORLD_TEST_URL;
const TENANT = "prj_retention_resolution";
const STABLE_EVE_WORKFLOW_NAMES = [
  "workflow//eve//workflowEntry",
  "workflow//eve//turnWorkflow",
  "workflow//eve//sessionTimeoutWorkflow",
  "workflow//eve//taskRunWorkflow",
] as const;

describe.skipIf(!testUrl)("run retention resolution at the storage boundary", () => {
  let pool: Pool;
  let events: Storage["events"];

  beforeAll(async () => {
    pool = new Pool({ connectionString: testUrl, max: 2 });
    await runMigrations(pool, { migrationsDir: resolveMigrationsDir() });
    await ensureTenantPartitions(pool, TENANT);
    events = createEventsStorage(createClient(pool), TENANT);
  }, 60_000);

  beforeEach(async () => {
    for (const table of [
      "workflow_events",
      "workflow_steps",
      "workflow_hooks",
      "workflow_waits",
      "workflow_runs",
    ]) {
      await pool.query(`delete from workflow.${table} where tenant_id = $1`, [TENANT]);
    }
  });

  afterAll(async () => {
    await dropTenantPartitions(pool, TENANT).catch(() => {});
    await pool?.end().catch(() => {});
  });

  test("ordinary run_created persists the ambient platform class", async () => {
    const result = await withRunRetentionIntent("scheduled", () => createRun());

    expect(await readRetentionClass(result.run!.runId)).toBe("scheduled");
  });

  test.each(["scheduled", "interactive", "persistent"] as const)(
    "ordinary run_created inherits %s through generic SDK lineage",
    async (retentionClass) => {
      const root = await createRun({ retentionClass });
      const child = await createRun({
        attributes: {
          $parentRunId: root.run!.runId,
          $rootRunId: root.run!.runId,
        },
      });

      expect(await readRetentionClass(child.run!.runId)).toBe(retentionClass);
    },
  );

  test.each(STABLE_EVE_WORKFLOW_NAMES)(
    "%s uses the same generic lineage boundary",
    async (workflowName) => {
      for (const retentionClass of ["scheduled", "interactive", "persistent"] as const) {
        const root = await createRun({ retentionClass });
        const child = await createRun({
          workflowName,
          attributes: {
            $parentRunId: root.run!.runId,
            $rootRunId: root.run!.runId,
          },
        });
        expect(await readRetentionClass(child.run!.runId)).toBe(retentionClass);
      }
    },
  );

  test("resilient run_started uses the same lineage resolution", async () => {
    const root = await createRun({ retentionClass: "scheduled" });
    const childRunId = `wrun_${ulid()}`;
    const child = await events.create(childRunId, {
      eventType: "run_started",
      eventData: {
        deploymentId: "dep_retention",
        workflowName: "workflow//eve//sessionTimeoutWorkflow",
        input: new Uint8Array(),
        attributes: {
          $parentRunId: root.run!.runId,
          $rootRunId: root.run!.runId,
        },
        allowReservedAttributes: true,
      },
    });

    expect(await readRetentionClass(child.run!.runId)).toBe("scheduled");
  });

  async function createRun(options?: {
    retentionClass?: "scheduled" | "interactive" | "persistent";
    attributes?: Record<string, string>;
    workflowName?: string;
  }) {
    return events.create(null, {
      eventType: "run_created",
      eventData: {
        deploymentId: "dep_retention",
        workflowName: options?.workflowName ?? "arbitrary-workflow-name",
        input: new Uint8Array(),
        ...(options?.attributes === undefined
          ? {}
          : { attributes: options.attributes, allowReservedAttributes: true as const }),
        ...(options?.retentionClass === undefined
          ? {}
          : { retentionClass: options.retentionClass }),
      },
    } as unknown as Extract<AnyEventRequest, { eventType: "run_created" }>);
  }

  async function readRetentionClass(runId: string): Promise<string> {
    const { rows } = await pool.query<{ retention_class: string }>(
      `select retention_class
         from workflow.workflow_runs
        where tenant_id = $1 and id = $2`,
      [TENANT, runId],
    );
    return rows[0]!.retention_class;
  }
});
