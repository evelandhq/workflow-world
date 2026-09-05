import { Pool } from "pg";
import { afterAll, beforeAll, beforeEach, describe, expect, test } from "vitest";
import {
  dropTenantPartitions,
  ensureTenantPartitions,
  resolveMigrationsDir,
  runMigrations,
} from "./migrate.js";
import { reconcileWorkflowRuns } from "./reconciliation.js";

/**
 * The table invariant behind the operator-facing count: an unresolved dead
 * letter means a delivery this installation dropped and an operator still has
 * to decide about. That is only true while the run can still be replayed, so a
 * run reaching a terminal status resolves its letters — whoever writes that
 * status.
 *
 * Set `EVELAND_WORKFLOW_WORLD_TEST_URL` to a scratch database to run these.
 */
const testUrl = process.env.EVELAND_WORKFLOW_WORLD_TEST_URL;
const suffix = `${String(process.pid)}${Date.now().toString(36)}`;
const TENANT = `p_dead_letter_${suffix}`;

describe.skipIf(!testUrl)("dead-letter resolution", () => {
  let pool: Pool;

  beforeAll(async () => {
    pool = new Pool({ connectionString: testUrl, max: 3 });
    await runMigrations(pool, { migrationsDir: resolveMigrationsDir() });
    await ensureTenantPartitions(pool, TENANT);
  }, 60_000);

  beforeEach(async () => {
    await clearTenant();
  });

  afterAll(async () => {
    await clearTenant().catch(() => {});
    await dropTenantPartitions(pool, TENANT).catch(() => {});
    await pool?.end().catch(() => {});
  });

  test("a run reaching a terminal status resolves every letter it dropped", async () => {
    await insertRun("wrun_dead", "running");
    await insertRun("wrun_live", "running");
    await insertLetter("wrun_dead", "first delivery");
    await insertLetter("wrun_dead", "second delivery");
    await insertLetter("wrun_live", "still stuck");
    await insertLetter(null, "no run of its own");

    await pool.query(
      `update workflow.workflow_runs
          set status = 'completed', completed_at = now()
        where tenant_id = $1 and id = 'wrun_dead'`,
      [TENANT],
    );

    expect(await unresolvedReasons()).toEqual(["no run of its own", "still stuck"]);
  });

  test("host reconciliation settles the run and its letters in one step", async () => {
    await insertRun("wrun_orphan", "pending");

    // The reconciler's own quarantine letter is exactly the kind that outlives
    // its run, so the settle that follows must clear it alongside the organic
    // one the dispatcher wrote.
    await reconcileWorkflowRuns(pool, {
      tenantId: TENANT,
      runIds: ["wrun_orphan"],
      disposition: "quarantine",
      reason: "deployment not activatable",
    });
    await insertLetter("wrun_orphan", "Deployment is not activatable");
    expect(await unresolvedReasons()).toHaveLength(2);

    await reconcileWorkflowRuns(pool, {
      tenantId: TENANT,
      runIds: ["wrun_orphan"],
      disposition: "fail",
      reason: "deployment can never activate again",
      errorCode: "DEPLOYMENT_UNSTARTABLE",
    });

    expect(await unresolvedReasons()).toEqual([]);
  });

  test("resolution is not re-stamped and does not cross tenants", async () => {
    const other = `${TENANT}_x`;
    await ensureTenantPartitions(pool, other);
    try {
      await insertRun("wrun_shared", "running");
      await insertRun("wrun_shared", "running", other);
      await insertLetter("wrun_shared", "settled long ago");
      await insertLetter("wrun_shared", "still open");
      await insertLetter("wrun_shared", "theirs", other);
      await pool.query(
        `update workflow.dispatch_dead_letters
            set resolved_at = timestamptz '2020-01-01T00:00:00Z'
          where tenant_id = $1 and reason = 'settled long ago'`,
        [TENANT],
      );

      await pool.query(
        `update workflow.workflow_runs set status = 'cancelled', completed_at = now()
          where tenant_id = $1 and id = 'wrun_shared'`,
        [TENANT],
      );

      // An already-resolved letter keeps its own timestamp, the open one gets
      // stamped, and the other tenant's identically-named run keeps its letter.
      const { rows } = await pool.query<{
        tenant_id: string;
        reason: string;
        resolved_at: Date | null;
      }>(
        `select tenant_id, reason, resolved_at from workflow.dispatch_dead_letters
          where tenant_id = any($1) order by reason`,
        [[TENANT, other]],
      );
      expect(rows.map((row) => [row.reason, row.resolved_at === null ? null : "stamped"])).toEqual([
        ["settled long ago", "stamped"],
        ["still open", "stamped"],
        ["theirs", null],
      ]);
      expect(rows[0]!.resolved_at!.toISOString()).toBe("2020-01-01T00:00:00.000Z");
      expect(rows[2]!.tenant_id).toBe(other);
    } finally {
      await pool
        .query("delete from workflow.dispatch_dead_letters where tenant_id = $1", [other])
        .catch(() => {});
      await pool
        .query("delete from workflow.workflow_runs where tenant_id = $1", [other])
        .catch(() => {});
      await dropTenantPartitions(pool, other).catch(() => {});
    }
  });

  async function clearTenant() {
    await pool.query("delete from workflow.dispatch_dead_letters where tenant_id = $1", [TENANT]);
    await pool.query("delete from workflow.workflow_runs where tenant_id = $1", [TENANT]);
  }

  async function insertRun(runId: string, status: string, tenantId = TENANT) {
    await pool.query(
      `insert into workflow.workflow_runs
         (tenant_id, id, deployment_id, status, name, spec_version)
       values ($1, $2, 'dep_dead_letter', $3::workflow.status, 'greet', 6)`,
      [tenantId, runId, status],
    );
  }

  async function insertLetter(runId: string | null, reason: string, tenantId = TENANT) {
    await pool.query(
      `insert into workflow.dispatch_dead_letters
         (tenant_id, deployment_id, run_id, message_id, job_name, queue_name, attempt, reason, payload)
       values ($1, 'dep_dead_letter', $2, $3, 'eveland_wf_flows', 'wfrun:q', 1, $4, '{}'::jsonb)`,
      [tenantId, runId, `msg_${reason.replaceAll(" ", "_")}`, reason],
    );
  }

  async function unresolvedReasons(): Promise<string[]> {
    const { rows } = await pool.query<{ reason: string }>(
      `select reason from workflow.dispatch_dead_letters
        where tenant_id = $1 and resolved_at is null
        order by reason`,
      [TENANT],
    );
    return rows.map((row) => row.reason);
  }
});
