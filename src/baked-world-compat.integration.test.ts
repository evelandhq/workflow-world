import { Pool } from "pg";
import { afterAll, beforeAll, describe, expect, test } from "vitest";
import { resolveMigrationsDir, runMigrations } from "./migrate.js";

const testUrl = process.env.EVELAND_WORKFLOW_WORLD_TEST_URL;

/**
 * A World is a build-time property of each Release, so the shared schema must
 * keep answering the queries of every World version still baked into a live
 * Release — not just the one this package ships.
 */
describe.skipIf(!testUrl)("schema compatibility with older baked Worlds", () => {
  let pool: Pool;

  beforeAll(async () => {
    pool = new Pool({ connectionString: testUrl, max: 2 });
    await runMigrations(pool, { migrationsDir: resolveMigrationsDir() });
  }, 60_000);

  afterAll(async () => {
    await pool.end().catch(() => {});
  });

  test("the <= 0.12.0 enqueue-path quarantine lookup still has its table", async () => {
    // Verbatim from 0.12.0's `isRunQuarantined`: it runs on every enqueue that
    // carries a runId, so dropping the table fails every delivery of every
    // deployment built against that line.
    const { rows } = await pool.query(
      `select 1 from workflow.run_quarantines
        where tenant_id = $1 and run_id = $2 and resolved_at is null`,
      ["t_compat", "run_compat"],
    );
    expect(rows).toHaveLength(0);
  });
});
