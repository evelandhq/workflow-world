import { Pool } from "pg";
import { afterAll, beforeAll, describe, expect, test } from "vitest";
import { readLatestCutoverProof, recordCutoverProof } from "./cutover-proof.js";
import { runMigrations } from "./migrate.js";

/**
 * The World-visible cutover proof: what lets a recover-paused dispatcher know
 * the operator's postcondition actually ran and passed against THIS database.
 * Set `EVELAND_WORKFLOW_WORLD_TEST_URL` to a scratch database to run it.
 */
const testUrl = process.env.EVELAND_WORKFLOW_WORLD_TEST_URL;
const OPERATION = `cut_proof_${String(process.pid)}${Date.now().toString(36)}`;

describe.skipIf(!testUrl)("cutover proofs", () => {
  let pool: Pool;

  beforeAll(async () => {
    pool = new Pool({ connectionString: testUrl, max: 2 });
    await runMigrations(pool);
  }, 60_000);

  afterAll(async () => {
    await pool
      ?.query("delete from workflow.cutover_proofs where operation_id = $1", [OPERATION])
      .catch(() => {});
    await pool?.end().catch(() => {});
  });

  test("append-only history; the newest proof wins and a failed one stays visible", async () => {
    expect(await readLatestCutoverProof(pool, OPERATION)).toBeNull();

    await recordCutoverProof(pool, {
      operationId: OPERATION,
      passed: false,
      claimableUnscopedJobs: 3,
      blockingRuns: 1,
      recordedBy: "cutover-cli-test",
    });
    expect(await readLatestCutoverProof(pool, OPERATION)).toMatchObject({
      passed: false,
      claimableUnscopedJobs: 3,
    });

    await recordCutoverProof(pool, {
      operationId: OPERATION,
      passed: true,
      claimableUnscopedJobs: 0,
      blockingRuns: 0,
      recordedBy: "cutover-cli-test",
    });
    expect(await readLatestCutoverProof(pool, OPERATION)).toMatchObject({
      passed: true,
      blockingRuns: 0,
    });
    // A different operation's proof never satisfies this one.
    expect(await readLatestCutoverProof(pool, "cut_other")).toBeNull();
  }, 60_000);
});
