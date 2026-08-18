import type { Pool } from "pg";

/**
 * World-visible proof that a cutover postcondition ran and passed against this
 * database. The dispatcher never reads the control-plane database, so this is
 * the only place its recover-paused preflight can learn that the operator's
 * classification/termination/migration actually converged here — a
 * control-plane fence alone is invisible to it.
 */
export type CutoverProof = {
  operationId: string;
  passed: boolean;
  claimableUnscopedJobs: number;
  blockingRuns: number;
  recordedAt: Date;
  recordedBy: string;
};

export async function recordCutoverProof(
  pool: Pool,
  input: {
    operationId: string;
    passed: boolean;
    claimableUnscopedJobs: number;
    blockingRuns: number;
    recordedBy: string;
  },
): Promise<void> {
  await pool.query(
    `insert into workflow.cutover_proofs
       (operation_id, passed, claimable_unscoped_jobs, blocking_runs, recorded_by)
     values ($1, $2, $3, $4, $5)`,
    [
      input.operationId,
      input.passed,
      input.claimableUnscopedJobs,
      input.blockingRuns,
      input.recordedBy,
    ],
  );
}

/** The newest proof for an operation; history is append-only. */
export async function readLatestCutoverProof(
  pool: Pool,
  operationId: string,
): Promise<CutoverProof | null> {
  const { rows } = await pool.query<{
    operation_id: string;
    passed: boolean;
    claimable_unscoped_jobs: number;
    blocking_runs: number;
    recorded_at: Date;
    recorded_by: string;
  }>(
    `select operation_id, passed, claimable_unscoped_jobs, blocking_runs, recorded_at, recorded_by
       from workflow.cutover_proofs
      where operation_id = $1
      order by recorded_at desc
      limit 1`,
    [operationId],
  );
  const row = rows[0];
  if (!row) return null;
  return {
    operationId: row.operation_id,
    passed: row.passed,
    claimableUnscopedJobs: row.claimable_unscoped_jobs,
    blockingRuns: row.blocking_runs,
    recordedAt: row.recorded_at,
    recordedBy: row.recorded_by,
  };
}
