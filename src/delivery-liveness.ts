import { sql, type SQL } from "drizzle-orm";

type Executor = {
  execute(query: SQL): Promise<{ rows: unknown[] }>;
  transaction<T>(run: (tx: Executor) => Promise<T>): Promise<T>;
};

let warnedReadFailure = false;

/**
 * Whether the delivery with this message id is in flight: a worker holds its
 * graphile job. The dispatcher (or the embedded runner) keeps the job locked
 * for the whole delivery and releases it when the delivery ends, fails or times
 * out. A dead worker's lock lasts until boot recovery releases it or, for an
 * embedded runner, until graphile's four-hour stale-lock rule, which this read
 * applies too.
 *
 * Reads graphile-worker 0.16's private job table, which has no index on the
 * payload, so callers ask only on the rare path that needs it. The read runs in
 * a savepoint so that a failure cannot abort the caller's transaction, and a
 * failure answers "not in flight": the caller then behaves as it did before
 * this check existed.
 */
export async function deliveryInFlight(db: Executor, messageId: string): Promise<boolean> {
  try {
    return await db.transaction(async (savepoint) => {
      const { rows } = await savepoint.execute(sql`
        select 1
          from graphile_worker._private_jobs
         where payload->>'messageId' = ${messageId}
           and locked_at > now() - interval '4 hours'
         limit 1`);
      return rows.length > 0;
    });
  } catch (err) {
    if (!warnedReadFailure) {
      warnedReadFailure = true;
      console.warn(
        "[eveland workflow world] Could not read whether a delivery is in flight; a running step may be restarted beside it:",
        err,
      );
    }
    return false;
  }
}
