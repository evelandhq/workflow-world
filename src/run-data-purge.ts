import { readRunRetention } from "@workflow/world";
import { type SQL, sql } from "drizzle-orm";
import type { Drizzle } from "./drizzle/index.js";

/** Anything that runs one statement: a drizzle database or transaction. */
export type PurgeExecutor = { execute(query: SQL): Promise<unknown> };

/**
 * Delete a finished run's user data now, for runs started with
 * `$retention: 0` (ported from upstream world-postgres's `retention.ts`).
 *
 * Every payload column of the run and of what hangs off it is set to SQL
 * `NULL` — not a CBOR-encoded `null`, which would still be bytes that read back
 * as a value — and the run's `expired_at` is stamped. That column is what eve
 * and `@workflow/core` read as "this run's data is gone"; without it the
 * deletion reads back as a run that had no input.
 *
 * **The rows stay.** Runs, steps, events and retained hooks keep their ids,
 * statuses, timestamps and attributes, so a purged run is still listable and
 * traceable. Deleting the rows is the retention classes' job
 * (`pruneExpiredWorkflowRuns`), which this does not touch: it writes none of
 * the columns the deadline trigger or the prune predicates read.
 *
 * **`hook_disposed` events keep their payload**, unlike upstream: it holds only
 * the token and the force-claiming run's ids, which the World itself reads.
 *
 * **Stream rows are deleted, not blanked**, unlike upstream. A packed block
 * (`stream-blocks.ts`) encodes its logical chunk ids inside its data, so
 * blanking one would break the reader that unpacks it. Deleting the run's
 * non-EOF rows is what `pruneExpiredStreamChunks` does later anyway; the EOF
 * rows stay, so a reader still terminates and `streams.list` still finds a
 * closed stream. Rows are matched by `run_id`, which is the run id the writer
 * passed, so chunks another run wrote into this run's stream are covered and
 * chunks this run wrote into another run's stream are not.
 *
 * **Dead letters keep their row, not their body.** A dead letter preserves the
 * queue message verbatim, and a message body can carry run input
 * (`runInput`), a hook payload (`hookInput`) or a step's input. By the time
 * this runs the terminal transition has already resolved the run's letters, so
 * nothing will replay them; the body's `data` is emptied and the envelope
 * (ids, reason, attempt) stays as the operator's record. A letter whose body
 * named no run (`run_id` NULL) cannot be attributed and is left alone.
 *
 * Every statement is tenant-scoped. The caller owns atomicity: run it inside a
 * transaction or savepoint so no reader sees half a purge.
 */
export async function writeRunUserDataPurge(
  db: PurgeExecutor,
  tenantId: string,
  runId: string,
  purgedAt: Date,
): Promise<void> {
  // The run first, so the statement order reads correctly to anyone who splits
  // this up: data must become unreadable no later than it becomes unrecoverable.
  // `expired_at` is a UTC wall-clock `timestamp`, the way drizzle writes the
  // table's other timestamps; a bare `Date` would be sent in local time.
  await db.execute(sql`
    update workflow.workflow_runs
       set expired_at = ${purgedAt.toISOString()}::timestamp,
           input_cbor = null,
           output_cbor = null,
           error_cbor = null
     where tenant_id = ${tenantId} and id = ${runId}`);
  await db.execute(sql`
    update workflow.workflow_steps
       set input_cbor = null,
           output_cbor = null,
           error_cbor = null
     where tenant_id = ${tenantId} and run_id = ${runId}`);
  // `hook_disposed` keeps its payload: it is World-written routing (the token
  // and `forceClaimedBy`), no user data, and `forceClaimRefusal` in storage.ts
  // reads it to redirect a delivery that resolved a force-claimed hook. eve's
  // session handoff victims finish right after the takeover, and clearing it
  // would turn that redirect into a plain not-found.
  await db.execute(sql`
    update workflow.workflow_events
       set payload_cbor = null
     where tenant_id = ${tenantId} and run_id = ${runId}
       and payload_cbor is not null
       and type <> 'hook_disposed'`);
  // A hook with a retained token outlives its run, and a hook read is not gated
  // on the run's `expired_at`, so its metadata is cleared in place.
  await db.execute(sql`
    update workflow.workflow_hooks
       set metadata_cbor = null
     where tenant_id = ${tenantId} and run_id = ${runId}`);
  await db.execute(sql`
    delete from workflow.workflow_stream_chunks
     where tenant_id = ${tenantId} and run_id = ${runId} and eof = false`);
  await db.execute(sql`
    update workflow.dispatch_dead_letters
       set payload = jsonb_set(payload, '{data}', '""'::jsonb)
     where tenant_id = ${tenantId} and run_id = ${runId}
       and payload ? 'data'
       and payload ->> 'data' <> ''`);
}

/** {@link writeRunUserDataPurge} in one transaction (a savepoint inside one). */
export async function purgeRunUserData(
  drizzle: Drizzle,
  tenantId: string,
  runId: string,
  purgedAt: Date,
): Promise<void> {
  await drizzle.transaction(async (tx) => {
    await writeRunUserDataPurge(tx, tenantId, runId, purgedAt);
  });
}

/**
 * Whether a finished run asked for its user data to be deleted now. Logs a
 * value this World does not implement, and keeps that run's data.
 */
export function requestsZeroRetention(
  runId: string,
  attributes: Record<string, string> | undefined,
): boolean {
  const retention = readRunRetention(attributes);
  if (retention.unsupported) {
    console.warn(
      `[workflow] run "${runId}" requested retention "${String(retention.raw)}", ` +
        (retention.wellFormed
          ? 'a duration this World cannot scale yet (only "0" is implemented, ' +
            "because zero is the one duration whose meaning does not depend " +
            "on the undecided unit)"
          : "which this World does not recognize") +
        "; keeping the data.",
    );
  }
  return retention.mode === "none";
}

/**
 * Purge a terminal run's user data if it asked for zero retention.
 *
 * Call it once per run, from the writer that won the terminal transition, after
 * the terminal event row is written: that event is itself payload-bearing
 * (`run_completed` carries the output). The response that writer returns is a
 * pre-purge snapshot, as in upstream and the Vercel World.
 *
 * Never throws. A run must still finish if its purge fails; the failure is
 * logged and the run keeps data it asked to have deleted.
 */
export async function purgeRunUserDataIfZeroRetention(
  drizzle: Drizzle,
  tenantId: string,
  runId: string,
  attributes: Record<string, string> | undefined,
  purgedAt: Date = new Date(),
): Promise<void> {
  if (!requestsZeroRetention(runId, attributes)) return;
  try {
    await purgeRunUserData(drizzle, tenantId, runId, purgedAt);
  } catch (cause) {
    console.error(
      `[workflow] failed to purge user data for zero-retention run "${runId}" (tenant "${tenantId}")`,
      cause,
    );
  }
}
