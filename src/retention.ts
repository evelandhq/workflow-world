import type { Pool } from "pg";
import { resolveRunRetentionClass, type RunRetentionClass } from "./run-retention-policy.js";
import { assertValidTenantId } from "./tenant.js";

/** A session-level lock keeps independently scheduled sweepers from piling up. */
const STREAM_RETENTION_LOCK_KEY = 0x65_76_72_74; // "evrt"
const RUN_RETENTION_LOCK_KEY = 0x65_76_72_72; // "evrr"

export type StreamRetentionOptions = {
  /** Age after terminal completion before non-EOF chunks become eligible. */
  retentionMs: number;
  /** Maximum rows deleted by one statement. */
  batchSize: number;
  /** Maximum DELETE statements issued by one invocation. */
  maxBatches: number;
};

export type StreamRetentionResult = {
  deletedRows: number;
  batches: number;
  hitBatchLimit: boolean;
  lockAcquired: boolean;
};

export type DeadlineRetentionOptions = Pick<StreamRetentionOptions, "batchSize" | "maxBatches">;

export type WorkflowRunRetentionResult = {
  deletedRuns: number;
  batches: number;
  hitBatchLimit: boolean;
  lockAcquired: boolean;
};

export type SetWorkflowRunRetentionClassOptions = {
  tenantId: string;
  runId: string;
  retentionClass: RunRetentionClass | "ephemeral";
};

/**
 * Irreversibly expires stream replay data for old terminal runs. The caller
 * owns scheduling and policy; this function only bounds and serializes one
 * database invocation.
 */
export async function pruneTerminalStreamChunks(
  pool: Pool,
  options: StreamRetentionOptions,
): Promise<StreamRetentionResult> {
  assertNonNegativeInteger(options.retentionMs, "retentionMs");
  assertPositiveInteger(options.batchSize, "batchSize");
  assertPositiveInteger(options.maxBatches, "maxBatches");

  const client = await pool.connect();
  let lockAcquired = false;
  try {
    const lockResult = await client.query<{ locked: boolean }>(
      "select pg_try_advisory_lock($1) as locked",
      [STREAM_RETENTION_LOCK_KEY],
    );
    lockAcquired = lockResult.rows[0]?.locked === true;
    if (!lockAcquired) {
      return {
        deletedRows: 0,
        batches: 0,
        hitBatchLimit: false,
        lockAcquired: false,
      };
    }

    let deletedRows = 0;
    for (let batches = 1; batches <= options.maxBatches; batches += 1) {
      const result = await client.query(
        `with victims as (
           select chunks.tableoid, chunks.ctid
             from workflow.workflow_stream_chunks as chunks
             join workflow.workflow_runs as runs
               on runs.tenant_id = chunks.tenant_id
              and runs.id = chunks.run_id
            where chunks.eof = false
              and runs.status in ('completed', 'failed', 'cancelled')
              and coalesce(runs.completed_at, runs.updated_at)
                    < now() - ($1::bigint * interval '1 millisecond')
              and not exists (
                select 1
                  from workflow.workflow_runs as lineage
                 where lineage.tenant_id = runs.tenant_id
                   and lineage.retention_root_run_id = runs.retention_root_run_id
                   and (
                     lineage.status not in ('completed', 'failed', 'cancelled')
                     or lineage.retention_class = 'persistent'
                     or coalesce(lineage.completed_at, lineage.updated_at)
                          >= now() - ($1::bigint * interval '1 millisecond')
                   )
              )
              and not exists (
                select 1
                  from workflow.workflow_hooks as capabilities
                  join workflow.workflow_runs as owners
                    on owners.tenant_id = capabilities.tenant_id
                   and owners.id = capabilities.run_id
                 where owners.tenant_id = runs.tenant_id
                   and owners.retention_root_run_id = runs.retention_root_run_id
                   and capabilities.token_retention_until > now()
              )
            order by coalesce(runs.completed_at, runs.updated_at),
                     chunks.tenant_id,
                     chunks.stream_id,
                     chunks.id
            limit $2
         )
         delete from workflow.workflow_stream_chunks as chunks
          using victims
          where chunks.tableoid = victims.tableoid
            and chunks.ctid = victims.ctid
         returning 1`,
        [options.retentionMs, options.batchSize],
      );
      const batchDeletedRows = result.rows.length;
      deletedRows += batchDeletedRows;

      if (batchDeletedRows < options.batchSize) {
        await deleteTerminalCheckpointsByAge(client, options.retentionMs);
        return {
          deletedRows,
          batches,
          hitBatchLimit: false,
          lockAcquired: true,
        };
      }
    }

    await deleteTerminalCheckpointsByAge(client, options.retentionMs);
    return {
      deletedRows,
      batches: options.maxBatches,
      hitBatchLimit: true,
      lockAcquired: true,
    };
  } finally {
    if (lockAcquired) {
      await client
        .query("select pg_advisory_unlock($1)", [STREAM_RETENTION_LOCK_KEY])
        .catch(() => {});
    }
    client.release();
  }
}

/** Delete non-EOF stream rows whose per-run `expire_after` has elapsed. */
export async function pruneExpiredStreamChunks(
  pool: Pool,
  options: DeadlineRetentionOptions,
): Promise<StreamRetentionResult> {
  assertPositiveInteger(options.batchSize, "batchSize");
  assertPositiveInteger(options.maxBatches, "maxBatches");

  const client = await pool.connect();
  let lockAcquired = false;
  try {
    const lockResult = await client.query<{ locked: boolean }>(
      "select pg_try_advisory_lock($1) as locked",
      [STREAM_RETENTION_LOCK_KEY],
    );
    lockAcquired = lockResult.rows[0]?.locked === true;
    if (!lockAcquired) return emptyStreamResult(false);

    let deletedRows = 0;
    for (let batches = 1; batches <= options.maxBatches; batches += 1) {
      const result = await client.query(
        `with victims as (
           select chunks.tableoid, chunks.ctid
             from workflow.workflow_stream_chunks as chunks
             join workflow.workflow_runs as runs
               on runs.tenant_id = chunks.tenant_id
              and runs.id = chunks.run_id
            where chunks.eof = false
              and runs.status in ('completed', 'failed', 'cancelled')
              and runs.expire_after is not null
              and runs.expire_after <= now()
              and not exists (
                select 1
                  from workflow.workflow_runs as lineage
                 where lineage.tenant_id = runs.tenant_id
                   and lineage.retention_root_run_id = runs.retention_root_run_id
                   and (
                     lineage.status not in ('completed', 'failed', 'cancelled')
                     or lineage.retention_class = 'persistent'
                     or lineage.expire_after is null
                     or lineage.expire_after > now()
                   )
              )
              and not exists (
                select 1
                  from workflow.workflow_hooks as capabilities
                  join workflow.workflow_runs as owners
                    on owners.tenant_id = capabilities.tenant_id
                   and owners.id = capabilities.run_id
                 where owners.tenant_id = runs.tenant_id
                   and owners.retention_root_run_id = runs.retention_root_run_id
                   and capabilities.token_retention_until > now()
              )
            order by runs.expire_after,
                     chunks.tenant_id,
                     chunks.stream_id,
                     chunks.id
            limit $1
         )
         delete from workflow.workflow_stream_chunks as chunks
          using victims
          where chunks.tableoid = victims.tableoid
            and chunks.ctid = victims.ctid
         returning 1`,
        [options.batchSize],
      );
      const batchDeletedRows = result.rows.length;
      deletedRows += batchDeletedRows;
      if (batchDeletedRows < options.batchSize) {
        await deleteExpiredCheckpoints(client);
        return {
          deletedRows,
          batches,
          hitBatchLimit: false,
          lockAcquired: true,
        };
      }
    }

    await deleteExpiredCheckpoints(client);
    return {
      deletedRows,
      batches: options.maxBatches,
      hitBatchLimit: true,
      lockAcquired: true,
    };
  } finally {
    if (lockAcquired) {
      await client
        .query("select pg_advisory_unlock($1)", [STREAM_RETENTION_LOCK_KEY])
        .catch(() => {});
    }
    client.release();
  }
}

/** Delete detailed workflow graph rows after `detail_expire_after`, retaining EOF. */
export async function pruneExpiredWorkflowRuns(
  pool: Pool,
  options: DeadlineRetentionOptions,
): Promise<WorkflowRunRetentionResult> {
  assertPositiveInteger(options.batchSize, "batchSize");
  assertPositiveInteger(options.maxBatches, "maxBatches");

  const client = await pool.connect();
  let lockAcquired = false;
  try {
    const lockResult = await client.query<{ locked: boolean }>(
      "select pg_try_advisory_lock($1) as locked",
      [RUN_RETENTION_LOCK_KEY],
    );
    lockAcquired = lockResult.rows[0]?.locked === true;
    if (!lockAcquired) return emptyRunResult(false);

    let deletedRuns = 0;
    for (let batches = 1; batches <= options.maxBatches; batches += 1) {
      await client.query("begin");
      try {
        const victims = await client.query<{ tenant_id: string; id: string }>(
          `select tenant_id, id
             from workflow.workflow_runs
            where status in ('completed', 'failed', 'cancelled')
              and detail_expire_after is not null
              and detail_expire_after <= now()
              and not exists (
                select 1
                  from workflow.workflow_runs as lineage
                 where lineage.tenant_id = workflow_runs.tenant_id
                   and lineage.retention_root_run_id = workflow_runs.retention_root_run_id
                   and (
                     lineage.status not in ('completed', 'failed', 'cancelled')
                     or lineage.retention_class = 'persistent'
                     or lineage.detail_expire_after is null
                     or lineage.detail_expire_after > now()
                   )
              )
              and not exists (
                select 1
                  from workflow.workflow_hooks as capabilities
                  join workflow.workflow_runs as owners
                    on owners.tenant_id = capabilities.tenant_id
                   and owners.id = capabilities.run_id
                 where owners.tenant_id = workflow_runs.tenant_id
                   and owners.retention_root_run_id = workflow_runs.retention_root_run_id
                   and capabilities.token_retention_until > now()
              )
            order by detail_expire_after, tenant_id, id
            limit $1
            for update skip locked`,
          [options.batchSize],
        );

        if (victims.rows.length > 0) {
          const tenants = victims.rows.map((row) => row.tenant_id);
          const runIds = victims.rows.map((row) => row.id);
          for (const { table, extraPredicate } of [
            { table: "workflow_stream_checkpoints", extraPredicate: "" },
            {
              table: "workflow_stream_chunks",
              // EOF is the durable tombstone proving an expired stream ended.
              extraPredicate: "and rows.eof = false",
            },
            { table: "workflow_events", extraPredicate: "" },
            { table: "workflow_event_slots", extraPredicate: "" },
            { table: "workflow_steps", extraPredicate: "" },
            { table: "workflow_hooks", extraPredicate: "" },
            { table: "workflow_waits", extraPredicate: "" },
          ]) {
            await client.query(
              `delete from workflow.${table} as rows
                using unnest($1::text[], $2::text[]) as victims(tenant_id, run_id)
                where rows.tenant_id = victims.tenant_id
                  and rows.run_id = victims.run_id
                  ${extraPredicate}`,
              [tenants, runIds],
            );
          }
          const deleted = await client.query(
            `delete from workflow.workflow_runs as runs
              using unnest($1::text[], $2::text[]) as victims(tenant_id, run_id)
              where runs.tenant_id = victims.tenant_id
                and runs.id = victims.run_id
              returning 1`,
            [tenants, runIds],
          );
          deletedRuns += deleted.rows.length;
        }
        await client.query(
          `delete from workflow.workflow_hooks as hooks
            where hooks.token_retention_until is not null
              and hooks.token_retention_until <= now()
              and not exists (
                select 1 from workflow.workflow_runs as runs
                 where runs.tenant_id = hooks.tenant_id
                   and runs.id = hooks.run_id
              )`,
        );
        await client.query(
          `delete from workflow.workflow_stream_checkpoints as checkpoints
            where not exists (
              select 1 from workflow.workflow_runs as runs
               where runs.tenant_id = checkpoints.tenant_id
                 and runs.id = checkpoints.run_id
            )`,
        );
        await client.query("commit");

        if (victims.rows.length < options.batchSize) {
          return {
            deletedRuns,
            batches,
            hitBatchLimit: false,
            lockAcquired: true,
          };
        }
      } catch (error) {
        await client.query("rollback").catch(() => {});
        throw error;
      }
    }
    return {
      deletedRuns,
      batches: options.maxBatches,
      hitBatchLimit: true,
      lockAcquired: true,
    };
  } finally {
    if (lockAcquired) {
      await client.query("select pg_advisory_unlock($1)", [RUN_RETENTION_LOCK_KEY]).catch(() => {});
    }
    client.release();
  }
}

/** Classify a run; the database trigger recomputes deadlines atomically. */
export async function setWorkflowRunRetentionClass(
  pool: Pool,
  options: SetWorkflowRunRetentionClassOptions,
): Promise<{ updated: boolean; retentionClass: RunRetentionClass }> {
  assertValidTenantId(options.tenantId);
  const retentionClass = resolveRunRetentionClass(options.retentionClass);
  const result = await pool.query(
    `update workflow.workflow_runs
        set retention_class = $3
      where tenant_id = $1 and id = $2
      returning 1`,
    [options.tenantId, options.runId, retentionClass],
  );
  return { updated: result.rows.length === 1, retentionClass };
}

async function deleteExpiredCheckpoints(client: {
  query(query: string): Promise<unknown>;
}): Promise<void> {
  await client.query(
    `delete from workflow.workflow_stream_checkpoints as checkpoints
      using workflow.workflow_runs as runs
      where runs.tenant_id = checkpoints.tenant_id
        and runs.id = checkpoints.run_id
        and runs.status in ('completed', 'failed', 'cancelled')
        and runs.expire_after is not null
        and runs.expire_after <= now()
        and not exists (
          select 1
            from workflow.workflow_runs as lineage
           where lineage.tenant_id = runs.tenant_id
             and lineage.retention_root_run_id = runs.retention_root_run_id
             and (
               lineage.status not in ('completed', 'failed', 'cancelled')
               or lineage.retention_class = 'persistent'
               or lineage.expire_after is null
               or lineage.expire_after > now()
             )
        )
        and not exists (
          select 1
            from workflow.workflow_hooks as capabilities
            join workflow.workflow_runs as owners
              on owners.tenant_id = capabilities.tenant_id
             and owners.id = capabilities.run_id
           where owners.tenant_id = runs.tenant_id
             and owners.retention_root_run_id = runs.retention_root_run_id
             and capabilities.token_retention_until > now()
        )`,
  );
}

async function deleteTerminalCheckpointsByAge(
  client: { query(query: string, values: unknown[]): Promise<unknown> },
  retentionMs: number,
): Promise<void> {
  await client.query(
    `delete from workflow.workflow_stream_checkpoints as checkpoints
      using workflow.workflow_runs as runs
      where runs.tenant_id = checkpoints.tenant_id
        and runs.id = checkpoints.run_id
        and runs.status in ('completed', 'failed', 'cancelled')
        and coalesce(runs.completed_at, runs.updated_at)
              < now() - ($1::bigint * interval '1 millisecond')
        and not exists (
          select 1
            from workflow.workflow_runs as lineage
           where lineage.tenant_id = runs.tenant_id
             and lineage.retention_root_run_id = runs.retention_root_run_id
             and (
               lineage.status not in ('completed', 'failed', 'cancelled')
               or lineage.retention_class = 'persistent'
               or coalesce(lineage.completed_at, lineage.updated_at)
                    >= now() - ($1::bigint * interval '1 millisecond')
             )
        )
        and not exists (
          select 1
            from workflow.workflow_hooks as capabilities
            join workflow.workflow_runs as owners
              on owners.tenant_id = capabilities.tenant_id
             and owners.id = capabilities.run_id
           where owners.tenant_id = runs.tenant_id
             and owners.retention_root_run_id = runs.retention_root_run_id
             and capabilities.token_retention_until > now()
        )`,
    [retentionMs],
  );
}

function emptyStreamResult(lockAcquired: boolean): StreamRetentionResult {
  return { deletedRows: 0, batches: 0, hitBatchLimit: false, lockAcquired };
}

function emptyRunResult(lockAcquired: boolean): WorkflowRunRetentionResult {
  return { deletedRuns: 0, batches: 0, hitBatchLimit: false, lockAcquired };
}

function assertNonNegativeInteger(value: number, name: string): void {
  if (!Number.isSafeInteger(value) || value < 0) {
    throw new TypeError(`${name} must be a non-negative safe integer`);
  }
}

function assertPositiveInteger(value: number, name: string): void {
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new TypeError(`${name} must be a positive safe integer`);
  }
}
