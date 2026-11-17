import { readFile, readdir } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import type { Pool } from "pg";
import { HOOK_RESUME_UNIQUE, PARTITIONED_TABLES } from "./drizzle/schema.js";
import {
  assertValidTenantId,
  dedupIndexName,
  derivePartitionName,
  resumeDedupIndexName,
} from "./tenant.js";

/**
 * Migrations are hand-written SQL rather than drizzle-kit output because the
 * partitioned tables are not expressible in drizzle-kit's generator, and a
 * generated file that silently drops `PARTITION BY` would be worse than no
 * generator at all.
 */
const MIGRATIONS_TABLE = "workflow.eveland_migrations";

/**
 * Chosen once and never changed: concurrent `bin/setup` invocations (dev server
 * plus test runner, or two workers booting together) must serialize, and
 * `CREATE SCHEMA IF NOT EXISTS` is famously not race-safe.
 */
export const MIGRATION_LOCK_KEY = 0x65_76_65_77; // "evew"

export function resolveMigrationsDir(): string {
  // dist/migrate.js → package root → migrations/
  return path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..", "migrations");
}

export async function runMigrations(
  pool: Pool,
  options: { migrationsDir?: string; log?: (message: string) => void } = {},
): Promise<string[]> {
  const migrationsDir = options.migrationsDir ?? resolveMigrationsDir();
  const log = options.log ?? (() => {});
  const files = (await readdir(migrationsDir)).filter((name) => name.endsWith(".sql")).sort();

  const client = await pool.connect();
  const applied: string[] = [];
  try {
    await client.query("select pg_advisory_lock($1)", [MIGRATION_LOCK_KEY]);
    await client.query("create schema if not exists workflow");
    await client.query(
      `create table if not exists ${MIGRATIONS_TABLE} (
         name varchar primary key,
         applied_at timestamptz default now() not null
       )`,
    );
    const { rows } = await client.query<{ name: string }>(`select name from ${MIGRATIONS_TABLE}`);
    const seen = new Set(rows.map((row) => row.name));

    for (const name of files) {
      if (seen.has(name)) continue;
      const sql = await readFile(path.join(migrationsDir, name), "utf8");
      // Each migration is one transaction: a half-applied schema is much harder
      // to reason about than a failed boot.
      await client.query("begin");
      try {
        await client.query(sql);
        await client.query(`insert into ${MIGRATIONS_TABLE} (name) values ($1)`, [name]);
        await client.query("commit");
      } catch (error) {
        await client.query("rollback").catch(() => {});
        throw error;
      }
      applied.push(name);
      log(`applied ${name}`);
    }
    return applied;
  } finally {
    await client.query("select pg_advisory_unlock($1)", [MIGRATION_LOCK_KEY]).catch(() => {});
    client.release();
  }
}

/**
 * Creates this tenant's partitions. Idempotent, and safe to call concurrently:
 * the duplicate_table race between two provisioning paths is treated as
 * success.
 */
export async function ensureTenantPartitions(pool: Pool, tenantId: string): Promise<void> {
  assertValidTenantId(tenantId);
  for (const table of PARTITIONED_TABLES) {
    const partition = derivePartitionName(table, tenantId);
    try {
      await pool.query(
        `create table workflow.${quoteIdentifier(partition)} ` +
          `partition of workflow.${quoteIdentifier(table)} ` +
          `for values in (${literal(tenantId)})`,
      );
    } catch (error) {
      if (sqlState(error) !== "42P07") throw error;
    }
  }
  await renameChildIndexes(pool, tenantId);
}

/**
 * The partitioned unique indexes of `workflow_events` whose violations storage
 * recognises, each with the name its child on a tenant's partition gets.
 *
 * The hook-resume index comes first. Provisioning code older than migration
 * 0002 picked "the partition's only partial unique index" for the dedup
 * rename, so with both indexes present it could have given the resume child
 * the dedup name; renaming the resume child first frees that name again.
 */
const CHILD_INDEX_NAMES: readonly { parent: string; name: (tenantId: string) => string }[] = [
  { parent: HOOK_RESUME_UNIQUE, name: resumeDedupIndexName },
  { parent: "workflow_events_entity_creation_unique", name: dedupIndexName },
];

/**
 * Give this tenant's child indexes of the unique event indexes names the
 * storage layer can predict.
 *
 * A unique violation reports the child index's name, and Postgres derives that
 * from the partition name plus columns, truncated to 63 bytes — unpredictable
 * and length-dependent. `events.create` has to recognise the conflict to
 * translate it (the correlated-event one into EntityConflictError, the dedup
 * signal the runtime expects), so each child is renamed once, here, rather
 * than matched by guesswork. Migration 0002 renamed the hook-resume children
 * of the partitions that existed before it the same way.
 *
 * Each child is found through `pg_inherits` as the index attached to its
 * parent, so the two partial unique indexes cannot be confused.
 */
async function renameChildIndexes(pool: Pool, tenantId: string): Promise<void> {
  const partition = derivePartitionName("workflow_events", tenantId);
  for (const { parent, name } of CHILD_INDEX_NAMES) {
    const target = name(tenantId);
    const { rows } = await pool.query<{ relname: string }>(
      `select ci.relname
         from pg_inherits inh
         join pg_class ci on ci.oid = inh.inhrelid
         join pg_index i on i.indexrelid = ci.oid
         join pg_class ct on ct.oid = i.indrelid
         join pg_namespace n on n.oid = ct.relnamespace
        where n.nspname = 'workflow'
          and ct.relname = $1
          and inh.inhparent = to_regclass($2)`,
      [partition, `workflow.${parent}`],
    );
    const current = rows[0]?.relname;
    if (!current || current === target) continue;
    await pool.query(
      `alter index workflow.${quoteIdentifier(current)} rename to ${quoteIdentifier(target)}`,
    );
  }
}

/**
 * Project deletion's reclaim path: DROP TABLE on a partition detaches and drops
 * in one step, returning the space immediately instead of leaving dead tuples
 * for autovacuum — the lesson from issue #213, where one project's chunk table
 * reached 14 GB.
 */
export async function dropTenantPartitions(pool: Pool, tenantId: string): Promise<void> {
  assertValidTenantId(tenantId);
  for (const table of PARTITIONED_TABLES) {
    const partition = derivePartitionName(table, tenantId);
    await pool.query(`drop table if exists workflow.${quoteIdentifier(partition)}`);
  }
  // Slot markers are intentionally not partitioned: one tiny row per run is
  // cheaper than another partition tree, but project deletion must reclaim
  // them alongside the event partition.
  await pool.query("delete from workflow.workflow_event_slots where tenant_id = $1", [tenantId]);
  await pool.query("delete from workflow.workflow_start_hook_admissions where tenant_id = $1", [
    tenantId,
  ]);
}

export async function tenantPartitionsExist(pool: Pool, tenantId: string): Promise<boolean> {
  assertValidTenantId(tenantId);
  for (const table of PARTITIONED_TABLES) {
    const partition = derivePartitionName(table, tenantId);
    const { rowCount } = await pool.query(
      "select 1 from pg_class c join pg_namespace n on n.oid = c.relnamespace where n.nspname = 'workflow' and c.relname = $1",
      [partition],
    );
    if (!rowCount) return false;
  }
  return true;
}

/**
 * `CREATE TABLE ... PARTITION OF ... FOR VALUES IN (...)` takes a literal, not a
 * bind parameter, so the tenant id is escaped explicitly. `assertValidTenantId`
 * has already restricted it to `[A-Za-z0-9_-]`, making this belt-and-braces.
 */
function literal(value: string): string {
  return `'${value.replaceAll("'", "''")}'`;
}

function quoteIdentifier(value: string): string {
  return `"${value.replaceAll('"', '""')}"`;
}

function sqlState(error: unknown): string | undefined {
  return error instanceof Error && "code" in error ? (error as { code?: string }).code : undefined;
}

/** SQLSTATE raised when a row's tenant has no partition. */
export const NO_PARTITION_SQLSTATE = "23514";
