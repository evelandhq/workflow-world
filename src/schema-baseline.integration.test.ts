import { getTableConfig, PgTable } from "drizzle-orm/pg-core";
import { Pool } from "pg";
import { afterAll, beforeAll, describe, expect, test } from "vitest";
import { Schema } from "./drizzle/index.js";
import { resolveMigrationsDir, runMigrations } from "./migrate.js";

/**
 * The SQL migrations and the drizzle schema are written by hand, separately:
 * drizzle-kit cannot express the partitioned tables. So nothing but this
 * suite checks that every table storage queries exists with exactly the
 * columns the queries name, and that the migrations create no table storage
 * does not know about.
 *
 * Set `WORKFLOW_WORLD_TEST_URL` to a scratch database to run it.
 */
const testUrl = process.env.WORKFLOW_WORLD_TEST_URL;

const declaredTables = (Object.values(Schema) as unknown[])
  .filter((value): value is PgTable => value instanceof PgTable)
  .map((table) => getTableConfig(table));

/**
 * Tables only raw SQL reads and writes: the migration bookkeeping and the
 * dispatcher's dead letters.
 */
const RAW_SQL_TABLES = ["dispatch_dead_letters", "eveland_migrations"];

describe.skipIf(!testUrl)("the migrated schema matches the drizzle schema", () => {
  let pool: Pool;

  beforeAll(async () => {
    pool = new Pool({ connectionString: testUrl, max: 1 });
    await runMigrations(pool, { migrationsDir: resolveMigrationsDir() });
  }, 60_000);

  afterAll(async () => {
    await pool?.end();
  });

  test("every declared table has exactly its declared columns", async () => {
    const { rows } = await pool.query<{
      table_name: string;
      column_name: string;
      is_nullable: "YES" | "NO";
    }>(
      `select table_name, column_name, is_nullable
         from information_schema.columns
        where table_schema = 'workflow'`,
    );
    for (const table of declaredTables) {
      const actual = rows
        .filter((row) => row.table_name === table.name)
        .map((row) => `${row.column_name} ${row.is_nullable === "NO" ? "not null" : "null"}`)
        .sort();
      const declared = table.columns
        .map((column) => `${column.name} ${column.notNull ? "not null" : "null"}`)
        .sort();
      expect(actual, table.name).toEqual(declared);
    }
  });

  test("the migrations create no table the drizzle schema does not declare", async () => {
    const { rows } = await pool.query<{ tablename: string }>(
      `select c.relname as tablename
         from pg_class c
         join pg_namespace n on n.oid = c.relnamespace
        where n.nspname = 'workflow'
          and c.relkind in ('r', 'p')
          and not c.relispartition`,
    );
    expect(rows.map((row) => row.tablename).sort()).toEqual(
      [...declaredTables.map((table) => table.name), ...RAW_SQL_TABLES].sort(),
    );
  });
});
