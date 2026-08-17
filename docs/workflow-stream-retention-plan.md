# Shared workflow stream retention plan

> Historical rollout plan. The original 24-hour host primitive shipped first;
> storage v2 now adds snapshot stripping/rehydration, database checkpoints,
> physical blocks, and deadline-driven retention classes. See the README and
> `docs/design.md` for the current contract.

## Status and decisions

This plan ports the production-proven retention policy from
[evelandhq/eveland#214](https://github.com/evelandhq/eveland/pull/214) to the shared,
tenant-partitioned `@evelandhq/workflow-world` database.

The following decisions are fixed for this work:

- Keep terminal runs' stream data for **24 hours** (`86_400_000` ms).
- Keep each stream's `eof = true` marker after its data chunks expire.
- Treat retention as a safety boundary independent of any snapshot-compaction work.
- Keep the existing Eveland sweep for legacy per-project `@workflow/world-postgres`
  databases while adding a separate sweep for the shared World.
- Put schema-aware deletion code in `workflow-world`; put scheduling and product
  policy in `eveland`.

The reason for the split is the same one established in
[evelandhq/eveland#213](https://github.com/evelandhq/eveland/issues/213): once an Eve
run is terminal, its chunks are delivery/replay data rather than session program
memory. Retention bounds how long replay from an old raw cursor remains available.
It does not fix Eve's O(n²) snapshot-per-delta writes, the active-run peak, WAL or
historical read amplification; those remain separate work tracked by
[vercel/eve#1441](https://github.com/vercel/eve/issues/1441) and
[evelandhq/workflow-world#16](https://github.com/evelandhq/workflow-world/pull/16).

## Cross-repository contract

`workflow-world` will publish one bounded administrative primitive from its main
package export:

```ts
export type StreamRetentionOptions = {
  /** Required product policy; Eveland passes 86_400_000. */
  retentionMs: number;
  /** Rows per DELETE statement; Eveland passes 50_000 by default. */
  batchSize: number;
  /** Maximum DELETE statements in this invocation; Eveland passes 20 by default. */
  maxBatches: number;
};

export type StreamRetentionResult = {
  deletedRows: number;
  batches: number;
  hitBatchLimit: boolean;
  lockAcquired: boolean;
};

export function pruneTerminalStreamChunks(
  pool: import("pg").Pool,
  options: StreamRetentionOptions,
): Promise<StreamRetentionResult>;
```

Contract requirements:

- `retentionMs`, `batchSize`, and `maxBatches` must be finite integers;
  `retentionMs >= 0`, `batchSize > 0`, and `maxBatches > 0`. Invalid values fail
  before SQL is issued.
- Eligibility is determined with database time:
  `coalesce(completed_at, updated_at) < now() - retentionMs`.
- Only runs in `completed`, `failed`, or `cancelled` are eligible.
- The run/chunk join always uses both `tenant_id` and `run_id`.
- Only `eof = false` rows are deleted. EOF markers, runs, events, steps, hooks and
  waits are out of scope.
- Every statement deletes at most `batchSize` rows. One invocation executes at
  most `maxBatches` statements.
- Candidates are ordered oldest-first, then deterministically by tenant/chunk, so
  an invocation makes predictable progress across the backlog.
- The implementation uses a dedicated checked-out connection and
  `pg_try_advisory_lock` for the whole invocation. A concurrent caller returns
  `lockAcquired: false` without deleting anything.
- A partitioned-table row is identified by `(tableoid, ctid)`, not `ctid` alone.
- `hitBatchLimit` is true only when the last permitted batch was full; Eveland can
  use it to report that backlog probably remains.
- The function does not run on World startup and has no environment-variable
  defaults. Calling it is an explicit, destructive administrative policy choice.

The first release containing this API must land before the Eveland integration.
Do not guess its version in Eveland; use the version actually produced by the
`workflow-world` release process.

## Session A: `evelandhq/workflow-world`

### Goal

Provide the safe shared-database retention primitive and the indexes, tests and
documentation needed for Eveland to schedule it. Do not implement a timer or
Eveland environment variables in this repository.

### Implementation

1. Add a module dedicated to retention, for example `src/retention.ts`, containing
   the public types, option validation, advisory-lock lifecycle, batch loop and
   deletion query.
2. Export the types and `pruneTerminalStreamChunks` from `src/index.ts`. The main
   package export is sufficient; a new package subpath is unnecessary.
3. Add a hand-written migration for a partial expression index supporting the
   terminal-age lookup. Start with the following shape and confirm the column
   order with `EXPLAIN` against realistic data:

   ```sql
   create index if not exists workflow_runs_terminal_retention_index
     on workflow.workflow_runs (
       (coalesce(completed_at, updated_at)),
       tenant_id,
       id
     )
     where status in ('completed', 'failed', 'cancelled');
   ```

   The migration runner wraps migrations in a transaction, so do not use
   `CREATE INDEX CONCURRENTLY` without first redesigning the runner. This index is
   on `workflow_runs`, not the large chunk table; still measure its build time on
   a production-sized copy before rollout.

4. Implement each deletion as a bounded CTE. The important SQL shape is:

   ```sql
   with victims as (
     select c.tableoid, c.ctid
       from workflow.workflow_stream_chunks c
       join workflow.workflow_runs r
         on r.tenant_id = c.tenant_id
        and r.id = c.run_id
      where c.eof = false
        and r.status in ('completed', 'failed', 'cancelled')
        and coalesce(r.completed_at, r.updated_at)
              < now() - make_interval(secs => $1)
      order by coalesce(r.completed_at, r.updated_at), c.tenant_id, c.id
      limit $2
   )
   delete from workflow.workflow_stream_chunks c
    using victims v
    where c.tableoid = v.tableoid
      and c.ctid = v.ctid;
   ```

   Bind milliseconds as a PostgreSQL `bigint` and multiply by
   `interval '1 millisecond'`; do not interpolate values into SQL.
   Confirm the final query through the `pg` driver and an integration test rather
   than copying this sketch verbatim if PostgreSQL requires a small syntax change.

5. Hold a package-specific advisory-lock key on one checked-out client for the
   entire batch loop. Always unlock in `finally`, then release the client.
6. Stop when a batch deletes fewer than `batchSize` rows or when `maxBatches` is
   reached. Return the structured result above; do not log from the library.
7. Document the API and its irreversible replay consequence in `README.md` and
   `docs/design.md`. State clearly that ordinary `DELETE` makes pages reusable but
   does not necessarily return relation files to the operating system.

### Tests

Add PostgreSQL integration coverage proving all of the following:

- data chunks of completed, failed and cancelled runs older than 24 hours are
  deleted;
- recent terminal runs and pending/running runs are untouched;
- EOF rows are retained and an expired stream reads as empty-and-done;
- two tenants may use the same `run_id` without one tenant's cleanup touching the
  other;
- multiple batches run, `maxBatches` is honoured, and `hitBatchLimit` is accurate;
- a concurrent invocation fails to acquire the lock and performs no deletion;
- database time, rather than a JavaScript timestamp, determines the boundary;
- invalid options fail before issuing SQL;
- the migration creates the intended partial index and the query prunes tenant
  partitions/usefully uses the run/chunk indexes (`EXPLAIN` assertion or a
  documented manual plan check, whichever is stable in CI).

Run the normal repository gates:

```bash
npm run typecheck
npm run lint
npm run fmt:check
npm test
npm run test:conformance
```

The real-Eve E2E suite is desirable but not a blocker if it requires credentials;
record whether it was run. No stream-compaction code belongs in this PR.

### Deliverable and handoff

- Merge the `workflow-world` PR.
- Release the package through the existing release-please flow.
- Give Session B the released version and the final generated TypeScript
  declaration for `pruneTerminalStreamChunks`.

### Copy/paste prompt for Session A

> Implement the `evelandhq/workflow-world` portion of
> `docs/workflow-stream-retention-plan.md`. Stay strictly within Session A's
> scope. Add the bounded, advisory-locked shared stream-retention primitive, its
> supporting migration, tests and documentation. Preserve EOF rows and join runs
> by both tenant and run ID. Do not add a scheduler, Eveland environment variables
> or stream compaction. Run the listed verification and report the final public
> API that Eveland should consume.

## Session B: `evelandhq/eveland`

### Dependency

Start this session after Session A has published a package release. Upgrade the
worker's `@evelandhq/workflow-world` dependency to that exact released range and
update the lockfile before writing the integration.

### Goal

Schedule the new shared-World retention primitive with the existing production
policy while leaving the proven #214 legacy-database sweep operational.

### Implementation

1. Keep `apps/worker/src/runtime/workflow-world-reaper.ts` responsible for legacy
   per-project `@workflow/world-postgres` databases. It must continue to skip the
   shared platform World database.
2. Add a separate module, for example
   `apps/worker/src/runtime/shared-workflow-world-reaper.ts`, that:

   - resolves the worker-reachable shared URL from
     `EVELAND_WORKFLOW_WORLD_BOOTSTRAP_URL`, falling back to
     `EVELAND_WORKFLOW_WORLD_URL`;
   - returns a no-op result when neither variable is configured;
   - creates/reuses a small `pg.Pool` (maximum one connection is sufficient for
     this sweep);
   - calls `pruneTerminalStreamChunks` with the values below;
   - closes its pool during worker shutdown;
   - returns the structured result for logging/telemetry and does not hide errors.

3. Preserve the existing defaults and add one bound for shared-database work:

   | variable                                    |      default | use                                                |
   | ------------------------------------------- | -----------: | -------------------------------------------------- |
   | `EVELAND_WORKFLOW_STREAM_RETENTION_MS`      | `86_400_000` | shared and legacy retention window                 |
   | `EVELAND_WORKFLOW_SWEEP_INTERVAL_MS`        |  `3_600_000` | scheduler interval; `0` disables both paths        |
   | `EVELAND_WORKFLOW_SWEEP_BATCH_SIZE`         |     `50_000` | rows per DELETE statement                          |
   | `EVELAND_WORKFLOW_SHARED_SWEEP_MAX_BATCHES` |         `20` | maximum shared-DB DELETE statements per invocation |

   Sanitize finite integers before calling the library. Keep 24 hours as the
   default and operational recommendation.

4. Change the worker's scheduled sweep wrapper to run the legacy and shared paths
   independently. A failure in one must not suppress the other. Use
   `Promise.allSettled` or equivalent explicit isolation and produce one concise
   summary per path.
5. Continue running a sweep once on worker startup and then on the configured
   interval. The library advisory lock is the cross-process overlap guard; avoid
   emitting an error when another worker legitimately holds it.
6. Add low-cardinality observability:

   - deleted row count;
   - batch count;
   - duration;
   - advisory-lock skip count;
   - backlog warning when `hitBatchLimit` is true.

   Do not label metrics by tenant or run ID. Logs may include a database role
   (`legacy`/`shared`) but must not print connection strings.

7. Update `docs/environment-variables.md` and
   `packages/core/src/config-diagnostics.ts` so the existing variables explicitly
   cover both legacy and shared retention, and document the new max-batches
   variable.
8. Ensure the new `workflow-world` migration is applied to the shared database
   before a worker version capable of invoking the sweep is enabled. Use the
   existing shared-World setup/deployment path; do not make the worker mutate
   schema on every sweep.

### Tests

Add unit/orchestration tests proving:

- no shared URL means no shared sweep call;
- the bootstrap URL is preferred over the runtime URL;
- defaults passed to the library are exactly 24 hours, 50,000 rows and 20 batches;
- invalid environment values fall back according to the existing configuration
  conventions;
- legacy and shared sweeps both run and failures are isolated;
- an advisory-lock miss is a normal no-op, not an error;
- worker shutdown closes the shared retention pool and timer;
- logs/config diagnostics redact the database URL and expose the effective
  settings.

If the Eveland integration suite has a real shared Postgres fixture, add one smoke
test that provisions two tenant partitions, invokes the worker wrapper, and
confirms the 24-hour/EOF behavior. The authoritative SQL behavior remains covered
in `workflow-world` and need not be duplicated extensively.

Run the affected-package tests first, then the monorepo gates:

```bash
pnpm --filter @evelandhq/worker test
pnpm --filter @evelandhq/worker typecheck
pnpm --filter @evelandhq/core test
pnpm -r test
pnpm typecheck
pnpm fmt:check
pnpm lint
```

### Production rollout

1. Deploy/apply the new `workflow-world` migration to the shared database.
2. Deploy the Eveland worker with the shared sweep enabled and the 24-hour default.
3. On the first sweep, observe database load, WAL rate, deleted rows, batch-limit
   warnings, table/TOAST size and autovacuum progress. Do not run a concurrent
   manual cleanup.
4. If `hitBatchLimit` remains true, let hourly sweeps drain the backlog or
   temporarily raise the max-batches setting after checking database headroom.
5. Verify after at least 48 hours that retained non-EOF chunk bytes settle near a
   rolling 24-hour window instead of continuing to grow with database age.
6. Verify a cursor within 24 hours still resumes normally and a cursor older than
   24 hours returns an empty, completed stream because its EOF marker remains.
7. Treat physical disk reclaim separately. Normal deletes allow the same tenant
   partition to reuse pages but may not shrink files. Use tenant-partition
   `pg_repack`, a copy-and-swap rewrite, or project deletion's partition drop only
   when operationally necessary.

### Deliverable

- An Eveland PR that bumps the released `workflow-world` version, retains the
  legacy sweep, adds the shared sweep, configuration/docs, tests and observability.
- A rollout note recording the migration, effective settings and the first 48-hour
  storage measurements.

### Copy/paste prompt for Session B

> Implement the `evelandhq/eveland` portion of the shared stream retention plan.
> Read the plan supplied from the `workflow-world` repository, use the released
> package API from Session A, and stay strictly within Session B's scope. Preserve
> the existing #214 per-project database sweep, add an independently isolated
> shared-World sweep with a 24-hour window, wire configuration/observability and
> tests, and document the migration-first rollout. Do not reimplement the deletion
> SQL in Eveland and do not add stream compaction.

## Completion criteria across both repositories

The work is complete only when all of the following are true:

- the shared database migration is applied;
- the worker consumes a released `workflow-world` retention API;
- terminal data chunks remain replayable for 24 hours and are deleted afterward;
- EOF markers survive cleanup;
- active and recent runs are unaffected;
- same-valued run IDs in different tenants cannot cross-delete;
- one sweep invocation and concurrent workers are both bounded;
- legacy per-project databases continue to be swept;
- metrics show deletion progress and remaining backlog without high-cardinality
  labels;
- production storage is observed for at least 48 hours and demonstrates a rolling
  retention window;
- snapshot compaction remains a separate decision and can be added or removed
  without changing the retention contract.
