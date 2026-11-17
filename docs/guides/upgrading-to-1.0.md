# Upgrading to 1.0

[Documentation](../README.md) · [Getting started](./getting-started.md) ·
[Configuration](../reference/configuration.md)

1.0 cannot upgrade a database in place. Its schema starts from a single
migration, `migrations/0001_baseline.sql`, that applies only to an empty
`workflow` schema. A database set up by any 0.x version has to be recreated,
and **everything in it is lost**: runs, steps, events, hooks, waits, streams,
dead letters and queued jobs. No migration carries 0.x data forward.

If you cannot give up the runs in flight yet, stay on a maintenance line, which
still receives fixes without moving `latest`:

| line   | npm dist-tag   | for                 |
| ------ | -------------- | ------------------- |
| 0.31.x | `release-0.31` | eve 1.0.0-rc.6 pins |
| 0.27.x | `release-0.27` | eve 0.81 pins       |

```bash
npm install @evelandhq/workflow-world@release-0.31
```

## What happens if you skip the rebuild

Run against a 0.x database, 1.0's `workflow-world-setup` fails on the
baseline's first statement:

```text
Failed to set up the Eveland workflow world: error: type "status" already exists
```

(SQLSTATE `42710`). The migration runs in one transaction and rolls back, so
the database is left as it was. Do not start a 1.0 deployment or dispatcher
against that database anyway: the 1.0 code expects the baseline's tables and
columns.

## Before the cutover

1. Move deployments to eve 1.0.0 (the Workflow 5.5 runtime) and update their
   configuration as described in [configuration changes](#configuration-changes).
2. Stop starting new runs, and let the active ones finish or cancel them. Runs
   asleep or waiting on a hook count as active. To see what is left:

   ```sql
   select tenant_id, status, count(*)
     from workflow.workflow_runs
    where status in ('pending', 'running')
    group by 1, 2;
   ```

3. Back up the old schemas if you may need to read them later:

   ```bash
   pg_dump --schema=workflow --schema=graphile_worker "$WORKFLOW_WORLD_URL" > world-0.x.sql
   ```

## The cutover

1. **Stop the dispatcher and every deployment** that writes to the database.
   A 0.x process still running writes to the schema being dropped.
2. **Recreate the database.** Either point `WORKFLOW_WORLD_URL` (and
   `WORKFLOW_WORLD_BOOTSTRAP_URL`, if set) at a new empty database, which keeps
   the old one untouched, or drop both World schemas in place:

   ```sql
   drop schema workflow cascade;
   drop schema graphile_worker cascade;
   ```

   Drop `graphile_worker` as well: its queued jobs belong to the runs that were
   just deleted. Only skip it if something other than the World uses Graphile
   Worker on this database.

3. **Run the 1.0 setup** with `WORKFLOW_WORLD_URL` in its environment:

   ```bash
   npx workflow-world-setup
   ```

   It should report `applied 0001_baseline.sql` and then `Workflow world ready.`

4. **Provision every tenant again** with `ensureTenantPartitions(pool, tenantId)`.
   Dropping the schema dropped their partitions, and a deployment of an
   unprovisioned tenant fails on its first write. See
   [getting started](./getting-started.md#provision-the-database-and-tenants).
5. **Start the 1.0 dispatcher**, then **deploy each app** built with 1.0. 1.0
   has no embedded mode: nothing is delivered until the dispatcher is running.

Retire the old deployments rather than keeping them for their runs: those runs
are gone.

## Configuration changes

The World and the dispatcher read only `WORKFLOW_WORLD_*` names. Rename:

| 0.x name                               | 1.0 name                           |
| -------------------------------------- | ---------------------------------- |
| `EVELAND_WORKFLOW_WORLD_URL`           | `WORKFLOW_WORLD_URL`               |
| `EVELAND_WORKFLOW_WORLD_BOOTSTRAP_URL` | `WORKFLOW_WORLD_BOOTSTRAP_URL`     |
| `EVELAND_PROJECT_ID`                   | `WORKFLOW_WORLD_TENANT_ID`         |
| `EVELAND_DEPLOYMENT_ID`                | `WORKFLOW_WORLD_DEPLOYMENT_ID`     |
| `EVELAND_SCHEDULER_RUNTIME_SECRET`     | `WORKFLOW_WORLD_RUNTIME_SECRET`    |
| `EVELAND_WORKFLOW_STREAM_COMPACTION`   | `WORKFLOW_WORLD_STREAM_COMPACTION` |

`WORKFLOW_WORLD_STREAM_COMPACTION` exists in 1.x only: 2.0 no longer reads it
([upgrading to 2.0](./upgrading-to-2.0.md#configuration-changes)).

These are no longer read. Remove them:

- `WORKFLOW_WORLD_RUNNER` / `EVELAND_WORKFLOW_RUNNER` and
  `WORKFLOW_POSTGRES_WORKER_CONCURRENCY`: embedded mode is gone.
- `WORKFLOW_SEALED_LOG`.

`WORKFLOW_SPEC_VERSION` must be unset or `9`; any other value fails the World at
startup. The flow route refuses (401) any request without the dispatcher's
headers, so it cannot be called directly any more.

## Code changes

Only code that imports from the package needs these:

- The World config no longer takes the `runner`, `port` or `queueConcurrency`
  options, and the `WorkflowRunnerMode` type is gone.
- `RUNTIME_SECRET_ENV_NAMES` is now `RUNTIME_SECRET_ENV_NAME`, a single name.
- `reenqueueTenantRuns`, `isHookWake`, `mayBypassRunQueue`, `wakeFlag`, the
  retention-class backfill exports and the `ephemeral` retention class are
  removed.
- `BootRecoveryRun.queueNamespace` is a string, `''` for none, never `null`.
- Host reconciliation records its reason in `reconciled_reason` and no longer
  writes the run's error text column.

The [changelog](../../CHANGELOG.md) lists every breaking change.
