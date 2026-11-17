# Getting started

[Documentation](../README.md) · [Configuration](../reference/configuration.md)

The World has two halves. A deployment loads it through the Workflow SDK and
writes runs, events and queue jobs to a shared PostgreSQL database. The
dispatcher, an always-running host process, claims those jobs and delivers them
back to the deployment each run is pinned to. Nothing is delivered until a
dispatcher is running.

## Install

Use Node.js 24+ and PostgreSQL.

```bash
npm install @evelandhq/workflow-world
```

## Provision the database and tenants

Run `workflow-world-setup` with `WORKFLOW_WORLD_URL` in its environment. It
applies the package's migrations (`migrations/*.sql`) that the database has not
had yet, starting from the baseline on an empty `workflow` schema, and
initializes Graphile; **it does not create tenant partitions**. Call `ensureTenantPartitions(pool, tenantId)` once per
tenant afterwards:

```ts
import { Pool } from "pg";
import { ensureTenantPartitions } from "@evelandhq/workflow-world";

const pool = new Pool({ connectionString: process.env.WORKFLOW_WORLD_URL });
await ensureTenantPartitions(pool, "my_tenant");
await pool.end();
```

There is no default partition, so a deployment of an unprovisioned tenant fails
on its first write.

A database set up by a 0.x version cannot be upgraded in place: see
[upgrading to 1.0](./upgrading-to-1.0.md).

### Upgrading within 1.x

A release can add a migration. Run `workflow-world-setup` again (the
dispatcher also applies pending migrations when it starts) before any
deployment or tenant provisioning runs the new version: its code reads and
writes the new columns. `0002_hook_resume_dedup.sql` adds the hook resume
columns and unique index to `workflow_events` and renames that index on every
existing tenant partition; it holds a lock that blocks event writes while it
scans each partition. Provision tenants with the new version only, since
older provisioning code can misname the new index (provisioning again with
the new version repairs it).

## Configure deployments

The SDK loads the World through these environment variables:

```bash
WORKFLOW_TARGET_WORLD=@evelandhq/workflow-world
WORKFLOW_WORLD_URL=postgres://world:world@127.0.0.1:5432/world
WORKFLOW_WORLD_TENANT_ID=my_tenant
WORKFLOW_WORLD_DEPLOYMENT_ID=my_tenant-v1
WORKFLOW_WORLD_RUNTIME_SECRET=<shared with the dispatcher>
```

Configure your framework's Workflow integration too (for Next.js,
`withWorkflow` from `workflow/next`). Each build of an app is a deployment with
its own id: a run stays pinned to the deployment that created it.

The deployment's flow route (`/.well-known/workflow/v1/flow`) accepts only
requests that carry the runtime secret and name this deployment, which only
the dispatcher sends. Keep the route off any public proxy all the same.

## Run the dispatcher

Run `npx workflow-dispatcher` as an always-running host process, with the same
`WORKFLOW_WORLD_URL` and `WORKFLOW_WORLD_RUNTIME_SECRET`. It finds a run's
deployment in one of two ways:

- `WORKFLOW_DISPATCHER_ACTIVATION_API_URL`: the host's activation API, which
  locates or wakes the run's pinned deployment and implements the lease
  contract in [the design document](../design.md#the-activation-lease). Use it
  for executors that scale to zero.
- `WORKFLOW_DISPATCHER_STATIC_ENDPOINTS`: fixed `<deploymentId>=<origin>` pairs
  for executors that are always running.

See the [configuration reference](../reference/configuration.md) for every
setting, and [dispatcher operations](../operations/dispatcher.md) for pool
sizing, readiness, and recovery.
