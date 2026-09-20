# Getting started

[Documentation](../README.md) · [Configuration](../reference/configuration.md)

## Install

```bash
npm install @evelandhq/workflow-world
```

## Workflow SDK integration

Use Node.js 24+ and PostgreSQL. The standalone
[Next.js example](../../examples/nextjs/README.md) installs the public npm package,
provisions a tenant, and runs a workflow with a step and a durable sleep in
`embedded` mode. It pins `workflow@5.0.0-beta.48` and `@evelandhq/workflow-world@0.15.2`
as a verified pair; the SDK's default `latest` tag is a separate release line.

The SDK loads the World through these environment variables:

```bash
WORKFLOW_TARGET_WORLD=@evelandhq/workflow-world
WORKFLOW_WORLD_URL=postgres://world:world@127.0.0.1:5432/world
WORKFLOW_WORLD_TENANT_ID=quickstart
WORKFLOW_WORLD_DEPLOYMENT_ID=local-v1
WORKFLOW_WORLD_RUNNER=embedded
```

Configure your framework's Workflow integration too (for Next.js,
`withWorkflow` from `workflow/next`). `experimental.workflow.world` is Eve's
integration setting, not the general SDK setup.

Before starting the app, run `workflow-world-setup` with the database URL in its
environment. It applies schema migrations and initializes Graphile; **it does
not create tenant partitions**. Call `ensureTenantPartitions(pool, tenantId)`
once per tenant afterwards. The example's `npm run setup` performs both steps;
see its [provisioning script](../../examples/nextjs/scripts/provision-tenant.mjs).

## External runner

Once embedded mode works, switch to `WORKFLOW_WORLD_RUNNER=external`, configure
the host activation API and shared runtime secret in the
[configuration reference](../reference/configuration.md), and run
`npx workflow-dispatcher` as an always-running host process. The activation API
must locate or wake the run's pinned deployment and implement the lease contract
in [the design document](../design.md#the-activation-lease). The dispatcher and
activation service must remain available while executors scale to zero.

See [dispatcher operations](../operations/dispatcher.md) for pool sizing,
readiness, recovery, and upgrades.
