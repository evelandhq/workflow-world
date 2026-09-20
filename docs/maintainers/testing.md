# Testing

Run these commands from the repository root after installing dependencies with
pnpm. Replace the example URLs with disposable PostgreSQL databases whose user
can create schemas and tables; the Eve E2E suite also needs permission to create
databases. Database-backed cases in the default suite are skipped when
`EVELAND_WORKFLOW_WORLD_TEST_URL` is unset.

## Unit and integration

```bash
EVELAND_WORKFLOW_WORLD_TEST_URL=postgres://…/wfw_test pnpm test
```

## External conformance

This command builds the package and runs upstream's suite with the dispatcher
in the loop:

```bash
WORKFLOW_WORLD_CONFORMANCE_URL=postgres://…/wfw_conformance pnpm run test:conformance
```

## Eve end-to-end

Builds and drives a real Eve agent for each enabled version:

```bash
WORKFLOW_WORLD_E2E_URL=postgres://…/postgres pnpm run test:e2e
```

## Long-step lease check

Run manually, outside CI. This holds one dispatch open for 330 seconds (past
undici's 300-second fetch deadline) against the production lease settings, plus
a control that proves the renewals kept it alive. Each run takes minutes.

```bash
WORKFLOW_WORLD_LEASE_CHECK_URL=postgres://…/wfw_lease pnpm run check:long-step
```

## What the suites cover

The conformance project is the gate that matters: it runs
`@workflow/world-testing` against `runner: external`, so a green run exercises the
whole out-of-process path rather than just the storage layer. See
[conformance/README.md](../../conformance/README.md) for how it closes the loop, and
for what it structurally cannot prove. [e2e-tests/](../../e2e-tests/) is the other
half — it builds a released eve and proves eve can resolve, bundle and drive this
World, which conformance never loads an eve to check.
