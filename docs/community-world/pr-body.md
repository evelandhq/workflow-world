Adds Eveland to the community Worlds manifest. Eveland is a PostgreSQL World with native tenant isolation and an out-of-process dispatcher that can wake executors through a host activation API for scale-to-zero deployments.

The entry uses embedded mode for the shared test environment. Its setup command applies schema migrations, initializes Graphile, and provisions the test tenant's partitions. The public package is `@evelandhq/workflow-world@0.15.2`; documentation is linked to the repository README.

Validation:

- Installed the public npm package with `workflow@5.0.0-beta.48` and Next.js 16.3.4 on Node.js 24; initialized PostgreSQL 18.4 and the tenant, built the app, and completed a real SDK workflow containing a step and a durable sleep (`Hello, World!`). Repeated setup successfully. This is a local verification, not an upstream CI result.
- [Repository CI on fa019125](https://github.com/evelandhq/workflow-world/actions/runs/34438951363) passed all four external-mode conformance variants, Eve 0.50.0 / 0.51.1 / 0.52.5 E2E jobs, and the tarball contract. The conformance harness pins `@workflow/world-testing@5.0.0-beta.42`; the package pins `@workflow/world@5.0.0-beta.33`, `@workflow/world-local@5.0.0-beta.42`, and tests against `@workflow/core@5.0.0-beta.48`.

This requests listing, not official compatibility certification. The community E2E job is currently disabled, and its per-world setup uses a hardcoded allowlist rather than the manifest's `setup` field. Enabling official testing for Eveland will also require adding its initialization to that allowlist. Workflow 4.x and other beta versions are not covered by the standalone verification above.
