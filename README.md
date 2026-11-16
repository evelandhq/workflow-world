# @evelandhq/workflow-world

A multi-tenant [Workflow SDK](https://github.com/vercel/workflow) World backed by
PostgreSQL, with a dispatcher for executors that scale to zero.

A deployment never runs its own queue. Every message it sends is a job in the
shared database, and the dispatcher, an always-running host process, claims
each job and delivers it back to the deployment that owns the run, waking that
deployment through the host's activation API when it is stopped. Tenants share
one database and are isolated by partitions.

## Quick start

Requires Node.js 24+ and PostgreSQL. Install in a Workflow SDK app:

```bash
npm install @evelandhq/workflow-world
```

Follow the [integration guide](./docs/guides/getting-started.md) to provision
the database and tenants, configure deployments, and run the dispatcher. See
the [documentation index](./docs/README.md) for configuration, operations, and
architecture.

## Development

Requires Node.js 24+, the pinned pnpm version, and a disposable PostgreSQL
database for integration tests. From a fresh checkout:

```bash
git clone https://github.com/evelandhq/workflow-world.git
cd workflow-world
npm install --global pnpm@11.7.0
pnpm install --frozen-lockfile
pnpm run build
```

For a local test database (requires Docker):

```bash
docker run --rm --name workflow-world-dev \
  -e POSTGRES_USER=wfw -e POSTGRES_PASSWORD=wfw -e POSTGRES_DB=wfw_test \
  -p 127.0.0.1:5433:5432 -d postgres:17-alpine
docker exec workflow-world-dev pg_isready -U wfw -d wfw_test
```

Once PostgreSQL reports that it is accepting connections, run:

```bash
pnpm run typecheck
WORKFLOW_WORLD_TEST_URL=postgres://wfw:wfw@127.0.0.1:5433/wfw_test pnpm test
```

The tests apply migrations and provision their own tenants. With an existing
test database, substitute its URL. Stop the disposable database with
`docker stop workflow-world-dev`; this removes its test data.

See [development and contributing](./docs/maintainers/development.md) for dependency
management and checks, and [testing](./docs/maintainers/testing.md) for conformance,
Eve E2E, and long-step lease checks.

## Contributing

Open an issue or pull request describing the problem, the proposed change, and
how it was verified. Follow the [contribution guide](./docs/maintainers/development.md#before-opening-a-pull-request)
and use conventional commit messages. Release procedures are documented in
[Releasing](./docs/maintainers/releasing.md).

## License and attribution

Copyright 2026 Eveland. Licensed under [Apache-2.0](./LICENSE).

Storage, streamer, queue, and Drizzle schema modules derive from Vercel's
`@workflow/world-postgres`. [NOTICE](./NOTICE) records upstream attribution, the
base revision, and modifications; the [compatibility contract](./docs/world-postgres-beta34-compatibility.md)
defines the behavioral boundary.
