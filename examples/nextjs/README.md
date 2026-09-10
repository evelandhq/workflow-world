# Workflow SDK Quick Start

A standalone Next.js app using the public `@evelandhq/workflow-world@0.15.2`
package, `workflow@5.0.0-beta.48`, and an embedded queue runner. No Eve,
activation service, or model credentials are needed.

## Run

Requires Node.js 24+, npm, and PostgreSQL. For a disposable local database:

```bash
docker run --rm --name eveland-workflow-quickstart \
  -e POSTGRES_USER=world -e POSTGRES_PASSWORD=world -e POSTGRES_DB=world \
  -p 127.0.0.1:5432:5432 -d postgres:18-alpine
docker exec eveland-workflow-quickstart pg_isready -U world -d world
```

Wait until PostgreSQL reports that it is accepting connections. From this
repository's root:

```bash
cd examples/nextjs
npm ci
cp .env.example .env.local
npm run setup
npm run dev
```

If using an existing database, edit `WORKFLOW_WORLD_URL` in `.env.local` before
running setup. Use a dedicated development database. Setup requires permission
to create schemas, tables, and tenant partitions. It can be run again safely.

`npm run setup` loads `.env.local` explicitly, applies the package's migrations
and Graphile initialization, then calls `ensureTenantPartitions()` for
`WORKFLOW_WORLD_TENANT_ID`. Skipping the second step leaves the tenant without
partitions and its first workflow write fails.

Next.js loads `.env.local` for development, build, and start. Keep
`WORKFLOW_TARGET_WORLD` set for all three so both compilation and runtime select
the same World. The example uses port 3000; if changing it, set `PORT` in the
shell that launches Next.js as well as `.env.local` so queue delivery and the
HTTP listener agree.

In another terminal:

```bash
curl -fsS -X POST http://localhost:3000/api/start
# {"runId":"wrun_..."}

# Substitute the returned runId. Repeat until status is completed.
curl -fsS http://localhost:3000/api/runs/wrun_...
# {"runId":"wrun_...","status":"completed","result":"Hello, World!"}
```

With the server running, `npm run smoke` automates these requests and checks
that the persisted run completes with `Hello, World!`. The CI job runs this
against a production build and the published package.

The workflow executes a step, sleeps for two seconds, and returns the greeting.
The routes start and inspect a real persisted SDK run. For a production build
check, stop the development server, then run `npm run build && npm start` and
repeat the requests. These unauthenticated demo routes are for local use.

## Configuration and versions

- `WORKFLOW_TARGET_WORLD` tells the SDK which package to load.
- `WORKFLOW_WORLD_URL` selects the shared PostgreSQL database.
- `WORKFLOW_WORLD_TENANT_ID` selects the provisioned tenant.
- `WORKFLOW_WORLD_DEPLOYMENT_ID` identifies this executor's code deployment.
- `WORKFLOW_WORLD_RUNNER=embedded` runs queue delivery inside the app.

Versions and the npm lockfile are pinned deliberately. This example exercises
Workflow 5 beta.48; it does not establish compatibility with the default
Workflow 4.x release or every newer beta.

Embedded mode needs the app to stay running to deliver jobs. For executors that
scale to zero, use external mode with a separate dispatcher and a host-provided
activation API; see the [root README](../../README.md#quick-start-workflow-sdk-without-eve)
and [dispatch contract](../../docs/design.md).

Stop Next.js with Ctrl-C. If you started the disposable container above,
`docker stop eveland-workflow-quickstart` removes it and its database.
