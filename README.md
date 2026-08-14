# @evelandhq/workflow-world

A natively multi-tenant [Workflow SDK](https://github.com/vercel/workflow) World
backed by one shared PostgreSQL database, plus the dispatcher that drives it from
outside the executor process.

> **Experimental, `0.x`.** No open gaps are known — the ones that were tracked
> are closed and covered by the suites under [Tests](#tests) — but this has
> carried no production traffic yet, and that is the risk that remains.

## What problem this solves

In the reference PostgreSQL World the queue runner lives _inside_ the process that
executes workflows. That is fine when the process is always up. It is not fine on
a platform that scales agents to zero: a durable timer is a row waiting to be
claimed, and the only thing that could claim it is the process that just got
reaped. A `sleep 1h` on a quiet project resumes when unrelated traffic happens to
wake the agent — possibly never.

So orchestration moves out and execution stays put:

```
agent deployment                 shared Postgres                  dispatcher
(executor + this World)                                           (this package's CLI)
      │                                                                  │
      │  queue.send → add_job(flags=[project:X], run_at) ───────────────► │
      │                                                    graphile claim │
      │                                            affinity: run → deployment
      │ ◄──────────────── held vqs POST (one per in-flight step) ─────────│
      │  executor runs the step; storage/events/chunks written direct     │
      │  ──────────────── POST returns → job complete ──────────────────► │
```

Postgres is the rendezvous. The dispatcher is the only actively polling party.
An idle deployment holds nothing, and the platform never loads tenant code.

## Multi-tenancy

`tenant_id` leads every primary key and every query predicate. Events and stream
chunks are LIST-partitioned per tenant, so reclaiming a tenant's storage is
`DROP PARTITION` rather than a mass delete. NOTIFY channels are tenant-scoped.
There is no default partition: an unprovisioned tenant fails loudly instead of
silently pooling its rows with everyone else's.

Prefix- or namespace-based isolation is deliberately **not** used anywhere. It has
been tried on this problem and it leaks — the failure mode is one project's runner
claiming another project's jobs.

## Two runner modes

| mode       | who runs the queue                                      | use                                                                  |
| ---------- | ------------------------------------------------------- | -------------------------------------------------------------------- |
| `embedded` | an in-process graphile runner, delivering over loopback | local development, and the mode upstream's conformance suite assumes |
| `external` | the dispatcher in this package                          | production; the reason this package exists                           |

`embedded` job names carry a per-tenant suffix. On a shared database an
in-process runner claiming a shared job name would claim other tenants' jobs, and
graphile's `forbiddenFlags` is a deny-list that cannot express "only mine". Only
`external` mode uses the shared name the dispatcher claims.

## Install

```bash
npm install @evelandhq/workflow-world
```

Point the Workflow SDK at it (`experimental.workflow.world`), then apply the
schema and provision each tenant:

```bash
npx workflow-world-setup                   # migrations
npx workflow-dispatcher                    # the long-running dispatcher
```

## Configuration

Every variable has one canonical `WORKFLOW_*` name. The `EVELAND_*` names are
accepted as aliases wherever they appear, and both ends of the system read the
same ordered list — `src/env-contract.test.ts` asserts that, because a name
honoured by only one end is a silent failure rather than a loud one.

### Deployment side (read by the World)

| variable                        | alias                              | meaning                                                                                                                                      |
| ------------------------------- | ---------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------- |
| `WORKFLOW_WORLD_URL`            | `EVELAND_WORKFLOW_WORLD_URL`       | the shared database. Required; there is no fallback chain, because falling back onto a single-tenant database is worse than failing to start |
| `WORKFLOW_WORLD_TENANT_ID`      | `EVELAND_PROJECT_ID`               | this deployment's tenant                                                                                                                     |
| `WORKFLOW_WORLD_DEPLOYMENT_ID`  | `EVELAND_DEPLOYMENT_ID`            | recorded on every run, so an in-flight run stays pinned to an executor that can still run it                                                 |
| `WORKFLOW_WORLD_RUNNER`         | `EVELAND_WORKFLOW_RUNNER`          | `embedded` (default) or `external`                                                                                                           |
| `WORKFLOW_WORLD_RUNTIME_SECRET` | `EVELAND_SCHEDULER_RUNTIME_SECRET` | authenticates platform dispatch                                                                                                              |

### Host side (read by the dispatcher)

| variable                                      | default            | meaning                                                                                                  |
| --------------------------------------------- | ------------------ | -------------------------------------------------------------------------------------------------------- |
| `WORKFLOW_WORLD_URL`                          | —                  | same database as above                                                                                   |
| `WORKFLOW_WORLD_BOOTSTRAP_URL`                | —                  | override when the host and the containers reach one database by different hostnames                      |
| `WORKFLOW_DISPATCHER_ACTIVATION_API_URL`      | —                  | the host's activation API. Required                                                                      |
| `WORKFLOW_DISPATCHER_ACTIVATION_TOKEN`        | —                  | bearer token for it. Required unless `NODE_ENV=development`                                              |
| `WORKFLOW_DISPATCHER_POOL_SIZE`               | `10`               | the authority: graphile takes one connection per running job plus one held for LISTEN                    |
| `WORKFLOW_DISPATCHER_CONCURRENCY`             | `poolSize - 1`     | must be below the pool size, and is checked                                                              |
| `WORKFLOW_DISPATCHER_POLL_INTERVAL_MS`        | `500`              |                                                                                                          |
| `WORKFLOW_DISPATCHER_MAX_INFLIGHT_PER_TENANT` | derived from cores | fairness ceiling, not a throttle                                                                         |
| `WORKFLOW_DISPATCHER_DISPATCH_TIMEOUT_MS`     | `900000`           | a backstop against a wedged executor. Liveness is the lease renewal's job                                |
| `WORKFLOW_DISPATCHER_ACTIVATION_LEASE_TTL_MS` | `180000`           | must match what the host's control API issues                                                            |
| `WORKFLOW_DISPATCHER_LEASE_RENEW_INTERVAL_MS` | `TTL / 3`          | must be well below the TTL, and is checked. Transient failures are absorbed while the lease has headroom |
| `WORKFLOW_DISPATCHER_QUEUE_GC_INTERVAL_MS`    | `300000`           | reclaims the per-run graphile queue rows; graphile does not free them on its own                         |

The dispatcher binds no port. Readiness is the literal line
`workflow-dispatcher: ready` on stdout — a stable contract, matched by supervisors
and by the conformance harness.

`WORKFLOW_QUEUE_NAMESPACE` is eve's, read by eve's own resolver on the deployment
side only. Do not set it on the host: the dispatcher must take the namespace from
the run it is recovering, never from its own environment.

### Administrative stream retention

The package exposes a bounded cleanup primitive for hosts that need to cap stream
snapshot growth:

```ts
import { Pool } from "pg";
import { pruneTerminalStreamChunks } from "@evelandhq/workflow-world";

const pool = new Pool({ connectionString: process.env.WORKFLOW_WORLD_URL });
const result = await pruneTerminalStreamChunks(pool, {
  retentionMs: 24 * 60 * 60 * 1_000,
  batchSize: 50_000,
  maxBatches: 20,
});
```

Only non-EOF chunks whose run has been terminal for longer than the requested
window are deleted. Runs, events and EOF markers remain. The operation uses a
database advisory lock, so `lockAcquired: false` is a normal result when another
host is already sweeping; `hitBatchLimit: true` means the bounded invocation may
have left more eligible rows.

Calling this function is an explicit, destructive replay policy: a raw stream
cursor older than the retention window can no longer replay its expired chunks.
The package does not schedule it or choose a default. Apply package migrations
before invoking it. Ordinary PostgreSQL `DELETE` makes pages reusable but does
not necessarily shrink relation files on disk.

### Upgrading from 0.3.0 or earlier

Runs created before this version have no recorded queue namespace, and boot
recovery can only fall back to the default `__wkf_workflow_` prefix for them. For
a namespaced deployment that fallback is refused with `400 Unhandled queue`, so
**drain or cancel active runs before cutting over**. The dispatcher logs every
run it recovers without a recorded namespace, and a
`runsWithUnknownQueueNamespace` count alongside the boot-recovery summary, so an
incomplete drain is visible rather than silent. See
[Upgrading past 0.3.0](docs/design.md#upgrading-past-030).

## Tests

```bash
# unit + integration; needs a Postgres it may create schemas in
EVELAND_WORKFLOW_WORLD_TEST_URL=postgres://…/wfw_test npm test

# upstream's conformance suite, in EXTERNAL mode, dispatcher in the loop
WORKFLOW_WORLD_CONFORMANCE_URL=postgres://…/wfw_conformance npm run test:conformance
```

```bash
# a real eve agent, built and driven for each supported eve version
WORKFLOW_WORLD_E2E_URL=postgres://…/postgres npm run test:e2e
```

```bash
# by hand, not in CI: one dispatch held open for 200s against the production
# lease settings, plus the control that proves the renewals are what kept it
# alive. Minutes per run, which is why it is not in the matrix.
WORKFLOW_WORLD_LEASE_CHECK_URL=postgres://…/wfw_lease npm run check:long-step
```

The conformance project is the gate that matters: it runs
`@workflow/world-testing` against `runner: external`, so a green run exercises the
whole out-of-process path rather than just the storage layer. See
[conformance/README.md](./conformance/README.md) for how it closes the loop, and
for what it structurally cannot prove. [e2e-tests/](./e2e-tests/) is the other
half — it builds a released eve and proves eve can resolve, bundle and drive this
World, which conformance never loads an eve to check.

## Following eve

An eve release is almost never a reason to do anything here. What matters is not
that eve shipped, but whether the `@workflow/*` set it installs moved. The
current supported window, 0.34.0 through 0.37.1, contains two sets: 0.34.0 uses
world beta.25 and world-local beta.34, while 0.35.0 onward uses beta.26 and
beta.35. Core stays on beta.41 throughout. Exact patches still matter: Workflow
pins have moved within an eve minor line before, so a minor is not a set.

Two versions with very different cadences are easy to conflate:

- **`eve` is a devDependency.** It exists so `src/eve-pin-contract.test.ts` has
  something to read the expected `@workflow/*` versions out of. Consumers never
  resolve it, so bumping it alone is not a reason to release — it can ride along
  with the next real change.
- **The `@workflow/*` runtime packages are real dependencies.** When those move,
  what consumers resolve moves with them, so the change belongs in the next
  release even if that release is intentionally deferred.

The check runs weekly and files an issue only when a newer eve actually moves the
set. Expect it to be silent for months:

```bash
npm run check:eve-drift
```

When it does fire, it is a heads-up rather than a deadline. Nothing can be
deployed until Eveland's `packages/core/src/eve-compatibility.ts` verifies a
version on that line, and the pin here should follow **that** version rather than
npm's `latest` — the two are routinely different. Then bump the devDependency and
let the contract test name what has to move with it. The 0.37.1 alignment kept
`specVersion` and the package's major/prerelease line stable, but beta.26 made
the `EventResult` preload fields an all-or-none group and added optional slot
event ids. Neither detail is visible from version numbers alone, so typecheck and
run a real eve build against the candidate set before believing a bump is inert.

The shape of the `specVersion` check is itself not fixed. Through eve 0.33.1 it
is literal equality against the runtime's `SPEC_VERSION_CURRENT`;
`@workflow/core` beta.41, which eve 0.33.2 is the first release to install,
widened it to the range `[SPEC_VERSION_CURRENT, SPEC_VERSION_MAX_SUPPORTED]` and
ships it as `>= 5 && <= 6`. That is a loosening and cannot newly reject anything,
but it loosens in one direction only: the floor is still the runtime's current
version, so a World pinned behind it fails just as it did before. The headroom
above is for a World that opts into a higher version than the default —
`world-vercel` declares the slot-identity version so its runs get slot event ids
— which this World does not do.

## Releasing

Releases are cut by [release-please](https://github.com/googleapis/release-please),
which reads the conventional-commit history on `main` and keeps one open pull
request holding the next version bump and its `CHANGELOG.md` entry. Nothing is
published while that PR sits there.

**Merging the release PR is the decision to release.** That merge tags the
commit, cuts a GitHub release, and only then does the publish job run. Publishing
on every merge to `main` would make cutting a version a side effect of merging a
fix, which is the wrong default for a package this young.

The publish itself uses npm
[trusted publishing](https://docs.npmjs.com/trusted-publishers/): the workflow
authenticates over OIDC, so there is no `NPM_TOKEN` in this repository to store,
rotate or leak. npm attaches a provenance attestation automatically, because the
repository and the package are both public.

Version bumps follow conventional commits, with `bump-minor-pre-major` set: while
below `1.0.0`, `feat:` and breaking changes both move the minor, and `fix:` moves
the patch.

### One-time setup

Trusted publishing cannot be configured for a package that does not exist yet, so
the first version has to be published by hand:

```bash
npm publish
```

Then, on npmjs.com, add a trusted publisher for the package — repository
`evelandhq/workflow-world`, workflow `release.yml` — and every release after that
comes from the pipeline with no credentials involved.

## Design

[docs/design.md](./docs/design.md) is why the architecture is shaped this way:
the two failures that forced orchestration out of the executor process, the
invariants that hold the tenancy boundary together, the dispatch contract and its
failure semantics, and what is deliberately left for later.

## Relationship to upstream

The storage, streamer, queue and Drizzle schema modules are derived from
`@workflow/world-postgres`, which upstream describes as "a reference
implementation — a production-ready solution might run workers in separate
processes with a more robust queuing system". This is that. Apache-2.0, matching
upstream; [NOTICE](./NOTICE) records the base revision and every change.
