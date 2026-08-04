# @evelandhq/workflow-world

A natively multi-tenant [Workflow SDK](https://github.com/vercel/workflow) World
backed by one shared PostgreSQL database, plus the dispatcher that drives it from
outside the executor process.

> **Experimental, `0.x`.** Not ready for production traffic — see
> [KNOWN-GAPS.md](./KNOWN-GAPS.md), starting with G1.

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

## Tests

```bash
# unit + integration; needs a Postgres it may create schemas in
EVELAND_WORKFLOW_WORLD_TEST_URL=postgres://…/wfw_test npm test

# upstream's conformance suite, in EXTERNAL mode, dispatcher in the loop
WORKFLOW_WORLD_CONFORMANCE_URL=postgres://…/wfw_conformance npm run test:conformance
```

The conformance project is the gate that matters: it runs
`@workflow/world-testing` against `runner: external`, so a green run exercises the
whole out-of-process path rather than just the storage layer. See
[conformance/README.md](./conformance/README.md) for how it closes the loop, and
for what it structurally cannot prove.

## Relationship to upstream

The storage, streamer, queue and Drizzle schema modules are derived from
`@workflow/world-postgres`, which upstream describes as "a reference
implementation — a production-ready solution might run workers in separate
processes with a more robust queuing system". This is that. Apache-2.0, matching
upstream; [NOTICE](./NOTICE) records the base revision and every change.
