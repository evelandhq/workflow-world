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

## Quick Start (Workflow SDK, without Eve)

Use Node.js 24+ and PostgreSQL. The standalone
[Next.js example](./examples/nextjs/README.md) installs the public npm package,
provisions a tenant, and runs a workflow with a step and a durable sleep in
`embedded` mode. It pins `workflow@5.0.0-beta.48` to match this World's runtime
dependencies; the SDK's default `latest` tag is a separate release line.

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
see its [provisioning script](./examples/nextjs/scripts/provision-tenant.mjs).

Once embedded mode works, switch to `WORKFLOW_WORLD_RUNNER=external`, configure
the host activation API and shared runtime secret described below, and run
`npx workflow-dispatcher` as an always-running host process. The activation API
must locate or wake the run's pinned deployment and implement the lease contract
in [the design document](./docs/design.md). The dispatcher and activation service
must remain available while executors scale to zero.

## Configuration

Every variable has one canonical `WORKFLOW_*` name. The `EVELAND_*` names are
accepted as aliases wherever they appear, and both ends of the system read the
same ordered list — `src/env-contract.test.ts` asserts that, because a name
honoured by only one end is a silent failure rather than a loud one.

### Deployment side (read by the World)

| variable                           | alias                                | meaning                                                                                                                                      |
| ---------------------------------- | ------------------------------------ | -------------------------------------------------------------------------------------------------------------------------------------------- |
| `WORKFLOW_WORLD_URL`               | `EVELAND_WORKFLOW_WORLD_URL`         | the shared database. Required; there is no fallback chain, because falling back onto a single-tenant database is worse than failing to start |
| `WORKFLOW_WORLD_TENANT_ID`         | `EVELAND_PROJECT_ID`                 | this deployment's tenant                                                                                                                     |
| `WORKFLOW_WORLD_DEPLOYMENT_ID`     | `EVELAND_DEPLOYMENT_ID`              | recorded on every run, so an in-flight run stays pinned to an executor that can still run it                                                 |
| `WORKFLOW_WORLD_RUNNER`            | `EVELAND_WORKFLOW_RUNNER`            | `embedded` (default) or `external`                                                                                                           |
| `WORKFLOW_WORLD_RUNTIME_SECRET`    | `EVELAND_SCHEDULER_RUNTIME_SECRET`   | authenticates platform dispatch                                                                                                              |
| `WORKFLOW_WORLD_STREAM_COMPACTION` | `EVELAND_WORKFLOW_STREAM_COMPACTION` | `on` (default); `off` is the emergency switch for write-side snapshot stripping                                                              |

### Host side (read by the dispatcher)

| variable                                                 | default            | meaning                                                                                                  |
| -------------------------------------------------------- | ------------------ | -------------------------------------------------------------------------------------------------------- |
| `WORKFLOW_WORLD_URL`                                     | —                  | same database as above                                                                                   |
| `WORKFLOW_WORLD_BOOTSTRAP_URL`                           | —                  | override when the host and the containers reach one database by different hostnames                      |
| `WORKFLOW_DISPATCHER_ACTIVATION_API_URL`                 | —                  | the host's activation API. Required                                                                      |
| `WORKFLOW_DISPATCHER_ACTIVATION_TOKEN`                   | —                  | bearer token for it. Required unless `NODE_ENV=development`                                              |
| `WORKFLOW_DISPATCHER_POOL_SIZE`                          | `10`               | claim/complete throughput, plus one connection held for Graphile LISTEN and one for dispatcher ownership |
| `WORKFLOW_DISPATCHER_CONCURRENCY`                        | `poolSize - 2`     | held dispatches in flight across all tenants. Independent of the pool — see below                        |
| `WORKFLOW_DISPATCHER_POLL_INTERVAL_MS`                   | `500`              |                                                                                                          |
| `WORKFLOW_DISPATCHER_MAX_INFLIGHT_PER_TENANT`            | derived from cores | fairness ceiling, not a throttle                                                                         |
| `WORKFLOW_DISPATCHER_DISPATCH_TIMEOUT_MS`                | `900000`           | a backstop against a wedged executor. Liveness is the lease renewal's job                                |
| `WORKFLOW_DISPATCHER_ACTIVATION_LEASE_TTL_MS`            | `180000`           | must match what the host's control API issues                                                            |
| `WORKFLOW_DISPATCHER_LEASE_RENEW_INTERVAL_MS`            | `TTL / 3`          | must be well below the TTL, and is checked. Transient failures are absorbed while the lease has headroom |
| `WORKFLOW_DISPATCHER_QUEUE_GC_INTERVAL_MS`               | `300000`           | reclaims the per-run graphile queue rows; graphile does not free them on its own                         |
| `WORKFLOW_DISPATCHER_BOOT_RECOVERY_DEPLOYMENTS_PER_WAVE` | `4`                | deployments whose recovered runs are released together at boot                                           |
| `WORKFLOW_DISPATCHER_BOOT_RECOVERY_WAVE_INTERVAL_MS`     | `30000`            | delay between those waves; `0` releases every recovered run at once                                      |
| `WORKFLOW_DISPATCHER_EXECUTOR_FAILURE_LIMIT`             | `5`                | consecutive executor `5xx` on one run before it is dead-lettered instead of retried to exhaustion        |
| `WORKFLOW_DISPATCHER_EXECUTOR_FAILURE_MIN_SPAN_MS`       | `60000`            | the streak must also last this long, so a short database outage does not quarantine healthy runs         |
| `WORKFLOW_DISPATCHER_MAINTENANCE_INTERVAL_MS`            | `60000`            | storage maintenance cadence; `0` disables the automatic loop                                             |
| `WORKFLOW_DISPATCHER_MAINTENANCE_STREAM_BATCH_SIZE`      | `50000`            | maximum physical stream rows deleted by one statement                                                    |
| `WORKFLOW_DISPATCHER_MAINTENANCE_MAX_BATCHES`            | `20`               | maximum stream/run deletion batches per pass                                                             |
| `WORKFLOW_DISPATCHER_MAINTENANCE_MAX_STREAMS_TO_PACK`    | `100`              | maximum terminal streams rewritten into blocks per pass                                                  |
| `WORKFLOW_DISPATCHER_MAINTENANCE_RUN_BATCH_SIZE`         | `1000`             | maximum expired workflow graphs deleted by one statement                                                 |
| `WORKFLOW_WORLD_STREAM_COMPACTION`                       | `on`               | also controls snapshot stripping during terminal block rewrites                                          |
| `WORKFLOW_DISPATCHER_OWNERSHIP_LIVENESS_MS`              | `90000`            | how long a dead dispatcher may keep the ownership lock before the server reclaims it — see below         |
| `WORKFLOW_DISPATCHER_OWNERSHIP_RETRY_INTERVAL_MS`        | `5000`             | retry cadence while another dispatcher holds the lock                                                    |
| `WORKFLOW_DISPATCHER_OWNERSHIP_WAIT_MS`                  | unbounded          | give up on the lock after this long; `0` fails on the first miss, as versions before 0.16 always did     |

#### Sizing the dispatcher pool

The pool and the concurrency are independent knobs. Graphile checks a connection
out of the pool for `getJob` and for `completeJob`, and returns it in between:
`makeWithPgClientFromPool` acquires around a callback and releases in its
`finally`, and the task handler is invoked outside that callback. A dispatch held
open for minutes waiting on an executor therefore occupies **no** connection —
only a socket and an in-flight lease renewal.

Two connections in the pool are held for the process lifetime: the lifecycle
advisory lock (session-scoped, so its client cannot be returned) and Graphile's
LISTEN. Everything above that is transient, so size the pool against how fast
jobs are claimed and completed, not against how many are running.

Measured on graphile-worker 0.16.6 at `concurrency=50`, median wall-clock:

| pool | 50 held dispatches (2s each) | 500 fast dispatches (50ms each) | 1000 instant jobs |
| ---- | ---------------------------- | ------------------------------- | ----------------- |
| 3    | 2078ms                       | 724ms                           | 1282ms            |
| 4    | 2070ms                       | 643ms                           | 857ms             |
| 6    | 2065ms                       | 597ms                           | 626ms             |
| 10   | 2043ms                       | 599ms                           | 381ms             |
| 16   | 2057ms                       | 655ms                           | 290ms             |
| 52   | 2076ms                       | 659ms                           | 211ms             |

Held dispatches — the real workload — are flat: pool 3 and pool 52 finish in the
same time against a 2000ms floor. Only the rightmost column, where handlers do
nothing at all and the job loop is pure SQL, rewards a large pool, and no real
dispatch behaves that way. The default of 10 sits at the knee for realistic
dispatch rates; the useful range is 6–16 regardless of concurrency.

So to run more dispatches at once, raise `WORKFLOW_DISPATCHER_CONCURRENCY` and
leave the pool alone. Earlier versions rejected a concurrency above
`poolSize - 2`, on the belief that a running job holds a connection; that bound
is gone, and only a floor of 4 on the pool itself remains.

The dispatcher binds no port. Readiness is the literal line
`workflow-dispatcher: ready` on stdout — a stable contract, matched by supervisors
and by the conformance harness.

The current dispatcher is deliberately single-instance. On startup it holds a
PostgreSQL advisory lock for its whole lifetime, reclaims only the old Graphile
worker ids found on active runs' exact `wfrun:<tenant>:<run>` queues, re-enqueues
the active runs that have no job left on their queue, and only then starts its
worker pool and reports ready. A run whose job is still queued — a pending
delivery, a sleep's timer, or the delivery the dead dispatcher was holding — is
left to that job, and a run that holds a hook is left to that hook's
resolution; re-enqueueing beside either only bought a cold start per run. The
runs it does re-enqueue are released a few deployments at a time
(`WORKFLOW_DISPATCHER_BOOT_RECOVERY_DEPLOYMENTS_PER_WAVE` every
`WORKFLOW_DISPATCHER_BOOT_RECOVERY_WAVE_INTERVAL_MS`), so a restart does not ask
the host to cold-start every deployment with an active run at once. A second
dispatcher pointed at the same database fails closed instead of sharing claims.
When first upgrading from a version that did not take this ownership lock, stop
the old dispatcher before starting the new one; the new lock cannot fence a
binary that never participates in it.

#### Ownership liveness

A session-level advisory lock is released when its session ends, and PostgreSQL
only learns that a session has ended when the socket closes. A dispatcher that
crashes on the same host as the database closes its socket at once. One whose
host lost power, or whose connection crosses a NAT, VPN, or port forward that
swallows the close, leaves an idle backend holding the lock until the kernel's
TCP keepalive gives up — over two hours at Linux defaults — and every restart in
that window would find the database owned by a ghost.

So the owning session is made to prove it is alive, in two independent layers
derived from one budget, `WORKFLOW_DISPATCHER_OWNERSHIP_LIVENESS_MS` (`L`):

- server-side TCP keepalives on the owning session (`tcp_keepalives_idle = L/3`,
  `tcp_keepalives_interval = L/9`, three probes), which notice a vanished peer
  within `2L/3` with no cooperation from the client;
- `idle_session_timeout = L` on the owning session (PostgreSQL 14+) plus a
  `select 1` heartbeat every `L/3`, so a session that stops heartbeating — a
  wedged process, a black-holed network — is terminated by the server itself.

The heartbeat is also the client's own view of the same fact: when it fails, or
the server ends the session (an operator's `pg_terminate_backend`, the timeout
above, a restart), the dispatcher reports `ownership_lost` through its lifecycle
callback and stops itself. It no longer owns the database and must not claim; the
host's supervisor starts a fresh process, which acquires the lock anew. A server
that refuses any of the settings (an older major, a platform without keepalive
control) logs `workflow_dispatcher.ownership_liveness_unavailable` at startup
and runs with whatever layers it accepted.

A dispatcher that finds the lock held now waits for it instead of exiting: it
retries every `WORKFLOW_DISPATCHER_OWNERSHIP_RETRY_INTERVAL_MS`, logging
`workflow_dispatcher.ownership_wait` with the holder's pid, `application_name`,
client address and connection time from `pg_locks` each time, and starts the
moment the holder is gone. That is what a restart during the liveness window
looks like — a few warnings, then `ready` — rather than a crash loop that trips
the supervisor's start limit. Set `WORKFLOW_DISPATCHER_OWNERSHIP_WAIT_MS` to
bound the wait; `0` restores the old fail-fast behaviour. The holder is also
readable programmatically through `readDispatcherOwnershipHolder`, and
`terminateDispatcherOwnershipHolder` is the escape hatch for a holder the layers
cannot reach (a pre-0.16 dispatcher that never set them).

An exhausted or terminal dispatch is written to `workflow.dispatch_dead_letters`,
and so is a run whose executor keeps answering `5xx` delivery after delivery (see
`WORKFLOW_DISPATCHER_EXECUTOR_FAILURE_LIMIT`). While that row is unresolved, the
still-active workflow run is quarantined: boot recovery skips it and live dispatch
drops its messages with a log line; resolving it makes the run deliverable again.
The dispatcher does not manufacture a workflow `run_failed` event or stream EOF for
a transport failure, so operators can choose between replay and an explicit workflow
cancel/fail action without losing the original message. Taking the second choice
resolves the letters by itself: a run reaching a terminal status resolves every
unresolved letter it left behind, because neither the replay nor the quarantine
those rows stand for can mean anything once the run is over.

The host has two seams over this machinery. `reconcileWorkflowRuns` (root export)
is the supported write path for runs whose executor the host knows is gone for
good: it moves selected `pending`/`running` runs to `failed`/`cancelled` with the
World's own terminal semantics, or quarantines them behind an unresolved dead
letter whose payload is replayable exactly like an organic one. And
`startDispatcherService({ filterBootRecoveryRuns })` lets the host decide, per
boot, which of the sweep's candidates are worth replaying — a run bound to a
Deployment that is not activatable can be skipped without being settled; it stays
active and is offered again on the next boot.

`WORKFLOW_QUEUE_NAMESPACE` is eve's, read by eve's own resolver on the deployment
side only. Do not set it on the host: the dispatcher must take the namespace from
the run it is recovering, never from its own environment.

### Stream storage safety boundary

Eve-compatible stream bytes stay unchanged at the public boundary, while the
database uses three internal optimizations:

- `messageSoFar` and `reasoningSoFar` are stripped before persistence and
  reconstructed from deltas on every read path. Unknown framing and event shapes
  pass through unchanged; the deployment-side switch above disables only new
  stripping, while readers always support mixed old/new streams.
- rehydration state is checkpointed in PostgreSQL every 128 logical chunks or
  64 KiB. Cursor resumes start from the nearest checkpoint rather than scanning
  the stream from its beginning. Checkpoint state never enters the client cursor.
- `writeMulti` packs up to 64 logical chunks into a physical v2 block, capped at
  256 KiB. Readers expand legacy rows and v2 blocks into the same logical stream.
  `packTerminalStreamBlocks` is the bounded, advisory-locked fallback for streams
  written one row at a time before EOF.

Logical chunk IDs remain inside each block and continue to drive cursor and
`startIndex` behavior. Repacking physical rows therefore does not invalidate an
existing cursor.

### Retention classes and maintenance

Every run has one internal retention class:

| class / outcome                      | compact after | expire non-EOF stream data | expire workflow graph |
| ------------------------------------ | ------------: | -------------------------: | --------------------: |
| `scheduled` / `ephemeral`, completed |      1 minute |                 15 minutes |              24 hours |
| `scheduled` / `ephemeral`, failed    |      1 minute |                     1 hour |                7 days |
| `scheduled` / `ephemeral`, cancelled |      1 minute |                     1 hour |                3 days |
| `interactive` (default), any outcome |     5 minutes |                   24 hours |               30 days |
| `persistent`                         |         never |                      never |                 never |

EOF rows survive both stream expiry and workflow-graph expiry, so an old stream
still resolves as complete. Active or waiting runs have no deadlines. A database
trigger assigns deadlines when a run enters a terminal state; classification can
be supplied on run creation, through the `workflow-world.retention-class` run
attribute, or with `setWorkflowRunRetentionClass`.

Run creation resolves the class once, in this order: explicit `retentionClass`,
the public attribute, Workflow SDK root/parent lineage, a platform-owned root
invocation context, then the `interactive` default. Lineage is tenant-scoped and
workflow-name agnostic, so Eve turn, timeout, task, subagent, and custom child
workflows inherit the stored root class. A delivery to an existing session also
uses that stored lineage; a new scheduled delivery cannot widen or shorten an
existing conversation's policy. The resolved root is materialized on every run
for indexed graph-level maintenance. Unresolvable lineage is rejected instead
of silently changing class.

The dispatcher runs block packing, deadline-driven stream expiry, and full graph
expiry once at startup and every minute. Each task is bounded, advisory-locked,
non-overlapping, and failure-isolated. A lineage remains ineligible while any
member is active, has a later deadline, is persistent, or owns a hook token whose
reservation has not expired. This protects terminal parents while background
children, approvals, callbacks, or task-input capabilities remain live.

The legacy caller-selected primitive remains available for hosts that do not run
the dispatcher:

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

Only non-EOF chunks whose complete lineage has been terminal for longer than the
requested window are deleted. Persistent members and unexpired hook capabilities
hold the lineage. Runs, events and EOF markers remain. The operation uses a
database advisory lock, so `lockAcquired: false` is a normal result when another
host is already sweeping; `hitBatchLimit: true` means the bounded invocation may
have left more eligible rows.

Calling any stream-expiry function is an explicit, destructive replay policy: a raw stream
cursor older than the retention window can no longer replay its expired chunks.
Apply package migrations before enabling maintenance. Ordinary PostgreSQL
`DELETE` makes pages reusable but does not necessarily shrink relation files on
disk.

#### Previewing and repairing historical scheduler graphs

Historical repair requires an exact, durable root attribute. It never infers a
class from a workflow name or title. Preview first; the result is grouped by
tenant, resolved root trigger, run type, workflow name, status, and current class:

```ts
import { Pool } from "pg";
import {
  backfillWorkflowRunRetentionClass,
  inspectWorkflowRunRetentionMismatches,
  previewWorkflowRunRetentionBackfill,
} from "@evelandhq/workflow-world";

const pool = new Pool({ connectionString: process.env.WORKFLOW_WORLD_URL });
const selector = {
  tenantId: "proj_example",
  rootAttribute: "$eve.trigger",
  rootValue: "channel:eveland-scheduler",
  retentionClass: "scheduled" as const,
};

console.log(await previewWorkflowRunRetentionBackfill(pool, selector));
console.log(await inspectWorkflowRunRetentionMismatches(pool, { ...selector, limit: 100 }));
```

Apply repeats one tenant-safe transaction at a time. Active runs are selected
before terminal history, existing `persistent` rows are never changed, and the
database trigger recomputes terminal deadlines from the original completion
timestamp:

```ts
for (;;) {
  const result = await backfillWorkflowRunRetentionClass(pool, {
    ...selector,
    batchSize: 1_000,
  });
  console.log(result);
  if (!result.hitBatchLimit) break;
}
await pool.end();
```

After reclassification, use the normal bounded dispatcher maintenance rather
than an unbounded delete. Report the backfill counts and maintenance deletion
counts separately; dead tuples and relation size require PostgreSQL statistics,
and ordinary deletion is expected to reuse a high-water mark rather than shrink
the file immediately.

### Upgrading from 0.3.0 or earlier

Runs created before this version have no recorded queue namespace, and boot
recovery can only fall back to the default `__wkf_workflow_` prefix for them. For
a namespaced deployment that fallback is refused with `400 Unhandled queue`, so
**drain or cancel active runs before cutting over**. The dispatcher logs every
run it recovers without a recorded namespace, and a
`runsWithUnknownQueueNamespace` count alongside the boot-recovery summary, so an
incomplete drain is visible rather than silent. See
[Upgrading past 0.3.0](docs/design.md#upgrading-past-030).

## Development

Use Node.js 24 or newer and the pnpm version pinned in `package.json`:

```bash
npm install --global pnpm@11.7.0
pnpm install --frozen-lockfile
pnpm run build
```

Commit dependency changes in `pnpm-lock.yaml`; use `pnpm add` or `pnpm install`
when updating dependencies. `pnpm-workspace.yaml` allows install scripts only
for the native dependencies listed there. Review that list when adding a package
that needs an install script. The release-age exception for `eve@0.52.5` covers
the existing tested pin; review any new exception when updating Eve.

CI uses pnpm for repository dependencies, builds, and tests. Tarball consumer
checks and the E2E agent fixtures keep npm to match the production installation
path; releases also keep `npm publish` for trusted publishing. Run these commands
with pnpm available on `PATH`, since `prepack` invokes the pnpm build script.

## Tests

```bash
# unit + integration; needs a Postgres it may create schemas in
EVELAND_WORKFLOW_WORLD_TEST_URL=postgres://…/wfw_test pnpm test

# upstream's conformance suite, in EXTERNAL mode, dispatcher in the loop
WORKFLOW_WORLD_CONFORMANCE_URL=postgres://…/wfw_conformance pnpm run test:conformance
```

```bash
# a real eve agent, built and driven for each supported eve version
WORKFLOW_WORLD_E2E_URL=postgres://…/postgres pnpm run test:e2e
```

```bash
# by hand, not in CI: one dispatch held open for 200s against the production
# lease settings, plus the control that proves the renewals are what kept it
# alive. Minutes per run, which is why it is not in the matrix.
WORKFLOW_WORLD_LEASE_CHECK_URL=postgres://…/wfw_lease pnpm run check:long-step
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
current supported window, {0.49.x, 0.50.x}, contains a single set: both lines
use world beta.32, world-local beta.41 and core beta.47. (0.48 was superseded
within hours and never deployed; 0.47.x, whose set was world beta.28,
world-local beta.37 and core beta.43, left the window with eve 0.50.0.) Exact
patches still matter: Workflow pins have moved within an eve minor line before,
so a minor is not a set.

The other axis an eve release can move is the message stream, which the World
touches through snapshot stripping and rehydration. eve 0.50.0 took it to v25,
where appends carry a delta and no cumulative snapshot -- the same shape a
compacted v24 row already had, so nothing here changed. Rehydration stays
mandatory while 0.49.x is deployable, because a v24 runtime serves persisted
appends verbatim; a v25 runtime normalizes the rebuilt snapshot away again
before the wire.

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
pnpm run check:eve-drift
```

When it does fire, it is a heads-up rather than a deadline. Nothing can be
deployed until Eveland's `packages/core/src/eve-compatibility.ts` verifies a
version on that line, and the pin here should follow **that** version rather than
npm's `latest` — the two are routinely different. Then bump the devDependency and
let the contract test name what has to move with it. The 0.38.3 alignment opts
this World into spec v6 slot identity: new runs use dense `evnt_` positions,
while runs created before migration 0006 remain on their original `wevt_` ULID
scheme. Neither detail is visible from version numbers alone, so typecheck and
run real eve builds against both old and new releases before believing a bump is
inert.

The shape of the `specVersion` check is itself not fixed. Through eve 0.33.1 it
is literal equality against the runtime's `SPEC_VERSION_CURRENT`;
`@workflow/core` beta.41, which eve 0.33.2 is the first release to install,
widened it to the range `[SPEC_VERSION_CURRENT, SPEC_VERSION_MAX_SUPPORTED]` and
ships it as `>= 5 && <= 6`. That is a loosening and cannot newly reject anything,
but it loosens in one direction only: the floor is still the runtime's current
version, so a World pinned behind it fails just as it did before. The headroom
above was the staging space for slot identity. In beta.42 the runtime floor and
`SPEC_VERSION_CURRENT` are both 6, and slots are part of the World contract
rather than an optional capability. Compatibility with existing v5 runs is
pinned by a per-run scheme marker rather than by rewriting their event ids.

`@workflow/world` beta.32 (eve 0.49) split the two ends of that range: the
runtime floor stays at slot identity (v6) while `SPEC_VERSION_CURRENT` moves to
v7, the "sealed log", and the runtime stamps each new run with whatever the
World declares. This World declares v6 deliberately (`src/index.ts`). Every line
in the {0.49.x, 0.50.x} window reads v6 or v7, so the declaration is no longer
what keeps the window readable -- but a Release built against a v6-only eve can
still be running when the window slides past it, and a v6-only reader rejects a
v7 run outright. Declaring v6 costs nothing either way: v7's reader contract is
the `noop` filler a backend emits when it pre-assigns event positions, and this
World allocates each position inside the INSERT that occupies it, so it never
has a hole to seal. Move the declaration to `SPEC_VERSION_CURRENT` only once no
v6-only eve can still be serving.

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

The precise compatibility boundary is documented in
[docs/world-postgres-beta34-compatibility.md](./docs/world-postgres-beta34-compatibility.md):
public `@workflow/world` behavior is required to match beta.34, while tenancy,
physical storage, retention and runner topology are explicit differences.
