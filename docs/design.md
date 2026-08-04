# Design

Why this package is shaped the way it is, what it guarantees, and where it is
deliberately incomplete. [README.md](../README.md) is the operator's view —
install, configure, run. This is the reasoning underneath it.

## The problem the shape solves

In the reference PostgreSQL World the queue runner lives inside the process that
executes workflows. A durable timer is a row waiting to be claimed, and the only
thing that can claim it is that process.

That is fine when the process is always up. On a platform that scales agents to
zero it is not: the idle reaper stops the deployment, and the timer's only
claimant goes with it. A `sleep 1h` on a quiet project resumes when unrelated
traffic happens to wake the agent — possibly never. The failure is silent, and it
gets worse the quieter the project is.

There is a second failure in the same family. Deployment retention protects a
deployment that is serving requests or holding a session. A sleeping workflow run
is neither, so retention cannot see it, and a pinned run outside the keep-window
loses the executor that was the only build able to replay it.

Both follow from the same coupling: orchestration and execution in one process
whose lifetime is governed by traffic. So they are split. Orchestration —
claiming, timers, retries, affinity — moves to a resident dispatcher. Execution
stays inside the deployment, because only that build's bundle can replay its own
event log.

A third constraint shapes it further. Per-project databases put a floor of about
two held connections under every agent, and the ceiling scales with the fleet.
Connection budget, not CPU, is what runs out first. One shared database with
tenancy in the schema removes the floor; pgbouncer in transaction mode is not an
alternative, because both graphile and the streamer need `LISTEN`/`NOTIFY`.

## Two ids, two roles

Everything in this package hangs off the distinction:

- **`tenantId`** is tenancy — data scoping, fairness flags, storage reclaim. It
  leads every primary key and every query predicate.
- **`deploymentId`** is affinity — which executor may run this. An in-flight run
  is pinned to the deployment that created it; a new run follows whatever is
  promoted.

Conflating them is the mistake the design exists to prevent. Tenancy questions
must never be answered with a deployment id, and affinity questions must never be
answered with a tenant id.

## The life of one step

```mermaid
sequenceDiagram
    participant A as agent deployment<br/>(eve executor + this World)
    participant PG as shared Postgres<br/>(control tables + partitions + graphile)
    participant D as workflow dispatcher
    A->>PG: world.queue → add_job(flags=[project:X], queueName=wfrun:…, run_at)
    D->>PG: graphile claim (LISTEN/NOTIFY + poll, forbiddenFlags skips capped tenants)
    D->>D: affinity: run.deploymentId → activation lease → endpointPort
    D->>A: held POST /.well-known/workflow/v1/flow (for the step's whole duration)
    A->>PG: executor runs the step; storage/events/chunks written direct
    A-->>D: 200 → job complete (failure → maxAttempts 3 + backoff)
```

Postgres is the rendezvous. The dispatcher is the only actively polling party.
Agents receive POSTs and write to the database; they never call the platform, and
the platform never loads tenant code.

There is one route, `flow`, because `@workflow/world` has one queue kind. The
runtime runs steps inline inside the flow handler.

## Runner modes

| mode       | who runs the queue             | for                                                    |
| ---------- | ------------------------------ | ------------------------------------------------------ |
| `embedded` | an in-process graphile runner  | local development, and upstream's conformance topology |
| `external` | the dispatcher in this package | production; the reason this package exists             |

`embedded` is not a transitional mode — it is the local-development story
permanently, and it is the mode upstream's suite assumes.

It needs isolation the single-tenant original did not. On a shared database an
in-process runner claiming a shared graphile job name would claim other tenants'
jobs, and graphile's `forbiddenFlags` is a deny-list that cannot express "only
mine". So `embedded` job names carry a per-tenant suffix, and only `external`
uses the shared name the dispatcher claims. A tenant switching modes drains its
old suffixed jobs through the old deployment's runner.

## Invariants

Ranked by how expensive they are to violate.

1. **No tenant code in the platform process.** The dispatcher claims, resolves
   and POSTs. It never imports a project bundle.
2. **No prefix or namespace isolation for tenancy.** It has been tried on this
   problem and it leaks; the failure mode is one project's runner claiming
   another project's jobs. Tenancy is a `tenant_id` column on every row and a
   predicate on every query.
3. **graphile does not leak past the queue module.** The `World` API, the
   dispatch contract and the dispatcher surface stay graphile-ignorant. Enqueue
   only through the public `add_job` API, and never `DELETE` from graphile's
   internal tables.
4. **Payloads carry ids only** — `tenantId`, `deploymentId`, `runId`,
   `messageId`, attempt. State lives in the workflow tables, so a payload can
   never be a stale copy of state.
5. **NOTIFY channels are tenant-scoped.** A global channel on a shared database
   wakes every agent in the fleet on every chunk of every run.
6. **The dispatch contract is versioned and checked.** A deployment's world is
   baked at build time and never upgraded in place, so old bundles must keep
   working against a newer dispatcher.
7. **Big tables keep hard isolation.** Events and stream chunks are
   LIST-partitioned by tenant so reclaim is `DROP PARTITION` rather than a mass
   delete.

## The World surface

`World extends Queue, Streamer, Storage`. The required members are ported from
the reference implementation with a tenant predicate added. Four optional ones
carry decisions worth stating.

**`specVersion` is the literal `5`.** eve compiles a literal
`world.specVersion !== 5` check per release, so this tracks the `@workflow/world`
line the package depends on, and a contract test asserts the two still agree
against the installed eve. The package must also declare `@workflow/world` (or
`@workflow/core`) on a matching version line, because eve checks the manifest's
major and prerelease tag before it ever loads the world.

**`processExitTriggersQueueRedelivery` stays `false`.** Under an external runner
it looks like it should be `true` — a failed POST _is_ redelivered by the
platform, which is what the flag describes. But eve reacts to `true` by calling
`process.exit(1)` when a run exhausts its replay budget, and that process is also
serving the project's chat and scheduler traffic. Recycling one run must not drop
unrelated in-flight sessions. Failures surface through the event log instead.

**`resolveLatestDeploymentId` is implemented but incomplete.** See
[Known limitation](#known-limitation) below.

**`hooks.getByToken` is scoped by tenant _and_ token.** A token is its only
argument, which invites reading the tenant out of the row it finds. That is not
what this does: a world instance always runs inside one deployment and therefore
has an ambient tenant, so guessing another tenant's token resolves to nothing
rather than to their hook.

Two capabilities are declared rather than assumed. Hook token retention is
announced through `capabilities.hookRetention`, because a World that stays silent
is treated as not supporting it. And `events.create` reports `maxEvents` on every
response that carries a run, because the runtime enforces the ceiling and a World
that omits the field leaves a runaway workflow with no bound at all.

`streams.writeMulti`, `analytics` and `getEncryptionKeyForRun` are deliberately
not implemented; the first because Postgres batching is not the bottleneck, the
other two because they belong to work that is out of scope below.

## Data model

### The tenancy column is `tenant_id`

`workflow_hooks` already carries a `project_id` column upstream, currently
written as an empty string. It is reserved for upstream's notion of a project,
and overloading it would collide the moment upstream populates it. So the
platform's tenancy column is `tenant_id`, and upstream's column keeps its own
meaning and is not read here.

### Tables

| table                    | partitioned | role                                                            |
| ------------------------ | ----------- | --------------------------------------------------------------- |
| `workflow_runs`          | no          | the dispatcher's index: tenant, deployment, status, attributes  |
| `workflow_steps`         | no          | promote to partitioned if it outgrows runs — same recipe        |
| `workflow_hooks`         | no          | plus `token_retention_until` for retained tokens                |
| `workflow_waits`         | no          | `resume_at` is the sleeping timer; graphile's `run_at` fires it |
| `workflow_events`        | LIST        | append-heavy, and the only write path for run state             |
| `workflow_stream_chunks` | LIST        | one row per token delta — the highest-volume table by far       |
| `dispatch_dead_letters`  | no          | platform-owned; see [Dead letters](#dead-letters)               |

`tenant_id` leads the primary key on every one of them, including the
unpartitioned tables where Postgres does not require it. Run and step ids come
from the runtime, so a bare `id` primary key would let one tenant's insert
collide with another tenant's row.

### Partitioning constraints

These bite hard enough to be worth stating outright.

- **Every primary key and unique index on a partitioned table must contain the
  partition key.** Chunks become `PK (tenant_id, stream_id, id)`, events become
  `PK (tenant_id, id)`, and the correlated-event uniqueness index gains
  `tenant_id`. This is not a soft failure — `CREATE TABLE … PARTITION BY` rejects
  it outright.
- **There is no `DEFAULT` partition.** Attaching a partition while a default
  exists makes Postgres scan the default for conflicting rows, turning tenant
  provisioning into an O(fleet) operation at the worst moment. Without one, an
  insert for an unprovisioned tenant fails loudly, which is the correct outcome.
- **Partition count grows at two per tenant.** Postgres plans fine into the low
  thousands with pruning, but planning time grows with the count. The escape
  hatch is HASH-by-tenant buckets, accepting `DELETE` instead of `DROP` for
  reclaim.
- **A partition's child index name is not derivable by convention.** Postgres
  generates it from the partition name plus columns and truncates to 63 bytes, so
  it is neither the parent's name nor a stable suffix of it. Provisioning renames
  the child to a derived, predictable name so that translating a unique-violation
  into a domain error can compare exactly rather than by pattern.

### Indexes beyond the ported ones

`workflow_runs (tenant_id, status)` for fairness and queue-depth metrics;
`workflow_runs (deployment_id) WHERE status IN ('pending','running')` for the
retention guard, partial so it stays small as terminal runs accumulate; and
`workflow_runs (tenant_id, created_at DESC)` for listing surfaces.

graphile's own tables live in the same database, in their own schema, untouched.

## The dispatch contract

One held `POST http://127.0.0.1:<endpointPort>/.well-known/workflow/v1/flow` per
in-flight step, open for the step's whole duration. The POST returning is what
marks the job complete.

### Request

| header                                                | source       | meaning                                                       |
| ----------------------------------------------------- | ------------ | ------------------------------------------------------------- |
| `x-vqs-queue-name`                                    | eve protocol | the full prefixed name; eve 400s on a bare sub-queue id       |
| `x-vqs-message-id`                                    | eve protocol | stable across redeliveries — see below                        |
| `x-vqs-message-attempt`                               | eve protocol | graphile's attempt number                                     |
| `x-eveland-runtime-secret`                            | this package | what distinguishes platform dispatch from any other caller    |
| `x-eveland-dispatch-version`                          | this package | currently `1`; the receiving side rejects what it cannot read |
| `x-eveland-project-id` / `-deployment-id` / `-run-id` | this package | binds the request to one target                               |

The queue name carries eve's queue namespace when one is configured, and that
namespace travels **on the message** rather than being re-derived by the
dispatcher. The dispatcher runs on the host, where the environment holds the
host's value, not the tenant's.

There is deliberately **no bearer token**. This port is served by the tenant's
own agent process, which can read any header it receives, so a credential that
authorizes activating or releasing leases would be a privilege escalation across
projects. The runtime secret authenticates the dispatch, and the deployment id
binds it to one target so a captured request cannot be replayed at another
deployment on the same host.

Because this package supplies `createQueueHandler`, the contract is enforced on
the receiving side and not merely sent: a dispatch claiming a version the bundle
does not understand is rejected, as is one carrying the wrong secret or addressed
to a different deployment.

### Responses

| response                  | dispatcher behaviour                                          |
| ------------------------- | ------------------------------------------------------------- |
| `200`                     | job complete                                                  |
| `200 {timeoutSeconds: N}` | not complete — re-enqueue the _same_ `messageId` at `now + N` |
| `4xx`                     | terminal; never retried, and dead-lettered                    |
| `5xx` / network / timeout | throw → graphile retry with backoff, `maxAttempts: 3`         |

The reschedule case has two load-bearing properties. The `messageId` is preserved
because the runtime uses it as the step-ownership lease, so minting a fresh one
would silently degrade crash recovery into the slower backstop path. And the
follow-up job is enqueued _before_ the handler returns, so a dispatcher crash
between the two cannot lose the wake-up.

### The activation lease

The dispatcher acquires a lease before the POST and holds it for the POST's
duration. The lease does two jobs at once: it keeps the idle reaper off the
deployment mid-step, and it registers as an active request in the platform's
retention accounting, so a running step protects its own deployment from being
archived.

But a lease TTL is finite and a step's duration is not — model calls are
unbounded. **The lease must be renewed on an interval well inside the TTL for the
whole life of the held POST**, and released when it returns. A dispatcher that
acquires and forgets has its executor reaped out from under any step that
outlives one TTL.

Renewal failures are tolerated while the lease still has headroom. The interval
is a fraction of the TTL by construction, so a single refusal from the control
plane leaves room for further attempts, and aborting on the first one would turn
a blip into a burned graphile attempt — three of which end the run. The dispatch
is aborted only once sustained failure means the lease is about to lapse, and a
success resets the tolerance so alternating pass/fail cannot keep a dying lease
alive indefinitely.

Note what the lease does **not** cover. A _sleeping_ run holds no lease and no
connection, by design — that is the whole point of moving the claimant out. Such
a run needs a separate retention reason on the platform side; the lease protects
running steps only.

## Delivery and failure semantics

Delivery is **at-least-once**. The runtime is replay-based and idempotent per
message; this package's job is to not break the assumptions that make that true.

**Ordering is guaranteed per run, and only per run.** Every enqueue gives a
message the graphile queue name `wfrun:<tenantId>:<runId>`, and graphile runs
jobs sharing a queue name strictly one at a time. Without it, two claimed jobs
replay one run's event log concurrently. All three enqueue paths — the World's
own send, the dispatcher's reschedule, and boot recovery — must derive the name
identically or the serialization is silently partial, so it has exactly one
definition.

This costs a sweep. graphile does not reclaim a queue row when its last job
completes, so one row per run would accumulate for ever; the dispatcher runs
graphile's own queue cleanup on an interval.

**Deduplication is a bounded in-process filter, not a durable record.** Only
messages carrying an idempotency key participate, and only a _completed_ dispatch
is suppressed — suppressing on failure would let one transient blip swallow a
message permanently. The bound matches what embedded mode has always offered
rather than exceeding it: a durable table would cost a write per message on the
hottest table in a shared database, and it would still not be a correctness
boundary, because what makes redelivery safe is the runtime replaying from the
event log. A restart legitimately forgets, and the consequence is a replay.

**Cross-run ordering is not guaranteed and is not needed**, because workflows
derive state from the event log rather than from message order. Do not add
ordering later without re-checking that assumption.

| failure                         | what happens                             | recovered by                            |
| ------------------------------- | ---------------------------------------- | --------------------------------------- |
| agent crashes mid-step          | held POST fails → job fails              | graphile retry, then re-activation      |
| dispatcher crashes mid-POST     | the job stays locked to a dead worker    | boot recovery's run-keyed re-enqueue    |
| deployment archived or failed   | activation is not-activatable → terminal | should be unreachable; alarms           |
| deployment unavailable or cold  | activation unavailable → retry           | graphile retry                          |
| lease lapses during a long step | executor reaped mid-step                 | **prevented** by renewal, not recovered |
| `maxAttempts` exhausted         | graphile stops retrying                  | dead-letter row                         |
| duplicate enqueue               | job key dedupes at enqueue               | by construction                         |

Boot recovery is a **run-keyed re-enqueue**, which supersedes a job locked to a
dead worker rather than waiting for its lock to time out. Reaching into graphile
to force-unlock a previous generation's workers is not a viable alternative:
graphile mints its own worker ids and will not accept an external one.

### Dead letters

A run whose retries are exhausted must not simply stop. It lands in
`dispatch_dead_letters` with the message preserved verbatim, rather than becoming
a `run_failed` event, because a run that could still have succeeded is an
operator problem and not a workflow outcome — the distinction is lost if it is
recorded as the workflow's own failure.

Every path into that table matters, including the one that is easy to miss: a
dispatch that _throws_ rather than returning a failure outcome must still reach
it, or the final attempt vanishes with no record at all.

## Configuration

Every variable has one canonical `WORKFLOW_*` name, with `EVELAND_*` accepted as
aliases. The full table is in [README.md](../README.md#configuration) and is not
duplicated here — a second copy would drift.

Two properties of the surface are design rather than documentation. The database
URL has **no fallback chain**: falling back onto a plausible-looking single-tenant
database is worse than failing to start, so a missing value throws by name. And
both ends of the system read the same ordered list of names, because a name
honoured by only one end produces a dispatcher that starts cleanly and then polls
a database nothing writes to.

## Known limitation

`resolveLatestDeploymentId` returns the deployment the process _is_, not the
project's currently promoted one. eve calls it when a run starts with
`deploymentId: 'latest'`.

For ordinary traffic the two coincide, because new sessions are routed to the
promoted deployment. They diverge in exactly the case this package introduces: a
superseded deployment woken by the dispatcher to finish a pinned run would start
a new `'latest'` run on itself rather than on the newest code.

Resolving it needs promotion state, which lives in the control plane rather than
the workflow database. Reaching for it from inside a tenant process would break
the rule that agents and the platform rendezvous only in Postgres, so this is
open rather than solved.

## Deliberately out of scope

Named because each is a plausible next step and each has a reason to wait.

- **Async-ack dispatch.** The held POST is sync-hold v1. A `202` plus callback
  removes the standing connection per in-flight step, and becomes worth doing
  when in-flight counts start to hurt rather than before.
- **HTTP storage.** Agents reaching zero Postgres connections is both the true
  tenant boundary and the full payoff of a single pool. It replaces WHERE-clause
  discipline with a boundary that cannot be forgotten.
- **Multi-machine dispatcher replicas.** The claim design is already replica-safe
  and must stay that way: no in-memory claim state, ever.
- **Row-level security and `SET ROLE`.** Hardening on top of the tenant
  predicate, not a replacement for it.
- **At-rest payload encryption**, through `getEncryptionKeyForRun`.
- **Declarative platform workflows** layered on the dispatch primitive.

## Design risks

Properties of these choices worth watching, as distinct from defects.

- **The tenant boundary is WHERE-clause discipline.** It is accident-level, not
  attack-level: every storage method must carry the predicate, and a new query is
  a new place to forget it. This is why every read is tested for cross-tenant
  reach, and why HTTP storage is the real fix.
- **`@workflow/*` is a moving beta target.** The `specVersion` literal is
  compiled into eve per release, so every eve bump has to re-verify the contract
  rather than assume it.
- **A single dispatcher is a restart-pause single point of failure.** Keeping it
  stateless — all claim state in Postgres — is what makes a restart a brief pause
  plus boot recovery rather than data loss.
- **graphile's jobs table is hot on a shared database**, with delete-on-complete
  churn. Autovacuum behaviour there is worth a metric.
- **Payloads are unencrypted at rest** on a database every tenant shares.
- **Partition count has a ceiling**, with the documented escape hatch above.
