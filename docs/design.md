# Design

Why this package is shaped the way it is, what it guarantees, and where it is
deliberately incomplete. Start with [getting started](./guides/getting-started.md),
the [configuration reference](./reference/configuration.md), and
[dispatcher operations](./operations/dispatcher.md) for installation and operation.
This document explains the reasoning behind them.

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
    D->>D: affinity: run.deploymentId → activation lease → executor endpoint
    D->>A: held POST /.well-known/workflow/v1/flow (for the step's whole duration)
    A->>PG: executor runs the step; storage/events/chunks written direct
    A-->>D: 200 → job complete (failure → maxAttempts 49 + backoff)
```

Postgres is the rendezvous. The dispatcher is the only actively polling party.
Agents receive POSTs and write to the database; they never call the platform, and
the platform never loads tenant code.

There is one route, `flow`, because `@workflow/world` has one queue kind. The
runtime runs steps inline inside the flow handler.

## Runner modes

| mode       | who runs the queue                                      | for                                                    |
| ---------- | ------------------------------------------------------- | ------------------------------------------------------ |
| `embedded` | an in-process graphile runner, delivering over loopback | local development, and upstream's conformance topology |
| `external` | the dispatcher in this package                          | production; the reason this package exists             |

`embedded` is not a transitional mode — it is the local-development story
permanently, and it is the mode upstream's suite assumes.

Recovery follows the runner. The World's `start()` re-enqueues the tenant's
active runs only in `embedded` mode, where the in-process runner is the sole
claimer and a job locked by a runner that died with the process would otherwise
wait out graphile's stale-lock threshold. In `external` mode it enqueues
nothing: the dispatcher reclaims dead worker locks and replays on its own boot,
and an agent boot is not a recovery event. It used to re-enqueue in both modes,
and on a shared database that was a cascade — each enqueue is an un-keyed job,
and the dispatcher routes it to the deployment the run is pinned to, so one
deployment's boot woke every deployment with an active run, each of which booted
and did the same.

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
the reference implementation with a tenant predicate added. The remaining
optional members carry decisions worth stating.

**`specVersion` is `mintedSpecVersion()`: hook force-claim, `8`, unless
`WORKFLOW_SEALED_LOG=0` opts the deployment back to slot identity, `6`.** eve
compiles the runtime protocol check into every release. Through core beta.40
that was literal equality; beta.41 uses an inclusive range. In beta.42 both the
floor and ceiling are 6 and slot identity is mandatory rather than a capability.
`@workflow/world` beta.32 (eve 0.49) minted spec 7, the "sealed log", and
beta.38 (eve 0.66.3) minted 8, the hook force-claim reader contract; the
runtime's floor stays at 6, and the runtime stamps each new run with whatever
the World declares. Both are reader contracts. Spec 7's exists for backends
that pre-assign event positions and must seal abandoned ones with `noop`
events; this World allocates each position inside the INSERT that occupies it,
so it never has a hole to seal. Spec 8's is the `hook_disposed{forceClaimedBy}`
row another run's takeover writes into a run's log (see
[Hook force-claim](#hook-force-claim)): a run stamped 8 reads it as "your hook
was taken", a run below it would read it as its own `dispose()` and wait on the
hook forever, so the stamped version is what a takeover consults before it
writes one.

Through 0.17.0 this World pinned 6, and through 0.26 it capped the declaration
at 7: it never took a token over, and declaring 8 would have shut out every
runtime before core beta.56 (eve 0.66.2 and older). Every line in Eveland's
window has read 8 since, and eve 0.81 needs force-claim for its session
handoffs, so the cap came off together with `capabilities.hookForceClaim`. An
in-flight run is pinned to the deployment that created it, so a version stamped
by a new Release is never replayed by an older one. The env var is the fallback
if a v6-only executor ever has to be served again; its runs can force-claim but
can never be claimed from. The declared value is read once per `createWorld()`
and handed to storage, so the run-creation fallback and legacy event stamps
agree with what the runtime was told. A conformance test verifies that the
installed runtime and this World agree.
The package must also declare `@workflow/world` (or `@workflow/core`) on a
matching version line, because eve checks the manifest's major and prerelease
tag before it ever loads the world.

**`resolveLatestDeploymentId` is implemented but incomplete.** See
[Known limitation](#known-limitation) below.

**`hooks.getByToken` is scoped by tenant _and_ token.** A token is its only
argument, which invites reading the tenant out of the row it finds. That is not
what this does: a world instance always runs inside one deployment and therefore
has an ambient tenant, so guessing another tenant's token resolves to nothing
rather than to their hook.

Capabilities are declared rather than assumed. Hook token retention is
announced through `capabilities.hookRetention`, because a World that stays silent
is treated as not supporting it. Hook force-claim is announced through
`capabilities.hookForceClaim` (see [Hook force-claim](#hook-force-claim)), and
eve 0.81 moves a session to a fresh run only when both of those are declared.
Deployment affinity is announced through `capabilities.deploymentAffinity` (see
below). And `events.create` reports `maxEvents` on every
response that carries a run, because the runtime enforces the ceiling and a World
that omits the field leaves a runaway workflow with no bound at all.

`streams.writeMulti` generates monotonic logical ids up front, strips Eve's
cumulative snapshots, and packs up to 64 logical chunks / 256 KiB into each v2
physical row. One NOTIFY names one block; readers expand both legacy rows and v2
blocks before applying cursor or index semantics. `analytics` and
`getEncryptionKeyForRun` are deliberately not implemented; both belong to work
that is out of scope below.

Three optional members added in `@workflow/world` 5.0.0-beta.24 are left
unimplemented on purpose. Each one fails open, and the reasons differ enough to
be worth recording:

- **`getEnvironment()`** exists so a run's creating environment can travel on its
  queue message and a consuming deployment can notice it was handed a message
  from a different one. Eveland has no environment dimension for a World to
  report — a deployment serves exactly one project, and the tenant predicate is
  the boundary — so returning anything here would manufacture a distinction that
  does not exist. Upstream expects Postgres-backed Worlds to omit it.
- **`capabilities.hookResumeDedup`** is the backend half of `resumeHook()`'s
  parallel fast path: the producer's direct write and the queue consumer's
  re-ensure both carry one `resumeId`, and the backend must collapse them onto a
  single `hook_received`. There is no `(runId, resumeId)` constraint here — the
  events table's uniqueness is on `(tenant, run, correlation, type)` for entity
  creation, which is a different key — so the capability stays unset and the
  runtime keeps the sequential single-writer path. Declaring it without the
  constraint would duplicate a `hook_received` on replay. Adding the constraint
  is a real option later; it is a migration, not a flag.
- **`CreateEventParams.computeInstanceId`** is ambient per-event identity, read
  back through the analytics surface. It is dropped for the same reason
  `requestId` already is: this World does not implement `analytics`, so there is
  nothing to read it back through.

The same release grew `hookInput` and `preconditionReinvocations` on the queue
payload and `environment` on `RunInput`. None of them reach this package: the vqs
body is carried as opaque bytes on `MessageData.data`, and only eve's own handler
parses it.

5.0.0-beta.25 — first picked up by eve 0.31.2 — added one required param and four
more optional members. The required one, `runId` on
`ListEventsByCorrelationIdParams`, is implemented; see
`events.listByCorrelationId`. Of the optional four, one is now declared and the
other three are unimplemented:

- **`capabilities.deploymentAffinity`** was left unset here until eve 0.76.1,
  and is declared now. On its own it turns on the runtime's affinity guard: a
  misrouted delivery is re-routed to the run's deployment and ultimately fails
  the run with `DEPLOYMENT_MISMATCH`. This World qualifies — the guard is unsafe
  only for Worlds whose deployment id is synthetic or version-tagged, and
  `queue.getDeploymentId` returns the real one — but the dispatcher already
  resolves affinity from the run row before any delivery happens
  (`resolveAffinity`), so the guard alone was a second mechanism for a condition
  the first is designed to make unreachable. eve 0.76.1 gave the flag a second
  reader: eve itself takes it to mean "this World routes each run to its own
  deployment". A World without it is assumed to run one eve version at a time,
  so eve compares every run's `$eve.version` attribute with its own version,
  parks or cancels deliveries for runs another version created, and ends a
  session whose owner run another version created (`session_stranded`) instead
  of resuming that run and letting it hand the session over. On Eveland, where
  Deployments on different eve versions run side by side, that turned every
  channel-address continuation across an eve-version promote into a reset. The
  guard's remedy still differs from the dispatcher's (a terminal run's straggler
  is dropped by the dispatcher, failed by the guard), but with the dispatcher
  routing on the run row the guard should never fire; if it does, the
  dispatcher's routing has a hole worth fixing directly.
- **`queue.isDeploymentUnavailableError`** lets the runtime distinguish "this
  deployment definitively cannot receive the message" from a transient failure.
  The dispatcher already makes exactly that distinction on the activation
  result — `not-activatable` dead-letters, `unavailable` retries — but it makes
  it dispatcher-side, where the activation lease is, rather than inside the
  World's queue surface. Nothing in the runtime path consults it here.
- **`runs.cancelMany`** is a bulk cancel, capped at 500 ids. `@workflow/core`'s
  `cancelRuns` falls back to bounded-concurrency single cancels when it is
  absent, which is correct and is what happens today.
- **`getRuntimeDeadline()`** reports when the hosting platform will terminate the
  current invocation. On this platform a step's invocation is a held vqs POST
  whose lifetime the dispatcher controls through lease renewal, so there is no
  externally-imposed wall clock for the World to report.

`CreateEventParams.preloadEvents` is an advisory opt-in on `hook_received` that a
World MAY answer with the run's complete replay log. It is ignored, which the
contract explicitly permits: the runtime observes that no usable preload came
back and falls back to its `run_started` setup. Answering it demands the whole log
read atomically with the `hook_received` write and `hasMore: false`, so it is a
performance option to take deliberately, not a field to fill in.

5.0.0-beta.26 made the replay page on `EventResult` an all-or-none group;
beta.27 is the line eve 0.38.3 installs and the one pinned here. This World
returns `events`, `cursor` and `hasMore` together for `run_started`, and omits all
three keys for ordinary events unless `eventCount` or `sinceCursor` requests an
inline delta. New runs receive a marker in `workflow_event_slots`, start at
`evnt_…0001`, and allocate dense positions inside the INSERT that occupies
them. The event key is therefore
`(tenant_id, run_id, id)`. Runs with no marker predate the migration and continue
minting `wevt_` ULIDs, preserving v5 replay order without a backfill.

### Hook force-claim

`createHook({ experimental_force: true })` asks for a token another run holds
and expects to get it rather than a `hook_conflict`. eve 0.81 is built on it: a
session moves to a fresh `workflowEntry` run (after a compaction, or onto a
newer deployment) by starting the successor, which force-claims every session
hook from the current owner. Without the capability eve logs `session handoff
unavailable: the Workflow world cannot force-claim hooks` and the session never
leaves its first run, so its event log grows until `WORKFLOW_MAX_EVENTS`
(25,000 by default) fails the run with `MAX_EVENTS_EXCEEDED`.

The takeover is one transaction, ported from `@workflow/world-postgres` 5.0.5
with the tenant predicate in every statement:

1. Resolve the owner by token, lock the owner's **run** row, then the token's
   **hook** row, in that order -- the order `hook_received` takes, so a
   delivery racing the takeover queues behind it rather than deadlocking. If
   the locked row names a different owner than the unlocked read did, the
   token moved in between and the transaction restarts (at most five times,
   then a 503 the runtime retries). Re-resolving by token is what turns two
   concurrent claimers into a chain `v -> c1 -> c2` instead of a lost claim.
2. Decide. A running owner stamped below 8 is not taken from: the claimer gets
   `hook_conflict{forceRefusedReason: 'victim-spec-version'}` and nothing is
   written. A finished owner holding a retained token (`hookRetention`) is
   taken at any version and gets no row, since it has no reader to strand. A
   finished owner whose retention has lapsed does not hold the token at all,
   so the claim is an ordinary creation. Upstream treats that last case as a
   takeover and records a `claimedFrom`; this World reads it the way every
   by-token lookup already does.
3. Write, in this order: the victim's `hook_disposed{token, forceClaimedBy}`
   (only for a running victim), delete the victim's hook row, insert the
   claimer's with `claimed_from` (migration 0020), and journal the claimer's
   `hook_created{force, forceClaimedFrom}`. `claimedFrom` carries the victim's
   run and hook and, for a running victim, the workflow name, deployment and
   stamped version the claimer's runtime wakes it with; every hook read returns
   it.

Two orderings make this safe, and both rest on row locks held to commit:

- **No delivery is journaled behind its hook's disposal.** `hook_received`
  re-reads its hook `FOR UPDATE` after locking the run, and `hook_disposed`
  (the run's own) now deletes the row and appends its event in one transaction.
  A delivery either commits before the disposal or finds the row gone. Gone
  because of a takeover is a redirect, not a drop: the World answers
  `HookForceClaimedError` naming the token's current owner, read from the
  victim's own disposal row and the token, and `resumeHook()` follows it with
  the same payload. Gone for any other reason is `HookNotFoundError`, as before.
  A delivery to a finished victim is refused as before (terminal run or missing
  hook), and the runtime re-resolves the token from the claimer's `claimedFrom`.
- **No disposal is journaled ahead of its hook's creation.** `hook_created`
  writes its hook row and its event in one transaction (they were two
  statements; the orphaned-row recovery stays for rows an older World left),
  so a takeover that can see the row also sees the creation already journaled.

The World cannot publish the victim's wake -- the claimer's runtime does that
from `claimedFrom`, through the ordinary queue, with an idempotency key per
claimer hook.

## Data model

### The tenancy column is `tenant_id`

`workflow_hooks` already carries a `project_id` column upstream, currently
written as an empty string. It is reserved for upstream's notion of a project,
and overloading it would collide the moment upstream populates it. So the
platform's tenancy column is `tenant_id`, and upstream's column keeps its own
meaning and is not read here.

### Tables

| table                         | partitioned | role                                                            |
| ----------------------------- | ----------- | --------------------------------------------------------------- |
| `workflow_runs`               | no          | the dispatcher's index: tenant, deployment, status, attributes  |
| `workflow_steps`              | no          | promote to partitioned if it outgrows runs — same recipe        |
| `workflow_hooks`              | no          | plus `token_retention_until`, and `claimed_from` for a takeover |
| `workflow_waits`              | no          | `resume_at` is the sleeping timer; graphile's `run_at` fires it |
| `workflow_events`             | LIST        | append-heavy, and the only write path for run state             |
| `workflow_stream_chunks`      | LIST        | legacy logical rows plus v2 packed physical blocks              |
| `workflow_stream_checkpoints` | no          | retired: no longer written, kept until a migration drops it     |
| `dispatch_dead_letters`       | no          | platform-owned; see [Dead letters](#dead-letters)               |

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
`workflow_runs (tenant_id, created_at DESC)` for listing surfaces. The
legacy stream-retention sweep uses the partial expression index on
`coalesce(completed_at, updated_at), tenant_id, id`; deadline-driven maintenance
uses the `compact_after`, `expire_after`, and `detail_expire_after` indexes, then
the `(tenant_id, retention_root_run_id, status)` index to validate whole-lineage
eligibility.

graphile's own tables live in the same database, in their own schema, untouched.

### Stream compaction and terminal retention

The stream boundary is logical, not physical. Write-side snapshot stripping turns
a v24-shaped `messageSoFar` / `reasoningSoFar` append back into a delta; every
supported Eve line already writes deltas (stream v25), so the guard is normally
a no-op. Readers serve stored bytes verbatim. The read-side rehydrator and its
`workflow_stream_checkpoints` rows were removed when the last v24 line left the
window: rebuilding snapshots re-inflated every read to O(n²) bytes that a v25
runtime normalized away again. Packed rows retain every logical chunk id, so a
terminal rewrite and a rolling upgrade do not change cursors, `startIndex`, or
`tailIndex`.

Eligibility is joined through both `tenant_id` and `run_id`; only non-EOF chunks
of completed, failed or cancelled runs older than the caller's window are
removed. Candidate rows are ordered oldest-first and deleted in bounded batches.
Because chunks are partitioned, the physical row identity is `(tableoid, ctid)` —
`ctid` alone is not unique across child tables. One dedicated connection holds a
session advisory lock across the batch loop, preventing independently scheduled
workers from multiplying database load.

New runs record an explicit retention class. A database trigger converts a
terminal transition into `compact_after`, `expire_after`, and
`detail_expire_after`, which prevents write-time deletion from racing final
NOTIFY/read delivery. Successful scheduled runs use 1 minute / 15 minutes / 24
hours. Failed scheduled runs use 1 minute / 1 hour / 7 days, and cancelled ones
use 1 minute / 1 hour / 3 days. Interactive runs use 5 minutes / 24 hours / 30
days; persistent runs receive no deadlines. Active runs receive none either.

Creation resolves explicit input, the public run attribute, tenant-scoped SDK
root/parent lineage, then an optional platform-owned root invocation context in
that order. Only a root can reach the context/default branch; descendants with
lineage inherit the stored ancestor class or fail if that ancestor cannot be
resolved. This keeps the World generic: it contains no Eve workflow-name or
trigger policy, and a later delivery to an existing graph cannot reclassify it.
Every member also stores the resolved root id, avoiding recursive JSON lineage
walks during maintenance.

The dispatcher runs bounded maintenance once at startup and every minute. Block
packing, stream expiry, and graph expiry are failure-isolated and protected by
advisory locks. Eligibility is decided across the complete stored lineage: any
active member, later member deadline, persistent member, or unexpired retained
hook holds both stream and graph data. Stream and graph expiry preserve EOF
forever. The older
`pruneTerminalStreamChunks(retentionMs)` entry point remains for standalone hosts.
Expiring chunks expires replay from old raw cursors even though EOF still makes
the stream resolve as complete. Normal deletes make pages reusable but do not
promise to shrink relation files.

### `queue_namespace` is provenance, not tenancy

eve registers its workflow handlers under a namespace: a deployment whose
namespace is `acme` owns `__acme_wkf_workflow_<name>` and nothing else. The
namespace is eve's, baked into the workflow bundle at build time — so a
container with no `WORKFLOW_QUEUE_NAMESPACE` in its environment can still be
serving a namespaced world, and namespaced deployments are the norm.

Live enqueue never had to store it: the deployment resolves the namespace in its
own process and records it on the message, and the delivery side rebuilds the
topic from there. Boot recovery is different. It reconstructs a message from a
run row, on the host, after the original job is gone — no enqueue closure to read
and no tenant environment to resolve from. Guessing the default prefix there
addresses a topic the executor does not own, which eve refuses with a 400
`Unhandled queue`; a 400 is terminal, so the run dead-letters and stays active
for the next boot to strand again.

So the value is stamped on the run at creation, in both production write paths
(resilient start and `run_created`), and read back by the sweep. It is immutable
provenance — the inserts are `onConflictDoNothing`, so a later process replaying
the same run cannot overwrite what the creating deployment recorded.

Three states, and the third is the point:

| value  | meaning                                          |
| ------ | ------------------------------------------------ |
| `'ns'` | the creating deployment resolved that namespace  |
| `''`   | it resolved none — the default prefix is correct |
| `NULL` | unknown: written by code that did not record one |

`''` is a safe sentinel because upstream requires a namespace to match
`^[a-z][a-z0-9]*$`, so it can never collide with a real one. `NULL` is kept
distinct rather than folded into "no namespace" because they are not the same
claim — see [Upgrading past 0.3.0](#upgrading-past-030).

This is **not** tenancy. Invariant 2 stands: prefix and namespace isolation leak,
and tenancy is the `tenant_id` column. This column exists only so the platform
can address the topic eve already chose.

### Upgrading past 0.3.0

`0.3.0` and earlier had nowhere to record the namespace, so every run they
created reads back `NULL`. An older deployment still running mid-upgrade keeps
writing `NULL` too — the world is baked into a release and is never upgraded in
place.

For those rows boot recovery falls back to the default prefix and **logs each
one**, with a `runsWithUnknownQueueNamespace` count on the summary line. That is
correct for an un-namespaced deployment and wrong for a namespaced one, and the
package cannot tell which from the row alone. Deriving it is not an option: the
namespace is eve's, and reimplementing how eve mints it would fork the algorithm
against a version this package only pins.

The supported procedure is therefore to drain: let active runs finish, or cancel
them, before cutting over. The shared-world path has not carried production
traffic, so an explicit drain is cheap here — and it is a stated policy with a
test behind it rather than a silent "null means default".

## The dispatch contract

One held `POST <executor>/.well-known/workflow/v1/flow` per in-flight step, open
for the step's whole duration. The POST returning is what marks the job complete.

`<executor>` comes from the activation. `endpointPort` means loopback,
`http://127.0.0.1:<endpointPort>`, which is every host that runs its executors
beside the dispatcher. `endpointUrl` is an origin anywhere else — typically a
service in front of several replicas of one deployment — and wins when both are
present, so a control plane can add it without taking away the port an older
dispatcher still reads. Only the origin is used: the route is this package's to
choose, because the dispatch carries the runtime secret and whoever supplies the
address must not be able to aim it at another path. A URL with a path, a query
or credentials is refused, and the message is dead-lettered rather than retried
against an address that will not change.

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
| `5xx` / network / timeout | throw → graphile retry with backoff, `maxAttempts: 49`        |
| `5xx`, same run, repeated | dead-lettered once the streak reaches the configured limit    |

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

### Executors that are always running

A host whose executors do not scale to zero has nothing to wake and nothing that
reaps, so it needs no activation API. `WORKFLOW_DISPATCHER_STATIC_ENDPOINTS`
(`createStaticActivationClient` for an embedding host) is a fixed table from
deployment id to executor origin; its lease renews unconditionally and releasing
it does nothing.

The table is keyed by deployment id because pinning still applies. Replicas of
one deployment share an id and sit behind one origin, and which of them takes a
dispatch is decided there. A new build that cannot replay the old one's event
log is a new deployment id at its own origin, and the old entry stays in the
table until its runs are over. A deployment missing from the table is
not-activatable: its messages are dead-lettered, which is the alarm for having
removed an entry too early.

Replacing a replica without running a step twice is the host's job, with one
number from this package. A delivery cut off by a stopping process is delivered
again — correct, but the step and its side effects run a second time. So the
host first stops routing to the replica (its readiness check fails), waits for
`inflightDeliveries()` to reach zero, and only then sends the signal. The count
is process-wide and lives on `globalThis`, because the runtime loads the World
from its package name while the host route that reports the number is usually
bundled separately.

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

**A hook wake sent while its run's queue is held is the one exception.** It is
new input for the run, not a duplicate, and it is the only way a cancel reaches
a step that is running: eve runs the step inside the delivery that claimed it,
and the replay the cancel's wake starts is what aborts it. Queued behind that
delivery, the replay would start after the step had finished and its side
effect had gone out. So when the World sends a hook wake (`isHookWake`) and a
delivery holds the run's queue, the wake takes no queue name. On an idle run it
queues as usual. Such a wake is also counted against a cap of its own rather
than the tenant's delivery cap (`wakeFlag`): the delivery it interrupts already
holds a slot of that cap. The wake cap has the size of the delivery cap, and
like it, it is approximate: graphile reads the forbidden flags before a claim
and the count rises after it, so idle workers claiming at once can overshoot. A
wake with no input in its payload and no producer idempotency key also takes a
per-run job key, so a burst of input during a long step leaves one pending wake
rather than one per message. That is safe because `resumeHook` commits the input
to the event log before it publishes such a wake; a wake that carries its input
(`hookInput`) or a producer key keeps a key of its own.

The wake a force-claim sends its victim is not a hook wake. The claimer's
runtime publishes it from `Hook.claimedFrom` with an idempotency key of
`hook-force-claim-<claimer hookId>` and neither `hookResumeTiming` nor
`hookInput`, so it queues on the victim's run like any other delivery. Nothing
is lost by waiting: the `hook_disposed{forceClaimedBy}` it announces committed
before it was sent, and whichever replay of the victim runs next reads it.

A wake running beside its owner does not weaken the exclusion a step had
before. A replay executes inline
only a step it claims itself, and the storage claim on a new step's
`step_started` gives that to one caller.

That holds only while neither replay starts a step optimistically, that is,
runs a new step's body before its claim resolves, which two racing replays can
both do. `@workflow/core` 5.2 reaches optimistic start two ways, and both stay
away from a run that can receive a hook wake: a deployment opts in with
`WORKFLOW_OPTIMISTIC_INLINE_START=1`, which must not be set where this World
runs; and turbo forces it on a run's first delivery, but only until the run has
a hook or a wait, and a run that receives a hook wake has a hook. Without per-run
serialization the forced start does race: twelve competing deliveries during a
live run ran step bodies a second time in three of three runs, and in none of
four with `WORKFLOW_OPTIMISTIC_INLINE_START=0`. Both conditions are internal to
core, so `src/core-concurrency-contract.test.ts` checks them against the
installed dist, and a core bump that changes either fails there. Any other step it can only enqueue as a
step message; while the owner's ownership lease (860 s by default) is live,
`@workflow/core` arms only a delayed backstop replay of the run instead. Past the
lease, a step message can be claimed while the step still runs, on any
replica, once the run's queue is free; while the owner runs, the queue is free
only when the owner is a wake that bypassed it. So storage keeps the delivery that started
a step on the step (`owner_message_id`), and refuses another delivery's
`step_started` with a retry while that delivery's graphile job is still locked
(`deliveryInFlight`). The owner's own redelivery may start it again, and a
refused start comes back later, at a tenth of how long the step has run (30 s
to 5 min): each retry is a delivery, so the retries thin out on a long step.

The fence ends where the queue lock always ended: when the owner's delivery
does. A delivery past the dispatch timeout is aborted and its job unlocked
while the step body may still run on its executor; the owner's own redelivery
then ran it again before this change too. A dead embedded runner leaves its job
locked until graphile's four-hour stale-lock threshold, as it leaves a run's
queue locked, so a step it owned waits that long to start again. Rows written
before migration 0019, or by an older World, have no owner and are not fenced;
their owner held the run's queue, which still orders the step message behind
it.

Limits, in order of likelihood:

- The decision is made once, at send time. A wake that finds the queue idle
  waits behind a step message that is already queued there, and a wake the
  dispatcher reschedules goes back into the queue. A replay that is already
  queued is no problem: it reads the input from the event log. The other way
  round, a wake that found the queue held may run beside whichever delivery
  holds it by the time it is claimed; the step claim and the restart fence
  above cover that delivery too.
- The wake still needs a free dispatcher slot and a free slot of its tenant's
  wake cap. With either held by running work, it waits like any other job. A
  wake's replay can go on to run ordinary work in the same job, such as the turn
  for input that queued up during the cancelled step, and that holds a wake slot
  meanwhile. Reserving dispatcher slots for wakes would need them under a job
  name of their own; lowering the tenant cap instead starves a parent step that
  holds a slot while it waits for its child run.
- A wake that meets a running step leaves that delayed backstop replay behind,
  at most one per step start. It is a no-op replay, but in external mode it
  usually lands on an idle deployment and costs a cold start.
- The per-run key folds only wakes still pending. A wake already claimed keeps
  running, so with free dispatcher slots a burst of input gives about one
  replay per message. A claimed wake that a newer one supersedes loses its
  retries: if it then fails, its job row stays behind as failed, with no dead
  letter, and the newer wake replays the same log.
- A step that resumes a hook of its own run sends a wake that bypasses its own
  delivery: one extra replay and one backstop replay, no second execution. eve
  resumes other runs' hooks (a parent session), so this is rare.
- The restart fence reads graphile's private job table by payload, which has
  no index. It runs only when a running step is started again, which is rare,
  and a failed read lets the start through as before.

Boot recovery releases a dead worker's lock on a flow job in no queue by the job
itself, since there is no queue to find it by.

This costs a sweep. graphile does not reclaim a queue row when its last job
completes, so one row per run would accumulate for ever; the dispatcher runs
graphile's own queue cleanup on an interval.

It also costs every claim. graphile 0.16 locks every queue that holds any job,
due or not, before it picks one, so a queue must not outlive the work that is
due on it. A delivery due more than a minute out therefore waits as a timer job
with no queue and returns to the run's queue when it is due; serialization
covers the delivery, never the wait. See
[delayed deliveries](operations/dispatcher.md#delayed-deliveries-and-claim-cost).

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

| failure                         | what happens                             | recovered by                                |
| ------------------------------- | ---------------------------------------- | ------------------------------------------- |
| agent crashes mid-step          | held POST fails → job fails              | graphile retry, then re-activation          |
| dispatcher crashes mid-POST     | job and per-run queue stay locked        | ownership-gated unlock; graphile redelivers |
| deployment archived or failed   | activation is not-activatable → terminal | should be unreachable; alarms               |
| deployment unavailable or cold  | activation unavailable → retry           | graphile retry                              |
| lease lapses during a long step | executor reaped mid-step                 | **prevented** by renewal, not recovered     |
| `maxAttempts` exhausted         | graphile stops retrying                  | dead-letter quarantine + operator action    |
| run fails at execution forever  | 5xx streak → dead-letter, redelivery off | quarantine + operator action                |
| duplicate enqueue               | job key dedupes at enqueue               | by construction, keyed paths only           |

Boot recovery runs before the worker pool starts and only while the service holds
a lifecycle PostgreSQL advisory lock. It joins active runs to their exact
`wfrun:<tenant>:<run>` queue rows, collects the old random `locked_by` values,
passes those ids to Graphile's `forceUnlockWorkers`, and then performs the
run-keyed re-enqueue for the runs that have no job left. A job key alone cannot
recover the run: it deduplicates or replaces a job but does not clear
`_private_job_queues.locked_by`, whose stale threshold is four hours.

The re-enqueue is the narrow half. A run whose queue still holds a job graphile
will deliver — due now, due later for a sleep, or locked by the worker id just
released — needs nothing more: that job is its wake-up. Re-enqueueing beside it
was correct (the handler replays) and expensive (one activation per run), and
with a few hundred active runs on a few dozen deployments every restart became
a cold-start storm. The sweep now skips those runs.

It also skips runs that hold a hook. A hook is the run's other wake-up: whoever
resolves it enqueues the run's next delivery through the World's own `queue()`,
whenever that happens, and a dispatcher restart changes nothing about that. On
the incident host every candidate left after the live-job skip was an agent
session parked on its inbox hook; recovering them woke eighteen deployments,
skipping them woke none. The trade-off is a run that created a hook, moved on,
and then lost the only job that would have driven it: it now resumes when that
hook does rather than at the next boot. What the sweep re-enqueues, then, is
the run with nothing else to wake it — no retryable job, no hook.

What the sweep does enqueue it paces by deployment: the deployments it meets are
grouped `WORKFLOW_DISPATCHER_BOOT_RECOVERY_DEPLOYMENTS_PER_WAVE` at a time, and
each group's jobs are due `WORKFLOW_DISPATCHER_BOOT_RECOVERY_WAVE_INTERVAL_MS`
after the previous group's. The unit is the deployment because its cold start
is the cost — a deployment's second run is warm — and the host, not the
dispatcher, has to absorb those starts.

The order is a correctness boundary: ownership, unlock, re-enqueue, worker pool,
ready. Unlocking after the new pool starts could clear a lock belonging to the
new generation.

### Dead letters

A run whose retries are exhausted must not simply stop. It lands in
`dispatch_dead_letters` with the message preserved verbatim, rather than becoming
a `run_failed` event, because a run that could still have succeeded is an
operator problem and not a workflow outcome — the distinction is lost if it is
recorded as the workflow's own failure.

An unresolved dead letter is therefore the dispatcher's terminal **quarantine**
state even though the workflow run itself remains `pending` or `running`. Boot
recovery excludes quarantined runs, so every dispatcher restart cannot recreate
the same terminal delivery forever. Platform deployment-retention queries must
apply the same exclusion: an unresolved dispatch failure no longer protects a
dead executor indefinitely. This deliberately does not append `run_failed` or a
stream EOF on the workflow's behalf.

Operator resolution is the explicit retry boundary. Once `resolved_at` is set,
the active run becomes eligible for boot recovery and deployment protection
again; an operator who wants to abandon it should cancel/fail it through the
workflow control surface instead. This keeps transport failure, workflow
outcome, and manual replay as three distinct states.

Quarantine also holds live dispatch, not only boot recovery: a message that
arrives for a run with an unresolved dead letter is dropped with a log line
instead of being delivered. A run that fails at execution enqueues wake-ups of
its own on every failed replay, so redelivering to it is what turns one bad run
into hundreds of jobs and a deployment that is woken every few minutes to fail
again. The dead letter keeps a replayable copy of the message; resolving it is
the explicit retry boundary.

The same reasoning puts a run into quarantine before graphile's attempts are
spent. An executor `5xx` means the flow route ran the message and threw; once is
a blip, but the same run failing that way on `WORKFLOW_DISPATCHER_EXECUTOR_FAILURE_LIMIT`
consecutive deliveries spread over at least
`WORKFLOW_DISPATCHER_EXECUTOR_FAILURE_MIN_SPAN_MS` is a run that can never
execute — a bundle that cannot replay its log, a row the database refuses — and
it is dead-lettered then. Transport failures never count: a dead or restarting
process is exactly what retries exist for. The streak is in-process and bounded,
like the dedup cache; a dispatcher restart forgets it and costs a few retries.

Quarantine is a claim about a run that can still be replayed, so it ends when
the run does: a trigger resolves a run's unresolved letters as it reaches a
terminal status, whichever writer put it there. Without that, abandoning a
wedged run leaves its letters standing forever and the operator-facing
"unresolved" count stops measuring outstanding work and starts measuring
history — one permanently unstartable Deployment can contribute hundreds of
rows. `resolved_at` is never re-stamped, so an operator's own resolution
timestamp survives.

Every path into that table matters, including the one that is easy to miss: a
dispatch that _throws_ rather than returning a failure outcome must still reach
it, or the final attempt vanishes with no record at all.

### Host-driven run reconciliation

Run status is otherwise written only from inside the World, by the workflow's
own lifecycle events. That default is right — the workflow owns its outcome —
but it leaves the host platform with knowledge and no pen: it supervises the
agent processes, so it alone knows when a run's executor is gone for good
(idle-reaped, crashed, deployment retired). Without a write path those runs
stay `running` forever, and boot recovery replays every one of them on every
dispatcher start.

`reconcileWorkflowRuns` is that write path, and it draws the same line the
dead-letter design draws: a host verdict is not a workflow-authored outcome, so
it never appends `run_failed` events or stream EOFs on the workflow's behalf.
It offers three dispositions over an explicitly selected set of a tenant's
active runs (by run id and/or deployment id — there is deliberately no
"everything" form):

- **fail** / **cancel** write the run row the way the World's own terminal
  transitions do: conditional on the run still being active, hooks deleted
  except retained tokens, waits deleted, retention deadlines recomputed by the
  database trigger. `fail` records a machine-readable `error_code` and leaves
  `error_cbor` NULL — this process cannot honestly mint runtime-serialized
  error data, and pretending otherwise would poison hydration.
- **quarantine** leaves the run's status alone and parks it behind an
  unresolved dead letter whose payload is the same message boot recovery would
  have reconstructed. Operator replay of a host quarantine and of a
  retry-exhausted delivery is therefore one procedure, and resolution is the
  replay boundary for both.

The natural call sites are the host's instance reconciliation loop and the
dispatcher's `beforeBootRecovery` preflight, which receives the pool exactly so
orphans can be settled before the sweep ever sees them.

### Filtered boot recovery

Reconciliation settles runs the host has given up on. The complementary seam is
for runs it has merely lost the ability to replay _right now_: boot recovery's
`filterRuns` (surfaced as `filterBootRecoveryRuns` on the dispatcher service)
is called once with the full candidate list — one call, not a per-run
predicate, because "is this Deployment activatable?" is a control-plane lookup
the host will want to batch — and only the runs it returns are re-enqueued.
Candidates are the runs the sweep would enqueue: a run whose own job is still
queued is not offered, since nothing would be written for it either way.

A skipped run is deferred, not settled: it stays active and is offered again on
the next boot. Worker-lock reclaim still covers skipped runs, so a later
settle-or-readmit is not held behind graphile's stale-lock threshold.

## Configuration

Every variable has one canonical `WORKFLOW_*` name, with `EVELAND_*` accepted as
aliases. The full table is in the [configuration reference](./reference/configuration.md)
and is not duplicated here — a second copy would drift.

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
- **Multi-machine dispatcher replicas.** The current lifecycle advisory lock is
  intentionally single-owner. Replicas require durable generation heartbeats,
  worker-id-to-generation registration, expiry-aware unlock, and a per-run fence
  before this guard can be relaxed; no in-memory claim state may become authoritative.
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
- **`@workflow/*` is a moving beta target.** The `specVersion` range the
  runtime accepts is compiled into eve per release, so every eve bump has to
  re-verify the contract rather than assume it.
- **Migration 0006 replaces the event primary key.** PostgreSQL takes an
  `ACCESS EXCLUSIVE` lock for that catalog change. The migration bounds the wait
  at ten seconds; large installations should apply it in a maintenance window
  rather than let an unbounded lock queue stall event traffic.
- **A single dispatcher is a restart-pause single point of failure.** Keeping it
  stateless — all claim state in Postgres — is what makes a restart a brief pause
  plus boot recovery rather than data loss.
- **graphile's jobs table is hot on a shared database**, with delete-on-complete
  churn. Autovacuum behaviour there is worth a metric.
- **Payloads are unencrypted at rest** on a database every tenant shares.
- **Partition count has a ceiling**, with the documented escape hatch above.
