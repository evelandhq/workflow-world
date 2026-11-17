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

## Who runs the queue

A deployment never runs a queue runner. It writes each message as a job under
one shared graphile job name, and the dispatcher in this package claims those
jobs across every tenant and delivers each to the deployment its run is pinned
to. A deployment's flow route accepts only those deliveries: the runtime secret
and the target deployment travel on every one, and the runtime's own
queue-auth check is declared off (`queueAuthMode: 'skip'`).

An agent boot is not a recovery event. The World's `start()` enqueues nothing:
the dispatcher reclaims dead worker locks and replays on its own boot. A boot
that re-enqueued its tenant's active runs would cascade on a shared database,
because each enqueue is an un-keyed job the dispatcher routes to the
deployment the run is pinned to, so one deployment's boot would wake every
deployment with an active run, and each of those would do the same.

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

**`specVersion` is always `9`, the single-orchestrator version eve 1.0 asks
for** (`WORKFLOW_SPEC_VERSION=9`; see [In-band writer fence](#in-band-writer-fence)).
A deployment that asks for any other version is refused when the World is
created, rather than left believing it creates runs of a version it does not.
The runtime stamps each new run with the declared version and admits any World
whose version lies between slot identity (6) and the highest it can read.

Spec 9 includes every reader contract below it. Spec 7's exists for backends
that pre-assign event positions and must seal abandoned ones with `noop`
events; this World allocates each position inside the INSERT that occupies it,
so it never has a hole to seal. Spec 8's is the `hook_disposed{forceClaimedBy}`
row another run's takeover writes into a run's log (see
[Hook force-claim](#hook-force-claim)): a run stamped 8 or above reads it as
"your hook was taken", while a run below it would read it as its own
`dispose()` and wait on the hook forever, so the stamped version is what a
takeover consults before it writes one. The declared value is read once per
`createWorld()` and handed to storage, so the run-creation fallback agrees with
what the runtime was told. A conformance test verifies that the installed
runtime and this World agree.
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
eve moves a session to a fresh run only when both of those are declared.
Deduplicated hook resumes are announced through `capabilities.hookResumeDedup`
(see [Hook resume deduplication](#hook-resume-deduplication)).
Deployment affinity is announced through `capabilities.deploymentAffinity` (see
below). And `events.create` reports `maxEvents` on every
response that carries a run, because the runtime enforces the ceiling and a World
that omits the field leaves a runaway workflow with no bound at all.

`streams.writeMulti` generates monotonic logical ids up front and packs up to 64 logical chunks / 256 KiB into each v2
physical row. One NOTIFY names one block; readers expand both one-row-per-chunk
rows and v2 blocks before applying cursor or index semantics. A `streams.get`
reader takes in a page of rows when its consumer has room for it and treats a
NOTIFY as a wake-up only, so what a slow or abandoned reader holds does not grow
with its stream. `analytics` and
`getEncryptionKeyForRun` are deliberately not implemented; both belong to work
that is out of scope below.

**`events.subscribe`, the live event feed, is offered only to a delivery the
dispatcher granted one.** Every committed event write announces its run on the
tenant's events channel (`tenantEventsChannel`), and a force-claim announces its
victim run too: a `pg_notify` after the commit, never awaited, its failure
dropped. On each announcement a subscription reads the log from the slot after
the last one it delivered (the unfenced `events.list`) and delivers what
continues the sequence, each slot once; a hole stops it until a later
announcement fills it, and a lost announcement is caught up by the next. It ends
through `onError` on a `hook_received` carrying a `resumeId` (as upstream; see
[Hook resume deduplication](#hook-resume-deduplication)), a failed read, a lost `LISTEN`
connection, the end of the delivery that opened it, `world.close()` and
`stopLiveEventFeeds()`. The feed is a channel on the World's one `LISTEN`
connection, which stream readers share; a dropped connection is replaced with
backoff, and every stream reader then reads on from its position, so readers
outlive it. With a live feed eve holds a delivery open after its work is done
(its linger), which holds one of the dispatcher's slots, so `subscribe` refuses
(asynchronously, through `onError`) outside a delivery whose dispatch carried
`x-eveland-live-feed` (see [Request](#request)), and past
`WORKFLOW_WORLD_MAX_LIVE_EVENT_FEEDS` per process. A refused feed costs a
delivery nothing: the runtime polls the log tail and does not linger.

Several optional members are left unimplemented on purpose. Each one fails open,
and the reasons differ enough to be worth recording:

- **`getEnvironment()`** exists so a run's creating environment can travel on its
  queue message and a consuming deployment can notice it was handed a message
  from a different one. Eveland has no environment dimension for a World to
  report — a deployment serves exactly one project, and the tenant predicate is
  the boundary — so returning anything here would manufacture a distinction that
  does not exist. Upstream expects Postgres-backed Worlds to omit it.
- **`capabilities.resolvedHookResume`** lets core deliver to a cached Hook
  handle (eve's signed inbox handles) without a by-token lookup, sending
  `requireHookResumeDedup` with the write. Upstream's world-postgres 5.2.1 does
  not declare it and neither does this World, which ignores
  `requireHookResumeDedup` as upstream does: the contract requires refusing such
  a write before it writes whenever it cannot be deduplicated (for example one
  without a `resumeId`), and nothing in this World or in `@workflow/world`'s
  conformance modules checks that path. eve resolves those handles by token,
  and the write that follows is deduplicated through `hookResumeDedup`
  ([Hook resume deduplication](#hook-resume-deduplication)).
- **`CreateEventParams.computeInstanceId`** is ambient per-event identity, read
  back through the analytics surface. It is dropped for the same reason
  `requestId` already is: this World does not implement `analytics`, so there is
  nothing to read it back through.
- **`queue.isDeploymentUnavailableError`** lets the runtime distinguish "this
  deployment definitively cannot receive the message" from a transient failure.
  The dispatcher already makes exactly that distinction on the activation
  result — `not-activatable` dead-letters, `unavailable` retries — but it makes
  it dispatcher-side, where the activation lease is, rather than inside the
  World's queue surface. Nothing in the runtime path consults it here.
- **`runs.cancelMany`** is a bulk cancel, capped at 500 ids. `@workflow/core`'s
  `cancelRuns` falls back to bounded-concurrency single cancels when it is
  absent, which is correct.
- **`getRuntimeDeadline()`** reports when the hosting platform will terminate the
  current invocation. On this platform a step's invocation is a held vqs POST
  whose lifetime the dispatcher controls through lease renewal, so there is no
  externally-imposed wall clock for the World to report.

The queue payload's `hookInput` and `preconditionReinvocations`, and
`environment` on `RunInput`, do not reach this package: the vqs body is carried
as opaque bytes on `MessageData.data`, and only eve's own handler parses it.

**`capabilities.deploymentAffinity` is declared.** On its own it turns on the
runtime's affinity guard: a misrouted delivery is re-routed to the run's
deployment and ultimately fails the run with `DEPLOYMENT_MISMATCH`. This World
qualifies — the guard is unsafe only for Worlds whose deployment id is synthetic
or version-tagged, and `queue.getDeploymentId` returns the real one — but the
dispatcher already resolves affinity from the run row before any delivery
happens (`resolveAffinity`), so the guard alone would be a second mechanism for
a condition the first is designed to make unreachable. The flag has a second
reader: eve itself takes it to mean "this World routes each run to its own
deployment". A World without it is assumed to run one eve version at a time, so
eve compares every run's `$eve.version` attribute with its own version, parks
or cancels deliveries for runs another version created, and ends a session
whose owner run another version created (`session_stranded`) instead of
resuming that run and letting it hand the session over. On Eveland, where
Deployments on different eve versions run side by side, that would turn every
channel-address continuation across an eve-version promote into a reset. The
guard's remedy differs from the dispatcher's (a terminal run's straggler is
dropped by the dispatcher, failed by the guard), but with the dispatcher routing
on the run row the guard should never fire; if it does, the dispatcher's routing
has a hole worth fixing directly.

`CreateEventParams.preloadEvents` is an advisory opt-in on `hook_received` that a
World MAY answer with the run's complete replay log. It is ignored, which the
contract explicitly permits: the runtime observes that no usable preload came
back and falls back to its `run_started` setup. Answering it demands the whole log
read atomically with the `hook_received` write and `hasMore: false`, so it is a
performance option to take deliberately, not a field to fill in.

The replay page on `EventResult` is an all-or-none group. This World returns
`events`, `cursor` and `hasMore` together for `run_started`, and omits all three
keys for ordinary events unless `eventCount` or `sinceCursor` requests an inline
delta. Every run gets a `workflow_event_slots` row when it is created, starts at
`evnt_…0001`, and allocates dense positions inside the INSERT that occupies
them. The event key is therefore `(tenant_id, run_id, id)`.

### Hook force-claim

`createHook({ experimental_force: true })` asks for a token another run holds
and expects to get it rather than a `hook_conflict`. eve is built on it: a
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
   claimer's with `claimed_from`, and journal the claimer's
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
  writes its hook row and its event in one transaction, so a takeover that can
  see the row also sees the creation already journaled.

The World cannot publish the victim's wake -- the claimer's runtime does that
from `claimedFrom`, through the ordinary queue, with an idempotency key per
claimer hook.

### Hook resume deduplication

`capabilities.hookResumeDedup`. Because the World declares it, `resumeHook()` sends a `resumeId`
(one per logical resume, kept across force-claim redirects) and the hex
SHA-256 of the payload bytes with its `hook_received`. `createHookResume` in
`storage.ts`, ported from upstream world-postgres 5.2.1, writes it in one
transaction that locks the run and then the hook `FOR UPDATE`, the order of the
plain `hook_received` and of a force-claim:

1. A digest that does not match the payload: 422, before the transaction.
2. Run locked. Its data gone or going (`expired_at` set, or terminal and
   started with `$retention: 0`): 410 `INVOCATION_DATA_EXPIRED`, as upstream.
3. A `hook_received` already in the run with this `resumeId`: returned when its
   hook, digest and token match, even after the hook was disposed or the run
   finished; otherwise 422 (`Hook resume identity reused with different
contents`).
4. Otherwise as the plain write: a terminal run is `RunExpiredError`; the hook
   is locked, and one that is gone, belongs to another run or holds another
   token is `HookNotFoundError` (`HookForceClaimedError` after a takeover).
   The event is inserted with `resume_id` and `resume_payload_digest`.

The run lock serializes every resume of a run, so a second writer of one id
finds the first one's row. The partial unique index
`workflow_events_hook_resume_unique (tenant_id, run_id, resume_id) WHERE type =
'hook_received' AND resume_id IS NOT NULL` (migration 0002) is the backstop: its
violation retries once, then answers `EntityConflictError`. Events read back
carry `resumeId`; the digest is never returned. A write without a `resumeId`
takes the plain path, unchanged. A `$retention: 0` purge clears the digest with
the payload and keeps `resume_id`.

### In-band writer fence

A spec 9 run is driven by one orchestrator invocation at a time, and declares
`capabilities.inBandFence`. `workflow_event_slots.seq_in_band`
counts the positions accepted from the run's in-band writes -- everything its
orchestrator writes -- with `run_created` as the first. An in-band create
carries the count its writer expects (`expectedSeqInBand`), and storage runs it
in one transaction that first locks the run's slots row and compares: a stale
writer is refused with `InBandSupersededError` (412) before it touches any row,
and of concurrent writers holding the same count only the first through the
lock writes anything. The write's first slot-allocating insert advances the
count inside the same transaction, so a reader never sees the count include an
event it cannot list, and a write that appends nothing (a duplicate answered
with the existing event) leaves it alone. Out-of-band writes (a background
step, `resumeHook()`, `cancel()`) never take the lock and never move the count.
`events.list` reads `{ seq, seqInBand }` in one statement before the page, which
is where an orchestrator takes its count from. If creation commits between
these reads, the first query can miss the slot marker while the page already
contains slot-numbered events. The reader then reads the snapshot and page
again, in that order: attaching a newer count to the old page could hide an
in-band event the orchestrator has not consumed.

Two smaller spec 9 rules ride with it. An inline step's creation is its first
`step_started`, which keeps its input, and no synthetic `step_created` is
written for it: that would take a second position the orchestrator never
counted. And an in-band write is stored at the `occurredAt` it was sent with,
because the runtime hands an inline step's outcome to the workflow before the
write commits and reads the stored time back on replay.

Delivery accounting follows the runtime's queue contract
(`deliveryAccounting`, `continuationOf`). Every delivery reports
`x-vqs-message-delivery-count`, counted across retries, reschedules and
handoffs, because a spec 9 orchestrator re-sends a background step's message
from any delivery counted above 1. `x-vqs-message-attempt` starts over on a
`{ timeoutSeconds: n }` reschedule, as it always has, and carries over a spec 9
handoff (`{ timeoutSeconds: 0 }`). A spec 9 run's keyed message is enqueued
with graphile's `unsafe_dedupe`, so a re-send while the step's message lives is
a no-op rather than a second owner of its body.

Upstream's `@workflow/world-postgres` 5.1 implements the same contract with a
CTE on the event insert; holding the slots row lock through the insert makes a
plain conditional update enough here. `src/storage.in-band-fence.test.ts` runs
upstream's own conformance module against it.

### Atomic start Hook

`start(workflow, args, { experimental_hook: { token } })` admits a run together
with a Hook on `token`, and the World declares `capabilities.atomicStartHook`.
eve 1.0 starts every session this way, with the session's channel
address as the token, so two first messages racing to one address reach one
session; it refuses to start a session on a World that does not declare it.

`start()` publishes the candidate's queue message and then writes `run_created`;
the queued copy writes a resilient `run_started` carrying the same
`startHook`. Either may arrive first, so both decide in the transaction that
would create the run (`decideStartHookAdmission`, `src/start-hook.ts`). The
first attempt for a candidate run id records one immutable decision in
`workflow_start_hook_admissions`, which every later attempt,
through either path, returns:

- accepted: the run, its `run_created` (naming the start Hook) and the start
  Hook commit together. The start Hook is an ordinary `workflow_hooks` row with
  hook id `startHookId(runId)` and no `hook_created` of its own, so it resolves
  by token and takes deliveries while the run is still pending, and from then on
  follows the ordinary Hook lifecycle: disposal, the run's end, retention and
  force-claim. A repeat returns the run rather than `EntityConflictError`.
- rejected: nothing but the decision is written, and the refusal is
  `HookConflictError` naming the owner, thrown only after the decision commits.

Every Hook row claims its token (`claims_token`), and a
partial unique index allows one claiming row per token in a tenant, so the
insert, not the preceding read, decides between two admissions, or between an
admission and another run's `createHook`. A plain creation that loses the
claim answers `hook_conflict` like any other conflict. The index keys on
`workflow.hook_token_digest(token)`, a sha256, because a token can run to
several kilobytes. Past the 24h orphan bound
(`START_HOOK_CANDIDATE_MAX_AGE_MS`) a candidate is refused permanently with
`START_HOOK_ADMISSION_REJECTED`, which stops its queued copy retrying, and
decisions older than that are deleted as admissions go.

This is upstream world-postgres 5.1.1's implementation, scoped by tenant. Its
admission, Hook row and run commit in one transaction, so the orphan states a
World that admits in several writes must handle (a claim with no run yet) do
not exist here. `src/storage.atomic-start-hook.test.ts` runs upstream's own
conformance module against it; the five cases that stage those states are
skipped.

## Data model

### The tenancy column is `tenant_id`

`workflow_hooks` already carries a `project_id` column upstream, currently
written as an empty string. It is reserved for upstream's notion of a project,
and overloading it would collide the moment upstream populates it. So the
platform's tenancy column is `tenant_id`, and upstream's column keeps its own
meaning and is not read here.

### Tables

| table                            | partitioned | role                                                            |
| -------------------------------- | ----------- | --------------------------------------------------------------- |
| `workflow_runs`                  | no          | the dispatcher's index: tenant, deployment, status, attributes  |
| `workflow_steps`                 | no          | promote to partitioned if it outgrows runs — same recipe        |
| `workflow_hooks`                 | no          | plus `token_retention_until`, `claimed_from`, `claims_token`    |
| `workflow_waits`                 | no          | `resume_at` is the sleeping timer; graphile's `run_at` fires it |
| `workflow_events`                | LIST        | append-heavy, and the only write path for run state             |
| `workflow_stream_chunks`         | LIST        | one-row-per-chunk rows plus v2 packed physical blocks           |
| `workflow_event_slots`           | no          | one row per run: the in-band writer fence's count               |
| `workflow_start_hook_admissions` | no          | one decision per start Hook candidate; kept for 25 hours        |
| `dispatch_dead_letters`          | no          | platform-owned; see [Dead letters](#dead-letters)               |

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
  the children of both partial unique indexes on `workflow_events` (correlated
  events, `dedupIndexName`; hook resumes, `resumeDedupIndexName`), each found
  through `pg_inherits` from its parent, to derived, predictable names so that
  translating a unique-violation into a domain error can compare exactly rather
  than by pattern. Migration 0002 renamed the hook-resume children of the
  partitions that existed before it, deriving the same name in SQL.

### Indexes beyond the ported ones

`workflow_runs (tenant_id, status)` for fairness and queue-depth metrics;
`workflow_runs (deployment_id) WHERE status IN ('pending','running')` for the
retention guard, partial so it stays small as terminal runs accumulate; and
`workflow_runs (tenant_id, created_at DESC)` for listing surfaces. The
caller-selected stream cleanup uses the partial expression index on
`coalesce(completed_at, updated_at), tenant_id, id`; deadline-driven maintenance
uses the `expire_after` and `detail_expire_after` indexes, then
the `(tenant_id, retention_root_run_id, status)` index to validate whole-lineage
eligibility. `workflow_hooks (tenant_id, hook_token_digest(token)) WHERE
claims_token` is unique: it is what makes a token's owner one row (see
[Atomic start Hook](#atomic-start-hook)).

graphile's own tables live in the same database, in their own schema, untouched.

### Stream storage and terminal retention

The stream boundary is logical, not physical. Readers serve stored bytes
verbatim, and packed rows retain every logical chunk id, so cursors,
`startIndex`, and `tailIndex` do not depend on how rows are stored.

Eligibility is joined through both `tenant_id` and `run_id`; only non-EOF chunks
of completed, failed or cancelled runs older than the caller's window are
removed. Candidate rows are ordered oldest-first and deleted in bounded batches.
Because chunks are partitioned, the physical row identity is `(tableoid, ctid)` —
`ctid` alone is not unique across child tables. One dedicated connection holds a
session advisory lock across the batch loop, preventing independently scheduled
workers from multiplying database load.

New runs record an explicit retention class. A database trigger converts a
terminal transition into `expire_after` and
`detail_expire_after`, which prevents write-time deletion from racing final
NOTIFY/read delivery. Successful scheduled runs use 15 minutes / 24
hours. Failed scheduled runs use 1 hour / 7 days, and cancelled ones
use 1 hour / 3 days. Interactive runs use 24 hours / 30
days; persistent runs receive no deadlines. Active runs receive none either.

Creation resolves explicit input, the public run attribute, tenant-scoped SDK
root/parent lineage, then an optional platform-owned root invocation context in
that order. Only a root can reach the context/default branch; descendants with
lineage inherit the stored ancestor class or fail if that ancestor cannot be
resolved. This keeps the World generic: it contains no Eve workflow-name or
trigger policy, and a later delivery to an existing graph cannot reclassify it.
Every member also stores the resolved root id, avoiding recursive JSON lineage
walks during maintenance.

The dispatcher runs bounded maintenance once at startup and every minute. Stream
expiry and graph expiry are failure-isolated and protected by
advisory locks. Eligibility is decided across the complete stored lineage: any
active member, later member deadline, persistent member, or unexpired retained
hook holds both stream and graph data. Stream and graph expiry preserve EOF
forever. The caller-selected `pruneTerminalStreamChunks(retentionMs)` entry
point serves hosts that sweep on their own schedule.
Expiring chunks expires replay from old raw cursors even though EOF still makes
the stream resolve as complete. Normal deletes make pages reusable but do not
promise to shrink relation files.

`$retention: 0` is the per-run exception to waiting for a deadline
(`run-data-purge.ts`). The writer that wins a run's terminal transition clears
the run's payload columns and stamps `expired_at` after writing the terminal
event, so the event's own payload goes too; the response it returns is a
pre-purge snapshot. Host reconciliation does the same inside its settle
transaction. A purge failure is logged and never fails the transition. Stream
rows are deleted rather than blanked, since a packed block cannot be blanked
without losing its chunk ids, and EOF stays. The purge writes no column the
deadline trigger or the expiry predicates read, so the retention class still
removes the rows later. `hook_disposed` payloads stay: they are the token and
force-claim routing that `forceClaimRefusal` reads, and an eve handoff victim
finishes right after it is taken over. A resume's payload digest goes with its
payload; its `resume_id` stays, and a retry of the resume is answered 410.

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

`''` means the creating deployment resolved none, so the default prefix is
correct. It is a safe sentinel because upstream requires a namespace to match
`^[a-z][a-z0-9]*$`, so it can never collide with a real one.

This is **not** tenancy. Invariant 2 stands: prefix and namespace isolation leak,
and tenancy is the `tenant_id` column. This column exists only so the platform
can address the topic eve already chose.

## The dispatch contract

One held `POST <executor>/.well-known/workflow/v1/flow` per in-flight step, open
for the step's whole duration. The POST returning is what marks the job complete.

`<executor>` comes from the activation. `endpointPort` means loopback,
`http://127.0.0.1:<endpointPort>`, which is every host that runs its executors
beside the dispatcher. `endpointUrl` is an origin anywhere else — typically a
service in front of several replicas of one deployment — and wins when both are
present. Only the origin is used: the route is this package's to
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
| `x-eveland-live-feed`                                 | this package | `1` when the dispatcher granted this delivery a live feed     |

The live feed grant is optional within version 1. A dispatcher that does not
send it gets a World that refuses every feed, so no delivery lingers; a World
that does not read it ignores it. The dispatcher grants it only to orchestrator
deliveries (those `runQueueFor` places in a run queue), and only while granted
deliveries in flight stay at or below half its concurrency (rounded down, so a
concurrency of 1 grants none) and at or below half the tenant's in-flight
ceiling (rounded down, at least 1). It strips the header from a message's
producer headers, so a deployment cannot grant itself one. See
[live feed grants](operations/dispatcher.md#live-feed-grants).

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
number and one switch from this package. A delivery cut off by a stopping process is delivered
again — correct, but the step and its side effects run a second time. So the
host first stops routing to the replica (its readiness check fails), calls
`stopLiveEventFeeds()` so a delivery lingering on a live feed acknowledges at
once instead of waiting its linger out, waits for `inflightDeliveries()` to
reach zero, and only then sends the signal. Both are process-wide and live on
`globalThis`, because the runtime loads the World from its package name while
the host route that reports the number is usually bundled separately.

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
definition (`runQueueFor` in `src/dispatch-contract.ts`).

**Only a run's orchestrator deliveries wait in its run queue, and nothing
bypasses it.** Every run is a spec 9 (single-orchestrator) run. Its step
messages and a long wait's intermediate timer hops take no queue name: an
orchestrator runs its inline steps for as long as they take, and one the
dispatcher granted a live feed stays live up to 30 s more while it waits on a
background step (eve's linger; 5 s while only a hook is open), so a step queued
behind it would wait all of that out before it could start. A delivery that was
not granted a feed does not linger. Its hook wakes wait in the queue too: the
running orchestrator tails the run's log, so a resume or a cancel reaches it
without a second delivery, and a second orchestrator would run an inline step
the first is still running again. What the queue still lets through (a delivery that outlives its dispatch
timeout) the in-band writer fence refuses. A message that names a run but
carries no spec stamp (boot recovery rebuilds one from the run row) also waits
in that run's queue.

The wake a force-claim sends its victim queues on the victim's run like any
other delivery. The claimer's runtime publishes it from `Hook.claimedFrom` with
an idempotency key of `hook-force-claim-<claimer hookId>`. Nothing is lost by
waiting: the `hook_disposed{forceClaimedBy}` it announces committed before it
was sent, and whichever replay of the victim runs next reads it.

A step can be started by two deliveries at once. Its step message waits in no
run queue, so past the owner's ownership lease (860 s by default) a step
message can be claimed while the orchestrator still runs the step inline, on
any replica. So storage keeps the delivery that started a step on the step
(`owner_message_id`), and refuses another delivery's `step_started` with a
retry while that delivery's graphile job is still locked
(`deliveryInFlight`). The owner's own redelivery may start it again, and a
refused start comes back later, at a tenth of how long the step has run (30 s
to 5 min): each retry is a delivery, so the retries thin out on a long step.
The restart fence reads graphile's private job table by payload, which has no
index. It runs only when a running step is started again, which is rare, and a
failed read lets the start through.

The fence ends when the owner's delivery does. A delivery past the dispatch
timeout is aborted and its job unlocked while the step body may still run on
its executor, and the owner's own redelivery may then run it again.

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
message permanently. The bound is deliberate: a durable table would cost a write per message on the
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
apply the same exclusion, so an unresolved dispatch failure does not protect a
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
  database trigger. Both record the caller's reason in `reconciled_reason`.
  `fail` also records a machine-readable `error_code` and leaves `error_cbor`
  NULL — this process cannot honestly mint runtime-serialized error data, and
  pretending otherwise would poison hydration.
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

Every variable has one `WORKFLOW_*` name. The full table is in the
[configuration reference](./reference/configuration.md) and is not duplicated
here — a second copy would drift.

Two properties of the surface are design rather than documentation. The database
URL has **no fallback chain**: falling back onto a plausible-looking single-tenant
database is worse than failing to start, so a missing value throws by name. And
both ends of the system read the same names, because a name
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
- **A single dispatcher is a restart-pause single point of failure.** Keeping it
  stateless — all claim state in Postgres — is what makes a restart a brief pause
  plus boot recovery rather than data loss.
- **graphile's jobs table is hot on a shared database**, with delete-on-complete
  churn. Autovacuum behaviour there is worth a metric.
- **Payloads are unencrypted at rest** on a database every tenant shares.
- **Partition count has a ceiling**, with the documented escape hatch above.
