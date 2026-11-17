# Storage and retention

[Documentation](../README.md) · [Maintenance configuration](../reference/configuration.md#host-side-read-by-the-dispatcher)

## Stream storage safety boundary

Logical chunks stay intact at the public boundary, while the database stores
them physically in two shapes: one row per chunk, and v2 blocks. `writeMulti`
packs up to 64 logical chunks into one block, capped at 256 KiB. Readers expand
single-chunk rows and v2 blocks into the same logical stream, and serve stored
bytes verbatim.

Logical chunk IDs remain inside each block and continue to drive cursor and
`startIndex` behavior.

## Retention classes and maintenance

Every run has one internal retention class:

| class / outcome                      | expire non-EOF stream data | expire workflow graph |
| ------------------------------------ | -------------------------: | --------------------: |
| `scheduled`, completed               |                   1 minute |            15 minutes | 24 hours |
| `scheduled`, failed                  |                   1 minute |                1 hour | 7 days   |
| `scheduled`, cancelled               |                   1 minute |                1 hour | 3 days   |
| `interactive` (default), any outcome |                  5 minutes |              24 hours | 30 days  |
| `persistent`                         |                      never |                 never | never    |

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

The dispatcher runs deadline-driven stream expiry, and full graph
expiry once at startup and every minute. Each task is bounded, advisory-locked,
non-overlapping, and failure-isolated. A lineage remains ineligible while any
member is active, has a later deadline, is persistent, or owns a hook token whose
reservation has not expired. This protects terminal parents while background
children, approvals, callbacks, or task-input capabilities remain live.

### Zero retention

A run started with `$retention: 0` loses its user data when it reaches a
terminal state, whether the workflow finished it or `reconcileWorkflowRuns`
failed or cancelled it. In one transaction the run gets `expired_at`, and the
payload columns of the run, its steps, its events (except `hook_disposed`,
which holds only the token and force-claim routing) and its retained hooks are
set to NULL; the run's non-EOF stream rows are deleted, and its dead letters
keep their row with an empty message body. Ids, statuses, timestamps,
attributes and EOF rows stay, so the run still lists and its streams still end.
Any other value keeps the data and logs a warning.

eve 1.0 asks for zero retention by default unless its tracing records inputs and
outputs (`experimental.workflow.retention` overrides it), so most eve runs take
this path. Stream rows belong to the run id their writer passed. An eve session's stream
is written under its first run's id, so a handoff successor finishing does not
touch it; the stream goes when the first run ends.

The purge does not change the run's retention class or deadlines. The class
still decides when the purged rows are deleted, and a `persistent` run keeps
them, empty, indefinitely.

### Manual stream cleanup

A caller-selected primitive is available for hosts that need to cap stream
snapshot growth outside the dispatcher's maintenance:

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
