# Storage and retention

[Documentation](../README.md) · [Maintenance configuration](../reference/configuration.md#host-side-read-by-the-dispatcher)

## Stream storage safety boundary

Logical chunks stay intact at the public boundary, while the database uses two
internal optimizations:

- `messageSoFar` and `reasoningSoFar` are stripped before persistence. Every
  supported Eve line writes delta-only appends (message stream v25), so this is
  a no-op guard against a v24-shaped write rather than a transform in daily
  use. Unknown framing and event shapes pass through unchanged; the
  deployment-side `WORKFLOW_WORLD_STREAM_COMPACTION=off` switch disables only new stripping. Readers serve the
  stored bytes verbatim: nothing rebuilds a snapshot on read.
- `writeMulti` packs up to 64 logical chunks into a physical v2 block, capped at
  256 KiB. Readers expand single-chunk rows and v2 blocks into the same logical stream.
  `packTerminalStreamBlocks` is the bounded, advisory-locked fallback for streams
  written one row at a time before EOF.

Logical chunk IDs remain inside each block and continue to drive cursor and
`startIndex` behavior. Repacking physical rows therefore does not invalidate an
existing cursor.

## Retention classes and maintenance

Every run has one internal retention class:

| class / outcome                      | compact after | expire non-EOF stream data | expire workflow graph |
| ------------------------------------ | ------------: | -------------------------: | --------------------: |
| `scheduled`, completed               |      1 minute |                 15 minutes |              24 hours |
| `scheduled`, failed                  |      1 minute |                     1 hour |                7 days |
| `scheduled`, cancelled               |      1 minute |                     1 hour |                3 days |
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
