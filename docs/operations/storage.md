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
  stored bytes verbatim: nothing rebuilds a snapshot on read, and the
  `workflow_stream_checkpoints` table that once held rehydration state is no
  longer written (it stays declared until a migration drops it).
- `writeMulti` packs up to 64 logical chunks into a physical v2 block, capped at
  256 KiB. Readers expand legacy rows and v2 blocks into the same logical stream.
  `packTerminalStreamBlocks` is the bounded, advisory-locked fallback for streams
  written one row at a time before EOF.

Logical chunk IDs remain inside each block and continue to drive cursor and
`startIndex` behavior. Repacking physical rows therefore does not invalidate an
existing cursor.

## Retention classes and maintenance

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

### Manual stream cleanup

The legacy caller-selected primitive remains available for hosts that do not run
the dispatcher and need to cap stream snapshot growth:

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

### Previewing and repairing historical scheduler graphs

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
