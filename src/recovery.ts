import {
  getQueueTopicPrefix,
  type Queue,
  type Storage,
  type ValidQueueName,
} from "@workflow/world";

/**
 * Re-enqueue this tenant's active (pending/running) runs so they resume after a
 * restart. The workflow handler is idempotent — it replays the event log — so
 * duplicate enqueues are safe.
 *
 * This exists instead of upstream's `reenqueueActiveRuns` because that function
 * lists runs unfiltered. With one database per project that was merely wasteful;
 * on a shared database it would mean every agent's boot re-enqueues every
 * project's active runs. Passing an already tenant-scoped `runs` makes the
 * scoping structural rather than something this function has to remember.
 */
export async function reenqueueTenantRuns(input: {
  runs: Storage["runs"];
  enqueue: Queue["queue"];
  tenantId: string;
}): Promise<number> {
  /**
   * The default prefix here is not a namespace bug, though it reads like one.
   * `enqueue` is the deployment's own `queue.queue`, which runs the name back
   * through `parseQueueName` and keeps only the bare sub-queue id — the prefix
   * is discarded, and the closure re-attaches this deployment's real namespace
   * on the way out. So this call site cannot get the namespace wrong, and
   * passing the resolved one would change nothing.
   *
   * The external dispatcher's sweep is the one that had to change, because it
   * builds the message itself and has no such closure. See
   * `dispatcher/boot-recovery.ts`.
   */
  const workflowQueuePrefix = getQueueTopicPrefix("workflow");
  let reenqueued = 0;

  for (const status of ["pending", "running"] as const) {
    let cursor: string | undefined;
    let hasMore = true;
    while (hasMore) {
      const page = await input.runs.list({
        status,
        resolveData: "none",
        pagination: { cursor },
      });
      for (const run of page.data) {
        try {
          const queueName = `${workflowQueuePrefix}${run.workflowName}` as ValidQueueName;
          await input.enqueue(queueName, { runId: run.runId });
          reenqueued += 1;
        } catch (error) {
          console.warn(
            `[eveland workflow world] Failed to re-enqueue run ${run.runId} for ${input.tenantId}: ${String(error)}`,
          );
        }
      }
      hasMore = page.hasMore;
      cursor = page.cursor ?? undefined;
    }
  }

  if (reenqueued > 0) {
    console.log(
      `[eveland workflow world] Re-enqueued ${String(reenqueued)} active run(s) for ${input.tenantId} on startup`,
    );
  }
  return reenqueued;
}
