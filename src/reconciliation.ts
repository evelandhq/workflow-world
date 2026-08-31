import type { BulkCancelWorkflowRunsResult } from "@workflow/world";
import { and, asc, eq, inArray } from "drizzle-orm";
import type { Pool } from "pg";
import { createClient, Schema } from "./drizzle/index.js";
import { createRunsCancellation } from "./run-cancellation.js";
import { createEventsStorage } from "./storage.js";
import { assertValidTenantId } from "./tenant.js";

/**
 * Host-driven run reconciliation.
 *
 * Run status is World-owned: the host platform must never write
 * `workflow.workflow_runs` directly, but it is the only party that knows when a
 * run's agent process is gone for good — an idle-reaped RuntimeInstance whose
 * Sessions were settled, or a Deployment that can never activate again. These
 * helpers are the supported write path for that knowledge. They are
 * cross-tenant on purpose (the host has no tenant-bound World), and every
 * cancellation goes through the same event-sourced termination path the World
 * itself uses.
 *
 * Deliberately NOT done here:
 * - `dispatch_dead_letters` rows are left untouched. They are the operator's
 *   evidence of what failed and why; a terminal run status already keeps the
 *   run out of boot recovery and dispatch on its own.
 * - No stream EOF is appended. Same policy as the dispatcher's dead-letter
 *   path: the World does not speak on the workflow's behalf.
 */

export type ActiveWorkflowRunRef = {
  tenantId: string;
  runId: string;
  deploymentId: string;
  status: "pending" | "running";
  createdAt: Date;
  updatedAt: Date | null;
};

export type ListActiveWorkflowRunsInput = {
  /** Restrict to one tenant. Omit to list across all tenants. */
  tenantId?: string;
  /** Restrict to runs bound to one deployment. */
  deploymentId?: string;
  /** Cap the result set. Defaults to 1000. */
  limit?: number;
};

/**
 * Active (`pending`/`running`) runs, oldest first, for the host to judge.
 * This is a read the host previously hand-rolled against the World's tables;
 * exporting it keeps the SQL — and the schema knowledge — in one repo.
 */
export async function listActiveWorkflowRuns(
  pool: Pool,
  input: ListActiveWorkflowRunsInput = {},
): Promise<ActiveWorkflowRunRef[]> {
  if (input.tenantId !== undefined) assertValidTenantId(input.tenantId);
  const drizzle = createClient(pool);
  const rows = await drizzle
    .select({
      tenantId: Schema.runs.tenantId,
      runId: Schema.runs.runId,
      deploymentId: Schema.runs.deploymentId,
      status: Schema.runs.status,
      createdAt: Schema.runs.createdAt,
      updatedAt: Schema.runs.updatedAt,
    })
    .from(Schema.runs)
    .where(
      and(
        inArray(Schema.runs.status, ["pending", "running"]),
        input.tenantId !== undefined ? eq(Schema.runs.tenantId, input.tenantId) : undefined,
        input.deploymentId !== undefined
          ? eq(Schema.runs.deploymentId, input.deploymentId)
          : undefined,
      ),
    )
    .orderBy(asc(Schema.runs.createdAt))
    .limit(input.limit ?? 1000);
  return rows as ActiveWorkflowRunRef[];
}

export type CancelWorkflowRunsInput = {
  tenantId: string;
  /** 1–500 unique run ids, all belonging to `tenantId`. */
  runIds: string[];
  /** Recorded on each `run_cancelled` event (max 512 chars). Say why the host settled the run. */
  cancelReason?: string;
};

/**
 * Cancel runs the host knows are abandoned. Tenant-scoped per call; outcomes
 * are per-run and idempotent (`already_cancelled` on a repeat sweep), so
 * calling this from a periodic reconciler is safe.
 */
export async function cancelWorkflowRuns(
  pool: Pool,
  input: CancelWorkflowRunsInput,
): Promise<BulkCancelWorkflowRunsResult> {
  assertValidTenantId(input.tenantId);
  const drizzle = createClient(pool);
  const events = createEventsStorage(drizzle, input.tenantId);
  const cancelMany = createRunsCancellation(drizzle, input.tenantId, events);
  return cancelMany({
    runIds: input.runIds,
    ...(input.cancelReason !== undefined ? { cancelReason: input.cancelReason } : {}),
  });
}
