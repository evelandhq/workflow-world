import {
  BulkCancelWorkflowRunsRequestSchema,
  isTerminalWorkflowRunStatus,
  type BulkCancelWorkflowRunResult,
  type BulkCancelWorkflowRunsResult,
  type Storage,
} from "@workflow/world";
import {
  EntityConflictError,
  WorkflowRunNotFoundError,
  WorkflowWorldError,
} from "@workflow/errors";
import { and, eq } from "drizzle-orm";
import { type Drizzle, Schema } from "./drizzle/index.js";

/**
 * `runs.cancelMany` — the optional bulk-cancel surface the `Storage` interface
 * declares. Every cancellation is routed through `events.create` so it takes
 * the one termination path the World has: a `run_cancelled` event with an
 * allocated slot id, the terminal-guarded status UPDATE, and the hook/wait
 * cleanup — never a bare status write that would leave the event log claiming
 * the run is still going.
 *
 * Idempotent by outcome rather than by side effect: a run that is already
 * cancelled reports `already_cancelled` without appending another
 * `run_cancelled` event, so a reconciliation sweep that runs every few seconds
 * does not grow the event log of runs it has already settled.
 */
export function createRunsCancellation(
  drizzle: Drizzle,
  tenantId: string,
  events: Storage["events"],
): NonNullable<Storage["runs"]["cancelMany"]> {
  const readStatus = async (runId: string): Promise<string | undefined> => {
    const [row] = await drizzle
      .select({ status: Schema.runs.status })
      .from(Schema.runs)
      .where(and(eq(Schema.runs.tenantId, tenantId), eq(Schema.runs.runId, runId)))
      .limit(1);
    return row?.status;
  };

  const cancelOne = async (
    runId: string,
    cancelReason: string | undefined,
  ): Promise<BulkCancelWorkflowRunResult> => {
    const status = await readStatus(runId);
    if (status === undefined) return { runId, outcome: "not_found" };
    if (status === "cancelled") return { runId, outcome: "already_cancelled" };
    if (isTerminalWorkflowRunStatus(status)) {
      return { runId, outcome: "not_cancellable", status };
    }
    try {
      await events.create(runId, {
        eventType: "run_cancelled",
        ...(cancelReason !== undefined ? { eventData: { cancelReason } } : {}),
      });
      return { runId, outcome: "cancelled" };
    } catch (error) {
      // The pre-read races against every other writer, so the authoritative
      // answer is the write's own terminal guard. Re-read to name the state
      // that won.
      if (error instanceof EntityConflictError) {
        const lost = await readStatus(runId);
        if (lost === "cancelled") return { runId, outcome: "already_cancelled" };
        if (lost !== undefined && isTerminalWorkflowRunStatus(lost)) {
          return { runId, outcome: "not_cancellable", status: lost };
        }
      }
      if (error instanceof WorkflowRunNotFoundError) {
        return { runId, outcome: "not_found" };
      }
      return {
        runId,
        outcome: "failed",
        code: error instanceof WorkflowWorldError ? error.constructor.name : "internal_error",
        // A conflict that did not resolve to a terminal status above is a
        // transient race; anything else is unknown, and retrying an unknown
        // failure is the safe default for a cancellation.
        retryable: true,
      };
    }
  };

  return async (request) => {
    const parsed = BulkCancelWorkflowRunsRequestSchema.parse(request);
    const results: BulkCancelWorkflowRunResult[] = [];
    for (const runId of parsed.runIds) {
      results.push(await cancelOne(runId, parsed.cancelReason));
    }
    const count = (outcome: BulkCancelWorkflowRunResult["outcome"]) =>
      results.filter((r) => r.outcome === outcome).length;
    const summary: BulkCancelWorkflowRunsResult["summary"] = {
      requested: parsed.runIds.length,
      cancelled: count("cancelled"),
      alreadyCancelled: count("already_cancelled"),
      notCancellable: count("not_cancellable"),
      notFound: count("not_found"),
      failed: count("failed"),
    };
    return { summary, results };
  };
}
