import {
  HookConflictError,
  START_HOOK_ADMISSION_REJECTED,
  WorkflowWorldError,
} from "@workflow/errors";
import { debugLog } from "@workflow/utils";
import {
  START_HOOK_CANDIDATE_MAX_AGE_MS,
  type StartHook,
  startHookId,
  TERMINAL_WORKFLOW_RUN_STATUSES,
  ulidToDate,
} from "@workflow/world";
import {
  and,
  eq,
  exists,
  gt,
  inArray,
  isNull,
  lte,
  ne,
  notExists,
  or,
  type SQL,
  sql,
} from "drizzle-orm";
import { type Drizzle, Schema } from "./drizzle/index.js";
import type { RunStartHook } from "./drizzle/schema.js";

/**
 * Atomic start Hook admission (`start(workflow, args, { experimental_hook })`,
 * `capabilities.atomicStartHook`). See `start-hook.ts` in @workflow/world for
 * the contract; this is upstream world-postgres's implementation (its
 * `start-hook.ts`, 5.1.1), scoped by tenant.
 *
 * An accepted candidate's admission inserts its start Hook, an ordinary row in
 * `workflow_hooks` with hook id `startHookId(runId)`, in the transaction that
 * records the decision and creates the run. From that commit on the Hook
 * resolves by token and accepts deliveries like any Hook, and it follows the
 * ordinary Hook lifecycle: `hook_disposed` deletes the row, and the run's end
 * releases it unless its `token_retention_until` is in the future. No
 * `hook_created` is ever written for it: `run_created` stands for it.
 *
 * The start Hook row claims its token (`claims_token`), and a partial unique
 * index allows one claiming row per token in a tenant, so two admissions racing
 * for one token conflict on the insert instead of both winning. Postgres has no
 * orphan state: the decision, the Hook row and the run commit together or not
 * at all.
 */

/** What a transaction must offer the admission. */
export type StartHookTx = Pick<Drizzle, "select" | "insert" | "delete" | "execute">;

export type StartHookDecision =
  | { decision: "accepted"; replayed: boolean }
  | { decision: "rejected"; conflictingRunId: string }
  | { decision: "ordinary" };

/** The start Hook as the run's `run_created` event carries it. */
export function admittedStartHook(runId: string, startHook: StartHook): StartHook {
  return { ...startHook, hookId: startHookId(runId) };
}

/** The start Hook without its metadata: what the run row keeps. */
export function runStartHook(runId: string, startHook: StartHook): RunStartHook {
  const { metadata: _metadata, ...rest } = startHook;
  return { ...rest, hookId: startHookId(runId) };
}

/**
 * Throws the permanent refusal of a start Hook request this World will never
 * admit: a retention past its limit. A refusal carries the code that stops
 * the queued copy of the candidate retrying.
 */
export function assertStartHookAdmissible(startHook: StartHook, retentionLimitMs: number): void {
  if (
    startHook.tokenRetentionUntil !== undefined &&
    new Date(startHook.tokenRetentionUntil).getTime() > Date.now() + retentionLimitMs
  ) {
    throw new WorkflowWorldError(
      `Hook minimum retention cannot exceed ${String(retentionLimitMs / 86_400_000)} days in the Postgres World.`,
      { status: 400, code: START_HOOK_ADMISSION_REJECTED, reason: "invalid" },
    );
  }
}

/** The refusal for a candidate past the orphan bound. */
export function startHookCandidateExpired(runId: string): WorkflowWorldError {
  return new WorkflowWorldError(
    `Workflow run "${runId}" is too old to be admitted with a start Hook`,
    { status: 400, code: START_HOOK_ADMISSION_REJECTED, reason: "expired" },
  );
}

/** The refusal a rejected decision answers with, after its commit. */
export function startHookConflict(
  startHook: StartHook,
  conflictingRunId: string,
): HookConflictError {
  return new HookConflictError(startHook.token, conflictingRunId);
}

/**
 * Same predicate as `hookTokenEquals` in storage.ts: filters on `md5(token)`
 * so the lookup can use migration 0018's expression index, and keeps the exact
 * comparison so a digest collision cannot resolve to another token.
 */
function tokenEquals(token: string): SQL {
  return sql`md5(${Schema.hooks.token}) = md5(${token}) and ${Schema.hooks.token} = ${token}`;
}

/**
 * Decides admission for candidate `runId` inside the transaction that would
 * create its run.
 *
 * - A recorded decision is returned as-is (`replayed: true` for an accepted
 *   one, whose run already exists).
 * - A run that exists with no decision was created without a start Hook:
 *   `ordinary`, and nothing is written.
 * - A new candidate is accepted when no other run holds the token, writing its
 *   start Hook row and `accepted`; the caller then creates the run in the same
 *   transaction.
 * - Otherwise it is rejected, recording the owner. The caller must commit the
 *   transaction and then throw {@link startHookConflict}.
 */
export async function decideStartHookAdmission(
  tx: StartHookTx,
  tenantId: string,
  runId: string,
  startHook: StartHook,
  specVersion: number,
): Promise<StartHookDecision> {
  const hooks = Schema.hooks;
  const ownerRunIsTerminal = tx
    .select({ runId: Schema.runs.runId })
    .from(Schema.runs)
    .where(
      and(
        eq(Schema.runs.tenantId, tenantId),
        eq(Schema.runs.runId, hooks.runId),
        inArray(Schema.runs.status, TERMINAL_WORKFLOW_RUN_STATUSES),
      ),
    );
  /** The run owning a row of `token` that still holds it, other than this candidate. */
  const findActiveTokenOwner = async (): Promise<string | undefined> => {
    const [row] = await tx
      .select({ runId: hooks.runId })
      .from(hooks)
      .where(
        and(
          eq(hooks.tenantId, tenantId),
          tokenEquals(startHook.token),
          ne(hooks.runId, runId),
          or(gt(hooks.tokenRetentionUntil, sql`now()`), notExists(ownerRunIsTerminal)),
        ),
      )
      .limit(1);
    return row?.runId;
  };

  const recorded = await recordedDecision(tx, tenantId, runId, startHook);
  if (recorded) return recorded;

  // Orphan bound: a candidate this old can never be created. It also bounds
  // how long a decision matters, which is what lets old rows be deleted.
  const candidateTime = ulidToDate(runId.replace(/^wrun_/, ""));
  if (candidateTime && Date.now() - candidateTime.getTime() > START_HOOK_CANDIDATE_MAX_AGE_MS) {
    throw startHookCandidateExpired(runId);
  }

  const [run] = await tx
    .select({ runId: Schema.runs.runId })
    .from(Schema.runs)
    .where(and(eq(Schema.runs.tenantId, tenantId), eq(Schema.runs.runId, runId)))
    .limit(1);
  if (run) return { decision: "ordinary" };

  await deleteExpiredDecisions(tx);

  // A Hook whose run ended and whose retention passed no longer holds the
  // token, and would block the start Hook's unique claim.
  const released = await tx
    .delete(hooks)
    .where(
      and(
        eq(hooks.tenantId, tenantId),
        tokenEquals(startHook.token),
        exists(ownerRunIsTerminal),
        or(isNull(hooks.tokenRetentionUntil), lte(hooks.tokenRetentionUntil, sql`now()`)),
      ),
    )
    .returning({ runId: hooks.runId });
  for (const { runId: previousRunId } of released) {
    startHookLog("released an ended owner's token", { runId, previousRunId });
  }

  const owner = await findActiveTokenOwner();
  const [startHookRow] = owner
    ? []
    : await tx
        .insert(hooks)
        .values({
          tenantId,
          runId,
          hookId: startHookId(runId),
          token: startHook.token,
          ownerId: "",
          projectId: "",
          environment: "",
          tokenRetentionUntil:
            startHook.tokenRetentionUntil === undefined
              ? undefined
              : new Date(startHook.tokenRetentionUntil),
          metadata: startHook.metadata as typeof hooks.$inferInsert.metadata,
          specVersion,
          isWebhook: false,
          isSystem: false,
          claimsToken: true,
        })
        // Untargeted: the other copy of this candidate inserts the same
        // `hook_id` as well as the same claim.
        .onConflictDoNothing()
        .returning({ hookId: hooks.hookId });

  if (!startHookRow) {
    // Another claim committed first (the unique insert waited for it): the
    // other copy of this same candidate, whose decision is now visible, or
    // another run.
    const raced = await recordedDecision(tx, tenantId, runId, startHook);
    if (raced) return raced;
    const conflictingRunId = owner ?? (await findActiveTokenOwner());
    if (!conflictingRunId) {
      throw new WorkflowWorldError(
        `The start Hook token of candidate "${runId}" changed owner while being admitted; retry`,
        { status: 503 },
      );
    }
    return recordDecision(
      tx,
      tenantId,
      runId,
      startHook,
      { decision: "rejected", conflictingRunId },
      { decision: "rejected", conflictingRunId },
    );
  }
  return recordDecision(
    tx,
    tenantId,
    runId,
    startHook,
    { decision: "accepted" },
    { decision: "accepted", replayed: false },
  );
}

/**
 * Records the decision, unless the other copy of this candidate recorded one
 * first (both can decide a refusal concurrently: neither writes the Hook row
 * that would serialize them). That one wins, as it would for any later
 * attempt. An acceptance that loses to a refusal must not commit: its start
 * Hook, already written in this transaction, would hold the token for a run
 * that is never created. It answers a retryable error instead, which rolls the
 * Hook back, and the retry replays the refusal.
 */
async function recordDecision(
  tx: StartHookTx,
  tenantId: string,
  runId: string,
  startHook: StartHook,
  row: { decision: "accepted" | "rejected"; conflictingRunId?: string },
  decided: StartHookDecision,
): Promise<StartHookDecision> {
  const admissions = Schema.startHookAdmissions;
  const [inserted] = await tx
    .insert(admissions)
    .values({ tenantId, runId, token: startHook.token, ...row })
    .onConflictDoNothing({ target: [admissions.tenantId, admissions.runId] })
    .returning({ runId: admissions.runId });
  if (inserted) {
    startHookLog(row.decision === "accepted" ? "accepted a candidate" : "refused a candidate", {
      runId,
      ...(row.conflictingRunId ? { conflictingRunId: row.conflictingRunId } : {}),
    });
    return decided;
  }
  const recorded = await recordedDecision(tx, tenantId, runId, startHook);
  if (!recorded || (row.decision === "accepted" && recorded.decision !== "accepted")) {
    throw new WorkflowWorldError(
      `Workflow run "${runId}" was decided by another write while being admitted; retry`,
      { status: 503 },
    );
  }
  return recorded;
}

/** The decision recorded for candidate `runId`, if any. */
async function recordedDecision(
  tx: StartHookTx,
  tenantId: string,
  runId: string,
  startHook: StartHook,
): Promise<StartHookDecision | undefined> {
  const admissions = Schema.startHookAdmissions;
  const [existing] = await tx
    .select()
    .from(admissions)
    .where(and(eq(admissions.tenantId, tenantId), eq(admissions.runId, runId)))
    .limit(1);
  if (!existing) return undefined;
  if (existing.token !== startHook.token) {
    throw new WorkflowWorldError(
      `Workflow run "${runId}" was already admitted with a different start Hook token`,
      { status: 400, code: START_HOOK_ADMISSION_REJECTED, reason: "token_mismatch" },
    );
  }
  return existing.decision === "accepted"
    ? { decision: "accepted", replayed: true }
    : { decision: "rejected", conflictingRunId: existing.conflictingRunId ?? "" };
}

/**
 * Deletes a bounded batch of decisions older than every candidate window: no
 * attempt for those candidates can be admitted any more, so nothing reads
 * them. Bounded so one admission never pays for a backlog, and across tenants
 * because the window is the same for all of them.
 */
async function deleteExpiredDecisions(tx: StartHookTx): Promise<void> {
  const admissions = Schema.startHookAdmissions;
  const cutoff = new Date(Date.now() - START_HOOK_CANDIDATE_MAX_AGE_MS - 60 * 60 * 1000);
  await tx.execute(
    // SKIP LOCKED: rows another admission is already deleting are left to it,
    // so admissions of unrelated tokens never wait on each other here.
    sql`delete from ${admissions} where (${admissions.tenantId}, ${admissions.runId}) in (select ${admissions.tenantId}, ${admissions.runId} from ${admissions} where ${admissions.createdAt} < ${cutoff.toISOString()}::timestamptz limit 100 for update skip locked)`,
  );
}

/**
 * A start Hook admission breadcrumb, under `DEBUG=workflow:*`. Written inside
 * the deciding transaction, so a rolled-back attempt can log too. Names runs,
 * never the token: a token is the caller's idempotency key and may carry user
 * data.
 */
function startHookLog(message: string, detail: Record<string, unknown>): void {
  debugLog(`[workflow-world] start Hook: ${message}`, detail);
}
