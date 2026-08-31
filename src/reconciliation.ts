import type { Pool, PoolClient } from "pg";
import { runQueueName } from "./dispatch-contract.js";
import { MessageData } from "./message.js";
import { assertValidTenantId } from "./tenant.js";

/**
 * Host-driven run reconciliation.
 *
 * Run status is otherwise written only from inside the World, by the workflow's
 * own lifecycle events. That is the right default — the workflow owns its
 * outcome — but it leaves one party with knowledge and no pen: the host
 * platform, which supervises the agent processes, knows when a run's executor
 * is gone for good (idle-reaped, crashed, deployment retired). Without a write
 * path those runs stay `running` forever, and boot recovery replays them on
 * every dispatcher start.
 *
 * This is that write path. It deliberately does NOT append `run_failed` events
 * or stream EOFs on the workflow's behalf — the same line the dead-letter
 * design draws: a host verdict is not a workflow-authored outcome, and forging
 * the workflow's own event log would erase the distinction. It writes the run
 * row the way the World's terminal transitions do (conditional on the run still
 * being active, hooks and waits cleaned up, retention deadlines recomputed by
 * the database trigger), or quarantines the run behind an unresolved dead
 * letter without touching its status.
 */

export type WorkflowRunReconciliationDisposition = "fail" | "cancel" | "quarantine";

export type ReconcileWorkflowRunsOptions = {
  tenantId: string;
  /**
   * Which of the tenant's active runs to settle. At least one selector is
   * required, and they narrow each other when both are given. There is no
   * "everything" form on purpose: the callers this exists for always know the
   * dead executor's identity, and an unscoped settle is the kind of mistake
   * tenant scoping exists to make unrepresentable.
   */
  runIds?: string[];
  deploymentIds?: string[];
  disposition: WorkflowRunReconciliationDisposition;
  /** Why the host settled the run. Recorded durably on the row it lands in. */
  reason: string;
  /** Machine-readable code for `fail`; ignored otherwise. */
  errorCode?: string;
};

export type ReconciledWorkflowRun = {
  runId: string;
  workflowName: string;
  deploymentId: string;
  previousStatus: "pending" | "running";
};

export type ReconcileWorkflowRunsResult = {
  disposition: WorkflowRunReconciliationDisposition;
  reconciled: ReconciledWorkflowRun[];
};

const DEFAULT_ERROR_CODE = "RUN_ORPHANED";

export async function reconcileWorkflowRuns(
  pool: Pool,
  options: ReconcileWorkflowRunsOptions,
): Promise<ReconcileWorkflowRunsResult> {
  assertValidTenantId(options.tenantId);
  if (options.runIds === undefined && options.deploymentIds === undefined) {
    throw new TypeError(
      "reconcileWorkflowRuns requires runIds and/or deploymentIds: refusing to settle a tenant's entire active set implicitly.",
    );
  }
  if (!options.reason) {
    throw new TypeError("reconcileWorkflowRuns requires a reason.");
  }
  // An explicitly empty selector is an empty batch, not an error — the callers
  // are reconciliation sweeps, and "this dead instance held no runs" is their
  // ordinary case.
  if (options.runIds?.length === 0 && options.deploymentIds === undefined) {
    return { disposition: options.disposition, reconciled: [] };
  }
  if (options.deploymentIds?.length === 0 && options.runIds === undefined) {
    return { disposition: options.disposition, reconciled: [] };
  }

  const selector: string[] = [];
  const params: unknown[] = [options.tenantId];
  if (options.runIds !== undefined) {
    params.push(options.runIds);
    selector.push(`runs.id = any($${String(params.length)})`);
  }
  if (options.deploymentIds !== undefined) {
    params.push(options.deploymentIds);
    selector.push(`runs.deployment_id = any($${String(params.length)})`);
  }

  const client = await pool.connect();
  try {
    await client.query("begin");
    const reconciled =
      options.disposition === "quarantine"
        ? await quarantineRuns(client, options, selector, params)
        : await settleRuns(client, options, selector, params);
    await client.query("commit");
    return { disposition: options.disposition, reconciled };
  } catch (error) {
    await client.query("rollback").catch(() => {});
    throw error;
  } finally {
    client.release();
  }
}

/**
 * The terminal dispositions, written the way the World writes `run_failed` /
 * `run_cancelled`: conditional on the run still being active (the `for update`
 * subquery re-evaluates its predicate on lock, so a run that reaches a terminal
 * state concurrently drops out rather than being overwritten), hooks deleted
 * except retained tokens, waits deleted. `completed_at` flips the retention
 * trigger, which recomputes the row's compaction and expiry deadlines exactly
 * as it would for a workflow-authored terminal transition.
 */
async function settleRuns(
  client: PoolClient,
  options: ReconcileWorkflowRunsOptions,
  selector: string[],
  params: unknown[],
): Promise<ReconciledWorkflowRun[]> {
  const status = options.disposition === "fail" ? "failed" : "cancelled";
  const errorParams = [...params];
  let errorClause = "";
  if (options.disposition === "fail") {
    // `error_cbor` stays NULL deliberately: it holds runtime-serialized data
    // the workflow's own pipeline produced, and this process cannot mint that
    // honestly. `error_code` is the machine-readable verdict; the deprecated
    // text column gets a StructuredError-shaped breadcrumb so an operator
    // reading the row sees why the host failed it (the current read path
    // ignores it, and legacy readers parse exactly this shape).
    errorParams.push(options.errorCode ?? DEFAULT_ERROR_CODE);
    const codeParam = errorParams.length;
    errorParams.push(
      JSON.stringify({
        name: "WorkflowRunReconciled",
        message: options.reason,
        code: options.errorCode ?? DEFAULT_ERROR_CODE,
      }),
    );
    errorClause = `, error_code = $${String(codeParam)}, error = $${String(errorParams.length)}`;
  }

  const settled = await client.query<{
    id: string;
    name: string;
    deployment_id: string;
    previous_status: "pending" | "running";
  }>(
    `update workflow.workflow_runs as runs
        set status = '${status}', completed_at = now(), updated_at = now()${errorClause}
       from (select tenant_id, id, status as previous_status
               from workflow.workflow_runs as runs
              where runs.tenant_id = $1
                and runs.status in ('pending', 'running')
                and ${selector.join(" and ")}
              for update) as active
      where runs.tenant_id = active.tenant_id
        and runs.id = active.id
      returning runs.id, runs.name, runs.deployment_id, active.previous_status`,
    errorParams,
  );
  if (settled.rows.length > 0) {
    const runIds = settled.rows.map((row) => row.id);
    // Same cleanup as the World's terminal transitions. A retained hook token
    // outlives its run on purpose — that is the whole point of
    // `token_retention_until` — so only lapsed-or-never-retained hooks go.
    await client.query(
      `delete from workflow.workflow_hooks
        where tenant_id = $1
          and run_id = any($2)
          and (token_retention_until is null or token_retention_until <= now())`,
      [options.tenantId, runIds],
    );
    await client.query(
      `delete from workflow.workflow_waits
        where tenant_id = $1 and run_id = any($2)`,
      [options.tenantId, runIds],
    );
  }
  return settled.rows.map((row) => ({
    runId: row.id,
    workflowName: row.name,
    deploymentId: row.deployment_id,
    previousStatus: row.previous_status,
  }));
}

/**
 * Quarantine leaves the run `pending`/`running` and parks it behind an
 * unresolved dead letter — the dispatcher's existing quarantine state, which
 * boot recovery and the platform's deployment-retention queries already
 * exclude. Resolving the row is the explicit replay boundary, exactly as it is
 * for a delivery that dead-lettered on its own.
 *
 * The payload is the same message boot recovery would have reconstructed, so
 * an operator replay of a host-quarantined run and of a retry-exhausted one is
 * one procedure, not two.
 */
async function quarantineRuns(
  client: PoolClient,
  options: ReconcileWorkflowRunsOptions,
  selector: string[],
  params: unknown[],
): Promise<ReconciledWorkflowRun[]> {
  const active = await client.query<{
    id: string;
    name: string;
    deployment_id: string;
    queue_namespace: string | null;
    status: "pending" | "running";
  }>(
    `select runs.id, runs.name, runs.deployment_id, runs.queue_namespace, runs.status
       from workflow.workflow_runs as runs
      where runs.tenant_id = $1
        and runs.status in ('pending', 'running')
        and ${selector.join(" and ")}
        -- Already quarantined is success, not a second row: one unresolved
        -- letter per run is what makes "resolve it" an unambiguous operator
        -- action.
        and not exists (
          select 1
            from workflow.dispatch_dead_letters as dead
           where dead.tenant_id = runs.tenant_id
             and dead.run_id = runs.id
             and dead.resolved_at is null
        )
      for update of runs`,
    params,
  );

  const reconciled: ReconciledWorkflowRun[] = [];
  for (const row of active.rows) {
    const messageId = `msg_reconcile_${row.id}`;
    const message: MessageData = {
      id: row.name,
      data: Buffer.from(JSON.stringify({ runId: row.id })),
      attempt: 1,
      messageId: messageId as MessageData["messageId"],
      tenantId: options.tenantId,
      deploymentId: row.deployment_id,
      // Absent, not empty, when there is no namespace — the wire shape the live
      // enqueue path produces. See `dispatcher/boot-recovery.ts`.
      ...(row.queue_namespace ? { queueNamespace: row.queue_namespace } : {}),
    };
    await client.query(
      `insert into workflow.dispatch_dead_letters
         (tenant_id, deployment_id, run_id, message_id, job_name, queue_name, attempt, reason, payload)
       values ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
      [
        options.tenantId,
        row.deployment_id,
        row.id,
        messageId,
        "host_reconciliation",
        runQueueName(options.tenantId, row.id),
        0,
        options.reason,
        JSON.stringify(MessageData.encode(message)),
      ],
    );
    reconciled.push({
      runId: row.id,
      workflowName: row.name,
      deploymentId: row.deployment_id,
      previousStatus: row.status,
    });
  }
  return reconciled;
}
