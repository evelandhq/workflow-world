import { makeWorkerUtils, type WorkerUtils } from "graphile-worker";
import { Pool } from "pg";
import { afterAll, beforeAll, describe, expect, test } from "vitest";
import { reenqueueActiveRunsForAllTenants } from "./dispatcher/boot-recovery.js";
import { createWorld, ensureTenantPartitions, runMigrations } from "./index.js";
import { MessageData } from "./message.js";
import { dropTenantPartitions } from "./migrate.js";
import { reconcileWorkflowRuns } from "./reconciliation.js";

/**
 * The host-driven reconciliation write path.
 *
 * These runs are created through the real event path, exactly as a deployment
 * creates them — seeding rows directly would let the tests pass while the
 * production write path produced rows the reconciler's guards don't match.
 *
 * Set `EVELAND_WORKFLOW_WORLD_TEST_URL` to a scratch database to run them.
 */
const testUrl = process.env.EVELAND_WORKFLOW_WORLD_TEST_URL;
const suffix = `${String(process.pid)}${Date.now().toString(36)}`;
const FAILED = `p_recon_a_${suffix}`;
const CANCELLED = `p_recon_b_${suffix}`;
const QUARANTINED = `p_recon_c_${suffix}`;
const GUARDED = `p_recon_d_${suffix}`;
const TENANTS = [FAILED, CANCELLED, QUARANTINED, GUARDED];

describe.skipIf(!testUrl)("host-driven run reconciliation", () => {
  let admin: Pool;
  let workerUtils: WorkerUtils;
  const worlds: Array<ReturnType<typeof createWorld>> = [];

  beforeAll(async () => {
    admin = new Pool({ connectionString: testUrl, max: 4 });
    await runMigrations(admin);
    for (const tenant of TENANTS) {
      await ensureTenantPartitions(admin, tenant);
    }
    workerUtils = await makeWorkerUtils({ pgPool: admin });
    await workerUtils.migrate();
  }, 60_000);

  afterAll(async () => {
    await Promise.all(worlds.map(async (world) => await world.close?.()));
    await workerUtils?.release();
    await admin
      .query("delete from graphile_worker._private_jobs where payload->>'tenantId' = any($1)", [
        TENANTS,
      ])
      .catch(() => {});
    // The boot-recovery sweep used below is global and its keys are stable, so
    // leftover jobs would be claimed by whatever dispatcher a later file starts.
    await admin
      .query("delete from graphile_worker._private_jobs where payload->>'messageId' like $1", [
        "msg_recover_%",
      ])
      .catch(() => {});
    // `workflow_runs` is NOT partitioned; without this the still-active rows
    // would be re-enqueued by a later file's boot recovery.
    await admin
      .query("delete from workflow.workflow_runs where tenant_id = any($1)", [TENANTS])
      .catch(() => {});
    await admin
      .query("delete from workflow.dispatch_dead_letters where tenant_id = any($1)", [TENANTS])
      .catch(() => {});
    for (const tenant of TENANTS) {
      await dropTenantPartitions(admin, tenant).catch(() => {});
    }
    await admin?.end().catch(() => {});
  });

  async function createActiveRun(input: {
    tenantId: string;
    queueNamespace?: string;
    workflowName?: string;
    deploymentId?: string;
  }): Promise<string> {
    const deploymentId = input.deploymentId ?? `dep_${input.tenantId}`;
    const world = createWorld({
      connectionString: testUrl!,
      tenantId: input.tenantId,
      deploymentId,
      // `external` so no in-process runner claims anything while we look.
      runner: "external",
      ...(input.queueNamespace !== undefined ? { queueNamespace: input.queueNamespace } : {}),
    });
    worlds.push(world);
    const created = await world.events.create(null, {
      eventType: "run_created",
      eventData: {
        deploymentId,
        workflowName: input.workflowName ?? "greet",
        input: [],
      },
      specVersion: 5,
    });
    return created.run!.runId;
  }

  async function runRow(tenantId: string, runId: string) {
    const { rows } = await admin.query<{
      status: string;
      error_code: string | null;
      error: string | null;
      completed_at: Date | null;
      expire_after: Date | null;
    }>(
      `select status, error_code, error, completed_at, expire_after
         from workflow.workflow_runs where tenant_id = $1 and id = $2`,
      [tenantId, runId],
    );
    expect(rows).toHaveLength(1);
    return rows[0]!;
  }

  async function recoveredJobCount(runId: string): Promise<number> {
    const { rows } = await admin.query(
      "select 1 from graphile_worker._private_jobs where payload->>'messageId' = $1",
      [`msg_recover_${runId}`],
    );
    return rows.length;
  }

  test("fail settles an orphaned run the way the World fails one", async () => {
    const runId = await createActiveRun({ tenantId: FAILED });
    // One hook whose retention has lapsed (delete with the run) and one whose
    // token is retained (must survive it), plus a pending wait.
    await admin.query(
      `insert into workflow.workflow_hooks
         (tenant_id, run_id, hook_id, token, owner_id, project_id, environment, token_retention_until)
       values ($1, $2, 'hook_gone', 'tok_gone', 'owner', '', 'production', null),
              ($1, $2, 'hook_kept', 'tok_kept', 'owner', '', 'production', now() + interval '1 hour')`,
      [FAILED, runId],
    );
    await admin.query(
      `insert into workflow.workflow_waits (tenant_id, wait_id, run_id, status)
       values ($1, 'wait_1', $2, 'waiting')`,
      [FAILED, runId],
    );

    const result = await reconcileWorkflowRuns(admin, {
      tenantId: FAILED,
      deploymentIds: [`dep_${FAILED}`],
      disposition: "fail",
      reason: "RuntimeInstance reaped while the run was active",
    });
    expect(result.reconciled).toEqual([
      {
        runId,
        workflowName: "greet",
        deploymentId: `dep_${FAILED}`,
        previousStatus: "pending",
      },
    ]);

    const row = await runRow(FAILED, runId);
    expect(row.status).toBe("failed");
    expect(row.error_code).toBe("RUN_ORPHANED");
    expect(JSON.parse(row.error!)).toMatchObject({
      name: "WorkflowRunReconciled",
      message: "RuntimeInstance reaped while the run was active",
      code: "RUN_ORPHANED",
    });
    expect(row.completed_at).not.toBeNull();
    // The retention trigger fired: a terminal interactive run has deadlines.
    expect(row.expire_after).not.toBeNull();

    const hooks = await admin.query<{ hook_id: string }>(
      "select hook_id from workflow.workflow_hooks where tenant_id = $1 and run_id = $2",
      [FAILED, runId],
    );
    expect(hooks.rows.map((hook) => hook.hook_id)).toEqual(["hook_kept"]);
    const waits = await admin.query(
      "select 1 from workflow.workflow_waits where tenant_id = $1 and run_id = $2",
      [FAILED, runId],
    );
    expect(waits.rows).toHaveLength(0);

    // Settled means settled: the next boot has nothing left to replay.
    await reenqueueActiveRunsForAllTenants({ pool: admin, workerUtils });
    expect(await recoveredJobCount(runId)).toBe(0);
  }, 60_000);

  test("cancel is terminal without inventing an error", async () => {
    const runId = await createActiveRun({ tenantId: CANCELLED });

    const result = await reconcileWorkflowRuns(admin, {
      tenantId: CANCELLED,
      runIds: [runId],
      disposition: "cancel",
      reason: "operator abandoned the run",
    });
    expect(result.reconciled).toHaveLength(1);

    const row = await runRow(CANCELLED, runId);
    expect(row.status).toBe("cancelled");
    expect(row.error_code).toBeNull();
    expect(row.error).toBeNull();
    expect(row.completed_at).not.toBeNull();
  }, 60_000);

  test("quarantine parks the run behind an unresolved dead letter, replayably", async () => {
    const runId = await createActiveRun({ tenantId: QUARANTINED, queueNamespace: "acme" });

    const result = await reconcileWorkflowRuns(admin, {
      tenantId: QUARANTINED,
      runIds: [runId],
      disposition: "quarantine",
      reason: "deployment not activatable",
    });
    expect(result.reconciled).toHaveLength(1);

    // The run itself is untouched; the dead letter is what holds it back.
    expect((await runRow(QUARANTINED, runId)).status).toBe("pending");
    const letters = await admin.query<{
      reason: string;
      job_name: string;
      resolved_at: Date | null;
      payload: unknown;
    }>(
      "select reason, job_name, resolved_at, payload from workflow.dispatch_dead_letters where tenant_id = $1 and run_id = $2",
      [QUARANTINED, runId],
    );
    expect(letters.rows).toHaveLength(1);
    expect(letters.rows[0]).toMatchObject({
      reason: "deployment not activatable",
      job_name: "host_reconciliation",
      resolved_at: null,
    });
    // The payload is the message boot recovery would have built, namespace and
    // all, so operator replay of a host quarantine is the standard procedure.
    const message = MessageData.parse(letters.rows[0]!.payload);
    expect(message).toMatchObject({ id: "greet", queueNamespace: "acme" });
    expect(JSON.parse(message.data.toString())).toEqual({ runId });

    await reenqueueActiveRunsForAllTenants({ pool: admin, workerUtils });
    expect(await recoveredJobCount(runId)).toBe(0);

    // Idempotent: an already-quarantined run is success, not a second letter.
    const again = await reconcileWorkflowRuns(admin, {
      tenantId: QUARANTINED,
      runIds: [runId],
      disposition: "quarantine",
      reason: "deployment not activatable",
    });
    expect(again.reconciled).toHaveLength(0);
    const recount = await admin.query(
      "select 1 from workflow.dispatch_dead_letters where tenant_id = $1 and run_id = $2",
      [QUARANTINED, runId],
    );
    expect(recount.rows).toHaveLength(1);

    // Resolution is the replay boundary, exactly as for organic dead letters.
    await admin.query(
      "update workflow.dispatch_dead_letters set resolved_at = now() where tenant_id = $1 and run_id = $2",
      [QUARANTINED, runId],
    );
    await reenqueueActiveRunsForAllTenants({ pool: admin, workerUtils });
    expect(await recoveredJobCount(runId)).toBe(1);
  }, 60_000);

  test("only selected, still-active runs are touched", async () => {
    const target = await createActiveRun({ tenantId: GUARDED, deploymentId: "dep_dead" });
    const otherDeployment = await createActiveRun({ tenantId: GUARDED, deploymentId: "dep_live" });
    const terminal = await createActiveRun({ tenantId: GUARDED, deploymentId: "dep_dead" });
    await admin.query(
      "update workflow.workflow_runs set status = 'completed', completed_at = now() where tenant_id = $1 and id = $2",
      [GUARDED, terminal],
    );

    const result = await reconcileWorkflowRuns(admin, {
      tenantId: GUARDED,
      deploymentIds: ["dep_dead"],
      disposition: "fail",
      reason: "instance reaped",
      errorCode: "INSTANCE_REAPED",
    });
    expect(result.reconciled.map((run) => run.runId)).toEqual([target]);
    expect((await runRow(GUARDED, target)).error_code).toBe("INSTANCE_REAPED");
    expect((await runRow(GUARDED, otherDeployment)).status).toBe("pending");
    // The terminal guard held: the completed run kept its outcome.
    const completed = await runRow(GUARDED, terminal);
    expect(completed.status).toBe("completed");
    expect(completed.error_code).toBeNull();
  }, 60_000);

  test("an unscoped settle is refused; an empty batch is a no-op", async () => {
    await expect(
      reconcileWorkflowRuns(admin, {
        tenantId: GUARDED,
        disposition: "fail",
        reason: "everything",
      }),
    ).rejects.toThrow(/runIds and\/or deploymentIds/);
    await expect(
      reconcileWorkflowRuns(admin, {
        tenantId: GUARDED,
        runIds: ["wrun_x"],
        disposition: "fail",
        reason: "",
      }),
    ).rejects.toThrow(/reason/);
    const empty = await reconcileWorkflowRuns(admin, {
      tenantId: GUARDED,
      runIds: [],
      disposition: "fail",
      reason: "empty batch",
    });
    expect(empty.reconciled).toEqual([]);
  }, 60_000);
});
