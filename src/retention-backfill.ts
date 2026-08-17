import type { Pool, PoolClient } from "pg";
import { resolveRunRetentionClass, type RunRetentionClass } from "./run-retention-policy.js";
import { assertValidTenantId } from "./tenant.js";

export type WorkflowRunRetentionBackfillSelector = {
  /** Tenant whose run graph may be inspected or changed. */
  tenantId: string;
  /** Root-run JSON attribute that proves the graph's origin. */
  rootAttribute: string;
  /** Exact value required for `rootAttribute`. */
  rootValue: string;
  /** Class assigned to eligible graph members. */
  retentionClass: RunRetentionClass | "ephemeral";
};

export type WorkflowRunRetentionBackfillOptions = WorkflowRunRetentionBackfillSelector & {
  /** Maximum runs changed by one transaction. */
  batchSize: number;
};

export type WorkflowRunRetentionBackfillGroup = {
  tenantId: string;
  rootTrigger: string | null;
  runType: string | null;
  workflowName: string;
  status: string;
  retentionClass: RunRetentionClass;
  runs: number;
};

export type WorkflowRunRetentionBackfillPreview = {
  matchedRoots: number;
  eligibleRuns: number;
  excludedPersistentRuns: number;
  groups: WorkflowRunRetentionBackfillGroup[];
};

export type WorkflowRunRetentionBackfillResult = {
  updatedRuns: number;
  remainingRuns: number;
  hitBatchLimit: boolean;
};

export type WorkflowRunRetentionMismatchOptions = WorkflowRunRetentionBackfillSelector & {
  /** Maximum mismatches returned. */
  limit: number;
};

export type WorkflowRunRetentionMismatch = {
  kind: "root-class" | "child-root-class";
  runId: string;
  rootRunId: string;
  workflowName: string;
  status: string;
  retentionClass: RunRetentionClass;
  rootRetentionClass: RunRetentionClass;
};

export type WorkflowRunRetentionMismatchResult = {
  mismatches: WorkflowRunRetentionMismatch[];
  hitLimit: boolean;
};

type NormalizedSelector = Omit<WorkflowRunRetentionBackfillSelector, "retentionClass"> & {
  retentionClass: RunRetentionClass;
};

type CountRow = {
  matched_roots: string;
  eligible_runs: string;
  excluded_persistent_runs: string;
};

const GRAPH_CTE = `
  roots as (
    select runs.tenant_id, runs.id, runs.id as root_id
      from workflow.workflow_runs as runs
     where runs.tenant_id = $1
       and runs.attributes ->> $2 = $3
  ),
  graph as (
    select roots.tenant_id, roots.id, roots.root_id
      from roots
    union
    select child.tenant_id, child.id, graph.root_id
      from workflow.workflow_runs as child
      join graph
        on child.tenant_id = graph.tenant_id
       and (
         child.attributes ->> '$parentRunId' = graph.id
         or child.attributes ->> '$eve.parent' = graph.id
         or child.attributes ->> '$rootRunId' = graph.root_id
         or child.attributes ->> '$eve.root' = graph.root_id
       )
  )`;

/**
 * Inspect a provable run graph before changing it. This is intentionally
 * selector-driven: callers must supply an exact root marker instead of asking
 * this package to infer product-specific workflow names.
 */
export async function previewWorkflowRunRetentionBackfill(
  pool: Pool,
  selector: WorkflowRunRetentionBackfillSelector,
): Promise<WorkflowRunRetentionBackfillPreview> {
  const normalized = normalizeSelector(selector);
  const parameters = selectorParameters(normalized);
  const [counts, groups] = await Promise.all([
    pool.query<CountRow>(
      `with recursive ${GRAPH_CTE}
       select (select count(*) from roots)::text as matched_roots,
              count(*) filter (
                where runs.retention_class <> $4
                  and runs.retention_class <> 'persistent'
              )::text as eligible_runs,
              count(*) filter (where runs.retention_class = 'persistent')::text
                as excluded_persistent_runs
         from graph
         join workflow.workflow_runs as runs
           on runs.tenant_id = graph.tenant_id
          and runs.id = graph.id`,
      parameters,
    ),
    pool.query<{
      tenant_id: string;
      root_trigger: string | null;
      run_type: string | null;
      workflow_name: string;
      status: string;
      retention_class: RunRetentionClass;
      runs: string;
    }>(
      `with recursive ${GRAPH_CTE}
       select runs.tenant_id,
              root_runs.attributes ->> '$eve.trigger' as root_trigger,
              runs.attributes ->> '$eve.type' as run_type,
              runs.name as workflow_name,
              runs.status::text as status,
              runs.retention_class,
              count(*)::text as runs
         from graph
         join workflow.workflow_runs as runs
           on runs.tenant_id = graph.tenant_id
          and runs.id = graph.id
         join workflow.workflow_runs as root_runs
           on root_runs.tenant_id = graph.tenant_id
          and root_runs.id = graph.root_id
        group by runs.tenant_id, root_trigger, run_type,
                 runs.name, runs.status, runs.retention_class
        order by runs.tenant_id, root_trigger, run_type,
                 runs.name, runs.status, runs.retention_class`,
      parameters.slice(0, 3),
    ),
  ]);
  const count = counts.rows[0]!;
  return {
    matchedRoots: Number(count.matched_roots),
    eligibleRuns: Number(count.eligible_runs),
    excludedPersistentRuns: Number(count.excluded_persistent_runs),
    groups: groups.rows.map((row) => ({
      tenantId: row.tenant_id,
      rootTrigger: row.root_trigger,
      runType: row.run_type,
      workflowName: row.workflow_name,
      status: row.status,
      retentionClass: row.retention_class,
      runs: Number(row.runs),
    })),
  };
}

/**
 * Report scheduler roots that do not have the selected class and descendants
 * whose class differs from their resolved root. Explicit persistent rows are
 * excluded because persistence is an intentional product override, not an
 * inference this diagnostic is permitted to undo.
 */
export async function inspectWorkflowRunRetentionMismatches(
  pool: Pool,
  options: WorkflowRunRetentionMismatchOptions,
): Promise<WorkflowRunRetentionMismatchResult> {
  const normalized = normalizeSelector(options);
  assertPositiveInteger(options.limit, "limit");
  const { rows } = await pool.query<{
    kind: "root-class" | "child-root-class";
    run_id: string;
    root_run_id: string;
    workflow_name: string;
    status: string;
    retention_class: RunRetentionClass;
    root_retention_class: RunRetentionClass;
  }>(
    `with recursive ${GRAPH_CTE},
     mismatches as (
       select case
                when graph.id = graph.root_id then 'root-class'
                else 'child-root-class'
              end as kind,
              runs.id as run_id,
              graph.root_id as root_run_id,
              runs.name as workflow_name,
              runs.status::text as status,
              runs.retention_class,
              root_runs.retention_class as root_retention_class
         from graph
         join workflow.workflow_runs as runs
           on runs.tenant_id = graph.tenant_id
          and runs.id = graph.id
         join workflow.workflow_runs as root_runs
           on root_runs.tenant_id = graph.tenant_id
          and root_runs.id = graph.root_id
        where runs.retention_class <> 'persistent'
          and (
            (graph.id = graph.root_id and runs.retention_class <> $4)
            or (
              graph.id <> graph.root_id
              and runs.retention_class <> root_runs.retention_class
            )
          )
     )
     select *
       from mismatches
      order by case kind when 'root-class' then 0 else 1 end,
               root_run_id,
               run_id
      limit $5`,
    [...selectorParameters(normalized), options.limit + 1],
  );
  const hitLimit = rows.length > options.limit;
  return {
    mismatches: rows.slice(0, options.limit).map((row) => ({
      kind: row.kind,
      runId: row.run_id,
      rootRunId: row.root_run_id,
      workflowName: row.workflow_name,
      status: row.status,
      retentionClass: row.retention_class,
      rootRetentionClass: row.root_retention_class,
    })),
    hitLimit,
  };
}

/**
 * Change one bounded batch of a provable graph. Active rows are selected first
 * so a rollout converges their future children before spending the batch on
 * terminal history. The existing database trigger recomputes all deadlines in
 * the same update.
 */
export async function backfillWorkflowRunRetentionClass(
  pool: Pool,
  options: WorkflowRunRetentionBackfillOptions,
): Promise<WorkflowRunRetentionBackfillResult> {
  const normalized = normalizeSelector(options);
  assertPositiveInteger(options.batchSize, "batchSize");
  const parameters = [...selectorParameters(normalized), options.batchSize];
  const client = await pool.connect();
  try {
    await client.query("begin");
    try {
      const updated = await client.query(
        `with recursive ${GRAPH_CTE},
         victims as (
           select runs.tenant_id, runs.id
             from graph
             join workflow.workflow_runs as runs
               on runs.tenant_id = graph.tenant_id
              and runs.id = graph.id
            where runs.retention_class <> $4
              and runs.retention_class <> 'persistent'
            order by case when runs.status in ('pending', 'running') then 0 else 1 end,
                     runs.created_at,
                     runs.id
            limit $5
            for update of runs skip locked
         )
         update workflow.workflow_runs as runs
            set retention_class = $4
           from victims
          where runs.tenant_id = victims.tenant_id
            and runs.id = victims.id
         returning runs.id`,
        parameters,
      );
      const remainingRuns = await countRemaining(client, normalized);
      await client.query("commit");
      return {
        updatedRuns: updated.rows.length,
        remainingRuns,
        hitBatchLimit: remainingRuns > 0,
      };
    } catch (error) {
      await client.query("rollback").catch(() => {});
      throw error;
    }
  } finally {
    client.release();
  }
}

async function countRemaining(client: PoolClient, selector: NormalizedSelector): Promise<number> {
  const { rows } = await client.query<{ runs: string }>(
    `with recursive ${GRAPH_CTE}
     select count(*)::text as runs
       from graph
       join workflow.workflow_runs as runs
         on runs.tenant_id = graph.tenant_id
        and runs.id = graph.id
      where runs.retention_class <> $4
        and runs.retention_class <> 'persistent'`,
    selectorParameters(selector),
  );
  return Number(rows[0]!.runs);
}

function normalizeSelector(selector: WorkflowRunRetentionBackfillSelector): NormalizedSelector {
  assertValidTenantId(selector.tenantId);
  assertNonEmptyString(selector.rootAttribute, "rootAttribute");
  assertNonEmptyString(selector.rootValue, "rootValue");
  return {
    ...selector,
    retentionClass: resolveRunRetentionClass(selector.retentionClass),
  };
}

function selectorParameters(selector: NormalizedSelector): [string, string, string, string] {
  return [selector.tenantId, selector.rootAttribute, selector.rootValue, selector.retentionClass];
}

function assertNonEmptyString(value: string, name: string): void {
  if (value.trim().length === 0) throw new TypeError(`${name} must not be empty.`);
}

function assertPositiveInteger(value: number, name: string): void {
  if (!Number.isInteger(value) || value <= 0) {
    throw new TypeError(`${name} must be a positive integer.`);
  }
}
