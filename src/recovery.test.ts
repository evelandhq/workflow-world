/**
 * Ported from `@workflow/world-postgres`'s `reenqueue.test.ts`.
 *
 * Upstream's file mixes two subjects: the boot sweep itself, and `createWorld`
 * plumbing (connection-string fallbacks, shutdown-signal handling, close
 * ordering, pool ownership) that happens to live in the same suite. Only the
 * sweep is ported here; see the port report for the createWorld half, which
 * either does not exist in the fork or is already covered by
 * `env-contract.test.ts` and `world.integration.test.ts`.
 *
 * The last two tests are ones upstream cannot have. `reenqueueActiveRuns` there
 * lists runs with no tenant predicate, which was merely wasteful when every
 * project had its own database and is a correctness bug on the shared one: any
 * agent's boot would re-enqueue every project's active runs. Making the scoping
 * structural — the caller passes an already tenant-scoped `runs` — is the whole
 * reason this module exists, so it is pinned against a real database.
 */
import type { Queue, QueuePayload, Storage } from "@workflow/world";
import { Pool } from "pg";
import { afterAll, afterEach, beforeAll, describe, expect, test, vi } from "vitest";
import { FLOW_JOB_NAME, runQueueName } from "./dispatch-contract.js";
import { createWorld } from "./index.js";
import {
  dropTenantPartitions,
  ensureTenantPartitions,
  resolveMigrationsDir,
  runMigrations,
} from "./migrate.js";
import { reenqueueTenantRuns } from "./recovery.js";

type ActiveRun = { runId: string; workflowName: string };
type ActiveStatus = "pending" | "running";

describe("reenqueueTenantRuns", () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  test("re-enqueues every pending and running run", async () => {
    const { runs, list } = fakeRuns({
      pending: [{ runId: "wrun_AAA", workflowName: "wfA" }],
      running: [{ runId: "wrun_BBB", workflowName: "wfB" }],
    });
    const enqueue = vi.fn<Queue["queue"]>(async () => ({ messageId: null }));

    const reenqueued = await reenqueueTenantRuns({ runs, enqueue, tenantId: "prj_port_extra_u" });

    expect(reenqueued).toBe(2);
    expect(enqueue).toHaveBeenCalledTimes(2);
    expect(enqueue).toHaveBeenCalledWith("__wkf_workflow_wfA", { runId: "wrun_AAA" });
    expect(enqueue).toHaveBeenCalledWith("__wkf_workflow_wfB", { runId: "wrun_BBB" });
    // The sweep needs ids and workflow names, nothing else. Hydrating every
    // active run's input and output on every boot would read the whole blob set
    // to build messages that carry none of it.
    expect(list).toHaveBeenCalledWith(expect.objectContaining({ resolveData: "none" }));
  });

  test("does not enqueue anything when there are no active runs", async () => {
    const { runs } = fakeRuns({});
    const enqueue = vi.fn<Queue["queue"]>(async () => ({ messageId: null }));

    await expect(
      reenqueueTenantRuns({ runs, enqueue, tenantId: "prj_port_extra_u" }),
    ).resolves.toBe(0);
    expect(enqueue).not.toHaveBeenCalled();
  });

  test("pages through all active runs", async () => {
    let callCount = 0;
    const list = vi.fn(async (params: any) => {
      callCount++;
      // First call for each status returns one run with hasMore=true, second
      // call returns empty.
      if (!params?.pagination?.cursor) {
        return {
          data: [
            {
              runId: `wrun_page1_${String(params?.status)}`,
              workflowName: "paginatedWf",
              status: params?.status,
            },
          ],
          hasMore: true,
          cursor: "next",
        };
      }
      return { data: [], hasMore: false, cursor: null };
    });
    const runs = { list, get: vi.fn() } as unknown as Storage["runs"];
    const enqueue = vi.fn<Queue["queue"]>(async () => ({ messageId: null }));

    await reenqueueTenantRuns({ runs, enqueue, tenantId: "prj_port_extra_u" });

    // 2 statuses x 2 pages each.
    expect(callCount).toBe(4);
    expect(enqueue).toHaveBeenCalledTimes(2);
  });

  test("one failed enqueue does not abandon the rest of the sweep", async () => {
    // A boot sweep is best-effort: the workflow handler replays the event log, so
    // a run that misses its wake-up is stuck until something else nudges it. One
    // rejected send must not cost every later run its recovery.
    vi.spyOn(console, "warn").mockImplementation(() => {});
    const { runs } = fakeRuns({
      pending: [
        { runId: "wrun_AAA", workflowName: "wfA" },
        { runId: "wrun_BBB", workflowName: "wfB" },
      ],
    });
    const enqueue = vi.fn<Queue["queue"]>(async (_queueName, message) => {
      if ("runId" in message && message.runId === "wrun_AAA") {
        throw new Error("queue unavailable");
      }
      return { messageId: null };
    });

    const reenqueued = await reenqueueTenantRuns({ runs, enqueue, tenantId: "prj_port_extra_u" });

    expect(reenqueued).toBe(1);
    expect(enqueue).toHaveBeenCalledWith("__wkf_workflow_wfB", { runId: "wrun_BBB" });
  });
});

const testUrl = process.env.EVELAND_WORKFLOW_WORLD_TEST_URL;
// Unique per run so the suite is repeatable against a database it has already
// used: leftover rows from an earlier run would make the isolation assertions
// pass or fail for the wrong reason.
const suffix = `${String(process.pid)}${Date.now().toString(36)}`;
const ALPHA = `prj_port_extra_a_${suffix}`;
const BETA = `prj_port_extra_b_${suffix}`;

describe.skipIf(!testUrl)("reenqueueTenantRuns on a shared database", () => {
  let admin: Pool;
  let alpha: ReturnType<typeof createWorld>;
  let beta: ReturnType<typeof createWorld>;
  let alphaRunId: string;
  let betaRunId: string;

  beforeAll(async () => {
    admin = new Pool({ connectionString: testUrl, max: 2 });
    await runMigrations(admin, { migrationsDir: resolveMigrationsDir() });
    await ensureTenantPartitions(admin, ALPHA);
    await ensureTenantPartitions(admin, BETA);

    // `external` so neither world registers a graphile runner: these tests read
    // the enqueued jobs, and a runner would claim and execute them mid-assertion.
    alpha = createWorld({
      connectionString: testUrl!,
      tenantId: ALPHA,
      deploymentId: "dep_alpha_1",
      runner: "external",
    });
    beta = createWorld({
      connectionString: testUrl!,
      tenantId: BETA,
      deploymentId: "dep_beta_1",
      runner: "external",
    });

    // `run_created` leaves the run `pending`, which is what the sweep looks for.
    alphaRunId = (
      await alpha.events.create(null, {
        eventType: "run_created",
        eventData: { deploymentId: "dep_alpha_1", workflowName: "alphaWf", input: [] },
        specVersion: 5,
      })
    ).run!.runId;
    betaRunId = (
      await beta.events.create(null, {
        eventType: "run_created",
        eventData: { deploymentId: "dep_beta_1", workflowName: "betaWf", input: [] },
        specVersion: 5,
      })
    ).run!.runId;
  }, 60_000);

  afterAll(async () => {
    await alpha?.close?.();
    await beta?.close?.();
    // Dropping the partitions does not reach graphile's tables, and a leftover
    // job would outlive the tenant it belongs to on a database other suites
    // share. It is inert (only a runner registered for this tenant's suffixed
    // job name could claim it, and nothing else uses these ids) but it would
    // accumulate on every run.
    await admin
      .query("delete from graphile_worker._private_jobs where payload->>'tenantId' = any($1)", [
        [ALPHA, BETA],
      ])
      .catch(() => {});
    await dropTenantPartitions(admin, ALPHA).catch(() => {});
    await dropTenantPartitions(admin, BETA).catch(() => {});
    await admin?.end().catch(() => {});
  });

  test("a tenant's sweep never re-enqueues another tenant's runs", async () => {
    const fromAlpha: Array<{ queueName: string; message: QueuePayload }> = [];
    const fromBeta: Array<{ queueName: string; message: QueuePayload }> = [];

    await reenqueueTenantRuns({
      runs: alpha.runs,
      enqueue: recordingEnqueue(fromAlpha),
      tenantId: ALPHA,
    });
    await reenqueueTenantRuns({
      runs: beta.runs,
      enqueue: recordingEnqueue(fromBeta),
      tenantId: BETA,
    });

    expect(fromAlpha).toEqual([
      { queueName: "__wkf_workflow_alphaWf", message: { runId: alphaRunId } },
    ]);
    expect(fromBeta).toEqual([
      { queueName: "__wkf_workflow_betaWf", message: { runId: betaRunId } },
    ]);
  });

  test("start() enqueues graphile jobs for its own tenant only", async () => {
    await beta.start();

    // The public `graphile_worker.jobs` view omits `payload`, and the payload is
    // the only place the tenant is recorded, so this reads the private table.
    const { rows } = await admin.query<{
      task_identifier: string;
      queue_name: string | null;
      payload: { tenantId: string; deploymentId: string };
    }>(
      `select t.identifier as task_identifier, q.queue_name, j.payload
         from graphile_worker._private_jobs j
         join graphile_worker._private_tasks t on t.id = j.task_id
         left join graphile_worker._private_job_queues q on q.id = j.job_queue_id
        where j.payload->>'tenantId' = any($1)`,
      [[ALPHA, BETA]],
    );

    expect(rows.map((row) => row.payload.tenantId)).toEqual([BETA]);
    // External mode shares one job name across tenants — the dispatcher claims
    // across all of them on purpose — so the per-run queue name is the only
    // thing serializing deliveries for a run.
    expect(rows[0]?.task_identifier).toBe(FLOW_JOB_NAME);
    expect(rows[0]?.queue_name).toBe(runQueueName(BETA, betaRunId));
    expect(rows[0]?.payload.deploymentId).toBe("dep_beta_1");
  }, 60_000);
});

function fakeRuns(runsByStatus: Partial<Record<ActiveStatus, ActiveRun[]>>) {
  const list = vi.fn(async (params: any) => ({
    data: (runsByStatus[params?.status as ActiveStatus] ?? []).map((run) => ({
      ...run,
      status: params?.status,
    })),
    hasMore: false,
    cursor: null,
  }));
  return { runs: { list, get: vi.fn() } as unknown as Storage["runs"], list };
}

function recordingEnqueue(
  recorded: Array<{ queueName: string; message: QueuePayload }>,
): Queue["queue"] {
  return async (queueName, message) => {
    recorded.push({ queueName, message });
    return { messageId: null };
  };
}
