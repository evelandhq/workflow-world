import { readFileSync } from "node:fs";
import { Pool } from "pg";
import { afterAll, beforeAll, describe, expect, test } from "vitest";
import { ensureTenantPartitions, runMigrations } from "../src/index.js";
import { dropTenantPartitions } from "../src/migrate.js";
import { waitForRequiredEventTypes } from "./event-types.mts";
import { ENABLED_EVE_VERSIONS } from "./eve-versions.mts";
import {
  buildAgent,
  databaseFor,
  deploymentFor,
  installedEveVersion,
  installedWorldVersion,
  packWorld,
  startAgent,
  startSession,
  tenantFor,
  type StartedAgent,
} from "./harness.mts";

/**
 * A REAL eve agent, built by `eve build`, running against this package as its
 * World, driven through eve's own session API.
 *
 * This is deliberately different from `conformance/`, which runs upstream's
 * `@workflow/world-testing` suite. That harness ships its own bundled runtime, so
 * it proves the World satisfies the spec but says nothing about any particular eve
 * release. This suite is the other half: it proves a released eve can resolve,
 * bundle and drive this World.
 *
 * ## What one turn actually exercises
 *
 * More than a hand-written workflow would. eve's own agent turn IS a durable
 * workflow — `eve info` reports exactly one, `workflow//eve//workflowEntry`,
 * compiled from eve's own execution module — and user-authored `"use workflow"`
 * functions are not part of eve's compile surface in this version. So driving a
 * turn is not a convenience, it is the way a real agent uses a World. One turn
 * produces three runs, steps, hooks, a wait, and a cancellation.
 *
 * ## Why no model credentials
 *
 * The turn's model call fails with an AI Gateway auth error, and that is fine: by
 * then eve has already written its runs, steps, hooks and waits through the World.
 * Requiring credentials would make the suite unrunnable in CI and would not add
 * one assertion about *this* package.
 */
const thisPackageVersion = (
  JSON.parse(readFileSync(new URL("../package.json", import.meta.url), "utf8")) as {
    version: string;
  }
).version;

const baseUrl = process.env.WORKFLOW_WORLD_E2E_URL;
const adminUrl = process.env.WORKFLOW_WORLD_E2E_ADMIN_URL ?? baseUrl;

describe.skipIf(!baseUrl)("real eve agent against @evelandhq/workflow-world", () => {
  let worldTarball: string;

  beforeAll(() => {
    worldTarball = packWorld();
  });

  for (const [index, entry] of ENABLED_EVE_VERSIONS.entries()) {
    describe(`eve ${entry.version}`, () => {
      const tenantId = tenantFor(entry.version);
      const deploymentId = deploymentFor(entry.version);
      const database = databaseFor(entry.version);
      const port = 41900 + index;

      let pool: Pool;
      let agent: StartedAgent;
      let dir: string;
      let databaseUrl: string;

      beforeAll(async () => {
        // Own database per version, so one version's failure cannot cascade.
        const admin = new Pool({ connectionString: adminUrl, max: 1 });
        try {
          await admin.query(`drop database if exists ${database}`);
          await admin.query(`create database ${database}`);
        } finally {
          await admin.end();
        }

        databaseUrl = new URL(baseUrl!).toString().replace(/\/[^/]*$/, `/${database}`);
        pool = new Pool({ connectionString: databaseUrl, max: 4 });
        await runMigrations(pool);
        await ensureTenantPartitions(pool, tenantId);

        dir = buildAgent({ eveVersion: entry.version, worldTarball });
        agent = await startAgent({ dir, port, databaseUrl, tenantId, deploymentId });
      });

      afterAll(async () => {
        await agent?.stop();
        await dropTenantPartitions(pool, tenantId).catch(() => {});
        await pool?.end().catch(() => {});
      });

      test("the version under test is the one actually installed", () => {
        // Guards the matrix against being theatre: without this, three entries
        // could silently resolve one eve and every assertion below would still
        // pass.
        expect(installedEveVersion(dir)).toBe(entry.version);
        // And the World must be this package's packed artifact, not a link.
        expect(installedWorldVersion(dir)).toBe(thisPackageVersion);
      });

      test("one agent turn writes durable runs into this World", async () => {
        const { sessionId } = await startSession(port);
        expect(sessionId).toMatch(/^wrun_/);

        // Poll: the turn's runs are written as it proceeds.
        let names: string[] = [];
        for (let attempt = 0; attempt < 60; attempt += 1) {
          const { rows } = await pool.query<{ name: string; status: string }>(
            `select name, status from workflow.workflow_runs where tenant_id = $1`,
            [tenantId],
          );
          names = rows.map((row) => row.name);
          if (names.length >= 3) break;
          await new Promise((resolve) => setTimeout(resolve, 500));
        }

        // eve's own workflows, by name. Asserting the names rather than a count
        // means a change in eve's turn shape is visible rather than silently
        // absorbed.
        expect(names).toContain("workflow//eve//workflowEntry");
        expect(names).toContain("workflow//eve//turnWorkflow");
      });

      test("the run rows carry this deployment's tenancy", async () => {
        // The reason this World exists: every row is scoped, and a shared
        // database must never hand a run to the wrong tenant or deployment.
        const { rows } = await pool.query<{ tenant_id: string; deployment_id: string }>(
          `select distinct tenant_id, deployment_id from workflow.workflow_runs`,
        );
        expect(rows).toEqual([{ tenant_id: tenantId, deployment_id: deploymentId }]);
      });

      test("the turn drives steps, hooks and waits through this World", async () => {
        await waitForRequiredEventTypes(async () => {
          const { rows } = await pool.query<{ type: string; count: string }>(
            `select type, count(*)::text as count
               from workflow.workflow_events
              where tenant_id = $1
              group by type`,
            [tenantId],
          );
          return rows;
        });
      });

      test("the queue was used, not bypassed", async () => {
        // graphile's schema only exists once something has been enqueued, so its
        // presence is itself the assertion.
        const { rows } = await pool.query<{ exists: boolean }>(
          `select exists (
             select 1 from information_schema.schemata where schema_name = 'graphile_worker'
           ) as exists`,
        );
        expect(rows[0]?.exists).toBe(true);
      });
    });
  }
});
