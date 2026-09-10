import { ensureTenantPartitions } from "@evelandhq/workflow-world";
import { Pool } from "pg";

const connectionString = process.env.WORKFLOW_WORLD_URL;
const tenantId = process.env.WORKFLOW_WORLD_TENANT_ID;
if (!connectionString || !tenantId) {
  throw new Error("WORKFLOW_WORLD_URL and WORKFLOW_WORLD_TENANT_ID are required");
}
const pool = new Pool({ connectionString });
try {
  await ensureTenantPartitions(pool, tenantId);
  console.log(`Tenant ${tenantId} is ready.`);
} finally {
  await pool.end();
}
