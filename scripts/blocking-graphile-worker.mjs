import { run } from "graphile-worker";
import { Pool } from "pg";

const [taskName] = process.argv.slice(2);
const databaseUrl = process.env.EVELAND_WORKFLOW_WORLD_TEST_URL;
if (!databaseUrl || !taskName) {
  throw new Error("EVELAND_WORKFLOW_WORLD_TEST_URL and a task-name argument are required");
}

const pool = new Pool({
  connectionString: databaseUrl,
  max: 3,
  application_name: `workflow-world-test-blocker-${process.pid}`,
});

process.stdout.write("wfw-blocker:ready\n");
await new Promise((resolve, reject) => {
  const cleanup = () => {
    process.stdin.off("data", onData);
    process.stdin.off("end", onEnd);
  };
  const onData = () => {
    cleanup();
    resolve();
  };
  const onEnd = () => {
    cleanup();
    reject(new Error("parent ended before starting worker"));
  };
  process.stdin.once("data", onData);
  process.stdin.once("end", onEnd);
});

await run({
  pgPool: pool,
  concurrency: 1,
  pollInterval: 50,
  noHandleSignals: true,
  taskList: {
    [taskName]: async () => {
      process.stdout.write("wfw-blocker:claimed\n");
      await new Promise(() => {});
    },
  },
});
