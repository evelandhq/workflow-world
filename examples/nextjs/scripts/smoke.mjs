import assert from "node:assert/strict";
import { setTimeout } from "node:timers/promises";

const baseUrl = process.env.QUICKSTART_BASE_URL || "http://localhost:3000";
const deadline = Date.now() + 60_000;
while (true) {
  try {
    const response = await fetch(baseUrl, { signal: AbortSignal.timeout(2_000) });
    if (response.ok) break;
  } catch {
    // The server may still be starting.
  }
  assert.ok(Date.now() < deadline, "Example server did not become ready");
  await setTimeout(500);
}
const response = await fetch(`${baseUrl}/api/start`, {
  method: "POST",
  signal: AbortSignal.timeout(30_000),
});
assert.equal(response.status, 200, await response.clone().text());
const { runId } = await response.json();
assert.equal(typeof runId, "string");
const runDeadline = Date.now() + 60_000;
while (Date.now() < runDeadline) {
  const response = await fetch(`${baseUrl}/api/runs/${encodeURIComponent(runId)}`, {
    signal: AbortSignal.timeout(5_000),
  });
  assert.equal(response.status, 200, await response.clone().text());
  const run = await response.json();
  assert.ok(!["failed", "cancelled"].includes(run.status), JSON.stringify(run));
  if (run.status === "completed") {
    assert.equal(run.result, "Hello, World!");
    console.log(JSON.stringify(run));
    process.exit(0);
  }
  await setTimeout(500);
}
throw new Error(`Workflow ${runId} did not complete within 60 seconds`);
