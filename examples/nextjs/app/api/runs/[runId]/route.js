import { getRun } from "workflow/api";

export async function GET(_request, { params }) {
  const { runId } = await params;
  const run = getRun(runId);
  const status = await run.status;
  return Response.json({
    runId,
    status,
    ...(status === "completed" ? { result: await run.returnValue } : {}),
  });
}
