import { start } from "workflow/api";
import { hello } from "../../../workflows/hello.js";

export async function POST() {
  const run = await start(hello, ["World"]);
  return Response.json({ runId: run.runId });
}
