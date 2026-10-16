import { defineTool } from "eve/tools";
import { z } from "zod";

/** Markers the suite reads from the agent's output. */
const mark = (event: string) => console.log(`[e2e-cancel] ${event}`);
/** How long a send takes; a suite that needs a longer step sets it on the agent. */
const SECONDS = Number(process.env.E2E_TOOL_SECONDS ?? 8);

export default defineTool({
  description: "Sends the newsletter to every subscriber. Takes a few seconds.",
  inputSchema: z.object({ subject: z.string() }),
  async execute(_input, ctx) {
    mark("started");
    ctx.abortSignal?.addEventListener("abort", () => mark("aborted"), { once: true });
    for (let second = 0; second < SECONDS; second += 1) {
      if (ctx.abortSignal?.aborted) throw ctx.abortSignal.reason ?? new Error("aborted");
      await new Promise((resolve) => setTimeout(resolve, 1_000));
    }
    mark("sent");
    return { sent: true };
  },
});
