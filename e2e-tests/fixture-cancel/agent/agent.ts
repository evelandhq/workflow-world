import { defineAgent } from "eve";

const usage = {
  inputTokens: { total: 1, noCache: 1, cacheRead: 0, cacheWrite: 0 },
  outputTokens: { total: 1, text: 1, reasoning: 0 },
};

/**
 * A model that calls `send_newsletter` once and then answers, so the suite can
 * drive a real tool call without model credentials.
 */
const scripted = {
  specificationVersion: "v3" as const,
  provider: "e2e",
  modelId: "scripted",
  supportedUrls: {},
  async doGenerate(): Promise<never> {
    throw new Error("the scripted model only streams");
  },
  async doStream(options: { prompt: Array<{ role: string }> }) {
    const toolAnswered = options.prompt.some((message) => message.role === "tool");
    const parts = toolAnswered
      ? [
          { type: "text-start", id: "t1" },
          { type: "text-delta", id: "t1", delta: "Sent." },
          { type: "text-end", id: "t1" },
        ]
      : [
          {
            type: "tool-call",
            toolCallId: "call_1",
            toolName: "send_newsletter",
            input: JSON.stringify({ subject: "October update" }),
          },
        ];
    const finishReason = { unified: toolAnswered ? "stop" : "tool-calls", raw: undefined };
    return {
      stream: new ReadableStream({
        start(controller) {
          controller.enqueue({ type: "stream-start", warnings: [] });
          for (const part of parts) controller.enqueue(part);
          controller.enqueue({ type: "finish", finishReason, usage });
          controller.close();
        },
      }),
    };
  },
};

export default defineAgent({
  model: scripted as never,
  modelContextWindowTokens: 128_000,
  experimental: { workflow: { world: "@evelandhq/workflow-world" } },
});
