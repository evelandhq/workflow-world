import { AsyncLocalStorage } from "node:async_hooks";
import { defineChannel, POST } from "eve/channels";

type RunRetentionClass = "scheduled" | "interactive" | "persistent";
type RunRetentionIntent = { retentionClass: RunRetentionClass };
const runRetentionContextSymbol = Symbol.for("@evelandhq/workflow-world.run-retention-intent");

export default defineChannel({
  routes: [
    POST("/e2e/scheduled", async (_request, { from }) => {
      const session = await withRunRetentionIntent("scheduled", () =>
        from("scheduled-e2e").send("hello from the scheduled e2e harness", {
          auth: null,
          mode: "task",
          title: "Scheduled retention e2e",
        }),
      );
      return Response.json({ sessionId: session.id });
    }),
    POST("/e2e/persistent", async (_request, { from }) => {
      const session = await withRunRetentionIntent("persistent", () =>
        from("persistent-e2e").send("hello from the persistent e2e harness", {
          auth: null,
          mode: "task",
          title: "Persistent retention e2e",
        }),
      );
      return Response.json({ sessionId: session.id });
    }),
    POST("/e2e/preserve-interactive", async (_request, { from }) => {
      const interactive = await from("preserve-e2e").send("create an interactive owner", {
        auth: null,
        mode: "task",
        title: "Interactive retention preservation e2e",
      });
      const scheduled = await withRunRetentionIntent("scheduled", () =>
        from("preserve-e2e").send("scheduled follow-up on the existing owner", {
          auth: null,
          mode: "task",
          turnPolicy: "queue",
        }),
      );
      return Response.json({ sessionId: interactive.id, scheduledSessionId: scheduled.id });
    }),
  ],
});

function withRunRetentionIntent<T>(retentionClass: RunRetentionClass, operation: () => T): T {
  return getRunRetentionContext().run({ retentionClass }, operation);
}

function getRunRetentionContext(): AsyncLocalStorage<RunRetentionIntent> {
  const existing = Reflect.get(globalThis, runRetentionContextSymbol);
  if (existing instanceof AsyncLocalStorage) {
    return existing as AsyncLocalStorage<RunRetentionIntent>;
  }
  const created = new AsyncLocalStorage<RunRetentionIntent>();
  Reflect.set(globalThis, runRetentionContextSymbol, created);
  return created;
}
