/**
 * Replacement for `@eveland/platform-observability`.
 *
 * The dispatcher emits a handful of structured events and needs no OTel SDK to
 * do it. The host decides where they go: eveland's CLI wrapper hands in a sink
 * backed by `startPlatformObservability`, a bare `npx` run gets the console.
 */
export type DispatcherEvent = {
  severity: "debug" | "info" | "warn" | "error";
  eventName: string;
  body: string;
  attributes?: Record<string, string | number | boolean>;
};

export type DispatcherTelemetry = {
  emit(event: DispatcherEvent): void;
  shutdown(): Promise<void>;
};

export const consoleTelemetry: DispatcherTelemetry = {
  emit(event) {
    const line = `[workflow-dispatcher] ${event.eventName}: ${event.body}`;
    if (event.severity === "error") console.error(line, event.attributes ?? "");
    else if (event.severity === "warn") console.warn(line, event.attributes ?? "");
    else console.log(line, event.attributes ?? "");
  },
  async shutdown() {},
};
