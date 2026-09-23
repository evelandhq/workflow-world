/**
 * Exact releases from Eveland's supported eve window. The compatibility policy,
 * not npm's `latest`, is the authority on what may be deployed.
 *
 * The current window is {0.62.x, 0.64.x} -- gapped, verified at 0.62.0 and
 * 0.64.1. 0.63 is skipped: 0.63.0 shipped a hook regression (vercel/eve#3552)
 * that 0.64.0 fixed, so it is rejected. 0.58.x slid out on 2026-09-20. The
 * window carries TWO `@workflow/*` sets again -- 0.64.0 moved the bundled set,
 * and 0.64.1 re-bundled it unchanged:
 *
 * | package                 | 0.61.0-0.63.0 | 0.64.0-0.64.1 |
 * | ----------------------- | ------------- | ------------- |
 * | `@workflow/world`       | beta.36       | beta.37       |
 * | `@workflow/world-local` | beta.45       | beta.46       |
 * | `@workflow/core`        | beta.53       | beta.55       |
 * | `@workflow/errors`      | beta.21       | beta.22       |
 * | `@workflow/utils`       | beta.10       | beta.10       |
 *
 * World beta.37 adds an opt-in request/response path: `capabilities.invoke`,
 * `World.invoke()`, optional `invoke`/`requestId`/`input` on queue messages,
 * and a `createQueueHandler` handler typed `Promise<unknown>`. Core beta.55
 * takes it only when a World declares the capability, and this World does not,
 * so every delivery stays an ordinary wake: world-local beta.46 answers an
 * `invoke: true` body with the handler's result and otherwise reads
 * `{ timeoutSeconds }` as before. Core beta.55 also checks the new 8 KiB
 * `attr_set` eventData limit itself before writing. The pins here follow the
 * newer set, so the older entry is what proves a Release built on the previous
 * set still runs against this package.
 *
 * The whole window speaks message stream v25 and one execution model: every
 * turn runs as steps inside the session's own `workflowEntry` run (the model
 * 0.57 introduced), with a session inbox rebuilt on hook tokens
 * (`<sessionId>:anchor`, `<runId>:handoff`), deployment handoff and renewable
 * stream leases. The per-turn child `turnWorkflow` run left with 0.55.
 *
 * - 0.62.0 is the floor, which existing Releases run, and samples the older
 *   set. It still has the unstamped `taskRunWorkflow` run behind background
 *   `defineTool`.
 * - 0.64.1 is what new builds get, and samples the newer set. It keeps what
 *   0.63.0 changed -- background work is a workflow-tool run, and a background
 *   subagent's admission is no longer announced as `subagent.completed` -- and
 *   stamps the same workflows 0.63.0 did. Neither line adds a World call.
 *
 * Exact patches matter because Workflow pins move within a minor line: 0.51.1
 * did it, and 0.54.4 did it again. Each enabled entry costs an npm install plus
 * a full `eve build`. Local runs cover all enabled entries; CI supplies
 * EVE_VERSION so each matrix job covers only the release named in its label.
 */
export type EveVersion = {
  version: string;
  /** Set false to keep the entry documented without paying for it on every run. */
  enabled: boolean;
};

export const EVE_VERSIONS: readonly EveVersion[] = [
  { version: "0.62.0", enabled: true },
  { version: "0.64.1", enabled: true },
];

const eveVersionUnderTest = process.env.EVE_VERSION;

export const ENABLED_EVE_VERSIONS = EVE_VERSIONS.filter(
  (entry) => entry.enabled && (!eveVersionUnderTest || entry.version === eveVersionUnderTest),
);

if (eveVersionUnderTest && ENABLED_EVE_VERSIONS.length === 0) {
  throw new Error(`EVE_VERSION "${eveVersionUnderTest}" is not enabled`);
}
