export const RUN_RETENTION_ATTRIBUTE = "workflow-world.retention-class";

export type RunRetentionClass = "scheduled" | "interactive" | "persistent";
export type TerminalRunStatus = "completed" | "failed" | "cancelled";

const MINUTE_MS = 60 * 1000;
const DAY_MS = 24 * 60 * MINUTE_MS;

export function resolveRunRetentionClass(
  value: string | undefined,
  attributes?: Record<string, string>,
): RunRetentionClass {
  const selected = value ?? attributes?.[RUN_RETENTION_ATTRIBUTE] ?? "interactive";
  if (selected === "scheduled" || selected === "ephemeral") return "scheduled";
  if (selected === "interactive" || selected === "persistent") return selected;
  throw new TypeError(
    `Invalid workflow run retention class "${selected}": expected scheduled/ephemeral, interactive, or persistent.`,
  );
}

export function retentionDeadlines(
  retentionClass: RunRetentionClass,
  status: TerminalRunStatus,
  completedAt: Date,
): {
  compactAfter: Date | null;
  expireAfter: Date | null;
  detailExpireAfter: Date | null;
} {
  if (retentionClass === "persistent") {
    return { compactAfter: null, expireAfter: null, detailExpireAfter: null };
  }
  const completed = completedAt.getTime();
  if (retentionClass === "scheduled") {
    const streamRetention = status === "completed" ? 15 * MINUTE_MS : 60 * MINUTE_MS;
    const detailRetention =
      status === "completed" ? DAY_MS : status === "failed" ? 7 * DAY_MS : 3 * DAY_MS;
    return {
      compactAfter: new Date(completed + MINUTE_MS),
      expireAfter: new Date(completed + streamRetention),
      detailExpireAfter: new Date(completed + detailRetention),
    };
  }
  return {
    compactAfter: new Date(completed + 5 * MINUTE_MS),
    expireAfter: new Date(completed + DAY_MS),
    detailExpireAfter: new Date(completed + 30 * DAY_MS),
  };
}
