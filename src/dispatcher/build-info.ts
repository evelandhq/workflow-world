/**
 * Replacement for `@eveland/core/build-info` + `@eveland/core/server/build-info`.
 *
 * The platform's version is not this package's business; what a log line needs
 * is the dispatcher's own version plus whatever revision/channel the host
 * chooses to pass through.
 */
export const DISPATCHER_VERSION = "0.7.0"; // x-release-please-version

export type DispatcherBuildInfo = {
  service: string;
  version: string;
  revision: string;
  channel: string;
};

export function createBuildInfoFromEnv(
  environment: Record<string, string | undefined>,
): DispatcherBuildInfo {
  return {
    service: "workflow-dispatcher",
    version: DISPATCHER_VERSION,
    revision: environment.WORKFLOW_DISPATCHER_REVISION?.trim() || "unknown",
    channel: environment.WORKFLOW_DISPATCHER_CHANNEL?.trim() || "dev",
  };
}

export function formatBuildInfo(buildInfo: DispatcherBuildInfo): string {
  return `workflow-dispatcher ${buildInfo.version} (${buildInfo.channel}, ${buildInfo.revision})`;
}
