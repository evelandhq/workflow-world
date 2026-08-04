import type { ActivationClient } from "./activation-client.js";

/**
 * Holds an activation lease for the whole of `body`, renewing on an interval.
 *
 * This is the piece that makes long steps survive. The lease both keeps the
 * idle reaper away from the deployment and registers as `active_request` in the
 * control plane's retention accounting, so a running step also protects its
 * deployment from being archived. Neither holds if the lease is allowed to
 * expire mid-step.
 *
 * When renewal genuinely stops working the in-flight request is aborted rather
 * than left running against an executor the platform now considers idle: better a
 * retryable failure than a step that is silently no longer protected.
 *
 * But *one* failed renewal is not that. The TTL is several renewal intervals
 * wide by construction — `resolveDispatcherConfig` defaults the interval to a
 * third of the TTL and refuses a configuration where it is not well below —
 * so a single 503 from the control API leaves room for further attempts before
 * the lease could actually lapse. Aborting on the first failure turned a blip
 * into a burned graphile attempt, and three of those dead-letter the run.
 *
 * So renewal failures are tolerated while the lease still has headroom, and the
 * dispatch is aborted only once continued failure means it is about to expire.
 */
export async function withRenewedLease<T>(
  input: {
    client: ActivationClient;
    leaseId: string;
    renewIntervalMs: number;
    /**
     * The TTL the control plane issued. Used only to work out how many
     * consecutive failures fit before the lease lapses; when absent, a single
     * failure aborts, which is the old behaviour and the safe default.
     */
    leaseTtlMs?: number;
    onRenewFailure?: (leaseId: string, consecutiveFailures: number) => void;
  },
  body: (signal: AbortSignal) => Promise<T>,
): Promise<T> {
  const controller = new AbortController();

  // How many consecutive misses we can absorb and still renew before the TTL.
  // One interval is reserved as the margin that makes the last attempt useful:
  // renewing at the instant of expiry is a race, not a renewal.
  const allowedFailures =
    input.leaseTtlMs !== undefined && input.renewIntervalMs > 0
      ? Math.max(0, Math.floor(input.leaseTtlMs / input.renewIntervalMs) - 2)
      : 0;

  let consecutiveFailures = 0;

  const giveUp = (error: unknown) => {
    controller.abort(error instanceof Error ? error : new Error(String(error)));
  };

  const onFailure = (error: unknown) => {
    consecutiveFailures += 1;
    input.onRenewFailure?.(input.leaseId, consecutiveFailures);
    if (consecutiveFailures > allowedFailures) {
      giveUp(error);
    }
  };

  const timer = setInterval(() => {
    void input.client
      .renew(input.leaseId)
      .then((renewed) => {
        if (renewed) {
          // Only a success resets the count: alternating pass/fail must not be
          // able to keep a dying lease alive indefinitely.
          consecutiveFailures = 0;
          return;
        }
        onFailure(
          new Error(
            `Activation lease ${input.leaseId} could not be renewed ` +
              `${String(consecutiveFailures + 1)} time(s) in a row; aborting dispatch.`,
          ),
        );
      })
      .catch(onFailure);
  }, input.renewIntervalMs);
  // Renewal must never be the reason the process stays alive.
  timer.unref?.();

  try {
    return await body(controller.signal);
  } finally {
    clearInterval(timer);
    await input.client.release(input.leaseId);
  }
}
