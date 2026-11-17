import type { MessageData } from "../message.js";

/**
 * How many times one delivery re-runs in place for wakes that arrived while it
 * ran. Upstream world-postgres's bound: a run woken faster than it can finish
 * a delivery still lets its holder go, and the wake left pending then goes back
 * to the queue as an ordinary job.
 */
export const MAX_COALESCED_RERUNS = 32;

/**
 * A delivery starts no re-run once it has held its job this long. graphile
 * treats a lock older than four hours as stale and hands the job out again, so
 * a run woken without pause must not keep one job locked for 33 dispatch
 * timeouts in a row. One hour leaves the last re-run three hours of headroom.
 */
export const MAX_RERUN_HOLD_MS = 60 * 60_000;

/**
 * A wake that found its run's gate held: the message as claimed, and the
 * accounting its job had, so that re-adding it reports the same counts.
 */
export type PendingWake = {
  message: MessageData;
  attempt: number;
  deliveryCount: number;
  /** The attempts its job had left, which a re-added job keeps. */
  maxAttempts: number;
  /** Carries a hook resume's timing (`hookResumeTiming`), which a re-run reports. */
  hookResume: boolean;
};

/** What became of a wake offered to a held gate. */
export type WakeOffer = "pending" | "replaced" | "covered" | "closing";

/**
 * The per-run gate that serializes a run's orchestrator deliveries in this
 * dispatcher process.
 *
 * One entry per `(tenantId, runId)` whose orchestrator delivery is in flight.
 * The state machine, per run:
 *
 *   free ─tryEnter─▶ held ─offerWake─▶ held + one pending wake
 *    ▲                │ ▲                       │
 *    │                │ └─takeWake (re-run)─────┘
 *    │              close: the holder will not run again in place
 *    │                ▼
 *    └──── leave ── closing: the pending wake is re-added as a job, and a
 *                   wake offered now is refused ("closing")
 *
 * The holder is the one delivery that entered; nothing else of the run is
 * delivered until it leaves. A message that finds the gate held is either
 * coalesced (a wake, kept as the run's single pending wake) or set aside as a
 * delayed job by the caller (a data-bearing message, which is never dropped,
 * and any wake that finds the gate closing).
 *
 * The gate closes as soon as the holder's way out is decided and it will not
 * re-run: before it writes a continuation (a reschedule or a handoff), and on
 * a retry, a dead letter, a drop, the re-run cap, the hold limit or shutdown.
 * Closing re-adds the pending wake first, while the holder's job is locked and
 * still owns its key, so a failed re-add fails that job and graphile
 * redelivers it; from then on no wake of the run lives only in memory.
 *
 * Invariant: a pending wake is discarded only when a delivery of the same run
 * started after it was recorded: the holder's in-place re-run (`takeWake`), or
 * the delivery of the job closing re-adds it as. A wake offered while one is
 * already pending is covered by whichever of the two the gate keeps, because
 * the delivery that discards the kept one starts after both. Until that re-add
 * has committed, the holder's job is locked and still its own, so should this
 * process die, the job's redelivery is that later delivery.
 *
 * In-memory on purpose: one dispatcher process owns the queue (the lifecycle
 * advisory lock), so the gate needs no table. It is not what makes an overlap
 * safe; the storage's in-band writer fence is, and it stays the guard when a
 * dispatcher that lost its lock is still finishing POSTs beside its successor.
 */
export function createRunGate() {
  const held = new Map<string, { pending?: PendingWake; closing?: true }>();

  const takeWake = (key: string): PendingWake | undefined => {
    const entry = held.get(key);
    const wake = entry?.pending;
    if (entry) delete entry.pending;
    return wake;
  };

  /**
   * Closes the gate to new wakes and re-adds the pending one as a job of its
   * own (`readd`). If `readd` fails the error is thrown and the wake is gone
   * from memory: the caller fails the holder's job, which still owns its key,
   * and graphile's redelivery of it covers the wake.
   */
  const close = async (key: string, readd: (wake: PendingWake) => Promise<void>): Promise<void> => {
    const entry = held.get(key);
    if (!entry) return;
    entry.closing = true;
    const wake = takeWake(key);
    if (wake) await readd(wake);
  };

  return {
    /** Takes the run's gate if it is free. The caller must `leave` it. */
    tryEnter(key: string): boolean {
      if (held.has(key)) return false;
      held.set(key, {});
      return true;
    },

    /**
     * Leaves a wake for the gate's holder. Upstream's replacement rule: one
     * pending wake per run, and a wake carrying a hook resume's timing replaces
     * one without, so a re-run can report the resume it serves. The caller
     * completes the wake's job, unless this answers `closing`: then nothing was
     * recorded, and the caller puts the wake back on the queue itself.
     */
    offerWake(key: string, wake: PendingWake): WakeOffer {
      const entry = held.get(key);
      if (!entry) throw new Error(`Run gate ${key} is not held.`);
      if (entry.closing) return "closing";
      if (!entry.pending) {
        entry.pending = wake;
        return "pending";
      }
      if (wake.hookResume && !entry.pending.hookResume) {
        entry.pending = wake;
        return "replaced";
      }
      return "covered";
    },

    /** Clears and returns the run's pending wake, for the holder to serve it. */
    takeWake,

    /** Whether the run has a wake pending, without taking it. */
    hasWake(key: string): boolean {
      return held.get(key)?.pending !== undefined;
    },

    close,

    /**
     * Closes the gate to new wakes without re-adding the pending one yet, for
     * a caller that has decided the holder's exit in a synchronous step; `leave`
     * re-adds it.
     */
    markClosing(key: string): void {
      const entry = held.get(key);
      if (entry) entry.closing = true;
    },

    /**
     * Releases the gate, closing it first if the holder has not, so a wake
     * still pending goes back to the queue before the gate opens. The gate is
     * released even if that re-add fails, and the error is thrown.
     */
    async leave(key: string, readd: (wake: PendingWake) => Promise<void>): Promise<void> {
      try {
        await close(key, readd);
      } finally {
        held.delete(key);
      }
    },

    /** Runs whose gate is held, and how many of them have a wake pending. */
    snapshot(): { held: number; pendingWakes: number } {
      let pendingWakes = 0;
      for (const entry of held.values()) if (entry.pending) pendingWakes += 1;
      return { held: held.size, pendingWakes };
    },
  };
}

export type RunGate = ReturnType<typeof createRunGate>;

/** The gate key of a run. Tenant-scoped: two tenants could mint the same run id. */
export function runGateKey(tenantId: string, runId: string): string {
  return `${tenantId}\u0000${runId}`;
}

/**
 * The delay before a data-bearing message that found its run's gate held is
 * tried again: one second, doubling with each time it was set aside, at most
 * thirty.
 */
export function setAsideDelaySeconds(setAside: number): number {
  return Math.min(30, 2 ** Math.max(0, setAside));
}

/**
 * The body a coalesced re-run delivers: the holder's own message, carrying the
 * absorbed wake's hook resume timing and trace when it has one (upstream's
 * `rerunBody`). The holder's own `hookResumeTiming` measured a resume its
 * first delivery already served.
 */
export function rerunBody(holder: MessageData, wake: PendingWake): Buffer {
  const holderBody = readJson(holder.data);
  // Only an orchestrator delivery holds a gate, and its body is an object; any
  // other body is delivered again as it is.
  if (typeof holderBody !== "object" || holderBody === null) return holder.data;
  const { hookResumeTiming: _served, ...rest } = holderBody as Record<string, unknown>;
  const wakeBody = readJson(wake.message.data) as
    | { hookResumeTiming?: unknown; traceCarrier?: unknown }
    | undefined;
  const resume =
    wakeBody && typeof wakeBody === "object" && wakeBody.hookResumeTiming !== undefined
      ? {
          hookResumeTiming: wakeBody.hookResumeTiming,
          ...(wakeBody.traceCarrier !== undefined ? { traceCarrier: wakeBody.traceCarrier } : {}),
        }
      : {};
  return Buffer.from(JSON.stringify({ ...rest, ...resume }));
}

function readJson(data: Buffer): unknown {
  try {
    return JSON.parse(Buffer.from(data).toString("utf8")) as unknown;
  } catch {
    return undefined;
  }
}
