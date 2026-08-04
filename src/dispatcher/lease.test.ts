import { describe, expect, test, vi } from "vitest";
import type { ActivationClient } from "./activation-client.js";
import { withRenewedLease } from "./lease.js";

function client(overrides: Partial<ActivationClient> = {}): ActivationClient {
  return {
    activate: vi.fn(),
    renew: vi.fn(async () => true),
    release: vi.fn(async () => {}),
    ...overrides,
  } as ActivationClient;
}

/**
 * Lease renewal is what lets a step outlive the 180s activation TTL. Without
 * it the idle reaper stops the executor mid-step — the exact failure this whole
 * project exists to remove, reintroduced one layer up.
 */
describe("withRenewedLease", () => {
  test("renews while the body is still running", async () => {
    vi.useFakeTimers();
    try {
      const activation = client();
      let release!: () => void;
      const body = new Promise<string>((resolve) => {
        release = () => resolve("done");
      });

      const promise = withRenewedLease(
        { client: activation, leaseId: "lease_1", renewIntervalMs: 1_000 },
        () => body,
      );

      await vi.advanceTimersByTimeAsync(3_500);
      expect(activation.renew).toHaveBeenCalledTimes(3);

      release();
      await expect(promise).resolves.toBe("done");
    } finally {
      vi.useRealTimers();
    }
  });

  test("stops renewing and releases once the body settles", async () => {
    vi.useFakeTimers();
    try {
      const activation = client();
      await withRenewedLease(
        { client: activation, leaseId: "lease_1", renewIntervalMs: 1_000 },
        async () => "ok",
      );
      const callsAtFinish = (activation.renew as ReturnType<typeof vi.fn>).mock.calls.length;

      await vi.advanceTimersByTimeAsync(5_000);
      expect(activation.renew).toHaveBeenCalledTimes(callsAtFinish);
      expect(activation.release).toHaveBeenCalledWith("lease_1");
    } finally {
      vi.useRealTimers();
    }
  });

  test("releases the lease even when the body throws", async () => {
    const activation = client();
    await expect(
      withRenewedLease({ client: activation, leaseId: "lease_1", renewIntervalMs: 60_000 }, () =>
        Promise.reject(new Error("boom")),
      ),
    ).rejects.toThrow("boom");
    expect(activation.release).toHaveBeenCalledWith("lease_1");
  });

  test("aborts the in-flight request when renewal is refused and no TTL is known", async () => {
    // A lease the platform no longer honours means the executor may be reaped at
    // any moment. Failing fast turns that into a retry instead of a step running
    // on unprotected.
    //
    // With no `leaseTtlMs` there is no way to know how much headroom remains, so
    // the first failure aborts. When the TTL *is* known, transient failures are
    // absorbed instead — see the suite below.
    vi.useFakeTimers();
    try {
      const activation = client({ renew: vi.fn(async () => false) });
      const onRenewFailure = vi.fn();

      const promise = withRenewedLease(
        {
          client: activation,
          leaseId: "lease_1",
          renewIntervalMs: 1_000,
          onRenewFailure,
        },
        (signal) =>
          new Promise((_resolve, reject) => {
            signal.addEventListener("abort", () => reject(new Error("aborted")), { once: true });
          }),
      );
      // Attached before the timers run: the abort fires during the flush below,
      // so waiting until afterwards to assert would leave the rejection
      // momentarily unhandled and fail the run on a stray warning.
      const rejects = expect(promise).rejects.toThrow("aborted");

      await vi.advanceTimersByTimeAsync(1_100);
      await rejects;
      expect(onRenewFailure).toHaveBeenCalledWith("lease_1", 1);
      expect(activation.release).toHaveBeenCalledWith("lease_1");
    } finally {
      vi.useRealTimers();
    }
  });
});

/**
 * The TTL is several renewal intervals wide by construction, so a single failed
 * renewal is a blip rather than an expired lease. Aborting on the first one
 * turned one 503 from the control API into a burned graphile attempt, and three
 * of those dead-letter the run.
 */
describe("withRenewedLease tolerates transient renewal failures", () => {
  const ttlAndInterval = { renewIntervalMs: 1_000, leaseTtlMs: 10_000 } as const;

  test("a run of failures short of the TTL does not abort the dispatch", async () => {
    vi.useFakeTimers();
    try {
      // 10s TTL / 1s interval, minus one interval of margin => 8 tolerated.
      let calls = 0;
      const activation = client({
        renew: vi.fn(async () => {
          calls += 1;
          return calls > 3; // three consecutive misses, then recovery
        }),
      });
      const failures: number[] = [];

      let release!: () => void;
      const body = new Promise<string>((resolve) => {
        release = () => resolve("survived");
      });

      const promise = withRenewedLease(
        {
          client: activation,
          leaseId: "lease_flaky",
          ...ttlAndInterval,
          onRenewFailure: (_leaseId, consecutive) => failures.push(consecutive),
        },
        (signal) =>
          body.then((value) => {
            expect(signal.aborted).toBe(false);
            return value;
          }),
      );

      await vi.advanceTimersByTimeAsync(5_500);
      expect(failures).toEqual([1, 2, 3]);

      release();
      await expect(promise).resolves.toBe("survived");
    } finally {
      vi.useRealTimers();
    }
  });

  test("a success resets the tolerance, so alternating failures cannot run forever", async () => {
    vi.useFakeTimers();
    try {
      let calls = 0;
      const activation = client({
        // fail, succeed, fail, succeed, ... — never two in a row.
        renew: vi.fn(async () => {
          calls += 1;
          return calls % 2 === 0;
        }),
      });
      const failures: number[] = [];

      const promise = withRenewedLease(
        {
          client: activation,
          leaseId: "lease_alternating",
          ...ttlAndInterval,
          onRenewFailure: (_leaseId, consecutive) => failures.push(consecutive),
        },
        () => new Promise<string>(() => {}),
      );
      // Never settles; assert on the counter instead and let the timer stop.
      void promise.catch(() => {});

      await vi.advanceTimersByTimeAsync(20_000);
      // Every reported failure is the first of its run, because each success
      // resets the count. Crucially none of them escalated to an abort.
      expect(new Set(failures)).toEqual(new Set([1]));
    } finally {
      vi.useRealTimers();
    }
  });

  test("sustained failure past the TTL still aborts", async () => {
    vi.useFakeTimers();
    try {
      const activation = client({ renew: vi.fn(async () => false) });
      let aborted: unknown;

      const promise = withRenewedLease(
        { client: activation, leaseId: "lease_dead", ...ttlAndInterval },
        (signal) =>
          new Promise<never>((_resolve, reject) => {
            signal.addEventListener("abort", () => {
              aborted = signal.reason;
              reject(signal.reason as Error);
            });
          }),
      );

      await vi.advanceTimersByTimeAsync(12_000);
      await expect(promise).rejects.toThrow(/could not be renewed/);
      expect(String(aborted)).toContain("lease_dead");
    } finally {
      vi.useRealTimers();
    }
  });

  test("without a TTL the first failure aborts, preserving the safe default", async () => {
    vi.useFakeTimers();
    try {
      const activation = client({ renew: vi.fn(async () => false) });
      const promise = withRenewedLease(
        { client: activation, leaseId: "lease_no_ttl", renewIntervalMs: 1_000 },
        (signal) =>
          new Promise<never>((_resolve, reject) => {
            signal.addEventListener("abort", () => reject(signal.reason as Error));
          }),
      );

      await vi.advanceTimersByTimeAsync(1_500);
      await expect(promise).rejects.toThrow(/could not be renewed/);
      expect(activation.renew).toHaveBeenCalledTimes(1);
    } finally {
      vi.useRealTimers();
    }
  });
});
