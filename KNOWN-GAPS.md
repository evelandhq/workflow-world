# Known gaps

Open problems in this package, with the evidence for each. Nothing here is
speculative — every item was measured against a real database with the dispatcher
in the loop, and the numbers below are from those runs.

Gap IDs are stable and are never reused. G1 through G6 were closed and their
entries removed; what each one was and how it was measured is in the commit that
closed it. The decisions that would otherwise read as oversights live as comments
beside the code they explain — bounded rather than durable message dedup in
`src/dispatcher/dispatcher.ts`, refusing rather than clamping an over-long token
retention request in `src/storage.ts`.

One gap is left, and it is the one the whole design turns on: the activation
lease exists to carry a long step, and no test has yet run a step long enough to
need it.

---

## G7 — no step ever runs long enough to need a renewal

**Status:** open. Needs a new test, not a configuration change.

The lease exists so a step can outlive the 180s activation TTL. Renewal is what
keeps the idle reaper away from a deployment that is still working, and a lease
allowed to lapse mid-step reintroduces exactly the failure this package was built
to remove.

Nothing tests that.

### Measured

At default settings a whole conformance run reports `renew: 0` — around 68
activations, as many releases, and not one renewal. The longest dispatch is far
shorter than the 60s interval, so the timer never fires.

The three other matrix variants all reach the renewal code, but only by moving
the interval to 100ms — they say nothing about _duration_, because those
dispatches are still hundreds of milliseconds. `lease-renewal-refused-past-ttl`
is not the missing coverage either: it reaches the abort by shrinking the TTL,
not by lengthening the step.

So no suite anywhere holds a dispatch open across even one real 60s renewal
interval, let alone the multi-minute step the design is for.

### What closing it takes

A conformance case whose step body blocks for longer than several real renewal
intervals, asserting both that the run completes and that the stub saw the
renewals that kept it alive. Slow by construction, so it likely belongs in its
own matrix entry rather than in the default gate.
