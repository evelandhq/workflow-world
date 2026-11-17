# `@workflow/world-postgres` compatibility contract

The compatibility target is behavioral equivalence with upstream's
`@workflow/world-postgres` at the public `@workflow/world` boundary this package
pins (5.2.0), not identical SQL, topology, configuration names, or migration
history. The storage, streamer, queue and schema modules were ported from
world-postgres 5.0.0-beta.34 ([NOTICE](../NOTICE)) and follow upstream's
behavior since.

## Required parity

| Contract                                                                                  | Local evidence                                                       |
| ----------------------------------------------------------------------------------------- | -------------------------------------------------------------------- |
| Run/event/step/hook/wait lifecycle, errors, ordering and idempotency                      | Ported storage suites under `src/storage.*.test.ts`                  |
| `encryptionPublicKey` survives normal and resilient run creation                          | `src/storage.runs.test.ts`                                           |
| Retained hooks remain visible until expiry, then become invisible and release their token | `src/storage.hook-retention.test.ts`                                 |
| Logical stream bytes, indices, cursors, EOF and mixed legacy/v2 reads                     | `src/streamer.storage-v2.integration.test.ts` and stream unit suites |
| Graphile delivery budget is 49, for the World's sends and the dispatcher's reschedules    | `src/queue.test.ts` and `src/dispatcher/runner.test.ts`              |
| `$retention: 0` deletes a run's user data at its terminal transition, marking it expired  | `src/run-data-purge.integration.test.ts`                             |
| `hookResumeDedup`: one `hook_received` per `(run, resumeId)`, retries answered with it    | `src/storage.hook-resume-dedup.test.ts`                              |
| Public World conformance and supported Eve integration                                    | `conformance/` and `e2e-tests/`                                      |

These behaviors are release blockers. An upstream version bump must re-run this
matrix and port any new public field, state transition, error, capability, or
delivery guarantee before the dependency pins move.

## Intentional differences

The following are allowed differences and should not be "fixed" toward upstream
without an explicit architecture decision:

- Tenant columns, tenant-scoped keys/predicates/NOTIFY channels and partitions.
- Real deployment affinity instead of the constant deployment id `postgres`.
- No in-process runner: an out-of-process dispatcher, shared job names, fairness flags,
  activation leases, dead letters and per-run Graphile queues.
- Write-side snapshot stripping and packed physical stream blocks. Public
  logical chunks and cursors remain compatible.
- Explicit retention classes and automatic expiry. Use `persistent` when the
  upstream no-automatic-expiry behavior is required.
- A `$retention: 0` purge deletes the run's non-EOF stream rows instead of
  blanking them, because a packed block holds its chunk ids in its data. It also
  empties the body of the run's dead letters, keeps the payload of
  `hook_disposed` events (World-written token and force-claim routing, which
  the hook redirect reads), and a purged event reads back as
  its envelope without `eventData` (upstream's event reads reject it). The
  purged rows themselves are removed later by the run's retention class, like
  any other run's.
- Configuration names and migration history.
- Internal page sizes when `cursor`/`hasMore` semantics remain correct. In
  particular, callers must not assume one `events.list()` call returns every
  event.
- The absent `hooks.resume_context` cache column. Core falls back to the run read;
  adding it is a performance optimization, not a correctness requirement.

Everything not listed here is presumed to require public behavioral parity.
