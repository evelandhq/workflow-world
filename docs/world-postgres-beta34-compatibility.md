# `@workflow/world-postgres` beta.34 compatibility contract

The compatibility target is behavioral equivalence with
`@workflow/world-postgres@5.0.0-beta.34` at the public `@workflow/world` boundary,
not identical SQL, topology, configuration names, or migration history. The
matching public interface is `@workflow/world@5.0.0-beta.27`.

## Required parity

| Contract                                                                                  | Local evidence                                                       |
| ----------------------------------------------------------------------------------------- | -------------------------------------------------------------------- |
| Run/event/step/hook/wait lifecycle, errors, ordering and idempotency                      | Ported storage suites under `src/storage.*.test.ts`                  |
| `encryptionPublicKey` survives normal and resilient run creation                          | `src/storage.runs.test.ts`                                           |
| Retained hooks remain visible until expiry, then become invisible and release their token | `src/storage.hook-retention.test.ts`                                 |
| Logical stream bytes, indices, cursors, EOF and mixed legacy/v2 reads                     | `src/streamer.storage-v2.integration.test.ts` and stream unit suites |
| Graphile delivery budget is 49 and an embedded worker shutdown aborts its HTTP request    | `src/queue.test.ts`                                                  |
| External dispatcher reschedule and boot recovery use the same 49-attempt budget           | `src/dispatcher/runner.test.ts`                                      |
| Public World conformance and supported Eve integration                                    | `conformance/` and `e2e-tests/`                                      |

These behaviors are release blockers. An upstream version bump must re-run this
matrix and port any new public field, state transition, error, capability, or
delivery guarantee before the dependency pins move.

## Intentional differences

The following are allowed differences and should not be "fixed" toward upstream
without an explicit architecture decision:

- Tenant columns, tenant-scoped keys/predicates/NOTIFY channels and partitions.
- Real deployment affinity instead of the constant deployment id `postgres`.
- Embedded/external runner topology, dispatcher job names, fairness flags,
  activation leases, dead letters and per-run Graphile queues.
- Snapshot stripping/rehydration, server checkpoints and packed physical stream
  blocks. Public logical chunks and cursors remain compatible.
- Explicit retention classes and automatic expiry. Use `persistent` when the
  upstream no-automatic-expiry behavior is required.
- Configuration names and migration history.
- Internal page sizes when `cursor`/`hasMore` semantics remain correct. In
  particular, callers must not assume one `events.list()` call returns every
  event.
- The absent `hooks.resume_context` cache column. Core falls back to the run read;
  adding it is a performance optimization, not a correctness requirement.

Everything not listed here is presumed to require public behavioral parity.
