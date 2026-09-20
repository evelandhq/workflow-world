# Configuration

Every variable has one canonical `WORKFLOW_*` name. The `EVELAND_*` names are
accepted as aliases wherever they appear, and both ends of the system read the
same ordered list — [`src/env-contract.test.ts`](../../src/env-contract.test.ts)
asserts that, because a name honoured by only one end is a silent failure rather
than a loud one.

## Deployment side (read by the World)

| variable                           | alias                                | meaning                                                                                                                                      |
| ---------------------------------- | ------------------------------------ | -------------------------------------------------------------------------------------------------------------------------------------------- |
| `WORKFLOW_WORLD_URL`               | `EVELAND_WORKFLOW_WORLD_URL`         | the shared database. Required; there is no fallback chain, because falling back onto a single-tenant database is worse than failing to start |
| `WORKFLOW_WORLD_TENANT_ID`         | `EVELAND_PROJECT_ID`                 | this deployment's tenant                                                                                                                     |
| `WORKFLOW_WORLD_DEPLOYMENT_ID`     | `EVELAND_DEPLOYMENT_ID`              | recorded on every run, so an in-flight run stays pinned to an executor that can still run it                                                 |
| `WORKFLOW_WORLD_RUNNER`            | `EVELAND_WORKFLOW_RUNNER`            | `embedded` (default) or `external`                                                                                                           |
| `WORKFLOW_WORLD_RUNTIME_SECRET`    | `EVELAND_SCHEDULER_RUNTIME_SECRET`   | authenticates platform dispatch                                                                                                              |
| `WORKFLOW_WORLD_STREAM_COMPACTION` | `EVELAND_WORKFLOW_STREAM_COMPACTION` | `on` (default); `off` is the emergency switch for write-side snapshot stripping                                                              |

## Host side (read by the dispatcher)

| variable                                                 | default            | meaning                                                                                                                                                                                  |
| -------------------------------------------------------- | ------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `WORKFLOW_WORLD_URL`                                     | —                  | same database as above                                                                                                                                                                   |
| `WORKFLOW_WORLD_BOOTSTRAP_URL`                           | —                  | override when the host and the containers reach one database by different hostnames                                                                                                      |
| `WORKFLOW_DISPATCHER_ACTIVATION_API_URL`                 | —                  | the host's activation API. Required                                                                                                                                                      |
| `WORKFLOW_DISPATCHER_ACTIVATION_TOKEN`                   | —                  | bearer token for it. Required unless `NODE_ENV=development`                                                                                                                              |
| `WORKFLOW_DISPATCHER_POOL_SIZE`                          | `10`               | claim/complete throughput, plus one connection held for Graphile LISTEN and one for dispatcher ownership                                                                                 |
| `WORKFLOW_DISPATCHER_CONCURRENCY`                        | `poolSize - 2`     | held dispatches in flight across all tenants. Independent of the pool — see [pool sizing](../operations/dispatcher.md#sizing-the-dispatcher-pool)                                        |
| `WORKFLOW_DISPATCHER_POLL_INTERVAL_MS`                   | `500`              |                                                                                                                                                                                          |
| `WORKFLOW_DISPATCHER_MAX_INFLIGHT_PER_TENANT`            | derived from cores | fairness ceiling, not a throttle                                                                                                                                                         |
| `WORKFLOW_DISPATCHER_DISPATCH_TIMEOUT_MS`                | `900000`           | the only deadline on a held dispatch (the delivery does not use `fetch`, so undici's 300s cap does not apply). A backstop against a wedged executor; liveness is the lease renewal's job |
| `WORKFLOW_DISPATCHER_ACTIVATION_LEASE_TTL_MS`            | `180000`           | must match what the host's control API issues                                                                                                                                            |
| `WORKFLOW_DISPATCHER_LEASE_RENEW_INTERVAL_MS`            | `TTL / 3`          | must be well below the TTL, and is checked. Transient failures are absorbed while the lease has headroom                                                                                 |
| `WORKFLOW_DISPATCHER_QUEUE_GC_INTERVAL_MS`               | `300000`           | reclaims the per-run graphile queue rows; graphile does not free them on its own                                                                                                         |
| `WORKFLOW_DISPATCHER_BOOT_RECOVERY_DEPLOYMENTS_PER_WAVE` | `4`                | deployments whose recovered runs are released together at boot                                                                                                                           |
| `WORKFLOW_DISPATCHER_BOOT_RECOVERY_WAVE_INTERVAL_MS`     | `30000`            | delay between those waves; `0` releases every recovered run at once                                                                                                                      |
| `WORKFLOW_DISPATCHER_EXECUTOR_FAILURE_LIMIT`             | `5`                | consecutive executor `5xx` on one run before it is dead-lettered instead of retried to exhaustion                                                                                        |
| `WORKFLOW_DISPATCHER_EXECUTOR_FAILURE_MIN_SPAN_MS`       | `60000`            | the streak must also last this long, so a short database outage does not quarantine healthy runs                                                                                         |
| `WORKFLOW_DISPATCHER_MAINTENANCE_INTERVAL_MS`            | `60000`            | storage maintenance cadence; `0` disables the automatic loop                                                                                                                             |
| `WORKFLOW_DISPATCHER_MAINTENANCE_STREAM_BATCH_SIZE`      | `50000`            | maximum physical stream rows deleted by one statement                                                                                                                                    |
| `WORKFLOW_DISPATCHER_MAINTENANCE_MAX_BATCHES`            | `20`               | maximum stream/run deletion batches per pass                                                                                                                                             |
| `WORKFLOW_DISPATCHER_MAINTENANCE_MAX_STREAMS_TO_PACK`    | `100`              | maximum terminal streams rewritten into blocks per pass                                                                                                                                  |
| `WORKFLOW_DISPATCHER_MAINTENANCE_RUN_BATCH_SIZE`         | `1000`             | maximum expired workflow graphs deleted by one statement                                                                                                                                 |
| `WORKFLOW_WORLD_STREAM_COMPACTION`                       | `on`               | also controls snapshot stripping during terminal block rewrites                                                                                                                          |
| `WORKFLOW_DISPATCHER_OWNERSHIP_LIVENESS_MS`              | `90000`            | how long a dead dispatcher may keep the ownership lock before the server reclaims it — see [ownership liveness](../operations/dispatcher.md#ownership-liveness)                          |
| `WORKFLOW_DISPATCHER_OWNERSHIP_RETRY_INTERVAL_MS`        | `5000`             | retry cadence while another dispatcher holds the lock                                                                                                                                    |
| `WORKFLOW_DISPATCHER_OWNERSHIP_WAIT_MS`                  | unbounded          | give up on the lock after this long; `0` fails on the first miss, as versions before 0.16 always did                                                                                     |

See [dispatcher operations](../operations/dispatcher.md) for runtime behavior
and [storage and retention](../operations/storage.md) for maintenance policies.
