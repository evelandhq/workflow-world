# Changelog

## [0.13.1](https://github.com/evelandhq/workflow-world/compare/workflow-world-v0.13.0...workflow-world-v0.13.1) (2026-08-22)


### Fixes

* keep workflow.run_quarantines for Releases baked with &lt;= 0.12.0 ([#49](https://github.com/evelandhq/workflow-world/issues/49)) ([5ae2fe9](https://github.com/evelandhq/workflow-world/commit/5ae2fe9eb723d09b0c7014fc7c7c98e67a9e9d13))

## [0.13.0](https://github.com/evelandhq/workflow-world/compare/workflow-world-v0.12.0...workflow-world-v0.13.0) (2026-08-22)


### ⚠ BREAKING CHANGES

* `readLatestCutoverProof`/`recordCutoverProof`, `countClaimableUnscopedFlowJobs`/`migrateUnscopedRunJobs`/`readFlowJobRun`, the `quarantine*` exports, `DispatcherServiceOptions.startPaused`, `DispatcherService.resume()` and the `ready_paused` lifecycle phase are removed; `workflow.cutover_proofs` and `workflow.run_quarantines` are dropped on migration.

### Refactoring

* remove the completed cutover machinery ([#47](https://github.com/evelandhq/workflow-world/issues/47)) ([2cb0839](https://github.com/evelandhq/workflow-world/commit/2cb083960d93d3b938dcea7f3d267347712ab651))

## [0.12.0](https://github.com/evelandhq/workflow-world/compare/workflow-world-v0.11.0...workflow-world-v0.12.0) (2026-08-19)


### Features

* decouple dispatcher concurrency from the pool size ([#41](https://github.com/evelandhq/workflow-world/issues/41)) ([cfa2115](https://github.com/evelandhq/workflow-world/commit/cfa211506fcde07631e9efe351d721323583bce3))

## [0.11.0](https://github.com/evelandhq/workflow-world/compare/workflow-world-v0.10.1...workflow-world-v0.11.0) (2026-08-18)


### Features

* bind activation to the dispatcher instance and persist a World-visible cutover proof ([#39](https://github.com/evelandhq/workflow-world/issues/39)) ([b064c2b](https://github.com/evelandhq/workflow-world/commit/b064c2b2abdabfd283b4c15341bd2aacb6a6408c))

## [0.10.1](https://github.com/evelandhq/workflow-world/compare/workflow-world-v0.10.0...workflow-world-v0.10.1) (2026-08-18)


### Fixes

* compare against the exact per-run queue, never a wfrun: prefix ([#37](https://github.com/evelandhq/workflow-world/issues/37)) ([b06f1cb](https://github.com/evelandhq/workflow-world/commit/b06f1cbd7de68a4c4dc926119ee11e4ef21abe41))

## [0.10.0](https://github.com/evelandhq/workflow-world/compare/workflow-world-v0.9.0...workflow-world-v0.10.0) (2026-08-18)


### Features

* recover-paused lifecycle, durable run quarantine, and in-place early-external job migration ([#35](https://github.com/evelandhq/workflow-world/issues/35)) ([a4966bc](https://github.com/evelandhq/workflow-world/commit/a4966bce5d77af395295d5bf2cff4189e31d4427))

## [0.9.0](https://github.com/evelandhq/workflow-world/compare/workflow-world-v0.8.1...workflow-world-v0.9.0) (2026-08-17)


### Features

* enforce graph-safe scheduled retention ([#32](https://github.com/evelandhq/workflow-world/issues/32)) ([d377e83](https://github.com/evelandhq/workflow-world/commit/d377e831c46e5a3288c60425b15112ec439a9f32))

## [0.8.1](https://github.com/evelandhq/workflow-world/compare/workflow-world-v0.8.0...workflow-world-v0.8.1) (2026-08-17)


### Fixes

* recover stranded dispatcher queue locks ([a56624f](https://github.com/evelandhq/workflow-world/commit/a56624f1833153d4407d8074568c06f9a84450ec))

## [0.8.0](https://github.com/evelandhq/workflow-world/compare/workflow-world-v0.7.1...workflow-world-v0.8.0) (2026-08-17)


### Features

* propagate workflow retention classes ([0819cb2](https://github.com/evelandhq/workflow-world/commit/0819cb2aabd25adfd8b500354a5aa8ca329417b3))

## [0.7.1](https://github.com/evelandhq/workflow-world/compare/workflow-world-v0.7.0...workflow-world-v0.7.1) (2026-08-17)


### Fixes

* quarantine unresolved dispatch dead letters ([#26](https://github.com/evelandhq/workflow-world/issues/26)) ([d2e1007](https://github.com/evelandhq/workflow-world/commit/d2e1007864ee02c9602ebe43df86cf0f6874e918))

## [0.7.0](https://github.com/evelandhq/workflow-world/compare/workflow-world-v0.6.0...workflow-world-v0.7.0) (2026-08-17)


### Features

* add bounded stream storage v2 ([527b60f](https://github.com/evelandhq/workflow-world/commit/527b60f77c750b99263fb88f0d90a6cfc53079d5))


### Fixes

* align world-postgres beta.34 behavior ([013e6c0](https://github.com/evelandhq/workflow-world/commit/013e6c04adea2151ac5368843e62b05cfb28ad23))

## [0.6.0](https://github.com/evelandhq/workflow-world/compare/workflow-world-v0.5.0...workflow-world-v0.6.0) (2026-08-15)


### Features

* support workflow spec v6 ([#20](https://github.com/evelandhq/workflow-world/issues/20)) ([8e6de90](https://github.com/evelandhq/workflow-world/commit/8e6de909566af11441b9453fd79dfb14ae6cdbec))

## [0.5.0](https://github.com/evelandhq/workflow-world/compare/workflow-world-v0.4.0...workflow-world-v0.5.0) (2026-08-14)


### Features

* add bounded workflow stream retention ([#17](https://github.com/evelandhq/workflow-world/issues/17)) ([ca62033](https://github.com/evelandhq/workflow-world/commit/ca62033fde4b404cc24112ffe2fdfbcf5d579558))

## [0.4.0](https://github.com/evelandhq/workflow-world/compare/workflow-world-v0.3.0...workflow-world-v0.4.0) (2026-08-09)


### ⚠ BREAKING CHANGES

* runs created before this version have no recorded queue namespace, and boot recovery can only fall back to the default prefix for them — which a namespaced deployment refuses with 400 "Unhandled queue". Drain or cancel active runs before upgrading. The dispatcher logs every run it recovers without a recorded namespace so an incomplete drain is visible rather than silent.

### Fixes

* keep the queue namespace when the dispatcher rebuilds a recovered run ([#13](https://github.com/evelandhq/workflow-world/issues/13)) ([79e2a1c](https://github.com/evelandhq/workflow-world/commit/79e2a1c948a16393e92cc4b6b1fc54785d4f28c5))

## [0.3.0](https://github.com/evelandhq/workflow-world/compare/workflow-world-v0.2.0...workflow-world-v0.3.0) (2026-08-09)


### ⚠ BREAKING CHANGES

* `@workflow/world`, `@workflow/world-local` and `@workflow/errors` move to the set eve 0.31.2+ installs. This is a pairing that eve validates at runtime, so a consumer cannot stay on the beta.24 set and take this release. `events.listByCorrelationId` additionally requires `runId` and now returns only the requested run's events, which is observable where a hook is created under one run and received under another.

### Features

* track eve 0.31 and the [@workflow](https://github.com/workflow) pins it installs ([#12](https://github.com/evelandhq/workflow-world/issues/12)) ([903b071](https://github.com/evelandhq/workflow-world/commit/903b071fe6d88b5213c9b943e091e4eba79aaaad))


### Fixes

* report the version the dispatcher actually ships ([#9](https://github.com/evelandhq/workflow-world/issues/9)) ([87fd53d](https://github.com/evelandhq/workflow-world/commit/87fd53d3cf329a83d64d129c176cd85efb99f738))

## [0.2.0](https://github.com/evelandhq/workflow-world/compare/workflow-world-v0.1.0...workflow-world-v0.2.0) (2026-08-06)


### Features

* track eve 0.30 and the [@workflow](https://github.com/workflow) pins it installs ([#6](https://github.com/evelandhq/workflow-world/issues/6)) ([bd134c4](https://github.com/evelandhq/workflow-world/commit/bd134c48f66043cc5e85e70282eb346aa8d5fca2))
