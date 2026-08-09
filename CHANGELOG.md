# Changelog

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
