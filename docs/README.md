# Documentation

Start with the [project README](../README.md) for user and developer quick starts.

## Using the World

- [Getting started](./guides/getting-started.md): SDK integration, database and
  tenant provisioning, and switching to the external runner.
- [Next.js example](../examples/nextjs/README.md): a runnable app using the
  published package, including Docker setup and a smoke check.
- [Configuration reference](./reference/configuration.md): environment variables,
  aliases, and defaults for deployments and the dispatcher.

## Operations

- [Dispatcher operations](./operations/dispatcher.md): pool sizing, readiness,
  ownership liveness, boot recovery, dead letters, and upgrades.
- [Storage and retention](./operations/storage.md): stream storage, retention
  classes, bounded cleanup, and historical scheduler graph repair.

## Architecture and contracts

- [Design](./design.md): topology, tenant isolation, runner modes, data model,
  dispatch and activation contracts, failure semantics, and limitations.
- [Upstream compatibility](./world-postgres-beta34-compatibility.md): required
  behavioral parity and intentional differences from the reference World.

## Maintaining the project

- [Development and contributing](./maintainers/development.md): dependencies,
  static checks, and pull request guidance.
- [Testing](./maintainers/testing.md): unit, integration, external conformance,
  Eve E2E, and manual long-step lease checks.
- [Following Eve](./maintainers/eve-compatibility.md): supported runtime pins,
  drift checks, message stream versions, and Workflow spec compatibility.
- [Releasing](./maintainers/releasing.md): release PRs, versioning, and npm
  trusted publishing.

## Plans and upstream submissions

- [Stream retention rollout plan](./workflow-stream-retention-plan.md): historical
  cross-repository work; use the operations guide for the current contract.
- [Community World submission](./community-world/README.md): proposed upstream
  manifest entry, patch, and submission notes.
