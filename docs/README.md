# Documentation

Start with the [project README](../README.md) for user and developer quick starts.

## Using the World

- [Getting started](./guides/getting-started.md): database and tenant
  provisioning, deployment configuration, and running the dispatcher.
- [Configuration reference](./reference/configuration.md): environment variables
  and defaults for deployments and the dispatcher.
- [Upgrading to 2.0](./guides/upgrading-to-2.0.md): moving the dispatcher and
  every deployment to dispatch version 2 together, and what happens to jobs 1.x
  left behind.
- [Upgrading to 1.0](./guides/upgrading-to-1.0.md): recreating a database set
  up by 0.x, renamed configuration, and the maintenance lines.

## Operations

- [Dispatcher operations](./operations/dispatcher.md): pool sizing, readiness,
  ownership liveness, boot recovery, and dead letters.
- [Storage and retention](./operations/storage.md): stream storage, retention
  classes, and bounded cleanup.

## Architecture and contracts

- [Design](./design.md): topology, tenant isolation, data model,
  dispatch and activation contracts, failure semantics, and limitations.
- [Upstream compatibility](./world-postgres-compatibility.md): required
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
