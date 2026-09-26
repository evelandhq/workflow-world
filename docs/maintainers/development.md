# Development and contributing

Start with the [development quick start](../../README.md#development). This guide
covers dependency management and the checks to run before opening a pull request.

## Environment

Use Node.js 24 or newer and the pnpm version pinned in `package.json`:

```bash
npm install --global pnpm@11.7.0
pnpm install --frozen-lockfile
pnpm run build
```

Commit dependency changes in `pnpm-lock.yaml`; use `pnpm add` or `pnpm install`
when updating dependencies. `pnpm-workspace.yaml` allows install scripts only
for the native dependencies listed there. Review that list when adding a package
that needs an install script. The release-age exceptions cover the tested
`eve@0.67.0` pin and the `@workflow/*` set `@workflow/world-testing@beta.55`
pulls in; review any new exception when updating Eve or the harness.

CI uses pnpm for repository dependencies, builds, and tests. Tarball consumer
checks and the E2E agent fixtures keep npm to match the production installation
path; releases also keep `npm publish` for trusted publishing. Run these commands
with pnpm available on `PATH`, since `prepack` invokes the pnpm build script.

## Before opening a pull request

Describe the problem, the resulting behavior, and how you verified the change.
Update the relevant guide or reference when behavior changes. Keep the root
README focused on user and developer quick starts.

Run the static checks from the repository root:

```bash
pnpm run fmt:check
pnpm run lint
pnpm run typecheck
pnpm run build
```

Run the relevant [test suites](./testing.md) against disposable PostgreSQL
databases. Changes to storage, queue delivery, or runtime compatibility should
exercise the corresponding integration, external conformance, or Eve E2E suite.

Use conventional commit messages, such as `fix:`, `feat:`, or `docs:`, because
they drive [release automation](./releasing.md). See
[Eve compatibility](./eve-compatibility.md) before updating runtime pins.
