# Community World submission

Proposed upstream PR title: `docs: add Eveland to community Worlds`.

- `entry.json`: complete proposed entry, including environment, Postgres service,
  and schema / tenant initialization.
- `worlds-manifest.patch`: adds only that entry to upstream's root manifest.
- `pr-body.md`: submission text with precise versions and existing CI evidence.

Merge the standalone Quick Start into this repository before submitting so the
public README contains the onboarding path. Recheck upstream's manifest before
applying the patch with `git apply /path/to/worlds-manifest.patch` in a checkout
of `vercel/workflow`. No upstream workflow changes are included.

The manifest's setup command resolves `pg` relative to the World package so it
also works when pnpm does not expose transitive dependencies at the repository
root. Upstream CI currently does not execute this field; its hardcoded setup
allowlist needs a separate coordinated change when community testing resumes.

Sources checked on 2026-09-10:

- [Official submission instructions](https://github.com/vercel/workflow/blob/main/packages/workflow/README.md#run-anywhere)
- [Worlds manifest](https://github.com/vercel/workflow/blob/main/worlds-manifest.json)
- [Community E2E workflow](https://github.com/vercel/workflow/blob/main/.github/workflows/e2e-community-world.yml)
