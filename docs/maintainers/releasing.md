# Releasing

Releases are cut by [release-please](https://github.com/googleapis/release-please),
which reads the conventional-commit history on `main` and keeps one open pull
request holding the next version bump and its `CHANGELOG.md` entry. Nothing is
published while that PR sits there.

**Merging the release PR is the decision to release.** That merge tags the
commit, cuts a GitHub release, and only then does the publish job run. Publishing
on every merge to `main` would make cutting a version a side effect of merging a
fix, which is the wrong default for a package this young.

The publish itself uses npm
[trusted publishing](https://docs.npmjs.com/trusted-publishers/): the workflow
authenticates over OIDC, so there is no `NPM_TOKEN` in this repository to store,
rotate or leak. npm attaches a provenance attestation automatically, because the
repository and the package are both public.

Version bumps follow conventional commits, with `bump-minor-pre-major` set: while
below `1.0.0`, `feat:` and breaking changes both move the minor, and `fix:` moves
the patch.

## One-time setup

Trusted publishing cannot be configured for a package that does not exist yet, so
the first version has to be published by hand:

```bash
npm publish
```

Then, on npmjs.com, add a trusted publisher for the package — repository
`evelandhq/workflow-world`, workflow `release.yml` — and every release after that
comes from the pipeline with no credentials involved.
