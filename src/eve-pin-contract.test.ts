import { createRequire } from "node:module";
import { existsSync, readFileSync } from "node:fs";
import path from "node:path";
import { describe, expect, test } from "vitest";

/**
 * This package's `@workflow/*` versions must be exactly the ones the pinned `eve`
 * release installs.
 *
 * Not a style preference — it is the only pairing that can actually be deployed.
 * A World runs inside an eve executor, and eve validates the World's declared
 * `@workflow/*` line and its `specVersion` at runtime. Neither check is a type
 * error, so drift shows up as a deploy-time failure on a real project rather than
 * here.
 *
 * The rule is "follow what eve installs", so the expected values are read out of
 * the installed `eve` package rather than written down. eve 1.0 no longer
 * installs them as dependencies: it bundles the Workflow runtime into
 * `dist/src/compiled/@workflow` and records each bundled version in
 * `dist/src/compiled/.vendor-stamp.json`, so that stamp is what is read. That way an eve bump is a
 * one-line devDependency change and this test tells us exactly which packages
 * have to move with it — and it cannot be satisfied by copying numbers from
 * `vercel/eve@main`, which runs ahead of the released tarball.
 */
const require = createRequire(import.meta.url);
const repoRoot = path.resolve(import.meta.dirname, "..");

function readJson(file: string): Record<string, unknown> {
  return JSON.parse(readFileSync(file, "utf8")) as Record<string, unknown>;
}

function versionsFrom(manifest: Record<string, unknown>): Record<string, string> {
  const merged: Record<string, string> = {};
  for (const field of ["dependencies", "devDependencies", "peerDependencies"] as const) {
    const section = manifest[field];
    if (!section || typeof section !== "object") continue;
    for (const [name, range] of Object.entries(section as Record<string, string>)) {
      // First declaration wins: eve puts the real pins in `dependencies`, and a
      // peer range for the same package must not overwrite them.
      if (name.startsWith("@workflow/") && !(name in merged)) merged[name] = range;
    }
  }
  return merged;
}

/**
 * eve names the packages it bundles, not what those pull in. `@workflow/core`
 * requires `@workflow/utils`, and eve stopped listing utils itself in 0.66, so
 * a World that calls utils directly has to read the pairing one level down:
 * the manifest of eve's own `@workflow/core` names the utils that runtime was
 * built with. Only a package installed at exactly eve's version is consulted,
 * so a drifted copy cannot smuggle its own pins in; eve's own declarations win
 * where both name a package.
 */
function withBundledRuntimePins(pins: Record<string, string>): Record<string, string> {
  const merged = { ...pins };
  for (const [name, version] of Object.entries(pins)) {
    const manifestPath = path.join(repoRoot, "node_modules", name, "package.json");
    if (!existsSync(manifestPath)) continue;
    const manifest = readJson(manifestPath);
    if (manifest.version !== version) continue;
    const dependencies = manifest.dependencies;
    if (!dependencies || typeof dependencies !== "object") continue;
    for (const [dep, range] of Object.entries(dependencies as Record<string, string>)) {
      if (dep.startsWith("@workflow/") && !(dep in merged)) merged[dep] = range;
    }
  }
  return merged;
}

/**
 * The `@workflow/*` versions eve 1.0 bundles, from its vendor stamp. A bundled
 * package is not installed, so nothing resolves it one level down: the stamp
 * names every package in the set, `@workflow/utils` included.
 */
function bundledWorkflowVersions(eveRoot: string): Record<string, string> {
  const stampPath = path.join(eveRoot, "dist", "src", "compiled", ".vendor-stamp.json");
  if (!existsSync(stampPath)) return {};
  const moduleVersions = readJson(stampPath).moduleVersions;
  if (!moduleVersions || typeof moduleVersions !== "object") return {};
  return Object.fromEntries(
    Object.entries(moduleVersions as Record<string, string>).filter(([name]) =>
      name.startsWith("@workflow/"),
    ),
  );
}

describe("@workflow/* pins track the installed eve", () => {
  // Resolved from the package that declares it. pnpm and npm both nest, and
  // resolving from the repo root can walk out of the checkout entirely.
  const evePath = require.resolve("eve/package.json", { paths: [repoRoot] });
  const eve = readJson(evePath);
  const eveWorkflowPins = {
    ...withBundledRuntimePins(versionsFrom(eve)),
    ...bundledWorkflowVersions(path.dirname(evePath)),
  };
  const manifest = readJson(path.join(repoRoot, "package.json"));
  const ours = versionsFrom(manifest);

  test("the installed eve is the one this package claims to target", () => {
    const evePin = (manifest.devDependencies as Record<string, unknown>).eve;
    // The registry drift check needs an exact version as its baseline.
    expect(evePin).toMatch(/^\d+\.\d+\.\d+$/);
    expect(eve.version).toBe(evePin);
    // Sanity: if eve stopped declaring @workflow/* at all, every assertion below
    // would vacuously pass.
    expect(Object.keys(eveWorkflowPins).length).toBeGreaterThan(3);
  });

  test("every @workflow package we depend on is pinned to eve's exact version", () => {
    const mismatches: string[] = [];
    for (const [name, ourRange] of Object.entries(ours)) {
      const eveRange = eveWorkflowPins[name];
      if (eveRange === undefined) continue; // ours to choose; see the next test
      if (ourRange !== eveRange) {
        mismatches.push(`${name}: ours ${ourRange}, eve ${eveRange}`);
      }
    }
    expect(mismatches).toEqual([]);
  });

  test("we pin exact versions, never ranges", () => {
    // A caret would let a resolution drift away from eve's without any signal.
    const ranged = Object.entries(ours)
      .filter(([, range]) => /^[\^~><=*]/.test(range))
      .map(([name, range]) => `${name}: ${range}`);
    expect(ranged).toEqual([]);
  });

  /**
   * Packages allowed to sit outside eve's set, with the reason. Anything not
   * listed here is skew waiting to happen, because it resolves independently of
   * eve's tree.
   */
  const EXEMPT_FROM_EVE_PINS: Record<string, string> = {
    // The conformance harness, and deliberately not part of the runtime pairing:
    // it ships its *own* runtime bundle and tracks a separate release line, so
    // agreeing with eve's set is a coincidence of timing, not a constraint.
    // Forcing it onto eve's set would mean testing against a runtime eve does
    // not ship. What matters is verified rather than assumed:
    // conformance/spec.test.mts asserts our specVersion lies in the range the
    // harness runtime accepts, resolved from the harness's own
    // `@workflow/world`. The real-eve e2e is what exercises the pinned core.
    "@workflow/world-testing": "test-only harness; ships its own runtime bundle",
  };

  test("we do not depend on a @workflow package eve does not install", () => {
    const unknown = Object.keys(ours).filter(
      (name) => !(name in eveWorkflowPins) && !(name in EXEMPT_FROM_EVE_PINS),
    );
    expect(unknown).toEqual([]);
  });

  test("every exemption is still actually needed", () => {
    // Keeps the allowlist honest: once eve installs one of these, the exemption
    // has to go so the pin is enforced again.
    const stale = Object.keys(EXEMPT_FROM_EVE_PINS).filter(
      (name) => name in eveWorkflowPins || !(name in ours),
    );
    expect(stale).toEqual([]);
  });
});

test("the CI matrix passes its Eve version into the E2E suite", () => {
  const workflow = readFileSync(path.join(repoRoot, ".github/workflows/ci.yml"), "utf8");
  expect(workflow).toContain("EVE_VERSION: ${{ matrix.eve }}");
});
