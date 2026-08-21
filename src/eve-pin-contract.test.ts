import { createRequire } from "node:module";
import { readFileSync } from "node:fs";
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
 * the installed `eve` package rather than written down. That way an eve bump is a
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

describe("@workflow/* pins track the installed eve", () => {
  // Resolved from the package that declares it. pnpm and npm both nest, and
  // resolving from the repo root can walk out of the checkout entirely.
  const evePath = require.resolve("eve/package.json", { paths: [repoRoot] });
  const eve = readJson(evePath);
  const eveWorkflowPins = versionsFrom(eve);
  const ours = versionsFrom(readJson(path.join(repoRoot, "package.json")));

  test("the installed eve is the one this package claims to target", () => {
    expect(typeof eve.version).toBe("string");
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
    // it ships its *own* runtime bundle and tracks a separate release line. That
    // line currently trails ours (beta.42 pins @workflow/world beta.27 while
    // eve 0.42.0 installs beta.28), which is fine for the same reason agreeing
    // was: the pairing is a coincidence of timing, not a constraint — the
    // harness releases on its own cadence and has sat on either side of eve's
    // pin before.
    // Forcing it onto eve's set would mean testing against a runtime eve does not
    // ship; picking up its own next release, which is what an eve bump here does,
    // is not the same thing and nothing enforces the outcome either way.
    // The agreement is verified rather than assumed: conformance/spec.test.mts
    // asserts our specVersion equals the one the harness runtime demands, which is
    // the property that actually matters and the one that survives the harness
    // drifting ahead again and re-nesting its own @workflow/world in the tree.
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
