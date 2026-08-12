/**
 * Has a newer eve started installing a different `@workflow/*` set than we pin?
 *
 * `src/eve-pin-contract.test.ts` holds the pins against the eve that is
 * *installed*, so it can only ever notice a mismatch someone already created by
 * bumping the devDependency. This is the other direction: it asks the registry
 * whether any eve released since our pin would force that bump, and stays quiet
 * when the answer is no.
 *
 * Quiet is the point. eve ships often and almost none of it reaches this
 * package — across 0.29.0 to 0.31.3 the `@workflow/*` set moved twice, at 0.30.0
 * and at 0.31.2, sitting still through roughly a dozen releases in between.
 * A check that fired on every eve release would be noise nobody reads; this one
 * fires roughly once per eve minor line, which is the real cadence of work here.
 *
 * Note that the 0.31.2 move landed mid-line, not on 0.31.0 — which is why this
 * reports an exact version rather than a line, and why nothing here should infer
 * a set from a minor.
 *
 *   node scripts/eve-pin-drift.mjs
 *
 * Exit 0 means nothing to do. Exit 1 means a newer eve moved the set, and the
 * report names which packages and from which version. Exit 2 is the check
 * itself failing (network, malformed registry data) — distinguished from drift
 * so a flaky fetch cannot be mistaken for a finding.
 *
 * ## Why the registry and not Eveland's compatibility policy
 *
 * The authority on what may actually be deployed is
 * `packages/core/src/eve-compatibility.ts` in the Eveland repository, and the
 * honest version of this check would read it. It cannot: that repository is
 * private and this one is public, so reaching it would mean giving a public
 * repo's scheduled job a cross-repo credential — a large amount of standing
 * access bought for a weekly convenience.
 *
 * The registry is the weaker but sufficient signal. Eveland's window only ever
 * verifies versions that already exist on npm, so a `@workflow/*` move always
 * shows up here first. Reporting it early is the useful direction to be wrong
 * in: the finding is "a future window will need this", not "the window has
 * moved", and the report says so rather than pretending to know.
 */

import { readFileSync } from "node:fs";

const REGISTRY_URL = "https://registry.npmjs.org/eve";

/** Exit codes, named so the workflow can tell drift from breakage. */
const CLEAN = 0;
const DRIFT = 1;
const BROKEN = 2;

function fail(message) {
  console.error(`eve-pin-drift: ${message}`);
  process.exit(BROKEN);
}

/**
 * The `@workflow/*` pins a manifest declares.
 *
 * Deliberately identical to `versionsFrom` in `src/eve-pin-contract.test.ts`,
 * including first-declaration-wins: eve puts the real pins in one section and a
 * looser peer range for the same package must not overwrite them. If the two
 * ever disagree, this check and the test would report different worlds.
 */
function workflowPinsFrom(manifest) {
  const merged = {};
  for (const field of ["dependencies", "devDependencies", "peerDependencies"]) {
    const section = manifest?.[field];
    if (!section || typeof section !== "object") continue;
    for (const [name, range] of Object.entries(section)) {
      if (name.startsWith("@workflow/") && !(name in merged)) merged[name] = range;
    }
  }
  return merged;
}

/** Stable releases only. A prerelease is not something the platform can verify. */
function isStable(version) {
  return /^\d+\.\d+\.\d+$/.test(version);
}

function compareVersions(a, b) {
  const [aMajor, aMinor, aPatch] = a.split(".").map(Number);
  const [bMajor, bMinor, bPatch] = b.split(".").map(Number);
  return aMajor - bMajor || aMinor - bMinor || aPatch - bPatch;
}

/**
 * What moved between the eve we pin and a newer one, restricted to packages we
 * actually depend on.
 *
 * The comparison is against the *pinned eve's* set rather than against our own
 * declared versions, and that distinction is what keeps the check quiet. Our
 * manifest also carries `@workflow/world-testing`, which eve has never installed
 * — it is the conformance harness, and `src/eve-pin-contract.test.ts` exempts it
 * by name. Comparing absolutely would read that permanent, intended difference
 * as a finding on every single run, which is how a check earns its way into
 * being ignored. Asking "did anything *move*" instead makes the exemption
 * irrelevant here, so there is no second copy of the allowlist to keep in step.
 *
 * A package the pinned eve installs and a newer one does not is drift too: it
 * means a new exemption is needed, or the dependency is stranded.
 */
function diffAgainstBase(ourPins, basePins, newPins) {
  const moved = [];
  const dropped = [];
  for (const name of Object.keys(ourPins)) {
    const base = basePins[name];
    if (base === undefined) continue; // never eve's to pin; see above
    const next = newPins[name];
    if (next === undefined) {
      dropped.push(name);
    } else if (next !== base) {
      moved.push({ name, ours: base, theirs: next });
    }
  }
  return { moved, dropped };
}

let ourManifest;
try {
  ourManifest = JSON.parse(readFileSync(new URL("../package.json", import.meta.url), "utf8"));
} catch (error) {
  fail(`cannot read this package's manifest: ${error.message}`);
}

const ourPins = workflowPinsFrom(ourManifest);
const ourEve = ourManifest.devDependencies?.eve;

if (!ourEve) fail("this package does not declare an `eve` devDependency");
if (!isStable(ourEve)) fail(`the pinned eve is not an exact stable version: ${ourEve}`);
if (Object.keys(ourPins).length === 0) fail("this package declares no @workflow/* dependencies");

/**
 * The full packument, not the abbreviated one: eve declares `@workflow/*` in
 * `devDependencies`, which the `install-v1` document omits entirely. Asking for
 * the abbreviated form would silently produce an empty set for every version
 * and report a clean run forever.
 */
const response = await fetch(REGISTRY_URL, { headers: { accept: "application/json" } }).catch(
  (error) => fail(`registry request failed: ${error.message}`),
);
if (!response.ok) fail(`registry returned ${response.status} ${response.statusText}`);

const packument = await response
  .json()
  .catch((error) => fail(`malformed packument: ${error.message}`));
const allVersions = Object.keys(packument?.versions ?? {});
if (allVersions.length === 0) fail("packument carried no versions");

/**
 * The pinned eve has to still be on the registry for the comparison to have a
 * baseline. If it were ever unpublished, silently falling back to an empty set
 * would make every package look "dropped" — loud, and about the wrong thing.
 */
const baseManifest = packument.versions[ourEve];
if (!baseManifest) fail(`the pinned eve ${ourEve} is not on the registry`);
const basePins = workflowPinsFrom(baseManifest);
if (Object.keys(basePins).length === 0) {
  fail(`eve ${ourEve} declares no @workflow/* packages; the comparison would be vacuous`);
}

const newer = allVersions
  .filter(isStable)
  .filter((version) => compareVersions(version, ourEve) > 0)
  .sort(compareVersions);

if (newer.length === 0) {
  console.log(`eve-pin-drift: nothing newer than the pinned eve ${ourEve}. Nothing to do.`);
  process.exit(CLEAN);
}

/**
 * Report the *earliest* newer release that moved the set, not the newest one.
 * The earliest is the actionable fact — it is the version a future compatibility
 * window would start at, and every release after it carries the same set until
 * something moves again.
 */
let firstMoved;
for (const version of newer) {
  const { moved, dropped } = diffAgainstBase(
    ourPins,
    basePins,
    workflowPinsFrom(packument.versions[version]),
  );
  if (moved.length > 0 || dropped.length > 0) {
    firstMoved = { version, moved, dropped };
    break;
  }
}

const newest = newer.at(-1);

if (!firstMoved) {
  console.log(
    `eve-pin-drift: eve is at ${newest}; we pin ${ourEve}. ` +
      `All ${newer.length} newer release(s) install the same @workflow/* set we do. Nothing to do.`,
  );
  process.exit(CLEAN);
}

const lines = [
  `eve ${firstMoved.version} is the first release since our pinned ${ourEve} to install a`,
  `different \`@workflow/*\` set. eve's newest is ${newest}.`,
  "",
];

if (firstMoved.moved.length > 0) {
  lines.push("Moved:", "");
  lines.push("| package | ours | eve |", "| --- | --- | --- |");
  for (const { name, ours, theirs } of firstMoved.moved) {
    lines.push(`| \`${name}\` | ${ours} | **${theirs}** |`);
  }
  lines.push("");
}

if (firstMoved.dropped.length > 0) {
  lines.push(
    "No longer installed by eve (needs an exemption in `src/eve-pin-contract.test.ts`,",
    "or the dependency is stranded):",
    "",
    ...firstMoved.dropped.map((name) => `- \`${name}\``),
    "",
  );
}

lines.push(
  "This is a heads-up, not a deadline. Nothing here can be deployed until Eveland's",
  "`packages/core/src/eve-compatibility.ts` verifies a version on that line — check it",
  "before bumping, and pin the version *it* verifies rather than npm's `latest`.",
  "",
  "When it is time: bump the `eve` devDependency, run `npm install`, and let",
  "`src/eve-pin-contract.test.ts` name the packages that have to move with it.",
  "Confirm `specVersion` is unchanged — eve compiles the runtime's check into each",
  "release, as literal equality through `@workflow/core` beta.40 and as a",
  "floor-and-ceiling range from beta.41 on, and the floor still rejects a World",
  "pinned behind the runtime. Then run `npm run test:e2e`, which is the only thing",
  "here that loads a real eve.",
);

console.log(lines.join("\n"));
process.exit(DRIFT);
