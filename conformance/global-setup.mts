import cp from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import { Pool } from "pg";
import { DISPATCHER_READY_TOKEN } from "../src/dispatcher/main.js";
import { ensureTenantPartitions, runMigrations } from "../src/index.js";
import { PACKAGE_NAME, resolveConformanceDatabaseUrl, TENANT_ID } from "./env.mts";

const here = import.meta.dirname;
const repoRoot = path.resolve(here, "..");

/**
 * eve resolves a World by importing `WORKFLOW_TARGET_WORLD` as a bare specifier,
 * so the package has to be resolvable by its own name from inside this repo.
 * A self-link is the honest way to do that: it resolves through the real
 * `exports` map, which points at `dist/` — exactly what a consumer installing
 * from npm would get.
 */
function linkSelf(): void {
  const [scope, name] = PACKAGE_NAME.split("/");
  const scopeDir = path.join(repoRoot, "node_modules", scope!);
  const linkPath = path.join(scopeDir, name!);
  fs.mkdirSync(scopeDir, { recursive: true });
  if (fs.existsSync(linkPath) || fs.lstatSync(linkPath, { throwIfNoEntry: false })) {
    fs.rmSync(linkPath, { recursive: true, force: true });
  }
  fs.symlinkSync(repoRoot, linkPath, "dir");
}

function assertBuilt(): void {
  const entry = path.join(repoRoot, "dist", "index.js");
  if (!fs.existsSync(entry)) {
    throw new Error(
      `dist/ is not built (${entry} is missing). The conformance harness runs the published resolution, so run \`npm run build\` first.`,
    );
  }
}

type Service = { name: string; proc: cp.ChildProcess; ready: Promise<void> };

function spawnService(name: string, script: string, readyToken: string): Service {
  const proc = cp.spawn(process.execPath, [script], {
    cwd: here,
    stdio: ["ignore", "pipe", "pipe"],
    env: process.env,
  });

  let settle!: (error?: Error) => void;
  const ready = new Promise<void>((resolve, reject) => {
    settle = (error) => (error ? reject(error) : resolve());
  });

  const onChunk = (chunk: Buffer) => {
    const text = chunk.toString();
    // Forwarded so a failure in CI is diagnosable from the vitest log alone.
    process.stdout.write(text);
    if (text.includes(readyToken)) settle();
  };
  proc.stdout?.on("data", onChunk);
  proc.stderr?.on("data", onChunk);
  proc.once("exit", (code, signal) => {
    settle(
      new Error(
        `${name} exited before becoming ready (code=${String(code)} signal=${String(signal)})`,
      ),
    );
  });
  const timeout = setTimeout(() => {
    settle(
      new Error(
        `${name} never printed its readiness token ${JSON.stringify(readyToken)} within 60s`,
      ),
    );
  }, 60_000);
  timeout.unref();

  return { name, proc, ready };
}

export default async function setup() {
  assertBuilt();
  linkSelf();

  const admin = new Pool({ connectionString: resolveConformanceDatabaseUrl(), max: 2 });
  try {
    await runMigrations(admin, {
      log: (message) => console.log(`[conformance] migrate: ${message}`),
    });
    await ensureTenantPartitions(admin, TENANT_ID);
  } finally {
    await admin.end();
  }

  const stub = spawnService(
    "stub-activation-api",
    "stub-activation-api.mjs",
    "[stub-activation-api] ready",
  );
  const dispatcher = spawnService("dispatcher", "dispatcher-boot.mjs", DISPATCHER_READY_TOKEN);
  await stub.ready;
  await dispatcher.ready;
  console.log("[conformance] stub activation api + dispatcher are up");

  return async () => {
    // SIGTERM, not SIGKILL: the dispatcher's graceful shutdown is part of what
    // this harness covers, and the stub prints its request tally on the way out.
    for (const service of [dispatcher, stub]) {
      if (service.proc.exitCode === null) service.proc.kill("SIGTERM");
    }
    await Promise.all(
      [dispatcher, stub].map(
        (service) =>
          new Promise<void>((resolve) => {
            if (service.proc.exitCode !== null) return resolve();
            service.proc.once("exit", () => resolve());
            setTimeout(() => {
              service.proc.kill("SIGKILL");
              resolve();
            }, 5_000).unref();
          }),
      ),
    );
  };
}
