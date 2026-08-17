import cp from "node:child_process";
import fs from "node:fs";
import path from "node:path";

const here = import.meta.dirname;
export const repoRoot = path.resolve(here, "..");
const workRoot = path.join(here, ".work");

/** Where a given eve version's agent project is built. */
export function agentDir(eveVersion: string): string {
  return path.join(workRoot, `eve-${eveVersion}`);
}

export function tenantFor(eveVersion: string): string {
  // Tenant ids are validated; keep it to the safe alphabet.
  return `prj_e2e_${eveVersion.replace(/\./g, "_")}`;
}

export function databaseFor(eveVersion: string): string {
  return `wfw_e2e_${eveVersion.replace(/\./g, "_")}`;
}

export function deploymentFor(eveVersion: string): string {
  return `dep_e2e_${eveVersion.replace(/\./g, "_")}`;
}

function run(command: string, cwd: string, extraEnv: Record<string, string> = {}): void {
  cp.execSync(command, {
    cwd,
    stdio: "pipe",
    env: { ...process.env, ...extraEnv },
    maxBuffer: 64 * 1024 * 1024,
  });
}

/**
 * Packs this package once and returns the tarball path.
 *
 * The tarball, not the workspace source: `exports` points at `dist/`, so
 * installing the packed artifact is what a consumer actually resolves. A
 * workspace link would test a different code path.
 */
export function packWorld(): string {
  fs.mkdirSync(workRoot, { recursive: true });
  for (const stale of fs.readdirSync(workRoot).filter((f) => f.endsWith(".tgz"))) {
    fs.rmSync(path.join(workRoot, stale));
  }
  run(`npm pack --pack-destination ${JSON.stringify(workRoot)}`, repoRoot);
  const tarball = fs.readdirSync(workRoot).find((f) => f.endsWith(".tgz"));
  if (!tarball) throw new Error("npm pack produced no tarball");
  return path.join(workRoot, tarball);
}

/**
 * Materialises and builds a real eve agent for one eve version.
 *
 * Deliberately a full `eve build`, not `eve dev`: the built server is what a
 * deployment runs, and the build is where eve resolves and bundles the World.
 */
export function buildAgent(input: { eveVersion: string; worldTarball: string }): string {
  const dir = agentDir(input.eveVersion);
  fs.rmSync(dir, { recursive: true, force: true });
  fs.mkdirSync(dir, { recursive: true });

  fs.cpSync(path.join(here, "fixture"), dir, { recursive: true });
  fs.writeFileSync(
    path.join(dir, "package.json"),
    `${JSON.stringify(
      {
        name: `wfw-e2e-${input.eveVersion.replace(/\./g, "-")}`,
        private: true,
        version: "0.0.0",
        type: "commonjs",
        dependencies: { eve: input.eveVersion },
      },
      null,
      2,
    )}\n`,
  );

  run("npm install --no-audit --no-fund", dir);
  // Same install flags Eveland's injector uses, so the World lands the same way.
  run(
    `npm install --no-save --package-lock=false --ignore-scripts ${JSON.stringify(input.worldTarball)}`,
    dir,
  );
  run("npx eve build", dir);
  return dir;
}

/** The eve version actually installed in a built agent — checked, not assumed. */
export function installedEveVersion(dir: string): string {
  const manifest = JSON.parse(
    fs.readFileSync(path.join(dir, "node_modules", "eve", "package.json"), "utf8"),
  ) as { version: string };
  return manifest.version;
}

/** The World version actually installed, likewise. */
export function installedWorldVersion(dir: string): string {
  const manifest = JSON.parse(
    fs.readFileSync(
      path.join(dir, "node_modules", "@evelandhq", "workflow-world", "package.json"),
      "utf8",
    ),
  ) as { version: string };
  return manifest.version;
}

export type StartedAgent = {
  port: number;
  stop: () => Promise<void>;
  log: () => string;
};

/**
 * Starts the built agent and waits for it to listen.
 *
 * Env is the deployment side of this package's contract: the World reads the
 * database, tenant, deployment, runner mode and dispatch secret from it. The
 * tenant must already be provisioned — there is no DEFAULT partition, so an
 * unprovisioned tenant fails loudly on first write rather than pooling rows.
 */
export async function startAgent(input: {
  dir: string;
  port: number;
  databaseUrl: string;
  tenantId: string;
  deploymentId: string;
}): Promise<StartedAgent> {
  let output = "";
  const proc = cp.spawn("npx", ["eve", "start", "--port", String(input.port)], {
    cwd: input.dir,
    stdio: ["ignore", "pipe", "pipe"],
    env: {
      ...process.env,
      PORT: String(input.port),
      WORKFLOW_WORLD_URL: input.databaseUrl,
      WORKFLOW_WORLD_TENANT_ID: input.tenantId,
      WORKFLOW_WORLD_DEPLOYMENT_ID: input.deploymentId,
      WORKFLOW_WORLD_RUNNER: "embedded",
      WORKFLOW_WORLD_RUNTIME_SECRET: "e2e-runtime-secret",
    },
  });

  const collect = (chunk: Buffer) => {
    output += chunk.toString();
  };
  proc.stdout?.on("data", collect);
  proc.stderr?.on("data", collect);

  const deadline = Date.now() + 90_000;
  let listening = false;
  while (Date.now() < deadline) {
    if (proc.exitCode !== null) break;
    if (/server listening at|Listening on:/i.test(output)) {
      listening = true;
      break;
    }
    await new Promise((resolve) => setTimeout(resolve, 250));
  }
  if (!listening) {
    proc.kill("SIGKILL");
    throw new Error(`agent for ${input.dir} never listened. Output:\n${output.slice(-3000)}`);
  }

  return {
    port: input.port,
    log: () => output,
    async stop() {
      if (proc.exitCode !== null) return;
      proc.kill("SIGTERM");
      await new Promise<void>((resolve) => {
        proc.once("exit", () => resolve());
        setTimeout(() => {
          proc.kill("SIGKILL");
          resolve();
        }, 8_000).unref();
      });
    },
  };
}

/**
 * Drives one real agent turn.
 *
 * No model credentials are needed or supplied. The turn's model call fails with
 * an AI Gateway auth error, and that is fine — by then eve has already created
 * its durable runs, steps, hooks and waits through the World, which is what this
 * suite is about. Requiring credentials would make the suite unrunnable in CI for
 * no extra coverage of *this* package.
 */
export async function startSession(port: number): Promise<{ sessionId: string }> {
  const response = await fetch(`http://127.0.0.1:${String(port)}/eve/v1/session`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ message: "hello from the e2e harness" }),
  });
  if (!response.ok) {
    throw new Error(`session create failed: ${String(response.status)} ${await response.text()}`);
  }
  return (await response.json()) as { sessionId: string };
}

/** Drives the same real Eve graph from a platform-scheduled root context. */
export async function startScheduledSession(port: number): Promise<{ sessionId: string }> {
  const response = await fetch(`http://127.0.0.1:${String(port)}/e2e/scheduled`, {
    method: "POST",
  });
  if (!response.ok) {
    throw new Error(
      `scheduled session create failed: ${String(response.status)} ${await response.text()}`,
    );
  }
  return (await response.json()) as { sessionId: string };
}

/** Proves generic lineage inheritance for the explicit durable class. */
export async function startPersistentSession(port: number): Promise<{ sessionId: string }> {
  const response = await fetch(`http://127.0.0.1:${String(port)}/e2e/persistent`, {
    method: "POST",
  });
  if (!response.ok) {
    throw new Error(
      `persistent session create failed: ${String(response.status)} ${await response.text()}`,
    );
  }
  return (await response.json()) as { sessionId: string };
}

/** Creates an interactive owner, then delivers to it from a scheduled context. */
export async function startScheduledTurnOnInteractiveSession(
  port: number,
): Promise<{ sessionId: string; scheduledSessionId: string }> {
  const response = await fetch(`http://127.0.0.1:${String(port)}/e2e/preserve-interactive`, {
    method: "POST",
  });
  if (!response.ok) {
    throw new Error(
      `existing session delivery failed: ${String(response.status)} ${await response.text()}`,
    );
  }
  return (await response.json()) as { sessionId: string; scheduledSessionId: string };
}
