import { describe, expect, test } from "vitest";
import { resolveConnectionString } from "./config.js";
import { RUNTIME_SECRET_ENV_NAMES, readRuntimeSecretFromEnv } from "./dispatch-contract.js";
import { resolveDispatcherConfig } from "./dispatcher/config.js";
import { resolveDispatchRuntimeSecret } from "./dispatcher/secrets.js";

/**
 * The two ends of this package run in different processes — the World inside a
 * tenant deployment, the dispatcher on the host — and they have to agree on the
 * names of the two things they share: the database and the dispatch secret.
 *
 * These are not hypothetical. An earlier draft gave the dispatcher a
 * `WORKFLOW_WORLD_*` namespace while leaving the deployment side on `EVELAND_*`,
 * which produced a dispatcher that started clean, passed every other test, and
 * then 401'd every single dispatch — and a second variant where the two ends
 * pointed at different databases, so the dispatcher polled a database nothing
 * wrote to and simply never delivered anything.
 */
describe("shared environment contract", () => {
  const baseDispatcherEnv = {
    WORKFLOW_DISPATCHER_ACTIVATION_API_URL: "http://127.0.0.1:4000",
  } satisfies NodeJS.ProcessEnv;

  describe("database URL", () => {
    // Each entry is a name a host might legitimately set on its own.
    for (const name of ["WORKFLOW_WORLD_URL", "EVELAND_WORKFLOW_WORLD_URL"] as const) {
      test(`${name} alone is honoured by BOTH ends`, () => {
        const env = { ...baseDispatcherEnv, [name]: "postgres://host/shared" };

        expect(resolveConnectionString(env)).toBe("postgres://host/shared");
        expect(resolveDispatcherConfig(env).worldUrl).toBe("postgres://host/shared");
      });
    }

    test("both ends prefer this package's own name over the legacy one", () => {
      const env = {
        ...baseDispatcherEnv,
        WORKFLOW_WORLD_URL: "postgres://host/canonical",
        EVELAND_WORKFLOW_WORLD_URL: "postgres://host/legacy",
      };

      expect(resolveConnectionString(env)).toBe("postgres://host/canonical");
      expect(resolveDispatcherConfig(env).worldUrl).toBe("postgres://host/canonical");
    });

    test("the dispatcher's bootstrap override outranks both, in either spelling", () => {
      // The host and the containers can reach one database by different
      // hostnames, so only the dispatcher takes the override — but it must still
      // be reading the same *variable* the deployment side falls back to.
      expect(
        resolveDispatcherConfig({
          ...baseDispatcherEnv,
          WORKFLOW_WORLD_URL: "postgres://host.docker.internal/shared",
          WORKFLOW_WORLD_BOOTSTRAP_URL: "postgres://localhost/shared",
        }).worldUrl,
      ).toBe("postgres://localhost/shared");

      expect(
        resolveDispatcherConfig({
          ...baseDispatcherEnv,
          EVELAND_WORKFLOW_WORLD_URL: "postgres://host.docker.internal/shared",
          EVELAND_WORKFLOW_WORLD_BOOTSTRAP_URL: "postgres://localhost/shared",
        }).worldUrl,
      ).toBe("postgres://localhost/shared");
    });

    test("neither end silently falls back to some other database", () => {
      const decoys = {
        ...baseDispatcherEnv,
        DATABASE_URL: "postgres://host/unrelated",
        WORKFLOW_POSTGRES_URL: "postgres://host/legacy-single-tenant",
      };

      expect(() => resolveConnectionString(decoys)).toThrow(/WORKFLOW_WORLD_URL is required/);
      expect(() => resolveDispatcherConfig(decoys)).toThrow(/WORKFLOW_WORLD_URL is required/);
    });
  });

  describe("dispatch runtime secret", () => {
    for (const name of RUNTIME_SECRET_ENV_NAMES) {
      test(`${name} alone is honoured by BOTH ends`, () => {
        // The deployment side reads the shared resolver directly in
        // `createQueueHandler`; the host side wraps it in the dev fallback.
        expect(readRuntimeSecretFromEnv({ [name]: "s3cret" })).toBe("s3cret");
        expect(resolveDispatchRuntimeSecret({ [name]: "s3cret" })).toBe("s3cret");
      });
    }

    test("precedence is this package's own name first", () => {
      const env = {
        WORKFLOW_WORLD_RUNTIME_SECRET: "canonical",
        EVELAND_SCHEDULER_RUNTIME_SECRET: "legacy",
      };

      expect(readRuntimeSecretFromEnv(env)).toBe("canonical");
      expect(resolveDispatchRuntimeSecret(env)).toBe("canonical");
    });

    test("an empty value does not shadow the next name", () => {
      expect(
        readRuntimeSecretFromEnv({
          WORKFLOW_WORLD_RUNTIME_SECRET: "",
          EVELAND_SCHEDULER_RUNTIME_SECRET: "legacy",
        }),
      ).toBe("legacy");
    });

    test("an unset NODE_ENV counts as production: no secret, no dev fallback", () => {
      // Fail closed. A host that forgot to configure the secret must not end up
      // guarding a privileged surface with a value published in this repository.
      expect(resolveDispatchRuntimeSecret({})).toBeUndefined();
      expect(resolveDispatchRuntimeSecret({ NODE_ENV: "production" })).toBeUndefined();
      expect(resolveDispatchRuntimeSecret({ NODE_ENV: "development" })).toBeTypeOf("string");
    });
  });
});
