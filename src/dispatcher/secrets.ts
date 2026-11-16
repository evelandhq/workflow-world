import { readRuntimeSecretFromEnv } from "../dispatch-contract.js";

/**
 * An explicit secret, or a published development value when NODE_ENV is
 * `development` or `test`. An unset NODE_ENV counts as production, so a host
 * that forgot to configure a secret fails to start rather than guarding a
 * privileged surface with a value published in a repository.
 */
export function resolveSecretWithDevFallback(
  env: NodeJS.ProcessEnv,
  explicitValue: string | undefined,
  developmentFallback: string,
): string | undefined {
  if (explicitValue) return explicitValue;
  return env.NODE_ENV === "development" || env.NODE_ENV === "test"
    ? developmentFallback
    : undefined;
}

/**
 * The dispatch secret the deployment side checks in `createQueueHandler`.
 *
 * The variable name comes from the contract module, which is the single place
 * that defines it, so this end and the deployment end cannot drift apart.
 */
export function resolveDispatchRuntimeSecret(env: NodeJS.ProcessEnv): string | undefined {
  return resolveSecretWithDevFallback(
    env,
    readRuntimeSecretFromEnv(env),
    "eveland-dev-scheduler-runtime-secret",
  );
}
