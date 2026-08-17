import { AsyncLocalStorage } from "node:async_hooks";
import {
  RUN_RETENTION_ATTRIBUTE,
  type RunRetentionClass,
  resolveRunRetentionClass,
} from "./run-retention-policy.js";

export const RUN_RETENTION_CONTEXT_SYMBOL_KEY = "@evelandhq/workflow-world.run-retention-intent";

const ROOT_RUN_ID_ATTRIBUTE = "$rootRunId";
const PARENT_RUN_ID_ATTRIBUTE = "$parentRunId";
const runRetentionContextSymbol = Symbol.for(RUN_RETENTION_CONTEXT_SYMBOL_KEY);

type RunRetentionIntent = {
  retentionClass: RunRetentionClass;
};

type RunRetentionClassForCreationInput = {
  retentionClass?: string;
  attributes?: Record<string, string>;
  getAncestorRetentionClass?: (runId: string) => Promise<RunRetentionClass | undefined>;
};

export type RunRetentionIdentity = {
  retentionClass: RunRetentionClass;
  retentionRootRunId: string;
};

type RunRetentionForCreationInput = {
  runId: string;
  retentionClass?: string;
  attributes?: Record<string, string>;
  getAncestorRetention?: (runId: string) => Promise<RunRetentionIdentity | undefined>;
};

export function withRunRetentionIntent<T>(
  retentionClass: RunRetentionClass,
  operation: () => T,
): T {
  return getRunRetentionContext().run({ retentionClass }, operation);
}

export async function resolveRunRetentionClassForCreation(
  input: RunRetentionClassForCreationInput,
): Promise<RunRetentionClass> {
  const attributeClass = input.attributes?.[RUN_RETENTION_ATTRIBUTE];
  if (input.retentionClass !== undefined || attributeClass !== undefined) {
    return resolveRunRetentionClass(input.retentionClass, input.attributes);
  }

  const rootRunId = input.attributes?.[ROOT_RUN_ID_ATTRIBUTE];
  const parentRunId = input.attributes?.[PARENT_RUN_ID_ATTRIBUTE];
  if (rootRunId !== undefined || parentRunId !== undefined) {
    if (input.getAncestorRetentionClass !== undefined) {
      for (const runId of new Set([rootRunId, parentRunId])) {
        if (runId === undefined) continue;
        const retentionClass = await input.getAncestorRetentionClass(runId);
        if (retentionClass !== undefined) return retentionClass;
      }
    }
    throw new TypeError(
      `Workflow run retention lineage could not resolve root ${JSON.stringify(rootRunId)} or parent ${JSON.stringify(parentRunId)}.`,
    );
  }

  const ambientIntent = getRunRetentionContext().getStore();
  if (ambientIntent !== undefined) return ambientIntent.retentionClass;

  return "interactive";
}

export async function resolveRunRetentionForCreation(
  input: RunRetentionForCreationInput,
): Promise<RunRetentionIdentity> {
  const rootRunId = input.attributes?.[ROOT_RUN_ID_ATTRIBUTE];
  const parentRunId = input.attributes?.[PARENT_RUN_ID_ATTRIBUTE];
  const ancestors = new Map<string, RunRetentionIdentity>();

  if (input.getAncestorRetention !== undefined) {
    for (const runId of new Set([rootRunId, parentRunId])) {
      if (runId === undefined) continue;
      const ancestor = await input.getAncestorRetention(runId);
      if (ancestor !== undefined) ancestors.set(runId, ancestor);
    }
  }

  const retentionClass = await resolveRunRetentionClassForCreation({
    retentionClass: input.retentionClass,
    attributes: input.attributes,
    getAncestorRetentionClass: async (runId) => ancestors.get(runId)?.retentionClass,
  });
  const ancestor = [...ancestors.values()][0];

  return {
    retentionClass,
    retentionRootRunId: ancestor?.retentionRootRunId ?? rootRunId ?? parentRunId ?? input.runId,
  };
}

function getRunRetentionContext(): AsyncLocalStorage<RunRetentionIntent> {
  const existing = Reflect.get(globalThis, runRetentionContextSymbol);
  if (existing instanceof AsyncLocalStorage) {
    return existing as AsyncLocalStorage<RunRetentionIntent>;
  }
  const created = new AsyncLocalStorage<RunRetentionIntent>();
  Reflect.set(globalThis, runRetentionContextSymbol, created);
  return created;
}
