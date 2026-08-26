import type { OperationDefinition, RuntimeDescriptor } from "@queryn/types";
import { validateJsonSchema } from "./schema.js";

export interface RegisteredOperation {
  definition: OperationDefinition;
  extensionId: string;
  extensionVersion?: string;
  runtime?: RuntimeDescriptor;
  artifactMediaTypes?: Record<string, string[]>;
}

export type BuiltinOperationHandler = (input: {
  projectPath: string;
  sessionId?: string;
  jobId: string;
  arguments: Record<string, unknown>;
  outboxPath: string;
  signal: AbortSignal;
  progress(value: number, message?: string): void;
  provenance: { runId: string; model?: string };
  recipient?: "local" | "cloud";
}) => Promise<Record<string, unknown>>;

export class OperationRegistry {
  readonly #operations = new Map<string, Map<string, RegisteredOperation>>();
  readonly #builtinHandlers = new Map<string, BuiltinOperationHandler>();

  register(operation: RegisteredOperation, handler?: BuiltinOperationHandler): void {
    const key = operation.extensionId === "queryn.builtin" ? "builtin" : operation.extensionVersion;
    if (!key) throw new Error(`Extension version is required for ${operation.definition.id}.`);
    const versions = this.#operations.get(operation.definition.id) ?? new Map<string, RegisteredOperation>();
    if (versions.has(key)) throw new Error(`Operation already registered: ${operation.definition.id}@${key}`);
    versions.set(key, operation);
    this.#operations.set(operation.definition.id, versions);
    if (handler) this.#builtinHandlers.set(operation.definition.id, handler);
  }

  replaceExtensionVersion(extensionId: string, version: string, operations: RegisteredOperation[]): void {
    this.removeExtension(extensionId, version);
    for (const operation of operations) this.register(operation);
  }

  removeExtension(extensionId: string, version?: string): void {
    for (const [id, versions] of this.#operations) {
      for (const [key, operation] of versions) if (operation.extensionId === extensionId && (!version || operation.extensionVersion === version)) versions.delete(key);
      if (!versions.size) this.#operations.delete(id);
    }
  }

  list(options: { includeHidden?: boolean; extensionVersions?: Record<string, string> } = {}): RegisteredOperation[] {
    return [...this.#operations.values()].flatMap((versions) => {
      const builtin = versions.get("builtin");
      if (builtin) return [builtin];
      const extensionId = versions.values().next().value?.extensionId as string | undefined;
      const selectedVersion = extensionId ? options.extensionVersions?.[extensionId] : undefined;
      if (selectedVersion) return versions.get(selectedVersion) ? [versions.get(selectedVersion)!] : [];
      return versions.size === 1 ? [...versions.values()] : [];
    })
      .filter(({ definition }) => options.includeHidden || definition.agentVisibility !== "hidden")
      .sort((left, right) => left.definition.id.localeCompare(right.definition.id));
  }

  get(operationId: string, extensionVersions: Record<string, string> = {}): RegisteredOperation {
    const versions = this.#operations.get(operationId);
    if (!versions) throw new Error(`Unknown operation: ${operationId}`);
    const builtin = versions.get("builtin");
    if (builtin) return builtin;
    const extensionId = versions.values().next().value?.extensionId as string | undefined;
    const selectedVersion = extensionId ? extensionVersions[extensionId] : undefined;
    if (selectedVersion && versions.has(selectedVersion)) return versions.get(selectedVersion)!;
    if (versions.size === 1) return versions.values().next().value!;
    throw new Error(`Operation ${operationId} has multiple installed versions and no project lock.`);
  }

  getBuiltinHandler(operationId: string): BuiltinOperationHandler | undefined {
    return this.#builtinHandlers.get(operationId);
  }

  validateInput(operationId: string, input: Record<string, unknown>, extensionVersions?: Record<string, string>): void {
    const result = validateJsonSchema(this.get(operationId, extensionVersions).definition.inputSchema, input);
    if (!result.valid) throw new Error(`Invalid arguments for ${operationId}: ${result.issues.join(" ")}`);
  }

  validateOutput(operationId: string, output: Record<string, unknown>, extensionVersions?: Record<string, string>): void {
    const result = validateJsonSchema(this.get(operationId, extensionVersions).definition.outputSchema, output);
    if (!result.valid) throw new Error(`Invalid result from ${operationId}: ${result.issues.join(" ")}`);
  }
}
