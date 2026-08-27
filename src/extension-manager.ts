import { createHash, randomUUID, verify } from "node:crypto";
import { createReadStream } from "node:fs";
import { chmod, lstat, mkdir, readFile, rename, rm, writeFile } from "node:fs/promises";
import path from "node:path";
import { isDeepStrictEqual } from "node:util";
import type { PackedExtension } from "@queryn/plugin-sdk/package";
import type { ExtensionManifest as PublicExtensionManifest, RuntimeContribution } from "@queryn/plugin-sdk";
import { validateExtensionManifest } from "@queryn/plugin-sdk";
import type { ExtensionRequirement, OperationDefinition, OperationResources, OperationRisk, Permission, RuntimeDescriptor } from "@queryn/types";
import { readManifest, serializeManifest } from "@queryn/manifest";
import { assertSafeRelativePath, resolveSafeExistingFile, writeJsonAtomic, writeTextAtomic } from "./atomic.js";
import type { OperationRegistry, RegisteredOperation } from "./operation-registry.js";
import type { PolicyEngine } from "./policy-engine.js";
import type { ContextBroker } from "./context-broker.js";
import type { RuntimeSupervisor } from "./runtime-supervisor.js";
import { createInvocationDirectories, removeInvocationDirectories, runtimeInvocationScopeRoot } from "./runtime-supervisor.js";
import type { ModelManager } from "./model-manager.js";
import { stageArtifacts, stageModels } from "./operation-service.js";
import type { ConnectorEngine, ConnectorItem } from "./connector-engine.js";
import type { AgentOrchestrator } from "./agent-orchestrator.js";
import type { ModelRequest, ModelResponse } from "./model-provider.js";
import type { ProjectService } from "./project-service.js";

export interface InstallExtensionOptions {
  allowUnsigned?: boolean;
  signature?: string;
  publicKey?: string;
  maxPackageBytes?: number;
  maxUnpackedBytes?: number;
}

export interface InstalledExtension {
  id: string;
  version: string;
  path: string;
  active: boolean;
  manifest: PublicExtensionManifest;
}

const hostVersion = "0.2.0";

interface ExtensionLock {
  schemaVersion: "1";
  extensions: Record<string, { version: string; integrity?: string }>;
}

interface InstalledRuntimeVersion {
  manifest: PublicExtensionManifest;
  root: string;
  runtimes: Map<string, RuntimeDescriptor>;
}

export class ExtensionManager {
  readonly #runtimes = new Map<string, Map<string, { extensionId: string; descriptor: RuntimeDescriptor }>>();
  readonly #installed = new Map<string, Map<string, InstalledRuntimeVersion>>();
  readonly #integrityIssues = new Map<string, string>();

  constructor(
    readonly dataRoot: string,
    readonly operations: OperationRegistry,
    readonly policy: PolicyEngine,
    readonly context: ContextBroker,
    readonly connectors: ConnectorEngine,
    readonly supervisor: RuntimeSupervisor,
    readonly models: ModelManager,
    readonly agent: AgentOrchestrator,
    readonly projects: ProjectService
  ) {}

  async install(packagePath: string, options: InstallExtensionOptions = {}): Promise<InstalledExtension> {
    const raw = await readFile(packagePath);
    if (raw.byteLength > (options.maxPackageBytes ?? 64 * 1024 * 1024)) throw new Error("Extension package is too large.");
    const packed = JSON.parse(raw.toString("utf8")) as PackedExtension;
    if (packed.format !== "queryn-extension-package/1") throw new Error("Unsupported extension package format.");
    const validation = validateExtensionManifest(packed.manifest);
    if (!validation.valid) throw new Error(validation.issues.join("\n"));
    if (!isCompatibleHostVersion(hostVersion, packed.manifest.queryn.minVersion)) {
      throw new Error(`Extension requires Queryn ${packed.manifest.queryn.minVersion} or newer; host is ${hostVersion}.`);
    }
    verifyPackageIntegrity(packed);
    verifyPackageSignature(packed, options);

    const versionRoot = this.#versionRoot(packed.manifest.id, packed.manifest.version);
    try {
      const installed = JSON.parse(await readFile(path.join(versionRoot, ".install.json"), "utf8")) as { integrity: string };
      // Installed versions are immutable, so only an identical integrity may reactivate one.
      if (installed.integrity !== packed.integrity) throw new Error("An installed extension version is immutable and has different contents.");
      await this.activate(packed.manifest.id, packed.manifest.version);
      return { id: packed.manifest.id, version: packed.manifest.version, path: versionRoot, active: true, manifest: packed.manifest };
    } catch (error) {
      // Only a missing install record starts a fresh install; every other failure must surface.
      if (!(error instanceof Error && "code" in error && error.code === "ENOENT")) throw error;
    }
    // Unique staging names isolate concurrent installers before rename publishes a complete version tree.
    // see queryn-docs/docs/adr/adr-0004-plugin-system.md
    const staging = `${versionRoot}.staging-${process.pid}-${Date.now()}`;
    await rm(staging, { recursive: true, force: true });
    await mkdir(staging, { recursive: true });
    let unpackedBytes = 0;
    try {
      for (const file of packed.files) {
        const relativePath = assertSafeRelativePath(file.path);
        const data = Buffer.from(file.content, "base64");
        unpackedBytes += data.byteLength;
        if (unpackedBytes > (options.maxUnpackedBytes ?? 256 * 1024 * 1024)) throw new Error("Unpacked extension exceeds size limit.");
        if (createHash("sha256").update(data).digest("hex") !== file.sha256) throw new Error(`Checksum mismatch: ${file.path}`);
        const outputPath = path.join(staging, ...relativePath.split("/"));
        await mkdir(path.dirname(outputPath), { recursive: true });
        await writeFile(outputPath, data, { mode: 0o600 });
      }
      await verifyStagedPackage(packed.manifest, staging);
      await writeJsonAtomic(path.join(staging, ".install.json"), {
        installedAt: new Date().toISOString(), integrity: packed.integrity,
        signed: Boolean(options.signature), sourcePackage: path.basename(packagePath),
        files: packed.files.map((file) => ({ path: file.path, sha256: file.sha256 }))
      });
      await mkdir(path.dirname(versionRoot), { recursive: true });
      await rename(staging, versionRoot);
      await this.activate(packed.manifest.id, packed.manifest.version);
      return { id: packed.manifest.id, version: packed.manifest.version, path: versionRoot, active: true, manifest: packed.manifest };
    } catch (error) {
      await rm(staging, { recursive: true, force: true });
      throw error;
    }
  }

  async activate(extensionId: string, version: string): Promise<void> {
    const manifest = await this.#readInstalledManifest(extensionId, version);
    await writeJsonAtomic(this.#activePath(extensionId), { version, activatedAt: new Date().toISOString() });
    this.#register(manifest, this.#versionRoot(extensionId, version));
  }

  async rollback(extensionId: string, version: string): Promise<void> { await this.activate(extensionId, version); }

  async list(): Promise<InstalledExtension[]> {
    const root = path.join(this.dataRoot, "extensions");
    const { readdir } = await import("node:fs/promises");
    let publishers;
    try { publishers = await readdir(root, { withFileTypes: true }); } catch { return []; }
    const results: InstalledExtension[] = [];
    const seenVersions = new Set<string>();
    for (const publisher of publishers.filter((entry) => entry.isDirectory())) {
      const extensions = await readdir(path.join(root, publisher.name), { withFileTypes: true });
      for (const extension of extensions.filter((entry) => entry.isDirectory())) {
        const id = `${publisher.name}.${extension.name}`;
        let activeVersion: string | undefined;
        try { activeVersion = (JSON.parse(await readFile(this.#activePath(id), "utf8")) as { version: string }).version; } catch { /* No active marker means no version is active. */ }
        const versionsPath = path.join(root, publisher.name, extension.name, "versions");
        let versions;
        try { versions = await readdir(versionsPath, { withFileTypes: true }); } catch { continue; }
        for (const version of versions.filter((entry) => entry.isDirectory())) {
          const versionKey = `${id}@${version.name}`;
          seenVersions.add(versionKey);
          try {
            const manifest = await this.#readInstalledManifest(id, version.name);
            this.#integrityIssues.delete(versionKey);
            results.push({ id, version: version.name, path: this.#versionRoot(id, version.name), active: version.name === activeVersion, manifest });
          } catch (error) {
            this.#integrityIssues.set(versionKey, error instanceof Error ? error.message : String(error));
          }
        }
      }
    }
    for (const key of this.#integrityIssues.keys()) if (!seenVersions.has(key)) this.#integrityIssues.delete(key);
    return results;
  }

  async loadActive(): Promise<void> {
    for (const extension of await this.list()) this.#register(extension.manifest, extension.path);
  }

  integrityIssues(): Array<{ extension: string; error: string }> {
    return [...this.#integrityIssues.entries()].map(([extension, error]) => ({ extension, error }));
  }

  async connect(projectPath: string, extensionId: string, version: string, permissions: Permission[], requirementVersion = version): Promise<void> {
    const installed = await this.#readInstalledManifest(extensionId, version);
    if (!satisfiesVersion(version, requirementVersion)) throw new Error(`${extensionId}@${version} does not satisfy project requirement ${requirementVersion}.`);
    const undeclared = permissions.filter((permission) => !installed.permissions.includes(permission));
    if (undeclared.length) throw new Error(`Cannot grant undeclared permissions: ${undeclared.join(", ")}`);
    const manifest = await readManifest(projectPath);
    const extensions = upsertRequirement(manifest.extensions ?? [], { id: extensionId, version: requirementVersion, enabled: true });
    await writeTextAtomic(path.join(projectPath, "queryn.json"), serializeManifest({ ...manifest, extensions, updatedAt: new Date().toISOString() }));
    const lockPath = path.join(projectPath, ".queryn", "extensions", "lock.json");
    const lock = await readJsonOr<ExtensionLock>(lockPath, { schemaVersion: "1", extensions: {} });
    const install = await readJsonOr<{ integrity?: string }>(path.join(this.#versionRoot(extensionId, version), ".install.json"), {});
    lock.extensions[extensionId] = { version, integrity: install.integrity };
    await writeJsonAtomic(lockPath, lock);
    await this.policy.grant(projectPath, { extensionId, permissions });
    await this.projects.open(projectPath);
    await this.#recordModelUsage(projectPath);
  }

  async disconnect(projectPath: string, extensionId: string): Promise<void> {
    const manifest = await readManifest(projectPath);
    const extensions = (manifest.extensions ?? []).map((requirement) => requirement.id === extensionId ? { ...requirement, enabled: false } : requirement);
    await writeTextAtomic(path.join(projectPath, "queryn.json"), serializeManifest({ ...manifest, extensions, updatedAt: new Date().toISOString() }));
    const lockPath = path.join(projectPath, ".queryn", "extensions", "lock.json");
    const lock = await readJsonOr<ExtensionLock>(lockPath, { schemaVersion: "1", extensions: {} });
    delete lock.extensions[extensionId];
    await writeJsonAtomic(lockPath, lock);
    await this.policy.revoke(projectPath, extensionId);
    await this.projects.open(projectPath);
    await this.#recordModelUsage(projectPath);
  }

  async reconcileProject(projectPath: string): Promise<{ resolved: Record<string, string>; missing: ExtensionRequirement[] }> {
    const manifest = await readManifest(projectPath);
    const installed = await this.list();
    const lockPath = path.join(projectPath, ".queryn", "extensions", "lock.json");
    const lock = await readJsonOr<ExtensionLock>(lockPath, { schemaVersion: "1", extensions: {} });
    const missing: ExtensionRequirement[] = [];
    for (const requirement of manifest.extensions ?? []) {
      if (requirement.enabled === false) { delete lock.extensions[requirement.id]; continue; }
      const candidates = installed.filter((item) => item.id === requirement.id && satisfiesVersion(item.version, requirement.version)).sort((left, right) => compareVersions(right.version, left.version));
      const locked = lock.extensions[requirement.id];
      const selected = candidates.find((item) => item.version === locked?.version) ?? candidates[0];
      if (!selected) { delete lock.extensions[requirement.id]; missing.push(requirement); continue; }
      const install = await readJsonOr<{ integrity?: string }>(path.join(selected.path, ".install.json"), {});
      lock.extensions[requirement.id] = { version: selected.version, integrity: install.integrity };
    }
    await writeJsonAtomic(lockPath, lock);
    await this.projects.open(projectPath);
    await this.#recordModelUsage(projectPath);
    return { resolved: this.projects.extensionVersions(projectPath), missing };
  }

  async modelDependents(sha256: string): Promise<string[]> {
    const { readdir } = await import("node:fs/promises");
    const root = path.join(this.dataRoot, "model-usage");
    let entries;
    try { entries = await readdir(root, { withFileTypes: true }); } catch { return []; }
    const projects: string[] = [];
    for (const entry of entries.filter((candidate) => candidate.isFile() && candidate.name.endsWith(".json"))) {
      try {
        const usage = JSON.parse(await readFile(path.join(root, entry.name), "utf8")) as { projectPath?: unknown; models?: unknown };
        if (typeof usage.projectPath === "string" && Array.isArray(usage.models) && usage.models.includes(sha256)) projects.push(usage.projectPath);
      } catch { projects.push(`<corrupt model usage record: ${entry.name}>`); }
    }
    return [...new Set(projects)].sort();
  }

  getRuntime(runtimeId: string, projectPath: string): RuntimeDescriptor {
    const versions = this.#runtimes.get(runtimeId);
    if (!versions) throw new Error(`Unknown extension runtime: ${runtimeId}`);
    const registered = versions.values().next().value as { extensionId: string; descriptor: RuntimeDescriptor } | undefined;
    if (!registered) throw new Error(`Unknown extension runtime: ${runtimeId}`);
    const connected = this.projects.get(projectPath).manifest.extensions?.some((extension) => extension.id === registered.extensionId && extension.enabled !== false);
    if (!connected) throw new Error(`Runtime extension is not connected to this project: ${registered.extensionId}`);
    const selectedVersion = this.projects.extensionVersions(projectPath)[registered.extensionId];
    const selected = selectedVersion ? versions.get(selectedVersion) : undefined;
    if (!selected) throw new Error(`Project has no installed lock for runtime ${runtimeId}.`);
    return selected.descriptor;
  }

  #register(manifest: PublicExtensionManifest, extensionPath: string): void {
    const runtimes = new Map((manifest.runtimes ?? []).map((runtime) => [runtime.id, toRuntimeDescriptor(runtime, extensionPath)]));
    const installedVersions = this.#installed.get(manifest.id) ?? new Map<string, InstalledRuntimeVersion>();
    installedVersions.set(manifest.version, { manifest, root: extensionPath, runtimes });
    this.#installed.set(manifest.id, installedVersions);
    for (const [runtimeId, descriptor] of runtimes) {
      const versions = this.#runtimes.get(runtimeId) ?? new Map<string, { extensionId: string; descriptor: RuntimeDescriptor }>();
      versions.set(manifest.version, { extensionId: manifest.id, descriptor });
      this.#runtimes.set(runtimeId, versions);
    }
    const toolRuntimes = new Map((manifest.contributes.tools ?? []).map((tool) => [tool.id, tool.runtimeId ? runtimes.get(tool.runtimeId) : undefined]));
    const artifactMediaTypes = Object.fromEntries((manifest.contributes.artifactTypes ?? []).filter((type) => type.mediaTypes?.length).map((type) => [type.id, type.mediaTypes!]));
    const operations: RegisteredOperation[] = (manifest.contributes.operations ?? []).map((definition) => ({
      extensionId: manifest.id,
      extensionVersion: manifest.version,
      definition: withRuntimePolicy(definition, toolRuntimes.get(definition.toolId)),
      runtime: toolRuntimes.get(definition.toolId),
      artifactMediaTypes
    }));
    this.operations.replaceExtensionVersion(manifest.id, manifest.version, operations);
    for (const provider of manifest.contributes.contextProviders ?? []) {
      this.context.register(provider.id, async (input) => {
        const selected = this.#resolveInstalled(manifest.id, input.projectPath);
        const selectedProvider = selected.manifest.contributes.contextProviders?.find((candidate) => candidate.id === provider.id);
        if (!selectedProvider) throw new Error(`Context provider ${provider.id} is unavailable in the project-locked extension version.`);
        const providerRuntime = selected.runtimes.get(selectedProvider.runtimeId);
        if (!providerRuntime) throw new Error(`Runtime is missing for context provider ${provider.id}.`);
        const policyOperation = syntheticOperation(provider.id, ["artifact:read", ...runtimePermissions(providerRuntime)], runtimeRisk("safe-read", providerRuntime));
        const evaluation = this.policy.evaluate(input.projectPath, manifest.id, policyOperation);
        if (!evaluation.allowed) throw new Error(`${evaluation.reason} Missing: ${evaluation.missingPermissions.join(", ")}`);
        if (evaluation.approvalRequired && !input.approval?.approved) throw new Error(`Context provider ${provider.id} requires explicit approval.`);
        if (input.approval) await this.policy.rememberApproval(input.projectPath, provider.id, input.approval);
        const directories = await createInvocationDirectories(runtimeInvocationScopeRoot(this.dataRoot, providerRuntime, input.projectPath), randomUUID());
        try {
          await stageArtifacts(input.projectPath, [input.artifact.id], directories.input);
          await stageModels(providerRuntime.models ?? [], this.models, directories.models);
          return await this.supervisor.call(providerRuntime, "context/resolve", {
            providerId: provider.id, level: input.level, budgetTokens: input.budgetTokens,
            recipient: input.recipient, artifactId: input.artifact.id,
            resourceUri: selectedProvider.resourceUriTemplate?.replaceAll("{artifactId}", encodeURIComponent(input.artifact.id))
          }, { paths: directories, timeoutMs: 30_000 });
        } finally { await removeInvocationDirectories(directories.root); }
      });
    }
    for (const connector of manifest.contributes.connectors ?? []) {
      const definitions = [...installedVersions.values()].flatMap((version) => version.manifest.contributes.connectors?.filter((candidate) => candidate.id === connector.id) ?? []);
      const connectorRuntimes = [...installedVersions.values()].flatMap((version) => definitions.flatMap((definition) => {
        const runtime = version.runtimes.get(definition.runtimeId);
        return runtime ? [runtime] : [];
      }));
      const supervisor = this.supervisor;
      const dataRoot = this.dataRoot;
      const models = this.models;
      const manager = this;
      this.connectors.register(connector.id, {
        async *pull(input) {
          const selected = manager.#resolveInstalled(manifest.id, input.project.rootPath);
          const selectedConnector = selected.manifest.contributes.connectors?.find((candidate) => candidate.id === connector.id);
          if (!selectedConnector) throw new Error(`Connector ${connector.id} is unavailable in the project-locked extension version.`);
          const connectorRuntime = selected.runtimes.get(selectedConnector.runtimeId);
          if (!connectorRuntime) throw new Error(`Runtime is missing for connector ${connector.id}.`);
          let checkpoint = input.checkpoint;
          while (!input.signal.aborted) {
            const directories = await createInvocationDirectories(runtimeInvocationScopeRoot(dataRoot, connectorRuntime, input.project.rootPath), randomUUID());
            try {
              await stageModels(connectorRuntime.models ?? [], models, directories.models);
              const result = await supervisor.call<{ items: ConnectorItem[]; done?: boolean }>(connectorRuntime, "connectors/pull", {
                connectorId: connector.id, checkpoint, limit: 100
              }, { paths: directories, signal: input.signal, timeoutMs: 60_000 });
              for (const item of result.items ?? []) { checkpoint = item.cursor; yield { ...item, outboxPath: directories.outbox }; }
              if (result.done !== false || !result.items?.length) return;
            } finally { await removeInvocationDirectories(directories.root); }
          }
        }
      }, {
        extensionId: manifest.id,
        permissions: [...new Set<Permission>(["artifact:create", ...definitions.flatMap((definition) => definition.permissions), ...connectorRuntimes.flatMap(runtimePermissions)])],
        risk: connectorRuntimes.reduce<OperationRisk>((risk, runtime) => runtimeRisk(risk, runtime), definitions.some((definition) => definition.scope === "external-explicit") ? "network-egress" : "project-write"),
        scope: definitions.some((definition) => definition.scope === "external-explicit") ? "external-explicit" : "project",
        produces: [...new Set(definitions.flatMap((definition) => definition.produces))],
        allowedMediaTypesByType: Object.fromEntries([...installedVersions.values()].flatMap((version) =>
          (version.manifest.contributes.artifactTypes ?? []).filter((type) => type.mediaTypes?.length).map((type) => [type.id, type.mediaTypes!])
        ))
      });
    }
    for (const provider of manifest.contributes.modelProviders ?? []) {
      const definitions = [...installedVersions.values()].flatMap((version) => version.manifest.contributes.modelProviders?.filter((candidate) => candidate.id === provider.id) ?? []);
      const providerRuntimes = [...installedVersions.values()].flatMap((version) => definitions.flatMap((definition) => {
        const runtime = version.runtimes.get(definition.runtimeId);
        return runtime ? [runtime] : [];
      }));
      this.agent.registerProvider({
        id: provider.id,
        recipient: definitions.some((definition) => definition.recipient === "cloud") || providerRuntimes.some(isExternalRemoteRuntime) ? "cloud" : "local",
        sourceExtensionId: manifest.id,
        permissions: [...new Set<Permission>(providerRuntimes.flatMap(runtimePermissions))],
        risk: providerRuntimes.reduce<OperationRisk>((risk, runtime) => runtimeRisk(risk, runtime), "safe-read"),
        complete: async (request: ModelRequest): Promise<ModelResponse> => {
          if (!request.projectPath) throw new Error(`Extension model provider ${provider.id} requires a project.`);
          const selected = this.#resolveInstalled(manifest.id, request.projectPath);
          const selectedProvider = selected.manifest.contributes.modelProviders?.find((candidate) => candidate.id === provider.id);
          if (!selectedProvider) throw new Error(`Model provider ${provider.id} is unavailable in the project-locked extension version.`);
          const providerRuntime = selected.runtimes.get(selectedProvider.runtimeId);
          if (!providerRuntime) throw new Error(`Runtime is missing for model provider ${provider.id}.`);
          const directories = await createInvocationDirectories(runtimeInvocationScopeRoot(this.dataRoot, providerRuntime, request.projectPath), randomUUID());
          try {
            await stageModels(providerRuntime.models ?? [], this.models, directories.models);
            const { signal, projectPath: _projectPath, ...wireRequest } = request;
            const response = await this.supervisor.call<ModelResponse>(providerRuntime, "models/complete", {
              providerId: provider.id,
              request: wireRequest
            }, { paths: directories, signal, timeoutMs: 300_000 });
            if (!response || typeof response.text !== "string" || typeof response.model !== "string") throw new Error(`Model provider ${provider.id} returned an invalid response.`);
            return response;
          } finally { await removeInvocationDirectories(directories.root); }
        }
      });
    }
  }

  #resolveInstalled(extensionId: string, projectPath: string): InstalledRuntimeVersion {
    const selectedVersion = this.projects.extensionVersions(projectPath)[extensionId];
    const selected = selectedVersion ? this.#installed.get(extensionId)?.get(selectedVersion) : undefined;
    if (!selected) throw new Error(`Project has no installed lock for extension ${extensionId}.`);
    return selected;
  }

  async #recordModelUsage(projectPath: string): Promise<void> {
    const models = new Set<string>();
    for (const [extensionId, version] of Object.entries(this.projects.extensionVersions(projectPath))) {
      const installed = this.#installed.get(extensionId)?.get(version);
      for (const runtime of installed?.runtimes.values() ?? []) for (const model of runtime.models ?? []) models.add(model.sha256);
    }
    const key = createHash("sha256").update(path.resolve(projectPath)).digest("hex");
    await writeJsonAtomic(path.join(this.dataRoot, "model-usage", `${key}.json`), {
      schemaVersion: "1", projectPath: path.resolve(projectPath), models: [...models].sort(), updatedAt: new Date().toISOString()
    });
  }

  async #readInstalledManifest(extensionId: string, version: string): Promise<PublicExtensionManifest> {
    await this.#verifyInstalledVersion(extensionId, version);
    return JSON.parse(await readFile(path.join(this.#versionRoot(extensionId, version), "extension.json"), "utf8")) as PublicExtensionManifest;
  }

  async #verifyInstalledVersion(extensionId: string, version: string): Promise<void> {
    const versionRoot = this.#versionRoot(extensionId, version);
    const metadata = JSON.parse(await readFile(path.join(versionRoot, ".install.json"), "utf8")) as {
      integrity?: unknown; files?: Array<{ path?: unknown; sha256?: unknown }>;
    };
    if (typeof metadata.integrity !== "string" || !Array.isArray(metadata.files)) throw new Error("Extension install metadata is incomplete; reinstall this version.");
    const entries: Array<{ path: string; sha256: string }> = [];
    for (const file of metadata.files) {
      if (typeof file.path !== "string" || typeof file.sha256 !== "string" || !/^[a-f0-9]{64}$/.test(file.sha256)) throw new Error("Extension install file metadata is invalid.");
      const filePath = await resolveSafeExistingFile(versionRoot, file.path, "Installed extension file");
      const sha256 = await hashFile(filePath);
      if (sha256 !== file.sha256) throw new Error(`Installed extension file was modified: ${file.path}`);
      entries.push({ path: file.path, sha256 });
    }
    const integrity = createHash("sha256").update(entries.map((file) => `${file.path}:${file.sha256}`).sort().join("\n")).digest("hex");
    if (integrity !== metadata.integrity) throw new Error("Installed extension integrity metadata does not match its files.");
  }

  #extensionRoot(extensionId: string): string {
    const [publisher, ...rest] = extensionId.split(".");
    if (!publisher || !rest.length) throw new Error("Extension id must be namespaced.");
    return path.join(this.dataRoot, "extensions", publisher, rest.join("."));
  }
  #versionRoot(id: string, version: string): string { return path.join(this.#extensionRoot(id), "versions", version); }
  #activePath(id: string): string { return path.join(this.#extensionRoot(id), "active.json"); }
}

function verifyPackageIntegrity(packed: PackedExtension): void {
  const targetPaths = new Set<string>();
  for (const file of packed.files) {
    const targetPath = path.posix.normalize(assertSafeRelativePath(file.path));
    const collisionKey = targetPath.toLocaleLowerCase("en-US");
    if (targetPaths.has(collisionKey)) throw new Error(`Extension package contains a duplicate target path: ${targetPath}`);
    targetPaths.add(collisionKey);
  }
  const integrity = createHash("sha256").update(packed.files.map((file) => `${file.path}:${file.sha256}`).sort().join("\n")).digest("hex");
  if (integrity !== packed.integrity) throw new Error("Extension package integrity mismatch.");
  const manifestFile = packed.files.find((file) => path.posix.normalize(file.path.replaceAll("\\", "/")) === "extension.json");
  if (!manifestFile) throw new Error("Extension package does not contain extension.json.");
  let fileManifest: unknown;
  try { fileManifest = JSON.parse(Buffer.from(manifestFile.content, "base64").toString("utf8")); }
  catch { throw new Error("Extension package contains an invalid extension.json."); }
  if (!isDeepStrictEqual(fileManifest, packed.manifest)) throw new Error("Packed manifest does not match the signed extension.json file.");
}

function verifyPackageSignature(packed: PackedExtension, options: InstallExtensionOptions): void {
  if (!options.signature || !options.publicKey) {
    if (!options.allowUnsigned) throw new Error("Unsigned extension requires explicit Developer Mode allowance.");
    return;
  }
  const valid = verify(null, Buffer.from(packed.integrity, "utf8"), options.publicKey, Buffer.from(options.signature, "base64"));
  if (!valid) throw new Error("Invalid extension signature.");
}

async function verifyStagedPackage(manifest: PublicExtensionManifest, stagingRoot: string): Promise<void> {
  for (const runtime of manifest.runtimes ?? []) {
    if (!runtime.entry) continue;
    const entry = await verifyPackagedRegularFile(stagingRoot, runtime.entry, "Runtime entry");
    if (runtime.kind === "native-process" && process.platform !== "win32") await chmod(entry, 0o700);
  }
  for (const theme of manifest.contributes.themes ?? []) {
    await verifyPackagedRegularFile(stagingRoot, theme.tokens, "Theme tokens");
    if (theme.icons) await verifyPackagedRegularFile(stagingRoot, theme.icons, "Theme icons");
  }
  for (const view of manifest.contributes.views ?? []) await verifyPackagedRegularFile(stagingRoot, view.entry, "View entry");
}

async function verifyPackagedRegularFile(stagingRoot: string, relativePath: string, label: string): Promise<string> {
  const filePath = path.join(stagingRoot, ...assertSafeRelativePath(relativePath).split("/"));
  let fileStat;
  try { fileStat = await lstat(filePath); }
  catch { throw new Error(`${label} is missing from the extension package: ${relativePath}`); }
  if (!fileStat.isFile() || fileStat.isSymbolicLink()) throw new Error(`${label} must be a regular packaged file: ${relativePath}`);
  return filePath;
}

function toRuntimeDescriptor(runtime: RuntimeContribution, root: string): RuntimeDescriptor {
  const entry = runtime.entry ? path.join(root, ...assertSafeRelativePath(runtime.entry).split("/")) : undefined;
  return { ...runtime, entry } as RuntimeDescriptor;
}

function upsertRequirement(requirements: ExtensionRequirement[], next: ExtensionRequirement): ExtensionRequirement[] {
  return [...requirements.filter((requirement) => requirement.id !== next.id), next].sort((left, right) => left.id.localeCompare(right.id));
}

async function readJsonOr<T>(filePath: string, fallback: T): Promise<T> {
  try { return JSON.parse(await readFile(filePath, "utf8")) as T; } catch { return fallback; }
}

function satisfiesVersion(version: string, range: string): boolean {
  if (range === "*" || range.toLowerCase() === "latest") return true;
  const actual = parseVersion(version);
  const expected = parseVersion(range.replace(/^[~^]/, ""));
  if (!actual || !expected) return version === range;
  if (!range.startsWith("^") && !range.startsWith("~")) return version === range;
  if (actual.prerelease && !expected.prerelease) return false;
  if (compareParsedVersions(actual, expected) < 0) return false;
  const [major, minor, patch] = expected.parts;
  const upper: ParsedVersion = range.startsWith("~")
    ? { parts: [major, minor + 1, 0] }
    : major > 0
      ? { parts: [major + 1, 0, 0] }
      : minor > 0
        ? { parts: [0, minor + 1, 0] }
        : { parts: [0, 0, patch + 1] };
  return compareParsedVersions(actual, upper) < 0;
}

function compareVersions(left: string, right: string): number {
  const a = parseVersion(left);
  const b = parseVersion(right);
  return a && b ? compareParsedVersions(a, b) : left.localeCompare(right);
}

interface ParsedVersion { parts: [number, number, number]; prerelease?: string }

function parseVersion(value: string): ParsedVersion | undefined {
  const match = /^(\d+)\.(\d+)\.(\d+)(?:-([0-9A-Za-z.-]+))?(?:\+[0-9A-Za-z.-]+)?$/.exec(value);
  return match ? { parts: [Number(match[1]), Number(match[2]), Number(match[3])], prerelease: match[4] } : undefined;
}

function compareParsedVersions(left: ParsedVersion, right: ParsedVersion): number {
  const core = left.parts[0] - right.parts[0] || left.parts[1] - right.parts[1] || left.parts[2] - right.parts[2];
  if (core) return core;
  if (left.prerelease === right.prerelease) return 0;
  if (!left.prerelease) return 1;
  if (!right.prerelease) return -1;
  return comparePrerelease(left.prerelease, right.prerelease);
}

function comparePrerelease(left: string, right: string): number {
  const a = left.split(".");
  const b = right.split(".");
  for (let index = 0; index < Math.max(a.length, b.length); index += 1) {
    if (a[index] === undefined) return -1;
    if (b[index] === undefined) return 1;
    if (a[index] === b[index]) continue;
    const aNumber = /^\d+$/.test(a[index]) ? Number(a[index]) : undefined;
    const bNumber = /^\d+$/.test(b[index]) ? Number(b[index]) : undefined;
    if (aNumber !== undefined && bNumber !== undefined) return aNumber - bNumber;
    if (aNumber !== undefined) return -1;
    if (bNumber !== undefined) return 1;
    return a[index].localeCompare(b[index]);
  }
  return 0;
}

function isCompatibleHostVersion(host: string, minimum: string): boolean {
  const actual = parseVersion(host);
  const required = parseVersion(minimum);
  return Boolean(actual && required && compareParsedVersions(actual, required) >= 0);
}

const riskOrder: OperationRisk[] = ["safe-read", "project-write", "network-egress", "external-side-effect", "privileged"];

function withRuntimePolicy(definition: OperationDefinition, runtime?: RuntimeDescriptor): OperationDefinition {
  const permissions = [...new Set<Permission>([
    ...definition.permissions,
    ...(definition.accepts?.length ? ["artifact:read" as const] : []),
    ...resourcePermissions(definition.resources),
    ...runtimePermissions(runtime)
  ])];
  return { ...definition, permissions, risk: runtimeRisk(resourceRisk(definition.risk, definition.resources), runtime) };
}

function runtimePermissions(runtime?: RuntimeDescriptor): Permission[] {
  if (!runtime) return [];
  return [...new Set<Permission>([
    ...(runtime.kind === "remote" || runtime.resources?.network ? ["network:use" as const] : []),
    ...(runtime.kind === "native-process" ? ["native:execute" as const] : []),
    ...(runtime.lifecycle !== "job" ? ["background:run" as const] : []),
    ...(runtime.resources?.gpu ? ["compute:gpu" as const] : []),
    ...(runtime.models?.length ? ["models:use" as const] : [])
  ])];
}

function resourcePermissions(resources?: OperationResources): Permission[] {
  return [
    ...(resources?.network ? ["network:use" as const] : []),
    ...(resources?.gpu ? ["compute:gpu" as const] : [])
  ];
}

function runtimeRisk(base: OperationRisk, runtime?: RuntimeDescriptor): OperationRisk {
  let risk = resourceRisk(base, runtime?.resources);
  if (runtime?.kind === "remote") risk = maxRisk(risk, "network-egress");
  if (runtime?.kind === "native-process") risk = maxRisk(risk, "privileged");
  return risk;
}

function resourceRisk(base: OperationRisk, resources?: OperationResources): OperationRisk {
  let risk = resources?.network ? maxRisk(base, "network-egress") : base;
  if (resources?.gpu) risk = maxRisk(risk, "privileged");
  return risk;
}

function maxRisk(left: OperationRisk, right: OperationRisk): OperationRisk {
  return riskOrder[Math.max(riskOrder.indexOf(left), riskOrder.indexOf(right))];
}

function syntheticOperation(id: string, permissions: Permission[], risk: OperationRisk): OperationDefinition {
  return {
    id, toolId: id, version: "1", title: id, inputSchema: {}, outputSchema: {}, permissions: [...new Set(permissions)],
    risk, agentVisibility: "hidden", execution: "immediate", cancellable: true, idempotent: true
  };
}

function isExternalRemoteRuntime(runtime: RuntimeDescriptor): boolean {
  if (runtime.kind !== "remote" || !runtime.endpoint) return false;
  try { return !["127.0.0.1", "localhost", "::1"].includes(new URL(runtime.endpoint).hostname); }
  catch { return true; }
}

async function hashFile(filePath: string): Promise<string> {
  const hash = createHash("sha256");
  for await (const chunk of createReadStream(filePath)) hash.update(chunk as Buffer);
  return hash.digest("hex");
}
