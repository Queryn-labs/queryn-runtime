import os from "node:os";
import path from "node:path";
import { mkdir, readFile, writeFile, rename, rm } from "node:fs/promises";
import { createNote, createArtifactRelation, importAsset, listArtifacts, listAssets, listNotes, readNote, registerExistingArtifact } from "@queryn/project";
import type { ApprovalDecision, OperationDefinition, RuntimeDescriptor } from "@queryn/types";
import { AgentKernel } from "./agent-kernel.js";
import { AgentOrchestrator } from "./agent-orchestrator.js";
import { ArtifactIngestor } from "./artifact-ingestor.js";
import { ConnectorEngine } from "./connector-engine.js";
import { ContextBroker, ProjectIndexer, readTextPrefixForMaterial } from "./context-broker.js";
import { resolveSafeExistingFile, writeJsonAtomic } from "./atomic.js";
import { createSystemCredentialStore } from "./credential-store.js";
import { DiagnosticsService } from "./diagnostics.js";
import { ExtensionManager } from "./extension-manager.js";
import { JobManager } from "./job-manager.js";
import { McpBridge, type McpServerDescriptor } from "./mcp-bridge.js";
import { ModelManager } from "./model-manager.js";
import { OpenAICompatibleProvider } from "./model-provider.js";
import { OperationRegistry } from "./operation-registry.js";
import { OperationService } from "./operation-service.js";
import { PolicyEngine } from "./policy-engine.js";
import { ProjectService } from "./project-service.js";
import { createInvocationDirectories, removeInvocationDirectories, runtimeInvocationScopeRoot, RuntimeSupervisor } from "./runtime-supervisor.js";
import { stageModels } from "./operation-service.js";
import { readSessionTranscript, searchSessions } from "./session-memory.js";
import { fetchPageText } from "./web-fetch.js";

export interface ModelProviderConfig {
  id: string;
  type: "openai-compatible";
  endpoint: string;
  credentialAccount?: string;
}

export class QuerynRuntime {
  readonly projects = new ProjectService();
  readonly registry = new OperationRegistry();
  readonly policy: PolicyEngine;
  readonly jobs: JobManager;
  readonly supervisor = new RuntimeSupervisor();
  readonly ingestor = new ArtifactIngestor();
  readonly extensions: ExtensionManager;
  readonly context = new ContextBroker();
  readonly indexer = new ProjectIndexer();
  readonly connectors: ConnectorEngine;
  readonly models: ModelManager;
  readonly credentials;
  readonly operations: OperationService;
  readonly agent: AgentOrchestrator;
  readonly agentKernel: AgentKernel;
  readonly mcp = new McpBridge();
  readonly diagnostics: DiagnosticsService;

  constructor(readonly dataRoot = defaultRuntimeDataRoot()) {
    this.policy = new PolicyEngine(dataRoot);
    this.connectors = new ConnectorEngine(this.ingestor, this.policy);
    this.jobs = new JobManager(dataRoot);
    this.models = new ModelManager(dataRoot);
    this.credentials = createSystemCredentialStore(dataRoot);
    this.operations = new OperationService(dataRoot, this.projects, this.registry, this.policy, this.jobs, this.supervisor, this.ingestor, this.models);
    this.agentKernel = new AgentKernel(dataRoot, this.registry, this.operations, this.jobs, this.context, this.indexer);
    this.agent = new AgentOrchestrator(this.registry, this.operations, this.agentKernel);
    this.extensions = new ExtensionManager(dataRoot, this.registry, this.policy, this.context, this.connectors, this.supervisor, this.models, this.agent, this.projects);
    this.diagnostics = new DiagnosticsService(dataRoot, this.extensions, this.models, this.jobs);
    registerBuiltins(this);
  }

  async initialize(): Promise<void> {
    await this.jobs.initialize();
    await this.extensions.loadActive();
    for (const config of await this.listModelProviderConfigs()) this.#registerModelProvider(config);
    for (const descriptor of await readMcpServers(this.dataRoot)) {
      try { await this.registerMcpServer(descriptor); }
      catch { /* An unreachable MCP server must not block runtime startup. */ }
    }
  }

  async openProject(projectPath: string) {
    const project = await this.projects.open(projectPath);
    await this.extensions.reconcileProject(project.rootPath);
    await this.policy.hydrateProject(project.rootPath);
    return this.projects.get(project.rootPath);
  }

  async configureModelProvider(config: ModelProviderConfig, secret?: string): Promise<ModelProviderConfig> {
    if (!/^[a-z0-9][a-z0-9._-]+$/.test(config.id)) throw new Error("Model provider id must be namespaced.");
    const endpoint = new URL(config.endpoint);
    if (endpoint.protocol !== "https:" && !["127.0.0.1", "localhost", "::1"].includes(endpoint.hostname)) throw new Error("Cloud model providers require HTTPS.");
    if (secret) {
      if (!config.credentialAccount) throw new Error("credentialAccount is required when saving a secret.");
      await this.credentials.set(config.credentialAccount, secret);
    }
    const configs = [...(await this.listModelProviderConfigs()).filter((item) => item.id !== config.id), config].sort((left, right) => left.id.localeCompare(right.id));
    await writeJsonAtomic(path.join(this.dataRoot, "model-providers.json"), configs);
    this.#registerModelProvider(config);
    return config;
  }

  async listModelProviderConfigs(): Promise<ModelProviderConfig[]> {
    try { return JSON.parse(await readFile(path.join(this.dataRoot, "model-providers.json"), "utf8")) as ModelProviderConfig[]; }
    catch { return []; }
  }

  async syncConnector(projectPath: string, connectorId: string, approval?: ApprovalDecision) {
    const project = this.projects.get(projectPath);
    const definition = this.connectors.get(connectorId);
    const job = await this.jobs.create({ projectPath: project.rootPath, operationId: connectorId, input: {} });
    void (async () => {
      try {
        await this.jobs.transition(job.id, "running");
        const artifacts = await this.connectors.sync(project, connectorId, { signal: this.jobs.signal(job.id), producedTypes: definition.produces, approval });
        await this.jobs.transition(job.id, "succeeded", { artifactIds: artifacts.map((artifact) => artifact.id), progress: 1, result: { imported: artifacts.length } });
      } catch (error) {
        if (this.jobs.get(job.id).status !== "cancelled") await this.jobs.transition(job.id, "failed", { error: error instanceof Error ? error.message : String(error) });
      }
    })();
    return job;
  }

  async removeModel(sha256: string): Promise<void> {
    await this.models.remove(sha256, await this.extensions.modelDependents(sha256));
  }

  /** Connects a stdio MCP server, exposes its tools to agents and RPC and remembers it across restarts. */
  async registerMcpServer(descriptor: McpServerDescriptor): Promise<{ id: string; tools: string[] }> {
    const { tools } = await this.mcp.connect(descriptor);
    this.mcp.registerInto(this.registry, descriptor, tools);
    await writeMcpServer(this.dataRoot, descriptor);
    return { id: descriptor.id, tools: tools.map((tool) => tool.name) };
  }

  async unregisterMcpServer(serverId: string): Promise<void> {
    await this.mcp.disconnect(serverId);
    await removeMcpServer(this.dataRoot, serverId);
  }
  async shutdown(): Promise<void> {
    await this.supervisor.stop();
    await this.mcp.shutdown();
  }

  status() {
    return {
      name: "queryn-runtime" as const, version: "0.2.0", status: "ready" as const,
      capabilities: ["projects", "extensions", "operations", "jobs", "artifacts", "sessions", "context", "connectors", "models", "agent", "oci-optional"],
      openProjects: this.projects.list().map((project) => project.rootPath),
      runtimes: this.supervisor.status()
    };
  }

  async startRuntime(runtimeId: string, projectPath: string) {
    const project = this.projects.get(projectPath);
    const descriptor = this.extensions.getRuntime(runtimeId, project.rootPath);
    const directories = await createInvocationDirectories(runtimeInvocationScopeRoot(this.dataRoot, descriptor, project.rootPath), `health-${Date.now()}`);
    try {
      await stageModels(descriptor.models ?? [], this.models, directories.models);
      const health = await this.supervisor.call<Record<string, unknown>>(descriptor, "health", {}, { paths: directories, timeoutMs: 30_000 });
      return { runtimeId, health, state: this.supervisor.status(runtimeId)[0] };
    } finally { await removeInvocationDirectories(directories.root); }
  }

  #registerModelProvider(config: ModelProviderConfig): void {
    if (config.type === "openai-compatible") this.agent.registerProvider(new OpenAICompatibleProvider(config.id, config.endpoint, this.credentials, config.credentialAccount));
  }
}

export function defaultRuntimeDataRoot(): string {
  if (process.env.QUERYN_RUNTIME_HOME) return path.resolve(process.env.QUERYN_RUNTIME_HOME);
  if (process.platform === "darwin") return path.join(os.homedir(), "Library", "Application Support", "Queryn", "runtime");
  if (process.platform === "win32") return path.join(process.env.LOCALAPPDATA ?? os.homedir(), "Queryn", "runtime");
  return path.join(os.homedir(), ".local", "share", "queryn", "runtime");
}

function registerBuiltins(runtime: QuerynRuntime): void {
  register(runtime, {
    id: "queryn.project.search", toolId: "queryn.project", version: "1.0.0", title: "Search project materials",
    description: "Full-text search across project notes and artifacts. Returns ranked matches with short snippets.",
    inputSchema: { type: "object", required: ["query"], additionalProperties: false, properties: { query: { type: "string", minLength: 1 }, limit: { type: "integer", minimum: 1, maximum: 20 } } },
    outputSchema: { type: "object", required: ["matches"], properties: { matches: { type: "array" }, engine: { type: "string" } } },
    risk: "safe-read", agentVisibility: "automatic", execution: "immediate", cancellable: false, idempotent: true,
    permissions: ["project:read"]
  }, async ({ projectPath, arguments: args }) => {
    const matches = await runtime.indexer.search(projectPath, String(args.query), typeof args.limit === "number" ? args.limit : 10);
    return { structured: { matches } };
  });

  register(runtime, {
    id: "queryn.project.read", toolId: "queryn.project", version: "1.0.0", title: "Read project material",
    description: "Read the content of one note or text file by its project-relative path. Respects the material's context policy and sensitivity.",
    inputSchema: { type: "object", required: ["path"], additionalProperties: false, properties: { path: { type: "string", minLength: 1 }, maxChars: { type: "integer", minimum: 500, maximum: 20_000 } } },
    outputSchema: { type: "object", required: ["content"], properties: { content: { type: "string" }, truncated: { type: "boolean" }, sensitivity: { type: "string" } } },
    risk: "safe-read", agentVisibility: "automatic", execution: "immediate", cancellable: false, idempotent: true,
    permissions: ["project:read"]
  }, async ({ projectPath, arguments: args, recipient }) => {
    const relativePath = String(args.path);
    const policy = await runtime.context.describeMaterialPolicy(projectPath, relativePath);
    if (policy.mode === "none") {
      return { structured: { content: "", message: "This material is excluded from agent context by its policy.", sensitivity: policy.sensitive ? "sensitive" : "project" } };
    }
    if (policy.sensitive && recipient === "cloud") {
      throw new Error("Material is marked sensitive and cannot be read into a cloud model context.");
    }
    const maxChars = typeof args.maxChars === "number" ? args.maxChars : 8_000;
    try {
      const note = await readNote(projectPath, relativePath);
      const body = note.body.length > maxChars ? `${note.body.slice(0, maxChars)}…` : note.body;
      return { structured: { path: relativePath, title: note.summary.title, content: body, truncated: note.body.length > maxChars, sensitivity: policy.sensitive ? "sensitive" : "project" } };
    } catch {
      const asset = (await listAssets(projectPath)).find((candidate) => candidate.relativePath === relativePath);
      if (!asset) throw new Error(`Project material not found: ${relativePath}. Paths are project-relative (no drive or project-name prefixes); find the exact path with queryn.project.search by title or queryn.project.list with a folder.`);
      const mediaType = asset.mediaType ?? "";
      const isText = mediaType.startsWith("text/") || /\.(?:md|txt|c|cc|cpp|css|csv|go|h|hpp|html|ini|java|js|jsx|kt|log|mjs|py|rb|rs|sh|sql|svg|toml|ts|tsx|xml|ya?ml)$/i.test(relativePath);
      if (!isText) return { structured: { path: relativePath, name: asset.name, content: `Binary file (${mediaType || "unknown"}, ${asset.size} bytes); text preview unavailable.`, truncated: false, sensitivity: policy.sensitive ? "sensitive" : "project" } };
      const limited = await readTextPrefixForMaterial(projectPath, relativePath, maxChars);
      return { structured: { path: relativePath, name: asset.name, content: limited.text, truncated: limited.truncated, sensitivity: policy.sensitive ? "sensitive" : "project" } };
    }
  });

  register(runtime, {
    id: "queryn.project.list", toolId: "queryn.project", version: "1.0.0", title: "List project materials",
    description: "List notes, files and registered artifacts with titles and project-relative paths. Large projects are paginated: pass a folder (path prefix, e.g. \"notes/02_domains\") to narrow the listing.",
    inputSchema: { type: "object", additionalProperties: false, properties: { folder: { type: "string" } } },
    outputSchema: { type: "object", required: ["items"], properties: { items: { type: "array" }, truncated: { type: "boolean" } } },
    risk: "safe-read", agentVisibility: "automatic", execution: "immediate", cancellable: false, idempotent: true,
    permissions: ["project:read"]
  }, async ({ projectPath, arguments: args }) => {
    const folder = typeof args.folder === "string" && args.folder.trim() ? `${args.folder.trim().replace(/\/+$/, "")}/` : "";
    const matchesFolder = (relativePath: string): boolean => !folder || relativePath.startsWith(folder);
    const [notes, assets, artifacts] = await Promise.all([listNotes(projectPath), listAssets(projectPath), listArtifacts(projectPath)]);
    const filteredNotes = notes.filter((note) => matchesFolder(note.relativePath));
    const filteredAssets = assets.filter((asset) => matchesFolder(asset.relativePath));
    const filteredArtifacts = artifacts.filter((artifact) => matchesFolder(artifact.id));
    const items = [
      ...filteredNotes.slice(0, 80).map((note) => ({ kind: "note", title: note.title, path: note.relativePath })),
      ...filteredAssets.slice(0, 60).map((asset) => ({ kind: "file", title: asset.name, path: asset.relativePath })),
      ...filteredArtifacts.slice(0, 40).map((artifact) => ({ kind: "artifact", title: artifact.title ?? artifact.id, artifactId: artifact.id }))
    ];
    const total = filteredNotes.length + filteredAssets.length + filteredArtifacts.length;
    return {
      structured: {
        counts: { notes: filteredNotes.length, files: filteredAssets.length, artifacts: filteredArtifacts.length },
        truncated: items.length < total,
        hint: items.length < total ? `Showing ${items.length} of ${total}. Pass a folder prefix like the shown paths' directories to see more.` : undefined,
        items
      }
    };
  });
  register(runtime, {
    id: "queryn.web.fetch", toolId: "queryn.web", version: "1.0.0", title: "Fetch web page",
    description: "Fetch a public http(s) page by URL and return its readable text. Use it when the user gives a concrete link or asks for content of a known page.",
    inputSchema: { type: "object", required: ["url"], additionalProperties: false, properties: { url: { type: "string" }, maxChars: { type: "integer", minimum: 500, maximum: 20_000 } } },
    outputSchema: { type: "object", required: ["text"], properties: { url: { type: "string" }, title: { type: "string" }, text: { type: "string" }, truncated: { type: "boolean" } } },
    risk: "network-egress", agentVisibility: "automatic", execution: "immediate", cancellable: false, idempotent: true,
    permissions: ["network:use"]
  }, async ({ arguments: args }) => {
    const page = await fetchPageText(String(args.url), typeof args.maxChars === "number" ? args.maxChars : 8_000);
    return { structured: page };
  });

  register(runtime, {
    id: "queryn.artifact.resolve", toolId: "queryn.project", version: "1.0.0", title: "Resolve artifact context",
    description: "Resolve one registered artifact into readable context, honoring its context mode (automatic/declarative/custom providers).",
    inputSchema: { type: "object", required: ["artifactId"], additionalProperties: false, properties: { artifactId: { type: "string", minLength: 1 }, level: { type: "string", enum: ["compact", "expanded"] }, budgetTokens: { type: "integer", minimum: 256, maximum: 32_000 } } },
    outputSchema: { type: "object", required: ["text"], properties: { text: { type: "string" }, truncated: { type: "boolean" }, sensitivity: { type: "string" } } },
    risk: "safe-read", agentVisibility: "automatic", execution: "immediate", cancellable: false, idempotent: true,
    permissions: ["project:read", "artifact:read"]
  }, async ({ projectPath, arguments: args, recipient }) => {
    const envelope = await runtime.context.resolveOne({
      projectPath,
      artifactId: String(args.artifactId),
      level: args.level === "compact" ? "compact" : "expanded",
      budgetTokens: typeof args.budgetTokens === "number" ? args.budgetTokens : 4_000,
      recipient: recipient ?? "local"
    });
    if (!envelope.allowedRecipients.includes(recipient ?? "local")) {
      throw new Error(`Artifact cannot be resolved for recipient ${recipient ?? "local"}.`);
    }
    return { structured: { artifactId: String(args.artifactId), text: envelope.text ?? "", sources: envelope.sources, truncated: envelope.truncated, sensitivity: envelope.sensitivity } };
  });

  register(runtime, {

    id: "queryn.notes.create", toolId: "queryn.notes", version: "1.0.0", title: "Create note",
    description: "Create a Markdown note inside the project and register it as an artifact.",
    inputSchema: { type: "object", required: ["title"], additionalProperties: false, properties: { title: { type: "string", minLength: 1 }, body: { type: "string" }, folder: { type: "string" }, tags: { type: "array", items: { type: "string" } } } },
    outputSchema: { type: "object", required: ["artifactId", "relativePath"], properties: { artifactId: { type: "string" }, relativePath: { type: "string" } } },
    produces: ["queryn.note"], risk: "project-write", agentVisibility: "automatic", execution: "immediate", cancellable: false, idempotent: false,
    permissions: ["project:read", "artifact:create"]
  }, async ({ projectPath, arguments: args, provenance }) => {
    const project = runtime.projects.get(projectPath);
    const note = await createNote(project, { title: String(args.title), body: typeof args.body === "string" ? args.body : undefined, folderRelativePath: typeof args.folder === "string" ? args.folder : undefined, tags: Array.isArray(args.tags) ? args.tags.filter((tag): tag is string => typeof tag === "string") : undefined });
    const artifact = await registerExistingArtifact(project, { type: "queryn.note", title: note.title, projectRelativePath: note.relativePath, provenance: { source: "operation", toolId: "queryn.notes", operationId: "queryn.notes.create", runId: provenance.runId, model: provenance.model }, context: { mode: "automatic" } });
    return { structured: { artifactId: artifact.id, relativePath: note.relativePath }, publishedArtifactIds: [artifact.id] };
  });

  register(runtime, {
    id: "queryn.files.import", toolId: "queryn.files", version: "1.0.0", title: "Import file",
    inputSchema: { type: "object", required: ["sourcePath"], additionalProperties: false, properties: { sourcePath: { type: "string" }, folder: { type: "string" } } },
    outputSchema: { type: "object", required: ["artifactId", "relativePath"], properties: { artifactId: { type: "string" }, relativePath: { type: "string" } } },
    produces: ["queryn.file"], risk: "project-write", agentVisibility: "hidden", execution: "immediate", cancellable: false, idempotent: false,
    permissions: ["project:read", "artifact:create"]
  }, async ({ projectPath, arguments: args, provenance }) => {
    const project = runtime.projects.get(projectPath);
    const asset = await importAsset(project, { sourcePath: String(args.sourcePath), targetFolderRelativePath: typeof args.folder === "string" ? args.folder : undefined });
    const artifact = await registerExistingArtifact(project, { type: "queryn.file", title: asset.name, projectRelativePath: asset.relativePath, provenance: { source: "import", runId: provenance.runId, model: provenance.model }, context: { mode: "automatic" } });
    return { structured: { artifactId: artifact.id, relativePath: asset.relativePath }, publishedArtifactIds: [artifact.id] };
  });

  register(runtime, {
    id: "queryn.graph.link", toolId: "queryn.graph", version: "1.0.0", title: "Link artifacts",
    inputSchema: { type: "object", required: ["from", "to", "type"], additionalProperties: false, properties: { from: { type: "string" }, to: { type: "string" }, type: { type: "string", pattern: "^[a-z0-9][a-z0-9._-]+$" } } },
    outputSchema: { type: "object", required: ["relationId"], properties: { relationId: { type: "string" } } },
    risk: "project-write", agentVisibility: "automatic", execution: "immediate", cancellable: false, idempotent: false, permissions: ["project:read", "artifact:create"]
  }, async ({ projectPath, arguments: args }) => {
    const relation = await createArtifactRelation(projectPath, { from: { artifactId: String(args.from) }, to: { artifactId: String(args.to) }, type: String(args.type) });
    return { structured: { relationId: relation.id } };
  });
  register(runtime, {
    id: "queryn.session.search", toolId: "queryn.memory", version: "1.0.0", title: "Search past sessions",
    description: "Search past dialogue transcripts of this project. Available only when full memory is enabled for the current session.",
    inputSchema: { type: "object", required: ["query"], additionalProperties: false, properties: { query: { type: "string", minLength: 1 } } },
    outputSchema: { type: "object", required: ["matches"], properties: { matches: { type: "array" } } },
    risk: "safe-read", agentVisibility: "automatic", execution: "immediate", cancellable: false, idempotent: true,
    permissions: ["project:read"]
  }, async ({ projectPath, arguments: args }) => {
    const matches = await searchSessions(projectPath, String(args.query));
    return { structured: { matches } };
  });

  register(runtime, {
    id: "queryn.session.read", toolId: "queryn.memory", version: "1.0.0", title: "Read past session transcript",
    description: "Read the user/assistant transcript of one past session. Available only when full memory is enabled for the current session.",
    inputSchema: { type: "object", required: ["sessionId"], additionalProperties: false, properties: { sessionId: { type: "string", minLength: 1 } } },
    outputSchema: { type: "object", required: ["title", "text"], properties: { title: { type: "string" }, text: { type: "string" }, truncated: { type: "boolean" } } },
    risk: "safe-read", agentVisibility: "automatic", execution: "immediate", cancellable: false, idempotent: true,
    permissions: ["project:read"]
  }, async ({ projectPath, arguments: args }) => {
    const result = await readSessionTranscript(projectPath, String(args.sessionId));
    return { structured: result };
  });

  register(runtime, {
    id: "queryn.context.reindex", toolId: "queryn.knowledge", version: "1.0.0", title: "Rebuild project index",
    inputSchema: { type: "object", additionalProperties: false, properties: {} },
    outputSchema: { type: "object", required: ["indexed", "engine"], properties: { indexed: { type: "integer" }, engine: { type: "string" } } },
    risk: "safe-read", agentVisibility: "explicit", execution: "job", timeoutSeconds: 120, cancellable: true, idempotent: true, permissions: ["project:read"]
  }, async ({ projectPath, progress }) => {
    progress(0.1, "Reading project materials");
    const result = await runtime.indexer.rebuild(projectPath);
    progress(1, "Index rebuilt");
    return { structured: result };
  });
}

function register(runtime: QuerynRuntime, definition: OperationDefinition, handler: Parameters<OperationRegistry["register"]>[1]): void {
  const runtimeDescriptor: RuntimeDescriptor = { id: "queryn.builtin", kind: "builtin", lifecycle: "shared" };
  runtime.registry.register({ definition, extensionId: "queryn.builtin", runtime: runtimeDescriptor }, handler);
}

const MCP_SERVERS_FILE = "mcp-servers.json";

async function readMcpServers(dataRoot: string): Promise<McpServerDescriptor[]> {
  try {
    const parsed = JSON.parse(await readFile(path.join(dataRoot, MCP_SERVERS_FILE), "utf8")) as { servers?: McpServerDescriptor[] };
    return Array.isArray(parsed.servers) ? parsed.servers.filter((server) => typeof server?.id === "string" && typeof server?.command === "string") : [];
  } catch { return []; }
}

async function writeMcpServer(dataRoot: string, descriptor: McpServerDescriptor): Promise<void> {
  const servers = (await readMcpServers(dataRoot)).filter((server) => server.id !== descriptor.id);
  servers.push(descriptor);
  await persistMcpServers(dataRoot, servers);
}

async function removeMcpServer(dataRoot: string, serverId: string): Promise<void> {
  await persistMcpServers(dataRoot, (await readMcpServers(dataRoot)).filter((server) => server.id !== serverId));
}

async function persistMcpServers(dataRoot: string, servers: McpServerDescriptor[]): Promise<void> {
  const filePath = path.join(dataRoot, MCP_SERVERS_FILE);
  if (!servers.length) {
    await rm(filePath, { force: true });
    return;
  }
  await mkdir(dataRoot, { recursive: true });
  const temporaryPath = `${filePath}.${process.pid}.tmp`;
  await writeFile(temporaryPath, JSON.stringify({ schemaVersion: "1", servers }, null, 2));
  await rename(temporaryPath, filePath);
}
