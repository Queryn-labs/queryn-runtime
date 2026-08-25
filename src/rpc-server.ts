import { randomBytes } from "node:crypto";
import { chmod, rm } from "node:fs/promises";
import net from "node:net";
import os from "node:os";
import path from "node:path";
import { appendSessionEvent, createSession, forkSession, listArtifacts, listSessions, readArtifact, readSessionEvents, registerExistingArtifact, updateSession } from "@osnova/project";
import type { ApprovalDecision } from "@osnova/types";
import type { OsnovaRuntime } from "./runtime.js";
import { writeJsonAtomic } from "./atomic.js";

export interface RpcServerHandle {
  address: string;
  token: string;
  close(): Promise<void>;
}

interface RpcRequest { jsonrpc: "2.0"; id?: string | number; method: string; params?: Record<string, unknown> }

export async function startRpcServer(runtime: OsnovaRuntime, options: { address?: string; token?: string } = {}): Promise<RpcServerHandle> {
  const token = options.token ?? randomBytes(32).toString("base64url");
  const address = options.address ?? createRpcAddress();
  const authenticated = new Set<net.Socket>();
  const server = net.createServer((socket) => {
    socket.setEncoding("utf8");
    let buffer = "";
    socket.on("data", (chunk: string) => {
      buffer += chunk;
      if (Buffer.byteLength(buffer, "utf8") > 8 * 1024 * 1024) { socket.destroy(new Error("RPC line exceeds 8 MiB.")); return; }
      let newline;
      while ((newline = buffer.indexOf("\n")) >= 0) {
        const line = buffer.slice(0, newline).replace(/\r$/, "");
        buffer = buffer.slice(newline + 1);
        if (line) void receive(runtime, socket, line, token, authenticated);
      }
    });
    socket.once("close", () => authenticated.delete(socket));
  });
  const changed = (job: { status?: string }) => {
    broadcast(authenticated, { jsonrpc: "2.0", method: "job.changed", params: job });
    if (job.status === "waiting-approval") broadcast(authenticated, { jsonrpc: "2.0", method: "approval.required", params: job });
  };
  const runtimeChanged = (state: unknown) => broadcast(authenticated, { jsonrpc: "2.0", method: "runtime.changed", params: state });
  const artifactPublished = (event: unknown) => broadcast(authenticated, { jsonrpc: "2.0", method: "artifact.published", params: event });
  const agentActivity = (event: unknown) => broadcast(authenticated, { jsonrpc: "2.0", method: "agent.activity", params: event });
  const agentOutputDelta = (event: unknown) => broadcast(authenticated, { jsonrpc: "2.0", method: "agent.output.delta", params: event });
  runtime.jobs.on("changed", changed);
  runtime.supervisor.on("changed", runtimeChanged);
  runtime.ingestor.on("published", artifactPublished);
  runtime.agent.on("activity", agentActivity);
  runtime.agent.on("output.delta", agentOutputDelta);
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(address, () => resolve());
  });
  if (process.platform !== "win32") await chmod(address, 0o600);
  await writeJsonAtomic(path.join(runtime.dataRoot, "rpc.json"), { address, token, pid: process.pid, startedAt: new Date().toISOString(), protocol: "osnova-rpc/1" });
  return {
    address, token,
    async close() {
      runtime.jobs.off("changed", changed);
      runtime.supervisor.off("changed", runtimeChanged);
      runtime.ingestor.off("published", artifactPublished);
      runtime.agent.off("activity", agentActivity);
      runtime.agent.off("output.delta", agentOutputDelta);
      for (const socket of authenticated) socket.destroy();
      await new Promise<void>((resolve, reject) => server.close((error) => error ? reject(error) : resolve()));
      if (process.platform !== "win32") await rm(address, { force: true });
      await rm(path.join(runtime.dataRoot, "rpc.json"), { force: true });
    }
  };
}

async function receive(runtime: OsnovaRuntime, socket: net.Socket, line: string, token: string, authenticated: Set<net.Socket>): Promise<void> {
  let request: RpcRequest;
  try { request = JSON.parse(line) as RpcRequest; }
  catch { return send(socket, { jsonrpc: "2.0", id: null, error: { code: -32700, message: "Parse error" } }); }
  if (request.jsonrpc !== "2.0" || typeof request.method !== "string") return send(socket, { jsonrpc: "2.0", id: request.id ?? null, error: { code: -32600, message: "Invalid Request" } });
  const params = { ...(request.params ?? {}) };
  const suppliedToken = params._auth;
  delete params._auth;
  if (suppliedToken !== token) return send(socket, { jsonrpc: "2.0", id: request.id ?? null, error: { code: -32001, message: "Unauthorized" } });
  authenticated.add(socket);
  if (request.id === undefined) return;
  try {
    const result = await dispatch(runtime, request.method, params);
    send(socket, { jsonrpc: "2.0", id: request.id, result });
  } catch (error) {
    send(socket, { jsonrpc: "2.0", id: request.id, error: { code: -32000, message: error instanceof Error ? error.message : String(error) } });
  }
}

async function dispatch(runtime: OsnovaRuntime, method: string, params: Record<string, unknown>): Promise<unknown> {
  const projectPath = () => requiredString(params, "projectPath");
  switch (method) {
    case "runtime.status": return runtime.status();
    case "runtime.start": return runtime.startRuntime(requiredString(params, "runtimeId"), projectPath());
    case "runtime.stop": await runtime.supervisor.stop(optionalString(params.runtimeId)); return runtime.status();
    case "project.create": return runtime.projects.create({ rootPath: projectPath(), id: requiredString(params, "id"), name: requiredString(params, "name"), description: optionalString(params.description) });
    case "project.open": return runtime.openProject(projectPath());
    case "project.validate": return runtime.projects.validate(projectPath());
    case "project.migrate": return runtime.projects.migrate(projectPath(), { dryRun: params.dryRun === true });
    case "project.inspect-adoption": return runtime.projects.inspectAdoption(projectPath());
    case "project.adopt": return runtime.projects.adopt(projectPath(), { id: optionalString(params.id), name: optionalString(params.name), description: optionalString(params.description) }, { dryRun: params.dryRun === true });
    case "extension.install": return runtime.extensions.install(requiredString(params, "packagePath"), {
      allowUnsigned: params.allowUnsigned === true, signature: optionalString(params.signature), publicKey: optionalString(params.publicKey)
    });
    case "extension.update": return runtime.extensions.install(requiredString(params, "packagePath"), { allowUnsigned: params.allowUnsigned === true, signature: optionalString(params.signature), publicKey: optionalString(params.publicKey) });
    case "extension.rollback": await runtime.extensions.rollback(requiredString(params, "extensionId"), requiredString(params, "version")); return { ok: true };
    case "extension.connect": await runtime.extensions.connect(projectPath(), requiredString(params, "extensionId"), requiredString(params, "version"), (stringArray(params.permissions) ?? []) as never, optionalString(params.requirementVersion)); return { ok: true };
    case "extension.disconnect": await runtime.extensions.disconnect(projectPath(), requiredString(params, "extensionId")); return { ok: true };
    case "extension.list": return runtime.extensions.list();
    case "operation.list": return runtime.registry.list({ includeHidden: params.includeHidden === true, extensionVersions: optionalString(params.projectPath) ? runtime.projects.extensionVersions(requiredString(params, "projectPath")) : undefined });
    case "operation.invoke": return runtime.operations.invoke({
      projectPath: projectPath(), operationId: requiredString(params, "operationId"), arguments: record(params.arguments),
      sessionId: optionalString(params.sessionId), artifactIds: stringArray(params.artifactIds), publishArtifacts: params.publishArtifacts === true
    });
    case "approval.decide": return runtime.operations.decide(requiredString(params, "jobId"), params.decision as unknown as ApprovalDecision);
    case "artifact.import": {
      const project = runtime.projects.get(projectPath());
      const artifact = await registerExistingArtifact(project, {
        type: requiredString(params, "type"), title: optionalString(params.title), projectRelativePath: requiredString(params, "projectRelativePath"), context: params.context as never
      });
      runtime.ingestor.notifyPublished(project, [artifact]);
      return artifact;
    }
    case "artifact.publish": return runtime.operations.publishPending(requiredString(params, "jobId"), numberArray(params.indexes));
    case "artifact.read": return readArtifact(projectPath(), requiredString(params, "artifactId"));
    case "artifact.list": return listArtifacts(projectPath());
    case "session.create": return createSession(runtime.projects.get(projectPath()), { title: requiredString(params, "title"), goal: optionalString(params.goal), context: params.context as never });
    case "session.fork": return forkSession(runtime.projects.get(projectPath()), {
      sourceSessionId: requiredString(params, "sessionId"),
      throughEventId: requiredString(params, "throughEventId"),
      title: optionalString(params.title)
    });
    case "session.append": return appendSessionEvent(projectPath(), requiredString(params, "sessionId"), { type: requiredString(params, "type") as never, data: record(params.data) });
    case "session.update": return updateSession(projectPath(), requiredString(params, "sessionId"), sessionUpdatePatch(params));
    case "session.list": return listSessions(projectPath());
    case "session.events": return readSessionEvents(projectPath(), requiredString(params, "sessionId"));
    case "context.preview": return runtime.context.preview(projectPath());
    case "context.resolve": return runtime.context.resolve({ projectPath: projectPath(), artifactIds: stringArray(params.artifactIds), level: params.level === "expanded" ? "expanded" : "compact", budgetTokens: optionalNumber(params.budgetTokens) ?? 2_000, recipient: params.recipient === "cloud" ? "cloud" : "local", approval: params.approval as ApprovalDecision | undefined });
    case "context.reindex": return runtime.indexer.rebuild(projectPath());
    case "context.search": return runtime.indexer.search(projectPath(), requiredString(params, "query"), optionalNumber(params.limit));
    case "connector.list": return runtime.connectors.list();
    case "connector.sync": return runtime.syncConnector(projectPath(), requiredString(params, "connectorId"), params.approval as ApprovalDecision | undefined);
    case "agent.chat": return runtime.agent.chat({ projectPath: projectPath(), goal: requiredString(params, "goal"), sessionId: optionalString(params.sessionId), providerId: optionalString(params.providerId), model: requiredString(params, "model"), maxSteps: optionalNumber(params.maxSteps), maxDurationSeconds: optionalNumber(params.maxDurationSeconds), historyBudgetTokens: optionalNumber(params.historyBudgetTokens), recipientApproval: params.recipientApproval as never, requestId: optionalString(params.requestId) });
    case "agent.chat.cancel": return { cancelled: runtime.agent.cancelChat(requiredString(params, "requestId")) };
    case "agent.chat.get": return runtime.agent.getChat(requiredString(params, "runId"));
    case "agent.chat.resume": return runtime.agent.resumeChat(requiredString(params, "runId"));
    case "agent.chat.approve": return runtime.agent.approveChat(requiredString(params, "runId"), params.decision as unknown as ApprovalDecision);
    case "job.get": return runtime.jobs.get(requiredString(params, "jobId"));
    case "job.list": return runtime.jobs.list(optionalString(params.projectPath));
    case "job.cancel": return runtime.jobs.cancel(requiredString(params, "jobId"));
    case "job.subscribe": return { subscribed: true };
    case "model.install": return runtime.models.install(params.dependency as never, { allowNetwork: params.allowNetwork === true });
    case "model.list": return runtime.models.list();
    case "model.provider.configure": return runtime.configureModelProvider(params.config as never, optionalString(params.secret));
    case "model.provider.config-list": return runtime.listModelProviderConfigs();
    case "model.provider.list": return runtime.agent.listProviders();
    case "model.provider.models-list": return runtime.agent.listProviderModels();
    case "credential.remove": await runtime.credentials.delete(requiredString(params, "account")); return { ok: true };
    case "model.remove": await runtime.removeModel(requiredString(params, "sha256")); return { ok: true };
    case "diagnostics.doctor": return runtime.diagnostics.doctor(optionalString(params.projectPath));
    case "diagnostics.export": {
      const report = await runtime.diagnostics.doctor(optionalString(params.projectPath));
      const target = requiredString(params, "outputPath");
      await writeJsonAtomic(target, report);
      return { outputPath: target };
    }
    default: throw Object.assign(new Error(`Method not found: ${method}`), { code: -32601 });
  }
}

function send(socket: net.Socket, message: unknown): void { if (!socket.destroyed) socket.write(`${JSON.stringify(message)}\n`); }
function broadcast(sockets: Set<net.Socket>, message: unknown): void { for (const socket of sockets) send(socket, message); }
function createRpcAddress(): string { return process.platform === "win32" ? `\\\\.\\pipe\\osnova-${randomBytes(16).toString("hex")}` : path.join(os.tmpdir(), `osnova-${process.getuid?.() ?? "user"}-${randomBytes(12).toString("hex")}.sock`); }
function requiredString(params: Record<string, unknown>, key: string): string { const value = params[key]; if (typeof value !== "string" || !value) throw new Error(`${key} is required.`); return value; }
function optionalString(value: unknown): string | undefined { return typeof value === "string" ? value : undefined; }
function optionalNumber(value: unknown): number | undefined { return typeof value === "number" && Number.isFinite(value) ? value : undefined; }

function sessionUpdatePatch(params: Record<string, unknown>): { title?: string; goal?: string; memoryMode?: "full" | "off" } {
  const patch: { title?: string; goal?: string; memoryMode?: "full" | "off" } = {};
  const title = optionalString(params.title);
  const goal = optionalString(params.goal);
  if (title !== undefined) patch.title = title;
  if (goal !== undefined) patch.goal = goal;
  if (params.memoryMode !== undefined) {
    if (params.memoryMode !== "full" && params.memoryMode !== "off") throw new Error("memoryMode must be \"full\" or \"off\".");
    patch.memoryMode = params.memoryMode;
  }
  return patch;
}
function stringArray(value: unknown): string[] | undefined { return Array.isArray(value) ? value.filter((entry): entry is string => typeof entry === "string") : undefined; }
function numberArray(value: unknown): number[] | undefined { return Array.isArray(value) ? value.filter((entry): entry is number => typeof entry === "number") : undefined; }
function record(value: unknown): Record<string, unknown> { if (typeof value !== "object" || value === null || Array.isArray(value)) return {}; return value as Record<string, unknown>; }
