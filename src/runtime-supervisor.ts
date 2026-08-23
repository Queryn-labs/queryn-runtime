import { spawn, type ChildProcessWithoutNullStreams } from "node:child_process";
import { createHash } from "node:crypto";
import { EventEmitter } from "node:events";
import { lstat, mkdir, mkdtemp, readdir, realpath, rm, writeFile } from "node:fs/promises";
import { realpathSync } from "node:fs";
import os from "node:os";
import path from "node:path";
import type { OperationDefinition, RuntimeDescriptor, RuntimeState } from "@osnova/types";
import { httpJsonRpc, StdioToolClient } from "./tool-client.js";
import type { BuiltinOperationHandler } from "./operation-registry.js";
import { assertSafeRelativePath } from "./atomic.js";

export interface RuntimeInvocation {
  jobId: string;
  projectId: string;
  sessionId?: string;
  operation: OperationDefinition;
  arguments: Record<string, unknown>;
  inputPath: string;
  workPath: string;
  outboxPath: string;
  modelsPath: string;
  signal: AbortSignal;
  progress(value: number, message?: string): void;
  builtin?: BuiltinOperationHandler;
  projectPath: string;
  provenanceRunId?: string;
  model?: string;
  recipient?: "local" | "cloud";
}

export interface RuntimeInvocationResult {
  structured?: Record<string, unknown>;
  /** Trusted built-ins can report artifacts they published through core project APIs. */
  publishedArtifactIds?: string[];
  artifacts?: Array<{
    id?: string; type: string; title?: string;
    payloads: Array<{ path: string; mediaType?: string; role?: string; contentBase64?: string }>;
    context?: { mode: "none" | "automatic" | "declarative" | "custom"; providerId?: string; fields?: string[]; template?: string };
    tags?: string[]; metadata?: Record<string, unknown>;
  }>;
  message?: string;
}

interface ManagedProcess {
  key: string;
  runtime: RuntimeDescriptor;
  child: ChildProcessWithoutNullStreams;
  client: StdioToolClient;
  logs: string[];
  active: number;
  closing: boolean;
  idleTimer?: NodeJS.Timeout;
}

export class RuntimeSupervisor extends EventEmitter {
  readonly #states = new Map<string, RuntimeState>();
  readonly #processes = new Map<string, ManagedProcess>();
  readonly #starting = new Map<string, Promise<ManagedProcess>>();

  status(runtimeId?: string): RuntimeState[] {
    return [...this.#states.values()].filter((state) => !runtimeId || state.runtimeId === runtimeId).map((state) => structuredClone(state));
  }

  async invoke(runtime: RuntimeDescriptor | undefined, invocation: RuntimeInvocation): Promise<RuntimeInvocationResult> {
    if (!runtime || runtime.kind === "builtin") {
      if (!invocation.builtin) throw new Error(`No builtin handler for ${invocation.operation.id}.`);
      const structured = await invocation.builtin({
        projectPath: invocation.projectPath, sessionId: invocation.sessionId, jobId: invocation.jobId,
        arguments: invocation.arguments, outboxPath: invocation.outboxPath, signal: invocation.signal, progress: invocation.progress,
        provenance: { runId: invocation.provenanceRunId ?? invocation.jobId, model: invocation.model },
        recipient: invocation.recipient
      });
      return structured as RuntimeInvocationResult;
    }
    if (runtime.kind === "remote") this.#setState(runtime.id, "starting");
    try {
      const result = runtime.kind === "remote"
        ? await this.#invokeRemote(runtime, invocation)
        : await this.#invokeProcess(runtime, invocation);
      if (runtime.kind === "remote") this.#setState(runtime.id, "running");
      return result;
    } catch (error) {
      this.#setState(runtime.id, "failed", error instanceof Error ? error.message : String(error));
      throw error;
    }
  }

  async call<T>(runtime: RuntimeDescriptor, method: string, params: Record<string, unknown>, options: {
    paths: { input: string; work: string; outbox: string; models: string };
    signal?: AbortSignal;
    timeoutMs?: number;
  }): Promise<T> {
    const signal = options.signal ?? new AbortController().signal;
    if (runtime.kind === "remote") {
      if (!runtime.endpoint) throw new Error(`Remote runtime ${runtime.id} has no endpoint.`);
      assertSafeRemoteEndpoint(runtime.endpoint);
      const timeout = timeoutSignal(signal, options.timeoutMs ?? 30_000);
      try {
      if (runtime.protocol === "mcp" && method === "context/resolve") {
        const resourceUri = typeof params.resourceUri === "string" ? params.resourceUri : undefined;
        if (!resourceUri) throw new Error(`MCP context call for ${runtime.id} requires resourceUri.`);
        const response = await httpJsonRpc<{ contents?: Array<{ uri: string; mimeType?: string; text?: string; blob?: string }> }>(runtime.endpoint, "resources/read", { uri: resourceUri }, { signal: timeout.signal });
        const text = (response.contents ?? []).map((content) => typeof content.text === "string" ? content.text : `[Binary MCP resource: ${content.mimeType ?? "application/octet-stream"}]`).join("\n\n");
        return {
          level: params.level === "expanded" ? "expanded" : "compact", text,
          sources: [{ artifactId: String(params.artifactId), providerId: String(params.providerId) }],
          sensitivity: "project", allowedRecipients: ["local", "cloud"], tokenEstimate: Math.ceil(text.length / 4),
          truncated: false, freshness: new Date().toISOString(), providerVersion: "mcp.resources/1"
        } as T;
      }
      return await httpJsonRpc<T>(runtime.endpoint, method, params, { signal: timeout.signal });
      } finally { timeout.dispose(); }
    }
    const invocation: RuntimeInvocation = {
      jobId: `protocol-${Date.now()}`, projectId: "context", projectPath: "", arguments: {},
      operation: {
        id: `${runtime.id}.protocol-call`, toolId: `${runtime.id}.protocol`, version: "1", title: method,
        inputSchema: {}, outputSchema: {}, risk: "safe-read", agentVisibility: "hidden", execution: "immediate",
        permissions: [], timeoutSeconds: Math.ceil((options.timeoutMs ?? 30_000) / 1000)
      },
      inputPath: options.paths.input, workPath: options.paths.work, outboxPath: options.paths.outbox, modelsPath: options.paths.models,
      signal, progress() {}
    };
    const session = await this.#getProcess(runtime, invocation);
    session.active += 1;
    if (session.idleTimer) clearTimeout(session.idleTimer);
    const logOffset = session.logs.length;
    const budgetController = new AbortController();
    const callSignal = AbortSignal.any([signal, budgetController.signal]);
    const diskRoot = writableRuntimeScope(runtime, options.paths.input);
    const diskLimitBytes = (runtime.resources?.diskMb ?? 512) * 1024 * 1024;
    const stopDiskMonitor = monitorDirectoryBudget(diskRoot, diskLimitBytes, budgetController);
    try {
      const paths = runtime.kind === "oci"
        ? { input: "/osnova/input", work: "/osnova/work", outbox: "/osnova/outbox", models: "/osnova/models" }
        : options.paths;
      const result = await session.client.request<T>(method, { ...params, paths }, { signal: callSignal, timeoutMs: options.timeoutMs ?? 30_000 });
      await assertDirectoryBudget(diskRoot, diskLimitBytes);
      return result;
    } catch (error) {
      const detail = session.logs.slice(logOffset).join("").trim();
      const reason = budgetController.signal.aborted && budgetController.signal.reason instanceof Error ? budgetController.signal.reason : error;
      throw new Error(`${reason instanceof Error ? reason.message : String(reason)}${detail ? `\n${detail}` : ""}`);
    } finally {
      stopDiskMonitor();
      session.active -= 1;
      if (callSignal.aborted) await this.#shutdownProcess(session);
      else await this.#releaseProcess(session);
    }
  }

  async stop(runtimeId?: string): Promise<void> {
    const starting = [...this.#starting.values()];
    if (starting.length) await Promise.allSettled(starting);
    const sessions = [...this.#processes.values()].filter((session) => !runtimeId || session.runtime.id === runtimeId);
    await Promise.all(sessions.map((session) => this.#shutdownProcess(session)));
  }

  async #invokeRemote(runtime: RuntimeDescriptor, invocation: RuntimeInvocation): Promise<RuntimeInvocationResult> {
    if (!runtime.endpoint) throw new Error(`Remote runtime ${runtime.id} has no endpoint.`);
    assertSafeRemoteEndpoint(runtime.endpoint);
    const method = runtime.protocol === "mcp" ? "tools/call" : "jobs/start";
    const params = runtime.protocol === "mcp"
      ? { name: invocation.operation.id, arguments: invocation.arguments }
      : toolJobParams(invocation, false);
    const timeout = timeoutSignal(invocation.signal, (invocation.operation.timeoutSeconds ?? 300) * 1_000);
    let mcpTaskId: string | undefined;
    try {
      let result = await httpJsonRpc<RuntimeInvocationResult & { jobId?: string; status?: string; error?: string; task?: { taskId?: string; status?: string; error?: string }; result?: RuntimeInvocationResult }>(runtime.endpoint, method, params, { signal: timeout.signal });
      if (runtime.protocol === "mcp") {
        const taskId = result.task?.taskId;
        mcpTaskId = taskId;
        let status = result.task?.status;
        while (taskId && ["working", "queued", "running"].includes(status ?? "")) {
          await abortableDelay(250, timeout.signal);
          const task = await httpJsonRpc<{ taskId?: string; status?: string; error?: string; result?: RuntimeInvocationResult }>(runtime.endpoint, "tasks/get", { taskId }, { signal: timeout.signal });
          status = task.status;
          if (["failed", "cancelled"].includes(status ?? "")) throw new Error(task.error ?? `MCP task ${status}.`);
          if (status === "input_required") throw new Error("MCP task requires interactive input, which Osnova Tool Protocol v1 cannot supply.");
          if (status === "completed") return normalizeMcpResult(task.result ?? {});
        }
      } else {
        while (["queued", "running"].includes(result.status ?? "")) {
          await abortableDelay(250, timeout.signal);
          result = await httpJsonRpc(runtime.endpoint, "jobs/get", { jobId: result.jobId ?? invocation.jobId }, { signal: timeout.signal });
        }
        if (["failed", "cancelled"].includes(result.status ?? "")) throw new Error(result.error ?? `Remote tool job ${result.status}.`);
      }
      return await materializeInlinePayloads(normalizeMcpResult(result.result ?? result), invocation.outboxPath);
    } catch (error) {
      if (mcpTaskId && timeout.signal.aborted) {
        await httpJsonRpc(runtime.endpoint, "tasks/cancel", { taskId: mcpTaskId }, { signal: AbortSignal.timeout(2_000) }).catch(() => undefined);
      }
      throw error;
    } finally { timeout.dispose(); }
  }

  async #invokeProcess(runtime: RuntimeDescriptor, invocation: RuntimeInvocation): Promise<RuntimeInvocationResult> {
    const session = await this.#getProcess(runtime, invocation);
    session.active += 1;
    if (session.idleTimer) clearTimeout(session.idleTimer);
    const logOffset = session.logs.length;
    let killTimer: NodeJS.Timeout | undefined;
    const budgetController = new AbortController();
    const operationSignal = AbortSignal.any([invocation.signal, budgetController.signal]);
    const diskRoot = writableRuntimeScope(runtime, invocation.inputPath);
    const diskLimitBytes = (runtime.resources?.diskMb ?? 512) * 1024 * 1024;
    const stopDiskMonitor = monitorDirectoryBudget(diskRoot, diskLimitBytes, budgetController);
    const abort = () => {
      session.client.notify("jobs/cancel", { jobId: invocation.jobId });
      killTimer = setTimeout(() => { void this.#shutdownProcess(session); }, 1_000);
      killTimer.unref?.();
    };
    if (operationSignal.aborted) abort();
    else operationSignal.addEventListener("abort", abort, { once: true });
    const notification = (event: { method: string; params?: unknown }) => {
      if (event.method === "jobs/progress" && isProgress(event.params) && (!event.params.jobId || event.params.jobId === invocation.jobId)) {
        const notification = event.params;
        invocation.progress(notification.progress, notification.message);
      }
    };
    session.client.on("notification", notification);
    try {
      let result = await session.client.request<RuntimeInvocationResult & { jobId?: string; status?: string; error?: string }>("jobs/start", toolJobParams(invocation, runtime.kind === "oci"), {
        signal: operationSignal,
        timeoutMs: (invocation.operation.timeoutSeconds ?? 300) * 1000
      });
      while (result.status === "queued" || result.status === "running") {
        await abortableDelay(250, operationSignal);
        result = await session.client.request("jobs/get", { jobId: result.jobId ?? invocation.jobId }, { signal: operationSignal, timeoutMs: 10_000 });
      }
      if (result.status === "failed" || result.status === "cancelled") throw new Error(result.error ?? `Tool job ${result.status}.`);
      await assertDirectoryBudget(diskRoot, diskLimitBytes);
      return normalizeMcpResult(result);
    } catch (error) {
      const detail = session.logs.slice(logOffset).join("").trim();
      const reason = budgetController.signal.aborted && budgetController.signal.reason instanceof Error ? budgetController.signal.reason : error;
      throw new Error(`${reason instanceof Error ? reason.message : String(reason)}${detail ? `\n${detail}` : ""}`);
    } finally {
      stopDiskMonitor();
      operationSignal.removeEventListener("abort", abort);
      session.client.off("notification", notification);
      if (killTimer && !operationSignal.aborted) clearTimeout(killTimer);
      session.active -= 1;
      await this.#releaseProcess(session);
    }
  }

  async #getProcess(runtime: RuntimeDescriptor, invocation: RuntimeInvocation): Promise<ManagedProcess> {
    const key = this.#processKey(runtime, invocation);
    const existing = this.#processes.get(key);
    if (existing && !existing.closing && existing.child.exitCode === null) return existing;
    const pending = this.#starting.get(key);
    if (pending) return pending;
    const starting = this.#startProcess(key, runtime, invocation);
    this.#starting.set(key, starting);
    try { return await starting; }
    finally { this.#starting.delete(key); }
  }

  async #startProcess(key: string, runtime: RuntimeDescriptor, invocation: RuntimeInvocation): Promise<ManagedProcess> {
    const { command, args } = this.#command(runtime, invocation);
    const child = spawn(command, args, { stdio: ["pipe", "pipe", "pipe"], env: minimalEnvironment(), windowsHide: true });
    const client = new StdioToolClient(child);
    const session: ManagedProcess = { key, runtime, child, client, logs: [], active: 0, closing: false };
    client.on("log", (message: string) => {
      session.logs.push(message);
      if (session.logs.length > 200) session.logs.splice(0, session.logs.length - 200);
    });
    client.once("closed", (error: Error) => {
      const expected = session.closing;
      session.closing = true;
      if (this.#processes.get(key) === session) this.#processes.delete(key);
      if (!expected) this.#setState(runtime.id, "failed", error.message);
      else this.#markStoppedIfIdle(runtime.id);
    });
    this.#setState(runtime.id, "starting");
    try {
      await client.request("initialize", {
        protocolVersion: "1", client: { name: "osnova-runtime", version: "0.2.0" },
        capabilities: { jobs: true, cancellation: true, context: true, connectors: true }
      }, { timeoutMs: 10_000 });
      this.#processes.set(key, session);
      this.#setState(runtime.id, "running");
      return session;
    } catch (error) {
      session.closing = true;
      await client.shutdown();
      const detail = session.logs.join("").trim();
      throw new Error(`${error instanceof Error ? error.message : String(error)}${detail ? `\n${detail}` : ""}`);
    }
  }

  async #releaseProcess(session: ManagedProcess): Promise<void> {
    if (session.closing) return;
    if (session.runtime.lifecycle === "job" || session.runtime.kind === "oci") {
      await this.#shutdownProcess(session);
      return;
    }
    const idleSeconds = session.runtime.idleTimeoutSeconds ?? (session.runtime.lifecycle === "shared" ? 300 : undefined);
    if (idleSeconds && session.active === 0) {
      session.idleTimer = setTimeout(() => { void this.#shutdownProcess(session); }, idleSeconds * 1_000);
      session.idleTimer.unref?.();
    }
  }

  async #shutdownProcess(session: ManagedProcess): Promise<void> {
    if (session.closing) return;
    session.closing = true;
    if (session.idleTimer) clearTimeout(session.idleTimer);
    this.#setState(session.runtime.id, "stopping");
    await session.client.shutdown();
    if (this.#processes.get(session.key) === session) this.#processes.delete(session.key);
    this.#markStoppedIfIdle(session.runtime.id);
  }

  #processKey(runtime: RuntimeDescriptor, invocation: RuntimeInvocation): string {
    if (runtime.lifecycle === "job" || runtime.kind === "oci") return `${runtime.id}:job:${invocation.jobId}`;
    const scopeRoot = realpathSync(path.dirname(path.dirname(invocation.inputPath)));
    return `${runtime.id}:${runtime.lifecycle}:${scopeRoot}`;
  }

  #markStoppedIfIdle(runtimeId: string): void {
    if (![...this.#processes.values()].some((session) => session.runtime.id === runtimeId && !session.closing)) this.#setState(runtimeId, "stopped");
  }

  #command(runtime: RuntimeDescriptor, invocation: RuntimeInvocation): { command: string; args: string[] } {
    if (runtime.kind === "node-process") {
      if (!runtime.entry) throw new Error(`Runtime ${runtime.id} has no entry.`);
      const entry = realpathSync(runtime.entry);
      const persistentRoot = runtime.lifecycle === "job" ? undefined : realpathSync(path.dirname(path.dirname(invocation.inputPath)));
      return {
        command: process.execPath,
        args: [
          `--max-old-space-size=${Math.max(64, runtime.resources?.memoryMb ?? 512)}`,
          "--permission",
          ...(runtime.resources?.network && process.allowedNodeEnvironmentFlags.has("--allow-net") ? ["--allow-net"] : []),
          ...[path.dirname(entry), ...(persistentRoot ? [persistentRoot] : [invocation.inputPath, invocation.modelsPath])].map((root) => `--allow-fs-read=${realpathSync(root)}`),
          ...(persistentRoot ? [persistentRoot] : [invocation.workPath, invocation.outboxPath]).map((root) => `--allow-fs-write=${realpathSync(root)}`),
          entry
        ]
      };
    }
    if (runtime.kind === "native-process") {
      if (!runtime.entry) throw new Error(`Runtime ${runtime.id} has no entry.`);
      return { command: runtime.entry, args: [] };
    }
    if (runtime.kind === "oci") {
      if (!runtime.image || !/@sha256:[a-f0-9]{64}$/.test(runtime.image)) throw new Error("OCI image must be pinned by a full SHA-256 digest.");
      const args = ["run", "--rm", "-i", "--network", runtime.resources?.network ? "bridge" : "none", "--read-only", "--user", "65532:65532",
        "--mount", `type=bind,src=${invocation.inputPath},dst=/osnova/input,readonly`,
        "--mount", `type=bind,src=${invocation.outboxPath},dst=/osnova/outbox`,
        "--mount", `type=bind,src=${invocation.modelsPath},dst=/osnova/models,readonly`,
        "--pids-limit", "256", "--cap-drop", "ALL", "--security-opt", "no-new-privileges",
        "--memory", `${runtime.resources?.memoryMb ?? 1024}m`, "--cpus", String(runtime.resources?.cpu ?? 1),
        "--tmpfs", `/osnova/work:rw,nosuid,size=${runtime.resources?.diskMb ?? 512}m`,
        "--tmpfs", "/tmp:rw,noexec,nosuid,size=256m"];
      if (runtime.resources?.gpu) args.push("--gpus", "all");
      args.push(runtime.image);
      return { command: process.env.OSNOVA_OCI_COMMAND ?? "docker", args };
    }
    throw new Error(`Unsupported runtime kind: ${runtime.kind}`);
  }

  #setState(runtimeId: string, status: RuntimeState["status"], error?: string): void {
    const previous = this.#states.get(runtimeId);
    const state: RuntimeState = {
      runtimeId, status,
      startedAt: status === "running" ? previous?.startedAt ?? new Date().toISOString() : previous?.startedAt,
      lastUsedAt: new Date().toISOString(), error
    };
    this.#states.set(runtimeId, state);
    this.emit("changed", structuredClone(state));
  }
}

export async function createInvocationDirectories(runtimeRoot: string, jobId: string): Promise<{ root: string; input: string; work: string; outbox: string; models: string }> {
  await mkdir(runtimeRoot, { recursive: true });
  const root = await realpath(await mkdtemp(path.join(runtimeRoot, `${jobId}-`)));
  const paths = { root, input: path.join(root, "input"), work: path.join(root, "work"), outbox: path.join(root, "outbox"), models: path.join(root, "models") };
  await Promise.all([mkdir(paths.input), mkdir(paths.work), mkdir(paths.outbox), mkdir(paths.models)]);
  return paths;
}

export async function removeInvocationDirectories(root: string): Promise<void> { await rm(root, { recursive: true, force: true }); }

export function runtimeInvocationScopeRoot(dataRoot: string, runtime: RuntimeDescriptor | undefined, projectPath: string): string {
  const runtimeId = (runtime?.id ?? "osnova.builtin").replace(/[^a-zA-Z0-9._-]/g, "-");
  const lifecycle = runtime?.lifecycle ?? "job";
  const projectHash = createHash("sha256").update(projectPath).digest("hex").slice(0, 20);
  const scope = lifecycle === "shared" ? "shared" : lifecycle === "project" ? `project-${projectHash}` : "jobs";
  return path.join(dataRoot, "runs", runtimeId, scope);
}

function toolJobParams(invocation: RuntimeInvocation, inContainer: boolean): Record<string, unknown> {
  return {
    jobId: invocation.jobId, operationId: invocation.operation.id, input: invocation.arguments,
    sessionId: invocation.sessionId,
    paths: inContainer
      ? { input: "/osnova/input", work: "/osnova/work", outbox: "/osnova/outbox", models: "/osnova/models" }
      : { input: invocation.inputPath, work: invocation.workPath, outbox: invocation.outboxPath, models: invocation.modelsPath }
  };
}

function normalizeMcpResult(result: RuntimeInvocationResult & { content?: Array<{ type: string; text?: string }> }): RuntimeInvocationResult {
  if (!result.content) return result;
  return { ...result, message: result.content.filter((item) => item.type === "text").map((item) => item.text ?? "").join("\n") };
}

function isProgress(value: unknown): value is { progress: number; message?: string; jobId?: string } {
  return typeof value === "object" && value !== null && typeof (value as { progress?: unknown }).progress === "number";
}

function minimalEnvironment(): NodeJS.ProcessEnv {
  const keys = process.platform === "win32" ? ["PATH", "SystemRoot", "TEMP", "TMP"] : ["PATH", "TMPDIR", "LANG"];
  return Object.fromEntries(keys.flatMap((key) => process.env[key] ? [[key, process.env[key]]] : []));
}

function abortableDelay(ms: number, signal: AbortSignal): Promise<void> {
  return new Promise((resolve, reject) => {
    const timeout = setTimeout(() => { signal.removeEventListener("abort", abort); resolve(); }, ms);
    const abort = () => { clearTimeout(timeout); reject(signal.reason instanceof Error ? signal.reason : new Error("Aborted.")); };
    signal.addEventListener("abort", abort, { once: true });
  });
}

async function materializeInlinePayloads(result: RuntimeInvocationResult, outboxPath: string): Promise<RuntimeInvocationResult> {
  let totalBytes = 0;
  for (const artifact of result.artifacts ?? []) {
    for (const payload of artifact.payloads) {
      if (payload.contentBase64 === undefined) continue;
      if (!/^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/.test(payload.contentBase64)) throw new Error(`Remote payload is not valid base64: ${payload.path}`);
      const data = Buffer.from(payload.contentBase64, "base64");
      totalBytes += data.byteLength;
      if (totalBytes > 12 * 1024 * 1024) throw new Error("Remote inline payloads exceed 12 MiB.");
      const relativePath = assertSafeRelativePath(payload.path);
      const target = path.join(outboxPath, ...relativePath.split("/"));
      await mkdir(path.dirname(target), { recursive: true });
      await writeFile(target, data, { flag: "wx", mode: 0o600 });
      delete payload.contentBase64;
    }
  }
  return result;
}

function assertSafeRemoteEndpoint(endpoint: string): void {
  const url = new URL(endpoint);
  if (url.protocol !== "https:" && !(url.protocol === "http:" && ["127.0.0.1", "localhost", "::1"].includes(url.hostname))) {
    throw new Error("Remote runtimes require HTTPS unless they bind to loopback.");
  }
}

function timeoutSignal(parent: AbortSignal, timeoutMs: number): { signal: AbortSignal; dispose(): void } {
  const controller = new AbortController();
  const abort = () => controller.abort(parent.reason instanceof Error ? parent.reason : new Error("Request aborted."));
  parent.addEventListener("abort", abort, { once: true });
  const timer = setTimeout(() => controller.abort(new Error(`Tool request timed out after ${timeoutMs}ms.`)), Math.max(1, timeoutMs));
  timer.unref?.();
  return {
    signal: controller.signal,
    dispose() { clearTimeout(timer); parent.removeEventListener("abort", abort); }
  };
}

function writableRuntimeScope(runtime: RuntimeDescriptor, inputPath: string): string {
  const invocationRoot = path.dirname(inputPath);
  return runtime.lifecycle === "job" || runtime.kind === "oci" ? invocationRoot : path.dirname(invocationRoot);
}

function monitorDirectoryBudget(root: string, maxBytes: number, controller: AbortController): () => void {
  let scanning = false;
  const check = async () => {
    if (scanning || controller.signal.aborted) return;
    scanning = true;
    try { await assertDirectoryBudget(root, maxBytes); }
    catch (error) { controller.abort(error); }
    finally { scanning = false; }
  };
  const timer = setInterval(() => { void check(); }, 250);
  timer.unref?.();
  void check();
  return () => clearInterval(timer);
}

async function assertDirectoryBudget(root: string, maxBytes: number): Promise<void> {
  let total = 0;
  const pending = [root];
  while (pending.length) {
    const directory = pending.pop()!;
    let entries;
    try { entries = await readdir(directory, { withFileTypes: true }); }
    catch (error) {
      if (error instanceof Error && "code" in error && error.code === "ENOENT") continue;
      throw error;
    }
    for (const entry of entries) {
      const candidate = path.join(directory, entry.name);
      if (entry.isDirectory()) { pending.push(candidate); continue; }
      if (!entry.isFile()) continue;
      try { total += (await lstat(candidate)).size; }
      catch (error) {
        if (error instanceof Error && "code" in error && error.code === "ENOENT") continue;
        throw error;
      }
      if (total > maxBytes) throw new Error(`Tool writable data exceeds the declared ${Math.floor(maxBytes / 1024 / 1024)} MiB disk limit.`);
    }
  }
}
