import { randomUUID } from "node:crypto";
import { EventEmitter } from "node:events";
import { readFile } from "node:fs/promises";
import path from "node:path";
import { appendSessionEvent, listSessions, readSession, readSessionEvents } from "@queryn/project";
import type { JobDescriptor } from "@queryn/types";
import { writeJsonAtomic } from "./atomic.js";
import type { ContextBroker } from "./context-broker.js";
import { buildConversationHistory, trimHistoryToBudget } from "./history-builder.js";
import type { JobManager } from "./job-manager.js";
import type { ModelChatMessage, ModelProvider, ModelResponse, ModelToolCall, ToolSchema } from "./model-provider.js";
import type { OperationRegistry } from "./operation-registry.js";
import type { OperationService } from "./operation-service.js";
import { validateJsonSchema } from "./schema.js";
import type { ProjectIndexer } from "./context-broker.js";

const DEFAULT_MAX_STEPS = 24;
const MAX_STEPS_LIMIT = 50;
const DEFAULT_MAX_DURATION_SECONDS = 1_800;
const DEFAULT_HISTORY_BUDGET_TOKENS = 24_000;
const OBSERVATION_MAX_CHARS = 4_000;
export const PROGRESS_TOOL = "queryn.progress";

export type TokenCountSource = "provider" | "estimated" | "mixed";

export interface ModelGenerationMetrics {
  durationMs: number;
  ttftMs?: number;
  inputTokens?: number;
  outputTokens: number;
  tokensPerSecond?: number;
  finishReason?: string;
  tokenCountSource: Exclude<TokenCountSource, "mixed">;
}

export interface ChatRunMetrics {
  totalDurationMs: number;
  modelCalls: number;
  inputTokens?: number;
  outputTokens: number;
  tokenCountSource: TokenCountSource;
  finalResponse?: ModelGenerationMetrics;
}

export interface ChatRun {
  schemaVersion: "1";
  id: string;
  requestId: string;
  projectPath: string;
  sessionId?: string;
  goal: string;
  providerId: string;
  model: string;
  recipient: "local" | "cloud";
  status: "running" | "waiting-approval" | "succeeded" | "failed" | "cancelled";
  steps: number;
  maxSteps: number;
  maxDurationSeconds: number;
  historyBudgetTokens: number;
  pendingJobId?: string;
  pendingCallId?: string;
  pendingOperationId?: string;
  response?: string;
  metrics?: ChatRunMetrics;
  error?: string;
  createdAt: string;
  updatedAt: string;
}

export interface ChatInput {
  projectPath: string;
  sessionId?: string;
  goal: string;
  model: string;
  maxSteps?: number;
  maxDurationSeconds?: number;
  historyBudgetTokens?: number;
  recipientApproval?: { recipient: "cloud"; approved: boolean; decidedAt: string };
  requestId?: string;
}

export interface AgentKernelActivity {
  requestId: string;
  runId: string;
  projectPath: string;
  sessionId?: string;
  id: string;
  seq: number;
  kind: "phase" | "tool" | "progress";
  status: "running" | "completed" | "failed";
  title: string;
  detail?: string;
  operationId?: string;
  callId?: string;
  jobId?: string;
  durationMs?: number;
}

const TERMINAL_JOB_STATES = new Set(["succeeded", "failed", "cancelled", "interrupted"]);

export class AgentKernel extends EventEmitter {
  readonly #runsByRequest = new Map<string, AbortController>();
  readonly #runsByRunId = new Map<string, AbortController>();
  readonly #providersByRun = new Map<string, ModelProvider>();
  #activitySeq = new Map<string, number>();

  constructor(
    readonly dataRoot: string,
    readonly registry: OperationRegistry,
    readonly operations: OperationService,
    readonly jobs: JobManager,
    readonly context: ContextBroker,
    readonly indexer: ProjectIndexer
  ) { super(); }

  async start(provider: ModelProvider & { recipient: "local" | "cloud" }, input: ChatInput): Promise<ChatRun> {
    const requestId = input.requestId ?? randomUUID();
    if (!input.sessionId) throw new Error("Agent chat requires a session: the conversational history lives in the session log.");
    if (this.#runsByRequest.has(requestId)) throw new Error(`Agent request is already running: ${requestId}`);
    if (provider.recipient === "cloud" && !input.recipientApproval?.approved) {
      throw new Error("Cloud model planning requires explicit data-recipient approval.");
    }
    const run: ChatRun = {
      schemaVersion: "1",
      id: randomUUID(),
      requestId,
      projectPath: input.projectPath,
      sessionId: input.sessionId,
      goal: input.goal,
      providerId: provider.id,
      model: input.model,
      recipient: provider.recipient,
      status: "running",
      steps: 0,
      maxSteps: Math.min(Math.max(input.maxSteps ?? DEFAULT_MAX_STEPS, 1), MAX_STEPS_LIMIT),
      maxDurationSeconds: Math.min(Math.max(input.maxDurationSeconds ?? DEFAULT_MAX_DURATION_SECONDS, 1), 86_400),
      historyBudgetTokens: Math.min(Math.max(input.historyBudgetTokens ?? DEFAULT_HISTORY_BUDGET_TOKENS, 2_000), 200_000),
      createdAt: new Date().toISOString(),
      updatedAt: new Date().toISOString()
    };
    await this.#persist(run);
    const controller = new AbortController();
    this.#runsByRequest.set(requestId, controller);
    this.#runsByRunId.set(run.id, controller);
    this.#activitySeq.set(run.id, 0);
    this.#providersByRun.set(run.id, provider);
    try {
      return await this.#drive(run, provider, controller.signal);
    } finally {
      this.#runsByRequest.delete(requestId);
      this.#runsByRunId.delete(run.id);
      this.#activitySeq.delete(run.id);
      this.#providersByRun.delete(run.id);
    }
  }

  async resume(provider: ModelProvider & { recipient: "local" | "cloud" }, runId: string): Promise<ChatRun> {
    const run = await this.get(runId);
    if (!["running", "waiting-approval"].includes(run.status)) return run;
    const controller = new AbortController();
    this.#runsByRequest.set(run.requestId, controller);
    this.#runsByRunId.set(run.id, controller);
    this.#activitySeq.set(run.id, this.#activitySeq.get(run.id) ?? 0);
    this.#providersByRun.set(run.id, provider);
    try {
      return await this.#drive(run, provider, controller.signal);
    } finally {
      this.#runsByRequest.delete(run.requestId);
      this.#runsByRunId.delete(run.id);
      this.#activitySeq.delete(run.id);
      this.#providersByRun.delete(run.id);
    }
  }

  cancel(requestId: string): boolean {
    const controller = this.#runsByRequest.get(requestId);
    if (!controller) return false;
    controller.abort(new Error("Agent response was cancelled."));
    return true;
  }

  cancelRun(runId: string): boolean {
    const controller = this.#runsByRunId.get(runId);
    if (!controller) return false;
    controller.abort(new Error("Agent response was cancelled."));
    return true;
  }

  async get(runId: string): Promise<ChatRun> {
    return JSON.parse(await readFile(path.join(this.dataRoot, "chat-runs", `${runId}.json`), "utf8")) as ChatRun;
  }

  async #drive(started: ChatRun, provider: ModelProvider, cancellationSignal: AbortSignal): Promise<ChatRun> {
    const run = started;
    const deadline = Date.parse(run.createdAt) + run.maxDurationSeconds * 1_000;
    try {
      while (true) {
        if (cancellationSignal.aborted) throw new Error("Agent response was cancelled.");
        // Resume a tool call that was suspended waiting for approval.
        if (run.pendingJobId && run.pendingCallId && run.pendingOperationId) {
          const completed = await this.#awaitJobTerminal(run.pendingJobId, Math.max(1_000, deadline - Date.now()), cancellationSignal);
          if (completed.status === "waiting-approval") {
            run.status = "waiting-approval";
            await this.#persist(run);
            return run;
          }
          await this.#recordObservation(run, completed, run.pendingCallId, run.pendingOperationId);
          run.pendingJobId = undefined;
          run.pendingCallId = undefined;
          run.pendingOperationId = undefined;
        }
        run.status = "running";
        if (Date.now() >= deadline) throw new Error("Agent run exceeded its time budget.");
        const events = run.sessionId ? await readSessionEvents(run.projectPath, run.sessionId) : [];
        const messages: ModelChatMessage[] = [
          { role: "system", content: await this.#systemPrompt(run) },
          ...buildConversationHistory(events).messages
        ];
        trimHistoryToBudget(messages, run.historyBudgetTokens);
        const finalizing = run.steps >= run.maxSteps - 1;
        if (finalizing) messages.push({ role: "system", content: "You have reached the step limit. Do not call any tools. Write your final answer now." });
        const memoryMode = await this.#sessionMemoryMode(run);
        const tools = finalizing ? undefined : await this.#toolSchemas(run.projectPath, memoryMode);
        let response: ModelResponse;
        const modelCallStartedAt = performance.now();
        let firstTextDeltaAt: number | undefined;
        try {
          response = await provider.complete({
            projectPath: run.projectPath,
            model: run.model,
            messages,
            tools,
            maxTokens: 32_768,
            temperature: 0,
            signal: cancellationSignal,
            onTextDelta: (delta) => {
              if (firstTextDeltaAt === undefined) firstTextDeltaAt = performance.now();
              this.emit("output.delta", { requestId: run.requestId, projectPath: run.projectPath, sessionId: run.sessionId, delta });
            }
          });
        } catch (error) {
          if (cancellationSignal.aborted) throw new Error("Agent response was cancelled.");
          throw error;
        }
        const generationMetrics = createGenerationMetrics(response, modelCallStartedAt, firstTextDeltaAt, performance.now());
        recordGenerationMetrics(run, generationMetrics);
        run.steps += 1;
        if (response.toolCalls?.length && !finalizing) {
          if (run.sessionId && response.text?.trim()) {
            await appendSessionEvent(run.projectPath, run.sessionId, {
              type: "assistant-message",
              data: { content: response.text, providerId: run.providerId, model: run.model, runId: run.id, interim: true }
            });
          }
          let suspended = false;
          for (const call of response.toolCalls) {
            if (cancellationSignal.aborted) throw new Error("Agent response was cancelled.");
            suspended = await this.#executeToolCall(run, call);
            if (suspended) break;
          }
          if (suspended) {
            run.status = "waiting-approval";
            await this.#persist(run);
            return run;
          }
          continue;
        }
        if (response.text?.trim()) {
          run.response = response.text;
          run.status = "succeeded";
          finalizeRunMetrics(run, generationMetrics);
          if (run.sessionId) {
            await appendSessionEvent(run.projectPath, run.sessionId, {
              type: "assistant-message",
              data: { content: response.text, providerId: run.providerId, model: run.model, runId: run.id, metrics: run.metrics }
            });
          }
          await this.#persist(run);
          return run;
        }
        throw new Error("Model returned neither text nor tool calls.");
      }
    } catch (error) {
      const messageText = error instanceof Error ? error.message : String(error);
      const cancelled = cancellationSignal.aborted || /cancel|abort/i.test(messageText);
      run.status = cancelled ? "cancelled" : "failed";
      run.error = messageText;
      if (run.metrics) run.metrics.totalDurationMs = elapsedRunTime(run);
      await this.#persist(run);
      return run;
    }
  }

  /** Executes one tool call; returns true when suspended waiting for approval. */
  async #executeToolCall(run: ChatRun, call: ModelToolCall): Promise<boolean> {
    const callId = call.id || randomUUID();
    const startedAt = Date.now();
    let parsedArguments: Record<string, unknown> = {};
    try { parsedArguments = JSON.parse(call.argumentsJson || "{}") as Record<string, unknown>; }
    catch { parsedArguments = {}; }

    if (call.name === PROGRESS_TOOL) {
      const message = typeof parsedArguments.message === "string" && parsedArguments.message.trim() ? parsedArguments.message.trim() : "Работаю";
      await this.#recordToolEvents(run, callId, PROGRESS_TOOL, { message }, { ok: true, content: "ok", artifactIds: [] });
      this.#activity(run, { kind: "progress", status: "completed", title: message, durationMs: Date.now() - startedAt });
      return false;
    }

    let definition;
    try {
      definition = this.registry.get(call.name, this.operations.projects.extensionVersions(run.projectPath)).definition;
    } catch {
      await this.#recordToolEvents(run, callId, call.name, parsedArguments, { ok: false, content: `Unknown tool: ${call.name}. Use one of the listed tools.`, artifactIds: [] });
      this.#activity(run, { kind: "tool", status: "failed", title: `Неизвестный инструмент: ${call.name}`, operationId: call.name, durationMs: Date.now() - startedAt });
      return false;
    }
    if (definition.agentVisibility === "hidden" || !this.#availableOperations(run.projectPath).some((operation) => operation.definition.id === definition.id)) {
      await this.#recordToolEvents(run, callId, call.name, parsedArguments, { ok: false, content: `Tool ${call.name} is not available in this project.`, artifactIds: [] });
      this.#activity(run, { kind: "tool", status: "failed", title: `Инструмент недоступен: ${call.name}`, operationId: call.name, durationMs: Date.now() - startedAt });
      return false;
    }
    const validation = validateJsonSchema(definition.inputSchema, parsedArguments);
    if (!validation.valid) {
      await this.#recordToolEvents(run, callId, call.name, parsedArguments, { ok: false, content: `Invalid arguments for ${call.name}: ${validation.issues.join(" ")}`, artifactIds: [] });
      this.#activity(run, { kind: "tool", status: "failed", title: `Некорректные аргументы: ${definition.title ?? call.name}`, detail: validation.issues.join(" "), operationId: call.name, durationMs: Date.now() - startedAt });
      return false;
    }

    const phrase = describeToolCall(definition, parsedArguments);
    this.#activity(run, { kind: "tool", status: "running", title: phrase.title, detail: phrase.detail, operationId: call.name, callId });

    // The assistant turn must exist in the log before its observation is appended.
    // see queryn-docs/docs/adr/adr-0012-unified-agent-loop.md
    await appendSessionEvent(run.projectPath, run.sessionId!, {
      type: "tool-call",
      data: { requestId: run.requestId, runId: run.id, callId, operationId: call.name, arguments: parsedArguments }
    }).catch(() => undefined);

    let job: JobDescriptor;
    try {
      job = await this.operations.invoke({
        projectPath: run.projectPath,
        sessionId: run.sessionId,
        operationId: call.name,
        arguments: parsedArguments,
        publishArtifacts: true,
        provenanceRunId: run.id,
        model: run.model,
        recipient: run.recipient
      });
    } catch (error) {
      const reason = error instanceof Error ? error.message : String(error);
      await this.#appendObservationEvent(run, callId, call.name, { ok: false, content: `Tool could not start: ${reason}`, artifactIds: [] });
      this.#activity(run, { kind: "tool", status: "failed", title: phrase.title, detail: reason, operationId: call.name, callId, durationMs: Date.now() - startedAt });
      return false;
    }

    if (job.status === "waiting-approval") {
      run.pendingJobId = job.id;
      run.pendingCallId = callId;
      run.pendingOperationId = call.name;
      this.#activity(run, { kind: "tool", status: "running", title: phrase.title, detail: phrase.detail, operationId: call.name, callId, jobId: job.id });
      return true;
    }

    const completed = await this.#awaitJobTerminal(job.id, Math.max(1_000, Date.parse(run.createdAt) + run.maxDurationSeconds * 1_000 - Date.now()), new AbortController().signal);
    await this.#recordObservation(run, completed, callId, call.name, Date.now() - startedAt, phrase);
    return false;
  }

  async #recordObservation(run: ChatRun, job: JobDescriptor, callId: string, operationId: string, durationMs = 0, phrase?: { title: string }): Promise<void> {
    const observation = observationFromJob(job);
    await this.#appendObservationEvent(run, callId, operationId, observation);
    const baseTitle = phrase?.title ?? activityTitleForObservation(operationId);
    if (job.status === "succeeded") {
      this.#activity(run, { kind: "tool", status: "completed", title: baseTitle, operationId, callId, jobId: job.id, durationMs });
      // Written material must become searchable without waiting for an explicit reindex.
      const registered = (() => { try { return this.registry.get(operationId, this.operations.projects.extensionVersions(run.projectPath)); } catch { return undefined; } })();
      if (registered && ["project-write", "external-side-effect"].includes(registered.definition.risk)) {
        void this.indexer.rebuild(run.projectPath).catch(() => undefined);
      }
    } else {
      this.#activity(run, { kind: "tool", status: "failed", title: `${baseTitle} — не удалось`, detail: job.error, operationId, callId, jobId: job.id, durationMs });
    }
  }

  async #recordToolEvents(run: ChatRun, callId: string, operationId: string, toolArguments: Record<string, unknown>, observation: { ok: boolean; content: string; artifactIds: string[] }): Promise<void> {
    if (run.sessionId) {
      await appendSessionEvent(run.projectPath, run.sessionId, {
        type: "tool-call",
        data: { requestId: run.requestId, runId: run.id, callId, operationId, arguments: toolArguments }
      });
      await this.#appendObservationEvent(run, callId, operationId, observation);
    }
  }

  async #appendObservationEvent(run: ChatRun, callId: string, operationId: string, observation: { ok: boolean; content: string; artifactIds: string[] }): Promise<void> {
    if (!run.sessionId) return;
    await appendSessionEvent(run.projectPath, run.sessionId, {
      type: "observation",
      data: {
        requestId: run.requestId, runId: run.id, callId, operationId,
        ok: observation.ok,
        content: cap(observation.content, OBSERVATION_MAX_CHARS),
        artifactIds: observation.artifactIds
      }
    });
  }

  async #awaitJobTerminal(jobId: string, timeoutMs: number, cancellationSignal: AbortSignal): Promise<JobDescriptor> {
    const current = this.jobs.get(jobId);
    if (TERMINAL_JOB_STATES.has(current.status) || current.status === "waiting-approval") return current;
    return new Promise<JobDescriptor>((resolve, reject) => {
      const timeout = setTimeout(() => {
        this.jobs.off("changed", changed);
        reject(new Error("Agent run exceeded its time budget while a tool was running."));
      }, Math.max(1, timeoutMs));
      timeout.unref?.();
      const onAbort = () => { clearTimeout(timeout); this.jobs.off("changed", changed); resolve(this.jobs.get(jobId)); };
      if (cancellationSignal.aborted) return onAbort();
      cancellationSignal.addEventListener("abort", onAbort, { once: true });
      const changed = (job: JobDescriptor) => {
        if (job.id !== jobId) return;
        if (TERMINAL_JOB_STATES.has(job.status) || job.status === "waiting-approval") {
          clearTimeout(timeout);
          this.jobs.off("changed", changed);
          cancellationSignal.removeEventListener("abort", onAbort);
          resolve(job);
        }
      };
      this.jobs.on("changed", changed);
    });
  }

  async #systemPrompt(run: ChatRun): Promise<string> {
    const lines = [
      "You are the Queryn project agent working locally inside the user's knowledge project.",
      "Rules:",
      "1. Answer greetings, small talk and general-knowledge questions directly without any tools.",
      "2. Use project tools only when the answer depends on this project's materials. Before stating facts about the project, discover them with tools: queryn.project.search / queryn.project.list, then queryn.project.read or queryn.artifact.resolve for specifics.",
      "3. Prefer one targeted search over several broad ones; never repeat an identical search.",
      `4. Call ${PROGRESS_TOOL} with {"message": "..."} whenever you switch to a new phase of work. Use a short phrase in the user's language.`,
      "5. While working on a multi-step task, occasionally write a short paragraph (before calling the next tool) summarizing what you have established so far and what you will do next, in the user's language.",
      "6. Modify the project only when the user explicitly asks for changes.",
      "7. Never invent contents you have not read with a tool.",
      "8. When you have enough information, stop calling tools and write the final answer in the user's language."
    ];
    try {
      const past = (await listSessions(run.projectPath))
        .sort((a, b) => b.createdAt.localeCompare(a.createdAt))
        .slice(0, 10)
        .map((session) => `- ${session.title} (${session.id})`)
        .join("\n");
      if (past) lines.push(`Past sessions:\n${past}`);
      if (run.sessionId && (await readSession(run.projectPath, run.sessionId)).memoryMode === "full") {
        lines.push("Full memory is enabled for this session: you can search past dialogues with queryn.session.search and read them with queryn.session.read.");
      }
    } catch { /* A missing session catalog must not break the loop. */ }
    if (run.sessionId) {
      try {
        const session = await readSession(run.projectPath, run.sessionId);
        const pinned = session.context?.map((reference) => `- ${reference.artifactId}`).join("\n");
        if (pinned) lines.push(`Pinned artifacts for this session:\n${pinned}`);
      } catch { /* A missing session descriptor must not break the loop. */ }
    }
    return lines.join("\n");
  }

  async #toolSchemas(projectPath: string, memoryMode: "full" | "off"): Promise<ToolSchema[]> {
    const schemas: ToolSchema[] = [{
      name: PROGRESS_TOOL,
      description: "Report progress to the user when you switch to a new phase of work.",
      parameters: { type: "object", required: ["message"], additionalProperties: false, properties: { message: { type: "string" } } }
    }];
    for (const operation of this.#availableOperations(projectPath)) {
      if (operation.definition.agentVisibility !== "automatic") continue;
      if (operation.definition.id.startsWith("queryn.session.") && memoryMode !== "full") continue;
      schemas.push({
        name: operation.definition.id,
        description: operation.definition.description ?? operation.definition.title,
        parameters: operation.definition.inputSchema
      });
    }
    return schemas;
  }

  async #sessionMemoryMode(run: ChatRun): Promise<"full" | "off"> {
    if (!run.sessionId) return "off";
    try {
      return (await readSession(run.projectPath, run.sessionId)).memoryMode === "full" ? "full" : "off";
    } catch { return "off"; }
  }

  #availableOperations(projectPath: string) {
    const project = this.operations.projects.get(projectPath);
    const enabled = new Set((project.manifest.extensions ?? []).filter((extension) => extension.enabled !== false).map((extension) => extension.id));
    // Operator-connected MCP servers participate without per-project manifest grants.
    return this.registry.list({ extensionVersions: this.operations.projects.extensionVersions(projectPath) }).filter((operation) =>
      operation.extensionId === "queryn.builtin" || operation.extensionId.startsWith("queryn.mcp.") || enabled.has(operation.extensionId)
    );
  }

  #activity(
    run: ChatRun,
    event: Omit<AgentKernelActivity, "requestId" | "runId" | "projectPath" | "sessionId" | "id" | "seq">
  ): void {
    const seq = (this.#activitySeq.get(run.id) ?? 0) + 1;
    this.#activitySeq.set(run.id, seq);
    this.emit("activity", {
      requestId: run.requestId,
      runId: run.id,
      projectPath: run.projectPath,
      sessionId: run.sessionId,
      id: `${run.id}-${seq}`,
      seq,
      ...event
    } satisfies AgentKernelActivity);
  }

  async #persist(run: ChatRun): Promise<void> {
    run.updatedAt = new Date().toISOString();
    await writeJsonAtomic(path.join(this.dataRoot, "chat-runs", `${run.id}.json`), run);
  }
}

function createGenerationMetrics(response: ModelResponse, startedAt: number, firstTextDeltaAt: number | undefined, completedAt: number): ModelGenerationMetrics {
  const providerInputTokens = finiteTokenCount(response.usage?.inputTokens);
  const providerOutputTokens = finiteTokenCount(response.usage?.outputTokens);
  const outputTokens = providerOutputTokens ?? estimateOutputTokens(response.text);
  const durationMs = roundMetric(Math.max(0, completedAt - startedAt));
  const ttftMs = firstTextDeltaAt === undefined ? undefined : roundMetric(Math.max(0, firstTextDeltaAt - startedAt));
  const generationDurationMs = firstTextDeltaAt === undefined ? undefined : Math.max(1, completedAt - firstTextDeltaAt);
  const tokensPerSecond = generationDurationMs === undefined || outputTokens <= 0
    ? undefined
    : roundMetric(outputTokens / (generationDurationMs / 1_000));
  return {
    durationMs,
    ...(ttftMs === undefined ? {} : { ttftMs }),
    ...(providerInputTokens === undefined ? {} : { inputTokens: providerInputTokens }),
    outputTokens,
    ...(tokensPerSecond === undefined ? {} : { tokensPerSecond }),
    ...(response.finishReason ? { finishReason: response.finishReason } : {}),
    tokenCountSource: providerOutputTokens === undefined ? "estimated" : "provider"
  };
}

function recordGenerationMetrics(run: ChatRun, generation: ModelGenerationMetrics): void {
  const previous = run.metrics;
  const previousCalls = previous?.modelCalls ?? 0;
  const previousOutputTokens = previous?.outputTokens ?? 0;
  const hasCompleteInputTokens = previousCalls === 0
    ? generation.inputTokens !== undefined
    : previous?.inputTokens !== undefined && generation.inputTokens !== undefined;
  const tokenCountSource = previousCalls === 0 || previousOutputTokens === 0
    ? generation.tokenCountSource
    : generation.outputTokens === 0 || previous?.tokenCountSource === generation.tokenCountSource
      ? previous?.tokenCountSource ?? generation.tokenCountSource
      : "mixed";
  run.metrics = {
    totalDurationMs: elapsedRunTime(run),
    modelCalls: previousCalls + 1,
    ...(hasCompleteInputTokens ? { inputTokens: (previous?.inputTokens ?? 0) + (generation.inputTokens ?? 0) } : {}),
    outputTokens: previousOutputTokens + generation.outputTokens,
    tokenCountSource,
    ...(previous?.finalResponse ? { finalResponse: previous.finalResponse } : {})
  };
}

function finalizeRunMetrics(run: ChatRun, finalResponse: ModelGenerationMetrics): void {
  const metrics = run.metrics ?? {
    totalDurationMs: elapsedRunTime(run),
    modelCalls: 1,
    ...(finalResponse.inputTokens === undefined ? {} : { inputTokens: finalResponse.inputTokens }),
    outputTokens: finalResponse.outputTokens,
    tokenCountSource: finalResponse.tokenCountSource
  };
  run.metrics = { ...metrics, totalDurationMs: elapsedRunTime(run), finalResponse };
}

function elapsedRunTime(run: ChatRun): number {
  const startedAt = Date.parse(run.createdAt);
  return Number.isFinite(startedAt) ? Math.max(0, Date.now() - startedAt) : 0;
}

function finiteTokenCount(value: number | undefined): number | undefined {
  return typeof value === "number" && Number.isFinite(value) && value >= 0 ? Math.round(value) : undefined;
}

function estimateOutputTokens(text: string): number {
  const normalized = text.trim();
  return normalized ? Math.max(1, Math.ceil(normalized.length / 4)) : 0;
}

function roundMetric(value: number): number {
  return Math.round(value * 10) / 10;
}

function observationFromJob(job: JobDescriptor): { ok: boolean; content: string; artifactIds: string[] } {
  if (job.status === "cancelled" || job.status === "interrupted") {
    return { ok: false, content: `The user declined or cancelled this action (${job.status}). Continue without it or ask how to proceed.`, artifactIds: [] };
  }
  if (job.status === "failed") {
    return { ok: false, content: `Tool failed: ${job.error ?? "unknown error"}. You may retry with corrected arguments.`, artifactIds: [] };
  }
  const parts: string[] = [];
  const structured = job.result?.structured as Record<string, unknown> | undefined;
  if (structured && Object.keys(structured).length) parts.push(JSON.stringify(structured));
  if (typeof job.result?.message === "string" && job.result.message) parts.push(job.result.message);
  const artifactIds = job.artifactIds ?? [];
  if (artifactIds.length) parts.push(`Published artifacts: ${artifactIds.join(", ")}`);
  return { ok: true, content: parts.join("\n") || "Done.", artifactIds };
}

function describeToolCall(definition: { id: string; title: string; risk: string }, args: Record<string, unknown>): { title: string; detail?: string } {
  const query = typeof args.query === "string" ? args.query : undefined;
  const notePath = typeof args.path === "string" ? args.path : undefined;
  switch (definition.id) {
    case "queryn.project.search": return { title: query ? `Ищу в проекте: ${query}` : "Ищу в проекте" };
    case "queryn.project.read": return { title: notePath ? `Изучаю содержимое: ${notePath}` : "Изучаю содержимое проекта" };
    case "queryn.project.list": return { title: "Просматриваю каталог проекта" };
    case "queryn.artifact.resolve": return { title: typeof args.artifactId === "string" ? `Открываю артефакт: ${String(args.artifactId).slice(0, 24)}…` : "Открываю артефакт" };
    case "queryn.notes.create": return { title: typeof args.title === "string" ? `Создаю заметку: ${args.title}` : "Создаю заметку" };
    case "queryn.files.import": return { title: "Импортирую файл в проект" };
    case "queryn.graph.link": return { title: "Связываю материалы проекта" };
    case "queryn.context.reindex": return { title: "Пересобираю индекс проекта" };
    default: return { title: definition.title || definition.id };
  }
}

function activityTitleForObservation(operationId: string): string {
  const map: Record<string, string> = {
    "queryn.project.search": "Поиск выполнен",
    "queryn.project.read": "Материал прочитан",
    "queryn.project.list": "Каталог просмотрен",
    "queryn.artifact.resolve": "Артефакт прочитан",
    "queryn.notes.create": "Заметка создана",
    "queryn.files.import": "Файл импортирован",
    "queryn.graph.link": "Связь создана",
    "queryn.context.reindex": "Индекс пересобран"
  };
  return map[operationId] ?? operationId;
}

function cap(text: string, maxChars: number): string {
  return text.length <= maxChars ? text : `${text.slice(0, Math.max(0, maxChars - 20))}\n…[truncated]`;
}
