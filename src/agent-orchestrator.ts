import { randomUUID } from "node:crypto";
import { EventEmitter } from "node:events";
import { appendSessionEvent } from "@osnova/project";
import type { ApprovalDecision } from "@osnova/types";
import type { AgentKernel, ChatRun } from "./agent-kernel.js";
import type { ModelProvider } from "./model-provider.js";
import type { OperationRegistry } from "./operation-registry.js";
import type { OperationService } from "./operation-service.js";

export interface CreateChatInput {
  projectPath: string;
  sessionId?: string;
  goal: string;
  providerId?: string;
  model?: string;
  maxSteps?: number;
  maxDurationSeconds?: number;
  historyBudgetTokens?: number;
  recipientApproval?: { recipient: "cloud"; approved: boolean; decidedAt: string };
  providerApproval?: ApprovalDecision;
  requestId?: string;
}

export interface AgentOutputDelta {
  requestId: string;
  projectPath: string;
  sessionId?: string;
  delta: string;
}

export interface AgentActivity {
  requestId: string;
  projectPath: string;
  sessionId?: string;
  kind: "progress" | "tool";
  status: "running" | "completed" | "failed";
  stage?: string;
  title: string;
  detail?: string;
  message?: string;
  operationId?: string;
  durationMs?: number;
}

export class AgentOrchestrator extends EventEmitter {
  readonly #providers = new Map<string, ModelProvider>();

  constructor(
    readonly registry: OperationRegistry,
    readonly operations: OperationService,
    readonly kernel: AgentKernel
  ) {
    super();
    kernel.on("activity", (event) => this.emit("activity", event));
    kernel.on("output.delta", (event) => this.emit("output.delta", event));
  }

  registerProvider(provider: ModelProvider): void { this.#providers.set(provider.id, provider); }
  listProviders(): Array<{ id: string; recipient: "local" | "cloud"; sourceExtensionId?: string; permissions: string[]; risk: string }> {
    return [...this.#providers.values()].map(({ id, recipient, sourceExtensionId, permissions = [], risk = "safe-read" }) => ({ id, recipient, sourceExtensionId, permissions, risk }));
  }

  /** Tool-loop engine: one conversational agent turn per user message. */
  async chat(input: CreateChatInput): Promise<ChatRun> {
    const provider = input.providerId ? this.#providers.get(input.providerId) : undefined;
    const model = input.model;
    if (!provider || !model) throw new Error("Agent chat requires a configured model provider and model.");
    await this.#authorizeProviderUse(input, provider);
    return this.kernel.start(provider, { ...input, model });
  }

  cancelChat(requestId: string): boolean { return this.kernel.cancel(requestId); }

  async getChat(runId: string): Promise<ChatRun> { return this.kernel.get(runId); }

  async resumeChat(runId: string): Promise<ChatRun> {
    const run = await this.kernel.get(runId);
    const provider = this.#providers.get(run.providerId);
    if (!provider) throw new Error(`Model provider is not configured anymore: ${run.providerId}`);
    return this.kernel.resume(provider, runId);
  }

  async approveChat(runId: string, decision: ApprovalDecision): Promise<ChatRun> {
    const run = await this.kernel.get(runId);
    if (run.status !== "waiting-approval" || !run.pendingJobId) throw new Error(`Chat run is not waiting for approval: ${runId}`);
    await this.operations.decide(run.pendingJobId, { ...decision, planId: run.id, stepId: run.pendingJobId });
    const provider = this.#providers.get(run.providerId);
    if (!provider) throw new Error(`Model provider is not configured anymore: ${run.providerId}`);
    return this.kernel.resume(provider, runId);
  }

  async #authorizeProviderUse(
    input: { projectPath: string; sessionId?: string; recipientApproval?: { recipient: "cloud"; approved: boolean; decidedAt: string }; providerApproval?: ApprovalDecision },
    provider: ModelProvider
  ): Promise<void> {
    if (!provider.sourceExtensionId) return;
    const connected = this.operations.projects.get(input.projectPath).manifest.extensions?.some((extension) => extension.id === provider.sourceExtensionId && extension.enabled !== false);
    if (!connected) throw new Error(`Model provider extension is not connected to this project: ${provider.sourceExtensionId}`);
    const evaluation = this.operations.policy.evaluate(input.projectPath, provider.sourceExtensionId, {
      id: provider.id, toolId: provider.id, version: "1", title: provider.id, inputSchema: {}, outputSchema: {},
      permissions: provider.permissions ?? [], risk: provider.risk ?? "safe-read", agentVisibility: "hidden", execution: "immediate"
    });
    if (!evaluation.allowed) throw new Error(`${evaluation.reason} Missing: ${evaluation.missingPermissions.join(", ")}`);
    const cloudApproval = provider.recipient === "cloud" && input.recipientApproval?.approved;
    if (evaluation.approvalRequired && !(input.providerApproval?.approved || cloudApproval)) throw new Error(`Model provider ${provider.id} requires explicit runtime approval.`);
    if (input.providerApproval) {
      await this.operations.policy.rememberApproval(input.projectPath, provider.id, input.providerApproval);
      if (input.sessionId) await appendSessionEvent(input.projectPath, input.sessionId, {
        type: "approval", data: {
          kind: "model-provider-runtime", providerId: provider.id,
          permissions: provider.permissions ?? [], risk: provider.risk ?? "safe-read",
          approved: input.providerApproval.approved, scope: input.providerApproval.scope,
          decidedAt: input.providerApproval.decidedAt
        }
      });
    }
  }
}
