import type { OperationRisk, Permission, RecipientKind } from "@queryn/types";
import type { CredentialStore } from "./credential-store.js";
import { readBoundedJsonResponse } from "./tool-client.js";

const CONTEXT_SELECTION_MAX_TOKENS = 16_384;
const AGENT_REPLY_MAX_TOKENS = 32_768;
const AGENT_PLAN_MAX_TOKENS = 16_384;

export interface ToolSchema {
  name: string;
  description?: string;
  parameters?: Record<string, unknown>;
}

export interface ModelToolCall {
  id: string;
  name: string;
  argumentsJson: string;
}

export type ModelChatMessage =
  | { role: "system"; content: string }
  | { role: "user"; content: string }
  | { role: "assistant"; content?: string; toolCalls?: ModelToolCall[] }
  | { role: "tool"; toolCallId: string; name?: string; content: string };

export interface ModelRequest {
  projectPath?: string;
  model: string;
  messages: ModelChatMessage[];
  temperature?: number;
  maxTokens?: number;
  responseSchema?: Record<string, unknown>;
  tools?: ToolSchema[];
  signal?: AbortSignal;
  onTextDelta?: (delta: string) => void;
}

export interface ModelResponse {
  text: string;
  model: string;
  usage?: { inputTokens?: number; outputTokens?: number };
  toolCalls?: ModelToolCall[];
  finishReason?: string;
}

export interface ModelProviderModel {
  id: string;
  ownedBy?: string;
  created?: number;
}

export interface ModelProvider {
  id: string;
  recipient: "local" | "cloud";
  sourceExtensionId?: string;
  permissions?: Permission[];
  risk?: OperationRisk;
  complete(request: ModelRequest): Promise<ModelResponse>;
  listModels?(): Promise<ModelProviderModel[]>;
}

export class OpenAICompatibleProvider implements ModelProvider {
  constructor(
    readonly id: string,
    readonly endpoint: string,
    readonly credentials: CredentialStore,
    readonly recipient: RecipientKind,
    readonly credentialAccount?: string
  ) {}

  async complete(request: ModelRequest): Promise<ModelResponse> {
    const key = this.credentialAccount ? await this.credentials.get(this.credentialAccount) : undefined;
    const streaming = Boolean(request.onTextDelta);
    const response = await fetch(new URL("chat/completions", ensureSlash(this.endpoint)), {
      method: "POST", signal: request.signal,
      headers: { "content-type": "application/json", ...(key ? { authorization: `Bearer ${key}` } : {}) },
      body: JSON.stringify({
        model: request.model,
        messages: request.messages.map(serializeMessage),
        temperature: request.temperature ?? 0,
        max_tokens: request.maxTokens,
        stream: streaming,
        ...(streaming ? { stream_options: { include_usage: true } } : {}),
        ...(request.tools?.length ? {
          tools: request.tools.map((tool) => ({ type: "function", function: { name: tool.name, description: tool.description ?? "", parameters: tool.parameters ?? { type: "object", properties: {} } } })),
          tool_choice: "auto"
        } : {}),
        ...(request.responseSchema ? { response_format: { type: "json_schema", json_schema: { name: "queryn_response", strict: true, schema: request.responseSchema } } } : {})
      })
    });
    if (!response.ok) throw new Error(`Model provider returned HTTP ${response.status}.`);
    if (streaming && response.body && response.headers.get("content-type")?.includes("text/event-stream")) {
      return readStreamingResponse(response, request.model, request.onTextDelta!);
    }
    const body = await readBoundedJsonResponse(response) as {
      model?: string;
      choices?: Array<{ message?: { content?: string | null; tool_calls?: Array<{ id?: string; function?: { name?: string; arguments?: string } }> }; finish_reason?: string | null }>;
      usage?: { prompt_tokens?: number; completion_tokens?: number };
    };
    const choice = body.choices?.[0];
    const text = typeof choice?.message?.content === "string" ? choice.message.content : "";
    const toolCalls = normalizeToolCalls(choice?.message?.tool_calls);
    if (!text && !toolCalls.length) throw new Error("Model provider returned no text result.");
    return {
      text, model: body.model ?? request.model,
      usage: { inputTokens: body.usage?.prompt_tokens, outputTokens: body.usage?.completion_tokens },
      toolCalls: toolCalls.length ? toolCalls : undefined,
      finishReason: choice?.finish_reason ?? undefined
    };
  }

  async listModels(): Promise<ModelProviderModel[]> {
    const key = this.credentialAccount ? await this.credentials.get(this.credentialAccount) : undefined;
    const response = await fetch(new URL("models", ensureSlash(this.endpoint)), {
      method: "GET",
      signal: AbortSignal.timeout(15_000),
      headers: key ? { authorization: `Bearer ${key}` } : {}
    });
    if (!response.ok) throw new Error(`Model provider returned HTTP ${response.status} while listing models.`);
    const body = await readBoundedJsonResponse(response) as {
      data?: Array<{ id?: unknown; owned_by?: unknown; created?: unknown }>;
    };
    const models = Array.isArray(body.data) ? body.data : [];
    const normalized = new Map<string, ModelProviderModel>();
    for (const model of models) {
      if (typeof model.id !== "string" || !model.id.trim()) continue;
      const id = model.id.trim();
      if (normalized.has(id)) continue;
      normalized.set(id, {
        id,
        ...(typeof model.owned_by === "string" ? { ownedBy: model.owned_by } : {}),
        ...(typeof model.created === "number" ? { created: model.created } : {})
      });
    }
    return [...normalized.values()]
      .sort((left, right) => left.id.localeCompare(right.id));
  }
}

function serializeMessage(message: ModelChatMessage): Record<string, unknown> {
  if (message.role === "assistant") {
    return {
      role: "assistant",
      content: message.content ?? "",
      ...(message.toolCalls?.length ? { tool_calls: message.toolCalls.map((call) => ({ id: call.id, type: "function", function: { name: call.name, arguments: call.argumentsJson } })) } : {})
    };
  }
  if (message.role === "tool") {
    return { role: "tool", tool_call_id: message.toolCallId, ...(message.name ? { name: message.name } : {}), content: message.content };
  }
  return { role: message.role, content: message.content };
}

function normalizeToolCalls(raw: Array<{ id?: string; function?: { name?: string; arguments?: string } }> | undefined): ModelToolCall[] {
  if (!raw?.length) return [];
  return raw.map((call, index) => ({
    id: call.id || `call_${index}`,
    name: call.function?.name ?? "",
    argumentsJson: call.function?.arguments ?? "{}"
  })).filter((call) => call.name);
}

export async function requestContextSelection(
  provider: ModelProvider,
  model: string,
  goal: string,
  snapshot: string,
  projectPath: string,
  signal?: AbortSignal
): Promise<{ selection: unknown; model: string }> {
  const response = await provider.complete({
    projectPath,
    model,
    messages: [
      {
        role: "system",
        content: "Inspect the compact Queryn project catalog and choose only the sources needed to answer the user's goal. Return JSON only. Use exact project-relative paths and artifact ids from the catalog. Add focused search queries when titles alone are insufficient."
      },
      { role: "user", content: `Goal:\n${goal}\n\nCompact project catalog:\n${snapshot}` }
    ],
    responseSchema: contextSelectionSchema(),
    maxTokens: CONTEXT_SELECTION_MAX_TOKENS,
    signal
  });
  return { selection: parseModelJson(response.text), model: response.model };
}

export async function requestAgentReply(
  provider: ModelProvider,
  model: string,
  goal: string,
  researchedContext: string,
  projectPath: string,
  options: { signal?: AbortSignal; onDelta?: (delta: string) => void } = {}
): Promise<ModelResponse> {
  return provider.complete({
    projectPath,
    model,
    messages: [
      {
        role: "system",
        content: "Answer the user clearly using the researched Queryn project sources. Mention uncertainty when the sources are insufficient. Do not emit JSON or an operation plan in this response."
      },
      { role: "user", content: `Goal:\n${goal}\n\nResearched project context:\n${researchedContext}` }
    ],
    maxTokens: AGENT_REPLY_MAX_TOKENS,
    signal: options.signal ?? AbortSignal.timeout(300_000),
    onTextDelta: options.onDelta
  });
}

export async function requestAgentPlan(
  provider: ModelProvider,
  model: string,
  goal: string,
  snapshot: string,
  planSchema: Record<string, unknown>,
  projectPath: string,
  signal?: AbortSignal
): Promise<{ plan: unknown; model: string }> {
  const response = await provider.complete({
    projectPath,
    model,
    messages: [
      {
        role: "system",
        content: "Build a bounded Queryn operation plan only when project changes are needed. Return JSON only. Use only listed operations. Never invent filesystem or shell actions. Return an empty steps array for a read-only answer."
      },
      { role: "user", content: `Goal:\n${goal}\n\nAvailable operations and researched project context:\n${snapshot}` }
    ],
    responseSchema: planSchema,
    maxTokens: AGENT_PLAN_MAX_TOKENS,
    signal: signal ?? AbortSignal.timeout(300_000)
  });
  return { plan: parseModelJson(response.text), model: response.model };
}

function ensureSlash(value: string): string { return value.endsWith("/") ? value : `${value}/`; }

async function readStreamingResponse(response: Response, fallbackModel: string, onTextDelta: (delta: string) => void): Promise<ModelResponse> {
  const reader = response.body!.getReader();
  const decoder = new TextDecoder();
  let buffer = "";
  let text = "";
  let model = fallbackModel;
  let usage: ModelResponse["usage"];
  let finishReason: string | undefined;
  const pendingCalls = new Map<number, { id?: string; name?: string; arguments: string }>();
  let receivedBytes = 0;
  const processLine = (line: string): void => {
    const normalized = line.replace(/\r$/, "");
    if (!normalized.startsWith("data:")) return;
    const payload = normalized.slice(5).trim();
    if (!payload || payload === "[DONE]") return;
    const chunk = JSON.parse(payload) as {
      model?: string;
      choices?: Array<{
        delta?: {
          content?: string;
          tool_calls?: Array<{ index?: number; id?: string; function?: { name?: string; arguments?: string } }>;
        };
        message?: { content?: string };
        finish_reason?: string | null;
      }>;
      usage?: { prompt_tokens?: number; completion_tokens?: number };
      error?: { message?: string };
    };
    if (chunk.error) throw new Error(chunk.error.message || "Model provider streaming error.");
    model = chunk.model ?? model;
    finishReason = chunk.choices?.[0]?.finish_reason ?? finishReason;
    const delta = chunk.choices?.[0]?.delta ?? (chunk.choices?.[0]?.message?.content ? { content: chunk.choices[0].message.content } : undefined);
    if (typeof delta?.content === "string" && delta.content) {
      text += delta.content;
      onTextDelta(delta.content);
    }
    for (const call of delta?.tool_calls ?? []) {
      const index = typeof call.index === "number" ? call.index : 0;
      const current = pendingCalls.get(index) ?? { arguments: "" };
      current.id = call.id ?? current.id;
      current.name = call.function?.name ?? current.name;
      current.arguments += call.function?.arguments ?? "";
      pendingCalls.set(index, current);
    }
    if (chunk.usage) usage = { inputTokens: chunk.usage.prompt_tokens, outputTokens: chunk.usage.completion_tokens };
  };
  while (true) {
    const { value, done } = await reader.read();
    if (done) break;
    receivedBytes += value.byteLength;
    if (receivedBytes > 32 * 1024 * 1024) {
      await reader.cancel();
      throw new Error("Model streaming response exceeds 32 MiB.");
    }
    buffer += decoder.decode(value, { stream: true });
    let newline;
    while ((newline = buffer.indexOf("\n")) >= 0) {
      processLine(buffer.slice(0, newline));
      buffer = buffer.slice(newline + 1);
    }
  }
  buffer += decoder.decode();
  if (buffer.trim()) processLine(buffer);
  const indices = [...pendingCalls.keys()].sort((left, right) => left - right);
  const toolCalls = normalizeToolCalls(indices.map((index) => {
    const call = pendingCalls.get(index)!;
    return { id: call.id, function: { name: call.name, arguments: call.arguments } };
  }));
  if (!text && !toolCalls.length) throw new Error("Model provider returned no streamed text result.");
  if (finishReason === "length") throw new Error("Model response reached its output limit.");
  return { text, model, usage, toolCalls: toolCalls.length ? toolCalls : undefined, finishReason };
}

function contextSelectionSchema(): Record<string, unknown> {
  return {
    type: "object",
    required: ["queries", "projectRelativePaths", "artifactIds"],
    additionalProperties: false,
    properties: {
      queries: { type: "array", maxItems: 6, items: { type: "string" } },
      projectRelativePaths: { type: "array", maxItems: 12, items: { type: "string" } },
      artifactIds: { type: "array", maxItems: 8, items: { type: "string" } }
    }
  };
}

function parseModelJson(text: string): unknown {
  const trimmed = text.trim().replace(/^```(?:json)?\s*/i, "").replace(/\s*```$/, "");
  try {
    return JSON.parse(trimmed) as unknown;
  } catch (error) {
    const start = trimmed.indexOf("{");
    const end = trimmed.lastIndexOf("}");
    if (start >= 0 && end > start) {
      try { return JSON.parse(trimmed.slice(start, end + 1)) as unknown; }
      catch { /* The normalized error below is clearer than an engine offset. */ }
    }
    throw new Error(`Model returned incomplete structured data${error instanceof SyntaxError ? "" : "."}`);
  }
}
