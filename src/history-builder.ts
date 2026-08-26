import type { SessionEvent } from "@queryn/types";
import type { ModelChatMessage } from "./model-provider.js";

const TOOL_MESSAGE_MAX_CHARS = 6_000;

export interface BuiltHistory {
  messages: ModelChatMessage[];
  estimatedTokens: number;
}

export function buildConversationHistory(events: SessionEvent[]): BuiltHistory {
  const hiddenEventIds = new Set(events
    .filter((event) => event.type === "status" && event.data.kind === "events-hidden" && Array.isArray(event.data.eventIds))
    .flatMap((event) => (event.data.eventIds as unknown[]).filter((id): id is string => typeof id === "string")));
  const visibleEvents = events.filter((event) => !hiddenEventIds.has(event.id));
  const observedCallIds = new Set(
    visibleEvents
      .filter((event) => event.type === "observation")
      .map((event) => event.data.callId)
      .filter((callId): callId is string => typeof callId === "string")
  );
  const messages: ModelChatMessage[] = [];
  for (const event of visibleEvents) {
    const data = event.data;
    if (event.type === "user-message" && typeof data.content === "string") {
      messages.push({ role: "user", content: data.content });
      continue;
    }
    if (event.type === "assistant-message" && typeof data.content === "string" && data.content.trim()) {
      messages.push({ role: "assistant", content: data.content });
      continue;
    }
    if (event.type === "tool-call"
      && typeof data.callId === "string"
      && typeof data.operationId === "string"
      && typeof data.arguments === "object" && data.arguments !== null) {
      // A tool call without a matching observation means the run is suspended
      // (waiting for approval or crashed); replaying it would break protocol validity.
      if (!observedCallIds.has(data.callId)) continue;
      messages.push({
        role: "assistant",
        content: "",
        toolCalls: [{ id: data.callId, name: data.operationId, argumentsJson: JSON.stringify(data.arguments) }]
      });
      continue;
    }
    if (event.type === "observation"
      && typeof data.callId === "string"
      && typeof data.operationId === "string"
      && typeof data.content === "string") {
      messages.push({
        role: "tool",
        toolCallId: data.callId,
        name: data.operationId,
        content: data.content.slice(0, TOOL_MESSAGE_MAX_CHARS)
      });
    }
  }
  return { messages, estimatedTokens: estimateMessagesTokens(messages) };
}

export function trimHistoryToBudget(messages: ModelChatMessage[], budgetTokens: number): void {
  let estimated = estimateMessagesTokens(messages);
  let index = 0;
  while (estimated > budgetTokens && index < messages.length - 2) {
    const message = messages[index];
    if (message.role === "tool" && message.content.length > 200) {
      const trimmed = `${message.content.slice(0, 160)}… [observation trimmed to fit the context budget]`;
      estimated -= Math.ceil((message.content.length - trimmed.length) / 4);
      messages[index] = { ...message, content: trimmed };
    }
    index += 1;
  }
}

export function estimateMessagesTokens(messages: Array<{ content?: string; name?: string; toolCalls?: Array<{ name?: string; argumentsJson: string }> }>): number {
  let characters = 0;
  for (const message of messages) {
    characters += (message.content?.length ?? 0) + (message.name?.length ?? 0);
    for (const call of message.toolCalls ?? []) characters += call.argumentsJson.length + (call.name?.length ?? 0) + 32;
  }
  return Math.ceil(characters / 4);
}
