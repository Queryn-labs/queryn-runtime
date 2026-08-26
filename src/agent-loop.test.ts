import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import { createNote, createSession, readSessionEvents, registerExistingArtifact } from "@queryn/project";
import type { SessionEvent } from "@queryn/types";
import type { ChatRun } from "./agent-kernel.js";
import { OpenAICompatibleProvider, type ModelChatMessage, type ModelRequest, type ModelResponse, type ModelToolCall } from "./model-provider.js";
import { buildConversationHistory } from "./history-builder.js";
import { QuerynRuntime } from "./runtime.js";

interface ScriptedTurn {
  text?: string;
  toolCalls?: ModelToolCall[];
  usage?: ModelResponse["usage"];
  inspect?: (request: ModelRequest) => void;
}

type TurnSpec = ScriptedTurn | ((request: ModelRequest) => { text?: string; toolCalls?: ModelToolCall[]; usage?: ModelResponse["usage"] });

function scriptedProvider(id: string, recipient: "local" | "cloud", turns: TurnSpec[]) {
  let index = 0;
  const seenRequests: ModelRequest[] = [];
  return {
    id,
    recipient,
    seenRequests,
    async complete(request: ModelRequest): Promise<ModelResponse> {
      seenRequests.push(request);
      const turn = turns[Math.min(index++, turns.length - 1)];
      const resolved = typeof turn === "function" ? turn(request) : turn;
      if (typeof (turn as ScriptedTurn).inspect === "function") (turn as ScriptedTurn).inspect!(request);
      if (resolved.text) request.onTextDelta?.(resolved.text);
      return { text: resolved.text ?? "", model: request.model, toolCalls: resolved.toolCalls, usage: resolved.usage };
    }
  };
}

async function fixture(): Promise<{ root: string; runtime: QuerynRuntime; projectPath: string }> {
  const root = await mkdtemp(path.join(os.tmpdir(), "queryn-agent-loop-test-"));
  const runtime = new QuerynRuntime(path.join(root, "runtime"));
  await runtime.initialize();
  const projectPath = path.join(root, "project");
  await runtime.projects.create({ rootPath: projectPath, id: "test", name: "Test" });
  return { root, runtime, projectPath };
}

async function sessionFixture(item: Awaited<ReturnType<typeof fixture>>): Promise<string> {
  const session = await createSession(item.runtime.projects.get(item.projectPath), { title: "Loop" });
  await import("@queryn/project").then((project) => project.appendSessionEvent(item.projectPath, session.id, { type: "user-message", data: { content: "Explain self-attention using my notes." } }));
  return session.id;
}

function eventsOf(item: Awaited<ReturnType<typeof fixture>>, sessionId: string): Promise<SessionEvent[]> {
  return readSessionEvents(item.projectPath, sessionId);
}

test("provider protocol parses tool calls in streaming and non-streaming modes", async () => {
  const originalFetch = globalThis.fetch;
  try {
    globalThis.fetch = (async (_input: string | URL | Request, init?: RequestInit) => {
      const body = JSON.parse(String(init?.body)) as { stream?: boolean; messages: ModelChatMessage[]; tools?: unknown[]; tool_choice?: string };
      if (body.tools) {
        assert.deepEqual(body.tools[0], { type: "function", function: { name: "t", description: "", parameters: {} } });
        assert.equal(body.tool_choice, "auto");
      }
      if (body.stream) {
        // Arguments JSON is deliberately split across chunks mid-string.
        const first = `data: ${JSON.stringify({ model: "m1", choices: [{ delta: { tool_calls: [{ index: 0, id: "call_7", function: { name: "queryn.project.search", arguments: "{\"qu" } }] } }] })}\r\n`;
        const second = `data: ${JSON.stringify({ model: "m1", choices: [{ delta: { tool_calls: [{ index: 0, function: { arguments: "ery\":\"attention\"}" } }] } }] })}\r\n`;
        const third = `data: ${JSON.stringify({ model: "m1", choices: [{ delta: {}, finish_reason: "tool_calls" }] })}\r\n`;
        const encoder = new TextEncoder();
        const sse = first + second + third;
        return new Response(new ReadableStream({
          start(controller) {
            controller.enqueue(encoder.encode(sse.slice(0, 40)));
            controller.enqueue(encoder.encode(sse.slice(40)));
            controller.close();
          }
        }), { headers: { "content-type": "text/event-stream" } });
      }
      return new Response(JSON.stringify({
        model: "m1",
        choices: [{ message: { content: null, tool_calls: [{ id: "call_9", function: { name: "queryn.project.read", arguments: "{\"path\":\"notes/a.md\"}" } }] }, finish_reason: "tool_calls" }]
      }), { headers: { "content-type": "application/json" } });
    }) as typeof fetch;
    const provider = new OpenAICompatibleProvider("test.local", "http://127.0.0.1:1234/v1/", { async set() {}, async get() { return undefined; }, async delete() {} });
    const streamed = await provider.complete({ model: "m1", messages: [{ role: "user", content: "hi" }], tools: [{ name: "t", parameters: {} }], onTextDelta: () => undefined });
    assert.deepEqual(streamed.toolCalls, [{ id: "call_7", name: "queryn.project.search", argumentsJson: "{\"query\":\"attention\"}" }]);
    assert.equal(streamed.finishReason, "tool_calls");
    const nonStreamed = await provider.complete({ model: "m1", messages: [{ role: "user", content: "hi" }] });
    assert.deepEqual(nonStreamed.toolCalls, [{ id: "call_9", name: "queryn.project.read", argumentsJson: "{\"path\":\"notes/a.md\"}" }]);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test("OpenAI-compatible provider lists and normalizes available models", async () => {
  const originalFetch = globalThis.fetch;
  try {
    globalThis.fetch = (async (input: string | URL | Request, init?: RequestInit) => {
      assert.equal(String(input), "http://127.0.0.1:1234/v1/models");
      assert.equal(init?.method, "GET");
      assert.equal(new Headers(init?.headers).get("authorization"), "Bearer secret");
      return new Response(JSON.stringify({
        data: [
          { id: "zeta", owned_by: "local" },
          { id: "alpha", created: 2 },
          { id: "alpha", owned_by: "duplicate" },
          { id: "" },
          { owned_by: "invalid" }
        ]
      }), { headers: { "content-type": "application/json" } });
    }) as typeof fetch;
    const provider = new OpenAICompatibleProvider("test.local", "http://127.0.0.1:1234/v1/", {
      async set() {},
      async get(account: string) {
        assert.equal(account, "test-account");
        return "secret";
      },
      async delete() {}
    }, "test-account");
    assert.deepEqual(await provider.listModels(), [
      { id: "alpha", created: 2 },
      { id: "zeta", ownedBy: "local" }
    ]);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test("agent loop searches, reads and answers with observable activity", async () => {
  const item = await fixture();
  try {
    const note = await createNote(item.runtime.projects.get(item.projectPath), {
      title: "Attention",
      body: "Self-attention connects tokens through queries, keys and values."
    });
    await item.runtime.indexer.rebuild(item.projectPath);
    const sessionId = await sessionFixture(item);
    const activities: unknown[] = [];
    const deltas: string[] = [];
    item.runtime.agent.on("activity", (event) => activities.push(event));
    item.runtime.agent.on("output.delta", (event) => deltas.push((event as { delta: string }).delta));

    const provider = scriptedProvider("test.loop", "local", [
      { toolCalls: [{ id: "c1", name: "queryn.project.search", argumentsJson: JSON.stringify({ query: "attention" }) }], usage: { inputTokens: 40, outputTokens: 4 } },
      { toolCalls: [{ id: "c2", name: "queryn.project.read", argumentsJson: JSON.stringify({ path: note.relativePath }) }], usage: { inputTokens: 50, outputTokens: 4 } },
      { text: "Self-attention uses queries, keys and values.", usage: { inputTokens: 120, outputTokens: 8 } }
    ]);
    item.runtime.agent.registerProvider(provider);

    const run = await item.runtime.agent.chat({ projectPath: item.projectPath, sessionId, goal: "Explain self-attention.", providerId: "test.loop", model: "loop-1" });
    assert.equal(run.status, "succeeded");
    assert.match(run.response ?? "", /queries, keys and values/);
    assert.equal(run.metrics?.modelCalls, 3);
    assert.equal(run.metrics?.inputTokens, 210);
    assert.equal(run.metrics?.outputTokens, 16);
    assert.equal(run.metrics?.finalResponse?.outputTokens, 8);
    assert.equal(run.metrics?.finalResponse?.tokenCountSource, "provider");
    assert.equal((run.metrics?.finalResponse?.ttftMs ?? -1) >= 0, true);
    assert.equal((run.metrics?.finalResponse?.tokensPerSecond ?? 0) > 0, true);
    const events = await eventsOf(item, sessionId);
    const types = events.map((event) => event.type);
    // operation-call / operation-result are the generic audit trail written by
    // OperationService around every tool execution.
    assert.deepEqual(types, [
      "user-message",
      "tool-call", "operation-call", "operation-result", "observation",
      "tool-call", "operation-call", "operation-result", "observation",
      "assistant-message"
    ]);
    const finalMessage = events.at(-1);
    assert.equal(finalMessage?.data.runId, run.id);
    assert.deepEqual(finalMessage?.data.metrics, run.metrics);
    assert.match(deltas.join(""), /Self-attention uses/);
    const kinds = activities.map((activity) => (activity as { kind?: string }).kind);
    assert.equal(kinds.includes("tool"), true);
    assert.equal(activities.some((activity) => (activity as { status?: string }).status === "completed" && (activity as { operationId?: string }).operationId === "queryn.project.search"), true);
    // Second model turn must contain the observation of the first call.
    const secondRequest = provider.seenRequests[1];
    const lastMessage = secondRequest.messages.at(-1) as { role: string };
    assert.equal(lastMessage.role, "tool");
  } finally {
    await rm(item.root, { recursive: true, force: true });
  }
});

test("invalid tool arguments become an error observation the model can correct", async () => {
  const item = await fixture();
  try {
    const sessionId = await sessionFixture(item);
    const provider = scriptedProvider("test.invalid", "local", [
      { toolCalls: [{ id: "b1", name: "queryn.project.search", argumentsJson: "{}" }] },
      (request: ModelRequest) => {
        const last = request.messages.at(-1) as { role: string; content: string };
        assert.equal(last.role, "tool");
        assert.match(last.content, /Invalid arguments/);
        return { toolCalls: [{ id: "b2", name: "queryn.project.search", argumentsJson: JSON.stringify({ query: "anything" }) }] };
      },
      { text: "Recovered." }
    ]);
    item.runtime.agent.registerProvider(provider);
    const run = await item.runtime.agent.chat({ projectPath: item.projectPath, sessionId, goal: "Search something.", providerId: "test.invalid", model: "inv-1" });
    assert.equal(run.status, "succeeded");
    const observations = (await eventsOf(item, sessionId)).filter((event) => event.type === "observation");
    assert.equal(observations.length, 2);
    assert.equal(observations[0].data.ok, false);
    assert.equal(observations[1].data.ok, true);
  } finally {
    await rm(item.root, { recursive: true, force: true });
  }
});

async function privilegedFixture(item: Awaited<ReturnType<typeof fixture>>): Promise<void> {
  let calls = 0;
  item.runtime.registry.register({
    definition: {
      id: "test.external.call", toolId: "test.external", version: "1", title: "External call",
      inputSchema: { type: "object", required: ["target"], additionalProperties: false, properties: { target: { type: "string" } } },
      outputSchema: { type: "object" },
      risk: "external-side-effect", agentVisibility: "automatic", execution: "immediate",
      permissions: ["network:use"]
    },
    extensionId: "queryn.builtin",
    runtime: { id: "queryn.builtin", kind: "builtin", lifecycle: "shared" }
  }, async () => {
    calls += 1;
    return { structured: { done: true, calls } };
  });
}

test("privileged tool suspends the run until approval, then resumes", async () => {
  const item = await fixture();
  try {
    await privilegedFixture(item);
    const sessionId = await sessionFixture(item);
    const provider = scriptedProvider("test.approve", "local", [
      { toolCalls: [{ id: "p1", name: "test.external.call", argumentsJson: JSON.stringify({ target: "https://example.com" }) }] },
      { text: "External call completed." }
    ]);
    item.runtime.agent.registerProvider(provider);
    const pendingRun: ChatRun = await item.runtime.agent.chat({ projectPath: item.projectPath, sessionId, goal: "Call out.", providerId: "test.approve", model: "ap-1" });
    assert.equal(pendingRun.status, "waiting-approval");
    assert.ok(pendingRun.pendingJobId);
    assert.equal(item.runtime.jobs.get(pendingRun.pendingJobId!).status, "waiting-approval");

    const finalRun = await item.runtime.agent.approveChat(pendingRun.id, { planId: pendingRun.id, stepId: pendingRun.pendingJobId!, approved: true, scope: "once", decidedAt: new Date().toISOString() });
    assert.equal(finalRun.status, "succeeded");
    const events = await eventsOf(item, sessionId);
    const observation = events.find((event) => event.type === "observation");
    assert.equal(observation?.data.ok, true);
    assert.match(JSON.stringify(observation?.data.content), /done/);
    const approvalEvent = events.find((event) => event.type === "approval");
    assert.ok(approvalEvent);
  } finally {
    await rm(item.root, { recursive: true, force: true });
  }
});

test("denied approval records a declined observation and the loop continues", async () => {
  const item = await fixture();
  try {
    await privilegedFixture(item);
    const sessionId = await sessionFixture(item);
    const provider = scriptedProvider("test.deny", "local", [
      { toolCalls: [{ id: "d1", name: "test.external.call", argumentsJson: JSON.stringify({ target: "https://example.com" }) }] },
      { text: "Understood, staying local." }
    ]);
    item.runtime.agent.registerProvider(provider);
    const pendingRun = await item.runtime.agent.chat({ projectPath: item.projectPath, sessionId, goal: "Call out.", providerId: "test.deny", model: "dn-1" });
    assert.equal(pendingRun.status, "waiting-approval");
    const finalRun = await item.runtime.agent.approveChat(pendingRun.id, { planId: pendingRun.id, stepId: pendingRun.pendingJobId!, approved: false, scope: "once", decidedAt: new Date().toISOString() });
    assert.equal(finalRun.status, "succeeded");
    const observation = (await eventsOf(item, sessionId)).find((event) => event.type === "observation");
    assert.equal(observation?.data.ok, false);
    assert.match(String(observation?.data.content), /declined|cancelled/i);
  } finally {
    await rm(item.root, { recursive: true, force: true });
  }
});

test("sensitive material refuses to enter a cloud context but stays readable locally", async () => {
  const item = await fixture();
  try {
    const project = item.runtime.projects.get(item.projectPath);
    const secret = await createNote(project, { title: "Secret", body: "PRIVATE_TOKEN_9911" });
    await registerExistingArtifact(project, {
      type: "queryn.note", projectRelativePath: secret.relativePath,
      metadata: { sensitivity: "sensitive" }, context: { mode: "automatic" }
    });
    const sessionId = await sessionFixture(item);

    const cloudProvider = scriptedProvider("test.cloud", "cloud", [
      { toolCalls: [{ id: "s1", name: "queryn.project.read", argumentsJson: JSON.stringify({ path: secret.relativePath }) }] },
      { text: "Noted." }
    ]);
    item.runtime.agent.registerProvider(cloudProvider);
    const run = await item.runtime.agent.chat({
      projectPath: item.projectPath, sessionId, goal: "Read secret.",
      providerId: "test.cloud", model: "cl-1",
      recipientApproval: { recipient: "cloud", approved: true, decidedAt: new Date().toISOString() }
    });
    assert.equal(run.status, "succeeded");
    const observation = (await eventsOf(item, sessionId)).find((event) => event.type === "observation");
    assert.equal(observation?.data.ok, false);
    assert.match(String(observation?.data.content), /sensitive/);
    assert.equal(JSON.stringify((await eventsOf(item, sessionId))).includes("PRIVATE_TOKEN_9911"), false);

    const localSessionId = await sessionFixture(item);
    const localProvider = scriptedProvider("test.localread", "local", [
      { toolCalls: [{ id: "s2", name: "queryn.project.read", argumentsJson: JSON.stringify({ path: secret.relativePath }) }] },
      { text: "Read locally." }
    ]);
    item.runtime.agent.registerProvider(localProvider);
    const localRun = await item.runtime.agent.chat({ projectPath: item.projectPath, sessionId: localSessionId, goal: "Read secret.", providerId: "test.localread", model: "lc-1" });
    assert.equal(localRun.status, "succeeded");
    const localEvents = JSON.stringify(await eventsOf(item, localSessionId));
    assert.match(localEvents, /PRIVATE_TOKEN_9911/);
  } finally {
    await rm(item.root, { recursive: true, force: true });
  }
});

test("step budget forces a final answer without tools", async () => {
  const item = await fixture();
  try {
    const sessionId = await sessionFixture(item);
    const provider = scriptedProvider("test.budget", "local", [
      { toolCalls: [{ id: "m1", name: "queryn.project.search", argumentsJson: JSON.stringify({ query: "x" }) }] },
      { text: "Forced summary." }
    ]);
    item.runtime.agent.registerProvider(provider);
    const run = await item.runtime.agent.chat({ projectPath: item.projectPath, sessionId, goal: "Loop forever.", providerId: "test.budget", model: "bd-1", maxSteps: 2 });
    assert.equal(run.status, "succeeded");
    assert.equal(run.steps, 2);
    const finalRequest = provider.seenRequests.at(-1)!;
    assert.equal(finalRequest.tools, undefined);
    assert.equal(finalRequest.messages.some((message) => message.role === "system" && /step limit/.test(message.content)), true);
  } finally {
    await rm(item.root, { recursive: true, force: true });
  }
});

test("history builder pairs tool calls with observations and drops dangling calls", async () => {
  const base = { schemaVersion: "1" as const, sessionId: "s", sequence: 0, timestamp: new Date().toISOString() };
  const events = [
    { ...base, id: "e1", type: "user-message" as const, data: { content: "Goal" } },
    { ...base, id: "e2", type: "tool-call" as const, data: { callId: "pair", operationId: "queryn.project.search", arguments: { query: "q" } } },
    { ...base, id: "e3", type: "observation" as const, data: { callId: "pair", operationId: "queryn.project.search", ok: true, content: "{\"matches\":[]}", artifactIds: [] } },
    { ...base, id: "e4", type: "tool-call" as const, data: { callId: "dangling", operationId: "queryn.notes.create", arguments: { title: "T" } } },
    { ...base, id: "e5", type: "assistant-message" as const, data: { content: "Answer" } }
  ];
  const built = buildConversationHistory(events);
  const roles = built.messages.map((message) => message.role);
  assert.deepEqual(roles, ["user", "assistant", "tool", "assistant"]);
  const assistantWithCall = built.messages.find((message): message is Extract<ModelChatMessage, { role: "assistant" }> => message.role === "assistant");
  assert.equal(assistantWithCall?.toolCalls?.[0]?.name, "queryn.project.search");
});

test("history builder excludes events hidden by a portable session tombstone", () => {
  const base = { schemaVersion: "1" as const, sessionId: "s", sequence: 0, timestamp: new Date().toISOString() };
  const events: SessionEvent[] = [
    { ...base, id: "user", type: "user-message", data: { content: "Goal" } },
    { ...base, id: "call", type: "tool-call", data: { callId: "pair", operationId: "queryn.project.search", arguments: { query: "q" } } },
    { ...base, id: "observation", type: "observation", data: { callId: "pair", operationId: "queryn.project.search", content: "Result" } },
    { ...base, id: "answer", type: "assistant-message", data: { content: "Hidden answer" } },
    { ...base, id: "tombstone", type: "status", data: { kind: "events-hidden", eventIds: ["call", "observation", "answer"] } }
  ];
  const built = buildConversationHistory(events);
  assert.deepEqual(built.messages, [{ role: "user", content: "Goal" }]);
});
