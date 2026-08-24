import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import { appendSessionEvent, createSession, readSession, updateSession } from "@osnova/project";
import type { ModelRequest } from "./model-provider.js";
import { OsnovaRuntime } from "./runtime.js";
import { readSessionTranscript, searchSessions } from "./session-memory.js";

async function fixture(): Promise<{ root: string; runtime: OsnovaRuntime; projectPath: string }> {
  const root = await mkdtemp(path.join(os.tmpdir(), "osnova-session-memory-test-"));
  const runtime = new OsnovaRuntime(path.join(root, "runtime"));
  await runtime.initialize();
  const projectPath = path.join(root, "project");
  await runtime.projects.create({ rootPath: projectPath, id: "test", name: "Test" });
  return { root, runtime, projectPath };
}

function recordingProvider(id: string): { id: string; recipient: "local"; seenRequests: ModelRequest[]; complete(request: ModelRequest): Promise<{ text: string; model: string }> } {
  const seenRequests: ModelRequest[] = [];
  return {
    id,
    recipient: "local",
    seenRequests,
    async complete(request: ModelRequest) {
      seenRequests.push(request);
      return { text: "Done.", model: request.model };
    }
  };
}

function toolNames(request: ModelRequest): string[] {
  return (request.tools ?? []).map((tool) => tool.name);
}

test("full memory exposes session tools, search finds past dialogue, off hides them", async () => {
  const item = await fixture();
  try {
    const first = await createSession(item.runtime.projects.get(item.projectPath), { title: "Past research" });
    await appendSessionEvent(item.projectPath, first.id, { type: "user-message", data: { content: "Explain how transformer attention works." } });
    await appendSessionEvent(item.projectPath, first.id, { type: "assistant-message", data: { content: "Attention compares queries against keys of every token." } });

    const matches = await searchSessions(item.projectPath, "attention");
    assert.equal(matches.length >= 1, true);
    assert.equal(matches[0].sessionId, first.id);
    assert.match(matches[0].snippet, /attention/i);

    const transcript = await readSessionTranscript(item.projectPath, first.id);
    assert.equal(transcript.title, "Past research");
    assert.match(transcript.text, /User: Explain how transformer attention works\./);
    assert.match(transcript.text, /Assistant: Attention compares queries/);

    const second = await createSession(item.runtime.projects.get(item.projectPath), { title: "Follow-up" });
    await updateSession(item.projectPath, second.id, { memoryMode: "full" });
    const updated = await readSession(item.projectPath, second.id);
    assert.equal(updated.memoryMode, "full");
    assert.notEqual(updated.updatedAt, undefined);

    const provider = recordingProvider("test.memory");
    item.runtime.agent.registerProvider(provider);
    const run = await item.runtime.agent.chat({
      projectPath: item.projectPath,
      sessionId: second.id,
      goal: "What did we say about attention?",
      providerId: "test.memory",
      model: "mem-1"
    });
    assert.equal(run.status, "succeeded");

    const systemPrompt = provider.seenRequests[0].messages[0]!.content ?? "";
    assert.match(systemPrompt, /Past sessions:/);
    assert.match(systemPrompt, /Past research \(/);
    assert.match(systemPrompt, /Full memory is enabled/);
    const names = toolNames(provider.seenRequests[0]);
    assert.equal(names.includes("osnova.session.search"), true);
    assert.equal(names.includes("osnova.session.read"), true);

    // Without full memory the session tools must be absent from the same project's schema set.
    const third = await createSession(item.runtime.projects.get(item.projectPath), { title: "Private" });
    const privateRun = await item.runtime.agent.chat({
      projectPath: item.projectPath,
      sessionId: third.id,
      goal: "Anything.",
      providerId: "test.memory",
      model: "mem-1"
    });
    assert.equal(privateRun.status, "succeeded");
    const privateNames = toolNames(provider.seenRequests[1]);
    assert.equal(privateNames.includes("osnova.session.search"), false);
    assert.equal(privateNames.includes("osnova.session.read"), false);
    assert.match(provider.seenRequests[1].messages[0]!.content ?? "", /Past sessions:/);
  } finally {
    await rm(item.root, { recursive: true, force: true });
  }
});
