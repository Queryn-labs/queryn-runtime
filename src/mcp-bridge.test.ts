import assert from "node:assert/strict";
import { mkdtemp, rm, writeFile, chmod } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import { createSession } from "@queryn/project";
import { QuerynRuntime } from "./runtime.js";

async function fixture(): Promise<{ root: string; runtime: QuerynRuntime; projectPath: string }> {
  const root = await mkdtemp(path.join(os.tmpdir(), "queryn-mcp-test-"));
  const runtime = new QuerynRuntime(path.join(root, "runtime"));
  await runtime.initialize();
  const projectPath = path.join(root, "project");
  await runtime.projects.create({ rootPath: projectPath, id: "test", name: "Test" });
  return { root, runtime, projectPath };
}

const FAKE_SERVER = `#!/usr/bin/env node
let buffer = "";
process.stdin.setEncoding("utf8");
process.stdin.on("data", (chunk) => {
  buffer += chunk;
  let newline;
  while ((newline = buffer.indexOf("\\n")) >= 0) {
    const line = buffer.slice(0, newline);
    buffer = buffer.slice(newline + 1);
    if (!line.trim()) continue;
    const request = JSON.parse(line);
    respond(request);
  }
});
function respond(request) {
  let result;
  if (request.method === "initialize") {
    result = { protocolVersion: "2025-06-18", capabilities: { tools: {} }, serverInfo: { name: "fake", version: "1" } };
  } else if (request.method === "tools/list") {
    result = { tools: [{
      name: "echo",
      description: "Echo the given text.",
      inputSchema: { type: "object", required: ["text"], properties: { text: { type: "string" } } },
      annotations: { readOnlyHint: true }
    }] };
  } else if (request.method === "tools/call") {
    result = { content: [{ type: "text", text: "echo:" + request.params.arguments.text }] };
  } else if (request.method === "shutdown") {
    result = {};
  } else {
    process.stdout.write(JSON.stringify({ jsonrpc: "2.0", id: request.id, error: { code: -32601, message: "not found" } }) + "\\n");
    return;
  }
  process.stdout.write(JSON.stringify({ jsonrpc: "2.0", id: request.id, result }) + "\\n");
}
`;

test("MCP stdio server tools participate in the agent loop", async () => {
  const item = await fixture();
  try {
    const serverScript = path.join(item.root, "fake-mcp-server.mjs");
    await writeFile(serverScript, FAKE_SERVER);
    await chmod(serverScript, 0o755);

    const registered = await item.runtime.registerMcpServer({
      id: "fake",
      command: process.execPath,
      args: [serverScript],
      defaultRisk: "external-side-effect"
    });
    assert.deepEqual(registered.tools, ["echo"]);
    assert.equal(item.runtime.mcp.listServers()[0].id, "fake");

    // The mapped tool is visible to the agent toolset with safe-read risk from hints.
    const operations = item.runtime.registry.list({ extensionVersions: item.runtime.projects.extensionVersions(item.projectPath) });
    const echoOp = operations.find((operation) => operation.definition.id === "mcp.fake.echo");
    assert.ok(echoOp);
    assert.equal(echoOp.definition.risk, "safe-read");
    assert.equal(echoOp.extensionId, "queryn.mcp.fake");

    // Direct governed invocation through OperationService.
    const directJob = await item.runtime.operations.invokeAndWait({
      projectPath: item.projectPath,
      operationId: "mcp.fake.echo",
      arguments: { text: "hello" }
    });
    assert.equal(directJob.status, "succeeded");
    assert.equal(directJob.result?.message, "echo:hello");

    // Full agent loop uses the MCP tool like any other.
    const session = await createSession(item.runtime.projects.get(item.projectPath), { title: "MCP" });
    let turns = 0;
    item.runtime.agent.registerProvider({
      id: "mcp.driver",
      recipient: "local",
      async complete(request) {
        turns += 1;
        if (turns === 1) {
          assert.equal(request.tools?.some((tool) => tool.name === "mcp.fake.echo"), true);
          return {
            text: "",
            model: request.model,
            toolCalls: [{ id: "mc1", name: "mcp.fake.echo", argumentsJson: JSON.stringify({ text: "from-loop" }) }]
          };
        }
        const last = request.messages.at(-1) as { role: string; content: string };
        assert.equal(last.role, "tool");
        assert.match(last.content, /echo:from-loop/);
        return { text: "Loop used MCP.", model: request.model };
      }
    });
    const run = await item.runtime.agent.chat({ projectPath: item.projectPath, sessionId: session.id, goal: "Use the echo tool.", providerId: "mcp.driver", model: "m1" });
    assert.equal(run.status, "succeeded");

    await item.runtime.unregisterMcpServer("fake");
    assert.equal(item.runtime.mcp.listServers().length, 0);
  } finally {
    await item.runtime.mcp.shutdown().catch(() => undefined);
    await rm(item.root, { recursive: true, force: true });
  }
});

test("risky MCP tools suspend the loop for approval", async () => {
  const item = await fixture();
  try {
    const riskyServer = FAKE_SERVER.replace("readOnlyHint: true", "readOnlyHint: false").replace(
      'text: "echo:" + request.params.arguments.text',
      'text: "mutated"'
    );
    const serverScript = path.join(item.root, "risky-mcp-server.mjs");
    await writeFile(serverScript, riskyServer);
    await chmod(serverScript, 0o755);
    await item.runtime.registerMcpServer({ id: "risky", command: process.execPath, args: [serverScript] });

    const definition = item.runtime.registry.get("mcp.risky.echo", undefined).definition;
    assert.equal(definition.risk, "external-side-effect");

    const session = await createSession(item.runtime.projects.get(item.projectPath), { title: "Risky" });
    let turns = 0;
    item.runtime.agent.registerProvider({
      id: "risky.driver",
      recipient: "local",
      async complete(request) {
        turns += 1;
        if (turns === 1) return { text: "", model: request.model, toolCalls: [{ id: "r1", name: "mcp.risky.echo", argumentsJson: JSON.stringify({ text: "x" }) }] };
        return { text: "Approved and done.", model: request.model };
      }
    });
    const pendingRun = await item.runtime.agent.chat({ projectPath: item.projectPath, sessionId: session.id, goal: "Mutate.", providerId: "risky.driver", model: "m1" });
    assert.equal(pendingRun.status, "waiting-approval");
    assert.ok(pendingRun.pendingJobId);
    const finalRun = await item.runtime.agent.approveChat(pendingRun.id, { planId: pendingRun.id, stepId: pendingRun.pendingJobId!, approved: true, scope: "once", decidedAt: new Date().toISOString() });
    assert.equal(finalRun.status, "succeeded");
    assert.equal(turns >= 2, true);
  } finally {
    await item.runtime.mcp.shutdown().catch(() => undefined);
    await rm(item.root, { recursive: true, force: true });
  }
});
