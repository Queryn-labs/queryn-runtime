import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { createHash } from "node:crypto";
import { chmod, cp, mkdtemp, mkdir, readFile, rm, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import { promisify } from "node:util";
import { packExtension } from "@osnova/plugin-sdk/package";
import { createNote, createSession, registerExistingArtifact } from "@osnova/project";
import { JobManager } from "./job-manager.js";
import { RpcClient } from "./rpc-client.js";
import { startRpcServer } from "./rpc-server.js";
import { OsnovaRuntime } from "./runtime.js";
import { ProjectIndexer } from "./context-broker.js";
import { OpenAICompatibleProvider, requestAgentPlan, requestAgentReply } from "./model-provider.js";
import { stageArtifacts } from "./operation-service.js";
import { validateJsonSchema } from "./schema.js";
import { createInvocationDirectories, removeInvocationDirectories } from "./runtime-supervisor.js";

const execFileAsync = promisify(execFile);

async function fixture(): Promise<{ root: string; runtime: OsnovaRuntime; projectPath: string }> {
  const root = await mkdtemp(path.join(os.tmpdir(), "osnova-runtime-test-"));
  const runtime = new OsnovaRuntime(path.join(root, "runtime"));
  await runtime.initialize();
  const projectPath = path.join(root, "project");
  await runtime.projects.create({ rootPath: projectPath, id: "test", name: "Test" });
  return { root, runtime, projectPath };
}

test("builtin operation validates input and creates portable session history", async () => {
  const item = await fixture();
  try {
    const session = await createSession(item.runtime.projects.get(item.projectPath), { title: "Writing" });
    await assert.rejects(() => item.runtime.operations.invoke({ projectPath: item.projectPath, operationId: "osnova.notes.create", arguments: {} }), /title is required/);
    const job = await item.runtime.operations.invokeAndWait({ projectPath: item.projectPath, sessionId: session.id, operationId: "osnova.notes.create", arguments: { title: "Result", body: "Study material" }, publishArtifacts: true });
    assert.equal(job.status, "succeeded");
    const createdArtifactId = (job.result?.structured as { artifactId: string }).artifactId;
    assert.deepEqual(job.artifactIds, [createdArtifactId]);
    assert.equal((await (await import("@osnova/project")).readArtifact(item.projectPath, createdArtifactId)).provenance.runId, job.id);
    await assert.rejects(() => item.runtime.operations.invoke({ projectPath: item.projectPath, operationId: "osnova.notes.create", arguments: { title: "Nested" }, artifactIds: [createdArtifactId] }), /does not accept artifact inputs/);
    const events = await (await import("@osnova/project")).readSessionEvents(item.projectPath, session.id);
    assert.deepEqual(events.map((event) => event.type), ["operation-call", "operation-result", "artifact-linked"]);
    const indexed = await item.runtime.indexer.rebuild(item.projectPath);
    assert.equal(indexed.indexed >= 1, true);
    const matches = await item.runtime.indexer.search(item.projectPath, "Study");
    assert.equal(matches[0]?.title, "Result");
  } finally { await rm(item.root, { recursive: true, force: true }); }
});

test("context none never exposes payload", async () => {
  const item = await fixture();
  try {
    const project = item.runtime.projects.get(item.projectPath);
    const note = await createNote(project, { title: "Private", body: "DO_NOT_EXPOSE_4411" });
    const artifact = await registerExistingArtifact(project, { type: "test.private", projectRelativePath: note.relativePath, context: { mode: "none" }, metadata: { sensitivity: "sensitive" } });
    const context = await item.runtime.context.resolve({ projectPath: item.projectPath, artifactIds: [artifact.id], level: "expanded", budgetTokens: 500, recipient: "local" });
    assert.equal(context.text?.includes("DO_NOT_EXPOSE_4411"), false);
    const preview = await item.runtime.context.preview(item.projectPath);
    assert.equal(preview.allowedRecipients.includes("cloud"), false);
    assert.equal(preview.sensitivity, "sensitive");
    await item.runtime.indexer.rebuild(item.projectPath);
    assert.equal((await item.runtime.indexer.search(item.projectPath, "DO_NOT_EXPOSE_4411")).length, 0);
  } finally { await rm(item.root, { recursive: true, force: true }); }
});

test("portable context index keeps search available without node:sqlite", async () => {
  const item = await fixture();
  try {
    await createNote(item.runtime.projects.get(item.projectPath), { title: "Portable", body: "Offline architecture handbook" });
    const indexer = new ProjectIndexer({ preferSqlite: false });
    const result = await indexer.rebuild(item.projectPath);
    assert.equal(result.engine, "portable");
    const matches = await indexer.search(item.projectPath, "architecture handbook");
    assert.equal(matches[0]?.title, "Portable");
  } finally { await rm(item.root, { recursive: true, force: true }); }
});

test("portable search tolerates inflections and partial term matches", async () => {
  const item = await fixture();
  try {
    const project = item.runtime.projects.get(item.projectPath);
    await createNote(project, { title: "Transformer Architecture", body: "Трансформеры обрабатывают последовательности через self-attention." });
    await createNote(project, { title: "Заметки о планах", body: "Простые хозяйственные записи без отношения к теме." });
    const indexer = new ProjectIndexer({ preferSqlite: false });
    await indexer.rebuild(item.projectPath);

    // Russian inflection of a word that appears only in another form in the document.
    assert.equal((await indexer.search(item.projectPath, "трансформеров"))[0]?.title, "Transformer Architecture");
    // Multi-term query where one term is missing from the best document entirely.
    const mixed = await indexer.search(item.projectPath, "трансформеры attention");
    assert.equal(mixed[0]?.title, "Transformer Architecture");
    // Unrelated documents stay out of the results.
    assert.equal((await indexer.search(item.projectPath, "трансформеров")).some((match) => match.title === "Заметки о планах"), false);
  } finally { await rm(item.root, { recursive: true, force: true }); }
});

test("project context catalogs plain notes and resolves relevant content without artifact wrappers", async () => {
  const item = await fixture();
  try {
    const note = await createNote(item.runtime.projects.get(item.projectPath), {
      title: "Transformer architecture",
      body: "Self-attention connects tokens through queries, keys and values."
    });
    const preview = await item.runtime.context.preview(item.projectPath);
    assert.equal(preview.sources.some((source) => source.projectRelativePath === note.relativePath && source.kind === "note"), true);
    const researched = await item.runtime.context.research({
      projectPath: item.projectPath,
      query: "How does self-attention work?",
      projectRelativePaths: [note.relativePath],
      budgetTokens: 500,
      recipient: "local"
    });
    assert.match(researched.text ?? "", /queries, keys and values/);
    assert.equal(researched.sources[0]?.projectRelativePath, note.relativePath);
  } finally { await rm(item.root, { recursive: true, force: true }); }
});

test("OpenAI-compatible replies stream as plain text while plans remain bounded JSON", async () => {
  const originalFetch = globalThis.fetch;
  try {
    globalThis.fetch = (async (_input: string | URL | Request, init?: RequestInit) => {
      const request = JSON.parse(String(init?.body)) as { stream?: boolean };
      if (request.stream) {
        const encoder = new TextEncoder();
        const chunks = [
          `data: ${JSON.stringify({ model: "local-1", choices: [{ delta: { content: "При" } }] })}\r\n`,
          `data: ${JSON.stringify({ model: "local-1", choices: [{ delta: { content: "вет" }, finish_reason: "stop" }] })}`
        ];
        return new Response(new ReadableStream({
          start(controller) {
            controller.enqueue(encoder.encode(chunks[0].slice(0, 17)));
            controller.enqueue(encoder.encode(chunks[0].slice(17) + chunks[1]));
            controller.close();
          }
        }), { headers: { "content-type": "text/event-stream" } });
      }
      return new Response(JSON.stringify({
        model: "local-1",
        choices: [{ message: { content: "```json\n{\"goal\":\"read only\",\"steps\":[]}\n```" } }]
      }), { headers: { "content-type": "application/json" } });
    }) as typeof fetch;
    const provider = new OpenAICompatibleProvider("test.local", "http://127.0.0.1:1234/v1/", {
      async set() {}, async get() { return undefined; }, async delete() {}
    });
    const deltas: string[] = [];
    const reply = await requestAgentReply(provider, "local-1", "Поздоровайся", "", "/project", {
      onDelta: (delta) => deltas.push(delta)
    });
    assert.equal(reply.text, "Привет");
    assert.equal(deltas.join(""), "Привет");
    const plan = await requestAgentPlan(provider, "local-1", "Только чтение", "", {
      type: "object",
      required: ["goal", "steps"],
      properties: { goal: { type: "string" }, steps: { type: "array" } }
    }, "/project");
    assert.deepEqual(plan.plan, { goal: "read only", steps: [] });
  } finally {
    globalThis.fetch = originalFetch;
  }
});


test("operation schema validation resolves local refs and rejects unsafe patterns", () => {
  const schema = {
    type: "object", required: ["count", "label"], additionalProperties: false,
    properties: {
      count: { $ref: "#/$defs/positiveInteger" },
      label: { type: "string", pattern: "^[a-z-]+$" }
    },
    $defs: { positiveInteger: { type: "integer", exclusiveMinimum: 0 } }
  };
  assert.equal(validateJsonSchema(schema, { count: 2, label: "valid-label" }).valid, true);
  assert.equal(validateJsonSchema(schema, { count: 0, label: "INVALID", extra: true }).valid, false);
  assert.equal(validateJsonSchema({ type: "string", pattern: "(a+)+$" }, "aaaaaaaa!").issues.some((issue) => issue.includes("unsafe")), true);
});


test("MCP context adapter maps resources/read into a bounded context envelope", async () => {
  const item = await fixture();
  const originalFetch = globalThis.fetch;
  try {
    globalThis.fetch = (async (_input: string | URL | Request, init?: RequestInit) => {
      const request = JSON.parse(String(init?.body)) as { id: string; method: string; params: Record<string, unknown> };
      let result: unknown;
      if (request.method === "resources/read") {
        assert.equal(request.params.uri, "osnova://resources/artifact-1");
        result = { contents: [
          { uri: request.params.uri, mimeType: "text/plain", text: "MCP knowledge" },
          { uri: `${request.params.uri}/image`, mimeType: "image/png", blob: "ignored" }
        ] };
      } else if (request.method === "tools/call" && request.params.name === "example.mcp.inline") {
        result = { artifacts: [{ type: "example.remote.text", payloads: [{ path: "remote.txt", mediaType: "text/plain", contentBase64: Buffer.from("remote bytes").toString("base64") }] }] };
      } else if (request.method === "tools/call") result = { task: { taskId: "task-1", status: "working" } };
      else if (request.method === "tasks/get") result = { taskId: "task-1", status: "completed", result: { structured: { completed: true } } };
      else throw new Error(`Unexpected MCP method: ${request.method}`);
      return new Response(JSON.stringify({ jsonrpc: "2.0", id: request.id, result }), { status: 200, headers: { "content-type": "application/json" } });
    }) as typeof fetch;
    const envelope = await item.runtime.supervisor.call<{
      text: string; sources: Array<{ artifactId: string }>; providerVersion: string;
    }>({ id: "example.mcp", kind: "remote", lifecycle: "job", endpoint: "https://mcp.example.invalid/rpc", protocol: "mcp" }, "context/resolve", {
      resourceUri: "osnova://resources/artifact-1", artifactId: "artifact-1", providerId: "example.context", level: "expanded"
    }, { paths: { input: "", work: "", outbox: "", models: "" } });
    assert.match(envelope.text, /MCP knowledge/);
    assert.match(envelope.text, /Binary MCP resource: image\/png/);
    assert.equal(envelope.sources[0].artifactId, "artifact-1");
    assert.equal(envelope.providerVersion, "mcp.resources/1");
    const controller = new AbortController();
    const taskResult = await item.runtime.supervisor.invoke(
      { id: "example.mcp", kind: "remote", lifecycle: "job", endpoint: "https://mcp.example.invalid/rpc", protocol: "mcp" },
      {
        jobId: "mcp-job", projectId: "project", projectPath: item.projectPath, operation: {
          id: "example.mcp.run", toolId: "example.mcp", version: "1", title: "MCP", inputSchema: {}, outputSchema: {},
          risk: "network-egress", agentVisibility: "explicit", execution: "job", timeoutSeconds: 5, permissions: ["network:use"]
        }, arguments: {}, inputPath: "", workPath: "", outboxPath: "", modelsPath: "", signal: controller.signal, progress() {}
      }
    );
    assert.deepEqual(taskResult.structured, { completed: true });
    const remoteOutbox = path.join(item.root, "remote-outbox");
    await mkdir(remoteOutbox);
    await item.runtime.supervisor.invoke(
      { id: "example.mcp", kind: "remote", lifecycle: "job", endpoint: "https://mcp.example.invalid/rpc", protocol: "mcp" },
      {
        jobId: "mcp-inline", projectId: "project", projectPath: item.projectPath, operation: {
          id: "example.mcp.inline", toolId: "example.mcp", version: "1", title: "MCP inline", inputSchema: {}, outputSchema: {},
          risk: "network-egress", agentVisibility: "explicit", execution: "job", timeoutSeconds: 5, permissions: ["network:use"]
        }, arguments: {}, inputPath: "", workPath: "", outboxPath: remoteOutbox, modelsPath: "", signal: controller.signal, progress() {}
      }
    );
    assert.equal(await readFile(path.join(remoteOutbox, "remote.txt"), "utf8"), "remote bytes");
  } finally {
    globalThis.fetch = originalFetch;
    await rm(item.root, { recursive: true, force: true });
  }
});

test("OCI driver builds an isolated digest-pinned invocation", async (context) => {
  if (process.platform === "win32") { context.skip("Fake OCI executable fixture is POSIX-only."); return; }
  const item = await fixture();
  const previousCommand = process.env.OSNOVA_OCI_COMMAND;
  try {
    const argsPath = path.join(item.root, "oci-args.json");
    const fakeOci = path.join(item.root, "fake-oci.cjs");
    await writeFile(fakeOci, `#!/usr/bin/env node\nconst fs=require("node:fs");const rl=require("node:readline").createInterface({input:process.stdin});fs.writeFileSync(${JSON.stringify(argsPath)},JSON.stringify(process.argv.slice(2)));rl.on("line",line=>{const q=JSON.parse(line);if(q.id===undefined)return;const result=q.method==="initialize"?{protocolVersion:"1"}:q.method==="shutdown"?{ok:true}:q.method==="jobs/start"?{structured:{isolated:true}}:{status:"ready"};process.stdout.write(JSON.stringify({jsonrpc:"2.0",id:q.id,result})+"\\n");});\n`);
    await chmod(fakeOci, 0o700);
    process.env.OSNOVA_OCI_COMMAND = fakeOci;
    const invocationRoot = path.join(item.root, "oci-invocation");
    const inputPath = path.join(invocationRoot, "input");
    const workPath = path.join(invocationRoot, "work");
    const outboxPath = path.join(invocationRoot, "outbox");
    const modelsPath = path.join(invocationRoot, "models");
    await Promise.all([inputPath, workPath, outboxPath, modelsPath].map((directory) => mkdir(directory, { recursive: true })));
    const result = await item.runtime.supervisor.invoke({
      id: "example.oci", kind: "oci", lifecycle: "job",
      image: `example.invalid/tool@sha256:${"0".repeat(64)}`,
      resources: { cpu: 2, memoryMb: 256, diskMb: 32, network: false }
    }, {
      jobId: "oci-job", projectId: "project", projectPath: item.projectPath,
      operation: { id: "example.oci.run", toolId: "example.oci", version: "1", title: "OCI", inputSchema: {}, outputSchema: {}, risk: "project-write", agentVisibility: "explicit", execution: "job", timeoutSeconds: 5, permissions: [] },
      arguments: {}, inputPath, workPath, outboxPath, modelsPath, signal: new AbortController().signal, progress() {}
    });
    assert.deepEqual(result.structured, { isolated: true });
    const args = JSON.parse(await readFile(argsPath, "utf8")) as string[];
    assert.deepEqual(args.slice(0, 5), ["run", "--rm", "-i", "--network", "none"]);
    assert.equal(args.includes("--read-only"), true);
    assert.equal(args.includes("no-new-privileges"), true);
    assert.equal(args.join(" ").includes(item.projectPath), false);
    assert.equal(args.join(" ").includes("docker.sock"), false);
    assert.equal(args.at(-1), `example.invalid/tool@sha256:${"0".repeat(64)}`);
  } finally {
    if (previousCommand === undefined) delete process.env.OSNOVA_OCI_COMMAND;
    else process.env.OSNOVA_OCI_COMMAND = previousCommand;
    await item.runtime.shutdown();
    await rm(item.root, { recursive: true, force: true });
  }
});

test("process tools cannot exceed their declared writable disk budget", async () => {
  const item = await fixture();
  const entry = path.join(item.root, "disk-tool.mjs");
  const directories = await createInvocationDirectories(path.join(item.root, "disk-runs"), "disk-job");
  await writeFile(entry, `import fs from "node:fs/promises";import path from "node:path";import readline from "node:readline";readline.createInterface({input:process.stdin}).on("line",async line=>{const q=JSON.parse(line);if(q.id===undefined)return;let result;if(q.method==="initialize")result={protocolVersion:"1"};else if(q.method==="shutdown")result={ok:true};else if(q.method==="jobs/start"){await fs.writeFile(path.join(q.params.paths.outbox,"oversized.bin"),Buffer.alloc(2*1024*1024));result={structured:{ok:true}};}else throw new Error("unknown");process.stdout.write(JSON.stringify({jsonrpc:"2.0",id:q.id,result})+"\\n");});\n`);
  try {
    await assert.rejects(() => item.runtime.supervisor.invoke(
      { id: "example.disk", kind: "node-process", lifecycle: "job", entry, resources: { diskMb: 1 } },
      {
        jobId: "disk-job", projectId: "test", projectPath: item.projectPath,
        operation: { id: "example.disk.run", toolId: "example.disk", version: "1", title: "Disk", inputSchema: {}, outputSchema: {}, risk: "project-write", agentVisibility: "explicit", execution: "job", permissions: [], timeoutSeconds: 10 },
        arguments: {}, inputPath: directories.input, workPath: directories.work, outboxPath: directories.outbox, modelsPath: directories.models,
        signal: new AbortController().signal, progress() {}
      }
    ), /declared 1 MiB disk limit/);
  } finally {
    await item.runtime.shutdown();
    await removeInvocationDirectories(directories.root);
    await rm(item.root, { recursive: true, force: true });
  }
});

test("interrupted jobs recover without mutating project", async () => {
  const root = await mkdtemp(path.join(os.tmpdir(), "osnova-jobs-test-"));
  try {
    const first = new JobManager(root);
    await first.initialize();
    const job = await first.create({ projectPath: "/project", operationId: "test.op", input: {} });
    await first.transition(job.id, "running");
    const approval = await first.create({ projectPath: "/project", operationId: "test.approval", input: {}, status: "waiting-approval" });
    const recovered = new JobManager(root);
    await recovered.initialize();
    assert.equal(recovered.get(job.id).status, "interrupted");
    assert.equal(recovered.get(approval.id).status, "waiting-approval");
  } finally { await rm(root, { recursive: true, force: true }); }
});

test("model manager verifies content and provider config never enters the project", async () => {
  const item = await fixture();
  try {
    await assert.rejects(() => item.runtime.models.install({ id: "example.model", version: "1", source: "missing", sha256: "../../outside", size: 1, license: "MIT" }), /sha256/);
    const payloadPath = path.join(item.root, "model.bin");
    const payload = Buffer.from("local-model");
    await writeFile(payloadPath, payload);
    const sha256 = createHash("sha256").update(payload).digest("hex");
    const model = await item.runtime.models.install({ id: "example.model", version: "1", source: payloadPath, sha256, size: payload.length, license: "MIT", platforms: [process.platform as "darwin" | "win32"] });
    assert.equal((await item.runtime.models.resolve(model.sha256)).id, "example.model");
    await item.runtime.configureModelProvider({ id: "example.local", type: "openai-compatible", endpoint: "http://127.0.0.1:1234/v1/" });
    const projectManifest = await readFile(path.join(item.projectPath, "osnova.json"), "utf8");
    assert.equal(projectManifest.includes("example.local"), false);
    const usagePath = path.join(item.root, "runtime", "model-usage", "project.json");
    await mkdir(path.dirname(usagePath), { recursive: true });
    await writeFile(usagePath, JSON.stringify({ projectPath: item.projectPath, models: [sha256] }));
    await assert.rejects(() => item.runtime.removeModel(sha256), /still required/);
    await writeFile(usagePath, JSON.stringify({ projectPath: item.projectPath, models: [] }));
    await item.runtime.removeModel(sha256);
    await assert.rejects(() => item.runtime.models.resolve(sha256), /not installed/);
  } finally { await rm(item.root, { recursive: true, force: true }); }
});

test("connector checkpoints after atomic artifact publication", async () => {
  const item = await fixture();
  try {
    item.runtime.connectors.register("example.feed", {
      async *pull({ checkpoint }) {
        if (checkpoint) return;
        yield { cursor: "cursor-1", candidate: { type: "example.feed.item", title: "Imported", payloads: [{ path: "item.md", mediaType: "text/markdown" }], context: { mode: "automatic" }, metadata: { inlineText: "Imported material" } } };
      }
    }, { extensionId: "osnova.builtin", permissions: [], risk: "safe-read", scope: "project", produces: ["example.feed.item"] });
    const first = await item.runtime.connectors.sync(item.runtime.projects.get(item.projectPath), "example.feed", { producedTypes: ["example.feed.item"] });
    const second = await item.runtime.connectors.sync(item.runtime.projects.get(item.projectPath), "example.feed", { producedTypes: ["example.feed.item"] });
    assert.equal(first.length, 1);
    assert.equal(second.length, 0);
  } finally { await rm(item.root, { recursive: true, force: true }); }
});

test("artifact batch rolls back earlier candidates when a later candidate is invalid", async () => {
  const item = await fixture();
  try {
    const outbox = path.join(item.root, "outbox");
    await mkdir(outbox);
    await writeFile(path.join(outbox, "valid.md"), "valid", "utf8");
    await writeFile(path.join(outbox, "fake.wav"), "not audio", "utf8");
    await assert.rejects(() => item.runtime.ingestor.publish(item.runtime.projects.get(item.projectPath), outbox, [
      { id: "valid-first", type: "example.output", payloads: [{ path: "valid.md", mediaType: "text/markdown" }] },
      { id: "invalid-second", type: "example.output", payloads: [{ path: "fake.wav", mediaType: "audio/wav" }] }
    ], { source: "operation" }, { producedTypes: ["example.output"], allowedMediaTypes: ["text/markdown", "audio/wav"] }), /MIME mismatch/);
    assert.equal((await (await import("@osnova/project")).listArtifacts(item.projectPath)).length, 0);
    await assert.rejects(() => readFile(path.join(item.projectPath, "artifacts", "data", "valid-first", "valid.md")));
  } finally { await rm(item.root, { recursive: true, force: true }); }
});

test("artifact staging rejects a crafted descriptor that escapes the project", async () => {
  const item = await fixture();
  try {
    await writeFile(path.join(item.root, "secret.txt"), "outside", "utf8");
    await writeFile(path.join(item.projectPath, "artifacts", "malicious.json"), JSON.stringify({
      schemaVersion: "1", id: "malicious", type: "example.input", createdAt: new Date().toISOString(),
      payloads: [{ path: "../secret.txt", mediaType: "text/plain", size: 7, sha256: "0".repeat(64) }],
      provenance: { source: "manual" }
    }));
    const input = path.join(item.root, "staged-input");
    await mkdir(input);
    await assert.rejects(() => stageArtifacts(item.projectPath, ["malicious"], input), /Unsafe relative path/);
    await assert.rejects(() => item.runtime.context.resolve({ projectPath: item.projectPath, artifactIds: ["malicious"], level: "expanded", budgetTokens: 100, recipient: "local" }), /Unsafe relative path/);
  } finally { await rm(item.root, { recursive: true, force: true }); }
});

test("unsigned extension requires developer mode and connects with scoped grants", async () => {
  const item = await fixture();
  let resumed: OsnovaRuntime | undefined;
  try {
    const source = path.join(item.root, "extension");
    await mkdir(source, { recursive: true });
    await writeFile(path.join(source, "extension.json"), JSON.stringify({
      manifestVersion: "1", id: "example.echo", name: "Echo", version: "1.0.0", osnova: { minVersion: "0.2.0" },
      permissions: ["artifact:read", "artifact:create", "background:run"],
      runtimes: [{ id: "example.echo.runtime", kind: "node-process", lifecycle: "project", idleTimeoutSeconds: 60, entry: "server.js" }],
      contributes: {
        tools: [{ id: "example.echo.tool", title: "Echo", runtimeId: "example.echo.runtime" }],
        operations: [
          { id: "example.echo.run", toolId: "example.echo.tool", version: "1.0.0", title: "Echo", inputSchema: { type: "object", required: ["text"], properties: { text: { type: "string" } } }, outputSchema: { type: "object", required: ["echoed"], properties: { echoed: { type: "string" } } }, accepts: ["example.echo.report"], produces: ["example.echo.report"], risk: "project-write", agentVisibility: "explicit", execution: "immediate", permissions: ["artifact:read", "artifact:create", "background:run"] },
          { id: "example.echo.danger", toolId: "example.echo.tool", version: "1.0.0", title: "Dangerous echo", inputSchema: { type: "object", required: ["text"], properties: { text: { type: "string" } } }, outputSchema: { type: "object", required: ["echoed"], properties: { echoed: { type: "string" } } }, produces: ["example.echo.report"], risk: "external-side-effect", agentVisibility: "explicit", execution: "immediate", permissions: ["artifact:create", "background:run"] }
        ],
        artifactTypes: [{ id: "example.echo.report", title: "Echo report", mediaTypes: ["text/markdown"], context: { mode: "custom", providerId: "example.echo.context" } }],
        contextProviders: [{ id: "example.echo.context", artifactTypes: ["example.echo.report"], version: "1.0.0", runtimeId: "example.echo.runtime" }],
        modelProviders: [{ id: "example.echo.model", title: "Echo model", runtimeId: "example.echo.runtime", recipient: "local", capabilities: ["chat", "structured-output"] }]
      }
    }, null, 2));
    await writeFile(path.join(source, "server.js"), `const fs=require("node:fs");const path=require("node:path");const rl=require("node:readline").createInterface({input:process.stdin});rl.on("line",line=>{const q=JSON.parse(line);if(q.id===undefined)return;let result;if(q.method==="initialize")result={protocolVersion:"1"};else if(q.method==="shutdown")result={ok:true};else if(q.method==="health")result={status:"ready",pid:process.pid};else if(q.method==="context/resolve")result={level:q.params.level,text:"custom context:"+process.pid,sources:[{artifactId:q.params.artifactId}],sensitivity:"project",allowedRecipients:["local","cloud"],tokenEstimate:999,truncated:false,providerVersion:"example.echo.context/1"};else if(q.method==="models/complete"){const properties=q.params.request.responseSchema?.properties;const text=!properties?"Generated answer":JSON.stringify(properties.queries?{queries:[],projectRelativePaths:[],artifactIds:[]}:{goal:"generated",steps:[]});result={text,model:q.params.request.model};}else if(q.method==="jobs/start"){fs.writeFileSync(path.join(q.params.paths.outbox,"report.md"),q.params.input.text);result={structured:{echoed:q.params.input.text,pid:process.pid},artifacts:[{type:"example.echo.report",payloads:[{path:"report.md",mediaType:"text/markdown"}],context:{mode:"custom",providerId:"example.echo.context"}}]};}else throw new Error("unknown");process.stdout.write(JSON.stringify({jsonrpc:"2.0",id:q.id,result})+"\\n");});\n`);
    const packagePath = path.join(item.root, "echo.osnova-package.json");
    await packExtension(source, packagePath);
    const tamperedPath = path.join(item.root, "echo-tampered.osnova-package.json");
    const tampered = JSON.parse(await readFile(packagePath, "utf8")) as { manifest: { name: string } };
    tampered.manifest.name = "Tampered outer manifest";
    await writeFile(tamperedPath, JSON.stringify(tampered));
    await assert.rejects(() => item.runtime.extensions.install(tamperedPath, { allowUnsigned: true }), /does not match/);
    await assert.rejects(() => item.runtime.extensions.install(packagePath), /Unsigned extension/);
    await item.runtime.extensions.install(packagePath, { allowUnsigned: true });
    const untrustedManifest = JSON.parse(await readFile(path.join(item.projectPath, "osnova.json"), "utf8")) as { extensions?: unknown[] };
    untrustedManifest.extensions = [{ id: "example.echo", version: "1.0.0", enabled: true }];
    await writeFile(path.join(item.projectPath, "osnova.json"), JSON.stringify(untrustedManifest, null, 2));
    await mkdir(path.join(item.projectPath, ".osnova", "extensions"), { recursive: true });
    await writeFile(path.join(item.projectPath, ".osnova", "extensions", "grants.json"), JSON.stringify({ "example.echo": ["artifact:read", "artifact:create", "background:run"] }));
    await item.runtime.openProject(item.projectPath);
    await assert.rejects(() => item.runtime.operations.invoke({ projectPath: item.projectPath, operationId: "example.echo.run", arguments: { text: "untrusted grant" } }), /not granted/);
    await item.runtime.extensions.connect(item.projectPath, "example.echo", "1.0.0", ["artifact:read", "artifact:create", "background:run"]);
    assert.equal(item.runtime.registry.get("example.echo.run").extensionId, "example.echo");
    const started = await item.runtime.startRuntime("example.echo.runtime", item.projectPath);
    assert.equal(started.health.status, "ready");
    assert.equal(started.state?.status, "running");
    const job = await item.runtime.operations.invokeAndWait({ projectPath: item.projectPath, operationId: "example.echo.run", arguments: { text: "advanced result" }, publishArtifacts: true });
    assert.equal(job.status, "succeeded", job.error);
    assert.equal(job.artifactIds?.length, 1);
    const customContext = await item.runtime.context.resolve({ projectPath: item.projectPath, artifactIds: job.artifactIds, level: "expanded", budgetTokens: 20, recipient: "local" });
    const processId = (job.result?.structured as { pid?: number } | undefined)?.pid;
    assert.equal(customContext.text, `custom context:${processId}`);
    assert.equal(customContext.tokenEstimate < 20, true);

    const dangerous = await item.runtime.operations.invoke({ projectPath: item.projectPath, operationId: "example.echo.danger", arguments: { text: "approved after restart" }, publishArtifacts: true });
    assert.equal(dangerous.status, "waiting-approval");
    await item.runtime.shutdown();
    resumed = new OsnovaRuntime(path.join(item.root, "runtime"));
    await resumed.initialize();
    await resumed.openProject(item.projectPath);
    await resumed.operations.decide(dangerous.id, { planId: "manual", stepId: dangerous.id, approved: true, scope: "once", decidedAt: new Date().toISOString() });
    assert.equal((await waitForRuntimeJob(resumed, dangerous.id, (status) => ["succeeded", "failed"].includes(status))).status, "succeeded");

    const pending = await resumed.operations.invoke({ projectPath: item.projectPath, operationId: "example.echo.run", arguments: { text: "publish after restart" }, publishArtifacts: false });
    await waitForRuntimeJob(resumed, pending.id, (_status, job) => job.statusMessage === "Waiting for artifact publication.");
    await resumed.shutdown();
    const publisher = new OsnovaRuntime(path.join(item.root, "runtime"));
    resumed = publisher;
    await publisher.initialize();
    await publisher.openProject(item.projectPath);
    const published = await publisher.operations.publishPending(pending.id);
    assert.equal(published.length, 1);
    assert.equal(publisher.jobs.get(pending.id).status, "succeeded");
  } finally { await resumed?.shutdown(); await item.runtime.supervisor.stop(); await rm(item.root, { recursive: true, force: true }); }
});

test("project locks route different installed versions of the same extension", async () => {
  const item = await fixture();
  try {
    const incompatiblePackage = await createVersionedExtensionPackage(item.root, "9.0.0", "future", "9.0.0");
    await assert.rejects(() => item.runtime.extensions.install(incompatiblePackage, { allowUnsigned: true }), /requires Osnova 9.0.0/);
    const firstPackage = await createVersionedExtensionPackage(item.root, "1.0.0", "one");
    const secondPackage = await createVersionedExtensionPackage(item.root, "2.0.0", "two");
    await item.runtime.extensions.install(firstPackage, { allowUnsigned: true });
    await item.runtime.extensions.install(secondPackage, { allowUnsigned: true });

    const secondProject = path.join(item.root, "project-two");
    await item.runtime.projects.create({ rootPath: secondProject, id: "test-two", name: "Test two" });
    await item.runtime.extensions.connect(item.projectPath, "example.versioned", "1.0.0", [], "^1.0.0");
    await item.runtime.extensions.connect(secondProject, "example.versioned", "2.0.0", [], "^2.0.0");

    const first = await item.runtime.operations.invokeAndWait({ projectPath: item.projectPath, operationId: "example.versioned.run", arguments: {} });
    const second = await item.runtime.operations.invokeAndWait({ projectPath: secondProject, operationId: "example.versioned.run", arguments: {} });
    assert.equal((first.result?.structured as { marker?: string } | undefined)?.marker, "one");
    assert.equal((second.result?.structured as { marker?: string } | undefined)?.marker, "two");
    assert.equal(item.runtime.registry.list({ extensionVersions: item.runtime.projects.extensionVersions(item.projectPath) }).find((operation) => operation.definition.id === "example.versioned.run")?.extensionVersion, "1.0.0");
    assert.equal(item.runtime.registry.list({ extensionVersions: item.runtime.projects.extensionVersions(secondProject) }).find((operation) => operation.definition.id === "example.versioned.run")?.extensionVersion, "2.0.0");

    const firstLock = JSON.parse(await readFile(path.join(item.projectPath, ".osnova", "extensions", "lock.json"), "utf8")) as { extensions: Record<string, { version: string }> };
    const secondLock = JSON.parse(await readFile(path.join(secondProject, ".osnova", "extensions", "lock.json"), "utf8")) as { extensions: Record<string, { version: string }> };
    assert.equal(firstLock.extensions["example.versioned"].version, "1.0.0");
    assert.equal(secondLock.extensions["example.versioned"].version, "2.0.0");
    const doctor = await item.runtime.diagnostics.doctor(secondProject);
    assert.equal(doctor.checks.find((check) => check.id === "project.extensions")?.status, "ok");
    const firstInstall = (await item.runtime.extensions.list()).find((extension) => extension.id === "example.versioned" && extension.version === "1.0.0");
    assert.ok(firstInstall);
    await writeFile(path.join(firstInstall.path, "server.js"), "// modified after installation\n");
    await item.runtime.shutdown();
    const recovered = new OsnovaRuntime(path.join(item.root, "runtime"));
    try {
      await recovered.initialize();
      await recovered.openProject(item.projectPath);
      const recoveredDoctor = await recovered.diagnostics.doctor(item.projectPath);
      assert.equal(recoveredDoctor.checks.find((check) => check.id === "extensions")?.status, "error");
      assert.equal(recoveredDoctor.checks.find((check) => check.id === "project.extensions")?.status, "warning");
    } finally { await recovered.shutdown(); }
  } finally { await item.runtime.shutdown(); await rm(item.root, { recursive: true, force: true }); }
});



test("a copied project opens without derived state, AI, OCI, or required extensions", async () => {
  const item = await fixture();
  let destinationRuntime: OsnovaRuntime | undefined;
  try {
    const session = await createSession(item.runtime.projects.get(item.projectPath), { title: "Portable session" });
    await item.runtime.operations.invokeAndWait({
      projectPath: item.projectPath, sessionId: session.id, operationId: "osnova.notes.create",
      arguments: { title: "Portable note", body: "Moves between computers" }, publishArtifacts: true
    });
    const manifestPath = path.join(item.projectPath, "osnova.json");
    const manifest = JSON.parse(await readFile(manifestPath, "utf8")) as { extensions?: unknown[] };
    manifest.extensions = [{ id: "missing.example.tool", version: "^1.0.0", enabled: true }];
    await writeFile(manifestPath, JSON.stringify(manifest, null, 2));

    const copiedProject = path.join(item.root, "copied-project");
    await cp(item.projectPath, copiedProject, { recursive: true });
    await rm(path.join(copiedProject, ".osnova"), { recursive: true, force: true });
    destinationRuntime = new OsnovaRuntime(path.join(item.root, "destination-runtime"));
    await destinationRuntime.initialize();
    await destinationRuntime.openProject(copiedProject);
    assert.equal((await (await import("@osnova/project")).listSessions(copiedProject)).length, 1);
    assert.equal((await (await import("@osnova/project")).listArtifacts(copiedProject)).length, 1);
    const doctor = await destinationRuntime.diagnostics.doctor(copiedProject);
    assert.equal(doctor.checks.find((check) => check.id === "project.extensions")?.status, "warning");
    const builtIn = await destinationRuntime.operations.invokeAndWait({ projectPath: copiedProject, operationId: "osnova.notes.create", arguments: { title: "Still works" }, publishArtifacts: true });
    assert.equal(builtIn.status, "succeeded");
  } finally {
    await destinationRuntime?.shutdown();
    await item.runtime.shutdown();
    await rm(item.root, { recursive: true, force: true });
  }
});

test("local RPC rejects a wrong token and executes an operation", async (context) => {
  const item = await fixture();
  let server;
  try { server = await startRpcServer(item.runtime); }
  catch (error) {
    if (error instanceof Error && "code" in error && error.code === "EPERM") {
      await rm(item.root, { recursive: true, force: true });
      context.skip("Unix sockets are blocked by the current test sandbox.");
      return;
    }
    throw error;
  }
  try {
    const bad = new RpcClient(server.address, "wrong");
    await assert.rejects(() => bad.request("runtime.status"), /Unauthorized/);
    bad.close();
    const client = new RpcClient(server.address, server.token);
    await rm(path.join(item.projectPath, "sessions"), { recursive: true, force: true });
    const session = await client.request<{ id: string }>("session.create", {
      projectPath: item.projectPath,
      title: "Разобрать архитектуру трансформера"
    });
    assert.match(session.id, /^session-[a-f0-9-]{36}$/);
    const notifications: string[] = [];
    client.on("job.changed", () => notifications.push("job.changed"));
    client.on("artifact.published", () => notifications.push("artifact.published"));
    const job = await client.request<{ id: string; status: string }>("operation.invoke", {
      projectPath: item.projectPath, sessionId: session.id, operationId: "osnova.notes.create",
      arguments: { title: "Summary", body: "Verified" }, publishArtifacts: true
    });
    await waitForRuntimeJob(item.runtime, job.id, (status) => ["succeeded", "failed"].includes(status));
    const created = await client.request<Array<{ provenance: { runId?: string } }>>("artifact.list", { projectPath: item.projectPath });
    assert.equal(created[0]?.provenance.runId, job.id);
    assert.equal(notifications.includes("job.changed"), true);
    assert.equal(notifications.includes("artifact.published"), true);
    client.close();
  } finally { await server.close(); await item.runtime.shutdown(); await rm(item.root, { recursive: true, force: true, maxRetries: 10, retryDelay: 20 }); }
});

function waitForRuntimeJob(runtime: OsnovaRuntime, jobId: string, predicate: (status: string, job: ReturnType<OsnovaRuntime["jobs"]["get"]>) => boolean) {
  const current = runtime.jobs.get(jobId);
  if (predicate(current.status, current)) return Promise.resolve(current);
  return new Promise<ReturnType<OsnovaRuntime["jobs"]["get"]>>((resolve) => {
    const changed = (job: ReturnType<OsnovaRuntime["jobs"]["get"]>) => {
      if (job.id === jobId && predicate(job.status, job)) { runtime.jobs.off("changed", changed); resolve(job); }
    };
    runtime.jobs.on("changed", changed);
  });
}

async function createVersionedExtensionPackage(root: string, version: string, marker: string, minVersion = "0.2.0"): Promise<string> {
  const source = path.join(root, `versioned-${version}`);
  await mkdir(source, { recursive: true });
  await writeFile(path.join(source, "extension.json"), JSON.stringify({
    manifestVersion: "1", id: "example.versioned", name: "Versioned", version, osnova: { minVersion }, permissions: [],
    runtimes: [{ id: "example.versioned.runtime", kind: "node-process", lifecycle: "job", entry: "server.js" }],
    contributes: {
      tools: [{ id: "example.versioned.tool", title: "Versioned", runtimeId: "example.versioned.runtime" }],
      operations: [{
        id: "example.versioned.run", toolId: "example.versioned.tool", version, title: "Versioned",
        inputSchema: { type: "object", additionalProperties: false },
        outputSchema: { type: "object", required: ["marker"], properties: { marker: { type: "string" } }, additionalProperties: false },
        risk: "safe-read", agentVisibility: "explicit", execution: "immediate", permissions: []
      }]
    }
  }, null, 2));
  await writeFile(path.join(source, "server.js"), `const rl=require("node:readline").createInterface({input:process.stdin});rl.on("line",line=>{const q=JSON.parse(line);if(q.id===undefined)return;let result;if(q.method==="initialize")result={protocolVersion:"1"};else if(q.method==="shutdown")result={ok:true};else if(q.method==="health")result={status:"ready"};else if(q.method==="jobs/start")result={structured:{marker:${JSON.stringify(marker)}}};else throw new Error("unknown");process.stdout.write(JSON.stringify({jsonrpc:"2.0",id:q.id,result})+"\\n");});\n`);
  const packagePath = path.join(root, `versioned-${version}.osnova-package.json`);
  await packExtension(source, packagePath);
  return packagePath;
}
