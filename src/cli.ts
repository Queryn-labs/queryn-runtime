#!/usr/bin/env node
import { mkdtemp, rm } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { createSession, listArtifacts } from "@queryn/project";
import { QuerynRuntime } from "./runtime.js";
import { startRpcServer } from "./rpc-server.js";

const [command = "help", ...args] = process.argv.slice(2);

void main().catch((error) => {
  process.stderr.write(`${error instanceof Error ? error.stack ?? error.message : String(error)}\n`);
  process.exitCode = 1;
});

async function main(): Promise<void> {
  if (command === "help" || command === "--help" || command === "-h") return printHelp();
  if (command === "selftest") return selftest();
  const runtime = new QuerynRuntime(optional("--runtime-home") ?? undefined);
  await runtime.initialize();
  if (command === "serve") return serve(runtime);
  try {
    if (command === "doctor") return print(await runtime.diagnostics.doctor(optional("--project")));
    if (command === "status") return print(runtime.status());
    if (command === "project:create") return print(await runtime.projects.create({ rootPath: required("--path"), id: required("--id"), name: required("--name"), description: optional("--description") }));
    if (command === "project:open") return print(await runtime.openProject(required("--path")));
    if (command === "project:validate") return print(await runtime.projects.validate(required("--path")));
    if (command === "project:migrate") return print(await runtime.projects.migrate(required("--path"), { dryRun: flag("--dry-run") }));
    if (command === "extension:install" || command === "extension:update") return print(await runtime.extensions.install(required("--package"), { allowUnsigned: flag("--developer-mode") }));
    if (command === "extension:list") return print(await runtime.extensions.list());
    if (command === "extension:rollback") { await runtime.extensions.rollback(required("--extension"), required("--version")); return print({ rolledBack: true }); }
    if (command === "runtime:stop") { await runtime.supervisor.stop(optional("--runtime")); return print(runtime.status()); }
    if (command === "model:list") return print(await runtime.models.list());
    if (command === "model:provider-template-list") return print(runtime.listModelProviderTemplates());
    if (command === "model:provider-list") return print(runtime.agent.listProviders());
    if (command === "model:provider-config-list") return print(await runtime.listModelProviderConfigs());
    if (command === "model:install") return print(await runtime.models.install(jsonObject(required("--dependency")) as never, { allowNetwork: flag("--allow-network") }));
    if (command === "model:remove") { await runtime.removeModel(required("--sha256")); return print({ removed: true }); }
    if (command === "model:provider-configure") {
      const secret = flag("--secret-stdin") ? (await readStandardInput()).trimEnd() : undefined;
      return print(await runtime.configureModelProvider(jsonObject(required("--config")) as never, secret || undefined));
    }
    if (command === "model:provider-remove") { await runtime.removeModelProvider(required("--provider")); return print({ removed: true }); }
    if (command === "job:get") return print(runtime.jobs.get(required("--job")));
    if (command === "job:list") return print(runtime.jobs.list(optional("--project")));
    if (command === "job:cancel") return print(await runtime.jobs.cancel(required("--job")));
    if (command === "approval:decide") {
      const job = runtime.jobs.get(required("--job"));
      await runtime.openProject(job.projectPath);
      await runtime.operations.decide(job.id, approval(job.id, flag("--approve"), optional("--scope")));
      return print(await waitForJob(runtime, job.id, (next) => terminal(next.status)));
    }

    const projectPath = required("--project");
    await runtime.openProject(projectPath);
    if (command === "extension:connect") { await runtime.extensions.connect(projectPath, required("--extension"), required("--version"), (csv(optional("--permissions")) ?? []) as never, optional("--requirement")); return print({ connected: true }); }
    if (command === "extension:disconnect") { await runtime.extensions.disconnect(projectPath, required("--extension")); return print({ connected: false }); }
    if (command === "runtime:start") return print(await runtime.startRuntime(required("--runtime"), projectPath));
    if (command === "session:create") return print(await createSession(runtime.projects.get(projectPath), { title: required("--title"), goal: optional("--goal") }));
    if (command === "session:list") return print(await (await import("@queryn/project")).listSessions(projectPath));
    if (command === "session:events") return print(await (await import("@queryn/project")).readSessionEvents(projectPath, required("--session")));
    if (command === "operation:list") return print(runtime.registry.list({ includeHidden: flag("--all"), extensionVersions: runtime.projects.extensionVersions(projectPath) }));
    if (command === "operation:invoke") {
      let job = await runtime.operations.invoke({
        projectPath, operationId: required("--operation"), arguments: jsonObject(optional("--input") ?? "{}"),
        sessionId: optional("--session"), artifactIds: csv(optional("--artifacts")), publishArtifacts: flag("--publish")
      });
      if (job.status === "waiting-approval" && flag("--approve")) {
        await runtime.operations.decide(job.id, approval(job.id, true, optional("--scope")));
        job = runtime.jobs.get(job.id);
      }
      return print(await waitForJob(runtime, job.id, (next) => terminal(next.status) || next.status === "waiting-approval" || next.statusMessage === "Waiting for artifact publication."));
    }
    if (command === "artifact:list") return print(await listArtifacts(projectPath));
    if (command === "artifact:publish") return print(await runtime.operations.publishPending(required("--job"), csv(optional("--indexes"))?.map(Number)));
    if (command === "context:preview") return print(await runtime.context.preview(projectPath));
    if (command === "context:resolve") return print(await runtime.context.resolve({ projectPath, artifactIds: csv(optional("--artifacts")), level: flag("--expanded") ? "expanded" : "compact", budgetTokens: Number(optional("--budget") ?? 2000), recipient: flag("--cloud") ? "cloud" : "local", approval: flag("--approve") ? approval("context.resolve", true, optional("--scope")) : undefined }));
    if (command === "context:reindex") return print(await runtime.indexer.rebuild(projectPath));
    if (command === "connector:list") return print(runtime.connectors.list());
    if (command === "connector:sync") {
      const job = await runtime.syncConnector(projectPath, required("--connector"), flag("--approve") ? approval(required("--connector"), true, optional("--scope")) : undefined);
      return print(await waitForJob(runtime, job.id, (next) => terminal(next.status)));
    }
    throw new Error(`Unknown command: ${command}`);
  } finally { await runtime.shutdown(); }
}

async function serve(runtime: QuerynRuntime): Promise<void> {
  const handle = await startRpcServer(runtime);
  process.stdout.write(`${JSON.stringify({ address: handle.address, token: handle.token, protocol: "queryn-rpc/1", pid: process.pid })}\n`);
  const close = async () => { await handle.close(); await runtime.shutdown(); process.exit(0); };
  process.once("SIGINT", () => void close());
  process.once("SIGTERM", () => void close());
  await new Promise(() => undefined);
}

async function selftest(): Promise<void> {
  const root = await mkdtemp(path.join(os.tmpdir(), "queryn-runtime-selftest-"));
  try {
    const runtime = new QuerynRuntime(path.join(root, "runtime"));
    await runtime.initialize();
    const projectPath = path.join(root, "project");
    const project = await runtime.projects.create({ rootPath: projectPath, id: "selftest", name: "Runtime self-test" });
    const session = await createSession(project, { title: "Create a note" });
    const job = await runtime.operations.invokeAndWait({ projectPath, sessionId: session.id, operationId: "queryn.notes.create", arguments: { title: "First note", body: "Local-first knowledge." }, publishArtifacts: true });
    if (job.status !== "succeeded") throw new Error(job.error ?? `Unexpected job state: ${job.status}`);
    const artifacts = await listArtifacts(projectPath);
    if (artifacts.length !== 1) throw new Error("Self-test did not create an artifact descriptor.");
    const context = await runtime.context.resolve({ projectPath, artifactIds: [artifacts[0].id], level: "expanded", budgetTokens: 500, recipient: "local" });
    if (!context.text?.includes("Local-first knowledge")) throw new Error("Context extraction failed.");
    print({ ok: true, projectFormat: project.manifest.formatVersion, job: job.status, artifact: artifacts[0].id, contextTokens: context.tokenEstimate });
  } finally { await rm(root, { recursive: true, force: true }); }
}

function optional(name: string): string | undefined { const index = args.indexOf(name); return index >= 0 ? args[index + 1] : undefined; }
function required(name: string): string { const value = optional(name); if (!value) throw new Error(`${name} is required.`); return value; }
function flag(name: string): boolean { return args.includes(name); }
function csv(value?: string): string[] | undefined { return value ? value.split(",").map((item) => item.trim()).filter(Boolean) : undefined; }
function jsonObject(value: string): Record<string, unknown> { const parsed = JSON.parse(value) as unknown; if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) throw new Error("--input must be a JSON object."); return parsed as Record<string, unknown>; }
function terminal(status: string): boolean { return ["succeeded", "failed", "cancelled", "interrupted"].includes(status); }
function approval(stepId: string, approved: boolean, scope?: string) {
  return { planId: "headless-cli", stepId, approved, scope: scope === "operation-project" ? "operation-project" as const : "once" as const, decidedAt: new Date().toISOString() };
}
function waitForJob(runtime: QuerynRuntime, jobId: string, predicate: (job: ReturnType<QuerynRuntime["jobs"]["get"]>) => boolean) {
  const current = runtime.jobs.get(jobId);
  if (predicate(current)) return Promise.resolve(current);
  return new Promise<ReturnType<QuerynRuntime["jobs"]["get"]>>((resolve) => {
    const changed = (job: ReturnType<QuerynRuntime["jobs"]["get"]>) => {
      if (job.id === jobId && predicate(job)) { runtime.jobs.off("changed", changed); resolve(job); }
    };
    runtime.jobs.on("changed", changed);
  });
}
async function readStandardInput(): Promise<string> { let value = ""; for await (const chunk of process.stdin) value += String(chunk); return value; }
function print(value: unknown): void { process.stdout.write(`${JSON.stringify(value, null, 2)}\n`); }
function printHelp(): void {
  process.stdout.write(`queryn-runtime 0.2\n\nCommands:\n  serve | doctor | status | selftest\n  project:create|open|validate|migrate\n  extension:install|update|list|rollback|connect|disconnect\n  runtime:start|stop\n  session:create|list|events\n  operation:list|invoke | approval:decide\n  artifact:list|publish\n  context:preview|resolve|reindex\n  connector:list|sync\n  model:list|install|remove|provider-template-list|provider-list|provider-config-list|provider-configure|provider-remove\n  job:get|list|cancel\n\nUse --project PATH for project-scoped commands. Secrets are accepted only with --secret-stdin.\n`);
}
