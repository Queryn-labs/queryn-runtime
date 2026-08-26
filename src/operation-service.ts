import { createHash } from "node:crypto";
import { createReadStream } from "node:fs";
import { chmod, copyFile, mkdir, readFile, rm, writeFile } from "node:fs/promises";
import path from "node:path";
import type { ApprovalDecision, ArtifactDescriptor, ArtifactRef, JobDescriptor, Permission } from "@queryn/types";
import { appendSessionEvent, readArtifact } from "@queryn/project";
import type { ArtifactIngestor } from "./artifact-ingestor.js";
import type { JobManager } from "./job-manager.js";
import type { OperationRegistry } from "./operation-registry.js";
import type { PolicyEngine, PolicyEvaluation } from "./policy-engine.js";
import type { ModelManager } from "./model-manager.js";
import type { ProjectService } from "./project-service.js";
import { createInvocationDirectories, removeInvocationDirectories, runtimeInvocationScopeRoot, type RuntimeInvocationResult, type RuntimeSupervisor } from "./runtime-supervisor.js";
import { resolveSafeExistingFile, writeJsonAtomic } from "./atomic.js";

export interface InvokeOperationInput {
  projectPath: string;
  operationId: string;
  arguments: Record<string, unknown>;
  sessionId?: string;
  artifactIds?: string[];
  publishArtifacts?: boolean;
  approval?: ApprovalDecision;
  provenanceRunId?: string;
  model?: string;
  recipient?: "local" | "cloud";
}

interface PendingInvocation { input: InvokeOperationInput; jobId: string }

export class OperationService {
  readonly #pending = new Map<string, PendingInvocation>();
  readonly #pendingArtifacts = new Map<string, { outboxPath: string; result: RuntimeInvocationResult; inputRefs?: ArtifactRef[]; provenanceRunId?: string; model?: string }>();

  constructor(
    readonly runtimeRoot: string,
    readonly projects: ProjectService,
    readonly operations: OperationRegistry,
    readonly policy: PolicyEngine,
    readonly jobs: JobManager,
    readonly supervisor: RuntimeSupervisor,
    readonly ingestor: ArtifactIngestor,
    readonly models: ModelManager
  ) {}

  async invoke(input: InvokeOperationInput): Promise<JobDescriptor> {
    const project = this.projects.get(input.projectPath);
    const extensionVersions = this.projects.extensionVersions(project.rootPath);
    this.operations.validateInput(input.operationId, input.arguments, extensionVersions);
    const registered = this.operations.get(input.operationId, extensionVersions);
    await validateArtifactInputs(project.rootPath, input.artifactIds ?? [], registered.definition.accepts ?? []);
    const evaluation = this.#evaluate(project.rootPath, registered.extensionId, registered.definition, input.approval);
    if (!evaluation.allowed) throw new Error(`${evaluation.reason} Missing: ${evaluation.missingPermissions.join(", ")}`);
    const job = await this.jobs.create({
      projectPath: project.rootPath, sessionId: input.sessionId, operationId: input.operationId,
      input: input.arguments, status: evaluation.approvalRequired ? "waiting-approval" : "queued"
    });
    if (input.sessionId) await appendSessionEvent(project.rootPath, input.sessionId, {
      type: "operation-call", data: { jobId: job.id, operationId: input.operationId, arguments: input.arguments }
    });
    if (evaluation.approvalRequired) {
      const pending = { input, jobId: job.id };
      this.#pending.set(job.id, pending);
      await writeJsonAtomic(this.#pendingPath(job.id), pending);
      return job;
    }
    void this.#run(job.id, input);
    return job;
  }

  async invokeAndWait(input: InvokeOperationInput): Promise<JobDescriptor> {
    const job = await this.invoke(input);
    if (job.status === "waiting-approval") return job;
    return new Promise<JobDescriptor>((resolve) => {
      const terminal = new Set(["succeeded", "failed", "cancelled", "interrupted"]);
      const changed = (next: JobDescriptor) => {
        if (next.id === job.id && terminal.has(next.status)) {
          this.jobs.off("changed", changed);
          resolve(next);
        }
      };
      this.jobs.on("changed", changed);
      const current = this.jobs.get(job.id);
      if (terminal.has(current.status)) changed(current);
    });
  }

  async decide(jobId: string, decision: ApprovalDecision): Promise<JobDescriptor> {
    const pending = this.#pending.get(jobId) ?? await this.#readPending(jobId);
    if (!pending) throw new Error(`Job is not waiting for approval: ${jobId}`);
    const registered = this.operations.get(pending.input.operationId, this.projects.extensionVersions(pending.input.projectPath));
    if (decision.stepId !== jobId && decision.stepId !== pending.input.operationId) throw new Error("Approval does not match job or operation.");
    const project = this.projects.get(pending.input.projectPath);
    if (pending.input.sessionId) await appendSessionEvent(project.rootPath, pending.input.sessionId, {
      type: "approval", data: { jobId, operationId: pending.input.operationId, ...decision }
    });
    this.#pending.delete(jobId);
    await rm(this.#pendingPath(jobId), { force: true });
    if (!decision.approved) return this.jobs.cancel(jobId);
    await this.policy.rememberApproval(project.rootPath, registered.definition.id, decision);
    await this.jobs.transition(jobId, "queued");
    void this.#run(jobId, { ...pending.input, approval: decision });
    return this.jobs.get(jobId);
  }

  async publishPending(jobId: string, indexes?: number[]): Promise<ArtifactDescriptor[]> {
    const pending = this.#pendingArtifacts.get(jobId) ?? await this.#readPendingArtifacts(jobId);
    if (!pending) throw new Error(`Job has no pending artifacts: ${jobId}`);
    let job = this.jobs.get(jobId);
    if (job.status === "interrupted") {
      job = await this.jobs.transition(jobId, "queued", { error: undefined });
      job = await this.jobs.transition(jobId, "running");
    }
    const project = this.projects.get(job.projectPath);
    const registered = this.operations.get(job.operationId, this.projects.extensionVersions(job.projectPath));
    const candidates = pending.result.artifacts ?? [];
    const selected = indexes ? indexes.map((index) => candidates[index]).filter(Boolean) : candidates;
    const artifacts = await this.ingestor.publish(project, pending.outboxPath, selected, {
      source: "operation", toolId: registered.definition.toolId, operationId: registered.definition.id, runId: pending.provenanceRunId ?? job.id,
      model: pending.model,
      inputs: pending.inputRefs
    }, { producedTypes: registered.definition.produces ?? [], allowedMediaTypesByType: registered.artifactMediaTypes, maxPayloadBytes: 256 * 1024 * 1024 });
    this.#pendingArtifacts.delete(jobId);
    await rm(this.#pendingArtifactsPath(jobId), { force: true });
    await removeInvocationDirectories(path.dirname(pending.outboxPath));
    await this.#recordResult(job, pending.result, artifacts);
    await this.jobs.transition(jobId, "succeeded", {
      artifactIds: artifacts.map((artifact) => artifact.id),
      result: sanitizeResult(pending.result, artifacts)
    });
    return artifacts;
  }

  async #run(jobId: string, input: InvokeOperationInput): Promise<void> {
    const job = this.jobs.get(jobId);
    const project = this.projects.get(input.projectPath);
    const extensionVersions = this.projects.extensionVersions(project.rootPath);
    const registered = this.operations.get(input.operationId, extensionVersions);
    const directories = await createInvocationDirectories(runtimeInvocationScopeRoot(this.runtimeRoot, registered.runtime, project.rootPath), jobId);
    try {
      await this.jobs.transition(jobId, "running");
      const inputRefs = await stageArtifacts(project.rootPath, input.artifactIds ?? [], directories.input);
      await stageModels(registered.runtime?.models ?? [], this.models, directories.models);
      const result = await this.supervisor.invoke(registered.runtime, {
        jobId, projectId: project.manifest.id, projectPath: project.rootPath, sessionId: input.sessionId,
        operation: registered.definition, arguments: input.arguments,
        inputPath: directories.input, workPath: directories.work, outboxPath: directories.outbox, modelsPath: directories.models,
        signal: this.jobs.signal(jobId),
        progress: (value, message) => { void this.jobs.progress(jobId, value, message); },
        builtin: this.operations.getBuiltinHandler(input.operationId),
        provenanceRunId: input.provenanceRunId,
        model: input.model,
        recipient: input.recipient
      });
      this.operations.validateOutput(input.operationId, result.structured ?? {}, extensionVersions);
      if ((result.artifacts?.length ?? 0) > 0 && !input.publishArtifacts) {
        const pendingArtifacts = { outboxPath: directories.outbox, result, inputRefs, provenanceRunId: input.provenanceRunId, model: input.model };
        this.#pendingArtifacts.set(jobId, pendingArtifacts);
        await writeJsonAtomic(this.#pendingArtifactsPath(jobId), pendingArtifacts);
        await this.jobs.progress(jobId, 1, "Waiting for artifact publication.");
        return;
      }
      const ingestedArtifacts = result.artifacts?.length ? await this.ingestor.publish(project, directories.outbox, result.artifacts, {
        source: "operation", toolId: registered.definition.toolId, operationId: registered.definition.id, runId: input.provenanceRunId ?? jobId,
        model: input.model,
        inputs: inputRefs.length ? inputRefs : undefined
      }, { producedTypes: registered.definition.produces ?? [], allowedMediaTypesByType: registered.artifactMediaTypes, maxPayloadBytes: 256 * 1024 * 1024 }) : [];
      const builtinArtifacts = registered.extensionId === "queryn.builtin"
        ? await Promise.all((result.publishedArtifactIds ?? []).map((artifactId) => readArtifact(project.rootPath, artifactId)))
        : [];
      if (builtinArtifacts.length) this.ingestor.notifyPublished(project, builtinArtifacts);
      const artifacts = [...ingestedArtifacts, ...builtinArtifacts];
      await this.#recordResult(job, result, artifacts);
      await this.jobs.transition(jobId, "succeeded", {
        artifactIds: artifacts.map((artifact) => artifact.id), result: sanitizeResult(result, artifacts), progress: 1
      });
      await removeInvocationDirectories(directories.root);
    } catch (error) {
      if (this.jobs.get(jobId).status !== "cancelled") {
        await this.jobs.transition(jobId, "failed", { error: error instanceof Error ? error.message : String(error) });
      }
      await removeInvocationDirectories(directories.root);
    }
  }

  async #recordResult(job: JobDescriptor, result: RuntimeInvocationResult, artifacts: ArtifactDescriptor[]): Promise<void> {
    if (!job.sessionId) return;
    await appendSessionEvent(job.projectPath, job.sessionId, {
      type: "operation-result",
      data: { jobId: job.id, operationId: job.operationId, message: result.message, structured: result.structured, artifactIds: artifacts.map((artifact) => artifact.id) }
    });
    for (const artifact of artifacts) await appendSessionEvent(job.projectPath, job.sessionId, {
      type: "artifact-linked", data: { jobId: job.id, artifactId: artifact.id }
    });
  }

  #evaluate(projectPath: string, extensionId: string, operation: ReturnType<OperationRegistry["get"]>["definition"], approval?: ApprovalDecision): PolicyEvaluation {
    if (extensionId === "queryn.builtin" || extensionId.startsWith("queryn.mcp.")) {
      const approvalRequired = ["network-egress", "external-side-effect", "privileged"].includes(operation.risk) && !approval?.approved;
      return { allowed: true, approvalRequired, missingPermissions: [] };
    }
    const evaluation = this.policy.evaluate(projectPath, extensionId, operation);
    return approval?.approved ? { ...evaluation, approvalRequired: false } : evaluation;
  }

  #pendingPath(jobId: string): string { return path.join(this.runtimeRoot, "pending-approvals", `${jobId}.json`); }
  async #readPending(jobId: string): Promise<PendingInvocation | undefined> {
    try { return JSON.parse(await readFile(this.#pendingPath(jobId), "utf8")) as PendingInvocation; }
    catch { return undefined; }
  }
  #pendingArtifactsPath(jobId: string): string { return path.join(this.runtimeRoot, "pending-artifacts", `${jobId}.json`); }
  async #readPendingArtifacts(jobId: string): Promise<{ outboxPath: string; result: RuntimeInvocationResult; inputRefs?: ArtifactRef[]; provenanceRunId?: string; model?: string } | undefined> {
    try { return JSON.parse(await readFile(this.#pendingArtifactsPath(jobId), "utf8")) as { outboxPath: string; result: RuntimeInvocationResult; inputRefs?: ArtifactRef[]; provenanceRunId?: string; model?: string }; }
    catch { return undefined; }
  }
}

export async function stageModels(dependencies: NonNullable<ReturnType<OperationRegistry["get"]>["runtime"]>["models"], models: ModelManager, modelsPath: string): Promise<void> {
  const mounted: Array<{ id: string; version: string; path: string; sha256: string }> = [];
  for (const dependency of dependencies ?? []) {
    const installed = await models.resolve(dependency.sha256);
    const name = `${dependency.id}-${dependency.version}`.replace(/[^a-zA-Z0-9._-]/g, "-");
    const target = path.join(modelsPath, name);
    // Never hard-link the immutable global cache into an extension-controlled
    // directory: a trusted native tool could otherwise mutate the cache bytes.
    await copyFile(installed.path, target);
    await chmod(target, 0o400);
    mounted.push({ id: dependency.id, version: dependency.version, path: name, sha256: dependency.sha256 });
  }
  const descriptorPath = path.join(modelsPath, "models.json");
  await writeFile(descriptorPath, `${JSON.stringify(mounted, null, 2)}\n`, { encoding: "utf8", mode: 0o400 });
}

export async function stageArtifacts(projectPath: string, artifactIds: string[], inputPath: string): Promise<ArtifactRef[]> {
  const manifest: Array<{ artifactId: string; type: string; title?: string; payloads: Array<{ path: string; mediaType: string; role?: string }> }> = [];
  const refs: ArtifactRef[] = [];
  for (const artifactId of artifactIds) {
    const artifact = await readArtifact(projectPath, artifactId);
    if (!/^[a-zA-Z0-9][a-zA-Z0-9._-]*$/.test(artifact.id)) throw new Error(`Unsafe artifact id in descriptor: ${artifact.id}`);
    const directory = path.join(inputPath, artifact.id);
    await mkdir(directory, { recursive: true });
    const payloads = [];
    const fingerprints: Array<{ path: string; sha256: string }> = [];
    for (const [index, payload] of artifact.payloads.entries()) {
      const source = await resolveSafeExistingFile(projectPath, payload.path, "Artifact payload");
      const name = `${index}-${path.basename(payload.path)}`;
      const target = path.join(directory, name);
      await copyFile(source, target);
      await chmod(target, 0o400);
      fingerprints.push({ path: payload.path, sha256: await hashFile(target) });
      payloads.push({ path: `${artifact.id}/${name}`, mediaType: payload.mediaType, role: payload.role });
    }
    manifest.push({ artifactId: artifact.id, type: artifact.type, title: artifact.title, payloads });
    refs.push({ artifactId: artifact.id, payloads: fingerprints });
  }
  await writeFile(path.join(inputPath, "artifacts.json"), `${JSON.stringify(manifest, null, 2)}\n`, { encoding: "utf8", mode: 0o400 });
  return refs;
}

async function validateArtifactInputs(projectPath: string, artifactIds: string[], acceptedTypes: string[]): Promise<void> {
  if (artifactIds.length && !acceptedTypes.length) throw new Error("Operation does not accept artifact inputs.");
  for (const artifactId of artifactIds) {
    const artifact = await readArtifact(projectPath, artifactId);
    if (!acceptedTypes.includes(artifact.type)) throw new Error(`Operation does not accept artifact type: ${artifact.type}`);
  }
}

function sanitizeResult(result: RuntimeInvocationResult, artifacts: ArtifactDescriptor[]): Record<string, unknown> {
  return { structured: result.structured, message: result.message, artifacts: artifacts.map((artifact) => ({ id: artifact.id, type: artifact.type, title: artifact.title })) };
}

async function hashFile(filePath: string): Promise<string> {
  const hash = createHash("sha256");
  for await (const chunk of createReadStream(filePath)) hash.update(chunk as Buffer);
  return hash.digest("hex");
}
