import { EventEmitter } from "node:events";
import { randomUUID } from "node:crypto";
import { mkdir, readFile, readdir } from "node:fs/promises";
import path from "node:path";
import type { JobDescriptor, JobStatus } from "@queryn/types";
import { writeJsonAtomic } from "./atomic.js";

export interface CreateJobInput {
  projectPath: string;
  sessionId?: string;
  operationId: string;
  input: Record<string, unknown>;
  status?: JobStatus;
}

export class JobManager extends EventEmitter {
  readonly #jobs = new Map<string, JobDescriptor>();
  readonly #controllers = new Map<string, AbortController>();
  readonly #loadIssues: Array<{ file: string; error: string }> = [];

  constructor(readonly dataRoot: string) {
    super();
  }

  async initialize(): Promise<void> {
    await mkdir(this.#jobsPath(), { recursive: true });
    for (const entry of await readdir(this.#jobsPath(), { withFileTypes: true })) {
      if (!entry.isFile() || !entry.name.endsWith(".json")) continue;
      try {
        const job = JSON.parse(await readFile(path.join(this.#jobsPath(), entry.name), "utf8")) as JobDescriptor;
        if (job.status === "running" || job.status === "queued") {
          job.status = "interrupted";
          job.error = "Runtime stopped before the job completed.";
          job.updatedAt = new Date().toISOString();
          await this.#persist(job);
        }
        this.#jobs.set(job.id, job);
      } catch (error) {
        this.#loadIssues.push({ file: entry.name, error: error instanceof Error ? error.message : String(error) });
      }
    }
  }

  async create(input: CreateJobInput): Promise<JobDescriptor> {
    const now = new Date().toISOString();
    const job: JobDescriptor = {
      id: randomUUID(), projectPath: input.projectPath, sessionId: input.sessionId,
      operationId: input.operationId, status: input.status ?? "queued", createdAt: now, updatedAt: now, input: input.input
    };
    this.#jobs.set(job.id, job);
    this.#controllers.set(job.id, new AbortController());
    await this.#persist(job);
    this.#emit(job);
    return job;
  }

  get(jobId: string): JobDescriptor {
    const job = this.#jobs.get(jobId);
    if (!job) throw new Error(`Unknown job: ${jobId}`);
    return structuredClone(job);
  }

  list(projectPath?: string): JobDescriptor[] {
    return [...this.#jobs.values()]
      .filter((job) => !projectPath || job.projectPath === projectPath)
      .sort((left, right) => right.createdAt.localeCompare(left.createdAt))
      .map((job) => structuredClone(job));
  }

  loadIssues(): Array<{ file: string; error: string }> { return structuredClone(this.#loadIssues); }

  signal(jobId: string): AbortSignal {
    let controller = this.#controllers.get(jobId);
    if (!controller) {
      controller = new AbortController();
      this.#controllers.set(jobId, controller);
    }
    return controller.signal;
  }

  async transition(jobId: string, status: JobStatus, patch: Partial<JobDescriptor> = {}): Promise<JobDescriptor> {
    const current = this.#jobs.get(jobId);
    if (!current) throw new Error(`Unknown job: ${jobId}`);
    assertTransition(current.status, status);
    const job = { ...current, ...patch, id: current.id, status, updatedAt: new Date().toISOString() };
    this.#jobs.set(jobId, job);
    if (["succeeded", "failed", "cancelled", "interrupted"].includes(status)) this.#controllers.delete(jobId);
    await this.#persist(job);
    this.#emit(job);
    return structuredClone(job);
  }

  async progress(jobId: string, progress: number, statusMessage?: string): Promise<void> {
    const job = this.#jobs.get(jobId);
    if (!job) throw new Error(`Unknown job: ${jobId}`);
    job.progress = Math.max(0, Math.min(1, progress));
    job.statusMessage = statusMessage;
    job.updatedAt = new Date().toISOString();
    await this.#persist(job);
    this.#emit(job);
  }

  async cancel(jobId: string): Promise<JobDescriptor> {
    const job = this.get(jobId);
    if (["succeeded", "failed", "cancelled", "interrupted"].includes(job.status)) return job;
    this.#controllers.get(jobId)?.abort(new Error("Job cancelled."));
    return this.transition(jobId, "cancelled", { error: "Cancelled by user." });
  }

  async #persist(job: JobDescriptor): Promise<void> {
    await writeJsonAtomic(path.join(this.#jobsPath(), `${job.id}.json`), job);
  }

  #jobsPath(): string { return path.join(this.dataRoot, "jobs"); }
  #emit(job: JobDescriptor): void { this.emit("changed", structuredClone(job)); }
}

const allowedTransitions: Record<JobStatus, JobStatus[]> = {
  queued: ["waiting-approval", "running", "cancelled", "failed", "interrupted"],
  "waiting-approval": ["queued", "running", "cancelled", "failed", "interrupted"],
  running: ["succeeded", "failed", "cancelled", "interrupted"],
  succeeded: [], failed: [], cancelled: [], interrupted: ["queued"]
};

function assertTransition(from: JobStatus, to: JobStatus): void {
  if (from !== to && !allowedTransitions[from].includes(to)) throw new Error(`Invalid job transition: ${from} -> ${to}`);
}
