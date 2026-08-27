import { access, mkdir } from "node:fs/promises";
import { spawn } from "node:child_process";
import type { ExtensionManager } from "./extension-manager.js";
import type { ModelManager } from "./model-manager.js";
import type { JobManager } from "./job-manager.js";

export interface DiagnosticCheck { id: string; status: "ok" | "warning" | "error"; message: string; details?: unknown }

export class DiagnosticsService {
  constructor(readonly dataRoot: string, readonly extensions: ExtensionManager, readonly models: ModelManager, readonly jobs: JobManager) {}

  async doctor(projectPath?: string): Promise<{ healthy: boolean; checks: DiagnosticCheck[] }> {
    const checks: DiagnosticCheck[] = [];
    try { await mkdir(this.dataRoot, { recursive: true }); await access(this.dataRoot); checks.push({ id: "runtime.data", status: "ok", message: "Runtime data directory is writable." }); }
    catch (error) { checks.push({ id: "runtime.data", status: "error", message: error instanceof Error ? error.message : String(error) }); }
    checks.push({ id: "platform", status: ["darwin", "win32"].includes(process.platform) ? "ok" : "warning", message: `${process.platform}/${process.arch}, Node ${process.version}` });
    const docker = await commandAvailable(process.env.QUERYN_OCI_COMMAND ?? "docker", ["version", "--format", "{{.Server.Version}}"]);
    checks.push({ id: "runtime.oci", status: docker ? "ok" : "warning", message: docker ? `OCI runtime available (${docker}).` : "OCI runtime is unavailable; native and builtin tools still work." });
    const installedExtensions = await this.extensions.list();
    const integrityIssues = this.extensions.integrityIssues();
    checks.push({
      id: "extensions", status: integrityIssues.length ? "error" : "ok",
      message: integrityIssues.length ? `${integrityIssues.length} extension version(s) failed integrity verification.` : `${installedExtensions.length} extension version(s) installed.`,
      details: integrityIssues.length ? integrityIssues : undefined
    });
    checks.push({ id: "models", status: "ok", message: `${(await this.models.list()).length} model(s) installed.` });
    const jobIssues = this.jobs.loadIssues();
    checks.push({ id: "jobs.state", status: jobIssues.length ? "warning" : "ok", message: jobIssues.length ? `${jobIssues.length} corrupt derived job record(s) were ignored.` : "Derived job state is readable.", details: jobIssues.length ? jobIssues : undefined });
    if (projectPath) {
      try {
        const { missing } = await this.extensions.reconcileProject(projectPath);
        checks.push({ id: "project.extensions", status: missing.length ? "warning" : "ok", message: missing.length ? `Missing extensions: ${missing.map((item) => `${item.id}@${item.version}`).join(", ")}` : "Project extensions are available." });
      } catch (error) { checks.push({ id: "project", status: "error", message: error instanceof Error ? error.message : String(error) }); }
    }
    return { healthy: checks.every((check) => check.status !== "error"), checks };
  }
}

function commandAvailable(command: string, args: string[]): Promise<string | undefined> {
  return new Promise((resolve) => {
    const child = spawn(command, args, { stdio: ["ignore", "pipe", "ignore"], windowsHide: true });
    const chunks: Buffer[] = [];
    child.stdout?.on("data", (chunk: Buffer) => chunks.push(chunk));
    child.once("error", () => resolve(undefined));
    child.once("exit", (code) => resolve(code === 0 ? Buffer.concat(chunks).toString("utf8").trim() : undefined));
  });
}
