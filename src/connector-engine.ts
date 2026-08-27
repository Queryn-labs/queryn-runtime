import { randomUUID } from "node:crypto";
import { mkdir, rm, writeFile } from "node:fs/promises";
import path from "node:path";
import type { ApprovalDecision, ArtifactDescriptor, OperationRisk, QuerynProject, Permission } from "@queryn/types";
import type { ArtifactCandidate, ArtifactIngestor } from "./artifact-ingestor.js";
import { assertSafeRelativePath, writeJsonAtomic } from "./atomic.js";
import type { PolicyEngine } from "./policy-engine.js";

export interface ConnectorItem { cursor: string; candidate: ArtifactCandidate; outboxPath?: string }
export interface ConnectorHandler {
  pull(input: { project: QuerynProject; checkpoint?: string; signal: AbortSignal }): AsyncIterable<ConnectorItem>;
}

export interface ConnectorRegistration {
  id: string;
  extensionId: string;
  permissions: Permission[];
  risk: OperationRisk;
  scope: "project" | "external-explicit";
  produces: string[];
  allowedMediaTypesByType?: Record<string, string[]>;
  handler: ConnectorHandler;
}

export class ConnectorEngine {
  readonly #connectors = new Map<string, ConnectorRegistration>();
  constructor(readonly ingestor: ArtifactIngestor, readonly policy: PolicyEngine) {}
  register(connectorId: string, handler: ConnectorHandler, options: Omit<ConnectorRegistration, "id" | "handler"> = { extensionId: "queryn.builtin", permissions: [], risk: "safe-read", scope: "project", produces: [] }): void {
    this.#connectors.set(connectorId, { id: connectorId, handler, ...options });
  }
  list(): Array<Omit<ConnectorRegistration, "handler">> { return [...this.#connectors.values()].map(({ handler: _handler, ...connector }) => connector); }
  get(connectorId: string): Omit<ConnectorRegistration, "handler"> {
    const connector = this.#connectors.get(connectorId);
    if (!connector) throw new Error(`Unknown connector: ${connectorId}`);
    const { handler: _handler, ...definition } = connector;
    return definition;
  }

  async sync(project: QuerynProject, connectorId: string, options: { signal?: AbortSignal; producedTypes: string[]; approval?: ApprovalDecision }): Promise<ArtifactDescriptor[]> {
    const registration = this.#connectors.get(connectorId);
    if (!registration) throw new Error(`Unknown connector: ${connectorId}`);
    if (registration.extensionId !== "queryn.builtin") {
      const connected = project.manifest.extensions?.some((extension) => extension.id === registration.extensionId && extension.enabled !== false);
      if (!connected) throw new Error(`Connector extension is not connected to this project: ${registration.extensionId}`);
      const evaluation = this.policy.evaluate(project.rootPath, registration.extensionId, {
        id: connectorId, toolId: connectorId, version: "1", title: connectorId, inputSchema: {}, outputSchema: {},
        risk: registration.risk, agentVisibility: "hidden", execution: "job",
        permissions: registration.permissions, idempotent: true, cancellable: true
      });
      if (!evaluation.allowed) throw new Error(`${evaluation.reason} ${evaluation.missingPermissions.join(", ")}`);
      if (evaluation.approvalRequired && !options.approval?.approved) throw new Error("Connector sync requires explicit approval.");
      if (options.approval) await this.policy.rememberApproval(project.rootPath, connectorId, options.approval);
    }
    const handler = registration.handler;
    const checkpointPath = path.join(project.rootPath, ".queryn", "connectors", `${connectorId}.json`);
    let checkpoint: string | undefined;
    try { checkpoint = (JSON.parse(await (await import("node:fs/promises")).readFile(checkpointPath, "utf8")) as { cursor: string }).cursor; } catch {}
    const signal = options.signal ?? new AbortController().signal;
    const published: ArtifactDescriptor[] = [];
    for await (const item of handler.pull({ project, checkpoint, signal })) {
      if (signal.aborted) throw signal.reason;
      const ownedOutbox = !item.outboxPath;
      const outbox = item.outboxPath ?? path.join(project.rootPath, ".queryn", "connector-outbox", `${connectorId}-${randomUUID()}`);
      if (ownedOutbox) await mkdir(outbox, { recursive: true });
      try {
        const source = item.candidate.metadata?.inlineText;
        if (typeof source === "string" && item.candidate.payloads.length === 1) {
          const outputPath = path.join(outbox, ...assertSafeRelativePath(item.candidate.payloads[0].path).split("/"));
          await mkdir(path.dirname(outputPath), { recursive: true });
          await writeFile(outputPath, source, "utf8");
        }
        const descriptors = await this.ingestor.publish(project, outbox, [item.candidate], { source: "import" }, {
          producedTypes: options.producedTypes,
          allowedMediaTypesByType: registration.allowedMediaTypesByType,
          maxPayloadBytes: 256 * 1024 * 1024
        });
        published.push(...descriptors);
        await writeJsonAtomic(checkpointPath, { cursor: item.cursor, updatedAt: new Date().toISOString() });
      } finally { if (ownedOutbox) await rm(outbox, { recursive: true, force: true }); }
    }
    return published;
  }
}
