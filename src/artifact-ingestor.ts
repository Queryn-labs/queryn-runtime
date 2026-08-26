import type { ArtifactDescriptor, ArtifactProvenance, QuerynProject } from "@queryn/types";
import { publishArtifact } from "@queryn/project";
import { rm } from "node:fs/promises";
import path from "node:path";
import { EventEmitter } from "node:events";

export interface ArtifactCandidate {
  id?: string;
  type: string;
  title?: string;
  payloads: Array<{ path: string; mediaType?: string; role?: string }>;
  context?: { mode: "none" | "automatic" | "declarative" | "custom"; providerId?: string; fields?: string[]; template?: string };
  tags?: string[];
  metadata?: Record<string, unknown>;
}

export interface IngestPolicy {
  producedTypes: string[];
  allowedMediaTypes?: string[];
  allowedMediaTypesByType?: Record<string, string[]>;
  maxPayloadBytes?: number;
  maxArtifacts?: number;
}

export class ArtifactIngestor extends EventEmitter {
  async publish(
    project: QuerynProject,
    outboxPath: string,
    candidates: ArtifactCandidate[],
    provenance: ArtifactProvenance,
    policy: IngestPolicy
  ): Promise<ArtifactDescriptor[]> {
    const limit = policy.maxArtifacts ?? 32;
    if (candidates.length > limit) throw new Error(`Operation produced ${candidates.length} artifacts; limit is ${limit}.`);
    const descriptors: ArtifactDescriptor[] = [];
    try {
      for (const candidate of candidates) {
        if (!policy.producedTypes.includes(candidate.type)) throw new Error(`Undeclared artifact type: ${candidate.type}`);
        const allowedMediaTypes = policy.allowedMediaTypes?.length ? policy.allowedMediaTypes : policy.allowedMediaTypesByType?.[candidate.type];
        for (const payload of candidate.payloads) {
          if (payload.mediaType && allowedMediaTypes?.length && !allowedMediaTypes.includes(payload.mediaType)) {
            throw new Error(`Undeclared media type: ${payload.mediaType}`);
          }
        }
        descriptors.push(await publishArtifact(project, {
          ...candidate,
          outboxPath,
          provenance,
          maxPayloadBytes: policy.maxPayloadBytes,
          allowedMediaTypes
        }));
      }
      this.notifyPublished(project, descriptors);
      return descriptors;
    } catch (error) {
      await Promise.all(descriptors.flatMap((descriptor) => [
        rm(path.join(project.rootPath, "artifacts", `${descriptor.id}.json`), { force: true }),
        rm(path.join(project.rootPath, "artifacts", "data", descriptor.id), { recursive: true, force: true })
      ])).catch(() => undefined);
      throw error;
    }
  }

  notifyPublished(project: QuerynProject, descriptors: ArtifactDescriptor[]): void {
    if (descriptors.length) this.emit("published", { projectPath: project.rootPath, artifacts: structuredClone(descriptors) });
  }
}
