import { mkdir, open, readFile, rm, stat } from "node:fs/promises";
import path from "node:path";
import { listArtifacts, listAssets, listNotes, readArtifact, readNote } from "@queryn/project";
import type { ApprovalDecision, ArtifactDescriptor, ContextEnvelope, ContextLevel } from "@queryn/types";
import { writeJsonAtomic } from "./atomic.js";
import { resolveSafeExistingFile } from "./atomic.js";

const MAX_VISIBLE_NOTES = 60;
const MAX_VISIBLE_ASSETS = 40;
const MAX_VISIBLE_ARTIFACTS = 30;

// The research envelope limits results so the expanded context stays compact:
// at most this many ranked notes and requested asset files per envelope.
const MAX_RESEARCH_NOTES = 12;
const MAX_RESEARCH_ASSETS = 8;

// These catalog caps match the compact-envelope budget in the context policy.
// see queryn-docs/docs/adr/adr-0009-context-policy.md

export interface ContextRequest {
  projectPath: string;
  artifactIds?: string[];
  level: ContextLevel;
  budgetTokens: number;
  recipient: "local" | "cloud";
  approval?: ApprovalDecision;
}

export interface ContextResearchRequest {
  projectPath: string;
  query: string;
  queries?: string[];
  projectRelativePaths?: string[];
  artifactIds?: string[];
  budgetTokens: number;
  recipient: "local" | "cloud";
  approval?: ApprovalDecision;
}

export type CustomContextProvider = (input: {
  projectPath: string; artifact: ArtifactDescriptor; level: ContextLevel; budgetTokens: number; recipient: "local" | "cloud"; approval?: ApprovalDecision;
}) => Promise<ContextEnvelope>;

export class ContextBroker {
  readonly #providers = new Map<string, CustomContextProvider>();

  register(providerId: string, provider: CustomContextProvider): void { this.#providers.set(providerId, provider); }

  async resolveOne(request: { projectPath: string; artifactId: string; level: ContextLevel; budgetTokens: number; recipient: "local" | "cloud"; approval?: ApprovalDecision }): Promise<ContextEnvelope> {
    const artifact = await readArtifact(request.projectPath, request.artifactId);
    return this.#resolveArtifact(request.projectPath, artifact, request.level, request.budgetTokens, request.recipient, request.approval);
  }

  /** Strongest context policy (mode + sensitivity) that covers a project material path. */
  async describeMaterialPolicy(projectPath: string, relativePath: string): Promise<{ mode: "none" | "automatic" | "declarative" | "custom"; sensitive: boolean }> {
    const artifacts = await listArtifacts(projectPath);
    let best: ArtifactDescriptor | undefined;
    for (const artifact of artifacts) {
      if (!artifact.payloads.some((payload) => payload.path === relativePath)) continue;
      if (!best || contextRestriction(artifact) > contextRestriction(best)) best = artifact;
    }
    return {
      mode: best?.context?.mode ?? "automatic",
      sensitive: best?.metadata?.sensitivity === "sensitive"
    };
  }

  async preview(projectPath: string, budgetTokens = 1_000): Promise<ContextEnvelope> {
    const [artifacts, notes, allAssets] = await Promise.all([listArtifacts(projectPath), listNotes(projectPath), listAssets(projectPath)]);
    const assets = allAssets.filter((asset) => isContextAsset(asset.relativePath));
    const sensitive = artifacts.some((artifact) => artifact.metadata?.sensitivity === "sensitive");
    const materialPaths = new Set([...notes.map((note) => note.relativePath), ...assets.map((asset) => asset.relativePath)]);
    const visibleArtifacts = artifacts
      .filter((artifact) => !artifact.payloads.length || artifact.payloads.some((payload) => !materialPaths.has(payload.path)))
      .slice(0, MAX_VISIBLE_ARTIFACTS);
    const visibleNotes = notes.slice(0, MAX_VISIBLE_NOTES);
    const visibleAssets = assets.slice(0, MAX_VISIBLE_ASSETS);
    const lines = [
      `Project context catalog: ${notes.length} notes, ${assets.length} files, ${artifacts.length} registered artifacts.`,
      ...visibleNotes.map((note) => `- note: ${note.title} (${note.relativePath})`),
      ...visibleAssets.map((asset) => `- file: ${asset.name} (${asset.relativePath}, ${asset.mediaType ?? "unknown"})`),
      ...visibleArtifacts.map((artifact) => `- ${artifact.type}: ${artifact.title ?? artifact.id} [${artifact.id}]`)
    ];
    const text = lines.join("\n");
    const catalog = envelope(
      "compact", truncate(text, Math.max(0, budgetTokens)),
      [
        ...visibleNotes.map((note) => ({
          projectRelativePath: note.relativePath,
          title: note.title,
          kind: "note" as const,
          providerId: "host.note-catalog"
        })),
        ...visibleAssets.map((asset) => ({
          projectRelativePath: asset.relativePath,
          title: asset.name,
          kind: "asset" as const,
          providerId: "host.asset-catalog"
        })),
        ...visibleArtifacts.map((artifact) => ({
          artifactId: artifact.id,
          title: artifact.title ?? artifact.id,
          kind: "artifact" as const,
          providerId: "host.catalog"
        }))
      ],
      sensitive ? "sensitive" : "project", sensitive ? ["local"] : ["local", "cloud"], "host.catalog/1"
    );
    return {
      ...catalog,
      structured: {
        availableSourceCount: notes.length + assets.length + artifacts.length,
        catalogedSourceCount: catalog.sources.length,
        notes: notes.length,
        assets: assets.length,
        artifacts: artifacts.length
      }
    };
  }

  async research(request: ContextResearchRequest): Promise<ContextEnvelope> {
    const [artifacts, notes, allAssets] = await Promise.all([
      listArtifacts(request.projectPath),
      listNotes(request.projectPath),
      listAssets(request.projectPath)
    ]);
    const assets = allAssets.filter((asset) => isContextAsset(asset.relativePath));
    const artifactsById = new Map(artifacts.map((artifact) => [artifact.id, artifact]));
    const notesByPath = new Map(notes.map((note) => [note.relativePath, note]));
    const assetsByPath = new Map(assets.map((asset) => [asset.relativePath, asset]));
    const policyByPath = new Map<string, ArtifactDescriptor>();
    for (const artifact of artifacts) {
      for (const payload of artifact.payloads) {
        const current = policyByPath.get(payload.path);
        if (!current || contextRestriction(artifact) > contextRestriction(current)) policyByPath.set(payload.path, artifact);
      }
    }

    const requestedPaths = new Set(
      (request.projectRelativePaths ?? []).filter((relativePath) => notesByPath.has(relativePath))
    );
    const queryTerms = tokenize([request.query, ...(request.queries ?? [])].join(" "));
    const candidates = await Promise.all(notes.map(async (note) => {
      const document = await readNote(request.projectPath, note.relativePath);
      const policy = policyByPath.get(note.relativePath);
      const mode = policy?.context?.mode ?? "automatic";
      const searchableBody = mode === "none" || mode === "custom" ? "" : document.body;
      const score = requestedPaths.has(note.relativePath)
        ? 10_000
        : scoreDocument(`${note.title}\n${note.relativePath}`, searchableBody, queryTerms);
      return { note, document, policy, mode, score };
    }));
    const rankedNotes = candidates
      .filter((candidate) => candidate.score > 0 && candidate.mode !== "none" && candidate.mode !== "custom")
      .sort((left, right) => right.score - left.score || (right.note.updatedAt ?? "").localeCompare(left.note.updatedAt ?? ""))
      .slice(0, MAX_RESEARCH_NOTES);
    const requestedAssetPaths = (request.projectRelativePaths ?? []).filter((relativePath) => assetsByPath.has(relativePath));

    let remaining = Math.max(0, request.budgetTokens);
    const parts: string[] = [];
    const sources: ContextEnvelope["sources"] = [];
    let truncated = false;
    let sensitivity: ContextEnvelope["sensitivity"] = "project";
    let recipients: ContextEnvelope["allowedRecipients"] = ["local", "cloud"];

    for (const candidate of rankedNotes) {
      if (remaining <= 0) { truncated = true; break; }
      const policy = candidate.policy;
      const noteSensitivity = policy?.metadata?.sensitivity === "sensitive" ? "sensitive" : "project";
      const noteRecipients: ContextEnvelope["allowedRecipients"] = noteSensitivity === "sensitive" ? ["local"] : ["local", "cloud"];
      if (!noteRecipients.includes(request.recipient)) {
        throw new Error(`Material ${candidate.note.relativePath} cannot be sent to ${request.recipient}.`);
      }
      const declarative = candidate.mode === "declarative"
        ? JSON.stringify(policy?.metadata ?? {}, null, 2)
        : candidate.document.body;
      const limited = truncate(`Source: ${candidate.note.title} (${candidate.note.relativePath})\n${declarative}`, remaining);
      parts.push(limited.text);
      sources.push({
        projectRelativePath: candidate.note.relativePath,
        title: candidate.note.title,
        kind: "note",
        providerId: candidate.mode === "declarative" ? "host.declarative" : "host.automatic"
      });
      remaining -= estimateTokens(limited.text);
      truncated ||= limited.truncated;
      if (noteSensitivity === "sensitive") sensitivity = "sensitive";
      recipients = recipients.filter((recipient) => noteRecipients.includes(recipient));
    }

    for (const relativePath of requestedAssetPaths.slice(0, MAX_RESEARCH_ASSETS)) {
      if (remaining <= 0) { truncated = true; break; }
      const asset = assetsByPath.get(relativePath);
      if (!asset) continue;
      const policy = policyByPath.get(relativePath);
      const mode = policy?.context?.mode ?? "automatic";
      if (mode === "none" || mode === "custom") continue;
      const assetSensitivity = policy?.metadata?.sensitivity === "sensitive" ? "sensitive" : "project";
      const assetRecipients: ContextEnvelope["allowedRecipients"] = assetSensitivity === "sensitive" ? ["local"] : ["local", "cloud"];
      if (!assetRecipients.includes(request.recipient)) throw new Error(`Material ${relativePath} cannot be sent to ${request.recipient}.`);
      const header = `Source: ${asset.name} (${relativePath}, ${asset.mediaType ?? "unknown"}, ${asset.size} bytes)`;
      const body = mode === "declarative"
        ? JSON.stringify(policy?.metadata ?? {}, null, 2)
        : isTextAsset(asset.mediaType, relativePath)
          ? await readTextPrefix(await resolveSafeExistingFile(request.projectPath, relativePath, "Context asset"), remaining * 4 + 1)
          : "Binary content is not exposed by the standard context provider.";
      const limited = truncate(`${header}\n${body}`, remaining);
      parts.push(limited.text);
      sources.push({
        projectRelativePath: relativePath,
        title: asset.name,
        kind: "asset",
        providerId: mode === "declarative" ? "host.declarative" : "host.automatic"
      });
      remaining -= estimateTokens(limited.text);
      truncated ||= limited.truncated;
      if (assetSensitivity === "sensitive") sensitivity = "sensitive";
      recipients = recipients.filter((recipient) => assetRecipients.includes(recipient));
    }

    for (const artifactId of request.artifactIds ?? []) {
      if (remaining <= 0) { truncated = true; break; }
      const artifact = artifactsById.get(artifactId);
      if (!artifact) continue;
      const resolved = await this.#resolveArtifact(
        request.projectPath,
        artifact,
        "expanded",
        remaining,
        request.recipient,
        request.approval
      );
      if (resolved.text) parts.push(resolved.text);
      sources.push(...resolved.sources);
      remaining -= resolved.tokenEstimate;
      truncated ||= resolved.truncated;
      if (resolved.sensitivity === "sensitive") sensitivity = "sensitive";
      recipients = recipients.filter((recipient) => resolved.allowedRecipients.includes(recipient));
    }

    const text = parts.join("\n\n");
    return {
      level: "expanded",
      text,
      sources,
      sensitivity,
      allowedRecipients: recipients,
      tokenEstimate: estimateTokens(text),
      truncated,
      freshness: new Date().toISOString(),
      providerVersion: "host.research/1"
    };
  }

  async resolve(request: ContextRequest): Promise<ContextEnvelope> {
    const artifactIds = request.artifactIds ?? (await listArtifacts(request.projectPath)).map((artifact) => artifact.id);
    let remaining = Math.max(0, request.budgetTokens);
    const parts: string[] = [];
    const sources: ContextEnvelope["sources"] = [];
    let truncated = false;
    let sensitivity: ContextEnvelope["sensitivity"] = "public";
    let recipients: ContextEnvelope["allowedRecipients"] = ["local", "cloud"];
    for (const artifactId of artifactIds) {
      if (remaining <= 0) { truncated = true; break; }
      const artifact = await readArtifact(request.projectPath, artifactId);
      const resolved = await this.#resolveArtifact(request.projectPath, artifact, request.level, remaining, request.recipient, request.approval);
      if (!resolved.allowedRecipients.includes(request.recipient)) throw new Error(`Artifact ${artifact.id} cannot be sent to ${request.recipient}.`);
      if (resolved.text) parts.push(resolved.text);
      sources.push(...resolved.sources);
      remaining -= resolved.tokenEstimate;
      truncated ||= resolved.truncated;
      if (resolved.sensitivity === "sensitive") sensitivity = "sensitive";
      else if (resolved.sensitivity === "project" && sensitivity === "public") sensitivity = "project";
      recipients = recipients.filter((recipient) => resolved.allowedRecipients.includes(recipient));
    }
    const text = parts.join("\n\n");
    const limited = truncate(text, request.budgetTokens);
    return { level: request.level, text: limited.text, sources, sensitivity, allowedRecipients: recipients,
      tokenEstimate: estimateTokens(limited.text), truncated: truncated || limited.truncated,
      freshness: new Date().toISOString(), providerVersion: "host.broker/1" };
  }

  async #resolveArtifact(projectPath: string, artifact: ArtifactDescriptor, level: ContextLevel, budgetTokens: number, recipient: "local" | "cloud", approval?: ApprovalDecision): Promise<ContextEnvelope> {
    const policy = artifact.context ?? { mode: "automatic" as const };
    const sensitivity = artifact.metadata?.sensitivity === "sensitive" ? "sensitive" : "project";
    const recipients: ContextEnvelope["allowedRecipients"] = sensitivity === "sensitive" ? ["local"] : ["local", "cloud"];
    const metadata = `${artifact.title ?? artifact.id} (${artifact.type}); payloads: ${artifact.payloads.map((payload) => `${payload.mediaType}, ${payload.size} bytes`).join("; ")}`;
    if (policy.mode === "none" || level === "compact") {
      return envelope(level, truncate(metadata, budgetTokens), [{ artifactId: artifact.id }], sensitivity, recipients, "host.metadata/1");
    }
    if (policy.mode === "custom") {
      const provider = policy.providerId ? this.#providers.get(policy.providerId) : undefined;
      if (!provider) return envelope(level, truncate(`${metadata}\nPayload unavailable: context provider is missing.`, budgetTokens), [{ artifactId: artifact.id }], sensitivity, recipients, "host.missing-provider/1");
      return normalizeCustomEnvelope(await provider({ projectPath, artifact, level, budgetTokens, recipient, approval }), artifact, budgetTokens, sensitivity, recipients);
    }
    if (policy.mode === "declarative") {
      const selected = Object.fromEntries((policy.fields ?? Object.keys(artifact.metadata ?? {})).map((field) => [field, readField(artifact.metadata ?? {}, field)]));
      const rendered = policy.template
        ? policy.template.replace(/\{\{\s*([a-zA-Z0-9_.-]+)\s*\}\}/g, (_match, field: string) => String(readField(artifact.metadata ?? {}, field) ?? ""))
        : JSON.stringify(selected, null, 2);
      const text = `${metadata}\n${rendered}`;
      return envelope(level, truncate(text, budgetTokens), [{ artifactId: artifact.id }], sensitivity, recipients, "host.declarative/1");
    }
    const extracted: string[] = [metadata];
    for (const payload of artifact.payloads) {
      const filePath = await resolveSafeExistingFile(projectPath, payload.path, "Context payload");
      if (isTextMedia(payload.mediaType)) extracted.push(await readTextPrefix(filePath, budgetTokens * 4 + 1));
      else {
        const media = await safeMediaMetadata(filePath, payload.mediaType);
        if (media) extracted.push(media);
      }
    }
    if (extracted.length === 1) extracted.push("Payload content is not exposed for this media type; only safe metadata is available.");
    return envelope(level, truncate(extracted.join("\n\n"), budgetTokens), artifact.payloads.map((payload) => ({ artifactId: artifact.id, payloadPath: payload.path, providerId: "host.automatic" })), sensitivity, recipients, "host.automatic/1");
  }
}

export class ProjectIndexer {
  readonly #preferSqlite: boolean;
  readonly #rebuildTimers = new Map<string, NodeJS.Timeout>();

  constructor(options: { preferSqlite?: boolean } = {}) {
    this.#preferSqlite = options.preferSqlite ?? true;
  }

  /** Rebuild the index shortly after writes coalesce; safe to call per write. */
  scheduleRebuild(projectPath: string, delayMs = 1_500): void {
    const existing = this.#rebuildTimers.get(projectPath);
    if (existing) clearTimeout(existing);
    const timer = setTimeout(() => {
      this.#rebuildTimers.delete(projectPath);
      void this.rebuild(projectPath).catch(() => undefined);
    }, delayMs);
    timer.unref?.();
    this.#rebuildTimers.set(projectPath, timer);
  }

  /** Rebuilds the index when any note is newer than the stored index. */
  async ensureFresh(projectPath: string): Promise<"fresh" | "rebuilt"> {
    let indexMtime = 0;
    for (const candidate of ["context.sqlite", "context.json"]) {
      try { indexMtime = Math.max(indexMtime, (await stat(path.join(projectPath, ".queryn", "index", candidate))).mtimeMs); }
      catch { /* A missing index means it must be built. */ }
    }
    const cutoff = indexMtime + 5;
    const notes = await listNotes(projectPath);
    let checked = 0;
    for (const note of notes) {
      if (checked++ > 400) return "fresh";
      let mtime = 0;
      try { mtime = (await stat(path.join(projectPath, note.relativePath))).mtimeMs; }
      catch { continue; }
      if (mtime > cutoff) { await this.rebuild(projectPath); return "rebuilt"; }
    }
    return "fresh";
  }

  async rebuild(projectPath: string): Promise<{ indexed: number; engine: "sqlite-fts5" | "portable" }> {
    const documents = await collectIndexDocuments(projectPath);
    const indexPath = path.join(projectPath, ".queryn", "index", "context.sqlite");
    const portablePath = path.join(projectPath, ".queryn", "index", "context.json");
    if (this.#preferSqlite) {
      try {
        const { DatabaseSync } = await import("node:sqlite");
        await mkdir(path.dirname(indexPath), { recursive: true });
        const db = new DatabaseSync(indexPath);
        try {
          db.exec("PRAGMA journal_mode=WAL; CREATE VIRTUAL TABLE IF NOT EXISTS documents USING fts5(id UNINDEXED, kind UNINDEXED, title, body); DELETE FROM documents;");
          const insert = db.prepare("INSERT INTO documents (id, kind, title, body) VALUES (?, ?, ?, ?)");
          for (const document of documents) insert.run(document.id, document.kind, document.title, document.body);
        } finally { db.close(); }
        await rm(portablePath, { force: true });
        return { indexed: documents.length, engine: "sqlite-fts5" };
      } catch {
        // Electron releases can embed a Node version without node:sqlite. The
        // portable index preserves local search instead of blocking the project.
      }
    }
    await writeJsonAtomic(portablePath, { version: 1, documents });
    return { indexed: documents.length, engine: "portable" };
  }

  async search(projectPath: string, query: string, limit = 20): Promise<Array<{ id: string; kind: string; title: string; snippet: string }>> {
    await this.ensureFresh(projectPath).catch(() => undefined);
    if (this.#preferSqlite) {
      try {
        const indexPath = path.join(projectPath, ".queryn", "index", "context.sqlite");
        await stat(indexPath);
        const { DatabaseSync } = await import("node:sqlite");
        const db = new DatabaseSync(indexPath, { readOnly: true });
        try {
          return db.prepare("SELECT id, kind, title, snippet(documents, 3, '[', ']', '…', 20) AS snippet FROM documents WHERE documents MATCH ? LIMIT ?")
            .all(query, limit) as Array<{ id: string; kind: string; title: string; snippet: string }>;
        } finally { db.close(); }
      } catch {
        // Fall through to the index built for runtimes without SQLite FTS5.
      }
    }
    try {
      const portablePath = path.join(projectPath, ".queryn", "index", "context.json");
      const parsed = JSON.parse(await readFile(portablePath, "utf8")) as { version?: unknown; documents?: unknown };
      if (parsed.version !== 1 || !Array.isArray(parsed.documents)) throw new Error("Portable project index is invalid; rebuild it.");
      return searchPortableIndex(parsed.documents as IndexDocument[], query, limit);
    } catch {
      // A project without any built index simply has no searchable material yet.
      return [];
    }
  }
}

interface IndexDocument { id: string; kind: "note" | "artifact"; title: string; body: string }

async function collectIndexDocuments(projectPath: string): Promise<IndexDocument[]> {
  const documents: IndexDocument[] = [];
  const artifacts = await listArtifacts(projectPath);
  const pathPolicies = new Map<string, ArtifactDescriptor>();
  for (const artifact of artifacts) {
    for (const payload of artifact.payloads) {
      const existing = pathPolicies.get(payload.path);
      if (!existing || contextRestriction(artifact) > contextRestriction(existing)) pathPolicies.set(payload.path, artifact);
    }
  }
  for (const note of await listNotes(projectPath)) {
    const document = await readNote(projectPath, note.relativePath);
    const policyArtifact = pathPolicies.get(note.relativePath);
    const mode = policyArtifact?.context?.mode ?? "automatic";
    const body = mode === "none" || mode === "custom" ? "" : mode === "declarative" ? JSON.stringify(policyArtifact?.metadata ?? {}) : document.body;
    documents.push({ id: note.id, kind: "note", title: note.title, body });
  }
  for (const artifact of artifacts) {
    const mode = artifact.context?.mode ?? "automatic";
    const parts = [JSON.stringify(artifact.metadata ?? {})];
    if (mode === "automatic") {
      for (const payload of artifact.payloads.filter((candidate) => isTextMedia(candidate.mediaType))) {
        try { parts.push(await readTextPrefix(await resolveSafeExistingFile(projectPath, payload.path, "Index payload"), 1024 * 1024)); }
        catch { /* A corrupt derived descriptor is skipped without reading outside the project. */ }
      }
    }
    documents.push({ id: artifact.id, kind: "artifact", title: artifact.title ?? artifact.id, body: mode === "none" || mode === "custom" ? "" : parts.join("\n") });
  }
  return documents;
}

function contextRestriction(artifact: ArtifactDescriptor): number {
  return ({ automatic: 0, declarative: 1, custom: 2, none: 3 } as const)[artifact.context?.mode ?? "automatic"];
}

function searchPortableIndex(documents: IndexDocument[], query: string, limit: number): Array<{ id: string; kind: string; title: string; snippet: string }> {
  const terms = query.toLocaleLowerCase().split(/\s+/u).filter(Boolean);
  if (!terms.length) return [];
  const ranked: Array<{ document: IndexDocument; score: number; snippet: string }> = [];
  for (const document of documents) {
    const title = document.title.toLocaleLowerCase();
    const haystack = `${document.title}\n${document.body}`.toLocaleLowerCase();
    let score = 0;
    let first = -1;
    for (const term of terms) {
      const position = matchTerm(haystack, term);
      if (position < 0) continue;
      const exact = haystack.includes(term);
      score += (title.includes(term) ? 2 : exact ? 1 : 0.6) * (exact ? 1 : 0.7);
      if (first === -1) first = position;
    }
    if (score <= 0) continue;
    const snippetStart = Math.max(0, first - 50);
    ranked.push({ document, score, snippet: document.body.slice(snippetStart, snippetStart + 240) });
  }
  return ranked
    .sort((left, right) => right.score - left.score || left.document.title.localeCompare(right.document.title))
    .slice(0, Math.max(0, limit))
    .map(({ document, snippet }) => ({ id: document.id, kind: document.kind, title: document.title, snippet }));
}

/** Exact substring first, then a stem match tolerant to Russian-style inflections. */
function matchTerm(haystack: string, term: string): number {
  const exact = haystack.indexOf(term);
  if (exact >= 0) return exact;
  for (let trim = 1; trim <= 3; trim += 1) {
    if (term.length - trim < 4) break;
    const stem = term.slice(0, term.length - trim);
    const position = haystack.indexOf(stem);
    if (position >= 0) return position;
  }
  return -1;
}

function envelope(level: ContextLevel, limited: { text: string; truncated: boolean }, sources: ContextEnvelope["sources"], sensitivity: ContextEnvelope["sensitivity"], allowedRecipients: ContextEnvelope["allowedRecipients"], providerVersion: string): ContextEnvelope {
  return { level, text: limited.text, sources, sensitivity, allowedRecipients, tokenEstimate: estimateTokens(limited.text), truncated: limited.truncated, freshness: new Date().toISOString(), providerVersion };
}

function truncate(text: string, tokens: number): { text: string; truncated: boolean } {
  const maxCharacters = Math.max(0, tokens * 4);
  return text.length <= maxCharacters ? { text, truncated: false } : { text: `${text.slice(0, Math.max(0, maxCharacters - 1))}…`, truncated: true };
}

function estimateTokens(text: string): number { return Math.ceil(text.length / 4); }
function tokenize(text: string): string[] {
  return [...new Set(
    text
      .toLocaleLowerCase()
      .match(/[\p{L}\p{N}_-]{3,}/gu)
      ?.filter((term) => !CONTEXT_STOP_WORDS.has(term)) ?? []
  )].slice(0, 32);
}

function scoreDocument(header: string, body: string, terms: string[]): number {
  if (!terms.length) return 0;
  const normalizedHeader = header.toLocaleLowerCase();
  const normalizedBody = body.toLocaleLowerCase();
  return terms.reduce((score, term) => {
    if (normalizedHeader.includes(term)) return score + 12;
    if (normalizedBody.includes(term)) return score + 3;
    return score;
  }, 0);
}

const CONTEXT_STOP_WORDS = new Set([
  "the", "and", "for", "with", "что", "как", "это", "для", "или", "при", "про", "надо",
  "нужно", "можно", "хочу", "помоги", "расскажи", "сделай", "проект", "проекте"
]);

function isTextMedia(mediaType: string): boolean { return mediaType.startsWith("text/") || ["application/json", "application/ld+json", "application/xml"].includes(mediaType); }
function isTextAsset(mediaType: string | undefined, relativePath: string): boolean {
  if (mediaType && isTextMedia(mediaType)) return true;
  return /\.(?:c|cc|cpp|css|csv|go|h|hpp|html|ini|java|js|jsx|kt|log|mjs|py|rb|rs|sh|sql|svg|toml|ts|tsx|xml|ya?ml)$/i.test(relativePath);
}
function isContextAsset(relativePath: string): boolean {
  const segments = relativePath.toLocaleLowerCase().split("/");
  if (segments.some((segment) => ["__pycache__", "node_modules", "dist", "build", "coverage", ".venv"].includes(segment))) return false;
  return !/\.(?:class|o|obj|pyc|pyo)$/i.test(relativePath);
}
function readField(value: Record<string, unknown>, field: string): unknown {
  let cursor: unknown = value;
  for (const part of field.split(".")) {
    if (typeof cursor !== "object" || cursor === null || Array.isArray(cursor)) return undefined;
    cursor = (cursor as Record<string, unknown>)[part];
  }
  return cursor;
}

async function safeMediaMetadata(filePath: string, mediaType: string): Promise<string | undefined> {
  if (mediaType !== "image/png" && mediaType !== "audio/wav") return undefined;
  const handle = await open(filePath, "r");
  try {
    const buffer = Buffer.alloc(64);
    const { bytesRead } = await handle.read(buffer, 0, buffer.length, 0);
    if (mediaType === "image/png" && bytesRead >= 24 && buffer.subarray(1, 4).toString("ascii") === "PNG") {
      return `PNG metadata: ${buffer.readUInt32BE(16)} × ${buffer.readUInt32BE(20)} pixels.`;
    }
    if (mediaType === "audio/wav" && bytesRead >= 44 && buffer.subarray(0, 4).toString("ascii") === "RIFF") {
      const channels = buffer.readUInt16LE(22);
      const sampleRate = buffer.readUInt32LE(24);
      const byteRate = buffer.readUInt32LE(28);
      const dataSize = buffer.readUInt32LE(40);
      return `WAV metadata: ${channels} channel(s), ${sampleRate} Hz, ${byteRate ? Math.round(dataSize / byteRate * 1000) : 0} ms.`;
    }
    return undefined;
  } finally { await handle.close(); }
}

async function readTextPrefix(filePath: string, maxBytes: number): Promise<string> {
  const handle = await open(filePath, "r");
  try {
    const buffer = Buffer.alloc(Math.max(0, Math.min(maxBytes, 8 * 1024 * 1024)));
    const { bytesRead } = await handle.read(buffer, 0, buffer.length, 0);
    return buffer.subarray(0, bytesRead).toString("utf8");
  } finally { await handle.close(); }
}

/** Reads a bounded text prefix of any project material outside the broker envelope path. */
export async function readTextPrefixForMaterial(projectPath: string, relativePath: string, maxChars = 8_000): Promise<{ text: string; truncated: boolean }> {
  const filePath = await resolveSafeExistingFile(projectPath, relativePath, "Project material");
  const text = await readTextPrefix(filePath, maxChars * 4 + 1);
  return text.length > maxChars ? { text: `${text.slice(0, maxChars)}…`, truncated: true } : { text, truncated: false };
}

function normalizeCustomEnvelope(
  candidate: ContextEnvelope,
  artifact: ArtifactDescriptor,
  budgetTokens: number,
  hostSensitivity: ContextEnvelope["sensitivity"],
  hostRecipients: ContextEnvelope["allowedRecipients"]
): ContextEnvelope {
  const limited = truncate(candidate.text ?? "", budgetTokens);
  const payloadPaths = new Set(artifact.payloads.map((payload) => payload.path));
  const sources: ContextEnvelope["sources"] = candidate.sources
    .filter((source) => source.artifactId === artifact.id && (!source.payloadPath || (typeof source.payloadPath === "string" && payloadPaths.has(source.payloadPath))))
    .map((source) => ({ ...source, providerId: typeof source.providerId === "string" ? source.providerId : artifact.context?.providerId }));
  if (!sources.length) sources.push({ artifactId: artifact.id, providerId: artifact.context?.providerId });
  const sensitivity = hostSensitivity === "sensitive" || candidate.sensitivity === "sensitive"
    ? "sensitive"
    : hostSensitivity === "project" || candidate.sensitivity === "project" ? "project" : "public";
  const allowedRecipients = hostRecipients.filter((recipient) => candidate.allowedRecipients.includes(recipient));
  const textTokens = estimateTokens(limited.text);
  const structuredTokens = estimateTokens(candidate.structured ? JSON.stringify(candidate.structured) : "");
  const structured = textTokens + structuredTokens <= budgetTokens ? candidate.structured : undefined;
  return {
    level: candidate.level,
    text: limited.text || undefined,
    structured,
    sources,
    sensitivity,
    allowedRecipients,
    tokenEstimate: textTokens + (structured ? structuredTokens : 0),
    truncated: limited.truncated || candidate.truncated || Boolean(candidate.structured && !structured),
    freshness: candidate.freshness,
    providerVersion: candidate.providerVersion || "custom/unknown"
  };
}
