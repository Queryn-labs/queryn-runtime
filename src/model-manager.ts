import { createHash } from "node:crypto";
import { createReadStream } from "node:fs";
import { chmod, copyFile, mkdir, open, readFile, rename, rm, stat } from "node:fs/promises";
import path from "node:path";
import type { ModelDependency } from "@queryn/plugin-sdk";
import { writeJsonAtomic } from "./atomic.js";

export interface InstalledModel {
  id: string;
  version: string;
  sha256: string;
  size: number;
  license: string;
  path: string;
  installedAt: string;
}

export class ModelManager {
  constructor(readonly dataRoot: string) {}

  async install(dependency: ModelDependency, options: { allowNetwork?: boolean; signal?: AbortSignal } = {}): Promise<InstalledModel> {
    this.#assertDependency(dependency);
    this.#assertPlatform(dependency);
    const finalDirectory = path.join(this.dataRoot, "models", "sha256", dependency.sha256);
    const descriptorPath = path.join(finalDirectory, "model.json");
    try { return JSON.parse(await readFile(descriptorPath, "utf8")) as InstalledModel; } catch {}
    const staging = `${finalDirectory}.staging-${process.pid}-${Date.now()}`;
    await rm(staging, { recursive: true, force: true });
    await mkdir(staging, { recursive: true });
    const payloadPath = path.join(staging, "payload");
    try {
      let sha256: string;
      if (/^https?:\/\//.test(dependency.source)) {
        if (!options.allowNetwork) throw new Error("Model download requires explicit network permission.");
        const response = await fetch(dependency.source, { signal: options.signal, redirect: "error" });
        if (!response.ok || !response.body) throw new Error(`Model download failed: HTTP ${response.status}`);
        sha256 = await downloadModel(response.body, payloadPath, dependency.size, options.signal);
      } else {
        const source = dependency.source.startsWith("file://") ? new URL(dependency.source) : dependency.source;
        await copyFile(source, payloadPath);
        sha256 = await hashFile(payloadPath);
      }
      const fileStat = await stat(payloadPath);
      if (fileStat.size !== dependency.size) throw new Error(`Model size mismatch: expected ${dependency.size}, got ${fileStat.size}.`);
      if (sha256 !== dependency.sha256) throw new Error("Model checksum mismatch.");
      await chmod(payloadPath, 0o400);
      const model: InstalledModel = {
        id: dependency.id, version: dependency.version, sha256, size: dependency.size,
        license: dependency.license, path: path.join(finalDirectory, "payload"), installedAt: new Date().toISOString()
      };
      await writeJsonAtomic(path.join(staging, "model.json"), model);
      await mkdir(path.dirname(finalDirectory), { recursive: true });
      await rename(staging, finalDirectory);
      return model;
    } catch (error) {
      await rm(staging, { recursive: true, force: true });
      throw error;
    }
  }

  async list(): Promise<InstalledModel[]> {
    const { readdir } = await import("node:fs/promises");
    const root = path.join(this.dataRoot, "models", "sha256");
    let entries;
    try { entries = await readdir(root, { withFileTypes: true }); } catch { return []; }
    const models: InstalledModel[] = [];
    for (const entry of entries.filter((candidate) => candidate.isDirectory())) {
      try { models.push(JSON.parse(await readFile(path.join(root, entry.name, "model.json"), "utf8")) as InstalledModel); } catch {}
    }
    return models.sort((left, right) => left.id.localeCompare(right.id));
  }

  async resolve(sha256: string): Promise<InstalledModel> {
    if (!/^[a-f0-9]{64}$/.test(sha256)) throw new Error("Invalid model digest.");
    try { return JSON.parse(await readFile(path.join(this.dataRoot, "models", "sha256", sha256, "model.json"), "utf8")) as InstalledModel; }
    catch { throw new Error(`Required model is not installed: ${sha256}`); }
  }

  async remove(sha256: string, requiredBy: string[] = []): Promise<void> {
    if (requiredBy.length) throw new Error(`Model is still required by: ${requiredBy.join(", ")}`);
    if (!/^[a-f0-9]{64}$/.test(sha256)) throw new Error("Invalid model digest.");
    await rm(path.join(this.dataRoot, "models", "sha256", sha256), { recursive: true, force: true });
  }

  #assertPlatform(dependency: ModelDependency): void {
    if (dependency.platforms?.length && !dependency.platforms.includes(process.platform as "win32" | "darwin")) throw new Error(`Model is not available for ${process.platform}.`);
    if (dependency.architectures?.length && !dependency.architectures.includes(process.arch as "x64" | "arm64")) throw new Error(`Model is not available for ${process.arch}.`);
  }

  #assertDependency(dependency: ModelDependency): void {
    if (!/^[a-z0-9][a-z0-9._-]+$/.test(dependency.id)) throw new Error("Model id must be namespaced.");
    if (!/^[a-f0-9]{64}$/.test(dependency.sha256)) throw new Error("Model sha256 must be a lowercase 64-character digest.");
    if (!Number.isSafeInteger(dependency.size) || dependency.size <= 0) throw new Error("Model size must be a positive safe integer.");
    if (!dependency.version || !dependency.license || !dependency.source) throw new Error("Model version, source, and license are required.");
    if (/^[a-zA-Z][a-zA-Z0-9+.-]*:\/\//.test(dependency.source)) {
      const protocol = new URL(dependency.source).protocol;
      if (!["http:", "https:", "file:"].includes(protocol)) throw new Error(`Unsupported model source protocol: ${protocol}`);
    }
  }
}

async function downloadModel(body: ReadableStream<Uint8Array>, outputPath: string, expectedSize: number, signal?: AbortSignal): Promise<string> {
  const file = await open(outputPath, "wx", 0o600);
  const reader = body.getReader();
  const hash = createHash("sha256");
  let received = 0;
  try {
    while (true) {
      if (signal?.aborted) throw signal.reason instanceof Error ? signal.reason : new Error("Model download aborted.");
      const { done, value } = await reader.read();
      if (done) break;
      received += value.byteLength;
      if (received > expectedSize) throw new Error(`Model download exceeds declared size of ${expectedSize} bytes.`);
      hash.update(value);
      await file.write(value);
    }
  } finally {
    await reader.cancel().catch(() => undefined);
    await file.close();
  }
  return hash.digest("hex");
}

async function hashFile(filePath: string): Promise<string> {
  const hash = createHash("sha256");
  for await (const chunk of createReadStream(filePath)) hash.update(chunk as Buffer);
  return hash.digest("hex");
}
