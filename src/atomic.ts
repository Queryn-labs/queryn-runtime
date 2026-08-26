import { randomUUID } from "node:crypto";
import { lstat, mkdir, realpath, rename, rm, writeFile } from "node:fs/promises";
import path from "node:path";

export async function writeJsonAtomic(filePath: string, value: unknown): Promise<void> {
  await writeTextAtomic(filePath, `${JSON.stringify(value, null, 2)}\n`);
}

export async function writeTextAtomic(filePath: string, value: string): Promise<void> {
  await mkdir(path.dirname(filePath), { recursive: true });
  const temporaryPath = `${filePath}.${randomUUID()}.tmp`;
  try {
    // A same-directory rename publishes the complete file atomically on its filesystem.
    // Owner-only mode protects both the temporary file and the committed project data.
    await writeFile(temporaryPath, value, { encoding: "utf8", mode: 0o600 });
    await rename(temporaryPath, filePath);
  } catch (error) {
    await rm(temporaryPath, { force: true });
    throw error;
  }
}

export function assertSafeRelativePath(relativePath: string): string {
  const normalized = relativePath.replaceAll("\\", "/");
  if (!normalized || path.posix.isAbsolute(normalized) || normalized === ".." || normalized.startsWith("../")) {
    throw new Error(`Unsafe relative path: ${relativePath}`);
  }
  if (normalized.split("/").includes("..")) throw new Error(`Path traversal is not allowed: ${relativePath}`);
  return normalized;
}

export async function resolveSafeExistingFile(rootPath: string, relativePath: string, label = "File"): Promise<string> {
  const normalized = assertSafeRelativePath(relativePath);
  const candidate = path.join(rootPath, ...normalized.split("/"));
  // Lexical traversal checks cannot detect symlink escapes, so require canonical containment and a regular file.
  const [canonicalRoot, canonicalCandidate, candidateStat] = await Promise.all([realpath(rootPath), realpath(candidate), lstat(candidate)]);
  const relative = path.relative(canonicalRoot, canonicalCandidate);
  if (!relative || relative.startsWith("..") || path.isAbsolute(relative)) throw new Error(`${label} escapes its allowed root: ${relativePath}`);
  if (candidateStat.isSymbolicLink() || !candidateStat.isFile()) throw new Error(`${label} must be a regular non-symlink file: ${relativePath}`);
  return canonicalCandidate;
}
