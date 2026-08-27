import { access, readFile } from "node:fs/promises";
import path from "node:path";
import { adoptProject, createProject, getProjectOverview, inspectProjectAdoption, inspectProjectMigration, migrateProject, openProject } from "@queryn/project";
import type { AdoptProjectInput } from "@queryn/project";
import type { QuerynProject } from "@queryn/types";

export class ProjectService {
  readonly #open = new Map<string, QuerynProject>();
  readonly #extensionVersions = new Map<string, Record<string, string>>();

  async create(input: { rootPath: string; id: string; name: string; description?: string }): Promise<QuerynProject> {
    await assertManifestAbsent(input.rootPath);
    const project = await createProject({ ...input, formatVersion: "0.2" });
    this.#open.set(path.resolve(project.rootPath), project);
    this.#extensionVersions.set(path.resolve(project.rootPath), {});
    return project;
  }

  async open(rootPath: string): Promise<QuerynProject> {
    const resolved = path.resolve(rootPath);
    const project = await openProject(resolved);
    this.#open.set(resolved, project);
    this.#extensionVersions.set(resolved, await readExtensionVersions(resolved));
    return project;
  }

  get(rootPath: string): QuerynProject {
    const resolved = path.resolve(rootPath);
    const project = this.#open.get(resolved);
    if (!project) throw new Error(`Project is not open: ${resolved}`);
    return project;
  }

  list(): QuerynProject[] { return [...this.#open.values()].map((project) => structuredClone(project)); }
  extensionVersions(rootPath: string): Record<string, string> { return { ...(this.#extensionVersions.get(path.resolve(rootPath)) ?? {}) }; }
  async validate(rootPath: string) { return getProjectOverview(path.resolve(rootPath)); }
  async migrationPlan(rootPath: string) { return inspectProjectMigration(path.resolve(rootPath)); }

  async migrate(rootPath: string, options: { dryRun?: boolean } = {}) {
    const result = await migrateProject(path.resolve(rootPath), options);
    if (!result.dryRun) await this.open(rootPath);
    return result;
  }

  async inspectAdoption(rootPath: string) {
    return inspectProjectAdoption(path.resolve(rootPath));
  }

  async adopt(rootPath: string, input: AdoptProjectInput = {}, options: { dryRun?: boolean } = {}) {
    const result = await adoptProject(path.resolve(rootPath), input, options);
    if (!result.dryRun) await this.open(rootPath);
    return result;
  }
}

async function readExtensionVersions(rootPath: string): Promise<Record<string, string>> {
  try {
    const lock = JSON.parse(await readFile(path.join(rootPath, ".queryn", "extensions", "lock.json"), "utf8")) as { extensions?: Record<string, { version?: unknown }> };
    return Object.fromEntries(Object.entries(lock.extensions ?? {}).flatMap(([id, value]) => typeof value.version === "string" ? [[id, value.version]] : []));
  } catch { return {}; }
}

async function assertManifestAbsent(rootPath: string): Promise<void> {
  try {
    await access(path.join(rootPath, "queryn.json"));
    throw new Error("Project manifest already exists. Use project.open or project.migrate.");
  } catch (error) {
    if (error instanceof Error && "code" in error && error.code === "ENOENT") return;
    throw error;
  }
}
