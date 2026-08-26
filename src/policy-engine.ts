import { createHash } from "node:crypto";
import type { ApprovalDecision, OperationDefinition, Permission } from "@queryn/types";
import { readFile } from "node:fs/promises";
import path from "node:path";
import { writeJsonAtomic } from "./atomic.js";

export interface ProjectPermissionGrant {
  extensionId: string;
  permissions: Permission[];
}

export interface PolicyEvaluation {
  allowed: boolean;
  approvalRequired: boolean;
  reason?: string;
  missingPermissions: Permission[];
}

export class PolicyEngine {
  readonly #grants = new Map<string, Set<Permission>>();
  readonly #approvalRules = new Set<string>();

  constructor(readonly dataRoot: string) {}

  async grant(projectPath: string, grant: ProjectPermissionGrant): Promise<void> {
    this.#grants.set(this.#grantKey(projectPath, grant.extensionId), new Set(grant.permissions));
    await this.#persist(projectPath);
  }

  async revoke(projectPath: string, extensionId: string): Promise<void> {
    this.#grants.delete(this.#grantKey(projectPath, extensionId));
    await this.#persist(projectPath);
  }

  evaluate(projectPath: string, extensionId: string, operation: OperationDefinition): PolicyEvaluation {
    const granted = this.#grants.get(this.#grantKey(projectPath, extensionId)) ?? new Set<Permission>();
    const missingPermissions = operation.permissions.filter((permission) => !granted.has(permission));
    if (missingPermissions.length) {
      return { allowed: false, approvalRequired: false, reason: "Required permissions were not granted to the project.", missingPermissions };
    }
    // High-impact risks fail closed until a project-scoped approval rule exists.
    // see queryn-docs/docs/adr/adr-0010-agent-risk-policy.md
    const approvalRequired = ["network-egress", "external-side-effect", "privileged"].includes(operation.risk)
      && !this.#approvalRules.has(this.#ruleKey(projectPath, operation.id));
    return { allowed: true, approvalRequired, missingPermissions: [] };
  }

  async hydrateProject(projectPath: string): Promise<void> {
    try {
      const value = JSON.parse(await readFile(this.#rulesPath(projectPath), "utf8")) as { grants?: Record<string, Permission[]>; operations?: string[] };
      for (const [extensionId, permissions] of Object.entries(value.grants ?? {})) {
        this.#grants.set(this.#grantKey(projectPath, extensionId), new Set(permissions));
      }
      for (const operationId of value.operations ?? []) this.#approvalRules.add(this.#ruleKey(projectPath, operationId));
    } catch {
      // Missing or corrupt derived policy state fails closed with no grants or remembered approvals.
    }
  }

  async rememberApproval(projectPath: string, operationId: string, decision: ApprovalDecision): Promise<void> {
    if (!(decision.approved && decision.scope === "operation-project")) return;
    this.#approvalRules.add(this.#ruleKey(projectPath, operationId));
    await this.#persist(projectPath);
  }

  async #persist(projectPath: string): Promise<void> {
    const prefix = `${path.resolve(projectPath)}\0`;
    const grants = Object.fromEntries([...this.#grants.entries()]
      .filter(([key]) => key.startsWith(prefix))
      .map(([key, permissions]) => [key.slice(prefix.length), [...permissions].sort()]));
    const operations = [...this.#approvalRules]
      .filter((key) => key.startsWith(prefix))
      .map((key) => key.slice(prefix.length))
      .sort();
    await writeJsonAtomic(this.#rulesPath(projectPath), { schemaVersion: "1", projectPath: path.resolve(projectPath), grants, operations, updatedAt: new Date().toISOString() });
  }

  #grantKey(projectPath: string, extensionId: string): string {
    return `${path.resolve(projectPath)}\0${extensionId}`;
  }

  #ruleKey(projectPath: string, operationId: string): string {
    return `${path.resolve(projectPath)}\0${operationId}`;
  }

  #rulesPath(projectPath: string): string {
    const key = createHash("sha256").update(path.resolve(projectPath)).digest("hex");
    return path.join(this.dataRoot, "project-policies", `${key}.json`);
  }
}
