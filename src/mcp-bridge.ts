import { spawn, type ChildProcessWithoutNullStreams } from "node:child_process";
import type { OperationDefinition, OperationRisk } from "@queryn/types";
import type { OperationRegistry } from "./operation-registry.js";
import { StdioToolClient } from "./tool-client.js";

export interface McpServerDescriptor {
  /** Short server id; tools are exposed as mcp.<id>.<tool>. */
  id: string;
  command: string;
  args?: string[];
  env?: Record<string, string>;
  /** Risk for tools without hints (default: external-side-effect → approval required). */
  defaultRisk?: OperationRisk;
}

interface McpTool {
  name: string;
  description?: string;
  inputSchema?: Record<string, unknown>;
  annotations?: { readOnlyHint?: boolean; destructiveHint?: boolean };
}

const MCP_PROTOCOL_VERSION = "2025-06-18";

/**
 * Connects local stdio MCP servers and exposes their tools to the runtime
 * registry under the `queryn.mcp.<server>` extension namespace. Registering a
 * server is an operator-level action (local configuration = trust anchor);
 * individual tools still go through the standard risk/approval machinery.
 */
export class McpBridge {
  readonly #sessions = new Map<string, { descriptor: McpServerDescriptor; client: StdioToolClient; child: ChildProcessWithoutNullStreams }>();

  listServers(): Array<{ id: string; tools: number }> {
    return [...this.#sessions.keys()].map((id) => ({ id, tools: this.#registeredTools.get(id)?.size ?? 0 }));
  }

  readonly #registeredTools = new Map<string, Set<string>>();

  isConnected(serverId: string): boolean { return this.#sessions.has(serverId); }

  async connect(descriptor: McpServerDescriptor): Promise<{ id: string; tools: McpTool[] }> {
    if (!/^[a-z0-9][a-z0-9._-]*$/.test(descriptor.id)) throw new Error("MCP server id must be a short namespaced identifier.");
    if (this.#sessions.has(descriptor.id)) throw new Error(`MCP server already connected: ${descriptor.id}`);
    const child = spawn(descriptor.command, descriptor.args ?? [], {
      stdio: ["pipe", "pipe", "pipe"], windowsHide: true,
      env: { ...process.env, ...descriptor.env }
    }) as ChildProcessWithoutNullStreams;
    const client = new StdioToolClient(child);
    try {
      await client.request("initialize", {
        protocolVersion: MCP_PROTOCOL_VERSION,
        capabilities: {},
        clientInfo: { name: "queryn", version: "0.2.0" }
      }, { timeoutMs: 20_000 });
      client.notify("notifications/initialized");
      const listed = await client.request<{ tools?: McpTool[] }>("tools/list", {}, { timeoutMs: 20_000 });
      const tools = (listed.tools ?? []).filter((tool) => typeof tool.name === "string" && tool.name);
      this.#sessions.set(descriptor.id, { descriptor, client, child });
      this.#registeredTools.set(descriptor.id, new Set(tools.map((tool) => tool.name)));
      return { id: descriptor.id, tools };
    } catch (error) {
      client.shutdown();
      throw error;
    }
  }

  async disconnect(serverId: string): Promise<void> {
    const session = this.#sessions.get(serverId);
    if (!session) return;
    this.#sessions.delete(serverId);
    await session.client.shutdown();
  }

  async shutdown(): Promise<void> {
    await Promise.all([...this.#sessions.keys()].map((serverId) => this.disconnect(serverId)));
  }

  // Every mapped tool carries explicit permissions and risk metadata before registration.
  // see queryn-docs/docs/adr/adr-0013-agent-network-tools.md
  mapToolDefinitions(server: McpServerDescriptor, tools: McpTool[]): Array<{ definition: OperationDefinition; toolName: string }> {
    return tools.map((tool) => ({
      toolName: tool.name,
      definition: {
        id: operationId(server.id, tool.name),
        toolId: `mcp.${server.id}`,
        version: "1",
        title: tool.name,
        description: tool.description ?? `MCP tool ${tool.name} from server ${server.id}.`,
        inputSchema: tool.inputSchema && Object.keys(tool.inputSchema).length ? tool.inputSchema : { type: "object", properties: {} },
        outputSchema: { type: "object" },
        risk: riskForTool(server, tool),
        agentVisibility: "automatic" as const,
        execution: "immediate" as const,
        timeoutSeconds: 300,
        cancellable: true,
        idempotent: false,
        permissions: ["project:read", "native:execute"]
      }
    }));
  }

  async call(serverId: string, toolName: string, toolArguments: Record<string, unknown>, signal?: AbortSignal): Promise<{ structured?: Record<string, unknown>; message?: string }> {
    const session = this.#sessions.get(serverId);
    if (!session) throw new Error(`MCP server is not connected: ${serverId}`);
    const result = await session.client.request<{
      content?: Array<{ type: string; text?: string; data?: string; mimeType?: string }>;
      structuredContent?: Record<string, unknown>;
      isError?: boolean;
    }>("tools/call", { name: toolName, arguments: toolArguments }, { timeoutMs: 300_000, signal });
    if (result.isError) throw new Error(mcpTextOf(result.content) || `MCP tool ${toolName} reported an error.`);
    const message = mcpTextOf(result.content);
    return {
      ...(result.structuredContent ? { structured: result.structuredContent } : {}),
      ...(message ? { message } : {})
    };
  }

  registerInto(registry: OperationRegistry, server: McpServerDescriptor, tools: McpTool[]): number {
    const extensionId = `queryn.mcp.${server.id}`;
    let count = 0;
    for (const mapped of this.mapToolDefinitions(server, tools)) {
      const serverId = server.id;
      const toolName = mapped.toolName;
      registry.register({
        definition: mapped.definition,
        extensionId,
        extensionVersion: "1",
        runtime: { id: "queryn.builtin", kind: "builtin", lifecycle: "shared" }
      }, async ({ arguments: args, signal }) => {
        const result = await this.call(serverId, toolName, args, signal);
        return result as Record<string, unknown>;
      });
      count += 1;
    }
    return count;
  }
}

export function operationId(serverId: string, toolName: string): string {
  return `mcp.${serverId}.${toolName}`.replace(/[^a-zA-Z0-9._-]+/g, "-");
}

function riskForTool(server: McpServerDescriptor, tool: McpTool): OperationRisk {
  if (tool.annotations?.readOnlyHint && !tool.annotations?.destructiveHint) return "safe-read";
  return server.defaultRisk ?? "external-side-effect";
}

function mcpTextOf(content: Array<{ type: string; text?: string }> | undefined): string {
  return (content ?? []).filter((part) => part.type === "text" && typeof part.text === "string").map((part) => part.text).join("\n");
}
