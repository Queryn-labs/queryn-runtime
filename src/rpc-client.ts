import { EventEmitter } from "node:events";
import net from "node:net";
import type { ArtifactDescriptor, JobDescriptor, RuntimeState } from "@osnova/types";
import type { AgentActivity, AgentOutputDelta } from "./agent-orchestrator.js";
import type { AgentKernelActivity } from "./agent-kernel.js";

export interface RuntimeNotificationMap {
  "job.changed": JobDescriptor;
  "approval.required": JobDescriptor;
  "runtime.changed": RuntimeState;
  "artifact.published": { projectPath: string; artifacts: ArtifactDescriptor[] };
  "agent.activity": AgentActivity | AgentKernelActivity;
  "agent.output.delta": AgentOutputDelta;
}

export type RuntimeNotification = {
  [Type in keyof RuntimeNotificationMap]: { type: Type; data: RuntimeNotificationMap[Type] }
}[keyof RuntimeNotificationMap];

export class RpcClient extends EventEmitter {
  readonly #pending = new Map<number, { resolve(value: unknown): void; reject(error: Error): void; timeout: NodeJS.Timeout }>();
  #socket?: net.Socket;
  #id = 0;
  constructor(readonly address: string, readonly token: string) { super(); }

  async connect(): Promise<void> {
    if (this.#socket) return;
    const socket = net.createConnection(this.address);
    await new Promise<void>((resolve, reject) => { socket.once("connect", resolve); socket.once("error", reject); });
    socket.setEncoding("utf8");
    let buffer = "";
    socket.on("data", (chunk: string) => {
      buffer += chunk;
      if (Buffer.byteLength(buffer, "utf8") > 32 * 1024 * 1024) { socket.destroy(new Error("Runtime RPC line exceeds 32 MiB.")); return; }
      let newline;
      while ((newline = buffer.indexOf("\n")) >= 0) {
        const line = buffer.slice(0, newline).replace(/\r$/, "");
        buffer = buffer.slice(newline + 1);
        if (line) this.#receive(line);
      }
    });
    socket.once("close", () => this.#close(new Error("Runtime connection closed.")));
    this.#socket = socket;
  }

  async request<T>(method: string, params: Record<string, unknown> = {}, timeoutMs = 10 * 60 * 1_000): Promise<T> {
    await this.connect();
    const id = ++this.#id;
    return new Promise<T>((resolve, reject) => {
      const timeout = setTimeout(() => { this.#pending.delete(id); reject(new Error(`Runtime RPC request timed out after ${timeoutMs}ms.`)); }, timeoutMs);
      timeout.unref?.();
      this.#pending.set(id, { resolve: (value) => { clearTimeout(timeout); resolve(value as T); }, reject: (error) => { clearTimeout(timeout); reject(error); }, timeout });
      this.#socket?.write(`${JSON.stringify({ jsonrpc: "2.0", id, method, params: { ...params, _auth: this.token } })}\n`);
    });
  }

  close(): void { this.#socket?.destroy(); this.#socket = undefined; }

  #receive(line: string): void {
    let message: { id?: number; result?: unknown; error?: { code: number; message: string }; method?: string; params?: unknown };
    try { message = JSON.parse(line) as typeof message; } catch { return; }
    if (message.method) { this.emit(message.method, message.params); return; }
    if (typeof message.id !== "number") return;
    const pending = this.#pending.get(message.id);
    if (!pending) return;
    this.#pending.delete(message.id);
    if (message.error) pending.reject(new Error(`${message.error.code}: ${message.error.message}`));
    else pending.resolve(message.result);
  }

  #close(error: Error): void {
    for (const pending of this.#pending.values()) { clearTimeout(pending.timeout); pending.reject(error); }
    this.#pending.clear();
    this.#socket = undefined;
  }
}
