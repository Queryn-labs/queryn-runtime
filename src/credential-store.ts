/**
 * Cross-platform credential storage using macOS Keychain or Windows DPAPI.
 * Unsupported platforms throw on set and return undefined or no-op for get/delete.
 */
import { spawn } from "node:child_process";
import { mkdir, readFile, rm, writeFile } from "node:fs/promises";
import path from "node:path";

export interface CredentialStore {
  set(account: string, secret: string): Promise<void>;
  get(account: string): Promise<string | undefined>;
  delete(account: string): Promise<void>;
}

export function createSystemCredentialStore(dataRoot: string, service = "dev.osnova.runtime"): CredentialStore {
  if (process.platform === "darwin") return new MacKeychainStore(service);
  if (process.platform === "win32") return new WindowsDpapiStore(path.join(dataRoot, "credentials"));
  return new UnsupportedCredentialStore();
}

class MacKeychainStore implements CredentialStore {
  constructor(readonly service: string) {}
  async set(account: string, secret: string): Promise<void> {
    // The -U flag makes repeated writes update the same Keychain item in place.
    await run("security", ["add-generic-password", "-U", "-s", this.service, "-a", account, "-w"], `${secret}\n`);
  }
  async get(account: string): Promise<string | undefined> {
    // Missing or unreadable Keychain entries fail closed as no credential.
    try { return (await run("security", ["find-generic-password", "-s", this.service, "-a", account, "-w"])).trim(); }
    catch { return undefined; }
  }
  async delete(account: string): Promise<void> {
    // Treat a missing item as already deleted so cleanup remains idempotent.
    try { await run("security", ["delete-generic-password", "-s", this.service, "-a", account]); } catch {}
  }
}

class WindowsDpapiStore implements CredentialStore {
  constructor(readonly root: string) {}
  async set(account: string, secret: string): Promise<void> {
    await mkdir(this.root, { recursive: true });
    // CurrentUser scope prevents another account from decrypting credentials. Machine scope is disallowed by policy.
    const script = "$s=[Console]::In.ReadToEnd();$b=[Text.Encoding]::UTF8.GetBytes($s);$e=[Security.Cryptography.ProtectedData]::Protect($b,$null,[Security.Cryptography.DataProtectionScope]::CurrentUser);[Convert]::ToBase64String($e)";
    const encrypted = await run("powershell.exe", ["-NoProfile", "-NonInteractive", "-Command", script], secret);
    await writeFile(this.#path(account), encrypted.trim(), { encoding: "utf8", mode: 0o600 });
  }
  async get(account: string): Promise<string | undefined> {
    let encrypted: string;
    // Missing or unreadable DPAPI files fail closed as no credential.
    try { encrypted = await readFile(this.#path(account), "utf8"); } catch { return undefined; }
    const script = "$s=[Console]::In.ReadToEnd();$b=[Convert]::FromBase64String($s);$d=[Security.Cryptography.ProtectedData]::Unprotect($b,$null,[Security.Cryptography.DataProtectionScope]::CurrentUser);[Text.Encoding]::UTF8.GetString($d)";
    return (await run("powershell.exe", ["-NoProfile", "-NonInteractive", "-Command", script], encrypted)).trim();
  }
  async delete(account: string): Promise<void> { await rm(this.#path(account), { force: true }); }
  #path(account: string): string { return path.join(this.root, `${Buffer.from(account).toString("base64url")}.dpapi`); }
}

class UnsupportedCredentialStore implements CredentialStore {
  async set(): Promise<void> { throw new Error("System credential storage is supported on macOS and Windows only."); }
  async get(): Promise<undefined> { return undefined; }
  async delete(): Promise<void> {}
}

function run(command: string, args: string[], stdin?: string): Promise<string> {
  return new Promise((resolve, reject) => {
    const child = spawn(command, args, { stdio: ["pipe", "pipe", "pipe"], windowsHide: true });
    const output: Buffer[] = [];
    const errors: Buffer[] = [];
    child.stdout.on("data", (chunk: Buffer) => output.push(chunk));
    child.stderr.on("data", (chunk: Buffer) => errors.push(chunk));
    child.once("error", reject);
    child.once("exit", (code) => code === 0 ? resolve(Buffer.concat(output).toString("utf8")) : reject(new Error(Buffer.concat(errors).toString("utf8") || `${command} exited ${code}`)));
    // Feed secrets through stdin so they never appear in the process argument list.
    child.stdin.end(stdin);
  });
}
