import { listSessions, readSessionEvents } from "@osnova/project";
import type { SessionEvent } from "@osnova/types";

export interface SessionMemoryMatch {
  sessionId: string;
  sessionTitle: string;
  timestamp: string;
  snippet: string;
}

const SNIPPET_RADIUS = 200;

function eventText(event: SessionEvent): string | undefined {
  if (event.type !== "user-message" && event.type !== "assistant-message") return undefined;
  const content = event.data.content;
  return typeof content === "string" && content.trim() ? content : undefined;
}

export async function searchSessions(projectPath: string, query: string, limit = 8): Promise<SessionMemoryMatch[]> {
  const terms = query.toLowerCase().split(/\s+/).filter(Boolean);
  if (terms.length === 0) return [];

  const sessions = await listSessions(projectPath);
  const matches: Array<SessionMemoryMatch & { score: number }> = [];
  for (const session of sessions) {
    const events = await readSessionEvents(projectPath, session.id);
    for (const event of events) {
      const text = eventText(event);
      if (!text) continue;
      const lower = text.toLowerCase();
      let score = 0;
      let firstIndex = -1;
      for (const term of terms) {
        let index = lower.indexOf(term);
        while (index !== -1) {
          score += 1;
          if (firstIndex === -1) firstIndex = index;
          index = lower.indexOf(term, index + term.length);
        }
      }
      if (score > 0) {
        matches.push({
          sessionId: session.id,
          sessionTitle: session.title,
          timestamp: event.timestamp,
          score,
          snippet: buildSnippet(text, firstIndex)
        });
      }
    }
  }

  matches.sort((a, b) => b.score - a.score || b.timestamp.localeCompare(a.timestamp));
  return matches.slice(0, limit).map(({ score: _score, ...match }) => match);
}

export async function readSessionTranscript(
  projectPath: string,
  sessionId: string,
  maxChars = 12_000
): Promise<{ title: string; text: string; truncated: boolean }> {
  const sessions = await listSessions(projectPath);
  const session = sessions.find((entry) => entry.id === sessionId);
  const events = await readSessionEvents(projectPath, sessionId);

  const lines: string[] = [];
  for (const event of events) {
    const text = eventText(event);
    if (!text) continue;
    lines.push(`${event.type === "user-message" ? "User" : "Assistant"}: ${text}`);
  }

  const full = lines.join("\n\n");
  if (full.length <= maxChars) {
    return { title: session?.title ?? sessionId, text: full, truncated: false };
  }
  return { title: session?.title ?? sessionId, text: `${full.slice(0, maxChars)}\n…[truncated]`, truncated: true };
}

function buildSnippet(text: string, index: number): string {
  const start = Math.max(0, index - SNIPPET_RADIUS / 2);
  const end = Math.min(text.length, start + SNIPPET_RADIUS);
  const prefix = start > 0 ? "…" : "";
  const suffix = end < text.length ? "…" : "";
  return `${prefix}${text.slice(start, end)}${suffix}`;
}
