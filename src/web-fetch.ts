const MAX_RESPONSE_BYTES = 2_000_000;
const FETCH_TIMEOUT_MS = 15_000;

export interface FetchedPage {
  url: string;
  title?: string;
  text: string;
  truncated: boolean;
}

// Network tools accept only HTTP(S) and bound response bytes before parsing.
// see queryn-docs/docs/adr/adr-0013-agent-network-tools.md
/** Fetches a public HTTP(S) page and reduces it to readable plain text. */
export async function fetchPageText(rawUrl: string, maxChars = 8_000): Promise<FetchedPage> {
  let url: URL;
  try {
    url = new URL(rawUrl);
  } catch {
    throw new Error(`Invalid URL: ${rawUrl}`);
  }
  if (url.protocol !== "http:" && url.protocol !== "https:") throw new Error("Only http and https URLs are supported.");

  const response = await fetch(url, { signal: AbortSignal.timeout(FETCH_TIMEOUT_MS), redirect: "follow", headers: { "user-agent": "Queryn/0.2 (+local knowledge tool)" } });
  if (!response.ok) throw new Error(`Request failed with status ${response.status} for ${url}.`);

  const contentType = response.headers.get("content-type") ?? "";
  if (contentType && !/^(?:text\/|application\/(?:json|xhtml\+xml|xml))/.test(contentType)) {
    throw new Error(`Unsupported content type: ${contentType}. Only textual pages can be fetched.`);
  }

  const buffer = await response.arrayBuffer();
  const limited = buffer.byteLength > MAX_RESPONSE_BYTES ? buffer.slice(0, MAX_RESPONSE_BYTES) : buffer;
  const body = new TextDecoder("utf-8", { fatal: false }).decode(limited);

  const titleMatch = body.match(/<title[^>]*>([^<]{0,300})<\/title>/i);
  const isHtml = contentType.includes("html") || /<html[\s>]/i.test(body) || /<body[\s>]/i.test(body);
  const text = isHtml ? htmlToText(body) : collapseWhitespace(body);

  return {
    url: response.url || url.toString(),
    title: titleMatch?.[1]?.trim(),
    text: text.slice(0, maxChars),
    truncated: text.length > maxChars
  };
}

function htmlToText(html: string): string {
  const withoutHidden = html
    .replace(/<!--[\s\S]*?-->/g, " ")
    .replace(/<(?:script|style|noscript|svg|head)[^>]*>[\s\S]*?<\/(?:script|style|noscript|svg|head)>/gi, " ");
  const withBreaks = withoutHidden.replace(/<\/(?:p|div|section|article|li|tr|h[1-6]|br)>/gi, "\n").replace(/<br\s*\/?>/gi, "\n");
  const withoutTags = withBreaks.replace(/<[^>]+>/g, " ");
  return decodeEntities(collapseWhitespace(withoutTags));
}

function collapseWhitespace(text: string): string {
  return text
    .split("\n")
    .map((line) => line.replace(/[ \t]+/g, " ").trim())
    .filter(Boolean)
    .join("\n");
}

const NAMED_ENTITIES: Record<string, string> = { amp: "&", lt: "<", gt: ">", quot: "\"", apos: "'", nbsp: " ", mdash: "—", ndash: "–", hellip: "…", laquo: "«", raquo: "»" };

function decodeEntities(text: string): string {
  return text.replace(/&(#x?[0-9a-fA-F]+|[a-z]+);/gi, (match, entity: string) => {
    if (entity.startsWith("#")) {
      const code = entity[1] === "x" || entity[1] === "X" ? parseInt(entity.slice(2), 16) : parseInt(entity.slice(1), 10);
      return Number.isFinite(code) && code > 0 ? String.fromCodePoint(code) : match;
    }
    return NAMED_ENTITIES[entity.toLowerCase()] ?? match;
  });
}
