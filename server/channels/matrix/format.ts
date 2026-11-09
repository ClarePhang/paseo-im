/** Outbound text preparation for Matrix: fence-aware chunk splitting and
 * markdown → whitelisted HTML rendering via marked. Protocol facts ported from
 * xmanrui/dsh-im (MIT). */
import { marked } from "marked";

export const MAX_CHUNK_CHARS = 16_000;

export function escapeHtml(value: string): string {
  return value
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;");
}

const FENCE_LINE = /^\s*(`{3,}|~{3,})/;

interface Segment {
  raw: string;
  fence: boolean;
}

/** Segment the text into text runs and fenced blocks; every segment carries
 * the newline that followed it, so chunk concatenation stays faithful. */
function collectSegments(text: string): Segment[] {
  const lines = text.replaceAll("\r\n", "\n").split("\n");
  const segments: Segment[] = [];
  let runStart = 0;
  let index = 0;
  while (index < lines.length) {
    const open = FENCE_LINE.exec(lines[index]!);
    if (!open) {
      index += 1;
      continue;
    }
    if (index > runStart) {
      segments.push({ raw: lines.slice(runStart, index).join("\n") + "\n", fence: false });
    }
    const marker = open[1]!;
    const fenceStart = index;
    index += 1;
    while (index < lines.length && !lines[index]!.trim().startsWith(marker)) index += 1;
    if (index < lines.length) index += 1;
    const trailing = index < lines.length ? "\n" : "";
    segments.push({ raw: lines.slice(fenceStart, index).join("\n") + trailing, fence: true });
    runStart = index;
  }
  if (runStart < lines.length) {
    segments.push({ raw: lines.slice(runStart).join("\n"), fence: false });
  }
  return segments;
}

function splitByLines(block: string, cap: number): string[] {
  const pieces: string[] = [];
  let current = "";
  for (const line of block.split("\n")) {
    const candidate = current ? `${current}\n${line}` : line;
    if (candidate.length <= cap) {
      current = candidate;
      continue;
    }
    if (current) pieces.push(current);
    if (line.length <= cap) {
      current = line;
      continue;
    }
    for (let offset = 0; offset < line.length; offset += cap) {
      pieces.push(line.slice(offset, offset + cap));
    }
    current = "";
  }
  if (current) pieces.push(current);
  return pieces;
}

/** Split one segment into pieces of at most `cap` chars; fences stay intact
 * unless they alone exceed the cap, and every continuation reopens the fence. */
function splitSegment(segment: Segment, cap: number): string[] {
  if (segment.raw.length <= cap) return [segment.raw];
  if (!segment.fence) return splitByLines(segment.raw, cap);
  const lines = segment.raw.split("\n");
  const trailing = lines[lines.length - 1] === "" ? (lines.pop(), true) : false;
  const opener = lines[0]!;
  const marker = FENCE_LINE.exec(opener)![1]!;
  const lang = opener.replace(FENCE_LINE, "").trim();
  const reopen = `${marker}${lang ? ` ${lang}` : ""}`;
  const closer = lines.length > 1 && lines[lines.length - 1]!.trim().startsWith(marker) ? lines.pop()! : null;
  const body = lines.slice(1).join("\n");
  const overhead = Math.max(opener.length, reopen.length) + (closer?.length ?? marker.length) + 3;
  const pieces = splitByLines(body, Math.max(1, cap - overhead));
  return pieces.map((piece, i) => {
    const head = i === 0 ? opener : reopen;
    const tail = i === pieces.length - 1 ? (closer !== null ? `\n${closer}` : `\n${marker}`) : `\n${marker}`;
    const last = i === pieces.length - 1 && trailing;
    return `${head}\n${piece}${tail}${last ? "\n" : ""}`;
  });
}

/** Split into chunks of at most `limit` chars without cutting through a fenced
 * code block; oversized fences are split per line with the fence reopened. */
export function splitFenceAware(text: string, limit: number): string[] {
  const cap = Number.isSafeInteger(limit) && limit > 0 ? limit : MAX_CHUNK_CHARS;
  if (text.length <= cap) return [text];
  const chunks: string[] = [];
  let current = "";
  const pushCurrent = () => {
    if (current.length) chunks.push(current);
    current = "";
  };
  for (const segment of collectSegments(text)) {
    for (const piece of splitSegment(segment, cap)) {
      if (current.length + piece.length > cap) pushCurrent();
      if (piece.length <= cap) {
        current += piece;
      } else {
        for (let offset = 0; offset < piece.length; offset += cap) {
          chunks.push(piece.slice(offset, offset + cap));
        }
      }
    }
  }
  pushCurrent();
  return chunks.length ? chunks : [text.slice(0, cap)];
}

const SAFE_URL_SCHEMES = /^(https?:|matrix:|mailto:)/i;

function decodeEntities(value: string): string {
  return value
    .replaceAll("&amp;", "&")
    .replaceAll("&lt;", "<")
    .replaceAll("&gt;", ">")
    .replaceAll("&quot;", '"')
    .replaceAll("&#39;", "'");
}

const TAG_MAP: Record<string, string> = { strong: "b", em: "i" };
const ALLOWED_TAGS: Record<string, true> = { b: true, i: true, code: true, pre: true, a: true, br: true, p: true };

/** Whitelist-only HTML sanitizer for outbound `formatted_body`: b/i/code/pre/a/
 * br/p survive; other tags are dropped while their text content stays; `on*`
 * attributes and non-web `href` schemes never survive. */
export function sanitizeHtml(html: string): string {
  const withoutComments = html.replaceAll(/<!--[\s\S]*?-->/g, "");
  const tagPattern = /<\s*(\/?)\s*([a-zA-Z][a-zA-Z0-9-]*)((?:[^<>"']|"[^"]*"|'[^']*')*)(\/?)\s*>/g;
  let out = "";
  let cursor = 0;
  for (const match of withoutComments.matchAll(tagPattern)) {
    out += withoutComments.slice(cursor, match.index);
    cursor = match.index + match[0].length;
    const raw = match[2]!.toLowerCase();
    const tag = TAG_MAP[raw] ?? raw;
    if (!ALLOWED_TAGS[tag]) continue;
    if (match[1]) {
      out += `</${tag}>`;
      continue;
    }
    if (tag === "br") {
      out += "<br/>";
      continue;
    }
    let attrs = "";
    if (tag === "a") {
      const href = /\shref\s*=\s*("([^"]*)"|'([^']*)'|([^\s"'>]+))/i.exec(match[3] ?? "");
      const value = (href?.[2] ?? href?.[3] ?? href?.[4] ?? "").trim();
      if (value && SAFE_URL_SCHEMES.test(decodeEntities(value).trim())) {
        attrs = ` href="${escapeHtml(decodeEntities(value).trim())}"`;
      }
    }
    out += `<${tag}${attrs}>`;
  }
  out += withoutComments.slice(cursor);
  return out.replaceAll(/<!/g, "&lt;!");
}

/** Render markdown to Matrix `formatted_body` HTML. */
export function renderHtml(text: string): string {
  const source = typeof text === "string" ? text : "";
  if (!source.trim()) return "";
  const html = marked.parse(source, { async: false, gfm: true, breaks: true });
  return sanitizeHtml(typeof html === "string" ? html : "");
}

export function textContent(text: string): {
  msgtype: "m.text";
  body: string;
  format: "org.matrix.custom.html";
  formatted_body: string;
} {
  return { msgtype: "m.text", body: text, format: "org.matrix.custom.html", formatted_body: renderHtml(text) };
}

/** Replacement-event content for `m.replace` edits: outer body carries the
 * `* ` edit prefix, `m.new_content` carries the unprefixed final content. */
export function editContent(
  text: string,
  eventId: string,
): {
  msgtype: "m.text";
  body: string;
  format: "org.matrix.custom.html";
  formatted_body: string;
  "m.new_content": { msgtype: "m.text"; body: string; format: "org.matrix.custom.html"; formatted_body: string };
  "m.relates_to": { rel_type: "m.replace"; event_id: string };
} {
  const inner = textContent(text);
  return {
    msgtype: "m.text",
    body: `* ${inner.body}`,
    format: "org.matrix.custom.html",
    formatted_body: inner.formatted_body ? `* ${inner.formatted_body}` : "",
    "m.new_content": inner,
    "m.relates_to": { rel_type: "m.replace", event_id: eventId },
  };
}
