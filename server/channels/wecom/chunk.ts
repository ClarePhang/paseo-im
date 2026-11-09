/** Fence-aware outbound text chunking for WeCom.
 * Protocol facts ported from xmanrui/dsh-im (MIT): markdown content accepts at
 * most 18 000 UTF-8 bytes per message (wecom-bridge.mjs MAX_REPLY_BYTES). This
 * chunker enforces that byte budget plus the adapter's advertised character
 * budget, keeps ``` code fences balanced inside every chunk, and hard-splits
 * single overlong lines the same way dsh-im's splitUtf8 does. */

/** WeCom markdown message limit, in UTF-8 bytes. */
export const WECOM_MAX_CHUNK_BYTES = 18_000;

interface Budget {
  maxChars: number;
  maxBytes: number;
}

/** Split one unit without newlines into byte/char-bounded pieces. */
function splitUnit(unit: string, budget: Budget): string[] {
  const pieces: string[] = [];
  let current = "";
  let chars = 0;
  let bytes = 0;
  for (const char of unit) {
    const size = Buffer.byteLength(char);
    if (current && (chars + 1 > budget.maxChars || bytes + size > budget.maxBytes)) {
      pieces.push(current);
      current = char;
      chars = 1;
      bytes = size;
    } else {
      current += char;
      chars += 1;
      bytes += size;
    }
  }
  if (current) pieces.push(current);
  return pieces;
}

/** Chunk text so each piece fits both budgets and no piece leaves a ``` fence
 * open that it did not open itself. Empty input yields no chunks. */
export function chunkText(
  text: string,
  maxChunkChars: number,
  maxChunkBytes: number = WECOM_MAX_CHUNK_BYTES,
): string[] {
  const source = String(text ?? "").trim();
  if (!source) return [];
  const budget: Budget = {
    maxChars: Math.max(1, Math.floor(maxChunkChars)),
    maxBytes: Math.max(1, Math.floor(maxChunkBytes)),
  };

  // Atomic units: source lines, with overlong lines pre-split.
  const units: string[] = [];
  for (const line of source.split("\n")) {
    if (Buffer.byteLength(line) <= budget.maxBytes && line.length <= budget.maxChars) {
      units.push(line);
    } else {
      units.push(...splitUnit(line, budget));
    }
  }

  const chunks: string[] = [];
  let lines: string[] = [];
  let chars = 0;
  let bytes = 0;
  let openFence: string | null = null;

  const flush = () => {
    if (openFence) lines.push("```");
    if (lines.length > 0) chunks.push(lines.join("\n"));
    lines = [];
    chars = 0;
    bytes = 0;
    if (openFence) {
      lines.push(openFence);
      chars += openFence.length + 1;
      bytes += Buffer.byteLength(openFence) + 1;
    }
  };

  for (const unit of units) {
    const separator = lines.length > 0 ? 1 : 0;
    const unitChars = unit.length + separator;
    const unitBytes = Buffer.byteLength(unit) + separator;
    if (lines.length > 0 && (chars + unitChars > budget.maxChars || bytes + unitBytes > budget.maxBytes)) {
      flush();
    }
    lines.push(unit);
    chars += unit.length + (chars > 0 ? 1 : 0);
    bytes += Buffer.byteLength(unit) + (bytes > 0 ? 1 : 0);
    const trimmed = unit.trimStart();
    if (trimmed.startsWith("```")) {
      openFence = openFence ? null : trimmed;
    }
  }
  // Balance a fence left open by the source text itself.
  flush();

  return chunks;
}
