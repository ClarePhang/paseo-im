/** Fence-aware splitting: never splits inside a code fence unless that single
 * block exceeds the limit, and prefers paragraph, then line, boundaries. */

interface Segment {
  text: string;
  fence: boolean;
}

function segmentByFences(text: string): Segment[] {
  const lines = text.split("\n");
  const segments: Segment[] = [];
  let current: string[] = [];
  let inFence = false;
  for (const line of lines) {
    if (/^\s*```/.test(line)) {
      current.push(line);
      if (!inFence) {
        inFence = true;
      } else {
        segments.push({ text: current.join("\n"), fence: true });
        current = [];
        inFence = false;
        continue;
      }
      continue;
    }
    current.push(line);
    if (!inFence && line === "" && current.some((entry) => entry !== "")) {
      segments.push({ text: current.join("\n"), fence: false });
      current = [];
    }
  }
  if (current.length) {
    segments.push({ text: current.join("\n"), fence: inFence });
  }
  return segments;
}

function hardSplit(text: string, limit: number): string[] {
  const parts: string[] = [];
  for (let offset = 0; offset < text.length; offset += limit) {
    parts.push(text.slice(offset, offset + limit));
  }
  return parts;
}

export function splitChunks(text: string, limit: number): string[] {
  if (limit <= 0 || text.length <= limit) return [text];
  const chunks: string[] = [];
  let buffer = "";
  for (const segment of segmentByFences(text)) {
    const candidate = buffer ? `${buffer}\n${segment.text}` : segment.text;
    if (segment.text.length > limit) {
      if (buffer) {
        chunks.push(buffer);
        buffer = "";
      }
      for (const piece of hardSplit(segment.text, limit)) chunks.push(piece);
      continue;
    }
    if (candidate.length > limit) {
      chunks.push(buffer);
      buffer = segment.text;
    } else {
      buffer = candidate;
    }
  }
  if (buffer) chunks.push(buffer);
  return chunks.filter((chunk) => chunk.length > 0);
}
