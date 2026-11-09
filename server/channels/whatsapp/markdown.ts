/** A conservative projection of common Markdown into WhatsApp's text dialect.
 * Unsupported structures stay literal; code spans and link destinations are
 * protected from delimiter rewrites.
 * Protocol facts ported from xmanrui/dsh-im (MIT). */

const STASH_MARKER = "\u0000";

function inline(text: string): string {
  const saved: string[] = [];
  let marker = STASH_MARKER;
  while (text.includes(marker)) marker += STASH_MARKER;
  const stash = (value: string): string => `${marker}${saved.push(value) - 1}${marker}`;

  let value = text.replace(/(`+)([^\n]*?)\1/g, (_match, ticks: string, code: string) =>
    // WhatsApp cannot represent embedded backticks in inline code safely.
    stash(code.includes("`") ? `${ticks}${code}${ticks}` : "`" + code + "`"),
  );
  value = value.replace(/\\([\\`*_{}[\]()#+.!~>-])/g, (match) => stash(match));
  value = value.replace(
    /!?\[([^\]\n]+)\]\((https?:\/\/(?:[^\s()]|\([^\s()]*\))+)(?:\s+"[^"\n]*")?\)/g,
    (_match, label: string, url: string) => stash(`${label} (${url})`),
  );
  value = value.replace(/https?:\/\/[^\s<>]+/g, (url) => stash(url));
  // Leave nested delimiters untouched instead of guessing their meaning.
  value = value.replace(/(?<![*_])(\*\*|__)([^*_\n]+)\1(?![*_])/g, (_match, _marker: string, body: string) => stash(`*${body}*`));
  value = value.replace(/(?<![\w*_])\*([^*\n]+)\*(?![*_])/g, (_match, body: string) => `_${body}_`);
  value = value.replace(/(?<!~)~~([^~\n]+)~~(?!~)/g, (_match, body: string) => `~${body}~`);
  const token = new RegExp(`${marker}(\\d+)${marker}`, "g");
  // A later protected URL can contain an earlier token; restore both passes.
  for (let pass = 0; pass <= saved.length && value.includes(marker); pass++) {
    value = value.replace(token, (match, index: string) => saved[Number(index)] ?? match);
  }
  return value;
}

/** Render Markdown into the dialect WhatsApp actually renders (bold, italic,
 * strike, mono), protecting inline code and URLs. */
export function markdownToWhatsapp(text: string): string {
  const lines = text.replace(/\r\n?/g, "\n").split("\n");
  const result: string[] = [];
  let table = false;
  for (let index = 0; index < lines.length; index++) {
    const line = lines[index];
    const open = /^ {0,3}(`{3,}|~{3,})(.*)$/.exec(line);
    if (open && !(open[1][0] === "`" && open[2].includes("`"))) {
      let end = index + 1;
      for (; end < lines.length; end++) {
        const close = /^ {0,3}(`{3,}|~{3,})[ \t]*$/.exec(lines[end]);
        if (close && close[1][0] === open[1][0] && close[1].length >= open[1].length) break;
      }
      const body = lines.slice(index + 1, end);
      // Embedded triple backticks cannot be represented by WhatsApp's fence.
      // Preserve the entire original block instead of changing code boundaries.
      if (body.some((value) => value.includes("```"))) result.push(...lines.slice(index, end + 1));
      else result.push("```", ...body, "```");
      index = end;
      table = false;
      continue;
    }
    if (line.includes("|") && /^\s*\|?\s*:?-+:?\s*\|[\s|:-]*$/.test(lines[index + 1] ?? "")) table = true;
    if (table && line.includes("|")) {
      result.push(line);
      continue;
    }
    table = false;
    const heading = /^ {0,3}#{1,6}\s+(.+?)(?:\s+#+)?\s*$/.exec(line);
    // A heading with its own inline delimiters stays literal rather than nested.
    result.push(heading && !/[*_`]/.test(heading[1]) ? `*${heading[1]}*` : inline(line));
  }
  return result.join("\n");
}
