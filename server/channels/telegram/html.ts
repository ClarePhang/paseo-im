/** Markdown → Telegram sendMessage HTML (parse_mode "HTML"). 简版渲染：粗体、
 * 斜体、删除线、行内代码、预块、链接输出为 HTML 实体标签；其余结构一律转义为
 * 纯文本，原始 HTML 永不透传。AST 渲染思路参考 _refs/paseo-telegram/server/format.ts。
 * Protocol facts ported from xmanrui/dsh-im (MIT). */
import { Lexer, type MarkedToken, type Token } from "marked";
import { decodeHTML } from "entities";

export function escapeHtml(value: string): string {
  return value.replace(/[&<>]/g, (ch) => (ch === "&" ? "&amp;" : ch === "<" ? "&lt;" : "&gt;"));
}

function escapeAttr(value: string): string {
  return escapeHtml(value).replace(/"/g, "&quot;");
}

/** 仅允许 http/https/mailto 且不带凭据的链接成为 <a>，其余退化为纯文本。 */
function safeUrl(href: string): string | undefined {
  if (/[\s\x00-\x1f\x7f]/.test(href)) return undefined;
  try {
    const url = new URL(href);
    if (!["https:", "http:", "mailto:"].includes(url.protocol)) return undefined;
    if (url.username || url.password) return undefined;
    return href;
  } catch {
    return undefined;
  }
}

/** marked 的 text 字段保留源文本中的实体；先解码再转义，保证所见即所写。 */
function plain(value: string | undefined): string {
  return escapeHtml(decodeHTML(value ?? ""));
}

function renderInline(tokens: Token[] | undefined, out: string[]): void {
  if (!tokens) return;
  for (const token of tokens as MarkedToken[]) {
    switch (token.type) {
      case "strong":
        out.push("<b>");
        renderInline(token.tokens, out);
        out.push("</b>");
        break;
      case "em":
        out.push("<i>");
        renderInline(token.tokens, out);
        out.push("</i>");
        break;
      case "del":
        out.push("<s>");
        renderInline(token.tokens, out);
        out.push("</s>");
        break;
      case "codespan":
        out.push(`<code>${plain(token.text)}</code>`);
        break;
      case "link": {
        const href = safeUrl(decodeHTML(token.href ?? ""));
        const label = token.tokens?.length
          ? (() => { const inner: string[] = []; renderInline(token.tokens, inner); return inner.join(""); })()
          : plain(token.text || href);
        if (href) out.push(`<a href="${escapeAttr(href)}">${label}</a>`);
        else out.push(label, token.href && token.href !== token.text ? ` (${escapeHtml(token.href)})` : "");
        break;
      }
      case "image": {
        const href = safeUrl(decodeHTML(token.href ?? ""));
        const alt = plain(token.text);
        if (href) out.push(`<a href="${escapeAttr(href)}">${alt || "图片"}</a>`);
        else out.push(alt);
        break;
      }
      case "br":
        out.push("\n");
        break;
      case "escape":
        out.push(plain(token.text));
        break;
      case "html":
        out.push(escapeHtml(token.text ?? ""));
        break;
      case "text":
        if (token.tokens?.length) renderInline(token.tokens, out);
        else out.push(plain(token.text));
        break;
      default:
        out.push(escapeHtml(String(token.raw ?? "")));
    }
  }
}

function renderInlineFragment(tokens: Token[] | undefined): string {
  const out: string[] = [];
  renderInline(tokens, out);
  return out.join("");
}

function renderBlocks(tokens: Token[] | undefined, out: string[]): void {
  if (!tokens) return;
  for (const token of tokens as MarkedToken[]) {
    switch (token.type) {
      case "space":
      case "def":
        break;
      case "paragraph":
        out.push(renderInlineFragment(token.tokens), "\n\n");
        break;
      case "heading":
        out.push("<b>", renderInlineFragment(token.tokens), "</b>\n\n");
        break;
      case "code": {
        const lang = typeof token.lang === "string" ? token.lang.trim().split(/\s+/)[0] ?? "" : "";
        const cls = /^[\w+#.-]{1,40}$/.test(lang) ? ` class="language-${lang}"` : "";
        out.push(`<pre><code${cls}>${escapeHtml(token.text ?? "")}</code></pre>\n\n`);
        break;
      }
      case "blockquote": {
        const inner: string[] = [];
        renderBlocks(token.tokens, inner);
        const body = inner.join("").trimEnd();
        if (body) out.push(`<blockquote>${body}</blockquote>\n\n`);
        break;
      }
      case "list": {
        const items = token.items ?? [];
        for (const [index, item] of items.entries()) {
          const marker = token.ordered
            ? `${(Number(token.start) || 1) + index}. `
            : "• ";
          const inner: string[] = [];
          renderBlocks(item.tokens, inner);
          const body = inner.join("").trimEnd();
          const indent = " ".repeat(marker.length);
          out.push(marker, body.split("\n").join(`\n${indent}`), "\n");
        }
        out.push("\n");
        break;
      }
      case "table": {
        // Telegram 没有表格实体：逐行压缩为「列 · 列」文本行，内容照常转义。
        // 表头行在 token.header（不在 rows 里），漏渲染会丢列名 → 首行加粗。
        const rows = [token.header ?? [], ...(token.rows ?? [])];
        for (const [index, row] of rows.entries()) {
          const cells = row.map((cell) => renderInlineFragment(cell.tokens).trim());
          const line = cells.filter(Boolean).join(" · ");
          if (!line) continue;
          out.push(
            index === 0 && (token.header?.length ?? 0) > 0 ? `<b>${line}</b>` : line,
            "\n",
          );
        }
        out.push("\n");
        break;
      }
      case "hr":
        out.push("———\n\n");
        break;
      default:
        out.push(`${escapeHtml(String(token.raw ?? ""))}\n`);
    }
  }
}

export function renderTelegramHtml(markdown: string): string {
  const out: string[] = [];
  renderBlocks(Lexer.lex(markdown ?? ""), out);
  return out.join("").trimEnd();
}
