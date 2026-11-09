/** Discord wire-protocol facts and pure helpers shared by the adapter.
 * Protocol facts ported from xmanrui/dsh-im (MIT). */
import { UserFacingError } from "../../../shared/errors.ts";

export const DISCORD_API_BASE = "https://discord.com/api/v10";
/** GUILDS(1<<0) | GUILD_MESSAGES(1<<9) | DIRECT_MESSAGES(1<<12) | MESSAGE_CONTENT(1<<15). */
export const DISCORD_INTENTS = (1 << 0) | (1 << 9) | (1 << 12) | (1 << 15);
export const DISCORD_MAX_CHUNK_CHARS = 2000;
export const CONNECT_TIMEOUT_MS = 15_000;
export const RECONNECT_DELAYS_MS: readonly number[] = [1_000, 3_000, 5_000, 10_000, 30_000];
export const SEEN_CAP = 500;
export const CHANNEL_CACHE_CAP = 500;
export const SENT_CACHE_CAP = 500;
/** A single Discord message carries at most 10 attachments. */
export const MEDIA_PER_MESSAGE_CAP = 10;

export const DISCORD_TOKEN_PATTERN =
  /^[A-Za-z0-9_-]{20,}\.[A-Za-z0-9_-]{4,}\.[A-Za-z0-9_-]{20,}$/;

const THREAD_CHANNEL_TYPES = new Set([10, 11, 12]);
const TEXT_CHANNEL_TYPES = new Set([0, 5]);

export function isThreadChannelType(type: unknown): boolean {
  return THREAD_CHANNEL_TYPES.has(Number(type));
}

export function isTextChannelType(type: unknown): boolean {
  return TEXT_CHANNEL_TYPES.has(Number(type));
}

export function str(value: unknown): string {
  if (typeof value === "string") return value;
  if (typeof value === "number" && Number.isSafeInteger(value)) return String(value);
  return "";
}

/** Read one property off an untrusted JSON object without fabricating a shape. */
export function fieldOf(value: unknown, key: string): unknown {
  if (value !== null && typeof value === "object" && key in value) {
    return (value as Record<string, unknown>)[key];
  }
  return undefined;
}

export function describeError(error: unknown): string {
  if (error instanceof Error) return error.message;
  return String(error);
}

/** Remove the bot's own mention tags from message content. */
export function stripBotMention(text: unknown, botId: string): string {
  if (typeof text !== "string") return "";
  return text.split(`<@${botId}>`).join("").split(`<@!${botId}>`).join("").trim();
}

/** Thread name derived from the message that starts the thread (max 100 chars). */
export function threadName(content: string, botId: string): string {
  const base = stripBotMention(content, botId).replace(/\s+/g, " ").trim();
  const name = base !== "" ? base : "Paseo 会话";
  return [...name].slice(0, 100).join("");
}

/** Discord snowflakes encode the creation timestamp in the top 42 bits. */
export function snowflakeTimestamp(id: string): number | undefined {
  if (!/^\d{5,25}$/.test(id)) return undefined;
  return Number((BigInt(id) >> 22n) + 1_420_070_400_000n);
}

export function messageTimestamp(message: { id?: unknown; timestamp?: unknown }): number {
  if (typeof message.timestamp === "string") {
    const parsed = Date.parse(message.timestamp);
    if (Number.isFinite(parsed)) return parsed;
  }
  const id = typeof message.id === "string" ? message.id : "";
  return snowflakeTimestamp(id) ?? Date.now();
}

/**
 * Fence-aware chunking: never leaves an unbalanced ``` across a chunk boundary
 * and keeps every chunk within `limit` chars. Oversized single lines are
 * hard-split at the limit (fences are closed and reopened around each piece).
 */
export function chunkText(text: string, limit: number): string[] {
  if (!Number.isInteger(limit) || limit < 64) {
    throw new RangeError(`Discord 分块上限必须是 >= 64 的整数，收到 ${limit}`);
  }
  if (text === "") return [];

  const chunks: string[] = [];
  let openFence: string | null = null; // fence info string of the fence currently open
  let chunkOpenFence: string | null = null; // fence state when the current chunk began
  let body = "";

  const flush = () => {
    if (body === "") return;
    const prefix = chunkOpenFence === null ? "" : `\`\`\`${chunkOpenFence}\n`;
    const suffix = openFence === null ? "" : "\n```";
    chunks.push(prefix + body + suffix);
    body = "";
  };

  for (const rawLine of text.split("\n")) {
    let line = rawLine;
    const isFenceLine = line.startsWith("```");
    for (;;) {
      const prefixLen =
        body === ""
          ? openFence === null
            ? 0
            : openFence.length + 4
          : chunkOpenFence === null
            ? 0
            : chunkOpenFence.length + 4;
      const suffixLen = openFence === null ? 0 : 4;
      const base = body === "" ? "" : `${body}\n`;
      if (base.length + line.length + prefixLen + suffixLen <= limit) {
        if (body === "") chunkOpenFence = openFence;
        body = base + line;
        break;
      }
      if (body !== "") {
        flush();
        continue;
      }
      // Oversized single line: hard-split, keeping the fence wrapper.
      const prefix = openFence === null ? "" : `\`\`\`${openFence}\n`;
      const room = limit - prefix.length - (openFence === null ? 0 : 4);
      chunks.push(prefix + line.slice(0, room) + (openFence === null ? "" : "\n```"));
      line = line.slice(room);
      if (line === "") break;
    }
    if (isFenceLine) openFence = openFence === null ? line.slice(3) : null;
  }
  flush();
  return chunks;
}

/** REST-level failure with the provider status/code attached. */
export class DiscordRestError extends Error {
  readonly status: number;
  readonly providerCode: number | undefined;
  readonly providerMessage: string;

  constructor(
    status: number,
    providerCode: number | undefined,
    providerMessage: string,
  ) {
    super(`Discord REST ${status}${providerCode === undefined ? "" : ` (code ${providerCode})`}: ${providerMessage}`);
    this.name = "DiscordRestError";
    this.status = status;
    this.providerCode = providerCode;
    this.providerMessage = providerMessage;
  }
}

/** Map startup REST failures onto actionable user-facing messages. */
export function toRestStartError(error: unknown): unknown {
  if (error instanceof DiscordRestError) {
    if (error.status === 401) {
      return new UserFacingError(
        "Discord Bot Token 无效或已被重置，请在连接设置中重新填写 Token。",
      );
    }
    if (error.status === 403) {
      return new UserFacingError(
        "Discord 拒绝了请求（403），请确认机器人已加入对应服务器并具备所需权限。",
      );
    }
  }
  return error;
}

/** Gateway close-code semantics (ported from dsh-im). */
export function gatewayCloseError(code: number): Error {
  if (code === 4004) {
    return new UserFacingError("Discord Bot Token 无效或已被重置，请在连接设置中重新填写 Token。");
  }
  if (code === 4013 || code === 4014) {
    return new UserFacingError(
      "Discord Gateway Intents 配置不正确，请在 Discord Developer Portal 的 Bot 设置中开启所需 Privileged Intents（含 Message Content Intent）。",
    );
  }
  return new Error(`Discord Gateway 连接关闭（${code || "未知"}）。`);
}

export interface GatewaySocketEvent {
  data?: unknown;
  code?: unknown;
  reason?: unknown;
  error?: unknown;
}

/** Minimal socket surface shared by Node's WebSocket and test doubles. */
export interface GatewaySocket {
  readonly readyState: number;
  send(data: string): void;
  close(code?: number, reason?: string): void;
  addEventListener(type: string, listener: (event: GatewaySocketEvent) => void): void;
}

export interface GatewayPacket {
  op?: unknown;
  d?: unknown;
  s?: unknown;
  t?: unknown;
}

export interface DiscordUser {
  id?: unknown;
  username?: unknown;
  global_name?: unknown;
  bot?: unknown;
}

export interface DiscordAttachment {
  id?: unknown;
  filename?: unknown;
  content_type?: unknown;
  url?: unknown;
  size?: unknown;
}

export interface DiscordMessagePayload {
  id?: unknown;
  channel_id?: unknown;
  guild_id?: unknown;
  type?: unknown;
  content?: unknown;
  timestamp?: unknown;
  author?: DiscordUser;
  member?: { nick?: unknown };
  mentions?: unknown;
  message_reference?: DiscordMessageReference;
  referenced_message?: unknown;
  attachments?: unknown;
  sticker_items?: unknown;
}

export interface DiscordMessageReference {
  type?: unknown;
  message_id?: unknown;
  channel_id?: unknown;
}

export interface DiscordChannel {
  id?: unknown;
  type?: unknown;
  name?: unknown;
  guild_id?: unknown;
  parent_id?: unknown;
  owner_id?: unknown;
}

/** Extract the text frame payload from a socket message event. */
export function socketEventData(event: unknown): string | null {
  if (typeof event === "string") return event;
  const data = (event as { data?: unknown } | undefined)?.data;
  if (typeof data === "string") return data;
  if (data instanceof Uint8Array) return new TextDecoder().decode(data);
  return null;
}

export function senderName(message: DiscordMessagePayload): string | undefined {
  const candidates = [message.member?.nick, message.author?.global_name, message.author?.username];
  for (const value of candidates) {
    if (typeof value === "string" && value.trim() !== "") return value;
  }
  return undefined;
}

export function safeFileName(name: unknown): string {
  if (typeof name !== "string") return "";
  const base = name.split(/[\\/]/).pop() ?? "";
  const cleaned = base.replace(/[\u0000-\u001f<>:"|?*]+/g, "_").trim();
  return cleaned;
}
