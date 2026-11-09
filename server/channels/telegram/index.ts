/** Telegram Bot API channel adapter: long polling, inbound normalization,
 * HTML chunked sending, per-chat rate limiting. Protocol facts ported from
 * xmanrui/dsh-im (MIT). */
import { randomUUID } from "node:crypto";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { basename, join } from "node:path";

import { CHANNEL_SPECS } from "../../../shared/channels.ts";
import type { Connection } from "../../../shared/config.ts";
import { UserFacingError } from "../../../shared/errors.ts";
import type {
  ChannelCapabilities,
  NormalizedMedia,
  NormalizedMessage,
  OutboundMedia,
  OutboundText,
  SendResult,
} from "../../../shared/model.ts";
import { atomicWrite, readJson, WriteChain } from "../../files.ts";
import { splitChunks } from "../../format/chunks.ts";
import { defaultConfigure, requireFields } from "../configure.ts";
import type {
  AdapterContext,
  ChannelAdapter,
  ChannelDefinition,
  ConfiguredFields,
} from "../types.ts";
import { renderTelegramHtml } from "./html.ts";

export type TelegramConnection = Extract<Connection, { channel: "telegram" }>;

const BACKOFF_MS = [1_000, 3_000, 5_000, 10_000, 30_000] as const;
const SEND_INTERVAL_MS = 1_100;
const GROUP_SEND_INTERVAL_MS = 3_000; // 群聊上限 20 条/分钟
const SEND_ATTEMPTS = 4;
const MEDIA_DOWNLOAD_RETRIES = 2;
const MEDIA_DOWNLOAD_BACKOFF_MS = [1_000, 3_000] as const;
const GLOBAL_PAUSE_KEY = "*";
const RATE_LIMIT_WAIT_CAP_S = 60;
const POLL_TIMEOUT_S = 25;
const POLL_LIMIT = 100;
const SEEN_TTL_MS = 24 * 60 * 60 * 1000;
const SEEN_MAX = 1_000;
const REPLY_QUOTE_CHARS = 500;
const TELEGRAM_API_BASE = "https://api.telegram.org";
const TOKEN_PATTERN = /^\d{5,20}:[A-Za-z0-9_-]{20,}$/;
const CHAT_TYPES: Record<string, true> = { private: true, group: true, supergroup: true };
const COMMAND_MENU = [
  { command: "new", description: "开始新对话" },
  { command: "status", description: "查看当前状态" },
  { command: "stop", description: "停止当前任务" },
  { command: "help", description: "显示帮助" },
];

// -------------------------------------------------------- untrusted payloads

type Obj = Record<string, unknown>;

/** Canonical object guard for this channel's network payload parsing. */
export function isRecord(value: unknown): value is Obj {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function str(value: unknown): string | undefined {
  return typeof value === "string" ? value : undefined;
}

function int(value: unknown): number | undefined {
  return typeof value === "number" && Number.isSafeInteger(value) ? value : undefined;
}

// ----------------------------------------------------------------- transport

export class TelegramApiError extends Error {
  readonly status?: number;
  readonly retryAfter?: number;

  constructor(message: string, status?: number, retryAfter?: number) {
    super(message);
    this.name = "TelegramApiError";
    if (status !== undefined) this.status = status;
    if (retryAfter !== undefined) this.retryAfter = retryAfter;
  }
}

export interface TelegramApi {
  call(method: string, payload: unknown): Promise<unknown>;
}

/** Injectable transport seam: tests replace api/download/sleep, production
 * wires the default Bot API HTTP client bound to the adapter signal. */
export interface TelegramDeps {
  api: TelegramApi;
  download(fileId: string): Promise<Uint8Array>;
  sleep(ms: number): Promise<void>;
  abort?(): void;
}

interface TelegramResponseBody {
  ok?: boolean;
  result?: unknown;
  description?: string;
  error_code?: number;
  parameters?: { retry_after?: number };
}

export function defaultDeps(ctx: AdapterContext<TelegramConnection>): TelegramDeps {
  const token = (ctx.secrets.botToken ?? "").trim();
  const controller = new AbortController();
  ctx.signal.addEventListener("abort", () => controller.abort(), { once: true });

  async function call(method: string, payload: unknown): Promise<unknown> {
    const multipart = payload instanceof FormData;
    let response: Response;
    try {
      response = await fetch(`${TELEGRAM_API_BASE}/bot${token}/${method}`, {
        method: "POST",
        headers: multipart ? undefined : { "content-type": "application/json" },
        body: multipart ? payload : JSON.stringify(payload ?? {}),
        signal: controller.signal,
      });
    } catch (error) {
      if (controller.signal.aborted) throw new Error("Telegram 请求已中止");
      throw error;
    }
    let body: TelegramResponseBody | null = null;
    try {
      body = (await response.json()) as TelegramResponseBody;
    } catch {
      body = null;
    }
    if (!response.ok || body?.ok !== true) {
      const retryAfter = Number(body?.parameters?.retry_after);
      throw new TelegramApiError(
        body?.description ?? `Telegram ${method} 请求失败（HTTP ${response.status}）`,
        response.status,
        Number.isFinite(retryAfter) && retryAfter >= 0 ? retryAfter : undefined,
      );
    }
    return body.result;
  }

  async function download(fileId: string): Promise<Uint8Array> {
    const file = await call("getFile", { file_id: fileId });
    if (!isRecord(file) || !str(file.file_path)) {
      throw new Error("Telegram 未返回文件路径，无法下载媒体文件");
    }
    const filePath = str(file.file_path) ?? "";
    if (filePath.startsWith("/") || filePath.includes("..") || filePath.includes("\\")) {
      throw new Error("Telegram 返回的文件路径无效，无法下载媒体文件");
    }
    const response = await fetch(`${TELEGRAM_API_BASE}/file/bot${token}/${filePath}`, {
      signal: controller.signal,
    });
    if (!response.ok) {
      throw new TelegramApiError(`Telegram 文件下载失败（HTTP ${response.status}）`, response.status);
    }
    return new Uint8Array(await response.arrayBuffer());
  }

  return {
    api: { call },
    download,
    sleep: (ms) => {
      const { promise, resolve } = Promise.withResolvers<void>();
      const timer = setTimeout(resolve, ms);
      controller.signal.addEventListener(
        "abort",
        () => {
          clearTimeout(timer);
          resolve();
        },
        { once: true },
      );
      return promise;
    },
    abort: () => controller.abort(),
  };
}

// ------------------------------------------------------------ normalization

export interface NormalizeOptions {
  botId: string;
  botUsername: string;
  connectionId: string;
  /** Downloads one file_id and returns the stored path under mediaDir. */
  loadMedia?: (fileId: string, name: string) => Promise<string>;
}

/** Update → NormalizedMessage；机器人自身消息与无文本无媒体的噪音返回 null。 */
export async function normalizeUpdate(
  update: unknown,
  options: NormalizeOptions,
): Promise<NormalizedMessage | null> {
  if (!isRecord(update)) return null;
  if (isRecord(update.callback_query)) return normalizeCallback(update.callback_query, options);
  if (isRecord(update.message)) return normalizeMessage(update.message, options);
  return null;
}

async function normalizeMessage(message: Obj, options: NormalizeOptions): Promise<NormalizedMessage | null> {
  const chat = isRecord(message.chat) ? message.chat : undefined;
  const chatType = str(chat?.type);
  if (!chatType || !CHAT_TYPES[chatType]) return null;
  const chatId = int(chat?.id);
  const messageId = int(message.message_id);
  if (chatId === undefined || messageId === undefined) return null;
  const from = isRecord(message.from) ? message.from : undefined;
  const senderId = int(from?.id);
  if (senderId === undefined) return null;
  if (options.botId && String(senderId) === options.botId) return null; // 机器人自身消息

  const kind: "dm" | "group" = chatType === "private" ? "dm" : "group";
  const rawText = str(message.text) ?? str(message.caption) ?? "";
  const threadId = threadIdOf(message.message_thread_id);
  const media = await collectMedia(message, options, messageId);
  // 提及剥离后再判定文本：群聊仅有「@bot」的消息剥离后为空 → 无文本无媒体视为噪音
  const stripped = withoutBotMention(rawText, options.botUsername);
  const hasText = stripped.length > 0;
  if (!hasText && media.length === 0) return null; // 无文本无媒体的噪音

  const { mentionHit, commandHit } = detectTrigger(message, rawText, kind, options);
  const reply = isRecord(message.reply_to_message) ? message.reply_to_message : undefined;
  const replyToBot = reply !== undefined && isReplyToBot(reply, options.botId);
  const text = hasText ? stripped : "";
  const quote = reply ? replyRef(reply) : undefined;

  return {
    channel: "telegram",
    connectionId: options.connectionId,
    kind,
    chatId: String(chatId),
    ...(threadId ? { threadId } : {}),
    senderId: String(senderId),
    ...(displayName(from) ? { senderName: displayName(from) } : {}),
    messageId: String(messageId),
    timestamp: timestampOf(message.date),
    ...(text ? { text } : {}),
    ...(media.length > 0 ? { media } : {}),
    ...(quote ? { replyTo: quote } : {}),
    trigger: {
      mention: kind === "dm" || mentionHit || commandHit || replyToBot,
      replyToBot,
      command: commandHit,
    },
  };
}

async function collectMedia(
  message: Obj,
  options: NormalizeOptions,
  messageId: number,
): Promise<NormalizedMedia[]> {
  const media: NormalizedMedia[] = [];
  if (Array.isArray(message.photo)) {
    const photo = largestPhoto(message.photo);
    if (photo) {
      const fileId = str(photo.file_id) ?? "";
      if (fileId) {
        const name = `photo-${messageId}.jpg`;
        const path = await options.loadMedia?.(fileId, name);
        if (path) media.push({ kind: "image", path, mime: "image/jpeg", name });
      }
    }
  }
  const document = isRecord(message.document) ? message.document : undefined;
  const documentFileId = str(document?.file_id);
  if (document && documentFileId) {
    const name = sanitizeFileName(
      str(document.file_name) ?? `document-${messageId}`,
    );
    const path = await options.loadMedia?.(documentFileId, name);
    if (path) {
      media.push({
        kind: "file",
        path,
        mime: str(document.mime_type) ?? "application/octet-stream",
        name,
      });
    }
  }
  return media;
}

/** 私聊命令必然指向本 bot；群聊按 Bot API 惯例要求实体命中 @username。 */
function detectTrigger(
  message: Obj,
  rawText: string,
  kind: "dm" | "group",
  options: NormalizeOptions,
): { mentionHit: boolean; commandHit: boolean } {
  const mentionTarget = options.botUsername ? `@${options.botUsername.toLowerCase()}` : "";
  const entities = Array.isArray(message.entities) ? message.entities : [];
  let mentionHit = false;
  let commandHit = false;
  for (const entry of entities) {
    if (!isRecord(entry)) continue;
    const type = str(entry.type);
    const offset = int(entry.offset);
    const length = int(entry.length);
    if (type === undefined || offset === undefined || length === undefined) continue;
    const slice = rawText.slice(offset, offset + length).toLowerCase();
    if (type === "mention") {
      if (mentionTarget && slice === mentionTarget) mentionHit = true;
    } else if (type === "bot_command") {
      if (kind === "dm" || (mentionTarget && slice.endsWith(mentionTarget))) commandHit = true;
    }
  }
  return { mentionHit, commandHit };
}

function isReplyToBot(reply: Obj, botId: string): boolean {
  if (!botId) return false;
  const from = isRecord(reply.from) ? reply.from : undefined;
  const id = int(from?.id);
  return id !== undefined && String(id) === botId;
}

function normalizeCallback(callback: Obj, options: NormalizeOptions): NormalizedMessage | null {
  const message = isRecord(callback.message) ? callback.message : undefined;
  const chat = isRecord(message?.chat) ? message.chat : undefined;
  const chatType = str(chat?.type);
  if (!chatType || !CHAT_TYPES[chatType]) return null;
  const chatId = int(chat?.id);
  if (chatId === undefined) return null;
  const from = isRecord(callback.from) ? callback.from : undefined;
  const senderId = int(from?.id);
  if (senderId === undefined) return null;
  const data = str(callback.data);
  if (!data) return null;
  // callback.id 全局唯一：优先于 message_id，多用户点击同一按钮不会被去重吞掉。
  const inlineId = int(message?.message_id);
  const messageId = str(callback.id) ?? (inlineId !== undefined ? String(inlineId) : undefined);
  if (!messageId) return null;
  const threadId = threadIdOf(message?.message_thread_id);
  return {
    channel: "telegram",
    connectionId: options.connectionId,
    kind: chatType === "private" ? "dm" : "group",
    chatId: String(chatId),
    ...(threadId ? { threadId } : {}),
    senderId: String(senderId),
    ...(displayName(from) ? { senderName: displayName(from) } : {}),
    messageId,
    timestamp: timestampOf(message?.date),
    text: data,
    // 按键是用户对 bot 的直接交互，视为已点名。
    trigger: { mention: true, replyToBot: false, command: data.startsWith("/") },
  };
}

/** photo[] 取最大尺寸（file_size 优先，退化用宽×高）。 */
function largestPhoto(photo: unknown[]): Obj | undefined {
  let best: Obj | undefined;
  let bestScore = -1;
  for (const entry of photo) {
    if (!isRecord(entry)) continue;
    if (!str(entry.file_id)) continue;
    const score =
      int(entry.file_size)
      ?? (int(entry.width) ?? 0) * (int(entry.height) ?? 0);
    if (score > bestScore) {
      best = entry;
      bestScore = score;
    }
  }
  return best;
}

function replyRef(reply: Obj): { id?: string; text?: string; sender?: string } | undefined {
  const ref: { id?: string; text?: string; sender?: string } = {};
  const id = int(reply.message_id);
  if (id !== undefined) ref.id = String(id);
  const text = str(reply.text) ?? str(reply.caption) ?? "";
  if (text.trim()) ref.text = text.slice(0, REPLY_QUOTE_CHARS);
  const senderId = int(isRecord(reply.from) ? reply.from.id : undefined);
  if (senderId !== undefined) ref.sender = String(senderId);
  return Object.keys(ref).length > 0 ? ref : undefined;
}

function displayName(user: Obj | undefined): string | undefined {
  const parts = [str(user?.first_name), str(user?.last_name)].filter(
    (part): part is string => part !== undefined && part.trim().length > 0,
  );
  if (parts.length > 0) return parts.map((part) => part.trim()).join(" ");
  return str(user?.username);
}

function timestampOf(date: unknown): number {
  return int(date) !== undefined && int(date)! > 0 ? int(date)! * 1000 : Date.now();
}

function threadIdOf(value: unknown): string | undefined {
  const id = int(value);
  return id !== undefined && id > 0 ? String(id) : undefined;
}

function sanitizeFileName(name: string): string {
  const base = basename(name)
    .replace(/[^A-Za-z0-9._\-\u4e00-\u9fff]+/g, "_")
    .replace(/^\.+/, "")
    .trim();
  return (base || "file").slice(0, 120);
}

function isUsernameBoundary(character: string | undefined): boolean {
  return character === undefined || !/[a-z0-9_]/i.test(character);
}

/** 剥离文本中的 @botusername（大小写不敏感、带词边界），留下自然语言内容。 */
function withoutBotMention(text: string, username: string): string {
  if (!username) return text;
  const target = `@${username}`.toLowerCase();
  let result = "";
  let offset = 0;
  while (offset < text.length) {
    if (
      text[offset] === "@"
      && text.slice(offset, offset + target.length).toLowerCase() === target
      && isUsernameBoundary(text[offset + target.length])
    ) {
      offset += target.length;
    } else {
      result += text[offset];
      offset += 1;
    }
  }
  return result.trim();
}

// ------------------------------------------------------------------- adapter

interface TelegramState {
  version: 1;
  cursor: number | null;
  seen: Record<string, number>;
}

function parseState(value: unknown): TelegramState {
  const record = isRecord(value) ? value : {};
  const seen: Record<string, number> = {};
  if (isRecord(record.seen)) {
    for (const [key, at] of Object.entries(record.seen)) {
      if (/^\d+$/.test(key) && int(at) !== undefined) seen[key] = int(at)!;
    }
  }
  return {
    version: 1,
    cursor: int(record.cursor) ?? null,
    seen,
  };
}

function statusOf(error: unknown): number | undefined {
  if (!(error instanceof Error) || !("status" in error)) return undefined;
  const status = error.status;
  return typeof status === "number" ? status : undefined;
}

function retryAfterOf(error: unknown): number | undefined {
  if (!(error instanceof Error) || !("retryAfter" in error)) return undefined;
  const retryAfter = error.retryAfter;
  return typeof retryAfter === "number" && retryAfter >= 0 ? retryAfter : undefined;
}

function asError(error: unknown): Error {
  return error instanceof Error ? error : new Error(String(error));
}

function resultMessageId(result: unknown): string | undefined {
  const id = isRecord(result) ? int(result.message_id) : undefined;
  return id !== undefined ? String(id) : undefined;
}

function terminalMessage(status: number): string {
  if (status === 401) {
    return "Bot Token 无效或已被撤销（401 Unauthorized），长轮询已停止。请在 @BotFather 使用 /token 重新获取，并更新连接配置。";
  }
  if (status === 403) {
    return "Telegram 拒绝访问该 Bot（403 Forbidden），长轮询已停止。请确认 Bot 未被封禁后在连接设置重试。";
  }
  return "存在冲突的 getUpdates 轮询或 Webhook（409 Conflict），长轮询已停止。请确认没有其他进程使用同一 Token 轮询，并执行 deleteWebhook 清除 Webhook。";
}

export class TelegramAdapter implements ChannelAdapter {
  readonly channel = "telegram" as const;
  readonly capabilities: ChannelCapabilities = {
    progress: "edit",
    markdown: "html",
    uploadImage: true,
    uploadFile: true,
    reactions: false,
    typing: true,
    maxChunkChars: 3800,
    threads: true,
  };

  private readonly ctx: AdapterContext<TelegramConnection>;
  private readonly deps: TelegramDeps;
  private readonly signal: AbortSignal;
  private readonly controller = new AbortController();
  private readonly statePath: string;
  private readonly stateChain = new WriteChain();
  private readonly lastSendAt = new Map<string, number>();
  private readonly outbox = new Map<string, Promise<unknown>>();
  private state: TelegramState = { version: 1, cursor: null, seen: {} };
  private botId = "";
  private botUsername = "";
  private started = false;
  private fatal: Error | null = null;
  /** 429 暂停截止：按 chatId 隔离；GLOBAL_PAUSE_KEY 仅用于轮询侧无 chat 上下文的 429。 */
  private readonly pauseUntil = new Map<string, number>();
  private pollTask: Promise<void> | null = null;

  constructor(ctx: AdapterContext<TelegramConnection>, deps: TelegramDeps) {
    this.ctx = ctx;
    this.deps = deps;
    const forward = () => this.controller.abort();
    if (ctx.signal.aborted) forward();
    else ctx.signal.addEventListener("abort", forward, { once: true });
    this.signal = this.controller.signal;
    this.statePath = join(ctx.dataDir, "telegram-state.json");
  }

  // --------------------------------------------------------------- lifecycle

  async start(): Promise<void> {
    if (this.started) return;
    const token = (this.ctx.secrets.botToken ?? "").trim();
    if (!token) {
      throw new UserFacingError(
        "缺少 Bot Token：请在 Paseo → IM 的连接设置中填写 @BotFather 生成的 Bot Token 后重试。",
      );
    }
    if (!TOKEN_PATTERN.test(token)) {
      throw new UserFacingError(
        "Bot Token 格式不正确：应为「机器人ID:密钥」（示例：123456:ABC-DEF...）。请从 @BotFather 重新复制完整 Token。",
      );
    }
    this.started = true;
    this.state = (await readJson(this.statePath, parseState)) ?? this.state;
    await this.connect();
    this.pollTask = this.pollLoop();
  }

  async stop(): Promise<void> {
    this.controller.abort();
    this.deps.abort?.();
    const pollTask = this.pollTask;
    this.pollTask = null;
    if (pollTask) await pollTask.catch(() => {});
  }

  /** getMe 校验 + getWebhookInfo 冲突检查 + 游标初始化 + 命令菜单注册。 */
  private async connect(): Promise<void> {
    let attempt = 0;
    for (;;) {
      if (this.signal.aborted) throw new Error("连接已中止，Telegram 适配器无法启动");
      if (this.fatal) throw this.fatal;
      try {
        const me = await this.deps.api.call("getMe", {});
        if (
          !isRecord(me)
          || int(me.id) === undefined
          || me.is_bot !== true
        ) {
          throw new UserFacingError(
            "getMe 返回异常：该 Token 不属于任何机器人。请确认使用的是 @BotFather 签发的 Bot Token。",
          );
        }
        this.botId = String(int(me.id));
        this.botUsername = (str(me.username) ?? "").replace(/^@/, "").trim();
        if (!this.botUsername) {
          this.ctx.log("warn", "该 Bot 未返回 username：群聊 @提及 判定将不可用。");
        }
        const webhook = await this.deps.api.call("getWebhookInfo", {});
        const url = (isRecord(webhook) ? str(webhook.url) : undefined)?.trim() ?? "";
        if (url) {
          throw new UserFacingError(
            `该 Bot 已设置 Webhook（${url}），与长轮询模式冲突。请先调用 deleteWebhook（或在 BotFather 执行 /deletehook）后重试。`,
          );
        }
        if (this.state.cursor === null) {
          // 首次启动从最新一条 update 之后开始，避免重放历史消息。
          const latest = await this.deps.api.call("getUpdates", { offset: -1, timeout: 0, limit: 1 });
          const last = Array.isArray(latest) ? latest.at(-1) : undefined;
          const lastId = isRecord(last) ? int(last.update_id) : undefined;
          this.state.cursor = lastId !== undefined ? lastId + 1 : 0;
          await this.persistState();
        }
        try {
          await this.deps.api.call("setMyCommands", { commands: COMMAND_MENU });
        } catch (error) {
          this.ctx.log("warn", `注册 Telegram 命令菜单失败（不影响收发）：${asError(error).message}`);
        }
        return;
      } catch (error) {
        if (error instanceof UserFacingError) throw error;
        const status = statusOf(error);
        if (status === 401 || status === 403 || status === 409) {
          throw new UserFacingError(terminalMessage(status));
        }
        if (this.signal.aborted) throw new Error("连接已中止，Telegram 适配器无法启动");
        // 网络/传输层错误（fetch failed、DNS、超时）转用户可读文案
        const transient = asError(error);
        const networkish = /fetch failed|ECONN|ENOTFOUND|ETIMEDOUT|EAI_AGAIN|network/i.test(transient.message);
        this.ctx.onError(
          networkish
            ? new UserFacingError("无法连接 Telegram API（网络错误）。请检查本机网络对 api.telegram.org 的可达性，连接将自动重试。")
            : transient,
        );
        try {
          await this.sleep(BACKOFF_MS[Math.min(attempt, BACKOFF_MS.length - 1)]);
        } catch {
          throw new Error("连接已中止，Telegram 适配器无法启动");
        }
        attempt += 1;
      }
    }
  }

  private async pollLoop(): Promise<void> {
    let attempt = 0;
    while (!this.signal.aborted && !this.fatal) {
      let updates: unknown;
      try {
        updates = await this.deps.api.call("getUpdates", {
          offset: this.state.cursor ?? 0,
          timeout: POLL_TIMEOUT_S,
          limit: POLL_LIMIT,
          allowed_updates: ["message", "callback_query"],
        });
      } catch (error) {
        if (this.signal.aborted) return;
        const verdict = await this.handlePollFailure(error, attempt);
        if (verdict === "stop") return;
        if (verdict === "retry") attempt += 1;
        continue;
      }
      attempt = 0;
      if (this.signal.aborted || this.fatal) return;
      if (!Array.isArray(updates)) {
        this.ctx.log("warn", "Telegram getUpdates 返回了异常载荷，已忽略本轮结果");
        continue;
      }
      for (const update of updates) {
        if (this.signal.aborted || this.fatal) return;
        if (!isRecord(update)) continue;
        const updateId = int(update.update_id);
        if (updateId === undefined) continue;
        if (this.state.cursor !== null && updateId < this.state.cursor) continue;
        if (this.state.seen[String(updateId)] !== undefined) continue;
        try {
          await this.handleUpdate(update);
        } catch (error) {
          if (this.signal.aborted) return;
          this.ctx.log("warn", `处理 Telegram 更新 ${updateId} 失败：${asError(error).message}`);
        }
        this.state.cursor = updateId + 1;
        this.state.seen[String(updateId)] = Date.now();
        try {
          await this.persistState();
        } catch (error) {
          this.ctx.log("error", `持久化 Telegram 游标失败：${asError(error).message}`);
        }
      }
    }
  }

  /** 轮询失败的处置：stop=终止；immediate=429 全局暂停后立即重试；retry=退避重连。 */
  private async handlePollFailure(error: unknown, attempt: number): Promise<"stop" | "immediate" | "retry"> {
    const status = statusOf(error);
    if (status === 401 || status === 403 || status === 409) {
      this.fatal = new UserFacingError(terminalMessage(status));
      this.ctx.onError(this.fatal);
      return "stop";
    }
    const retryAfter = retryAfterOf(error);
    if (status === 429 && retryAfter !== undefined) {
      const waitMs = Math.min(retryAfter, RATE_LIMIT_WAIT_CAP_S) * 1000;
      this.pauseUntil.set(GLOBAL_PAUSE_KEY, Date.now() + waitMs); // getUpdates 无 chat 上下文：全局暂停
      try {
        await this.sleep(waitMs);
      } catch {
        return "stop";
      }
      return "immediate";
    }
    this.ctx.onError(asError(error));
    try {
      await this.sleep(BACKOFF_MS[Math.min(attempt, BACKOFF_MS.length - 1)]);
    } catch {
      return "stop";
    }
    return "retry";
  }

  private async handleUpdate(update: Obj): Promise<void> {
    const message = await normalizeUpdate(update, {
      botId: this.botId,
      botUsername: this.botUsername,
      connectionId: this.ctx.connectionId,
      loadMedia: (fileId, name) => this.storeMedia(fileId, name),
    });
    if (message) this.ctx.onMessage(message);
  }

  private async storeMedia(fileId: string, name: string): Promise<string> {
    // getFile+文件两段 fetch 任一瞬时故障都有界重试；耗尽仍失败才沿 handleUpdate
    // 上抛（轮询侧记录警告并推进游标，语义不变）。
    let bytes: Uint8Array;
    for (let attempt = 0; ; attempt += 1) {
      try {
        bytes = await this.deps.download(fileId);
        break;
      } catch (error) {
        if (attempt >= MEDIA_DOWNLOAD_RETRIES || this.signal.aborted) throw error;
        await this.sleep(MEDIA_DOWNLOAD_BACKOFF_MS[Math.min(attempt, MEDIA_DOWNLOAD_BACKOFF_MS.length - 1)]);
      }
    }
    const path = join(this.ctx.mediaDir, `${Date.now()}-${randomUUID().slice(0, 8)}-${sanitizeFileName(name)}`);
    await mkdir(this.ctx.mediaDir, { recursive: true });
    await writeFile(path, bytes);
    return path;
  }

  private persistState(): Promise<void> {
    return this.stateChain.run(async () => {
      pruneSeen(this.state);
      await atomicWrite(this.statePath, this.state);
    });
  }

  private async sleep(ms: number): Promise<void> {
    const signal = this.signal;
    if (signal.aborted) throw new Error("已中止");
    const { promise, reject } = Promise.withResolvers<never>();
    const onAbort = () => reject(new Error("已中止"));
    signal.addEventListener("abort", onAbort, { once: true });
    try {
      await Promise.race([this.deps.sleep(ms), promise]);
    } finally {
      signal.removeEventListener("abort", onAbort);
    }
  }

  // ------------------------------------------------------------------ outbound

  async sendText(outbound: OutboundText): Promise<SendResult> {
    const chunks = splitChunks(
      outbound.text ?? "",
      this.capabilities.maxChunkChars,
    ).filter((chunk) => chunk.trim().length > 0);
    let messageId: string | undefined;
    for (const [index, chunk] of chunks.entries()) {
      const payload: Record<string, unknown> = {
        chat_id: outbound.chatId,
        text: renderTelegramHtml(chunk),
        parse_mode: "HTML",
        link_preview_options: { is_disabled: true },
      };
      if (outbound.threadId) payload.message_thread_id = Number(outbound.threadId);
      if (index === 0 && outbound.replyToId) {
        payload.reply_parameters = {
          message_id: Number(outbound.replyToId),
          allow_sending_without_reply: true,
        };
      }
      const result = await this.send("sendMessage", outbound.chatId, payload);
      const sentId = resultMessageId(result);
      if (index === 0) messageId = sentId;
    }
    return { messageId };
  }

  async sendMedia(outbound: OutboundMedia): Promise<SendResult> {
    const bytes = await readFile(outbound.path);
    const isImage = outbound.kind === "image";
    const field = isImage ? "photo" : "document";
    const form = new FormData();
    form.append("chat_id", outbound.chatId);
    form.append(
      field,
      new Blob([bytes], {
        type: outbound.mime ?? (isImage ? "image/jpeg" : "application/octet-stream"),
      }),
      outbound.name ?? basename(outbound.path),
    );
    if (outbound.threadId) form.append("message_thread_id", outbound.threadId);
    if (outbound.caption?.trim()) {
      form.append("caption", renderTelegramHtml(outbound.caption));
      form.append("parse_mode", "HTML");
    }
    const result = await this.send(isImage ? "sendPhoto" : "sendDocument", outbound.chatId, form);
    return { messageId: resultMessageId(result) };
  }

  /** 进度占位编辑（8s 节流由 bridge 驱动）；幂等编辑视为成功。 */
  async editMessage(messageId: string, chatId: string, text: string): Promise<void> {
    const id = Number(messageId);
    if (!Number.isSafeInteger(id)) throw new Error(`无效的 Telegram 消息 ID：${messageId}`);
    const payload = {
      chat_id: chatId,
      message_id: id,
      text: renderTelegramHtml(text),
      parse_mode: "HTML",
      link_preview_options: { is_disabled: true },
    };
    try {
      await this.deps.api.call("editMessageText", payload);
    } catch (error) {
      if (/message is not modified/i.test(asError(error).message)) return;
      throw error;
    }
  }

  async sendTyping(chatId: string): Promise<void> {
    try {
      await this.deps.api.call("sendChatAction", { chat_id: chatId, action: "typing" });
    } catch {
      // typing 提示失败不影响消息收发。
    }
  }

  async isAdmin(chatId: string, senderId: string): Promise<boolean> {
    try {
      const member = await this.deps.api.call("getChatMember", {
        chat_id: chatId,
        user_id: Number(senderId),
      });
      if (!isRecord(member)) return false;
      return member.status === "administrator" || member.status === "creator";
    } catch {
      return false;
    }
  }

  async leaveChat(chatId: string): Promise<void> {
    await this.deps.api.call("leaveChat", { chat_id: chatId });
  }

  async testConnection(): Promise<{ ok: boolean; detail?: string }> {
    const token = (this.ctx.secrets.botToken ?? "").trim();
    if (!token) return { ok: false, detail: "缺少 Bot Token：请在连接设置中填写 @BotFather 生成的 Token。" };
    if (!TOKEN_PATTERN.test(token)) {
      return { ok: false, detail: "Bot Token 格式不正确：应为「机器人ID:密钥」（示例：123456:ABC-DEF...）。" };
    }
    try {
      const me = await this.deps.api.call("getMe", {});
      if (!isRecord(me) || int(me.id) === undefined || me.is_bot !== true) {
        return { ok: false, detail: "该 Token 不属于任何机器人，请核对 Bot Token。" };
      }
      const username = (str(me.username) ?? "").replace(/^@/, "");
      const webhook = await this.deps.api.call("getWebhookInfo", {});
      const url = (isRecord(webhook) ? str(webhook.url) : undefined)?.trim() ?? "";
      if (url) {
        return { ok: false, detail: `该 Bot 已设置 Webhook（${url}），与长轮询冲突，请先执行 deleteWebhook。` };
      }
      return {
        ok: true,
        detail: username ? `已连接 @${username}（ID ${String(int(me.id))}）` : `已连接（ID ${String(int(me.id))}）`,
      };
    } catch (error) {
      if (statusOf(error) === 401) {
        return { ok: false, detail: "Bot Token 无效或已被撤销（401），请在 @BotFather 重新获取。" };
      }
      return { ok: false, detail: `连接 Telegram 失败：${asError(error).message}` };
    }
  }

  // -------------------------------------------------------------- rate limits

  /** 同一 chat 的发送串行执行，并遵守 429 暂停（按 chat 隔离，仅轮询侧 429 全局）
   * 与每 chat 间隔（私聊 1100ms、群聊 3000ms）。 */
  private send(method: string, chatId: string, payload: unknown): Promise<unknown> {
    const tail = this.outbox.get(chatId) ?? Promise.resolve();
    const task = tail.then(
      () => this.deliver(method, chatId, payload),
      () => this.deliver(method, chatId, payload),
    );
    this.outbox.set(chatId, task.catch(() => {}));
    return task;
  }

  private async deliver(method: string, chatId: string, payload: unknown): Promise<unknown> {
    for (let attempt = 1; ; attempt += 1) {
      await this.awaitSendPause(chatId);
      await this.awaitChatInterval(chatId);
      try {
        const result = await this.deps.api.call(method, payload);
        this.lastSendAt.set(chatId, Date.now());
        return result;
      } catch (error) {
        if (this.signal.aborted) throw error;
        const status = statusOf(error);
        if (status === 401 || status === 403 || status === 409) {
          this.fail(error);
          throw error;
        }
        const retryAfter = retryAfterOf(error);
        if (status === 429 && retryAfter !== undefined && attempt < SEND_ATTEMPTS) {
          // 只暂停出错的 chat：一个 chat 被 429 不应饿死其它会话。
          this.pauseUntil.set(chatId, Date.now() + Math.min(retryAfter, RATE_LIMIT_WAIT_CAP_S) * 1000);
          continue;
        }
        throw error;
      }
    }
  }

  private async awaitSendPause(chatId: string): Promise<void> {
    const now = Date.now();
    let waitMs = 0;
    for (const key of [GLOBAL_PAUSE_KEY, chatId]) {
      const until = this.pauseUntil.get(key);
      if (until === undefined) continue;
      if (until <= now) {
        this.pauseUntil.delete(key); // 过期条目回收，避免 Map 随 chat 数无界增长
        continue;
      }
      waitMs = Math.max(waitMs, until - now);
    }
    if (waitMs > 0) await this.sleep(waitMs);
  }

  private async awaitChatInterval(chatId: string): Promise<void> {
    const last = this.lastSendAt.get(chatId);
    if (last === undefined) return;
    // 群聊（chatId 为负数）限 20 条/分钟 → 3000ms；私聊维持 1100ms。
    const intervalMs = chatId.startsWith("-") ? GROUP_SEND_INTERVAL_MS : SEND_INTERVAL_MS;
    const waitMs = last + intervalMs - Date.now();
    if (waitMs > 0) await this.sleep(waitMs);
  }

  /** 401/403/409：终止连接（轮询退出）并上报一次。 */
  private fail(error: unknown): void {
    if (this.fatal) return;
    const status = statusOf(error);
    this.fatal =
      status === 401 || status === 403 || status === 409
        ? new UserFacingError(terminalMessage(status))
        : asError(error);
    this.ctx.onError(this.fatal);
  }
}

function pruneSeen(state: TelegramState): void {
  const now = Date.now();
  for (const [key, at] of Object.entries(state.seen)) {
    if (now - at > SEEN_TTL_MS) delete state.seen[key];
  }
  const entries = Object.entries(state.seen).sort((a, b) => a[1] - b[1]);
  for (const [key] of entries.slice(0, Math.max(0, entries.length - SEEN_MAX))) {
    delete state.seen[key];
  }
}

export function createAdapter(
  ctx: AdapterContext<TelegramConnection>,
  deps: TelegramDeps = defaultDeps(ctx),
): ChannelAdapter {
  return new TelegramAdapter(ctx, deps);
}

// ---------------------------------------------------------------- definition

/** 连接 schema 要求 telegram 携带 botUserId/username：botUserId 取 Token 前缀
 *（Bot API 的 Token 结构即「机器人ID:密钥」）；username 只有 getMe 能确认，
 * 保存阶段写入占位值，适配器每次 start 都以 getMe 的实际 username 做提及判定。 */
export function configureTelegram(fields: Record<string, string>): ConfiguredFields {
  requireFields(CHANNEL_SPECS.telegram.fields, fields);
  const { config, secrets } = defaultConfigure("telegram", fields);
  const token = secrets.botToken ?? "";
  const separator = token.indexOf(":");
  const botUserId = separator > 0 ? token.slice(0, separator) : "";
  if (!/^\d{5,20}$/.test(botUserId)) {
    throw new UserFacingError(
      "Bot Token 格式不正确：应为「机器人ID:密钥」（示例：123456:ABC-DEF...）。请从 @BotFather 复制完整 Token。",
    );
  }
  const username = str(config.username) ?? "";
  if (username && !/^[A-Za-z0-9_]{4,64}$/.test(username)) {
    throw new UserFacingError("Telegram 用户名格式不正确：应为 4-64 位字母、数字或下划线。");
  }
  return {
    config: { ...config, botUserId, username: username || "telegram_bot" },
    secrets,
  };
}

export const definition: ChannelDefinition<TelegramConnection> = {
  channel: "telegram",
  create: (ctx) => createAdapter(ctx),
  configure: configureTelegram,
};
