/** DingTalk（钉钉）渠道适配器：dingtalk-stream 长连接接收机器人消息，
 * sessionWebhook / 机器人 REST 接口回复、媒体上下行与 AI Card 流式进度。
 * Protocol facts ported from xmanrui/dsh-im (MIT). */
import { randomUUID } from "node:crypto";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { basename, join } from "node:path";
import { z } from "zod";
import { DWClient, TOPIC_ROBOT } from "dingtalk-stream";
import { CHANNEL_SPECS } from "../../../shared/channels.ts";
import { UserFacingError } from "../../../shared/errors.ts";
import type { Connection } from "../../../shared/config.ts";
import type {
  ChannelCapabilities,
  NormalizedMedia,
  NormalizedMessage,
  OutboundMedia,
  OutboundText,
  SendResult,
} from "../../../shared/model.ts";
import { splitChunks } from "../../format/chunks.ts";
import { atomicWrite, readJson, sleep, WriteChain } from "../../files.ts";
import { defaultConfigure, requireFields } from "../configure.ts";
import type { AdapterContext, ChannelAdapter, ChannelDefinition, ConfiguredFields } from "../types.ts";
import {
  DINGTALK_API_BASE,
  DINGTALK_OAPI_BASE,
  DingTalkApi,
  DingTalkApiError,
  asText,
  cardMarkdown,
  isRecord,
  str,
  toNumber,
  type CardTarget,
  type DingTalkFrame,
  type DingtalkDeps,
  type DWClientLike,
  type DWDownStream,
  type RobotTarget,
} from "./api.ts";

export { DingTalkApiError } from "./api.ts";

const MAX_CHUNK_CHARS = 4000;
/** 断线退避：瞬时错误重连间隔，直到成功或停止。 */
const BACKOFF_MS = [1000, 3000, 5000, 10000, 30000];
const MONITOR_INTERVAL_MS = 5000;
/** sessionWebhook 被动窗口约 5 分钟；提前 60s 判定过期走机器人接口。 */
const WEBHOOK_MARGIN_MS = 60_000;
const SEEN_CAP = 1000;
const STATE_FILE = "dingtalk-state.json";
const CARD_THINKING_TEXT = "正在思考…";

export type DingTalkConnection = Extract<Connection, { channel: "dingtalk" }>;

const conversationRecordSchema = z
  .object({
    kind: z.enum(["dm", "group"]),
    robotCode: z.string().max(128).optional(),
    webhook: z.string().max(2048).optional(),
    webhookExpiresAt: z.number().optional(),
    senderId: z.string().max(128).optional(),
  })
  .strict();

const adapterStateSchema = z
  .object({
    seen: z.array(z.string().max(256)).max(SEEN_CAP).default([]),
    conversations: z.record(z.string().max(128), conversationRecordSchema).default({}),
  })
  .strict();

type AdapterState = z.output<typeof adapterStateSchema>;

export interface ConversationRecord {
  kind: "dm" | "group";
  robotCode?: string;
  webhook?: string;
  webhookExpiresAt?: number;
  senderId: string;
}

/** 待下载的媒体引用（帧内自带的地址或需换取的 downloadCode）。 */
export interface DingTalkMediaRef {
  kind: "image" | "file";
  name?: string;
  downloadUrl?: string;
  downloadCode?: string;
  robotCode?: string;
}

export type FrameOutcome =
  | { action: "skip"; reason: string; warn?: boolean }
  | {
      action: "emit";
      message: NormalizedMessage;
      media: DingTalkMediaRef[];
      conversation: ConversationRecord;
    };

export function isFrameEmit(
  outcome: FrameOutcome,
): outcome is Extract<FrameOutcome, { action: "emit" }> {
  return outcome.action === "emit";
}

function stripLeadingMentions(text: string | undefined): string | undefined {
  if (!text) return undefined;
  const stripped = text.replace(/^(?:@[^@\s]{1,64}(?:\s+|$))+/, "").trim();
  return stripped || undefined;
}

interface RichTextEntry {
  type: "text" | "picture";
  text?: string;
  downloadCode?: string;
  downloadUrl?: string;
}

function richTextEntries(content: Record<string, unknown>): RichTextEntry[] {
  const raw = content.richText ?? content.rich_text;
  if (!Array.isArray(raw)) return [];
  const entries: RichTextEntry[] = [];
  for (const item of raw) {
    if (!isRecord(item)) continue;
    const type = (str(item.type) ?? "text").toLowerCase();
    if (type === "picture") {
      const downloadCode = str(item.downloadCode) ?? str(item.download_code);
      if (downloadCode) {
        entries.push({
          type: "picture",
          downloadCode,
          downloadUrl: str(item.downloadUrl) ?? str(item.download_url),
        });
      }
    } else {
      const text = asText(isRecord(item.text) ? item.text.content : undefined) ?? str(item.content);
      if (text) entries.push({ type: "text", text });
    }
  }
  return entries;
}

function repliedTextOf(replied: Record<string, unknown>): string | undefined {
  const direct = asText(isRecord(replied.text) ? replied.text.content : undefined);
  if (direct) return direct;
  const raw = replied.content;
  if (typeof raw === "string") {
    try {
      const parsed = JSON.parse(raw) as unknown;
      if (isRecord(parsed)) {
        return asText(isRecord(parsed.text) ? parsed.text.content : undefined) ?? str(parsed.content);
      }
    } catch {
      return str(raw);
    }
    return undefined;
  }
  if (isRecord(raw)) {
    return asText(isRecord(raw.text) ? raw.text.content : undefined) ?? str(raw.content);
  }
  return undefined;
}

/** 群聊 @ 判定：isAtAll / isInAtList（真实帧中机器人命中 @ 列表的权威标记），
 * 或 atUsers 的 dingtalkId 比对 clientId。 */
function isBotMentioned(frame: DingTalkFrame, clientId: string): boolean {
  if (frame.isAtAll === true || frame.isInAtList === true) return true;
  if (!Array.isArray(frame.atUsers)) return false;
  return frame.atUsers.some(
    (user) => isRecord(user) && str(user.dingtalkId) === clientId,
  );
}

/** 真实入站帧形状 → 规范化消息；机器人自身消息与无文本无媒体噪音返回 skip。 */
export function normalizeFrame(
  frame: DingTalkFrame,
  context: { connectionId: string; clientId: string },
): FrameOutcome {
  const chatId = str(frame.conversationId);
  if (!chatId) return { action: "skip", reason: "缺少会话 ID" };
  const type = str(frame.conversationType);
  const kind = type === "1" ? ("dm" as const) : type === "2" ? ("group" as const) : undefined;
  if (!kind) return { action: "skip", reason: `未知的会话类型：${type ?? "空"}`, warn: true };

  const robotCode = str(frame.robotCode);
  const senderId = str(frame.senderStaffId) ?? str(frame.senderId);
  if (!senderId) return { action: "skip", reason: "缺少发送者 ID" };

  const botId = str(frame.chatbotUserId);
  if (botId && (frame.senderId === botId || frame.senderStaffId === botId)) {
    return { action: "skip", reason: "机器人自身消息" };
  }

  const msgtype = (str(frame.msgtype) ?? "text").toLowerCase();
  if (msgtype !== "text" && msgtype !== "picture" && msgtype !== "file" && msgtype !== "richtext") {
    return { action: "skip", reason: `暂不支持的钉钉消息类型：${msgtype}`, warn: true };
  }

  const content = isRecord(frame.content) ? frame.content : {};
  const downloadUrl = str(frame.messageFileDownloadUrl);
  const media: DingTalkMediaRef[] = [];
  let text: string | undefined;

  if (msgtype === "picture") {
    media.push({
      kind: "image",
      name: str(content.fileName) ?? str(content.file_name),
      downloadUrl,
      downloadCode: str(content.downloadCode) ?? str(content.download_code),
      robotCode,
    });
  } else if (msgtype === "file") {
    media.push({
      kind: "file",
      name: str(content.fileName) ?? str(content.file_name),
      downloadUrl,
      downloadCode: str(content.downloadCode) ?? str(content.download_code),
      robotCode,
    });
  } else if (msgtype === "richtext") {
    const entries = richTextEntries(content);
    const parts: string[] = [];
    let imageIndex = 0;
    for (const entry of entries) {
      if (entry.type === "picture") {
        imageIndex += 1;
        media.push({
          kind: "image",
          name: `dingtalk-image-${imageIndex}`,
          downloadUrl: entry.downloadUrl,
          downloadCode: entry.downloadCode,
          robotCode,
        });
      } else if (entry.text) {
        parts.push(entry.text);
      }
    }
    text = parts.length > 0 ? parts.join("\n") : asText(frame.text?.content);
  } else {
    text = asText(frame.text?.content);
  }

  text = stripLeadingMentions(text);
  const hasMedia = media.length > 0;
  if (!text && !hasMedia) return { action: "skip", reason: "空消息（没有文本或媒体）" };

  let replyTo: NormalizedMessage["replyTo"];
  let replyToBot = false;
  const replied = isRecord(frame.text?.repliedMsg) ? frame.text.repliedMsg : undefined;
  if (replied) {
    const repliedSender = str(replied.senderStaffId) ?? str(replied.senderId) ?? str(replied.sender);
    replyToBot = !!repliedSender && (repliedSender === robotCode || repliedSender === context.clientId);
    replyTo = {
      id: str(replied.msgId) ?? str(replied.messageId),
      text: repliedTextOf(replied),
      sender: str(replied.senderNick) ?? str(replied.senderName) ?? repliedSender,
    };
  }

  const created = toNumber(frame.createAt);
  const timestamp = Number.isFinite(created) && created > 0 ? created : Date.now();
  const message: NormalizedMessage = {
    channel: "dingtalk",
    connectionId: context.connectionId,
    kind,
    chatId,
    senderId,
    senderName: str(frame.senderNick),
    messageId: str(frame.msgId) ?? `dingtalk-${chatId}-${senderId}-${timestamp}`,
    timestamp,
    text,
    replyTo,
    trigger: {
      mention: kind === "dm" || isBotMentioned(frame, context.clientId),
      replyToBot,
      command: !!text && text.startsWith("/"),
    },
  };

  const expiresAt = toNumber(frame.sessionWebhookExpiredTime);
  return {
    action: "emit",
    message,
    media,
    conversation: {
      kind,
      robotCode,
      webhook: str(frame.sessionWebhook),
      webhookExpiresAt: Number.isFinite(expiresAt) && expiresAt > 0 ? expiresAt : undefined,
      senderId,
    },
  };
}

function definedEntries<T extends object>(value: T): T {
  return Object.fromEntries(
    Object.entries(value).filter(([, entry]) => entry !== undefined),
  ) as T;
}

const MIME_BY_EXT: Record<string, string> = {
  png: "image/png",
  jpg: "image/jpeg",
  jpeg: "image/jpeg",
  gif: "image/gif",
  webp: "image/webp",
  bmp: "image/bmp",
  pdf: "application/pdf",
  txt: "text/plain",
  md: "text/markdown",
  csv: "text/csv",
  json: "application/json",
  doc: "application/msword",
  docx: "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
  xls: "application/vnd.ms-excel",
  xlsx: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
  ppt: "application/vnd.ms-powerpoint",
  pptx: "application/vnd.openxmlformats-officedocument.presentationml.presentation",
  zip: "application/zip",
  mp3: "audio/mpeg",
  mp4: "video/mp4",
};

function safeFileName(name: string): string {
  const cleaned = basename(name).replace(/[\\/:*?"<>|\u0000-\u001f]/g, "_").trim();
  return cleaned || "file";
}

function defaultMediaName(kind: "image" | "file"): string {
  return kind === "image" ? "dingtalk.png" : "dingtalk.bin";
}

function mimeFromName(name: string): string | undefined {
  const index = name.lastIndexOf(".");
  return index > 0 ? MIME_BY_EXT[name.slice(index + 1).toLowerCase()] : undefined;
}

function asError(error: unknown): Error {
  return error instanceof Error ? error : new Error(String(error));
}

function streamOpen(dw: DWClientLike): boolean {
  return dw.connected === true || dw.socket?.readyState === 1;
}

const CONNECT_POLL_INTERVAL_MS = 50;
const CONNECT_WAIT_TIMEOUT_MS = 5_000;

/** connect() 可能在 socket open 前就 resolve：轮询就绪标记直至开口或超时。 */
async function waitUntilOpen(dw: DWClientLike, signal: AbortSignal): Promise<boolean> {
  const deadline = Date.now() + CONNECT_WAIT_TIMEOUT_MS;
  while (!streamOpen(dw)) {
    if (signal.aborted || Date.now() >= deadline) return false;
    await sleep(CONNECT_POLL_INTERVAL_MS, signal);
  }
  return true;
}

async function readDingTalkResponse(response: Response): Promise<unknown> {
  const raw = await response.text();
  let parsed: unknown;
  if (raw) {
    try {
      parsed = JSON.parse(raw);
    } catch {
      parsed = undefined;
    }
  }
  if (!response.ok) {
    const record = isRecord(parsed) ? parsed : {};
    const detail = str(record.errmsg) ?? str(record.message);
    const provider = record.errcode ?? record.code;
    throw new DingTalkApiError(`钉钉服务请求失败（HTTP ${response.status}${detail ? `：${detail}` : ""}）。`, {
      status: response.status,
      providerCode: typeof provider === "string" || typeof provider === "number" ? provider : undefined,
    });
  }
  if (isRecord(parsed) && parsed.errcode !== undefined && toNumber(parsed.errcode) !== 0) {
    const errcode = parsed.errcode;
    throw new DingTalkApiError(`钉钉服务拒绝了请求：${str(parsed.errmsg) ?? String(errcode)}。`, {
      status: response.status,
      providerCode: typeof errcode === "string" || typeof errcode === "number" ? errcode : undefined,
    });
  }
  return parsed;
}

/** 生产传输层：fetch 实现 REST/webhook/媒体下载，dingtalk-stream 实现 Stream。 */
export function defaultDeps(ctx: AdapterContext<DingTalkConnection>): DingtalkDeps {
  return {
    async rest(path, body, base = DINGTALK_API_BASE, method = "POST", token) {
      const url = new URL(path.replace(/^\/+/, ""), base);
      const form = body instanceof FormData;
      const headers: Record<string, string> = {};
      if (token) headers["x-acs-dingtalk-access-token"] = token;
      if (!form) headers["content-type"] = "application/json";
      const response = await fetch(url, {
        method,
        headers,
        body: form ? body : body === undefined ? undefined : JSON.stringify(body),
        signal: ctx.signal,
        redirect: "error",
      });
      return readDingTalkResponse(response);
    },
    async webhook(url, body) {
      const response = await fetch(url, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify(body),
        signal: ctx.signal,
        redirect: "error",
      });
      return readDingTalkResponse(response);
    },
    createDw(config) {
      // autoReconnect 关闭：断线由适配器自管 [1,3,5,10,30]s 退避重连（SDK 类型
      // 未声明该键，运行时合并进默认配置生效）。
      const options = {
        clientId: config.clientId,
        clientSecret: config.clientSecret,
        autoReconnect: false,
        keepAlive: true,
        debug: false,
      };
      // DWClient 带私有成员且回调签名与注入接口不一致，经 unknown 收窄。
      return new DWClient(options) as unknown as DWClientLike;
    },
    fetchUrl: (url) => fetch(url, { redirect: "follow", signal: ctx.signal }),
  };
}

/** DingTalk 渠道适配器：Stream 接收 + webhook/机器人接口发送 + AI 卡片流式进度。 */
export class DingtalkAdapter implements ChannelAdapter {
  readonly channel = "dingtalk" as const;
  readonly capabilities: ChannelCapabilities = {
    progress: "stream",
    markdown: "plain",
    uploadImage: true,
    uploadFile: true,
    reactions: false,
    typing: false,
    maxChunkChars: MAX_CHUNK_CHARS,
    threads: false,
  };

  private readonly ctx: AdapterContext<DingTalkConnection>;
  private readonly deps: DingtalkDeps;
  private readonly api: DingTalkApi;
  private readonly statePath: string;
  private readonly chain = new WriteChain();
  private readonly tasks = new Set<Promise<void>>();
  /** 在途卡片流：broken 表示卡片不可用已降级，最终回复直接发文本。 */
  private readonly streams = new Map<string, { chatId: string; broken: boolean }>();
  private readonly internal = new AbortController();
  private readonly onOuterAbort: () => void;
  private state: AdapterState = adapterStateSchema.parse({});
  private dw: DWClientLike | undefined;
  private monitor: NodeJS.Timeout | undefined;
  private ensureRunning = false;
  private backoffIndex = 0;
  private stopped = false;

  constructor(ctx: AdapterContext<DingTalkConnection>, deps: DingtalkDeps) {
    this.ctx = ctx;
    this.deps = deps;
    this.api = new DingTalkApi(deps, ctx.config.clientId, ctx.secrets.clientSecret ?? "", ctx.signal);
    this.statePath = join(ctx.dataDir, STATE_FILE);
    this.onOuterAbort = () => this.internal.abort(new Error("钉钉连接已停止"));
    ctx.signal.addEventListener("abort", this.onOuterAbort, { once: true });
  }

  /** 等待全部在途回调处理完成（优雅停机与测试用）。 */
  async settle(): Promise<void> {
    while (this.tasks.size > 0) {
      await Promise.allSettled([...this.tasks]);
    }
  }

  async start(): Promise<void> {
    if (this.dw) return;
    const clientId = this.ctx.config.clientId?.trim();
    const clientSecret = this.ctx.secrets.clientSecret?.trim();
    if (!clientId) {
      throw new UserFacingError(
        "钉钉 Client ID 未配置：请在连接设置中填写开放平台企业内部应用的 Client ID（AppKey）。",
      );
    }
    if (!clientSecret) {
      throw new UserFacingError(
        "钉钉 Client Secret 未配置：请在连接设置中填写应用的 Client Secret（AppSecret）。",
      );
    }
    try {
      const stored = await readJson(this.statePath, adapterStateSchema.parse);
      if (stored) this.state = stored;
    } catch (error) {
      // 状态文件损坏不阻断启动：重置空状态并告警（对齐飞书）。
      this.state = adapterStateSchema.parse({});
      this.ctx.log("warn", `钉钉适配器状态文件损坏，已重置去重记录：${asError(error).message}`);
    }

    const dw = this.deps.createDw({ clientId, clientSecret });
    this.dw = dw;
    dw.registerCallbackListener(TOPIC_ROBOT, (downstream) => this.onDownStream(dw, downstream));
    try {
      await dw.connect();
      this.backoffIndex = 0;
    } catch (error) {
      // 瞬时错误交给后台退避重连，不阻塞管理器启动。
      this.ctx.onError(asError(error));
      void this.ensureConnected();
    }
    this.monitor = setInterval(() => void this.monitorTick(), this.deps.monitorIntervalMs ?? MONITOR_INTERVAL_MS);
    this.monitor.unref?.();
  }

  async stop(): Promise<void> {
    this.stopped = true;
    this.internal.abort(new Error("钉钉连接已停止"));
    this.ctx.signal.removeEventListener("abort", this.onOuterAbort);
    if (this.monitor) {
      clearInterval(this.monitor);
      this.monitor = undefined;
    }
    const dw = this.dw;
    this.dw = undefined;
    this.streams.clear();
    try {
      dw?.disconnect();
    } catch (error) {
      this.ctx.log("warn", `钉钉连接断开失败：${asError(error).message}`);
    }
    if (this.tasks.size > 0) await Promise.allSettled([...this.tasks]);
  }

  private onDownStream(dw: DWClientLike, downstream: DWDownStream): { status: string; message: string } {
    const result = { status: "success", message: "OK" };
    const messageId = str(downstream?.headers?.messageId);
    if (messageId) {
      try {
        dw.socketCallBackResponse?.(messageId, result);
      } catch {
        // 应答失败不阻断消息处理。
      }
    }
    const task = this.consume(downstream).finally(() => {
      this.tasks.delete(task);
    });
    this.tasks.add(task);
    return result;
  }

  private async consume(downstream: DWDownStream): Promise<void> {
    let frame: unknown;
    try {
      frame = typeof downstream?.data === "string" ? JSON.parse(downstream.data) : downstream?.data;
    } catch {
      this.ctx.log("warn", "忽略无法解析的钉钉回调载荷。");
      return;
    }
    if (!isRecord(frame)) return;
    try {
      await this.acceptFrame(frame as DingTalkFrame);
    } catch (error) {
      if (!this.internal.signal.aborted) this.ctx.onError(asError(error));
    }
  }

  /** 单帧处理：normalize → 去重/会话记录持久化 → 媒体下载 → emit。 */
  async acceptFrame(frame: DingTalkFrame): Promise<void> {
    const outcome = normalizeFrame(frame, {
      connectionId: this.ctx.connectionId,
      clientId: this.ctx.config.clientId,
    });
    if (outcome.action === "skip") {
      if (outcome.warn) this.ctx.log("warn", `忽略钉钉消息：${outcome.reason}`);
      return;
    }
    const { message, media, conversation } = outcome;
    if (this.isSeen(message.messageId)) return;
    this.markSeen(message.messageId);
    const previous = this.state.conversations[message.chatId];
    this.state.conversations[message.chatId] = { ...previous, ...definedEntries(conversation) };
    await this.persist();

    const downloaded: NormalizedMedia[] = [];
    if (media.length > 0) {
      const settings = await this.ctx.settings();
      const capBytes = settings.imageMaxDownloadMb * 1024 * 1024;
      for (const ref of media) {
        try {
          const stored = await this.downloadMedia(ref, message.messageId, capBytes);
          if (stored) downloaded.push(stored);
        } catch (error) {
          this.ctx.log("warn", `钉钉媒体下载失败：${asError(error).message}`);
        }
      }
    }
    if (!message.text && downloaded.length === 0) return;
    this.ctx.onMessage({
      ...message,
      ...(downloaded.length > 0 ? { media: downloaded } : {}),
    });
  }

  async sendText(outbound: OutboundText): Promise<SendResult> {
    let result: SendResult = {};
    for (const chunk of splitChunks(outbound.text, this.capabilities.maxChunkChars)) {
      result = await this.sendChunk(outbound.chatId, chunk);
    }
    return result;
  }

  async sendMedia(outbound: OutboundMedia): Promise<SendResult> {
    const kind = outbound.kind === "image" ? ("image" as const) : ("file" as const);
    const bytes = await readFile(outbound.path).catch((error: NodeJS.ErrnoException) => {
      throw new UserFacingError(`无法读取待发送的${kind === "image" ? "图片" : "文件"} ${outbound.path}：${error.message}`);
    });
    const name = safeFileName(outbound.name ?? basename(outbound.path));
    const mediaId = await this.api.uploadMedia(kind, bytes, name, outbound.mime);
    await this.deliverMedia(outbound.chatId, kind, mediaId, name);
    return {};
  }

  async streamStart(chatId: string): Promise<{ streamId: string }> {
    const fallbackId = `paseo_broken_${randomUUID()}`;
    try {
      const target = this.cardTarget(chatId);
      const streamId = await this.api.createCard(target, cardMarkdown(CARD_THINKING_TEXT));
      this.streams.set(streamId, { chatId, broken: false });
      return { streamId };
    } catch (error) {
      // AI 卡片不可用（无会话记录或卡片接口失败）时降级为无进度的普通回复：
      // 记录 broken 流不抛错，投递分支照常执行，finish 直接 sendText。
      this.ctx.log("warn", `钉钉 AI 卡片不可用，将直接发送最终回复：${asError(error).message}`);
      this.streams.set(fallbackId, { chatId, broken: true });
      return { streamId: fallbackId };
    }
  }

  async streamAppend(streamId: string, text: string): Promise<void> {
    const record = this.streams.get(streamId);
    if (!record) throw new Error("未知的钉钉卡片流。");
    if (record.broken) return; // 降级流没有卡片可更新。
    await this.api.updateCard(streamId, cardMarkdown(text));
  }

  async streamFinish(streamId: string, text: string): Promise<{ messageId?: string }> {
    const record = this.streams.get(streamId);
    if (!record) throw new Error("未知的钉钉卡片流。");
    try {
      if (record.broken) {
        const content = text.trim();
        return content ? await this.sendText({ chatId: record.chatId, text: content }) : {};
      }
      await this.api.finishCard(streamId, cardMarkdown(text));
      return { messageId: streamId };
    } finally {
      // 无论收口成败都移除在途记录，防止失败后残留泄漏。
      this.streams.delete(streamId);
    }
  }

  async testConnection(): Promise<{ ok: boolean; detail?: string }> {
    try {
      await this.api.token();
      return { ok: true, detail: "钉钉凭据校验通过。" };
    } catch (error) {
      return { ok: false, detail: asError(error).message };
    }
  }

  private async sendChunk(chatId: string, content: string): Promise<SendResult> {
    if (!content.trim()) return {};
    const webhook = this.webhookUrl(chatId);
    if (webhook) {
      try {
        await this.api.sendWebhookText(webhook, content);
        return {};
      } catch (error) {
        if (this.ctx.signal.aborted) throw error;
        this.ctx.log("warn", `钉钉 webhook 回复失败，改用机器人接口：${asError(error).message}`);
      }
    }
    await this.api.robotSendText(this.robotTarget(chatId), content);
    return {};
  }

  private async deliverMedia(
    chatId: string,
    kind: "image" | "file",
    mediaId: string,
    name: string,
  ): Promise<void> {
    const webhook = this.webhookUrl(chatId);
    if (webhook) {
      try {
        await this.api.sendWebhookMedia(webhook, kind, mediaId, name);
        return;
      } catch (error) {
        if (this.ctx.signal.aborted) throw error;
        this.ctx.log("warn", `钉钉 webhook 媒体发送失败，改用机器人接口：${asError(error).message}`);
      }
    }
    await this.api.robotSendMedia(this.robotTarget(chatId), kind, mediaId, name);
  }

  private webhookUrl(chatId: string): string | undefined {
    const record = this.state.conversations[chatId];
    const webhook = record?.webhook;
    if (!webhook) return undefined;
    const expiresAt = record.webhookExpiresAt;
    if (expiresAt && this.now() >= expiresAt - WEBHOOK_MARGIN_MS) return undefined;
    return webhook;
  }

  private robotTarget(chatId: string): RobotTarget {
    const record = this.state.conversations[chatId];
    const robotCode = record?.robotCode || this.ctx.config.clientId;
    if (record?.kind === "group") {
      return { kind: "group", chatId, robotCode };
    }
    const userId = record?.senderId;
    if (!userId) {
      throw new UserFacingError("无法确定钉钉接收用户：请先在钉钉里向机器人发一条消息。");
    }
    return { kind: "dm", chatId, userId, robotCode };
  }

  private cardTarget(chatId: string): CardTarget {
    const record = this.state.conversations[chatId];
    const robotCode = record?.robotCode || this.ctx.config.clientId;
    if (record?.kind === "group") {
      return { type: "group", openConversationId: chatId, robotCode };
    }
    const userId = record?.senderId;
    if (!userId) {
      throw new UserFacingError("无法确定钉钉卡片接收用户：请先在钉钉里向机器人发一条消息。");
    }
    return { type: "user", userId, robotCode };
  }

  private async downloadMedia(
    ref: DingTalkMediaRef,
    messageId: string,
    capBytes: number,
  ): Promise<NormalizedMedia | undefined> {
    let url = ref.downloadUrl;
    if (!url && ref.downloadCode) {
      url = await this.api.downloadUrl(ref.downloadCode, ref.robotCode || this.ctx.config.clientId);
    }
    if (!url) throw new DingTalkApiError("钉钉没有提供可用的媒体下载地址。");
    const fetchUrl =
      this.deps.fetchUrl ?? ((target: string) => fetch(target, { redirect: "follow", signal: this.ctx.signal }));
    const response = await fetchUrl(url);
    if (!response.ok) {
      throw new DingTalkApiError(`钉钉媒体下载失败（HTTP ${response.status}）。`);
    }
    const bytes = Buffer.from(await response.arrayBuffer());
    if (capBytes > 0 && bytes.byteLength > capBytes) {
      this.ctx.log("warn", `钉钉媒体超过大小上限（${bytes.byteLength} 字节），已跳过。`);
      return undefined;
    }
    const name = safeFileName(ref.name ?? defaultMediaName(ref.kind));
    await mkdir(this.ctx.mediaDir, { recursive: true, mode: 0o700 });
    // message_id 前缀防同名覆盖（参照飞书），展示名保持原始文件名。
    const path = join(this.ctx.mediaDir, `${messageId}-${name}`);
    await writeFile(path, bytes, { mode: 0o600 });
    return { kind: ref.kind, path, mime: mimeFromName(name), name };
  }

  private async monitorTick(): Promise<void> {
    if (this.stopped || this.internal.signal.aborted || this.ensureRunning) return;
    const dw = this.dw;
    if (!dw || streamOpen(dw)) return;
    void this.ensureConnected();
  }

  private async ensureConnected(): Promise<void> {
    if (this.ensureRunning || this.internal.signal.aborted) return;
    this.ensureRunning = true;
    try {
      const dw = this.dw;
      if (!dw) return;
      while (!this.internal.signal.aborted && !streamOpen(dw)) {
        try {
          await dw.connect();
          // 部分 SDK 实现 connect 在 socket open 前同步 resolve：等就绪再清退
          // 避，否则 while 立即重入 connect 造成连接风暴。
          const open = await (this.deps.waitConnected ?? waitUntilOpen)(dw, this.internal.signal);
          if (!open) throw new DingTalkApiError("钉钉长连接未在超时内就绪。");
          this.backoffIndex = 0;
        } catch (error) {
          if (this.internal.signal.aborted) return;
          this.ctx.onError(asError(error));
          const delays = this.deps.backoffMs ?? BACKOFF_MS;
          const delay = delays[this.backoffIndex] ?? delays[delays.length - 1] ?? 30_000;
          this.backoffIndex += 1;
          await sleep(delay, this.internal.signal);
        }
      }
    } finally {
      this.ensureRunning = false;
    }
  }

  private isSeen(messageId: string): boolean {
    return this.state.seen.includes(messageId);
  }

  private markSeen(messageId: string): void {
    this.state.seen.push(messageId);
    const overflow = this.state.seen.length - SEEN_CAP;
    if (overflow > 0) this.state.seen.splice(0, overflow);
  }

  private persist(): Promise<void> {
    return this.chain.run(() => atomicWrite(this.statePath, this.state));
  }

  private now(): number {
    return this.deps.now?.() ?? Date.now();
  }
}

export function createAdapter(
  ctx: AdapterContext<DingTalkConnection>,
  deps: DingtalkDeps = defaultDeps(ctx),
): DingtalkAdapter {
  return new DingtalkAdapter(ctx, deps);
}

export const definition: ChannelDefinition<DingTalkConnection> = {
  channel: "dingtalk",
  create: (ctx) => createAdapter(ctx),
  configure(fields: Record<string, string>): ConfiguredFields {
    requireFields(CHANNEL_SPECS.dingtalk.fields, fields);
    return defaultConfigure("dingtalk", fields);
  },
};
