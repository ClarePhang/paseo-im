/** 企业微信智能机器人（WeCom AI bot）渠道适配器 — WebSocket 长连接。
 * Protocol facts ported from xmanrui/dsh-im (MIT): frame body shapes
 * (chattype/chatid/from/msgid/msgtype + text/image/voice/mixed/file/quote),
 * group @-prefix stripping, 18 000-UTF-8-byte reply chunking, media delivery
 * via uploadMedia → sendMediaMessage, and AES media download via
 * client.downloadFile(url, aeskey) (30 MB cap). */
import { randomUUID } from "node:crypto";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { basename, join } from "node:path";
import {
  WSAuthFailureError,
  WSClient,
  WSReconnectExhaustedError,
  type SendMsgBody,
  type UploadMediaFinishResult,
  type UploadMediaOptions,
  type WeComMediaType,
  type WSClientOptions,
  type WsFrame,
  type WsFrameHeaders,
} from "@wecom/aibot-node-sdk";
import { z } from "zod";
import { CHANNEL_SPECS } from "../../../shared/channels.ts";
import type { Connection } from "../../../shared/config.ts";
import { UserFacingError } from "../../../shared/errors.ts";
import { atomicWrite, readJson, WriteChain } from "../../files.ts";
import { defaultConfigure, requireFields } from "../configure.ts";
import type { AdapterContext, ChannelAdapter, ChannelDefinition } from "../types.ts";
import type {
  ChannelCapabilities,
  NormalizedMedia,
  NormalizedMessage,
  OutboundMedia,
  OutboundText,
  SendResult,
} from "../../../shared/model.ts";
import { chunkText } from "./chunk.ts";
import { createPairingSession } from "./pairing.ts";

export type WecomConnection = Extract<Connection, { channel: "wecom" }>;

/** WeCom markdown message limit, in UTF-8 bytes (dsh-im MAX_REPLY_BYTES). */
const MAX_CHUNK_BYTES = 18_000;
/** Characters advertised via capabilities; 6 000 chars ≤ 18 000 bytes for any
 * UTF-8 content together with the byte budget enforced in chunkText(). */
const MAX_CHUNK_CHARS = 6_000;
const READY_TIMEOUT_MS = 20_000;
/** Transient-fault reconnect backoff until ctx.signal aborts. */
const RECONNECT_DELAYS_MS = [1_000, 3_000, 5_000, 10_000, 30_000] as const;
const MAX_UPLOAD_BYTES = 30 * 1024 * 1024;
const SEEN_CAP = 1_000;
const FRAME_CACHE_CAP = 64;
const STREAMS_CAP = 32;
/** WeCom closes a streaming card 10 minutes after its FIRST frame and refreshes
 * do not extend the window (errcode 846608); rotate the card at 510s so a long
 * task never outlives it. */
const STREAM_CARD_TTL_MS = 510_000;

const adapterStateSchema = z
  .object({
    version: z.literal(1),
    seen: z.array(z.string().min(1).max(256)).max(SEEN_CAP).default([]),
  })
  .strict();

/** Structural subset of the SDK WSClient surface the adapter touches; test
 * fakes implement this instead of the networked SDK class. */
export interface WecomClientLike {
  on(event: "message", listener: (frame: WsFrame) => void): unknown;
  on(event: "authenticated", listener: () => void): unknown;
  on(event: "disconnected", listener: (reason: string) => void): unknown;
  on(event: "error", listener: (error: Error) => void): unknown;
  connect(): unknown;
  disconnect(): void;
  readonly isConnected: boolean;
  sendMessage(chatid: string, body: SendMsgBody): Promise<WsFrame>;
  replyStream(frame: WsFrameHeaders, streamId: string, content: string, finish?: boolean): Promise<WsFrame>;
  replyStreamNonBlocking?(
    frame: WsFrameHeaders,
    streamId: string,
    content: string,
    finish?: boolean,
  ): Promise<WsFrame | "skipped">;
  uploadMedia(fileBuffer: Buffer, options: UploadMediaOptions): Promise<UploadMediaFinishResult>;
  sendMediaMessage(chatid: string, mediaType: WeComMediaType, mediaId: string): Promise<WsFrame>;
  downloadFile(url: string, aesKey?: string): Promise<{ buffer: Buffer; filename?: string }>;
}

export interface WecomAdapterDeps {
  /** Pre-built transport (tests inject a fake); default builds the SDK client. */
  client?: WecomClientLike;
  createClient?: (options: WSClientOptions) => WecomClientLike;
  readyTimeoutMs?: number;
  reconnectDelaysMs?: readonly number[];
  /** Card lifetime before rotation; tests shrink it to exercise the 510s wall. */
  streamCardTtlMs?: number;
}

export function defaultDeps(): Required<Pick<WecomAdapterDeps, "createClient">> {
  return {
    createClient: (options) =>
      new WSClient({
        ...options,
        // The adapter owns reconnection backoff; the SDK must not race it.
        maxReconnectAttempts: 0,
        maxAuthFailureAttempts: 0,
      }),
  };
}

export function createAdapter(
  ctx: AdapterContext<WecomConnection>,
  deps: WecomAdapterDeps = defaultDeps(),
): ChannelAdapter {
  return new WecomAdapter(ctx, deps);
}

interface MediaTarget {
  kind: "image" | "file";
  url: string;
  aeskey?: string;
  name?: string;
}

interface StreamRecord {
  chatId: string;
  frame: WsFrameHeaders | null;
  started: boolean;
  finished: boolean;
  broken: boolean;
  /** Live card id; equals the engine streamId until a rotation swaps it. */
  cardId: string;
  /** Wall clock of the live card's first frame; 0 = card not opened yet. */
  cardStartedAt: number;
  /** Latest text delivered to the live card; reused to close it on rotation. */
  cardText: string;
}

function str(value: unknown): string {
  return typeof value === "string" ? value : "";
}

function nonEmpty(value: unknown): string | null {
  const text = str(value).trim();
  return text ? text : null;
}

function textOfContent(content: unknown): string {
  const raw = str(content);
  // Group callbacks retain the leading @bot mention that caused delivery.
  // It is routing metadata rather than part of the user's prompt or answer.
  return raw.replace(/^\s*@\S+(?:\s+|$)/u, "").trim();
}

function contentText(container: unknown): string {
  return str(readField(container, "content")).trim();
}

function mixedText(body: Record<string, unknown>): string {
  const items = (body.mixed as { msg_item?: Array<Record<string, unknown>> } | undefined)?.msg_item;
  if (!Array.isArray(items)) return "";
  return items
    .filter((item) => item?.msgtype === "text")
    .map((item) => contentText(item.text))
    .join("\n")
    .trim();
}

/** Raw conversation text per message type; voice carries the platform's
 * transcript, which the adapter surfaces as-is (placeholder when absent). */
function rawText(body: Record<string, unknown>): string {
  switch (body.msgtype) {
    case "text":
      return contentText(readField(body, "text"));
    case "voice":
      return contentText(readField(body, "voice")) || "[语音]";
    case "mixed":
      return mixedText(body);
    default:
      return "";
  }
}

function quoteText(body: Record<string, unknown>): string {
  const quote = body.quote as Record<string, unknown> | undefined;
  if (!quote) return "";
  switch (quote.msgtype) {
    case "text":
      return contentText(readField(quote, "text"));
    case "voice":
      return contentText(readField(quote, "voice"));
    case "mixed":
      return mixedText(quote);
    default:
      return "";
  }
}

function imageTargets(body: Record<string, unknown>): MediaTarget[] {
  const image = body.image as Record<string, unknown> | undefined;
  const url = nonEmpty(image?.url);
  return url ? [{ kind: "image", url, aeskey: nonEmpty(image?.aeskey) ?? undefined }] : [];
}

function fileTarget(body: Record<string, unknown>): MediaTarget[] {
  const file = body.file as Record<string, unknown> | undefined;
  const url = nonEmpty(file?.url);
  if (!url) return [];
  return [{
    kind: "file",
    url,
    aeskey: nonEmpty(file?.aeskey) ?? undefined,
    name: nonEmpty(file?.filename) ?? nonEmpty(file?.file_name) ?? nonEmpty(file?.name) ?? undefined,
  }];
}

function collectMediaTargets(body: Record<string, unknown>): MediaTarget[] {
  switch (body.msgtype) {
    case "image":
      return imageTargets(body);
    case "file":
      return fileTarget(body);
    case "mixed": {
      const items = (body.mixed as { msg_item?: Array<Record<string, unknown>> } | undefined)?.msg_item;
      if (!Array.isArray(items)) return [];
      return items
        .filter((item) => item?.msgtype === "image")
        .flatMap((item) => imageTargets(item));
    }
    default:
      return [];
  }
}

const MIME_BY_EXT: Record<string, string> = {
  jpg: "image/jpeg",
  jpeg: "image/jpeg",
  png: "image/png",
  gif: "image/gif",
  webp: "image/webp",
  bmp: "image/bmp",
  svg: "image/svg+xml",
  heic: "image/heic",
  pdf: "application/pdf",
  txt: "text/plain",
  md: "text/markdown",
  csv: "text/csv",
  json: "application/json",
  zip: "application/zip",
  mp3: "audio/mpeg",
  wav: "audio/wav",
  mp4: "video/mp4",
  doc: "application/msword",
  docx: "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
  xls: "application/vnd.ms-excel",
  xlsx: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
  ppt: "application/vnd.ms-powerpoint",
  pptx: "application/vnd.openxmlformats-officedocument.presentationml.presentation",
};

function guessMime(name: string, kind: "image" | "file"): string {
  const ext = name.includes(".") ? name.split(".").pop()!.toLowerCase() : "";
  if (ext && MIME_BY_EXT[ext]) return MIME_BY_EXT[ext];
  return kind === "image" ? "image/jpeg" : "application/octet-stream";
}

function sanitizeName(name: string, fallback: string): string {
  const cleaned = name.replace(/[\\/]/g, "_").replace(/[\u0000-\u001f]/g, "").trim().slice(0, 120);
  return cleaned || fallback;
}

function normalizeTimestamp(value: unknown): number {
  if (typeof value === "number" && Number.isFinite(value) && value > 0) {
    // WeCom protocol timestamps are seconds; guard against ms-shaped values.
    return value < 1e12 ? value * 1000 : value;
  }
  return Date.now();
}

function readField(source: unknown, key: string): unknown {
  if (source !== null && typeof source === "object" && key in source) {
    return (source as Record<string, unknown>)[key];
  }
  return undefined;
}

function providerMessageId(receipt: unknown): string | null {
  return nonEmpty(readField(readField(receipt, "body"), "msgid"))
    ?? nonEmpty(readField(readField(receipt, "body"), "message_id"));
}

/** The SDK rejects sends with the raw ack frame (errcode/errmsg, no .message);
 * wrap so callers always see a readable, user-facing Chinese error. */
function sendError(operation: string, error: unknown): UserFacingError {
  if (error instanceof UserFacingError) return error;
  const body = readField(error, "body");
  const code = [readField(error, "errcode"), readField(body, "errcode")]
    .map((value) => Number(value))
    .find((value) => Number.isFinite(value));
  const detail = nonEmpty(readField(error, "errmsg")) ?? nonEmpty(readField(body, "errmsg"))
    ?? (error instanceof Error ? error.message : String(error));
  const codeNote = code !== undefined && code !== 0 ? `，错误码 ${code}` : "";
  return new UserFacingError(`企业微信${operation}失败${codeNote}：${detail}`);
}

function descriptionOf(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

export class WecomAdapter implements ChannelAdapter {
  readonly channel = "wecom" as const;
  readonly capabilities: ChannelCapabilities = {
    progress: "stream",
    markdown: "native",
    uploadImage: true,
    uploadFile: true,
    reactions: false,
    typing: false,
    maxChunkChars: MAX_CHUNK_CHARS,
    threads: false,
  };

  readonly #ctx: AdapterContext<WecomConnection>;
  readonly #deps: WecomAdapterDeps;
  readonly #statePath: string;
  readonly #writeChain = new WriteChain();
  #client: WecomClientLike | null = null;
  #seen: string[] = [];
  #lastFrames = new Map<string, WsFrame>();
  #streams = new Map<string, StreamRecord>();
  #reconnectTimer: NodeJS.Timeout | null = null;
  #reconnectIndex = 0;
  #authFailed = false;
  #stopped = false;
  #onAbort: (() => void) | null = null;
  /** Aborted by #teardown so a start() stuck in #waitForReady cannot outlive stop(). */
  #readyGate: AbortController | null = null;

  constructor(ctx: AdapterContext<WecomConnection>, deps: WecomAdapterDeps) {
    this.#ctx = ctx;
    this.#deps = deps;
    this.#statePath = join(ctx.dataDir, "wecom-state.json");
  }

  async start(): Promise<void> {
    if (this.#client) throw new UserFacingError("企业微信适配器已启动。");
    const botId = nonEmpty(this.#ctx.config.botId);
    const secret = nonEmpty(this.#ctx.secrets.secret);
    if (!botId || !secret) {
      throw new UserFacingError("请在连接设置中填写企业微信机器人的 Bot ID，并在密钥中填写对应的 Secret。");
    }
    const client = this.#deps.client ?? this.#deps.createClient?.({ botId, secret });
    if (!client) throw new UserFacingError("企业微信客户端创建失败，请检查适配器依赖配置。");
    this.#client = client;

    client.on("message", (frame: WsFrame) => this.#handleFrame(frame));
    client.on("authenticated", () => {
      this.#reconnectIndex = 0;
    });
    client.on("disconnected", () => this.#scheduleReconnect());
    client.on("error", (error: Error) => this.#handleClientError(error));

    const onAbort = () => {
      this.#stopped = true;
      this.#clearReconnectTimer();
      client.disconnect();
    };
    this.#ctx.signal.addEventListener("abort", onAbort, { once: true });
    this.#onAbort = () => this.#ctx.signal.removeEventListener("abort", onAbort);

    await this.#loadState();
    const readyGate = new AbortController();
    this.#readyGate = readyGate;
    try {
      client.connect();
      await this.#waitForReady(client, readyGate.signal);
    } catch (error) {
      this.#teardown();
      // 失败路径不能留下看似已启动的 #client：testConnection 会误报“已连接”。
      this.#client = null;
      throw error;
    } finally {
      this.#readyGate = null;
    }
  }

  async stop(): Promise<void> {
    this.#teardown();
    await this.#writeChain.run(async () => undefined);
  }

  async sendText(outbound: OutboundText): Promise<SendResult> {
    const chunks = chunkText(outbound.text, this.capabilities.maxChunkChars);
    let messageId: string | undefined;
    // 多块发送只回传最后一块的非空 msgid：各块 ack 的 msgid 相互独立，调用方
    // 仅需要一个代表整条回复的 id（已读/去重锚点），逐块上报没有消费者。
    for (const chunk of chunks) {
      const receipt = await this.#sendMarkdown(outbound.chatId, chunk);
      messageId = providerMessageId(receipt) ?? messageId;
    }
    return { messageId };
  }

  async #sendMarkdown(chatId: string, content: string): Promise<WsFrame> {
    try {
      return await this.#requireClient().sendMessage(chatId, { msgtype: "markdown", markdown: { content } });
    } catch (error) {
      throw sendError("发送消息", error);
    }
  }

  async sendMedia(outbound: OutboundMedia): Promise<SendResult> {
    const mediaType: WeComMediaType = outbound.kind === "image" ? "image" : "file";
    const bytes = await readFile(outbound.path).catch((error: NodeJS.ErrnoException) => {
      throw new UserFacingError(`无法读取待发送的${outbound.kind === "image" ? "图片" : "文件"} ${outbound.path}：${error.message}`);
    });
    if (bytes.length === 0) {
      throw new UserFacingError(`无法发送空的${outbound.kind === "image" ? "图片" : "文件"}，企业微信拒绝空媒体。`);
    }
    if (bytes.length > MAX_UPLOAD_BYTES) {
      throw new UserFacingError(`待发送的${outbound.kind === "image" ? "图片" : "文件"}超过企业微信 30MB 媒体上限，未发送。`);
    }
    const filename = sanitizeName(outbound.name || basename(outbound.path), outbound.kind === "image" ? "image.jpg" : "file.bin");
    let uploaded: UploadMediaFinishResult;
    try {
      uploaded = await this.#requireClient().uploadMedia(bytes, { type: mediaType, filename });
    } catch (error) {
      throw sendError("上传媒体文件", error);
    }
    const mediaId = nonEmpty(uploaded?.media_id);
    if (!mediaId) throw new UserFacingError("企业微信媒体上传未返回 media_id，发送已取消。");
    try {
      const receipt = await this.#requireClient().sendMediaMessage(outbound.chatId, mediaType, mediaId);
      return { messageId: providerMessageId(receipt) ?? undefined };
    } catch (error) {
      throw sendError("发送媒体消息", error);
    }
  }

  async streamStart(chatId: string): Promise<{ streamId: string }> {
    const streamId = `stream-${randomUUID()}`;
    const frame = this.#lastFrames.get(chatId) ?? null;
    if (this.#streams.size >= STREAMS_CAP) {
      const oldest = this.#streams.keys().next().value;
      if (oldest !== undefined) this.#streams.delete(oldest);
    }
    this.#streams.set(streamId, {
      chatId,
      frame,
      started: false,
      finished: false,
      broken: false,
      cardId: streamId,
      cardStartedAt: 0,
      cardText: "",
    });
    return { streamId };
  }

  async streamAppend(streamId: string, text: string): Promise<void> {
    const record = this.#streams.get(streamId);
    if (!record || record.finished || record.broken || !record.frame || !text) return;
    const client = this.#requireClient();
    try {
      if (!record.started) {
        record.started = true;
        record.cardStartedAt = Date.now();
        record.cardText = text;
        await client.replyStream(record.frame, record.cardId, text, false);
        return;
      }
      // Long turns outlive a card's 10-minute window: rotate before the wall so
      // later frames (and the finish) keep landing on a live card. `>=` keeps a
      // TTL of 0 (tests) degenerate: every follow-up append rotates.
      if (Date.now() - record.cardStartedAt >= (this.#deps.streamCardTtlMs ?? STREAM_CARD_TTL_MS)) {
        await this.#rotateStreamCard(client, record, record.frame, text);
        return;
      }
      record.cardText = text;
      if (client.replyStreamNonBlocking) {
        await client.replyStreamNonBlocking(record.frame, record.cardId, text, false);
      } else {
        await client.replyStream(record.frame, record.cardId, text, false);
      }
    } catch (error) {
      record.broken = true;
      this.#ctx.log("warn", `企业微信流式进度发送失败，将在完成时整体发送：${descriptionOf(error)}`);
    }
  }

  /** Finish the expiring card with its last known text, then seed a fresh card
   * from the same frame; the engine keeps its streamId throughout. A failure to
   * open the new card propagates: the stream turns broken and streamFinish
   * falls back to a plain message, so content is never duplicated on a dead
   * card plus a re-send. */
  async #rotateStreamCard(client: WecomClientLike, record: StreamRecord, frame: WsFrameHeaders, nextText: string): Promise<void> {
    try {
      await client.replyStream(frame, record.cardId, record.cardText, true);
    } catch (error) {
      this.#ctx.log("warn", `企业微信流式卡片轮换收尾失败，忽略旧卡：${descriptionOf(error)}`);
    }
    const newCardId = `stream-${randomUUID()}`;
    const seeded = `（续）${nextText}`;
    await client.replyStream(frame, newCardId, seeded, false);
    record.cardId = newCardId;
    record.cardStartedAt = Date.now();
    record.cardText = seeded;
    this.#ctx.log("info", `企业微信流式卡片已轮换，后续进度写入新卡片：${record.chatId}`);
  }

  async streamFinish(streamId: string, text: string): Promise<{ messageId?: string }> {
    const record = this.#streams.get(streamId);
    if (!record) throw new UserFacingError(`企业微信流式回复不存在或已被清理，无法完成：${streamId}`);
    this.#streams.delete(streamId);
    record.finished = true;
    const content = text.trim() ? text : "";
    if (record.frame && !record.broken) {
      try {
        const receipt = await this.#requireClient().replyStream(record.frame, record.cardId, content, true);
        return { messageId: providerMessageId(receipt) ?? undefined };
      } catch (error) {
        this.#ctx.log("warn", `企业微信流式回复完成失败，改为整体发送：${descriptionOf(error)}`);
      }
    }
    if (!content) return {};
    return this.sendText({ chatId: record.chatId, text: content });
  }

  // WeCom pushes no group membership events over the WebSocket; the manager
  // must not wait for any from this adapter.
  // onGroups: intentionally never called.

  // WeCom bots cannot leave conversations, so leaveChat stays unsupported.

  async testConnection(): Promise<{ ok: boolean; detail?: string }> {
    const client = this.#client;
    if (!client) return { ok: false, detail: "适配器尚未启动。" };
    return client.isConnected ? { ok: true } : { ok: false, detail: "企业微信 WebSocket 未连接。" };
  }

  #requireClient(): WecomClientLike {
    if (!this.#client) throw new UserFacingError("企业微信适配器尚未启动。");
    return this.#client;
  }

  #handleClientError(error: Error): void {
    if (error instanceof WSReconnectExhaustedError) return; // 'disconnected' owns the retry
    if (error instanceof WSAuthFailureError) {
      this.#authFailed = true;
      this.#clearReconnectTimer();
      this.#ctx.onError(new UserFacingError("企业微信机器人认证失败：请检查 Bot ID 与 Secret 是否正确。"));
      return;
    }
    this.#ctx.onError(error);
  }

  #scheduleReconnect(): void {
    if (this.#stopped || this.#ctx.signal.aborted || this.#authFailed) return;
    this.#clearReconnectTimer();
    const delays = this.#deps.reconnectDelaysMs ?? RECONNECT_DELAYS_MS;
    const delay = delays[Math.min(this.#reconnectIndex, delays.length - 1)];
    this.#reconnectIndex += 1;
    this.#ctx.log("warn", `企业微信连接断开，${Math.round(delay / 1000)} 秒后重连。`);
    this.#reconnectTimer = setTimeout(() => {
      this.#reconnectTimer = null;
      if (this.#stopped || this.#ctx.signal.aborted || this.#authFailed) return;
      const client = this.#client;
      if (!client || client.isConnected) return;
      try {
        client.disconnect();
        client.connect();
      } catch (error) {
        this.#ctx.onError(error instanceof Error ? error : new Error(String(error)));
        this.#scheduleReconnect();
      }
    }, delay);
    this.#reconnectTimer.unref?.();
  }

  #clearReconnectTimer(): void {
    if (this.#reconnectTimer) {
      clearTimeout(this.#reconnectTimer);
      this.#reconnectTimer = null;
    }
  }

  #waitForReady(client: WecomClientLike, gateSignal: AbortSignal): Promise<void> {
    const { promise, resolve, reject } = Promise.withResolvers<void>();
    let settled = false;
    const done = (error?: Error) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      this.#ctx.signal.removeEventListener("abort", onAbort);
      gateSignal.removeEventListener("abort", onGateAbort);
      if (error) reject(error);
      else resolve();
    };
    const timer = setTimeout(
      () => done(new UserFacingError("连接企业微信 WebSocket 超时，请检查网络连接后重试。")),
      this.#deps.readyTimeoutMs ?? READY_TIMEOUT_MS,
    );
    const onAbort = () => done(this.#ctx.signal.reason instanceof Error ? this.#ctx.signal.reason : new Error("已中止"));
    this.#ctx.signal.addEventListener("abort", onAbort, { once: true });
    // stop()/teardown aborts the gate so a start() stuck here rejects instead
    // of hanging until the ready timeout.
    const onGateAbort = () => done(gateSignal.reason instanceof Error ? gateSignal.reason : new Error("已停止"));
    gateSignal.addEventListener("abort", onGateAbort, { once: true });
    client.on("authenticated", () => done());
    client.on("error", (error: Error) => {
      // Transient disconnects may still recover inside the ready window;
      // only an authentication failure is terminal during start.
      if (error instanceof WSAuthFailureError) {
        done(new UserFacingError("企业微信机器人认证失败：请检查 Bot ID 与 Secret 是否正确。"));
      }
    });
    return promise;
  }

  #teardown(): void {
    this.#stopped = true;
    this.#clearReconnectTimer();
    // Release a start() still waiting in #waitForReady (stop during start).
    this.#readyGate?.abort(new UserFacingError("企业微信适配器已停止。"));
    this.#onAbort?.();
    this.#onAbort = null;
    this.#client?.disconnect();
  }

  async #loadState(): Promise<void> {
    try {
      const state = await readJson(this.#statePath, (value) => adapterStateSchema.parse(value));
      this.#seen = state?.seen ?? [];
    } catch (error) {
      this.#seen = [];
      this.#ctx.log("warn", `企业微信适配器状态文件损坏，已重置去重记录：${descriptionOf(error)}`);
    }
  }

  async #markSeen(msgid: string): Promise<void> {
    this.#seen.push(msgid);
    if (this.#seen.length > SEEN_CAP) {
      this.#seen = this.#seen.slice(this.#seen.length - SEEN_CAP);
    }
    const snapshot = this.#seen;
    try {
      await this.#writeChain.run(() => atomicWrite(this.#statePath, { version: 1, seen: snapshot }));
    } catch (error) {
      // 写盘失败必须回滚内存里的 msgid：消息未投递且未持久化，保留去重记录会让
      // 平台重推的这条消息被永久吞掉。
      this.#seen = this.#seen.filter((id) => id !== msgid);
      throw error;
    }
  }

  #rememberFrame(chatId: string, frame: WsFrame): void {
    this.#lastFrames.delete(chatId);
    this.#lastFrames.set(chatId, frame);
    if (this.#lastFrames.size > FRAME_CACHE_CAP) {
      const oldest = this.#lastFrames.keys().next().value;
      if (oldest !== undefined) this.#lastFrames.delete(oldest);
    }
  }

  #handleFrame(frame: WsFrame): void {
    if (this.#stopped) return;
    const body = frame?.body && typeof frame.body === "object" ? (frame.body as Record<string, unknown>) : null;
    if (!body || body.msgtype === "event") return;
    const chattype = body.chattype;
    if (chattype !== "single" && chattype !== "group") return;
    const msgid = str(body.msgid);
    const senderId = str(readField(body.from, "userid"));
    const chatId = chattype === "group" ? str(body.chatid) : senderId;
    if (!msgid || !senderId || !chatId) return;
    if (this.#seen.includes(msgid)) return;
    this.#accept(frame, body, { msgid, senderId, chatId, chattype }).catch((error) => {
      this.#ctx.onError(error instanceof Error ? error : new Error(String(error)));
    });
  }

  async #accept(
    frame: WsFrame,
    body: Record<string, unknown>,
    ids: { msgid: string; senderId: string; chatId: string; chattype: "single" | "group" },
  ): Promise<void> {
    await this.#markSeen(ids.msgid);
    this.#rememberFrame(ids.chatId, frame);

    let media: NormalizedMedia[] = [];
    const targets = collectMediaTargets(body);
    if (targets.length > 0) {
      try {
        const settings = await this.#ctx.settings();
        const maxBytes = Math.max(1, settings.imageMaxDownloadMb) * 1024 * 1024;
        media = await this.#downloadMedia(targets.slice(0, Math.max(1, settings.imageMaxCount)), ids.msgid, maxBytes);
      } catch (error) {
        this.#ctx.log("warn", `企业微信媒体下载失败，本次消息仅保留文本：${descriptionOf(error)}`);
      }
    }

    const kind = ids.chattype === "group" ? "group" : "dm";
    const text = ids.chattype === "group" ? textOfContent(rawText(body)) : rawText(body).trim();
    const reply = quoteText(body);
    const message: NormalizedMessage = {
      channel: "wecom",
      connectionId: this.#ctx.connectionId,
      kind,
      chatId: ids.chatId,
      senderId: ids.senderId,
      messageId: ids.msgid,
      timestamp: normalizeTimestamp(body.create_time),
      ...(text ? { text } : {}),
      ...(media.length > 0 ? { media } : {}),
      ...(reply ? { replyTo: { text: reply } } : {}),
      // WeCom delivers group messages to the bot only when the bot is
      // engaged, so every delivered message counts as addressed to it.
      trigger: { mention: true, replyToBot: false, command: text.startsWith("/") },
    };
    if (!message.text && (message.media?.length ?? 0) === 0) return; // noise
    this.#ctx.onMessage(message);
  }

  async #downloadMedia(targets: MediaTarget[], msgid: string, maxBytes: number): Promise<NormalizedMedia[]> {
    const media: NormalizedMedia[] = [];
    await mkdir(this.#ctx.mediaDir, { recursive: true });
    for (const [index, target] of targets.entries()) {
      try {
        const result = await this.#requireClient().downloadFile(target.url, target.aeskey);
        const buffer = Buffer.from(result?.buffer ?? new Uint8Array());
        if (buffer.length === 0) throw new Error("下载内容为空");
        if (buffer.length > maxBytes) {
          this.#ctx.log("warn", `企业微信媒体超过 ${Math.round(maxBytes / 1024 / 1024)}MB 上限，已跳过下载。`);
          continue;
        }
        const fallback = target.kind === "image" ? "image.jpg" : "file.bin";
        const name = sanitizeName(result?.filename || target.name || fallback, fallback);
        const path = join(this.#ctx.mediaDir, `${msgid}-${index + 1}-${name}`);
        await writeFile(path, buffer);
        media.push({ kind: target.kind, path, mime: guessMime(name, target.kind), name });
      } catch (error) {
        this.#ctx.log("warn", `企业微信媒体下载失败，已跳过：${descriptionOf(error)}`);
      }
    }
    return media;
  }
}

export const definition: ChannelDefinition<WecomConnection> = {
  channel: "wecom",
  create: (ctx) => createAdapter(ctx),
  configure: (fields) => {
    requireFields(CHANNEL_SPECS.wecom.fields, fields);
    // WeCom needs no derived defaults: botId is plain config, secret a secret.
    return defaultConfigure("wecom", fields);
  },
  pairing: (connectionId, dataDir) => createPairingSession(connectionId, dataDir),
};
