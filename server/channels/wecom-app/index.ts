/** WeCom self-built application channel adapter: local callback listener,
 * passive replies and cgi-bin outbound messaging.
 * Protocol facts ported from xmanrui/dsh-im (MIT). */
import { randomBytes } from "node:crypto";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { basename } from "node:path";
import { join } from "node:path";

import { z } from "zod";
import { WecomCrypto } from "@wecom/aibot-node-sdk";

import { atomicWrite, readJson, sleep, WriteChain } from "../../files.ts";
import { splitChunks } from "../../format/chunks.ts";
import { defaultConfigure, requireFields } from "../configure.ts";
import type { AdapterContext, ChannelAdapter, ChannelDefinition, ConfiguredFields } from "../types.ts";
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
import { WecomAppApi } from "./api.ts";
import { acquireCallbackServer, releaseCallbackServer, routePath, type WecomAppCallbackServer } from "./callback.ts";
import {
  cleanString,
  splitUtf8ByBytes,
  WECOM_APP_TEXT_MAX_BYTES,
  WecomAppApiError,
  type WecomAppInboundMessage,
  type WecomCryptoLike,
} from "./protocol.ts";

export type WecomAppConnection = Extract<Connection, { channel: "wecom-app" }>;

const DEFAULT_PORT = 30987;
const PORT_ENV = "PASEO_IM_WECOM_APP_PORT";
/** WeCom passes a passive reply within ~5s; leave a small margin. */
const DEFAULT_REPLY_WINDOW_MS = 4_800;
const SEEN_LIMIT = 500;
const RECONNECT_DELAYS_MS = [1_000, 3_000, 5_000, 10_000, 30_000];
/** pic_url 走公网直链，无平台超时兜底；挂死会拖垮整个回复窗口。 */
const PIC_DOWNLOAD_TIMEOUT_MS = 10_000;

/** ~682 CJK chars fill 2048 UTF-8 bytes; keep chunks under the platform limit
 * so the byte-level splitter stays a corner-case guard. */
const MAX_CHUNK_CHARS = 680;

export interface WecomAppDeps {
  /** Outbound HTTP transport (API calls and pic_url media downloads). */
  fetch_?: typeof fetch;
  /** WecomCrypto-shaped signing/encryption; defaults to the real SDK crypto. */
  crypto?: WecomCryptoLike;
  /** Callback listener port; 0 picks an ephemeral port (tests). */
  port?: number;
  /** Passive-reply window for tests. */
  replyWindowMs?: number;
  /** Backoff schedule for the startup credential check. */
  healthRetryDelays?: number[];
  /** pic_url 下载超时（默认 10s）；测试缩短以验证降级路径。 */
  picDownloadTimeoutMs?: number;
}

export function defaultDeps(): WecomAppDeps {
  const raw = Number(process.env[PORT_ENV]);
  const port = Number.isInteger(raw) && raw >= 0 && raw <= 65_535 ? raw : DEFAULT_PORT;
  return { port };
}

const adapterStateSchema = z
  .object({
    version: z.literal(1),
    seen: z.array(z.string()).max(SEEN_LIMIT).default([]),
    chats: z.record(z.string(), z.enum(["dm", "group"])).default({}),
  })
  .strict();

type AdapterState = z.output<typeof adapterStateSchema>;

function emptyState(): AdapterState {
  return adapterStateSchema.parse({ version: 1 });
}

interface ReplyWindow {
  reply: (payload: { msgtype: "text"; text: { content: string } } | null) => void;
  timer: NodeJS.Timeout;
}

export class WecomAppAdapter implements ChannelAdapter {
  readonly channel = "wecom-app" as const;
  readonly capabilities: ChannelCapabilities = {
    // config.streamEnabled 暂未生效：本渠道 v1 固定无流式进度（progress "none"，
    // 仅被动回复/主动文本）。该字段仅保留在配置 schema 中，启用流式卡片前不要
    // 依据它改变行为。
    progress: "none",
    markdown: "plain",
    uploadImage: true,
    uploadFile: true,
    reactions: false,
    typing: false,
    maxChunkChars: MAX_CHUNK_CHARS,
    threads: false,
  };

  readonly #ctx: AdapterContext<WecomAppConnection>;
  readonly #api: WecomAppApi;
  readonly #cryptoFor: () => WecomCryptoLike;
  readonly #fetch_: typeof fetch;
  readonly #port: number;
  readonly #replyWindowMs: number;
  readonly #healthRetryDelays: number[];
  readonly #picDownloadTimeoutMs: number;
  readonly #chain = new WriteChain();
  readonly #windows = new Map<string, ReplyWindow>();
  readonly #stopController = new AbortController();
  #state: AdapterState = emptyState();
  #server: WecomAppCallbackServer | null = null;
  #registeredRoute: string | null = null;
  #sharedPort: number | null = null;

  constructor({
    ctx,
    api,
    cryptoFor,
    fetch_ = fetch,
    port,
    replyWindowMs = DEFAULT_REPLY_WINDOW_MS,
    healthRetryDelays = RECONNECT_DELAYS_MS,
    picDownloadTimeoutMs = PIC_DOWNLOAD_TIMEOUT_MS,
  }: {
    ctx: AdapterContext<WecomAppConnection>;
    api: WecomAppApi;
    cryptoFor: () => WecomCryptoLike;
    fetch_?: typeof fetch;
    port: number;
    replyWindowMs?: number;
    healthRetryDelays?: number[];
    picDownloadTimeoutMs?: number;
  }) {
    this.#ctx = ctx;
    this.#api = api;
    this.#cryptoFor = cryptoFor;
    this.#fetch_ = fetch_;
    this.#port = port;
    this.#replyWindowMs = replyWindowMs;
    this.#healthRetryDelays = healthRetryDelays;
    this.#picDownloadTimeoutMs = picDownloadTimeoutMs;
  }

  get callbackPath(): string {
    return routePath({ botId: this.#ctx.connectionId, callbackSecret: this.#ctx.config.callbackSecret });
  }

  /** Actual bound port after listen(0); null before start(). Test and UI seam. */
  get callbackPort(): number | null {
    return this.#server?.actualPort ?? null;
  }

  async start(): Promise<void> {
    this.#validateCredentials();
    const server = await acquireCallbackServer({ port: this.#port, log: this.#ctx.log });
    this.#sharedPort = this.#port;
    server.registerRoute({
      botId: this.#ctx.connectionId,
      callbackSecret: this.#ctx.config.callbackSecret,
      cryptoFor: this.#cryptoFor,
      onMessage: (message, reply) => this.#handleCallback(message, reply),
    });
    this.#server = server;
    this.#registeredRoute = this.callbackPath;
    try {
      this.#state = (await readJson(join(this.#ctx.dataDir, "wecom-app-state.json"), (value) => adapterStateSchema.parse(value))) ?? emptyState();
    } catch (error) {
      // 对齐 wecom #loadState：状态文件损坏时重置空状态并告警，不让启动失败。
      this.#state = emptyState();
      this.#ctx.log("warn", `企业微信应用状态文件损坏，已重置去重与会话记录：${error instanceof Error ? error.message : String(error)}`);
    }
    const base = cleanString(this.#ctx.config.callbackBaseUrl)!;
    // 日志不得携带路由中的 callbackSecret 段（机密不进日志）。
    this.#ctx.log("info", `企业微信应用回调已监听：${base}${redactCallbackPath(this.#registeredRoute)}`);
    this.#startHealthLoop();
  }

  async stop(): Promise<void> {
    this.#stopController.abort();
    for (const window of this.#windows.values()) window.reply(null);
    this.#windows.clear();
    this.#server?.unregisterRoute(this.#ctx.connectionId);
    this.#server = null;
    this.#registeredRoute = null;
    if (this.#sharedPort !== null) {
      await releaseCallbackServer(this.#sharedPort);
      this.#sharedPort = null;
    }
  }

  #validateCredentials(): void {
    const { config } = this.#ctx;
    const secret = cleanString(this.#ctx.secrets.secret);
    const token = cleanString(this.#ctx.secrets.token);
    const aesKey = cleanString(this.#ctx.secrets.encodingAESKey);
    if (!secret) {
      throw new UserFacingError("缺少应用 Secret：请在连接凭据中填写企业微信自建应用的 Secret。");
    }
    if (!token) {
      throw new UserFacingError("缺少回调 Token：请在企业微信后台“接收消息”配置中复制 Token，并填入连接凭据。");
    }
    if (!aesKey || !/^[A-Za-z0-9]{43}$/.test(aesKey)) {
      throw new UserFacingError("EncodingAESKey 不合法：应为 43 位字母或数字，请在企业微信后台“接收消息”配置中复制完整 EncodingAESKey。");
    }
    if (!/^[0-9a-f]{32}$/i.test(config.callbackSecret)) {
      throw new UserFacingError("callbackSecret 不合法：应为 32 位十六进制字符串，请重新保存连接以重新生成。");
    }
    if (!cleanString(config.callbackBaseUrl)) {
      throw new UserFacingError(
        "需要公网可达的回调基址：请在连接配置中填写 callbackBaseUrl（例如 https://your-domain.com），企业微信后台的回调 URL 必须能通过公网访问本服务。",
      );
    }
  }

  #startHealthLoop(): void {
    void (async () => {
      let attempt = 0;
      while (!this.#stopController.signal.aborted && !this.#ctx.signal.aborted) {
        try {
          await this.#api.health(this.#stopController.signal);
          this.#ctx.log("info", "企业微信应用凭据校验通过");
          return;
        } catch (error) {
          if (this.#stopController.signal.aborted || this.#ctx.signal.aborted) return;
          const transient = !(error instanceof WecomAppApiError) || error.transient;
          this.#ctx.onError(error instanceof Error ? error : new Error(String(error)));
          if (!transient) return;
          await sleep(this.#healthRetryDelays[Math.min(attempt, this.#healthRetryDelays.length - 1)], this.#stopController.signal).catch(() => undefined);
          attempt += 1;
        }
      }
    })();
  }

  async testConnection(): Promise<{ ok: boolean; detail?: string }> {
    try {
      await this.#api.health();
      return { ok: true, detail: "企业微信凭据有效，可以正常发送消息。" };
    } catch (error) {
      return { ok: false, detail: error instanceof Error ? error.message : String(error) };
    }
  }

  async sendText(outbound: OutboundText): Promise<SendResult> {
    const text = outbound.text.replace(/\s+$/u, "");
    if (!text) throw new Error("sendText 需要非空文本");
    const chunks = splitChunks(text, this.capabilities.maxChunkChars).flatMap((piece) => splitUtf8ByBytes(piece, WECOM_APP_TEXT_MAX_BYTES));
    const kind = this.#state.chats[outbound.chatId] ?? "dm";
    const key = `${kind}:${outbound.chatId}`;
    const window = this.#windows.get(key);
    if (window) {
      // Answer the pending callback passively with the first chunk; anything
      // beyond it (or the absence of a window) goes out via message/send.
      this.#windows.delete(key);
      window.reply({ msgtype: "text", text: { content: chunks[0]! } });
      for (const chunk of chunks.slice(1)) await this.#sendChunk(kind, outbound.chatId, chunk);
      return {};
    }
    for (const chunk of chunks) await this.#sendChunk(kind, outbound.chatId, chunk);
    return {};
  }

  async sendMedia(outbound: OutboundMedia): Promise<SendResult> {
    const bytes = await readFile(outbound.path);
    const kind = this.#state.chats[outbound.chatId] ?? "dm";
    await this.#api.sendMedia({
      ...(kind === "dm" ? { userId: outbound.chatId } : { chatId: outbound.chatId }),
      mediaType: outbound.kind === "file" ? "file" : "image",
      bytes,
      filename: outbound.name ?? basename(outbound.path),
    });
    return {};
  }

  async #sendChunk(kind: "dm" | "group", chatId: string, chunk: string): Promise<void> {
    await this.#api.sendText(kind === "dm" ? { userId: chatId, content: chunk } : { chatId, content: chunk });
  }

  #handleCallback(message: WecomAppInboundMessage, answer: (payload: { msgtype: "text"; text: { content: string } } | null) => void): void {
    // v1 keeps progress "none": no stream registry, so stream refreshes and
    // lifecycle events get an empty acknowledgement.
    if (message.msgtype === "stream" || message.event === "stream_refresh" || message.msgtype === "event") {
      answer(null);
      return;
    }
    const msgid = cleanString(message.msgid) ?? this.#syntheticId(message);
    if (this.#state.seen.includes(msgid)) {
      // WeCom retries unanswered POSTs with the same MsgId; the first attempt
      // already opened a reply window (or was answered).
      answer(null);
      return;
    }
    const normalized = this.#normalize(message, msgid);
    if (!normalized) {
      answer(null);
      return;
    }
    this.#rememberSeen(msgid);
    const kind = normalized.kind;
    const key = `${kind}:${normalized.chatId}`;
    const existing = this.#windows.get(key);
    if (existing) existing.reply(null);
    const timer = setTimeout(() => {
      this.#windows.delete(key);
      answer(null);
    }, this.#replyWindowMs);
    if (typeof timer.unref === "function") timer.unref();
    this.#windows.set(key, {
      reply: (payload) => {
        clearTimeout(timer);
        this.#windows.delete(key);
        answer(payload);
      },
      timer,
    });
    this.#state.chats[normalized.chatId] = kind;
    this.#persistState();
    if (message.msgtype === "image") {
      void this.#emitWithMedia(normalized, message, key);
      return;
    }
    this.#emit(normalized, key);
  }

  async #emitWithMedia(normalized: NormalizedMessage, message: WecomAppInboundMessage, key: string): Promise<void> {
    const media = await this.#downloadInboundImage(message);
    if (!media) {
      // 下载失败/超时不得静默丟消息：降级为无图文本继续回合；纯图片（无文字）
      // 用占位文本，让发送者知道图片未被处理。
      this.#emit({ ...normalized, text: normalized.text ?? "[图片]" }, key);
      return;
    }
    this.#emit({ ...normalized, media: [media] }, key);
  }

  #emit(message: NormalizedMessage, key: string): void {
    try {
      this.#ctx.onMessage(message);
    } catch (error) {
      this.#ctx.log("error", `入站消息处理失败：${error instanceof Error ? error.message : String(error)}`);
      this.#closeWindow(key);
    }
  }

  #closeWindow(key: string): void {
    const window = this.#windows.get(key);
    if (!window) return;
    window.reply(null);
  }

  #normalize(message: WecomAppInboundMessage, msgid: string): NormalizedMessage | null {
    const senderId = cleanString(message.fromUserId);
    if (!senderId) return null;
    const chattype = message.chattype === "group" || (!message.chattype && Boolean(message.chatid));
    const kind: "dm" | "group" = chattype ? "group" : "dm";
    const chatId = kind === "group" ? cleanString(message.chatid) : senderId;
    if (!chatId) return null;
    let text: string | undefined;
    if (message.msgtype === "text") {
      text = cleanString(message.text) ?? undefined;
    } else if (message.msgtype === "voice") {
      text = cleanString(message.recognition) ?? undefined;
    } else if (message.msgtype !== "image") {
      return null;
    }
    const isImage = message.msgtype === "image";
    const hasImage = isImage && Boolean(message.imageUrl || message.imageMediaId);
    if (!text && !hasImage) return null;
    const createTime = Number(message.createTime);
    const quote = message.quote;
    return {
      channel: "wecom-app",
      connectionId: this.#ctx.connectionId,
      kind,
      chatId,
      senderId,
      senderName: cleanString(message.fromName) ?? undefined,
      messageId: msgid,
      timestamp: Number.isFinite(createTime) && createTime > 0 ? createTime * 1_000 : Date.now(),
      text,
      replyTo: quote
        ? {
            id: cleanString(quote.msgid) ?? undefined,
            text: cleanString(quote.text) ?? undefined,
            sender: cleanString(quote.sender) ?? undefined,
          }
        : undefined,
      // WeCom only pushes group traffic to the app when the robot is invoked,
      // so a delivered group message counts as a mention; a quote is a reply.
      trigger: {
        mention: true,
        replyToBot: Boolean(quote),
        command: (text ?? "").startsWith("/"),
      },
    };
  }

  async #downloadInboundImage(message: WecomAppInboundMessage): Promise<NormalizedMedia | null> {
    const limitMb = (await this.#ctx.settings()).imageMaxDownloadMb;
    const maxBytes = limitMb * 1024 * 1024;
    const msgid = cleanString(message.msgid) ?? this.#syntheticId(message);
    try {
      let data: Buffer;
      let mime: string | null = null;
      let name: string | null = null;
      const url = cleanString(message.imageUrl);
      if (url) {
        // 公网直链可能挂死：叠加 10s 超时，且仍随连接停止一起取消。
        const response = await this.#fetch_(url, {
          signal: AbortSignal.any([this.#ctx.signal, AbortSignal.timeout(this.#picDownloadTimeoutMs)]),
        });
        if (!response.ok) throw new WecomAppApiError("download-failed", `图片下载失败（HTTP ${response.status}）`);
        mime = response.headers.get("content-type");
        data = Buffer.from(await response.arrayBuffer());
      } else {
        const mediaId = cleanString(message.imageMediaId);
        if (!mediaId) return null;
        const downloaded = await this.#api.downloadMedia({ mediaId, signal: this.#ctx.signal });
        data = downloaded.data;
        mime = downloaded.mime;
        name = downloaded.filename;
      }
      if (data.length > maxBytes) {
        this.#ctx.log("warn", `图片超过 ${limitMb}MB 下载上限，已降级为无图文本`);
        return null;
      }
      const ext = extensionFor(mime, url);
      const path = join(this.#ctx.mediaDir, `wecom-app-${msgid}${ext}`);
      await mkdir(this.#ctx.mediaDir, { recursive: true });
      await writeFile(path, data, { mode: 0o600 });
      return {
        kind: "image",
        path,
        mime: mime ?? undefined,
        name: name ?? `wecom-app-${msgid}${ext}`,
      };
    } catch (error) {
      this.#ctx.log("warn", `图片下载失败，已降级为无图文本：${error instanceof Error ? error.message : String(error)}`);
      return null;
    }
  }

  #syntheticId(message: WecomAppInboundMessage): string {
    return `${message.createTime ?? Date.now()}-${message.fromUserId ?? "unknown"}-${message.msgtype}`;
  }

  #rememberSeen(msgid: string): void {
    this.#state.seen.push(msgid);
    if (this.#state.seen.length > SEEN_LIMIT) {
      this.#state.seen = this.#state.seen.slice(-SEEN_LIMIT);
    }
    this.#persistState();
  }

  #persistState(): void {
    void this.#chain.run(async () => {
      await atomicWrite(join(this.#ctx.dataDir, "wecom-app-state.json"), this.#state);
    }).catch((error: unknown) => {
      this.#ctx.log("warn", `状态保存失败：${error instanceof Error ? error.message : String(error)}`);
    });
  }
}

/** The callback route embeds the per-connection secret as its last segment;
 * logs must show the path shape without carrying the secret itself. */
function redactCallbackPath(path: string): string {
  return path.replace(/\/[^/]+$/, "/****");
}

function extensionFor(mime: string | null, url: string | null): string {
  const fromMime = mime?.match(/image\/(png|jpe?g|gif|bmp|webp)/iu)?.[1]?.toLowerCase();
  if (fromMime) return `.${fromMime === "jpeg" ? "jpg" : fromMime}`;
  const fromPath = url?.match(/\.(png|jpe?g|gif|bmp|webp)(?:[?#]|$)/iu)?.[1]?.toLowerCase();
  if (fromPath) return `.${fromPath === "jpeg" ? "jpg" : fromPath}`;
  return ".bin";
}

/** Validate and split connect form fields into config entries and secrets;
 * derives the server-generated callbackSecret when absent. */
export function configureWecomApp(fields: Record<string, string>): ConfiguredFields {
  const specs = CHANNEL_SPECS["wecom-app"]!.fields;
  requireFields(specs, fields);
  const { config, secrets } = defaultConfigure("wecom-app", fields);
  const aesKey = secrets.encodingAESKey;
  if (!aesKey || !/^[A-Za-z0-9]{43}$/.test(aesKey)) {
    throw new UserFacingError("EncodingAESKey 不合法：应为 43 位字母或数字，请在企业微信后台“接收消息”配置中复制完整 EncodingAESKey。");
  }
  for (const key of ["callbackBaseUrl", "apiBaseUrl"] as const) {
    const value = config[key];
    if (typeof value === "string" && value && !/^https?:\/\//i.test(value)) {
      throw new UserFacingError(`${key === "callbackBaseUrl" ? "回调基址" : "API 代理地址"}必须是 http(s) 地址。`);
    }
  }
  const providedSecret = typeof config.callbackSecret === "string" ? config.callbackSecret.toLowerCase() : "";
  if (providedSecret) {
    if (!/^[0-9a-f]{32}$/.test(providedSecret)) {
      throw new UserFacingError("callbackSecret 不合法：应为 32 位十六进制字符串。");
    }
    config.callbackSecret = providedSecret;
  } else {
    config.callbackSecret = randomBytes(16).toString("hex");
  }
  return { config, secrets };
}

export function createAdapter(
  ctx: AdapterContext<WecomAppConnection>,
  deps: WecomAppDeps = defaultDeps(),
): ChannelAdapter {
  const config = ctx.config;
  const api = new WecomAppApi({
    corpId: config.corpId,
    corpSecret: ctx.secrets.secret ?? "",
    agentId: config.agentId,
    apiBaseUrl: config.apiBaseUrl,
    fetchImpl: deps.fetch_,
  });
  // Built lazily so an invalid EncodingAESKey surfaces as start()'s
  // user-facing validation error, not a raw SDK constructor throw.
  const cryptoFor: () => WecomCryptoLike = deps.crypto
    ? () => deps.crypto!
    : () => new WecomCrypto(ctx.secrets.token ?? "", ctx.secrets.encodingAESKey ?? "", config.corpId);
  const port = deps.port ?? defaultDeps().port!;
  return new WecomAppAdapter({
    ctx,
    api,
    cryptoFor,
    fetch_: deps.fetch_,
    port,
    replyWindowMs: deps.replyWindowMs,
    healthRetryDelays: deps.healthRetryDelays,
    picDownloadTimeoutMs: deps.picDownloadTimeoutMs,
  });
}

export const definition: ChannelDefinition<WecomAppConnection> = {
  channel: "wecom-app",
  create: (ctx) => createAdapter(ctx),
  configure: configureWecomApp,
};
