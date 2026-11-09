/** Weixin (iLink WeChat bot) channel adapter. Private chats only.
 *
 * Protocol facts ported from xmanrui/dsh-im (MIT) src/channels/weixin/weixin-api.mjs
 * (see ./api.ts). This file wires the protocol into the paseo-im adapter
 * contract: normalize inbound getupdates batches, chunk outbound text,
 * remember per-user context tokens, drive the typing indicator, upload media,
 * and run the QR pairing session. */
import { createHash, randomBytes, randomUUID } from "node:crypto";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { join } from "node:path";
import qrcode from "qrcode";
import { z } from "zod";
import { connectionSchema } from "../../../shared/config.ts";
import { CHANNEL_SPECS } from "../../../shared/channels.ts";
import { UserFacingError } from "../../../shared/errors.ts";
import type {
  ChannelCapabilities,
  NormalizedMedia,
  NormalizedMessage,
  OutboundMedia,
  OutboundText,
  SendResult,
} from "../../../shared/model.ts";
import { atomicWrite, readJson, sleep } from "../../files.ts";
import { splitChunks } from "../../format/chunks.ts";
import { defaultConfigure, requireFields } from "../configure.ts";
import type {
  AdapterContext,
  ChannelAdapter,
  ChannelDefinition,
  ConfiguredFields,
  PairingSession,
  PairingStatus,
} from "../types.ts";
import {
  BASE_INFO,
  DEFAULT_TIMEOUT_MS,
  LONG_POLL_TIMEOUT_MS,
  TEXT_SEND_RETRY_DELAYS_MS,
  TYPING_RETRY_DELAY_MS,
  WEIXIN_MAX_CHUNK_CHARS,
  WEIXIN_QR_BASE_URL,
  WeixinApiError,
  WeixinSendError,
  aesEcbPaddedSize,
  beginLogin,
  createHttp,
  decryptWeixinMedia,
  downloadCdnMedia,
  extractWeixinReplyReference,
  extractWeixinText,
  isRetryableProtocolError,
  isUncertainDeliveryError,
  itemList,
  messageTimestampMs,
  messageIdOf,
  normalizeWeixinBaseUrl,
  nonEmptyString,
  asRecord,
  orderMessages,
  parseWeixinAesKey,
  pollLogin,
  rejectedProviderResponse,
  toSendError,
  toUserFacingError,
  uploadWeixinCdn,
  weixinBaseFromServer,
  weixinCdnDownloadUrl,
  weixinCdnUploadUrl,
  type RawRecord,
  type WeixinHttp,
  type WeixinHttpOptions,
} from "./api.ts";
import { WeixinState } from "./state.ts";

export type WeixinConnection = Extract<z.output<typeof connectionSchema>, { channel: "weixin" }>;

/** Injectable transport and timing seams; defaults use the real network. */
export interface WeixinDeps {
  /** iLink JSON transport: endpoint path relative to the API base. */
  http(path: string, body: unknown, init?: WeixinHttpOptions): Promise<RawRecord>;
  /** Render the QR login image URL as a data URL for the pairing UI. */
  makeQr(qrUrl: string): Promise<string>;
  /** CDN ciphertext download (https GET, redirect-manual). */
  download(url: string | URL, opts: { signal?: AbortSignal; maxBytes: number }): Promise<Buffer>;
  /** Raw CDN upload transport for the encrypted media stream. */
  upload(url: string | URL, init: RequestInit): Promise<Response>;
  now(): number;
  sleep(ms: number, signal?: AbortSignal): Promise<void>;
  typingKeepaliveMs: number;
}

const START_RETRY_DELAYS_MS = [1_000, 3_000, 5_000, 10_000, 30_000] as const;

// qrUrl 是「扫码后打开」的落地链接（实测返回 text/html 落地页而非图片）——
// 本地把它编码成标准二维码 PNG（同 WhatsApp 适配器的 qrcode.toDataURL 模式）。
// 旧实现 fetch 该链接会把 HTML 页面 base64 成 data:text/html 塞给 <Image>，
// 手机端渲染为空白，配对永远出不了码。
function defaultMakeQr(qrUrl: string): Promise<string> {
  return qrcode.toDataURL(qrUrl);
}

export function defaultDeps(ctx: AdapterContext<WeixinConnection>): WeixinDeps {
  const getBaseUrl = (): string => ctx.config.baseUrl ?? WEIXIN_QR_BASE_URL;
  const getToken = (): string | undefined => nonEmptyString(ctx.secrets.token) ?? undefined;
  return {
    http: createHttp({ getBaseUrl, getToken }),
    makeQr: defaultMakeQr,
    download: (url, opts) => downloadCdnMedia(fetch, url, opts),
    upload: (url, init) => fetch(url, init),
    now: () => Date.now(),
    sleep: (ms, signal) => sleep(ms, signal),
    typingKeepaliveMs: 5_000,
  };
}

function sniffImageMime(bytes: Buffer): string | null {
  if (bytes.length < 12) return null;
  if (bytes[0] === 0xff && bytes[1] === 0xd8) return "image/jpeg";
  if (bytes[0] === 0x89 && bytes[1] === 0x50 && bytes[2] === 0x4e && bytes[3] === 0x47) return "image/png";
  if (bytes[0] === 0x47 && bytes[1] === 0x49 && bytes[2] === 0x46) return "image/gif";
  if (bytes.subarray(0, 4).toString("ascii") === "RIFF"
    && bytes.subarray(8, 12).toString("ascii") === "WEBP") return "image/webp";
  return null;
}

const EXT_BY_MIME: Record<string, string> = {
  "image/jpeg": ".jpg",
  "image/png": ".png",
  "image/gif": ".gif",
  "image/webp": ".webp",
};

function sanitizeFilename(name: string): string {
  const cleaned = name
    .replace(/[/\\:*?"<>|]+/g, "")
    // eslint-disable-next-line no-control-regex
    .replace(/[\u0000-\u001f]+/g, "")
    .trim();
  return cleaned.slice(0, 120) || "media";
}

export function createAdapter(
  ctx: AdapterContext<WeixinConnection>,
  overrides: Partial<WeixinDeps> = {},
): WeixinAdapter {
  return new WeixinAdapter(ctx, { ...defaultDeps(ctx), ...overrides });
}

const TYPING_SEND_TIMEOUT_MS = 10_000;

export class WeixinAdapter implements ChannelAdapter {
  readonly channel = "weixin" as const;
  readonly capabilities: ChannelCapabilities = {
    progress: "typing",
    markdown: "plain",
    uploadImage: true,
    uploadFile: true,
    reactions: false,
    typing: true,
    maxChunkChars: WEIXIN_MAX_CHUNK_CHARS,
    threads: false,
  };

  private readonly ctx: AdapterContext<WeixinConnection>;
  private readonly deps: WeixinDeps;
  private readonly state: WeixinState;
  private token: string | null = null;
  private started = false;
  private local: AbortController | null = null;
  private onCtxAbort: (() => void) | null = null;
  private monitorLoop: Promise<void> | null = null;
  private readonly typing = {
    ticket: null as string | null,
    target: null as string | null,
    timer: null as NodeJS.Timeout | null,
    generation: 0,
    retryAt: 0,
    stale: false,
  };

  constructor(ctx: AdapterContext<WeixinConnection>, deps: WeixinDeps) {
    this.ctx = ctx;
    this.deps = deps;
    this.state = new WeixinState(join(ctx.dataDir, "weixin-state.json"));
  }

  async start(): Promise<void> {
    if (this.started) return;
    const token = nonEmptyString(this.ctx.secrets.token);
    if (!token) {
      throw new UserFacingError(
        "缺少微信登录凭据（token）。请在 Paseo → IM 中移除该连接，重新扫码配对微信机器人。",
      );
    }
    try {
      normalizeWeixinBaseUrl(this.ctx.config.baseUrl ?? WEIXIN_QR_BASE_URL);
    } catch {
      throw new UserFacingError(
        "微信连接地址不受信任：必须是 https 且属于 weixin.qq.com 或 wechat.com 域名。请检查连接的 baseUrl 配置。",
      );
    }
    if (this.ctx.signal.aborted) {
      throw new UserFacingError("连接已停止，无法启动微信机器人。");
    }
    await mkdir(this.ctx.mediaDir, { recursive: true });
    await this.state.load();
    await this.state.bindContextTokens(token);
    this.token = token;
    const local = new AbortController();
    this.local = local;
    const onCtxAbort = (): void => local.abort(this.ctx.signal.reason);
    this.onCtxAbort = onCtxAbort;
    this.ctx.signal.addEventListener("abort", onCtxAbort, { once: true });
    try {
      await this.notifyStart(local.signal);
    } catch (error) {
      this.teardownLocal();
      throw toUserFacingError(error);
    }
    this.started = true;
    this.monitorLoop = this.runMonitor(local.signal).catch(() => {
      // Every loop error was already reported through ctx.onError.
    });
  }

  async stop(): Promise<void> {
    const local = this.local;
    this.started = false;
    this.local = null;
    local?.abort(new Error("微信连接已停止"));
    clearTimeout(this.typing.timer ?? undefined);
    this.typing.timer = null;
    await this.monitorLoop?.catch(() => {});
    this.monitorLoop = null;
    this.teardownLocal();
    if (local) {
      const stopSignal = AbortSignal.timeout(5_000);
      await this.stopTypingFor(null, stopSignal).catch(() => {});
      try {
        await this.deps.http("ilink/bot/msg/notifystop", { base_info: BASE_INFO }, {
          timeoutMs: 10_000,
          signal: AbortSignal.timeout(10_000),
        });
      } catch (error) {
        this.ctx.log("warn", `通知微信服务停止失败: ${(error as Error).message}`);
      }
    }
    await this.state.flush();
  }

  /** Credential sanity probe for the settings screen. */
  async testConnection(): Promise<{ ok: boolean; detail?: string }> {
    const token = nonEmptyString(this.ctx.secrets.token);
    if (!token) return { ok: false, detail: "缺少微信登录凭据（token）。" };
    if (this.started) return { ok: true, detail: "微信机器人已连接。" };
    try {
      const response = await this.deps.http("ilink/bot/msg/notifystart", { base_info: BASE_INFO }, {
        timeoutMs: 10_000,
        signal: AbortSignal.timeout(10_000),
      });
      const rejected = rejectedProviderResponse(response, ["errcode", "ret"]);
      if (rejected) {
        return {
          ok: false,
          detail: rejected === "-14"
            ? "微信登录凭据已失效，请重新扫码配对。"
            : `微信服务拒绝了连接测试（ret=${rejected}）。`,
        };
      }
      await this.deps.http("ilink/bot/msg/notifystop", { base_info: BASE_INFO }, {
        timeoutMs: 10_000,
        signal: AbortSignal.timeout(10_000),
      }).catch(() => {});
      return { ok: true, detail: "微信服务连接正常。" };
    } catch (error) {
      return { ok: false, detail: `微信服务连接失败：${(error as Error).message}` };
    }
  }

  private teardownLocal(): void {
    if (this.onCtxAbort) this.ctx.signal.removeEventListener("abort", this.onCtxAbort);
    this.onCtxAbort = null;
  }

  /** Long-poll getupdates forever; transient failures back off until abort. */
  private async runMonitor(signal: AbortSignal): Promise<void> {
    let failures = 0;
    while (!signal.aborted) {
      let response: RawRecord;
      try {
        response = await this.getUpdates(signal);
      } catch (error) {
        if (signal.aborted) return;
        if (error instanceof WeixinApiError && error.code === "stale-token") {
          this.ctx.onError(new UserFacingError(error.message));
          return;
        }
        failures += 1;
        this.ctx.onError(error instanceof Error ? error : new Error(String(error)));
        await this.deps.sleep(START_RETRY_DELAYS_MS[Math.min(failures - 1, START_RETRY_DELAYS_MS.length - 1)], signal);
        continue;
      }
      if (signal.aborted) return;
      failures = 0;
      for (const raw of orderMessages(Array.isArray(response.msgs) ? response.msgs : [])) {
        if (signal.aborted) return;
        try {
          await this.accept(raw, signal);
        } catch (error) {
          this.ctx.log("error", `处理微信消息失败: ${(error as Error).message}`);
        }
      }
      const buf = nonEmptyString(response.get_updates_buf);
      if (buf) await this.state.setGetUpdatesBuf(buf);
    }
  }

  /** getupdates long poll; a timeout is just an empty batch. */
  private async getUpdates(signal: AbortSignal): Promise<RawRecord> {
    try {
      const response = await this.deps.http("ilink/bot/getupdates", {
        get_updates_buf: this.state.getUpdatesBuf(),
        base_info: BASE_INFO,
      }, { timeoutMs: LONG_POLL_TIMEOUT_MS, signal });
      const rejected = rejectedProviderResponse(response);
      if (rejected) {
        throw new WeixinApiError(
          rejected === "-14" ? "stale-token" : "updates-rejected",
          rejected === "-14"
            ? "微信登录凭据已失效，请移除该连接后重新扫码配对。"
            : "微信消息同步请求被拒绝。",
          { providerCode: rejected },
        );
      }
      return response;
    } catch (error) {
      if (error instanceof WeixinApiError && error.code === "timeout") {
        return { ret: 0, msgs: [], get_updates_buf: this.state.getUpdatesBuf() };
      }
      throw error;
    }
  }

  private async notifyStart(signal: AbortSignal): Promise<void> {
    for (let attempt = 0; ; attempt += 1) {
      try {
        const response = await this.deps.http("ilink/bot/msg/notifystart", { base_info: BASE_INFO }, {
          timeoutMs: 10_000,
          signal,
        });
        const rejected = rejectedProviderResponse(response, ["errcode", "ret"]);
        if (rejected) {
          throw new WeixinApiError(
            rejected === "-14" ? "stale-token" : "start-rejected",
            rejected === "-14"
              ? "微信登录凭据已失效，请移除该连接后重新扫码配对。"
              : "微信账号连接启动失败，请稍后重试。",
            { providerCode: rejected },
          );
        }
        return;
      } catch (error) {
        if (!isRetryableProtocolError(error) || signal.aborted) {
          const fatal = toUserFacingError(error);
          this.ctx.onError(fatal);
          throw fatal;
        }
        this.ctx.onError(error instanceof Error ? error : new Error(String(error)));
        await this.deps.sleep(START_RETRY_DELAYS_MS[Math.min(attempt, START_RETRY_DELAYS_MS.length - 1)], signal);
      }
    }
  }

  /** One inbound iLink message: dedupe, download media, emit or drop. */
  private async accept(raw: unknown, signal: AbortSignal): Promise<void> {
    const message = asRecord(raw);
    if (!message) return;
    if (message.message_type === 2) return; // echo of the bot's own message
    const messageId = messageIdOf(message);
    const sender = nonEmptyString(message.from_user_id);
    if (!messageId || !sender) return;
    if (this.state.hasSeen(messageId)) return;

    let text = extractWeixinText(message);
    const { media, dropped } = await this.collectMedia(message, messageId, signal);
    if (!text && media.length === 0 && dropped > 0) {
      // 有界重试后仍下载失败：占位而非静默丢弃（静默丢弃会随游标推进永久丢失）
      text = "[附件下载失败，请让对方重发]";
    }
    if (!text && media.length === 0) {
      // No text and no downloadable media: noise, but do not reprocess it.
      await this.state.markSeen(messageId);
      return;
    }
    const now = this.deps.now();
    const contextToken = nonEmptyString(message.context_token);
    const runId = nonEmptyString(message.run_id);
    if (contextToken) {
      void this.state.rememberContextToken({
        userId: sender,
        contextToken,
        seq: message.seq,
        messageTimeMs: this.timestampOf(message, messageId, now),
      }).catch((error) => this.ctx.log("warn", `保存微信会话凭据失败: ${(error as Error).message}`));
    }
    if (runId) {
      void this.state.rememberInboundRun(messageId, sender, runId)
        .catch((error) => this.ctx.log("warn", `记录微信消息上下文失败: ${(error as Error).message}`));
    }
    await this.state.markSeen(messageId);
    const replyTo = extractWeixinReplyReference(message);
    this.ctx.onMessage({
      channel: "weixin",
      connectionId: this.ctx.connectionId,
      kind: "dm",
      chatId: sender,
      senderId: sender,
      messageId,
      timestamp: this.timestampOf(message, messageId, now),
      text: text ?? undefined,
      media: media.length > 0 ? media : undefined,
      replyTo: replyTo ?? undefined,
      trigger: {
        mention: false,
        replyToBot: false,
        command: typeof text === "string" && text.startsWith("/"),
      },
    });
  }

  private timestampOf(message: RawRecord, messageId: string, now: number): number {
    const createTime = typeof message.create_time_ms === "number"
      && Number.isFinite(message.create_time_ms)
      ? message.create_time_ms
      : null;
    return createTime ?? messageTimestampMs(messageId, now) ?? now;
  }

  /** Download and decrypt image/file items into ctx.mediaDir. */
  private async collectMedia(
    message: RawRecord,
    messageId: string,
    signal: AbortSignal,
  ): Promise<{ media: NormalizedMedia[]; dropped: number }> {
    const items = itemList(message);
    if (items.length === 0) return { media: [], dropped: 0 };
    const collected: NormalizedMedia[] = [];
    let dropped = 0;
    let imageCount = 0;
    let fileCount = 0;
    for (const item of items) {
      const imageItem = asRecord(item.image_item);
      const fileItem = asRecord(item.file_item);
      const isImage = imageItem !== null;
      if (!isImage && fileItem === null) continue;
      const source = (isImage ? imageItem : fileItem)!;
      const index = isImage ? (imageCount += 1) : (fileCount += 1);
      const declaredName = nonEmptyString(fileItem?.file_name);
      const label = isImage
        ? (imageCount === 1 ? "image" : `image-${imageCount}`)
        : (declaredName ?? (fileCount === 1 ? "file" : `file-${fileCount}`));
      // 策略性跳过（不受信任地址/缺 AES key 之外的字段校验）：不计 dropped，不触发占位
      let key: ReturnType<typeof parseWeixinAesKey>;
      let url: string | URL;
      try {
        key = parseWeixinAesKey(source);
        url = weixinCdnDownloadUrl(asRecord(source.media));
      } catch (error) {
        this.ctx.log("warn", `跳过不可解析的微信媒体项: ${(error as Error).message}`);
        continue;
      }
      try {
        let maxBytes = 30 * 1024 * 1024;
        try {
          const settings = await this.ctx.settings();
          maxBytes = Math.max(1, settings.imageMaxDownloadMb) * 1024 * 1024;
        } catch {
          // Settings unavailable: fall back to the default cap.
        }
        const ciphertext = await this.deps.download(url, {
          signal,
          maxBytes: maxBytes + (isImage ? 16 : 0), // 文件与图片同受 settings 上限约束
        });
        const plain = decryptWeixinMedia(ciphertext, key);
        const stem = `weixin-${sanitizeFilename(messageId)}-${index}`;
        const name = isImage
          ? `${stem}-${label}${EXT_BY_MIME[sniffImageMime(plain) ?? ""] ?? ""}`
          : `${stem}-${sanitizeFilename(label)}`;
        const path = join(this.ctx.mediaDir, name);
        await writeFile(path, plain);
        collected.push({
          kind: isImage ? "image" : "file",
          path,
          mime: isImage ? sniffImageMime(plain) ?? undefined : undefined,
          name: isImage ? label : declaredName ?? label,
        });
      } catch (error) {
        this.ctx.log("warn", `下载微信${isImage ? "图片" : "文件"}失败: ${(error as Error).message}`);
        dropped += 1;
      }
    }
    return { media: collected, dropped };
  }

  async sendText(outbound: OutboundText): Promise<SendResult> {
    const signal = this.assertReady();
    const toUserId = nonEmptyString(outbound.chatId);
    if (!toUserId) throw new UserFacingError("缺少发送目标（微信用户 ID）。");
    const text = nonEmptyString(outbound.text);
    if (!text) return {};
    signal.throwIfAborted();
    await this.stopTypingFor(toUserId, signal);
    const chunks = splitChunks(text, this.capabilities.maxChunkChars);
    const contextToken = this.state.contextTokenFor(toUserId);
    const replyToId = nonEmptyString(outbound.replyToId ?? "");
    const runId = replyToId ? this.state.runForReply(toUserId, replyToId) : undefined;
    let firstClientId: string | undefined;
    for (const chunk of chunks) {
      signal.throwIfAborted();
      const clientId = `paseo-weixin-${randomUUID()}`;
      firstClientId ??= clientId;
      try {
        await this.sendTextChunk(toUserId, chunk, clientId, contextToken, runId, signal);
      } catch (error) {
        // Ambiguous outcomes (no HTTP verdict / 5xx) may have been delivered.
        if (isUncertainDeliveryError(error)) return { uncertain: true };
        throw toSendError(error);
      }
    }
    return { messageId: firstClientId };
  }

  /** One chunk: retry only network-error (no HTTP verdict) with short delays,
   * keeping the same client_id across attempts. */
  private async sendTextChunk(
    toUserId: string,
    text: string,
    clientId: string,
    contextToken: string | undefined,
    runId: string | undefined,
    signal: AbortSignal,
  ): Promise<void> {
    const body = {
      msg: {
        from_user_id: "",
        to_user_id: toUserId,
        client_id: clientId,
        message_type: 2,
        message_state: 2,
        item_list: [{ type: 1, text_item: { text } }],
        ...(contextToken ? { context_token: contextToken } : {}),
        ...(runId ? { run_id: runId } : {}),
      },
      base_info: BASE_INFO,
    };
    for (let attempt = 0; ; attempt += 1) {
      try {
        const response = await this.deps.http("ilink/bot/sendmessage", body, {
          timeoutMs: DEFAULT_TIMEOUT_MS,
          signal,
        });
        // 回发观测点：排障「重复消息/静默丢失」时以本行计数与 client_id 为准。
        this.ctx.log("info", `sendmessage ok to=${toUserId} client=${clientId} bytes=${text.length}`);
        const rejected = rejectedProviderResponse(response);
        if (rejected) {
          throw new WeixinApiError("send-rejected", "微信服务拒绝了回复消息。", { providerCode: rejected });
        }
        return;
      } catch (error) {
        const delay = TEXT_SEND_RETRY_DELAYS_MS[attempt];
        if (delay === undefined || !(error instanceof WeixinApiError)
          || error.code !== "network-error" || signal.aborted) {
          throw error;
        }
        await this.deps.sleep(delay, signal);
      }
    }
  }

  async sendMedia(outbound: OutboundMedia): Promise<SendResult> {
    const signal = this.assertReady();
    const toUserId = nonEmptyString(outbound.chatId);
    if (!toUserId) throw new UserFacingError("缺少发送目标（微信用户 ID）。");
    signal.throwIfAborted();
    let bytes: Buffer;
    try {
      bytes = await readFile(outbound.path);
    } catch (error) {
      throw new UserFacingError(`无法读取待发送的${outbound.kind === "image" ? "图片" : "文件"}: ${(error as Error).message}`);
    }
    const isImage = outbound.kind === "image";
    const mediaType = isImage ? 1 : 3;
    const fileKey = randomBytes(16).toString("hex");
    const aesKey = randomBytes(16);
    const rawMd5 = createHash("md5").update(bytes).digest("hex");
    const ciphertextSize = aesEcbPaddedSize(bytes.byteLength);
    const fileName = sanitizeFilename(nonEmptyString(outbound.name) ?? (isImage ? "image" : "file"));
    const contextToken = this.state.contextTokenFor(toUserId);
    await this.stopTypingFor(toUserId, signal);
    const clientId = `paseo-weixin-${randomUUID()}`;
    try {
      const upload = await this.deps.http("ilink/bot/getuploadurl", {
        filekey: fileKey,
        media_type: mediaType,
        to_user_id: toUserId,
        rawsize: bytes.byteLength,
        rawfilemd5: rawMd5,
        filesize: ciphertextSize,
        no_need_thumb: true,
        aeskey: aesKey.toString("hex"),
        base_info: BASE_INFO,
      }, { timeoutMs: DEFAULT_TIMEOUT_MS, signal });
      const uploadRejection = rejectedProviderResponse(upload);
      if (uploadRejection) {
        throw new WeixinSendError(
          `微信服务拒绝了文件上传请求（ret=${uploadRejection}）。`,
          "upload-url-rejected",
          uploadRejection,
        );
      }
      const uploadUrl = weixinCdnUploadUrl(upload, fileKey);
      let downloadParam: string;
      try {
        downloadParam = await uploadWeixinCdn(this.deps.upload, uploadUrl, bytes, aesKey, { signal });
      } catch (error) {
        throw toSendError(error instanceof WeixinApiError
          ? error
          : new WeixinApiError("upload-failed", "微信文件上传失败。", { cause: error }));
      }
      signal.throwIfAborted();
      const media = {
        encrypt_query_param: downloadParam,
        aes_key: Buffer.from(aesKey.toString("hex")).toString("base64"),
        encrypt_type: 1,
      };
      const item = isImage
        ? { type: 2, image_item: { media, mid_size: ciphertextSize } }
        : { type: 4, file_item: { media, file_name: fileName, len: String(bytes.byteLength) } };
      const response = await this.deps.http("ilink/bot/sendmessage", {
        msg: {
          from_user_id: "",
          to_user_id: toUserId,
          client_id: clientId,
          message_type: 2,
          message_state: 2,
          item_list: [item],
          ...(contextToken ? { context_token: contextToken } : {}),
        },
        base_info: BASE_INFO,
      }, { timeoutMs: DEFAULT_TIMEOUT_MS, signal });
      const sendRejection = rejectedProviderResponse(response);
      if (sendRejection) {
        throw new WeixinSendError("微信服务拒绝了文件消息。", "send-rejected", sendRejection);
      }
    } catch (error) {
      // 与 sendText 同款：无 HTTP 结论/5xx 的模糊失败可能已送达，返回 uncertain。
      if (isUncertainDeliveryError(error)) return { uncertain: true };
      throw toSendError(error);
    }
    if (nonEmptyString(outbound.caption ?? "")) {
      // iLink media messages carry no caption: send it as a follow-up text.
      try {
        await this.sendText({ chatId: toUserId, text: outbound.caption!.trim() });
      } catch (error) {
        this.ctx.log("warn", `发送微信媒体说明文字失败: ${(error as Error).message}`);
      }
    }
    return { messageId: clientId };
  }

  async sendTyping(chatId: string): Promise<void> {
    const local = this.local;
    if (!this.started || !local || local.signal.aborted) return;
    const target = nonEmptyString(chatId);
    if (!target || this.deps.now() < this.typing.retryAt) return;
    if (this.typing.target === target && this.typing.ticket && this.typing.timer) return; // keepalive already running
    const generation = this.typing.generation + 1;
    this.typing.generation = generation;
    try {
      let ticket = this.typing.ticket;
      if (!ticket) {
        const contextToken = this.state.contextTokenFor(target);
        const response = await this.deps.http("ilink/bot/getconfig", {
          ilink_user_id: target,
          ...(contextToken ? { context_token: contextToken } : {}),
          base_info: BASE_INFO,
        }, { timeoutMs: TYPING_SEND_TIMEOUT_MS, signal: local.signal });
        const ret = response.ret;
        if (ret !== undefined && ret !== 0) {
          throw new WeixinApiError("config-rejected", "微信服务拒绝了机器人配置请求。");
        }
        ticket = nonEmptyString(response.typing_ticket);
        if (!ticket) {
          this.typing.retryAt = this.deps.now() + TYPING_RETRY_DELAY_MS;
          return;
        }
        if (generation !== this.typing.generation) return;
        this.typing.ticket = ticket;
      }
      this.typing.target = target;
      await this.deps.http("ilink/bot/sendtyping", {
        ilink_user_id: target,
        typing_ticket: ticket,
        status: 1,
        base_info: BASE_INFO,
      }, { timeoutMs: TYPING_SEND_TIMEOUT_MS, signal: local.signal });
      if (generation !== this.typing.generation) return;
      this.typing.stale = false;
      this.typing.retryAt = 0;
      this.scheduleTypingKeepalive(generation);
    } catch (error) {
      if (generation !== this.typing.generation || local.signal.aborted) return;
      this.clearTypingTimer();
      this.typing.stale = Boolean(this.typing.target && this.typing.ticket);
      this.typing.retryAt = this.deps.now() + TYPING_RETRY_DELAY_MS;
      this.ctx.log("warn", `发送微信输入状态失败: ${(error as Error).message}`);
    }
  }

  /** Typing keepalive: resend status 1 until cancelled or failed. */
  private scheduleTypingKeepalive(generation: number): void {
    this.clearTypingTimer();
    const local = this.local;
    if (generation !== this.typing.generation || !this.typing.target || !local) return;
    const timer = setTimeout(() => {
      if (this.typing.timer === timer) this.typing.timer = null;
      void (async () => {
        if (generation !== this.typing.generation || !this.typing.target || !this.local) return;
        const target = this.typing.target;
        const ticket = this.typing.ticket;
        if (!target || !ticket) return;
        await this.deps.http("ilink/bot/sendtyping", {
          ilink_user_id: target,
          typing_ticket: ticket,
          status: 1,
          base_info: BASE_INFO,
        }, { timeoutMs: TYPING_SEND_TIMEOUT_MS, signal: this.local.signal });
        this.scheduleTypingKeepalive(generation);
      })().catch((error) => {
        if (generation !== this.typing.generation || !this.local || this.local.signal.aborted) return;
        this.typing.stale = Boolean(this.typing.target && this.typing.ticket);
        this.typing.retryAt = this.deps.now() + TYPING_RETRY_DELAY_MS;
        this.ctx.log("warn", `微信输入状态保活失败: ${(error as Error).message}`);
      });
    }, this.deps.typingKeepaliveMs);
    timer.unref?.();
    this.typing.timer = timer;
  }

  /** Send typing status 2 (cancel). `toUserId` null cancels any active typing. */
  private async stopTypingFor(toUserId: string | null, signal: AbortSignal): Promise<void> {
    const target = this.typing.target;
    const ticket = this.typing.ticket;
    if (!target || !ticket || (toUserId !== null && target !== toUserId)) return;
    this.typing.generation += 1;
    this.clearTypingTimer();
    this.typing.target = null;
    const stale = this.typing.stale;
    try {
      await this.deps.http("ilink/bot/sendtyping", {
        ilink_user_id: target,
        typing_ticket: ticket,
        status: 2,
        base_info: BASE_INFO,
      }, { timeoutMs: 5_000, signal });
    } catch (error) {
      if (!signal.aborted) {
        this.ctx.log("warn", `取消微信输入状态失败: ${(error as Error).message}`);
      }
    } finally {
      if (stale && this.typing.ticket === ticket) this.typing.ticket = null;
      this.typing.stale = false;
    }
  }

  private clearTypingTimer(): void {
    clearTimeout(this.typing.timer ?? undefined);
    this.typing.timer = null;
  }

  private assertReady(): AbortSignal {
    const local = this.local;
    if (!this.started || !local) {
      throw new UserFacingError("微信机器人未连接，无法发送消息。");
    }
    return local.signal;
  }
}

export interface WeixinPairingSession extends PairingSession {
  /** Submit the pairing code shown in WeChat (additive, optional). */
  submitVerification?(code: string): Promise<void>;
}

export interface WeixinPairingOverrides {
  http?: WeixinHttp;
  makeQr?(qrUrl: string): Promise<string>;
  fetchImpl?: typeof fetch;
}

interface PairingDoc {
  version: 1;
  phase: "idle" | "pending" | "scanned" | "ok" | "expired" | "failed";
  qrcode?: string;
  qrUrl?: string;
  base?: string;
  verifyCode?: string;
  fields?: Record<string, string>;
  error?: string;
}

const PAIRING_PHASES: Record<string, true> = {
  idle: true,
  pending: true,
  scanned: true,
  ok: true,
  expired: true,
  failed: true,
};

function normalizePairingDoc(value: unknown): PairingDoc {
  const record = asRecord(value);
  if (!record || record.version !== 1) return { version: 1, phase: "idle" };
  const phase = typeof record.phase === "string" && PAIRING_PHASES[record.phase]
    ? (record.phase as PairingDoc["phase"])
    : "idle";
  const fields = asRecord(record.fields);
  return {
    version: 1,
    phase,
    qrcode: nonEmptyString(record.qrcode) ?? undefined,
    qrUrl: nonEmptyString(record.qrUrl) ?? undefined,
    base: nonEmptyString(record.base) ?? undefined,
    verifyCode: nonEmptyString(record.verifyCode) ?? undefined,
    fields: fields
      ? Object.fromEntries(Object.entries(fields).map(([key, entry]) => [key, String(entry)]))
      : undefined,
    error: nonEmptyString(record.error) ?? undefined,
  };
}

/** QR pairing session: startLogin → qr → pollLogin(verifyCode) → token. State
 * lives in the connection dataDir so polling survives restarts. */
export function createPairingSession(
  _connectionId: string,
  dataDir: string,
  overrides: WeixinPairingOverrides = {},
): WeixinPairingSession {
  const statePath = join(dataDir, "weixin-pairing.json");
  let doc: PairingDoc = { version: 1, phase: "idle" };
  const http = overrides.http ?? createHttp({
    getBaseUrl: () => doc.base ?? WEIXIN_QR_BASE_URL,
    fetchImpl: overrides.fetchImpl,
  });
  const makeQr = overrides.makeQr ?? defaultMakeQr;
  let controller: AbortController | null = null;
  let qrDataUrlCache: string | null = null;

  async function persist(): Promise<void> {
    await atomicWrite(statePath, doc);
  }

  async function load(): Promise<void> {
    const loaded = await readJson(statePath, normalizePairingDoc);
    if (loaded) doc = loaded;
  }

  async function qrDataUrl(): Promise<string | undefined> {
    if (qrDataUrlCache) return qrDataUrlCache;
    if (!doc.qrUrl) return undefined;
    try {
      qrDataUrlCache = await makeQr(doc.qrUrl);
    } catch {
      qrDataUrlCache = null;
      return undefined;
    }
    return qrDataUrlCache;
  }

  // —— 非阻塞交付：poll() 立即返回当前状态与二维码；pollLogin 长轮询转后台单飞。——
  // 旧行为在 poll() 里同步 await pollLogin（~30s）：客户端 1s 一发会堆起几十个并发
  // 长轮询，且首个二维码要 30s 后才送达（实机「一直准备中、无二维码」的根因）。
  let lastStatus: PairingStatus | null = null;
  let backgroundLogin: Promise<void> | null = null;
  let waitingVerifyCode = false;
  let stopped = false;

  function applyLoginResponse(response: RawRecord): void {
    const status = response.status as string;
    if (status === "wait") {
      doc.phase = "pending";
      lastStatus = { status: "pending" };
      return;
    }
    if (status === "scaned" || status === "scaned_but_redirect") {
      if (status === "scaned_but_redirect") {
        doc.base = weixinBaseFromServer(response.redirect_host, doc.base ?? WEIXIN_QR_BASE_URL);
      }
      doc.phase = "scanned";
      doc.verifyCode = undefined;
      lastStatus = { status: "scanned" };
      return;
    }
    if (status === "need_verifycode") {
      doc.phase = "pending";
      doc.verifyCode = undefined;
      // 暂停后台轮询：不带配对码的空转会触发 verify_code_blocked 锁死，
      // 必须等用户经 submitVerification 提交配对码后再继续。
      waitingVerifyCode = true;
      lastStatus = {
        status: "pending",
        detail: "微信要求配对码验证：请在微信中查看配对码，并在配对界面提交后继续。",
      };
      return;
    }
    if (status === "verify_code_blocked") {
      doc.phase = "failed";
      doc.error = "配对码多次错误，请重新扫码生成新的二维码。";
      lastStatus = { status: "failed", detail: doc.error };
      return;
    }
    if (status === "expired") {
      doc.phase = "expired";
      lastStatus = { status: "expired" };
      return;
    }
    if (status === "binded_redirect") {
      doc.phase = "failed";
      doc.error = "该微信账号已绑定其他客户端，请先解除绑定后重试。";
      lastStatus = { status: "failed", detail: doc.error };
      return;
    }
    const token = nonEmptyString(response.bot_token);
    const accountId = nonEmptyString(response.ilink_bot_id);
    const ownerUserId = nonEmptyString(response.ilink_user_id);
    if (!token || !accountId || !ownerUserId) {
      throw new WeixinApiError("incomplete-login", "微信授权成功，但返回的账号凭据不完整。");
    }
    doc.base = weixinBaseFromServer(response.baseurl, doc.base ?? WEIXIN_QR_BASE_URL);
    doc.phase = "ok";
    doc.fields = { token, accountId, ownerUserId, baseUrl: doc.base };
    doc.verifyCode = undefined;
    lastStatus = { status: "ok", fields: doc.fields };
  }

  async function refreshLoginOnce(): Promise<void> {
    const qrcode = doc.qrcode;
    if (!qrcode) return;
    controller = new AbortController();
    try {
      const response = await pollLogin(http, {
        qrcode,
        verifyCode: doc.verifyCode,
        signal: controller.signal,
      });
      applyLoginResponse(response);
      await persist();
    } catch (error) {
      if (!controllerAborted(error)) {
        doc.phase = "failed";
        doc.error = error instanceof WeixinApiError
          ? error.message
          : error instanceof Error
            ? error.message
            : "微信配对失败。";
        lastStatus = { status: "failed", detail: doc.error };
        // 尽力落盘：测试清理/连接删除后 dataDir 可能已不存在，落盘失败不该炸后台任务。
        await persist().catch(() => {});
      }
    } finally {
      controller = null;
    }
  }

  function ensureBackgroundLogin(): void {
    if (backgroundLogin || stopped || waitingVerifyCode) return;
    if (doc.phase === "ok" || doc.phase === "expired" || doc.phase === "failed") return;
    backgroundLogin = (async () => {
      try {
        while (!stopped && !waitingVerifyCode && (doc.phase === "pending" || doc.phase === "scanned")) {
          await refreshLoginOnce();
          // pollLogin 本身是微信侧长轮询（~25-30s）；仅在瞬时失败时补 1s 节流防打爆。
          if (doc.phase === "pending") await sleep(1000).catch(() => {});
        }
      } finally {
        backgroundLogin = null;
      }
    })();
  }

  const session: WeixinPairingSession = {
    async start(): Promise<void> {
      try {
        await load();
        // 仅续接进行中的配对（等待扫码/已扫码）；expired/failed 复位重开新登录，
        // 否则「重新扫码」会永远续接一张已过期的死二维码。
        if (doc.qrcode && (doc.phase === "pending" || doc.phase === "scanned")) return;
        const credentials = await beginLogin(http, { signal: controller?.signal });
        doc = { version: 1, phase: "pending", qrcode: credentials.qrcode, qrUrl: credentials.qrUrl, base: WEIXIN_QR_BASE_URL };
        await persist();
      } catch (error) {
        controller?.abort(new Error("配对已取消"));
        if (error instanceof UserFacingError) throw error;
        if (error instanceof WeixinApiError) throw new UserFacingError(error.message);
        throw error;
      }
    },

    async poll(): Promise<PairingStatus> {
      try {
        if (!doc.qrcode) await session.start();
        if (doc.phase === "ok") return { status: "ok", fields: doc.fields ?? {} };
        if (doc.phase === "expired") return { status: "expired" };
        if (doc.phase === "failed") return { status: "failed", detail: doc.error ?? "微信配对失败。" };
        ensureBackgroundLogin();
        const dataUrl = await qrDataUrl();
        if (doc.phase === "scanned") return { status: "scanned" };
        const base: PairingStatus =
          lastStatus && lastStatus.status === "pending" ? lastStatus : { status: "pending" };
        return {
          ...base,
          ...(dataUrl ? { qrDataUrl: dataUrl } : {}),
        };
      } catch (error) {
        if (error instanceof UserFacingError) throw error;
        if (error instanceof WeixinApiError) throw new UserFacingError(error.message);
        throw error;
      }
    },

    async stop(): Promise<void> {
      stopped = true;
      controller?.abort(new Error("配对已取消"));
      controller = null;
      // 等后台单飞彻底静止再返回：调用方（manager 停会话/测试清理）之后才可安全
      // 删除 dataDir，杜绝幽灵 persist 与目录删除竞态。
      await backgroundLogin?.catch(() => {});
    },

    async submitVerification(code: string): Promise<void> {
      const value = nonEmptyString(code);
      if (!value) throw new UserFacingError("配对码不能为空。");
      await load();
      doc.verifyCode = value;
      await persist();
      // 解除 need_verifycode 暂停，携带配对码恢复后台长轮询。
      waitingVerifyCode = false;
      lastStatus = null;
      ensureBackgroundLogin();
    },
  };
  return session;
}

function controllerAborted(error: unknown): boolean {
  return error instanceof Error && error.name === "AbortError";
}

export const definition: ChannelDefinition<WeixinConnection> = {
  channel: "weixin",
  create: (ctx) => createAdapter(ctx),
  configure(fields: Record<string, string>): ConfiguredFields {
    requireFields(CHANNEL_SPECS.weixin.fields, fields);
    const { config, secrets } = defaultConfigure("weixin", fields);
    if (!secrets.token) {
      throw new UserFacingError("缺少微信登录凭据（token），请通过扫码配对获取。");
    }
    const accountId = nonEmptyString(config.accountId);
    if (!accountId) {
      throw new UserFacingError("缺少微信账号 ID（accountId），请通过扫码配对获取。");
    }
    const result: Record<string, unknown> = { accountId };
    const ownerUserId = nonEmptyString(config.ownerUserId);
    if (ownerUserId) result.ownerUserId = ownerUserId;
    const baseUrl = nonEmptyString(config.baseUrl);
    if (baseUrl) {
      try {
        normalizeWeixinBaseUrl(baseUrl);
      } catch {
        throw new UserFacingError(
          "微信连接地址不受信任：必须是 https 且属于 weixin.qq.com 或 wechat.com 域名。",
        );
      }
      result.baseUrl = baseUrl;
    }
    return { config: result, secrets };
  },
  pairing: (connectionId, dataDir) => createPairingSession(connectionId, dataDir),
};
