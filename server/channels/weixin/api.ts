/** iLink WeChat bot protocol client for the weixin channel adapter.
 *
 * Protocol facts ported from xmanrui/dsh-im (MIT) src/channels/weixin/weixin-api.mjs:
 * endpoints, request/response body shapes, hex/base64 AES key handling, CDN
 * URL trust rules, login status names, error classification, and the
 * X-WECHAT-UIN header quirk all follow that implementation. */
import { createCipheriv, createDecipheriv, randomBytes } from "node:crypto";
import { UserFacingError } from "../../../shared/errors.ts";

export const WEIXIN_QR_BASE_URL = "https://ilinkai.weixin.qq.com/";
export const WEIXIN_PROTOCOL_VERSION = "2.4.6";
export const WEIXIN_BOT_TYPE = "3";
export const WEIXIN_CDN_BASE_URL = "https://novac2c.cdn.weixin.qq.com/c2c";
export const WEIXIN_CDN_HOST = "novac2c.cdn.weixin.qq.com";
/** iLink text messages are capped at 1800 chars before the platform mangles them. */
export const WEIXIN_MAX_CHUNK_CHARS = 1_800;

const ILINK_APP_ID = "bot";
const ILINK_CLIENT_VERSION = String((2 << 16) | (4 << 8) | 6);
export const DEFAULT_TIMEOUT_MS = 15_000;
export const LONG_POLL_TIMEOUT_MS = 35_000;
export const TEXT_SEND_RETRY_DELAYS_MS = [400, 1_200] as const;
const UPLOAD_RETRIES = 3;
const UPLOAD_IDLE_TIMEOUT_MS = 60_000;
const UPLOAD_CHUNK_BYTES = 64 * 1024;
const MESSAGE_ID_TIMESTAMP_SHIFT = 22n;
const MESSAGE_ID_MIN_TIMESTAMP_MS = Date.UTC(2020, 0, 1);
const MESSAGE_ID_MAX_FUTURE_MS = 24 * 60 * 60 * 1_000;
/** getconfig/sendtyping/notifystart failures back off before the next attempt. */
export const TYPING_RETRY_DELAY_MS = 60_000;

/** Status names returned by ilink/bot/get_qrcode_status. */
const LOGIN_STATUSES: Record<string, true> = {
  wait: true,
  scaned: true,
  confirmed: true,
  expired: true,
  scaned_but_redirect: true,
  need_verifycode: true,
  verify_code_blocked: true,
  binded_redirect: true,
};

/** Sent with every iLink bot request. */
export const BASE_INFO: Readonly<{ channel_version: string; bot_agent: string }> = Object.freeze({
  channel_version: WEIXIN_PROTOCOL_VERSION,
  bot_agent: "DeepSeekHarness/1.1.0",
});

export type RawRecord = Record<string, unknown>;

export class WeixinApiError extends Error {
  readonly code: string;
  readonly status?: number;
  readonly providerCode?: string;

  constructor(
    code: string,
    message: string,
    options: { status?: number; providerCode?: string; cause?: unknown } = {},
  ) {
    super(message, options.cause !== undefined ? { cause: options.cause } : undefined);
    this.name = "WeixinApiError";
    this.code = code;
    if (options.status !== undefined) this.status = options.status;
    if (options.providerCode !== undefined) this.providerCode = options.providerCode;
  }
}

/** Send failures shown to users in UI and on IM carry a Chinese message. */
export class WeixinSendError extends UserFacingError {
  readonly code: string;
  readonly providerCode?: string;

  constructor(message: string, code: string, providerCode?: string, options?: { cause?: unknown }) {
    super(message, options);
    this.name = "WeixinSendError";
    this.code = code;
    if (providerCode !== undefined) this.providerCode = providerCode;
  }
}

export function asRecord(value: unknown): RawRecord | null {
  return value !== null && typeof value === "object" && !Array.isArray(value)
    ? (value as RawRecord)
    : null;
}

export function nonEmptyString(value: unknown): string | null {
  if (typeof value !== "string") return null;
  const text = value.trim();
  return text ? text : null;
}

function num(value: unknown): number | null {
  return typeof value === "number" && Number.isFinite(value) ? value : null;
}

/** First non-zero ret/errcode field of a response document, or null when accepted. */
export function rejectedProviderResponse(value: unknown, fields: string[] = ["ret", "errcode"]): string | null {
  const record = asRecord(value);
  if (!record) return null;
  for (const field of fields) {
    const raw = record[field];
    if (raw !== undefined && raw !== 0 && raw !== "0") {
      return safeProviderCode(raw) ?? "rejected";
    }
  }
  return null;
}

function safeProviderCode(value: unknown): string | undefined {
  const code = value === undefined || value === null ? null : String(value).trim();
  return code && /^-?[A-Za-z0-9_.:-]{1,160}$/.test(code) ? code : undefined;
}

function strictBase64(value: unknown): Buffer | null {
  const text = nonEmptyString(value);
  if (!text || text.length % 4 !== 0 || !/^[A-Za-z0-9+/]+={0,2}$/.test(text)) return null;
  return Buffer.from(text, "base64");
}

/** Parse the two AES key encodings used by Weixin iLink media items. */
export function parseWeixinAesKey(item: RawRecord): Buffer {
  const directHex = nonEmptyString(item.aeskey);
  if (directHex) {
    if (!/^[0-9a-fA-F]{32}$/.test(directHex)) {
      throw new WeixinApiError("invalid-image-key", "微信媒体的加密密钥无效。");
    }
    return Buffer.from(directHex, "hex");
  }
  const media = asRecord(item.media);
  const encoded = strictBase64(media?.aes_key);
  if (encoded?.length === 16) return encoded;
  if (encoded?.length === 32 && /^[0-9a-fA-F]{32}$/.test(encoded.toString("ascii"))) {
    return Buffer.from(encoded.toString("ascii"), "hex");
  }
  throw new WeixinApiError("invalid-image-key", "微信媒体的加密密钥无效。");
}

/** AES-128-ECB decrypt of a CDN media ciphertext (PKCS#7 padded). */
export function decryptWeixinMedia(ciphertext: Buffer, key: Buffer): Buffer {
  if (key.length !== 16 || ciphertext.length === 0 || ciphertext.length % 16 !== 0) {
    throw new WeixinApiError("invalid-image-ciphertext", "微信媒体的加密数据无效。");
  }
  try {
    const decipher = createDecipheriv("aes-128-ecb", key, null);
    return Buffer.concat([decipher.update(ciphertext), decipher.final()]);
  } catch (error) {
    throw new WeixinApiError("image-decryption-failed", "微信媒体解密失败。", { cause: error });
  }
}

export function isWeixinHost(hostname: string): boolean {
  const normalized = hostname.toLowerCase().replace(/\.$/, "");
  return normalized === "weixin.qq.com" || normalized.endsWith(".weixin.qq.com")
    || normalized === "wechat.com" || normalized.endsWith(".wechat.com");
}

/** API base URLs must be https under the Weixin/WeChat hosts (proxy hosts
 * included in deployments must still pass this whitelist). */
export function normalizeWeixinBaseUrl(value: string): string {
  let url: URL;
  try {
    url = new URL(value);
  } catch {
    throw new WeixinApiError("invalid-base-url", "微信服务返回了无效的连接地址。");
  }
  if (url.protocol !== "https:" || !isWeixinHost(url.hostname)
    || (url.port !== "" && url.port !== "443")) {
    throw new WeixinApiError("untrusted-base-url", "微信服务返回了不受信任的连接地址。");
  }
  url.username = "";
  url.password = "";
  url.search = "";
  url.hash = "";
  if (!url.pathname.endsWith("/")) url.pathname += "/";
  return url.toString();
}

export function normalizeWeixinQrUrl(value: unknown): string {
  const text = nonEmptyString(value);
  if (!text) throw new WeixinApiError("invalid-qr", "微信服务没有返回扫码地址。");
  let url: URL;
  try {
    url = new URL(text);
  } catch {
    throw new WeixinApiError("invalid-qr", "微信服务返回了无效的扫码地址。");
  }
  if (url.protocol !== "https:" || !isWeixinHost(url.hostname)) {
    throw new WeixinApiError("untrusted-qr", "微信服务返回了不受信任的扫码地址。");
  }
  return url.toString();
}

/** Adopt a server-provided base/redirect host (bare host names get https). */
export function weixinBaseFromServer(value: unknown, fallback: string): string {
  const raw = nonEmptyString(value);
  if (!raw) return normalizeWeixinBaseUrl(fallback);
  return normalizeWeixinBaseUrl(raw.includes("://") ? raw : `https://${raw}`);
}

/** Trusted CDN download URL: encrypted_query_param form, or a full URL that
 * must be https under the CDN host with a /c2c/ path. */
export function weixinCdnDownloadUrl(media: RawRecord | null): URL {
  const query = nonEmptyString(media?.encrypt_query_param);
  if (query) {
    return new URL(`${WEIXIN_CDN_BASE_URL}/download?encrypted_query_param=${encodeURIComponent(query)}`);
  }
  const fullUrl = nonEmptyString(media?.full_url);
  if (!fullUrl) throw new WeixinApiError("missing-image-url", "微信媒体没有可用的下载地址。");
  let url: URL;
  try {
    url = new URL(fullUrl);
  } catch {
    throw new WeixinApiError("invalid-image-url", "微信媒体的下载地址无效。");
  }
  if (url.protocol !== "https:" || url.hostname !== WEIXIN_CDN_HOST
    || (url.port && url.port !== "443") || !url.pathname.startsWith("/c2c/")) {
    throw new WeixinApiError("untrusted-image-url", "微信媒体的下载地址不受信任。");
  }
  url.username = "";
  url.password = "";
  url.hash = "";
  return url;
}

/** Decode the millisecond timestamp carried by current 64-bit iLink message IDs. */
export function messageTimestampMs(messageId: string, now: number): number | null {
  const value = messageId.trim();
  if (!/^\d{16,20}$/.test(value)) return null;
  try {
    const timestampMs = Number(BigInt(value) >> MESSAGE_ID_TIMESTAMP_SHIFT);
    if (!Number.isSafeInteger(timestampMs)
      || timestampMs < MESSAGE_ID_MIN_TIMESTAMP_MS
      || timestampMs > now + MESSAGE_ID_MAX_FUTURE_MS) {
      return null;
    }
    return timestampMs;
  } catch {
    return null;
  }
}

export function messageIdOf(message: RawRecord): string | null {
  if (message.message_id !== undefined && message.message_id !== null) {
    return String(message.message_id);
  }
  return nonEmptyString(message.client_id);
}

export function itemList(message: RawRecord): RawRecord[] {
  const list = message.item_list;
  if (!Array.isArray(list)) return [];
  return list.map(asRecord).filter((item): item is RawRecord => item !== null);
}

/** Stable ordering by seq (falling back to create_time_ms) across a batch. */
export function orderMessages(messages: unknown[]): RawRecord[] {
  const records = messages.map(asRecord).filter((item): item is RawRecord => item !== null);
  if (records.length < 2) return records;
  const orderField = (["seq", "create_time_ms"] as const).find((field) => records.every((message) => {
    const value = message[field];
    if (typeof value === "number" && Number.isFinite(value)) return true;
    return typeof value === "string" && value.trim() !== "" && Number.isFinite(Number(value));
  }));
  if (!orderField) return records;
  // seq 可能超过 2^53（Number 精度丢失）：纯数字字符串用 BigInt 比较
  return records
    .map((message, index) => {
      const raw = String(message[orderField]).trim();
      const order = /^\d+$/.test(raw) ? BigInt(raw) : Number(message[orderField]);
      return { message, index, order, numeric: typeof order === "number" };
    })
    .sort((left, right) => {
      if (left.numeric && right.numeric) return (left.order as number) - (right.order as number) || left.index - right.index;
      if (typeof left.order === "bigint" && typeof right.order === "bigint") {
        return left.order < right.order ? -1 : left.order > right.order ? 1 : 0;
      }
      return left.index - right.index;
    })
    .map((entry) => entry.message);
}

/** Inbound text: item type 1 text_item.text, falling back to voice transcription. */
export function extractWeixinText(message: RawRecord): string | null {
  for (const item of itemList(message)) {
    if (item.type === 1 && typeof asRecord(item.text_item)?.text === "string") {
      const text = String(asRecord(item.text_item)?.text).trim();
      if (text) return text;
    }
    if (item.type === 3 && typeof asRecord(item.voice_item)?.text === "string") {
      const text = String(asRecord(item.voice_item)?.text).trim();
      if (text) return text;
    }
  }
  return null;
}

export interface WeixinReplyReference {
  id?: string;
  text?: string;
}

function weixinReplyPlaceholder(item: RawRecord | null): string | null {
  const type = num(item?.type);
  if (type === 2 || asRecord(item?.image_item)) return "[图片]";
  if (type === 3 || asRecord(item?.voice_item)) return "[语音]";
  if (type === 4 || asRecord(item?.file_item)) {
    const name = nonEmptyString(asRecord(item?.file_item)?.file_name);
    return name ? `[文件:${name}]` : "[文件]";
  }
  if (type === 5 || asRecord(item?.video_item)) return "[视频]";
  return null;
}

/** Extract the one-level reply snapshot embedded in an inbound iLink message. */
export function extractWeixinReplyReference(message: RawRecord): WeixinReplyReference | null {
  const container = itemList(message).find((item) => asRecord(item.ref_msg));
  if (!container) return null;
  const ref = asRecord(container.ref_msg)!;
  const quoted = asRecord(ref.message_item);
  const quotedText = nonEmptyString(asRecord(quoted?.text_item)?.text)
    ?? nonEmptyString(asRecord(quoted?.voice_item)?.text);
  const id = quoted?.msg_id === undefined || quoted?.msg_id === null
    ? undefined
    : nonEmptyString(String(quoted.msg_id)) ?? undefined;
  const text = quotedText ?? nonEmptyString(ref.title) ?? weixinReplyPlaceholder(quoted);
  if (!id && !text) return null;
  return { ...(id ? { id } : {}), ...(text ? { text } : {}) };
}

/** Padded ciphertext size for AES-128-ECB (always at least one block). */
export function aesEcbPaddedSize(size: number): number {
  return Math.ceil((size + 1) / 16) * 16;
}

function trustedCdnUploadUrl(value: string): URL {
  let url: URL;
  try {
    url = new URL(value);
  } catch {
    throw new WeixinApiError("invalid-upload-url", "微信服务返回了无效的文件上传地址。");
  }
  if (url.protocol !== "https:" || url.hostname !== WEIXIN_CDN_HOST
    || (url.port && url.port !== "443") || url.pathname !== "/c2c/upload"
    || url.username || url.password) {
    throw new WeixinApiError("untrusted-upload-url", "微信服务返回了不受信任的文件上传地址。");
  }
  url.hash = "";
  return url;
}

/** Build the CDN upload URL from getuploadurl's response. */
export function weixinCdnUploadUrl(response: RawRecord, fileKey: string): URL {
  const fullUrl = nonEmptyString(response.upload_full_url);
  if (fullUrl) return trustedCdnUploadUrl(fullUrl);
  const uploadParam = nonEmptyString(response.upload_param);
  if (!uploadParam) {
    throw new WeixinApiError("missing-upload-url", "微信服务没有返回文件上传地址。");
  }
  const url = new URL(`${WEIXIN_CDN_BASE_URL}/upload`);
  url.searchParams.set("encrypted_query_param", uploadParam);
  url.searchParams.set("filekey", fileKey);
  return trustedCdnUploadUrl(url.toString());
}

function abortError(signal: AbortSignal | undefined): Error {
  if (signal?.reason instanceof Error) return signal.reason;
  return new DOMException("The operation was aborted", "AbortError");
}

/** Stream the file through AES-128-ECB encryption as the CDN upload is consumed. */
function encryptUploadStream(
  bytes: Buffer,
  key: Buffer,
  signal: AbortSignal,
  onProgress: () => void,
): ReadableStream<Uint8Array> {
  const cipher = createCipheriv("aes-128-ecb", key, null);
  let offset = 0;
  let finalized = false;
  return new ReadableStream<Uint8Array>({
    pull(controller) {
      try {
        if (signal.aborted) {
          controller.error(abortError(signal));
          return;
        }
        if (offset < bytes.byteLength) {
          const chunk = cipher.update(bytes.subarray(offset, offset + UPLOAD_CHUNK_BYTES));
          offset = Math.min(offset + UPLOAD_CHUNK_BYTES, bytes.byteLength);
          onProgress();
          if (chunk.byteLength) controller.enqueue(chunk);
          return;
        }
        if (!finalized) {
          finalized = true;
          onProgress();
          controller.enqueue(cipher.final());
        }
        controller.close();
      } catch (error) {
        controller.error(error);
      }
    },
    cancel() {
      finalized = true;
    },
  });
}

/** POST the encrypted bytes to the CDN upload URL and read back the download
 * parameter. Retries transient failures; 4xx rejections are final. */
export async function uploadWeixinCdn(
  uploadFetch: (url: string | URL, init: RequestInit) => Promise<Response>,
  url: URL,
  bytes: Buffer,
  key: Buffer,
  opts: { signal?: AbortSignal } = {},
): Promise<string> {
  let lastError: unknown;
  for (let attempt = 1; attempt <= UPLOAD_RETRIES; attempt += 1) {
    opts.signal?.throwIfAborted();
    const idleController = new AbortController();
    const uploadSignal = opts.signal
      ? AbortSignal.any([opts.signal, idleController.signal])
      : idleController.signal;
    let timer: NodeJS.Timeout | undefined;
    let active = true;
    const onProgress = (): void => {
      if (!active) return;
      if (timer) clearTimeout(timer);
      timer = setTimeout(() => idleController.abort(new WeixinApiError(
        "upload-timeout",
        "微信文件上传长时间没有进展，已超时。",
      )), UPLOAD_IDLE_TIMEOUT_MS);
    };
    const body = encryptUploadStream(bytes, key, uploadSignal, onProgress);
    let response: Response | undefined;
    onProgress();
    try {
      response = await uploadFetch(url, {
        method: "POST",
        headers: {
          "content-type": "application/octet-stream",
          "content-length": String(aesEcbPaddedSize(bytes.byteLength)),
        },
        body,
        duplex: "half",
        signal: uploadSignal,
        // 密文只允许发给 getuploadurl 指定的白名单 CDN 主机，302 一律失败。
        redirect: "error",
      } as RequestInit);
      uploadSignal.throwIfAborted();
      if (response.status >= 400 && response.status < 500) {
        throw new WeixinApiError(
          "upload-rejected",
          `微信文件上传被拒绝（HTTP ${response.status}）。`,
          { status: response.status },
        );
      }
      if (response.status !== 200) {
        throw new WeixinApiError(
          "upload-failed",
          `微信文件上传失败（HTTP ${response.status}）。`,
          { status: response.status },
        );
      }
      const downloadParam = nonEmptyString(response.headers.get("x-encrypted-param"));
      if (!downloadParam) {
        throw new WeixinApiError("invalid-upload-response", "微信文件上传响应缺少下载参数。");
      }
      return downloadParam;
    } catch (error) {
      if (opts.signal?.aborted) throw abortError(opts.signal);
      if (idleController.signal.aborted) error = idleController.signal.reason;
      if (error instanceof WeixinApiError
        && (error.code === "upload-rejected" || (error.status !== undefined && error.status < 500))) {
        throw error;
      }
      lastError = error;
    } finally {
      active = false;
      clearTimeout(timer);
      try {
        await response?.body?.cancel();
      } catch {
        // Drain failures do not affect the upload result.
      }
    }
  }
  if (lastError instanceof WeixinApiError) throw lastError;
  throw new WeixinApiError("upload-failed", "微信文件上传失败。", { cause: lastError });
}

/** Download CDN media ciphertext (image/file), rejecting untrusted hosts. */
export async function downloadCdnMedia(
  fetchImpl: typeof fetch,
  url: string | URL,
  opts: { signal?: AbortSignal; maxBytes: number },
): Promise<Buffer> {
  const response = await fetchImpl(new URL(url), {
    method: "GET",
    signal: opts.signal,
    redirect: "manual",
  });
  if (!response.ok) {
    try {
      await response.body?.cancel();
    } catch {
      // Drain failures do not affect the error below.
    }
    throw new WeixinApiError(
      "file-download-failed",
      `微信媒体下载失败（HTTP ${response.status}）。`,
      { status: response.status },
    );
  }
  const buffer = Buffer.from(await response.arrayBuffer());
  if (Number.isFinite(opts.maxBytes) && buffer.length > opts.maxBytes) {
    throw new WeixinApiError("file-too-large", "微信媒体文件超过大小限制。");
  }
  return buffer;
}

function commonHeaders(): Record<string, string> {
  return {
    "iLink-App-Id": ILINK_APP_ID,
    "iLink-App-ClientVersion": ILINK_CLIENT_VERSION,
  };
}

function authenticatedHeaders(token: string | undefined): Record<string, string> {
  const headers: Record<string, string> = {
    ...commonHeaders(),
    "content-type": "application/json",
    AuthorizationType: "ilink_bot_token",
    // Quirk of the original client: the UIN is the decimal string of a random
    // uint32, base64-encoded as UTF-8 text.
    "X-WECHAT-UIN": Buffer.from(String(randomBytes(4).readUInt32BE(0)), "utf8").toString("base64"),
  };
  const bearer = nonEmptyString(token);
  if (bearer) headers.Authorization = `Bearer ${bearer}`;
  return headers;
}

export interface WeixinHttpOptions {
  method?: "GET" | "POST";
  timeoutMs?: number;
  signal?: AbortSignal;
  /** QR login endpoints run without the bot token. */
  authenticated?: boolean;
}

export type WeixinHttp = (
  path: string,
  body: unknown,
  init?: WeixinHttpOptions,
) => Promise<RawRecord>;

/** Default iLink JSON transport: normalizes and trusts the base URL on every
 * call, joins the endpoint, applies timeouts, and classifies failures into
 * network-error / timeout / http-error / invalid-response. */
export function createHttp(options: {
  getBaseUrl: () => string;
  getToken?: () => string | undefined;
  fetchImpl?: typeof fetch;
}): WeixinHttp {
  const fetchImpl = options.fetchImpl ?? fetch;
  return async function http(path, body, init = {}): Promise<RawRecord> {
    const method = init.method ?? "POST";
    const baseUrl = normalizeWeixinBaseUrl(options.getBaseUrl());
    const url = new URL(path, baseUrl);
    if (!isWeixinHost(url.hostname)) {
      throw new WeixinApiError("untrusted-endpoint", "拒绝访问不受信任的微信服务地址。");
    }
    if (init.signal?.aborted) throw abortError(init.signal);
    const callerSignal = init.signal;
    const controller = new AbortController();
    const onAbort = (): void => controller.abort(callerSignal?.reason);
    callerSignal?.addEventListener("abort", onAbort, { once: true });
    const timeoutMs = init.timeoutMs ?? DEFAULT_TIMEOUT_MS;
    let timedOut = false;
    const timer = timeoutMs > 0
      ? setTimeout(() => {
        timedOut = true;
        controller.abort();
      }, timeoutMs)
      : null;
    const token = options.getToken?.();
    const headers = init.authenticated === false
      ? commonHeaders()
      : authenticatedHeaders(token);
    try {
      const response = await fetchImpl(url, {
        method,
        headers,
        body: method === "GET" || body === undefined ? undefined : JSON.stringify(body),
        signal: controller.signal,
      });
      if (!response.ok) {
        try {
          await response.body?.cancel();
        } catch {
          // Drain failures do not affect the error below.
        }
        throw new WeixinApiError(
          "http-error",
          `微信服务请求失败（HTTP ${response.status}）。`,
          { status: response.status },
        );
      }
      try {
        return (await response.json()) as RawRecord;
      } catch (error) {
        throw new WeixinApiError("invalid-response", "微信服务返回了无法解析的响应。", { cause: error });
      }
    } catch (error) {
      if (callerSignal?.aborted) throw abortError(callerSignal);
      if (timedOut) throw new WeixinApiError("timeout", "微信服务请求超时。", { cause: error });
      if (error instanceof WeixinApiError) throw error;
      throw new WeixinApiError("network-error", "暂时无法访问微信服务。", { cause: error });
    } finally {
      if (timer) clearTimeout(timer);
      callerSignal?.removeEventListener("abort", onAbort);
    }
  };
}

export interface WeixinLoginCredentials {
  qrcode: string;
  qrUrl: string;
}

/** Step 1 of QR pairing: request a qrcode token and its image URL. */
export async function beginLogin(
  http: WeixinHttp,
  opts: { botType?: string; signal?: AbortSignal } = {},
): Promise<WeixinLoginCredentials> {
  const response = await http(
    `ilink/bot/get_bot_qrcode?bot_type=${encodeURIComponent(opts.botType ?? WEIXIN_BOT_TYPE)}`,
    { local_token_list: [] },
    { timeoutMs: 10_000, signal: opts.signal },
  );
  const providerCode = rejectedProviderResponse(response, ["errcode", "ret"]);
  if (providerCode) {
    throw new WeixinApiError("qr-request-rejected", "微信服务拒绝了二维码申请。", { providerCode });
  }
  const qrcode = nonEmptyString(response.qrcode);
  if (!qrcode) throw new WeixinApiError("invalid-qr", "微信服务没有返回二维码令牌。");
  const qrUrl = normalizeWeixinQrUrl(response.qrcode_img_content);
  return { qrcode, qrUrl };
}

function validateLoginResponse(value: RawRecord): RawRecord {
  if (typeof value.status !== "string" || !LOGIN_STATUSES[value.status]) {
    throw new WeixinApiError("invalid-login-status", "微信服务返回了无法识别的扫码状态。");
  }
  return value;
}

/** Step 2 of QR pairing: long-poll the qrcode status (optionally with the
 * pairing verify code entered by the user). */
export async function pollLogin(
  http: WeixinHttp,
  opts: { qrcode: string; verifyCode?: string; signal?: AbortSignal },
): Promise<RawRecord> {
  let endpoint = `ilink/bot/get_qrcode_status?qrcode=${encodeURIComponent(opts.qrcode)}`;
  const verifyCode = nonEmptyString(opts.verifyCode);
  if (verifyCode) endpoint += `&verify_code=${encodeURIComponent(verifyCode)}`;
  const response = await http(endpoint, undefined, {
    method: "GET",
    timeoutMs: LONG_POLL_TIMEOUT_MS,
    signal: opts.signal,
    authenticated: false,
  });
  return validateLoginResponse(response);
}

/** Delivery result is ambiguous: the request may have reached the platform. */
export function isUncertainDeliveryError(error: unknown): boolean {
  if (!(error instanceof WeixinApiError)) return false;
  if (error.code === "network-error" || error.code === "timeout" || error.code === "invalid-response") {
    return true;
  }
  const status = error.status;
  return typeof status === "number" && status >= 500 && status < 600;
}

/** Wrap a failed send into a user-readable error, preserving classification. */
export function toSendError(error: unknown): Error {
  if (error instanceof WeixinSendError) return error;
  if (error instanceof WeixinApiError) {
    return new WeixinSendError(error.message, error.code, error.providerCode, { cause: error });
  }
  if (error instanceof UserFacingError) return error;
  return new WeixinSendError("微信消息发送失败，请稍后重试。", "send-failed");
}

/** Connection start/login errors shown to users carry an actionable message. */
export function toUserFacingError(error: unknown): Error {
  if (error instanceof UserFacingError) return error;
  if (error instanceof WeixinApiError) return new UserFacingError(error.message);
  return new UserFacingError("微信服务暂时不可用，请稍后重试。");
}

/** Transient failures eligible for connection-level retry. */
export function isRetryableProtocolError(error: unknown): boolean {
  if (!(error instanceof WeixinApiError)) return false;
  if (error.code === "network-error" || error.code === "timeout") return true;
  return error.code === "http-error"
    && (error.status === 408 || error.status === 425 || error.status === 429
      || (error.status !== undefined && error.status >= 500 && error.status < 600));
}
