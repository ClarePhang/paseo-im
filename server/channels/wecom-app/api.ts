/** Outbound WeCom self-built application API: token cache, message/send,
 * media/upload and media/get.
 * Protocol facts ported from xmanrui/dsh-im (MIT). */
import {
  cleanString,
  isRecord,
  normalizeApiBaseUrl,
  splitUtf8ByBytes,
  WecomAppApiError,
} from "./protocol.ts";

export interface ApiTarget {
  /** User id for direct messages, mutually exclusive with chatId. */
  userId?: string;
  /** Group chat id for group conversations, mutually exclusive with userId. */
  chatId?: string;
  signal?: AbortSignal;
}

export interface TokenCacheOptions {
  /** Fetch the raw token document from /cgi-bin/gettoken. */
  fetchToken: (signal?: AbortSignal) => Promise<{ access_token?: unknown; expires_in?: unknown }>;
}

/** Cached access_token, refreshed 300s before expiry; concurrent callers share
 * a single in-flight request. */
export class WecomAppTokenCache {
  readonly #fetchToken: TokenCacheOptions["fetchToken"];
  #value: string | null = null;
  #expiresAt = 0;
  #pending: Promise<string> | null = null;

  constructor({ fetchToken }: TokenCacheOptions) {
    this.#fetchToken = fetchToken;
  }

  invalidate(): void {
    this.#value = null;
    this.#expiresAt = 0;
  }

  async getToken(signal?: AbortSignal): Promise<string> {
    if (this.#value && Date.now() < this.#expiresAt) return this.#value;
    if (!this.#pending) {
      this.#pending = this.#fetchToken(signal)
        .then((payload) => {
          const token = cleanString(payload.access_token);
          if (!token) throw new WecomAppApiError("token-missing", "企业微信没有返回 access_token，请检查 CorpID 与应用 Secret");
          const lifetime = Number(payload.expires_in);
          const margin = Number.isFinite(lifetime) && lifetime > 300 ? lifetime - 300 : 3_600;
          this.#value = token;
          this.#expiresAt = Date.now() + margin * 1_000;
          return token;
        })
        .finally(() => {
          this.#pending = null;
        });
    }
    return this.#pending;
  }
}

export interface WecomAppApiOptions {
  corpId: string;
  corpSecret: string;
  agentId: string;
  apiBaseUrl?: string;
  fetchImpl?: typeof fetch;
  timeoutMs?: number;
}

interface ApiResult {
  status: number;
  payload: Record<string, unknown>;
}

/** Outbound API client. All failures surface as WecomAppApiError with Chinese,
 * user-actionable messages. */
export class WecomAppApi {
  readonly corpId: string;
  readonly corpSecret: string;
  readonly agentId: string;
  readonly apiBaseUrl: string;
  readonly #fetchImpl: typeof fetch;
  readonly #timeoutMs: number;
  readonly #tokens: WecomAppTokenCache;

  constructor({ corpId, corpSecret, agentId, apiBaseUrl, fetchImpl, timeoutMs = 15_000 }: WecomAppApiOptions) {
    if (!cleanString(corpId) || !cleanString(agentId)) {
      throw new TypeError("WecomAppApi requires corpId and agentId");
    }
    // corpSecret may legitimately be absent at construction time (the adapter
    // validates credentials in start() with a user-facing error); requests
    // fail with a Chinese WecomAppApiError instead of a raw TypeError.
    this.corpId = cleanString(corpId)!;
    this.corpSecret = corpSecret ?? "";
    this.agentId = cleanString(agentId)!;
    this.apiBaseUrl = normalizeApiBaseUrl(apiBaseUrl);
    this.#fetchImpl = fetchImpl ?? fetch;
    this.#timeoutMs = timeoutMs;
    this.#tokens = new WecomAppTokenCache({ fetchToken: (signal) => this.#getTokenDocument(signal) });
  }

  invalidateToken(): void {
    this.#tokens.invalidate();
  }

  async #fetchJson(
    url: URL,
    { method = "GET", body, headers, signal, timeoutMs }: {
      method?: string;
      body?: string | FormData;
      headers?: Record<string, string>;
      signal?: AbortSignal;
      timeoutMs?: number;
    } = {},
  ): Promise<ApiResult> {
    const controller = new AbortController();
    const abort = () => controller.abort();
    const timer = setTimeout(abort, timeoutMs ?? this.#timeoutMs);
    if (signal) {
      if (signal.aborted) abort();
      else signal.addEventListener("abort", abort, { once: true });
    }
    try {
      const response = await this.#fetchImpl(url, { method, body, headers, signal: controller.signal });
      const text = await response.text();
      let payload: Record<string, unknown> = {};
      if (text) {
        try {
          const parsed: unknown = JSON.parse(text);
          if (isRecord(parsed)) payload = parsed;
        } catch {
          // 200 + HTML/截断响应同样无法当作成功：errcode 缺失会让消息静默丢失，
          // 必须按 bad-response 抛错（5xx/429 保持可重试语义）。
          throw new WecomAppApiError(
            "bad-response",
            `企业微信 API 返回了无法解析的内容（HTTP ${response.status}）`,
            { transient: response.status >= 500 || response.status === 429 },
          );
        }
      }
      if (!response.ok && !payload.errcode) {
        throw new WecomAppApiError(
          "bad-status",
          `企业微信 API 请求失败（HTTP ${response.status}）`,
          { transient: response.status >= 500 || response.status === 429 },
        );
      }
      return { status: response.status, payload };
    } catch (error) {
      if (error instanceof WecomAppApiError) throw error;
      if (signal?.aborted || controller.signal.aborted) {
        throw new WecomAppApiError("api-aborted", "企业微信 API 请求已取消", { transient: true });
      }
      throw new WecomAppApiError(
        "network-failed",
        `企业微信 API 网络请求失败：${error instanceof Error ? error.message : String(error)}`,
        { transient: true },
      );
    } finally {
      clearTimeout(timer);
      signal?.removeEventListener("abort", abort);
    }
  }

  #apiError(errcode: number, errmsg: unknown): WecomAppApiError {
    const code = String(errcode);
    const message = `企业微信 API 调用失败（${code}：${cleanString(errmsg) ?? "unknown"}）`;
    const providerCode = code;
    if (errcode === 60020) {
      return new WecomAppApiError("trusted-ip", `${message}。请在企业微信后台将该服务器公网 IP 加入“企业可信 IP”`, { providerCode });
    }
    if (errcode === 81013) {
      return new WecomAppApiError("invalid-user", `${message}。userid 或 chatid 不存在，请检查应用可见范围与会话`, { providerCode });
    }
    if (errcode === 40056) {
      return new WecomAppApiError("invalid-agent", `${message}。agentid 不合法，请核对自建应用 AgentId`, { providerCode });
    }
    if (errcode === 40001 || errcode === 41001 || errcode === 42001 || errcode === 40014) {
      return new WecomAppApiError("bad-credential", `${message}。请检查 CorpID 与应用 Secret 是否正确`, { providerCode });
    }
    return new WecomAppApiError("api-failed", message, { providerCode, transient: errcode === -1 });
  }

  async #getTokenDocument(signal?: AbortSignal): Promise<{ access_token?: unknown; expires_in?: unknown }> {
    const url = new URL(`${this.apiBaseUrl}/cgi-bin/gettoken`);
    url.searchParams.set("corpid", this.corpId);
    url.searchParams.set("corpsecret", this.corpSecret);
    const { payload } = await this.#fetchJson(url, { signal });
    const errcode = Number(payload.errcode ?? 0);
    if (errcode !== 0) throw this.#apiError(errcode, payload.errmsg);
    return payload;
  }

  /** Generic JSON GET/POST against the cgi-bin API with token injection and a
   * single retry when the cached token has expired server-side. */
  async request(pathname: string, { query, signal, retryOnToken = true }: {
    query?: Record<string, string>;
    signal?: AbortSignal;
    retryOnToken?: boolean;
  } = {}): Promise<Record<string, unknown>> {
    const token = await this.#tokens.getToken(signal);
    const url = new URL(`${this.apiBaseUrl}${pathname}`);
    for (const [key, value] of Object.entries(query ?? {})) {
      if (value !== undefined && value !== null) url.searchParams.set(key, String(value));
    }
    url.searchParams.set("access_token", token);
    const { payload } = await this.#fetchJson(url, { signal });
    const errcode = Number(payload.errcode ?? 0);
    if (errcode !== 0) {
      if (retryOnToken && (errcode === 40014 || errcode === 42001)) {
        this.#tokens.invalidate();
        return this.request(pathname, { query, signal, retryOnToken: false });
      }
      throw this.#apiError(errcode, payload.errmsg);
    }
    return payload;
  }

  async #send({ payload, signal, retryOnToken = true }: { payload: Record<string, unknown>; signal?: AbortSignal; retryOnToken?: boolean }): Promise<Record<string, unknown>> {
    const token = await this.#tokens.getToken(signal);
    const url = new URL(`${this.apiBaseUrl}/cgi-bin/message/send`);
    url.searchParams.set("access_token", token);
    const { payload: result } = await this.#fetchJson(url, {
      method: "POST",
      body: JSON.stringify({ ...payload, agentid: Number(this.agentId) }),
      headers: { "content-type": "application/json" },
      signal,
    });
    const errcode = Number(result.errcode ?? 0);
    if (retryOnToken && (errcode === 40014 || errcode === 42001)) {
      // Server-side token expiry: refresh once and resend.
      this.#tokens.invalidate();
      return this.#send({ payload, signal, retryOnToken: false });
    }
    if (errcode !== 0) throw this.#apiError(errcode, result.errmsg);
    const invalid = result.invaliduser;
    if (Array.isArray(invalid) && invalid.length > 0) {
      throw new WecomAppApiError(
        "invalid-user",
        `消息没有送达：${invalid.map(String).join(", ")}。请确认接收人仍在应用可见范围内`,
        { providerCode: "81013" },
      );
    }
    return result;
  }

  /** Sends a text message, chunked at the 2048 UTF-8 byte platform limit. */
  async sendText({ userId, chatId, content, signal }: ApiTarget & { content: string }): Promise<Record<string, unknown>> {
    const target = cleanString(userId);
    const group = cleanString(chatId);
    if (!target && !group) throw new WecomAppApiError("bad-request", "发送文本消息需要 touser 或 chatid");
    const results: Record<string, unknown>[] = [];
    for (const chunk of splitUtf8ByBytes(content)) {
      if (!chunk) continue;
      const payload: Record<string, unknown> = target
        ? { touser: target, msgtype: "text", text: { content: chunk }, safe: 0 }
        : { chatid: group, msgtype: "text", text: { content: chunk }, safe: 0 };
      results.push(await this.#send({ payload, signal }));
    }
    return results.at(-1) ?? {};
  }

  /** Uploads bytes as temporary media and sends them as an image/file message. */
  async sendMedia({ userId, chatId, mediaType, bytes, filename, signal }: ApiTarget & {
    mediaType: "image" | "file";
    bytes: Uint8Array;
    filename?: string;
  }): Promise<Record<string, unknown>> {
    const target = cleanString(userId);
    const group = cleanString(chatId);
    if (!target && !group) throw new WecomAppApiError("bad-request", "发送媒体消息需要 touser 或 chatid");
    if (bytes.length === 0) throw new WecomAppApiError("bad-request", "媒体内容为空，无法发送");
    const upload = await this.uploadMedia({ type: mediaType, bytes, filename, signal });
    const mediaId = cleanString(upload.media_id);
    if (!mediaId) throw new WecomAppApiError("upload-failed", "媒体上传没有返回 media_id，请稍后重试");
    const payload: Record<string, unknown> = target
      ? { touser: target, msgtype: mediaType, [mediaType]: { media_id: mediaId } }
      : { chatid: group, msgtype: mediaType, [mediaType]: { media_id: mediaId } };
    return this.#send({ payload, signal });
  }

  async uploadMedia({ type = "image", bytes, filename, signal }: {
    type?: "image" | "file";
    bytes: Uint8Array;
    filename?: string;
    signal?: AbortSignal;
  }): Promise<{ media_id: string }> {
    const mediaType = type === "file" ? "file" : "image";
    if (bytes.length === 0) throw new WecomAppApiError("bad-request", "uploadMedia 需要非空内容");
    const token = await this.#tokens.getToken(signal);
    const url = new URL(`${this.apiBaseUrl}/cgi-bin/media/upload`);
    url.searchParams.set("access_token", token);
    url.searchParams.set("type", mediaType);
    const form = new FormData();
    form.append("media", new Blob([new Uint8Array(bytes)]), filename ?? (mediaType === "image" ? "image.png" : "file.bin"));
    const { payload } = await this.#fetchJson(url, { method: "POST", body: form, signal, timeoutMs: 60_000 });
    const errcode = Number(payload.errcode ?? 0);
    if (errcode !== 0) throw this.#apiError(errcode, payload.errmsg);
    const mediaId = cleanString(payload.media_id);
    if (!mediaId) throw new WecomAppApiError("upload-failed", "媒体上传没有返回 media_id，请稍后重试");
    return { media_id: mediaId };
  }

  /** Downloads temporary media (image/voice) bytes by media_id. */
  async downloadMedia({ mediaId, signal }: { mediaId: string; signal?: AbortSignal }): Promise<{
    data: Buffer;
    filename: string | null;
    mime: string | null;
  }> {
    const id = cleanString(mediaId);
    if (!id) throw new WecomAppApiError("bad-request", "下载媒体需要 media_id");
    const token = await this.#tokens.getToken(signal);
    const url = new URL(`${this.apiBaseUrl}/cgi-bin/media/get`);
    url.searchParams.set("access_token", token);
    url.searchParams.set("media_id", id);
    const controller = new AbortController();
    const abort = () => controller.abort();
    const timer = setTimeout(abort, this.#timeoutMs);
    if (signal) {
      if (signal.aborted) abort();
      else signal.addEventListener("abort", abort, { once: true });
    }
    try {
      const response = await this.#fetchImpl(url, { signal: controller.signal });
      if (!response.ok) {
        throw new WecomAppApiError(
          "download-failed",
          `企业微信媒体下载失败（HTTP ${response.status}）`,
          { transient: response.status >= 500 || response.status === 429 },
        );
      }
      const mime = response.headers.get("content-type");
      if (mime && mime.includes("application/json")) {
        const text = await response.text();
        let payload: Record<string, unknown> = {};
        try {
          const parsed: unknown = JSON.parse(text);
          if (isRecord(parsed)) payload = parsed;
        } catch {
          // fall through to generic error
        }
        throw this.#apiError(Number(payload.errcode ?? -1), payload.errmsg);
      }
      const data = Buffer.from(await response.arrayBuffer());
      const disposition = response.headers.get("content-disposition") ?? "";
      const filename = disposition.match(/filename\*?=(?:UTF-8'')?"?([^";]+)"?/iu)?.[1] ?? null;
      return { data, filename, mime: mime && mime !== "application/octet-stream" ? mime : null };
    } catch (error) {
      if (error instanceof WecomAppApiError) throw error;
      throw new WecomAppApiError(
        "network-failed",
        `企业微信媒体下载失败：${error instanceof Error ? error.message : String(error)}`,
        { transient: true },
      );
    } finally {
      clearTimeout(timer);
      signal?.removeEventListener("abort", abort);
    }
  }

  /** Credential sanity check: resolves (and caches) a fresh access_token. */
  async health(signal?: AbortSignal): Promise<void> {
    await this.#tokens.getToken(signal);
  }
}
