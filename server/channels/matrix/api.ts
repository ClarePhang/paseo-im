/** Matrix CS API v3 transport for paseo-im. All JSON calls funnel through
 * `rest(method, path, body)` so tests can inject a recorder; binary media goes
 * through dedicated download/upload hooks with byte caps. Protocol facts
 * ported from xmanrui/dsh-im (MIT). */
import type { Connection } from "../../../shared/config.ts";
import { UserFacingError } from "../../../shared/errors.ts";
import { sleep } from "../../../server/files.ts";

export type MatrixConnection = Extract<Connection, { channel: "matrix" }>;

export const CLIENT_V3 = "/_matrix/client/v3";
export const SYNC_TIMEOUT_MS = 30_000;
export const TYPING_TIMEOUT_MS = 20_000;
export const TYPING_RENEW_MS = 10_000;
export const MAX_MEDIA_BYTES = 100 * 1024 * 1024;
/** 媒体下载手动跟随重定向的最大跳数（ref MATRIX_FILE_HOST_MAX_REDIRECTS）。 */
const MAX_MEDIA_REDIRECTS = 3;

export class MatrixApiError extends Error {
  readonly status?: number;
  readonly errcode?: string;
  /** Auth failures terminate the sync loop; everything else retries. */
  readonly permanent: boolean;

  constructor(message: string, options: { status?: number; errcode?: string; permanent?: boolean } = {}) {
    super(message);
    this.name = "MatrixApiError";
    this.status = options.status;
    this.errcode = options.errcode;
    this.permanent = options.permanent ?? (options.status === 401 || options.errcode === "M_UNKNOWN_TOKEN");
  }
}

export function homeserverUrl(raw: string): string | null {
  const value = typeof raw === "string" ? raw.trim() : "";
  if (!value) return null;
  let url: URL;
  try {
    url = new URL(value);
  } catch {
    return null;
  }
  if (url.protocol !== "https:" && url.protocol !== "http:") return null;
  if (url.username || url.password || url.hash) return null;
  return url.href.replace(/\/+$/, "");
}

export function isMatrixUserId(value: unknown): value is string {
  return typeof value === "string" && /^@[-.=_+/A-Za-z0-9]+:[-.A-Za-z0-9]+(?::\d{1,5})?$/.test(value);
}

export function isMatrixRoomId(value: unknown): value is string {
  return typeof value === "string" && /^[!#][^:\s]+:[A-Za-z0-9.-]+(?::\d{1,5})?$/.test(value);
}

export function parseMxc(uri: string): { server: string; mediaId: string } | null {
  const match = /^mxc:\/\/([^/:]+)\/([^/?]+)$/.exec(typeof uri === "string" ? uri.trim() : "");
  if (!match) return null;
  const server = decodeURIComponent(match[1]!);
  const mediaId = decodeURIComponent(match[2]!);
  if (!/^[A-Za-z0-9.:-]+$/.test(server) || !/^[A-Za-z0-9._-]+$/.test(mediaId)) return null;
  return { server, mediaId };
}

export interface MatrixRestResponse {
  status: number;
  errcode?: string;
}

export interface MatrixDeps {
  /** JSON CS API call. `path` includes the query string. Non-2xx rejects with
   * MatrixApiError carrying status/errcode. */
  rest(method: string, path: string, body?: unknown, signal?: AbortSignal): Promise<Record<string, unknown>>;
  /** Download an `mxc://` URI from the configured homeserver, capped at maxBytes. */
  mediaDownload(mxc: string, maxBytes: number, signal?: AbortSignal): Promise<{ bytes: Uint8Array; mime?: string }>;
  /** Upload bytes and return the resulting `content_uri`. */
  mediaUpload(bytes: Uint8Array, mime: string, filename?: string, signal?: AbortSignal): Promise<string>;
  sleep(ms: number, signal?: AbortSignal): Promise<void>;
  txnId(): string;
  now(): number;
}

export interface DefaultDepsOptions {
  homeserver: string;
  /** Live access token; resolved per call so password-login tokens attach mid-flight. */
  token: () => string;
  fetchImpl?: typeof fetch;
}

export function defaultDeps(options: DefaultDepsOptions): MatrixDeps {
  const base = homeserverUrl(options.homeserver);
  const fetchImpl = options.fetchImpl ?? globalThis.fetch;
  const requestSignal = (signal: AbortSignal | undefined, timeoutMs: number) => {
    const timeout = AbortSignal.timeout(timeoutMs);
    return signal ? AbortSignal.any([signal, timeout]) : timeout;
  };
  const abortReason = (signal?: AbortSignal) =>
    signal?.reason instanceof Error ? signal.reason : new DOMException("The operation was aborted", "AbortError");

  const readErrorBody = async (response: Response): Promise<{ errcode?: string; error?: string }> => {
    // homeserver 错误体是外部 JSON，统一按索引签名读取，字段保持 unknown
    const body = (await response.json().catch(() => ({}))) as Record<string, unknown>;
    const errcode = typeof body.errcode === "string" ? body.errcode : undefined;
    const error = typeof body.error === "string" ? body.error : undefined;
    return { errcode, error };
  };
  const rest = async (
    method: string,
    path: string,
    body?: unknown,
    signal?: AbortSignal,
  ): Promise<Record<string, unknown>> => {
    if (!base) throw new UserFacingError("Homeserver 地址无效，请填写形如 https://matrix.example.org 的地址。");
    const token = options.token();
    const headers: Record<string, string> = { accept: "application/json" };
    if (token) headers.authorization = `Bearer ${token}`;
    const hasBody = body !== undefined;
    if (hasBody) headers["content-type"] = "application/json";
    const response = await fetchImpl(`${base}${CLIENT_V3}${path}`, {
      method,
      redirect: "error",
      headers,
      body: hasBody ? JSON.stringify(body) : undefined,
      signal: requestSignal(signal, method === "GET" && path.startsWith("/sync?") ? SYNC_TIMEOUT_MS + 15_000 : 30_000),
    }).catch((cause: unknown) => {
      const name = cause instanceof Error ? cause.name : "";
      if (name === "AbortError" || name === "TimeoutError") throw abortReason(signal);
      throw new MatrixApiError(`Matrix 网络请求失败：${String(cause)}`);
    });
    if (!response.ok) {
      const { errcode, error } = await readErrorBody(response);
      const detail = [errcode, error].filter(Boolean).join(": ");
      throw new MatrixApiError(`Matrix 请求失败（HTTP ${response.status}${detail ? `，${detail}` : ""}）`, {
        status: response.status,
        errcode,
      });
    }
    return (await response.json().catch(() => ({}))) as Record<string, unknown>;
  };

  const downloadOnce = async (
    path: string,
    maxBytes: number,
    signal?: AbortSignal,
  ): Promise<{ bytes: Uint8Array; mime?: string }> => {
    const token = options.token();
    const baseHost = base ? new URL(base).host : "";
    let target = new URL(`${base}${path}`);
    for (let hop = 0; ; hop += 1) {
      const response = await fetchImpl(target, {
        method: "GET",
        // 手动跟随重定向：只允许 homeserver 同域/原域、至多 3 跳，防止凭据被
        // 302 带去外部主机（redirect:"follow" 会无条件透传 Authorization）。
        redirect: "manual",
        headers: { authorization: `Bearer ${token}`, accept: "*/*" },
        signal: requestSignal(signal, 60_000),
      }).catch((cause: unknown) => {
        const name = cause instanceof Error ? cause.name : "";
        if (name === "AbortError" || name === "TimeoutError") throw abortReason(signal);
        throw new MatrixApiError(`Matrix 媒体下载失败：${String(cause)}`);
      });
      if (response.status >= 300 && response.status < 400) {
        await response.body?.cancel().catch(() => {});
        if (hop >= MAX_MEDIA_REDIRECTS) {
          throw new MatrixApiError(`Matrix 媒体下载重定向超过 ${MAX_MEDIA_REDIRECTS} 跳上限`);
        }
        const location = response.headers.get("location");
        if (!location) throw new MatrixApiError("Matrix 媒体重定向缺少 location 头");
        let next: URL;
        try {
          next = new URL(location, target);
        } catch {
          throw new MatrixApiError("Matrix 媒体重定向地址无效");
        }
        if (next.protocol !== "https:" && next.protocol !== "http:") {
          throw new MatrixApiError(`Matrix 媒体重定向协议不允许（${next.protocol}）`);
        }
        if (next.host !== baseHost && next.host !== target.host) {
          throw new MatrixApiError(`Matrix 媒体重定向离开了 homeserver 域（${next.host}）`);
        }
        target = next;
        continue;
      }
      if (!response.ok) {
        await response.body?.cancel().catch(() => {});
        const { errcode } = await readErrorBody(response);
        throw new MatrixApiError(`Matrix 媒体下载失败（HTTP ${response.status}）`, { status: response.status, errcode });
      }
      const declared = Number(response.headers.get("content-length"));
      if (Number.isFinite(declared) && declared > maxBytes) {
        await response.body?.cancel().catch(() => {});
        throw new MatrixApiError(`Matrix 媒体超过 ${maxBytes} 字节下载上限`);
      }
      const chunks: Uint8Array[] = [];
      let total = 0;
      if (response.body?.[Symbol.asyncIterator]) {
        for await (const chunk of response.body) {
          const bytes = chunk instanceof Uint8Array ? chunk : new Uint8Array(chunk);
          total += bytes.byteLength;
          if (total > maxBytes) {
            await response.body.cancel().catch(() => {});
            throw new MatrixApiError(`Matrix 媒体超过 ${maxBytes} 字节下载上限`);
          }
          chunks.push(bytes);
        }
      } else {
        const buffer = new Uint8Array(await response.arrayBuffer());
        total = buffer.byteLength;
        if (total > maxBytes) throw new MatrixApiError(`Matrix 媒体超过 ${maxBytes} 字节下载上限`);
        chunks.push(buffer);
      }
      const bytes = new Uint8Array(total);
      let offset = 0;
      for (const chunk of chunks) {
        bytes.set(chunk, offset);
        offset += chunk.byteLength;
      }
      const mime = response.headers.get("content-type")?.split(";")[0]?.trim() || undefined;
      return { bytes, mime };
    }
  };

  const mediaDownload = async (
    mxc: string,
    maxBytes: number,
    signal?: AbortSignal,
  ): Promise<{ bytes: Uint8Array; mime?: string }> => {
    if (!base) throw new UserFacingError("Homeserver 地址无效，请填写形如 https://matrix.example.org 的地址。");
    const parsed = parseMxc(mxc);
    if (!parsed) throw new MatrixApiError("Matrix 媒体地址（mxc://）无效");
    const encoded = `/${encodeURIComponent(parsed.server)}/${encodeURIComponent(parsed.mediaId)}`;
    try {
      return await downloadOnce(`${CLIENT_V3}/media/download${encoded}`, maxBytes, signal);
    } catch (error) {
      const retryable =
        error instanceof MatrixApiError && (error.status === 404 || error.status === 405 || error.errcode === "M_UNRECOGNIZED");
      if (!retryable) throw error;
      return await downloadOnce(`/_matrix/media/v3/download${encoded}`, maxBytes, signal);
    }
  };

  const mediaUpload = async (
    bytes: Uint8Array,
    mime: string,
    filename?: string,
    signal?: AbortSignal,
  ): Promise<string> => {
    if (!base) throw new UserFacingError("Homeserver 地址无效，请填写形如 https://matrix.example.org 的地址。");
    if (!(bytes instanceof Uint8Array) || bytes.byteLength === 0) throw new MatrixApiError("Matrix 媒体上传内容为空");
    const query = filename ? `?filename=${encodeURIComponent(filename)}` : "";
    const token = options.token();
    const response = await fetchImpl(`${base}${CLIENT_V3}/media/upload${query}`, {
      method: "POST",
      redirect: "error",
      headers: {
        authorization: `Bearer ${token}`,
        "content-type": mime || "application/octet-stream",
        accept: "application/json",
      },
      body: bytes satisfies ArrayBufferView,
      signal: requestSignal(signal, 120_000),
    }).catch((cause: unknown) => {
      const name = cause instanceof Error ? cause.name : "";
      if (name === "AbortError" || name === "TimeoutError") throw abortReason(signal);
      throw new MatrixApiError(`Matrix 媒体上传失败：${String(cause)}`);
    });
    if (!response.ok) {
      await response.body?.cancel().catch(() => {});
      const { errcode } = await readErrorBody(response);
      throw new MatrixApiError(`Matrix 媒体上传失败（HTTP ${response.status}）`, { status: response.status, errcode });
    }
    const body = (await response.json().catch(() => ({}))) as Record<string, unknown>;
    const contentUri = body.content_uri;
    if (typeof contentUri !== "string" || !contentUri) {
      throw new MatrixApiError("Matrix 媒体上传未返回 content_uri");
    }
    return contentUri;
  };

  return {
    rest,
    mediaDownload,
    mediaUpload,
    sleep,
    txnId: () => globalThis.crypto.randomUUID(),
    now: () => Date.now(),
  };
}
