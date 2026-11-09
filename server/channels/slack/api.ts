/** Slack transport: Web API REST client, Socket Mode socket shape, and the
 * three-step file upload plus authenticated file download.
 * Protocol facts ported from xmanrui/dsh-im (MIT). */
import { mkdir, writeFile } from "node:fs/promises";
import { randomUUID } from "node:crypto";
import { join } from "node:path";
import { sleep } from "../../files.ts";
import {
  slackRestResponseSchema,
  type SlackFile,
  type SlackRestResponse,
} from "./schema.ts";

export const SLACK_API_BASE = "https://slack.com/api/";
export const SLACK_FILE_HOST = "files.slack.com";

const REST_TIMEOUT_MS = 15_000;
const FILE_TIMEOUT_MS = 120_000;

export class SlackApiError extends Error {
  /** Slack `error` 字段；传输层故障为 "transport"。 */
  readonly providerCode: string;
  readonly status?: number;
  readonly transient: boolean;

  constructor(
    message: string,
    options: { providerCode?: string; status?: number; transient?: boolean; cause?: unknown } = {},
  ) {
    super(message, options.cause !== undefined ? { cause: options.cause } : undefined);
    this.name = "SlackApiError";
    this.providerCode = options.providerCode ?? "unknown_error";
    this.status = options.status;
    this.transient = options.transient ?? false;
  }
}

/** Minimal Socket Mode socket surface accepted from dependency injection
 * (真实实现为全局 WebSocket，测试注入 EventEmitter 风格的 fake)。 */
export interface SlackSocketEvent {
  data?: unknown;
  code?: number;
  reason?: string;
  error?: unknown;
}

export interface SlackSocketLike {
  readonly readyState?: number;
  send(data: string): unknown;
  close(code?: number, reason?: string): unknown;
  addEventListener(type: string, listener: (event: SlackSocketEvent) => void): unknown;
  removeEventListener?(type: string, listener: (event: SlackSocketEvent) => void): unknown;
}

export interface SlackDeps {
  rest(method: string, body?: Record<string, unknown>): Promise<SlackRestResponse>;
  openSocket(url: string): SlackSocketLike;
  /** 下载 url_private_download（host 必须 files.slack.com）到 mediaDir。 */
  downloadFile(url: string, file: SlackFile): Promise<string>;
  /** PUT upload_url（FormData file）。 */
  uploadBytes(url: string, form: FormData): Promise<{ status: number }>;
}

/** 从 message 事件中提取文本负载（string / Buffer / ArrayBuffer…）。 */
export function socketEventData(event: unknown): string | null {
  const value =
    event !== null && typeof event === "object" && "data" in event
      ? event.data
      : event;
  const decoder = new TextDecoder();
  if (typeof value === "string") return value;
  if (value instanceof Uint8Array) return decoder.decode(value);
  if (value instanceof ArrayBuffer) return decoder.decode(new Uint8Array(value));
  return null;
}

/** Socket Mode 地址必须为 wss://，否则拒绝连接。 */
export function validateSocketUrl(value: unknown): string {
  let url: URL;
  try {
    url = new URL(typeof value === "string" ? value : "");
  } catch {
    throw new Error("Slack 未返回有效的 Socket Mode 地址");
  }
  if (url.protocol !== "wss:") {
    throw new Error("Slack Socket Mode 地址必须为 wss://");
  }
  return url.href;
}

/** 文件下载/上传地址必须 https + files.slack.com（无凭据、标准端口）。 */
export function validateHttpsFileUrl(value: unknown, what: string): URL {
  let url: URL;
  try {
    url = new URL(typeof value === "string" ? value : "");
  } catch {
    throw new Error(`Slack ${what}地址无效`);
  }
  if (
    url.protocol !== "https:" ||
    url.username !== "" ||
    url.password !== "" ||
    (url.port !== "" && url.port !== "443") ||
    url.hostname !== SLACK_FILE_HOST
  ) {
    throw new Error(`Slack ${what}地址未托管在 ${SLACK_FILE_HOST}，已拒绝`);
  }
  return url;
}

export interface RestClientOptions {
  botToken: () => string;
  appToken: () => string;
  signal: AbortSignal;
}

export type SlackRest = (method: string, body?: Record<string, unknown>) => Promise<SlackRestResponse>;

function entries(body: Record<string, unknown>): [string, string][] {
  return Object.entries(body).map(([key, value]) => [key, String(value)]);
}

/** Slack Web API 客户端：apps.connections.open 用 App Token，其余用 Bot
 * Token；429 按 Retry-After 退避重试一次；15s 超时。 */
export function createRestClient(options: RestClientOptions): SlackRest {
  return async function rest(method, body = {}) {
    // dsh-im 对 files.getUploadURLExternal / files.info 使用 form-encoded（其余 JSON）
    const formEncoded = method === "files.getUploadURLExternal" || method === "files.info";
    const token = (
      method === "apps.connections.open" ? options.appToken() : options.botToken()
    ).trim();
    const request = (): Promise<Response> =>
      fetch(new URL(method, SLACK_API_BASE), {
        method: "POST",
        headers: {
          authorization: `Bearer ${token}`,
          "content-type": formEncoded
            ? "application/x-www-form-urlencoded;charset=utf-8"
            : "application/json;charset=utf-8",
        },
        body: formEncoded
          ? new URLSearchParams(entries(body)).toString()
          : JSON.stringify(body),
        signal: AbortSignal.any([options.signal, AbortSignal.timeout(REST_TIMEOUT_MS)]),
        redirect: "error",
      });

    let response: Response;
    try {
      response = await request();
    } catch (error) {
      throw new SlackApiError(`Slack ${method} 网络请求失败`, {
        providerCode: "transport",
        transient: true,
        cause: error,
      });
    }
    if (response.status === 429) {
      const seconds = Number(response.headers.get("retry-after")) || 1;
      void response.body?.cancel().catch(() => {});
      await sleep(Math.min(10_000, Math.max(100, seconds * 1_000)), options.signal);
      try {
        response = await request();
      } catch (error) {
        throw new SlackApiError(`Slack ${method} 网络请求失败`, {
          providerCode: "transport",
          transient: true,
          cause: error,
        });
      }
    }

    let payload: unknown;
    try {
      payload = await response.json();
    } catch (error) {
      throw new SlackApiError(`Slack ${method} 返回了无效 JSON`, {
        providerCode: "transport",
        transient: true,
        status: response.status,
        cause: error,
      });
    }
    if (!response.ok || slackRestResponseSchema.safeParse(payload).success === false) {
      const rawError =
        payload !== null && typeof payload === "object" && "error" in payload
          ? payload.error
          : undefined;
      const code = typeof rawError === "string" ? rawError : `http_${response.status}`;
      throw new SlackApiError(`Slack ${method} 调用失败（${code}）`, {
        providerCode: code,
        status: response.status,
        transient: response.status >= 500 || response.status === 429,
      });
    }
    return payload as SlackRestResponse;
  };
}

function fileNameFor(file: SlackFile): string {
  // id 段同样清洗：file.id 异常取值不得引入路径分隔符
  const id = (file.id || randomUUID().slice(0, 8)).replace(/[^\w.-]/g, "_");
  const raw = file.name || file.title || "slack-file";
  const safe = raw.replace(/[^\w.\-\u4e00-\u9fff]+/g, "_").replace(/^\.+/, "_").slice(0, 120);
  return `${id}-${safe || "slack-file"}`;
}

async function cancelBody(response: Response): Promise<void> {
  void response.body?.cancel().catch(() => {});
}

export interface FileDownloaderOptions {
  botToken: () => string;
  mediaDir: string;
  signal: AbortSignal;
  /** 字节上限（来自全局设置 imageMaxDownloadMb），≤0 表示不限。 */
  maxBytes: () => Promise<number>;
}

/** 下载 url_private_download：必须 https + files.slack.com，携带 Bearer
 * botToken；redirect: "error" 不跟随重定向（防止跨域携带 Bearer），被 302
 * 到 HTML（缺 files:read scope 的典型症状）会直接失败。 */
export function createFileDownloader(options: FileDownloaderOptions) {
  let dirReady = false;
  return async function downloadFile(url: string, file: SlackFile): Promise<string> {
    const target = validateHttpsFileUrl(url, "文件下载");
    const limit = await options.maxBytes();
    let response: Response;
    try {
      response = await fetch(target, {
        headers: { authorization: `Bearer ${options.botToken().trim()}` },
        signal: AbortSignal.any([options.signal, AbortSignal.timeout(FILE_TIMEOUT_MS)]),
        redirect: "error",
      });
    } catch (error) {
      throw new Error(`Slack 文件下载失败：${(error as Error).message}`);
    }
    if (!response.ok) {
      await cancelBody(response);
      throw new Error(`Slack 文件下载失败（HTTP ${response.status}）`);
    }
    if (/^text\/html\b/i.test(response.headers.get("content-type") ?? "")) {
      await cancelBody(response);
      throw new Error("Slack 文件访问被拒绝，请为应用添加 files:read 权限后重新安装");
    }
    const bytes = new Uint8Array(await response.arrayBuffer());
    if (limit > 0 && bytes.byteLength > limit) {
      throw new Error(`Slack 文件大小 ${bytes.byteLength} 字节超过上限 ${limit} 字节，已跳过`);
    }
    if (!dirReady) {
      await mkdir(options.mediaDir, { recursive: true });
      dirReady = true;
    }
    const path = join(options.mediaDir, fileNameFor(file));
    await writeFile(path, bytes);
    return path;
  };
}

/** files.getUploadURLExternal 返回的 upload_url：PUT + FormData，host 必须
 * files.slack.com。 */
export function createUploader(options: { signal: AbortSignal }) {
  return async function uploadBytes(
    url: string,
    form: FormData,
  ): Promise<{ status: number }> {
    const target = validateHttpsFileUrl(url, "文件上传");
    let response: Response;
    try {
      response = await fetch(target, {
        method: "PUT",
        body: form,
        signal: AbortSignal.any([options.signal, AbortSignal.timeout(FILE_TIMEOUT_MS)]),
      });
    } catch (error) {
      throw new SlackApiError("Slack 文件上传网络请求失败", {
        providerCode: "transport",
        transient: true,
        cause: error,
      });
    }
    await cancelBody(response);
    return { status: response.status };
  };
}
