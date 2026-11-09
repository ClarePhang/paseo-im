/** QQ 开放平台渠道适配器：一条 WebSocket 网关接收 C2C 私聊与群 @ 消息，
 * REST v2 发送（markdown msg_type=2 优先，被平台拒绝后回退纯文本），
 * 并按入站 msg_id 维护被动回复额度。
 *
 * Protocol facts ported from xmanrui/dsh-im (MIT)，并对照
 * @tencent-connect/qqbot-nodejs@1.0.4 校验：令牌端点、网关 opcode/intents、
 * @ 提及标记、附件 URL 白名单、markdown 拒绝码 40034090、被动回复额度
 * （私聊 4 条/60 分钟，群 5 条/5 分钟）。 */
import { randomUUID } from "node:crypto";
import { mkdir, writeFile } from "node:fs/promises";
import { join } from "node:path";
import type { Connection } from "../../../shared/config.ts";
import type {
  ChannelCapabilities,
  NormalizedMedia,
  NormalizedMessage,
  OutboundText,
  SendResult,
} from "../../../shared/model.ts";
import { atomicWrite, readJson, WriteChain } from "../../files.ts";
import { UserFacingError } from "../../../shared/errors.ts";
import { CHANNEL_SPECS } from "../../../shared/channels.ts";
import { defaultConfigure, requireFields } from "../configure.ts";
import { splitChunks } from "../../format/chunks.ts";
import type { AdapterContext, ChannelAdapter, ChannelDefinition } from "../types.ts";

export type QqConnection = Extract<Connection, { channel: "qq" }>;

const STATE_FILE = "qq-state.json";
const SEEN_TTL_MS = 24 * 60 * 60_000;
const SEEN_LIMIT = 512;
const DEFAULT_BACKOFF_MS = [1_000, 3_000, 5_000, 10_000, 30_000];
/** GROUP_AND_C2C intent：同时覆盖 C2C_MESSAGE_CREATE 与 GROUP_AT_MESSAGE_CREATE。 */
const INTENT_GROUP_AND_C2C = 1 << 25;
const MAX_CHUNK_CHARS = 3_800;
/** 平台拒绝 markdown 模板时的业务码。 */
const MARKDOWN_REJECTION_CODES = new Set([400_340_90]);
const PARTIAL_REPLY_NOTICE = "回答较长，后续内容未能通过 QQ 完整发送，请回复“继续”。";
const PASSIVE_LIMIT = { c2c: 4, group: 5 } as const;
const PASSIVE_WINDOW_MS = { c2c: 60 * 60_000, group: 5 * 60_000 } as const;
const GROUP_PROACTIVE_LIMIT = 5;
const GROUP_PROACTIVE_WINDOW_MS = 5 * 60_000;
const ALLOWED_ATTACHMENT_HOST_SUFFIXES = [
  ".qq.com",
  ".qq.com.cn",
  ".qpic.cn",
  ".myqcloud.com",
  ".tencentcos.com",
  ".ugcimg.cn",
];
const IMAGE_FILENAME_PATTERN = /\.(?:gif|jpe?g|png|webp)$/i;
const TOKEN_PATH = "/app/getAppAccessToken";
const TOKEN_BASE_URL = "https://bots.qq.com";
const API_BASE_URL = "https://api.sgroup.qq.com";

/** QQ REST API 错误：携带 HTTP 状态与平台业务码，供 markdown 回退判定。 */
export class QqApiError extends Error {
  readonly httpStatus: number;
  readonly bizCode?: number;

  constructor(message: string, httpStatus: number, bizCode?: number) {
    super(message);
    this.httpStatus = httpStatus;
    this.bizCode = bizCode;
  }
}

export function isMarkdownRejection(error: unknown): error is QqApiError {
  return (
    error instanceof QqApiError &&
    error.httpStatus >= 400 &&
    error.httpStatus < 500 &&
    typeof error.bizCode === "number" &&
    MARKDOWN_REJECTION_CODES.has(error.bizCode)
  );
}

/** 与网关事件同构的最小 socket 接口；生产用全局 WebSocket，测试注入 fake。 */
export type QqSocketEvent =
  | { type: "message"; data: unknown }
  | { type: "error"; error: unknown }
  | { type: "close"; code: number; reason: string };

export interface QqSocket {
  send(data: string): void;
  close(code?: number, reason?: string): void;
  addEventListener(type: "message" | "error" | "close", listener: (event: QqSocketEvent) => void): void;
}

/** 可注入传输层：REST 走 api()，网关走 createWs()；测试用 fake 完全替代。 */
export interface QqDeps {
  api(path: string, method: string, body?: unknown): Promise<unknown>;
  createWs(url: string): QqSocket;
  /** 入站附件下载实现（默认全局 fetch）。 */
  fetchImpl?: typeof fetch;
  /** 重连退避序列，耗尽后停在末值（默认 [1,3,5,10,30]s）。 */
  backoffMs?: number[];
}

/** Narrow an unknown payload to an indexable record; every field read stays
 * `unknown` and goes through `str()`/`typeof` guards afterwards. */
function asRecord(value: unknown): Record<string, unknown> {
  return value !== null && typeof value === "object" ? (value as Record<string, unknown>) : {};
}

export function defaultDeps(options: {
  appId: string;
  appSecret: string;
  fetchImpl?: typeof fetch;
  backoffMs?: number[];
}): QqDeps {
  const fetchImpl = options.fetchImpl ?? fetch;
  let cached: { token: string; expiresAt: number } | null = null;
  let inflight: Promise<string> | null = null;

  const fetchToken = async (): Promise<string> => {
    const response = await fetchImpl(`${TOKEN_BASE_URL}${TOKEN_PATH}`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ appId: options.appId, clientSecret: options.appSecret }),
    });
    const text = await response.text();
    let parsed: { access_token?: string; expires_in?: number; message?: string } = {};
    try {
      parsed = JSON.parse(text) as typeof parsed;
    } catch {
      // 非 JSON 响应体在下方按原始文本提示。
    }
    if (!response.ok) {
      if (response.status >= 400 && response.status < 500) {
        throw new UserFacingError(
          `QQ 开放平台拒绝了凭据（HTTP ${response.status}）：请检查 AppID 与 AppSecret 是否正确。${parsed.message ? ` 平台返回：${parsed.message}` : ""}`,
        );
      }
      throw new Error(`获取 QQ 访问令牌失败（HTTP ${response.status}）：${text.slice(0, 200)}`);
    }
    if (!parsed.access_token) {
      throw new Error(`QQ 访问令牌响应缺少 access_token：${text.slice(0, 200)}`);
    }
    cached = { token: parsed.access_token, expiresAt: Date.now() + (parsed.expires_in ?? 7_200) * 1000 };
    return cached.token;
  };

  const getToken = async (): Promise<string> => {
    if (cached && Date.now() < cached.expiresAt - 5 * 60_000) return cached.token;
    if (!inflight) inflight = fetchToken().finally(() => { inflight = null; });
    return inflight;
  };

  const request = async (path: string, method: string, body?: unknown, retried = false): Promise<unknown> => {
    const token = await getToken();
    const response = await fetchImpl(`${API_BASE_URL}${path}`, {
      method,
      headers: { "Content-Type": "application/json", Authorization: `QQBot ${token}` },
      body: body === undefined ? undefined : JSON.stringify(body),
    });
    if (response.status === 401 && !retried) {
      cached = null;
      return request(path, method, body, true);
    }
    const text = await response.text();
    let parsed: unknown;
    try {
      parsed = JSON.parse(text);
    } catch {
      parsed = undefined;
    }
    if (!response.ok) {
      const detail = asRecord(parsed);
      const bizCode = typeof detail.code === "number" ? detail.code : undefined;
      const bizMessage = typeof detail.message === "string" ? detail.message : text.slice(0, 200);
      throw new QqApiError(
        `QQ API ${method} ${path} 失败（HTTP ${response.status}）：${bizMessage}`,
        response.status,
        bizCode,
      );
    }
    return parsed;
  };

  return {
    api: (path, method, body) => {
      if (path === TOKEN_PATH) return getToken();
      return request(path, method, body);
    },
    createWs: (url) => {
      const ws = new WebSocket(url);
      return {
        send: (data) => { ws.send(data); },
        close: (code, reason) => { ws.close(code ?? 1000, reason); },
        addEventListener: (type, listener) => {
          if (type === "message") {
            ws.addEventListener("message", (event: MessageEvent) => {
              listener({ type, data: event.data });
            });
          } else if (type === "close") {
            ws.addEventListener("close", (event: CloseEvent) => {
              listener({ type, code: event.code, reason: event.reason });
            });
          } else {
            ws.addEventListener("error", (event: Event) => {
              const error = "error" in event ? event.error : undefined;
              listener({ type, error: error ?? new Error("QQ WebSocket 连接错误") });
            });
          }
        },
      };
    },
    fetchImpl,
  };
}

interface QqState {
  seen: Record<string, number>;
  scopes: Record<string, "c2c" | "group">;
  lastInbound: Record<string, { msgId: string; at: number }>;
}

interface GatewayFrame {
  op?: unknown;
  d?: unknown;
  s?: unknown;
  t?: unknown;
}

const GATEWAY_OP = {
  DISPATCH: 0,
  HEARTBEAT: 1,
  IDENTIFY: 2,
  RESUME: 6,
  RECONNECT: 7,
  INVALID_SESSION: 9,
  HELLO: 10,
} as const;

function escapeRegExp(value: string): string {
  return value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function str(value: unknown): string | undefined {
  return typeof value === "string" && value.length > 0 ? value : undefined;
}

function parseTimestamp(value: unknown): number {
  const parsed = typeof value === "string" ? Date.parse(value) : Number.NaN;
  return Number.isNaN(parsed) ? Date.now() : parsed;
}

function nextMsgSeq(): number {
  const timePart = Date.now() % 100_000_000;
  const random = Math.floor(Math.random() * 65_536);
  return (timePart ^ random) % 65_536;
}

function decodeFrameData(data: unknown): string {
  if (typeof data === "string") return data;
  if (data instanceof Uint8Array) return Buffer.from(data).toString("utf8");
  if (data instanceof ArrayBuffer) return Buffer.from(new Uint8Array(data)).toString("utf8");
  return String(data ?? "");
}

function isAllowedAttachmentUrl(url: string): boolean {
  try {
    const hostname = new URL(url.startsWith("//") ? `https:${url}` : url).hostname.toLowerCase();
    return ALLOWED_ATTACHMENT_HOST_SUFFIXES.some(
      (suffix) => hostname === suffix.slice(1) || hostname.endsWith(suffix),
    );
  } catch {
    return false;
  }
}

function safeFilename(name: string): string {
  return name.replace(/[\\/\u0000-\u001f]/g, "_").slice(-120) || "attachment";
}

function urlBasename(url: string): string | undefined {
  try {
    const name = decodeURIComponent(new URL(url).pathname.split("/").pop() ?? "");
    return name || undefined;
  } catch {
    return undefined;
  }
}

function attachmentKind(contentType: string | undefined, filename: string): "image" | "file" {
  if (contentType?.startsWith("image/")) return "image";
  if (IMAGE_FILENAME_PATTERN.test(filename)) return "image";
  return "file";
}

export function createAdapter(
  ctx: AdapterContext<QqConnection>,
  deps: QqDeps = defaultDeps({
    appId: ctx.config.appId,
    appSecret: ctx.secrets.appSecret ?? "",
  }),
): ChannelAdapter {
  const appId = ctx.config.appId?.trim() ?? "";

  const capabilities: ChannelCapabilities = {
    progress: "typing",
    markdown: "custom",
    uploadImage: false,
    uploadFile: false,
    reactions: false,
    typing: true,
    maxChunkChars: MAX_CHUNK_CHARS,
    threads: false,
  };

  const statePath = join(ctx.dataDir, STATE_FILE);
  const writes = new WriteChain();
  let state: QqState = { seen: {}, scopes: {}, lastInbound: {} };
  let stopped = false;
  let authFatal = false;
  let intentionalClose = false;
  let socket: QqSocket | null = null;
  let authToken: string | null = null;
  let heartbeatTimer: ReturnType<typeof setInterval> | null = null;
  let reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  let reconnectAttempt = 0;
  let sessionId: string | null = null;
  let lastSeq: number | null = null;

  const passiveUsed = new Map<string, number>();
  const ownMessageIds = new Set<string>();
  const ownTextsByChat = new Map<string, string[]>();
  const proactiveGroupSends: number[] = [];
  let msgSeqSeed = 0;

  async function loadState(): Promise<void> {
    const loaded = await readJson<QqState>(statePath, (value) => {
      const record = (value ?? {}) as Partial<QqState>;
      return {
        seen: record.seen ?? {},
        scopes: record.scopes ?? {},
        lastInbound: record.lastInbound ?? {},
      };
    });
    if (loaded) state = loaded;
  }

  function pruneSeen(): void {
    const cutoff = Date.now() - SEEN_TTL_MS;
    for (const [id, ts] of Object.entries(state.seen)) {
      if (ts < cutoff) delete state.seen[id];
    }
    const entries = Object.entries(state.seen).sort((a, b) => a[1] - b[1]);
    while (entries.length > SEEN_LIMIT) {
      const [id] = entries.shift()!;
      delete state.seen[id];
    }
  }

  function saveState(): void {
    pruneSeen();
    void writes.run(async () => {
      try {
        await atomicWrite(statePath, state);
      } catch (error) {
        ctx.log("warn", `QQ 状态保存失败：${error instanceof Error ? error.message : String(error)}`);
      }
    });
  }

  function sendFrame(payload: unknown): void {
    const current = socket;
    if (!current || stopped) return;
    try {
      current.send(JSON.stringify(payload));
    } catch (error) {
      ctx.log("warn", `QQ WebSocket 发送失败：${error instanceof Error ? error.message : String(error)}`);
    }
  }

  function stopHeartbeat(): void {
    if (heartbeatTimer) {
      clearInterval(heartbeatTimer);
      heartbeatTimer = null;
    }
  }

  function startHeartbeat(intervalMs: number): void {
    stopHeartbeat();
    heartbeatTimer = setInterval(() => {
      sendFrame({ op: GATEWAY_OP.HEARTBEAT, d: lastSeq });
    }, intervalMs);
    heartbeatTimer.unref();
  }

  function closeCurrent(): void {
    const current = socket;
    if (!current) return;
    intentionalClose = true;
    socket = null;
    try {
      current.close(1000);
    } catch {
      // 已断开的 socket 关闭失败可忽略。
    }
  }

  function scheduleReconnect(): void {
    if (stopped || authFatal || ctx.signal.aborted) return;
    if (reconnectTimer) return;
    const delays = deps.backoffMs ?? DEFAULT_BACKOFF_MS;
    const delay = delays[Math.min(reconnectAttempt, delays.length - 1)]!;
    reconnectAttempt += 1;
    reconnectTimer = setTimeout(() => {
      reconnectTimer = null;
      void reconnect();
    }, delay);
    reconnectTimer.unref();
  }

  function handleFrame(raw: string): void {
    let frame: GatewayFrame;
    try {
      frame = JSON.parse(raw) as GatewayFrame;
    } catch {
      ctx.log("warn", "QQ 网关帧解析失败，已忽略。");
      return;
    }
    if (typeof frame.s === "number") lastSeq = frame.s;
    const op = typeof frame.op === "number" ? frame.op : -1;
    const eventType = typeof frame.t === "string" ? frame.t : undefined;
    switch (op) {
      case GATEWAY_OP.HELLO: {
        if (sessionId !== null && lastSeq !== null && authToken) {
          sendFrame({
            op: GATEWAY_OP.RESUME,
            d: { token: `QQBot ${authToken}`, session_id: sessionId, seq: lastSeq },
          });
        } else {
          sendFrame({
            op: GATEWAY_OP.IDENTIFY,
            d: { token: `QQBot ${authToken ?? ""}`, intents: INTENT_GROUP_AND_C2C, shard: [0, 1] },
          });
        }
        const interval = asRecord(frame.d).heartbeat_interval;
        if (typeof interval === "number" && interval > 0) startHeartbeat(interval);
        break;
      }
      case GATEWAY_OP.DISPATCH: {
        switch (eventType) {
          case "READY": {
            const sessionIdValue = asRecord(frame.d).session_id;
            if (typeof sessionIdValue === "string" && sessionIdValue) sessionId = sessionIdValue;
            reconnectAttempt = 0;
            ctx.log("info", "QQ 网关连接就绪。");
            break;
          }
          case "RESUMED": {
            reconnectAttempt = 0;
            ctx.log("info", "QQ 网关会话已恢复。");
            break;
          }
          case "C2C_MESSAGE_CREATE":
            void acceptMessage("c2c", asRecord(frame.d)).catch((error) => ctx.onError(error));
            break;
          case "GROUP_AT_MESSAGE_CREATE":
            void acceptMessage("group", asRecord(frame.d)).catch((error) => ctx.onError(error));
            break;
          case "GROUP_ADD_ROBOT":
          case "GROUP_DEL_ROBOT": {
            const groupOpenid = asRecord(frame.d).group_openid;
            if (typeof groupOpenid === "string" && groupOpenid) {
              ctx.onGroups([{ chatId: groupOpenid, added: eventType === "GROUP_ADD_ROBOT" }]);
            }
            break;
          }
        }
        break;
      }
      case GATEWAY_OP.RECONNECT: {
        // 服务端要求重连：保留会话，恢复路径走 RESUME。
        ctx.log("info", "QQ 网关要求重连。");
        reconnectAttempt = 0;
        closeCurrent();
        scheduleReconnect();
        break;
      }
      case GATEWAY_OP.INVALID_SESSION: {
        if (frame.d !== true) {
          sessionId = null;
          lastSeq = null;
        }
        ctx.log("warn", "QQ 网关会话失效，正在重连。");
        closeCurrent();
        scheduleReconnect();
        break;
      }
      case 11: // HEARTBEAT_ACK：无需处理
        break;
    }
  }

  function handleDisconnect(code: number): void {
    if (intentionalClose || stopped) return;
    if (code === 4004) {
      authFatal = true;
      ctx.onError(new UserFacingError(
        "QQ 鉴权失败（WebSocket 关闭码 4004）：请检查 AppID 与 AppSecret 是否正确，以及机器人是否已在平台启用。",
      ));
      return;
    }
    scheduleReconnect();
  }

  async function connect(): Promise<void> {
    const gatewayUrl = str(asRecord(await deps.api("/gateway", "GET")).url);
    if (!gatewayUrl) throw new Error("QQ 网关地址响应无效：缺少 url 字段。");
    const token = await deps.api(TOKEN_PATH, "POST");
    authToken = typeof token === "string" ? token : null;
    if (!authToken) throw new Error("QQ 访问令牌为空，无法连接网关。");
    const ws = deps.createWs(gatewayUrl);
    socket = ws;
    intentionalClose = false;
    ws.addEventListener("message", (event) => {
      if (event.type !== "message") return;
      void handleFrame(decodeFrameData(event.data));
    });
    ws.addEventListener("error", (event) => {
      if (event.type !== "error") return;
      const error = event.error instanceof Error ? event.error : new Error(String(event.error));
      ctx.onError(error);
    });
    ws.addEventListener("close", (event) => {
      if (event.type !== "close") return;
      stopHeartbeat();
      handleDisconnect(event.code);
    });
  }

  async function reconnect(): Promise<void> {
    if (stopped || authFatal || ctx.signal.aborted) return;
    try {
      await connect();
    } catch (error) {
      if (error instanceof UserFacingError) {
        authFatal = true;
        ctx.onError(error);
        return;
      }
      ctx.onError(error instanceof Error ? error : new Error(String(error)));
      scheduleReconnect();
    }
  }

  async function start(): Promise<void> {
    if (!appId) throw new UserFacingError("缺少 AppID：请在连接设置中填写 QQ 开放平台的 AppID。");
    const appSecret = ctx.secrets.appSecret?.trim() ?? "";
    if (!appSecret) {
      throw new UserFacingError("缺少 AppSecret：请在连接设置中填写 QQ 开放平台的 AppSecret 后重试。");
    }
    await loadState();
    ctx.signal.addEventListener("abort", () => { void stop(); }, { once: true });
    try {
      await connect();
    } catch (error) {
      if (error instanceof UserFacingError) throw error;
      ctx.onError(error instanceof Error ? error : new Error(String(error)));
      scheduleReconnect();
    }
  }

  async function stop(): Promise<void> {
    stopped = true;
    stopHeartbeat();
    if (reconnectTimer) {
      clearTimeout(reconnectTimer);
      reconnectTimer = null;
    }
    closeCurrent();
  }

  function sanitizeContent(content: string): string {
    let text = content
      .replace(/\[<face,id=\d+\/?>]/g, "")
      .replace(/<faceType=\d+,faceId="[^"]*",ext="[^"]*">/g, "");
    if (appId) text = text.replace(new RegExp(`<@!?${escapeRegExp(appId)}>\\s*`, "g"), "");
    return text.trim();
  }

  function extractQuote(data: Record<string, unknown>): { text: string } | undefined {
    const elements = data.msg_elements;
    if (!Array.isArray(elements) || elements.length === 0) return undefined;
    const text = str(asRecord(elements[0]).content);
    return text ? { text } : undefined;
  }

  function quoteMatchesOwn(chatId: string, quoteText: string | undefined): boolean {
    if (!quoteText) return false;
    const own = ownTextsByChat.get(chatId);
    return own?.includes(quoteText.trim()) === true;
  }

  async function downloadAttachment(
    attachment: Record<string, unknown>,
    url: string,
    maxBytes: number,
  ): Promise<NormalizedMedia> {
    const normalizedUrl = url.startsWith("//") ? `https:${url}` : url;
    const rawContentType = str(attachment.content_type);
    const contentType = rawContentType?.split(";")[0]?.trim().toLowerCase();
    const filename = str(attachment.filename)?.trim() || urlBasename(normalizedUrl) || "attachment";
    const fetchImpl = deps.fetchImpl ?? fetch;
    const response = await fetchImpl(normalizedUrl);
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    const declaredLength = Number(response.headers.get("content-length"));
    if (Number.isFinite(declaredLength) && declaredLength > maxBytes) {
      throw new Error(`附件 ${Math.ceil(declaredLength / 1024 / 1024)}MB 超过下载上限`);
    }
    const buffer = Buffer.from(await response.arrayBuffer());
    if (buffer.byteLength > maxBytes) {
      throw new Error(`附件 ${Math.ceil(buffer.byteLength / 1024 / 1024)}MB 超过下载上限`);
    }
    await mkdir(ctx.mediaDir, { recursive: true, mode: 0o700 });
    const path = join(ctx.mediaDir, `${randomUUID()}-${safeFilename(filename)}`);
    await writeFile(path, buffer, { mode: 0o600 });
    return {
      kind: attachmentKind(contentType, filename),
      path,
      ...(contentType ? { mime: contentType } : {}),
      name: filename,
    };
  }

  async function downloadAttachments(chatId: string, attachments: unknown): Promise<NormalizedMedia[]> {
    if (!Array.isArray(attachments)) return [];
    const media: NormalizedMedia[] = [];
    const settings = await ctx.settings();
    const maxBytes = settings.imageMaxDownloadMb * 1024 * 1024;
    for (const entry of attachments) {
      const attachment = asRecord(entry);
      const url = str(attachment.url)?.trim();
      if (!url || !isAllowedAttachmentUrl(url)) {
        ctx.log("warn", `QQ 附件来源不在白名单（.qq.com/.qq.com.cn/.qpic.cn/.myqcloud.com/.tencentcos.com/.ugcimg.cn），已跳过：${url?.slice(0, 120) ?? "(无 URL)"}`);
        continue;
      }
      try {
        media.push(await downloadAttachment(attachment, url, maxBytes));
      } catch (error) {
        ctx.log("warn", `QQ 附件下载失败，已跳过：${error instanceof Error ? error.message : String(error)}`);
      }
    }
    return media;
  }

  async function acceptMessage(kind: "c2c" | "group", data: Record<string, unknown>): Promise<void> {
    const messageId = str(data.id);
    if (!messageId) return;
    if (ownMessageIds.has(messageId) || state.seen[messageId] !== undefined) return;
    state.seen[messageId] = Date.now();

    const author = asRecord(data.author);
    if (author.bot === true) {
      saveState();
      return;
    }
    const senderName = str(author.username);
    const chatId = kind === "group" ? str(data.group_openid) : (str(author.user_openid) ?? str(author.id));
    const senderId = str(author.member_openid) ?? str(author.user_openid) ?? str(author.id);
    if (!chatId || !senderId) {
      saveState();
      ctx.log("warn", "QQ 消息缺少会话或发送者标识，已忽略。");
      return;
    }

    state.scopes[chatId] = kind;
    state.lastInbound[chatId] = { msgId: messageId, at: Date.now() };

    const text = sanitizeContent(str(data.content) ?? "");
    const media = await downloadAttachments(chatId, data.attachments);
    if (!text && media.length === 0) {
      // 无文本无媒体：平台噪音，不投递。
      saveState();
      return;
    }

    const quote = extractQuote(data);
    const message: NormalizedMessage = {
      channel: "qq",
      connectionId: ctx.connectionId,
      kind: kind === "group" ? "group" : "dm",
      chatId,
      senderId,
      ...(senderName ? { senderName } : {}),
      messageId,
      timestamp: parseTimestamp(data.timestamp),
      ...(text ? { text } : {}),
      ...(media.length > 0 ? { media } : {}),
      ...(quote ? { replyTo: quote } : {}),
      trigger: {
        // 私聊天然面向机器人；群消息只有 @机器人 才会推送。
        mention: true,
        replyToBot: quoteMatchesOwn(chatId, quote?.text),
        command: text.startsWith("/"),
      },
    };
    saveState();
    ctx.onMessage(message);
  }

  function rememberOwnText(chatId: string, text: string): void {
    const list = ownTextsByChat.get(chatId) ?? [];
    list.push(text.trim());
    if (list.length > 20) list.splice(0, list.length - 20);
    ownTextsByChat.set(chatId, list);
  }

  function enforceGroupProactiveQuota(): void {
    const now = Date.now();
    while (proactiveGroupSends.length > 0 && now - proactiveGroupSends[0]! > GROUP_PROACTIVE_WINDOW_MS) {
      proactiveGroupSends.shift();
    }
    if (proactiveGroupSends.length >= GROUP_PROACTIVE_LIMIT) {
      throw new UserFacingError(
        "QQ 群主动消息已达频率上限（5 条/5 分钟）：请稍后重试，或回复群内消息触发被动发送。",
      );
    }
    proactiveGroupSends.push(now);
  }

  function passiveReplyTarget(scope: "c2c" | "group", replyToId: string | undefined): string | undefined {
    if (!replyToId) return undefined;
    const receivedAt = Object.values(state.lastInbound).find((entry) => entry.msgId === replyToId)?.at;
    if (receivedAt === undefined) return undefined;
    return Date.now() - receivedAt <= PASSIVE_WINDOW_MS[scope] ? replyToId : undefined;
  }

  function nextSeq(): number {
    if (msgSeqSeed === 0) msgSeqSeed = nextMsgSeq();
    const value = msgSeqSeed;
    msgSeqSeed = (msgSeqSeed + 1) & 0xffff;
    return value;
  }

  async function sendChunk(
    scope: "c2c" | "group",
    chatId: string,
    chunk: string,
    msgId: string | undefined,
    msgSeq: number,
    mode: "markdown" | "plain",
  ): Promise<string | undefined> {
    const path = scope === "c2c"
      ? `/v2/users/${chatId}/messages`
      : `/v2/groups/${chatId}/messages`;
    const body: Record<string, unknown> = mode === "markdown"
      ? { msg_type: 2, markdown: { content: chunk }, msg_seq: msgSeq }
      : { msg_type: 0, content: chunk, msg_seq: msgSeq };
    if (msgId !== undefined) body.msg_id = msgId;
    const response = asRecord(await deps.api(path, "POST", body));
    const id = str(response.id);
    if (id) ownMessageIds.add(id);
    return id;
  }

  async function sendChunkWithFallback(
    scope: "c2c" | "group",
    chatId: string,
    chunk: string,
    msgId: string | undefined,
    msgSeq: number,
  ): Promise<string | undefined> {
    try {
      return await sendChunk(scope, chatId, chunk, msgId, msgSeq, "markdown");
    } catch (error) {
      if (!isMarkdownRejection(error)) throw error;
      ctx.log("warn", `QQ 拒绝 markdown（${error.bizCode ?? "未知错误码"}），本条回退纯文本重发。`);
      return sendChunk(scope, chatId, chunk, msgId, msgSeq, "plain");
    }
  }

  async function sendText(outbound: OutboundText): Promise<SendResult> {
    if (!outbound.text?.trim()) return {};
    const scope = state.scopes[outbound.chatId] ?? "c2c";
    const chunks = splitChunks(outbound.text, MAX_CHUNK_CHARS).filter((chunk) => chunk.length > 0);
    if (chunks.length === 0) return {};

    const passiveMsgId = passiveReplyTarget(scope, outbound.replyToId);
    const budget = passiveMsgId !== undefined
      ? Math.max(1, PASSIVE_LIMIT[scope] - (passiveUsed.get(passiveMsgId) ?? 0))
      : 0;
    const overflow = passiveMsgId !== undefined && chunks.length > budget;
    const passiveCount = overflow ? Math.max(1, budget - 1) : chunks.length;

    const sendChunkAs = async (chunk: string, msgId: string | undefined): Promise<string | undefined> => {
      if (msgId === undefined && scope === "group") enforceGroupProactiveQuota();
      const id = await sendChunkWithFallback(scope, outbound.chatId, chunk, msgId, nextSeq());
      rememberOwnText(outbound.chatId, chunk);
      if (msgId !== undefined) passiveUsed.set(msgId, (passiveUsed.get(msgId) ?? 0) + 1);
      return id;
    };

    let lastId: string | undefined;
    let index = 0;
    for (const chunk of chunks) {
      if (index >= passiveCount) break;
      lastId = await sendChunkAs(chunk, passiveMsgId);
      index += 1;
    }

    if (overflow && passiveMsgId !== undefined) {
      // 超出被动额度：先用提示占住剩余被动额度（若有），提示用户其余分片转主动发送。
      const noticeMsgId = (passiveUsed.get(passiveMsgId) ?? 0) < PASSIVE_LIMIT[scope] ? passiveMsgId : undefined;
      try {
        const noticeId = await sendChunk(scope, outbound.chatId, PARTIAL_REPLY_NOTICE, noticeMsgId, nextSeq(), "plain");
        if (noticeId) ownMessageIds.add(noticeId);
        if (noticeMsgId !== undefined) {
          passiveUsed.set(noticeMsgId, (passiveUsed.get(noticeMsgId) ?? 0) + 1);
        }
      } catch (error) {
        ctx.log("warn", `QQ 长回复提示发送失败：${error instanceof Error ? error.message : String(error)}`);
      }
      // 提示发出后，剩余分片全部转主动发送（无 msg_id）。
      for (; index < chunks.length; index += 1) {
        lastId = await sendChunkAs(chunks[index]!, undefined);
      }
    }
    return { messageId: lastId };
  }

  async function sendTyping(chatId: string): Promise<void> {
    const scope = state.scopes[chatId] ?? "c2c";
    if (scope !== "c2c") return; // 平台仅支持私聊输入状态
    try {
      await deps.api(`/v2/users/${chatId}/messages`, "POST", {
        msg_type: 6,
        input_notify: { input_type: 1, input_second: 60 },
        msg_seq: nextSeq(),
      });
    } catch (error) {
      ctx.log("warn", `QQ 输入状态发送失败：${error instanceof Error ? error.message : String(error)}`);
    }
  }

  /** 从设置页触发：验证凭据能否换取访问令牌。 */
  async function testConnection(): Promise<{ ok: boolean; detail?: string }> {
    try {
      await deps.api(TOKEN_PATH, "POST");
      return { ok: true };
    } catch (error) {
      return { ok: false, detail: error instanceof Error ? error.message : String(error) };
    }
  }

  return {
    channel: "qq",
    capabilities,
    start,
    stop,
    sendText,
    sendTyping,
    testConnection,
    remembersOwnMessages: true,
    isOwnMessage: (messageId) => ownMessageIds.has(messageId),
    rememberOwnMessage: (messageId) => { ownMessageIds.add(messageId); },
  };
}

export const definition: ChannelDefinition<QqConnection> = {
  channel: "qq",
  create: (ctx) => createAdapter(ctx),
  configure: (fields) => {
    requireFields(CHANNEL_SPECS.qq.fields, fields);
    return defaultConfigure("qq", fields);
  },
};
