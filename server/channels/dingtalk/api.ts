/** DingTalk REST protocol facts: access token, session-webhook and robot-API
 * sends, media upload/download and AI Card streaming.
 * Protocol facts ported from xmanrui/dsh-im (MIT). */
import { randomUUID } from "node:crypto";
import { sleep } from "../../files.ts";

export const DINGTALK_API_BASE = "https://api.dingtalk.com/";
export const DINGTALK_OAPI_BASE = "https://oapi.dingtalk.com/";
/** 钉钉官方 AI 流式卡片模板。 */
export const AI_CARD_TEMPLATE_ID = "02fcf2f4-5e02-4a85-b672-46d1f715543e.schema";
/** 相邻卡片请求之间的最小间隔，避免触发服务端限流。 */
export const CARD_MIN_INTERVAL_MS = 50;
/** 卡片请求被 403 拒绝后的重试等待。 */
export const CARD_RETRY_DELAY_MS = 1000;

export interface DWDownStream {
  headers?: { messageId?: string; topic?: string };
  data: string | unknown;
}

export interface DWClientLike {
  registerCallbackListener(topic: string, listener: (downstream: DWDownStream) => void): unknown;
  connect(): Promise<void>;
  disconnect(): void;
  socketCallBackResponse?(messageId: string, result: unknown): void;
  connected?: boolean;
  socket?: { readyState?: number } | null;
}

/** 传输层注入点：测试用假实现替换，生产用 defaultDeps（见 index.ts）。 */
export interface DingtalkDeps {
  /** JSON 请求；上传时 body 为 FormData、token 走 query，其余带访问令牌头。 */
  rest(path: string, body?: unknown, base?: string, method?: "POST" | "PUT", token?: string): Promise<unknown>;
  webhook(url: string, body: unknown): Promise<unknown>;
  createDw(config: { clientId: string; clientSecret: string }): DWClientLike;
  fetchUrl?(url: string): Promise<{ ok: boolean; status: number; arrayBuffer(): Promise<ArrayBuffer> }>;
  backoffMs?: number[];
  monitorIntervalMs?: number;
  now?: () => number;
  /** connect() 返回后确认 socket 就绪；默认轮询 connected/readyState（上限 5s）。 */
  waitConnected?(dw: DWClientLike, signal: AbortSignal): Promise<boolean>;
  /** 卡片 403 重试等待（测试可缩短）。 */
  cardRetryDelayMs?: number;
}

export interface DingTalkRepliedMsg {
  msgId?: unknown;
  messageId?: unknown;
  senderStaffId?: unknown;
  senderId?: unknown;
  senderNick?: unknown;
  senderName?: unknown;
  sender?: unknown;
  msgtype?: unknown;
  content?: unknown;
  text?: unknown;
}

/** 机器人消息回调帧（dingtalk-stream TOPIC_ROBOT 的 data 载荷）。 */
export interface DingTalkFrame {
  conversationId?: unknown;
  conversationType?: unknown;
  msgId?: unknown;
  msgtype?: unknown;
  senderStaffId?: unknown;
  senderId?: unknown;
  senderNick?: unknown;
  chatbotUserId?: unknown;
  robotCode?: unknown;
  atUsers?: unknown;
  isAtAll?: unknown;
  isInAtList?: unknown;
  text?: { content?: unknown; repliedMsg?: unknown };
  content?: unknown;
  sessionWebhook?: unknown;
  sessionWebhookExpiredTime?: unknown;
  createAt?: unknown;
  messageFileDownloadUrl?: unknown;
}

export interface RobotTarget {
  kind: "dm" | "group";
  chatId: string;
  userId?: string;
  robotCode: string;
}

export interface CardTarget {
  type: "group" | "user";
  openConversationId?: string;
  userId?: string;
  robotCode: string;
}

export class DingTalkApiError extends Error {
  readonly status?: number;
  readonly providerCode?: string | number;
  constructor(message: string, options: { status?: number; providerCode?: string | number } = {}) {
    super(message);
    this.name = "DingTalkApiError";
    this.status = options.status;
    this.providerCode = options.providerCode;
  }
}

export function str(value: unknown): string | undefined {
  if (typeof value === "string") {
    const trimmed = value.trim();
    return trimmed ? trimmed : undefined;
  }
  if (typeof value === "number" && Number.isFinite(value)) return String(value);
  return undefined;
}

export function asText(value: unknown): string | undefined {
  return typeof value === "string" ? value : undefined;
}

export function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

export function toNumber(value: unknown): number {
  if (typeof value === "number") return Number.isFinite(value) ? value : Number.NaN;
  if (typeof value === "string" && value.trim()) {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : Number.NaN;
  }
  return Number.NaN;
}

export function extOf(fileName: string): string {
  const index = fileName.lastIndexOf(".");
  return index > 0 ? fileName.slice(index + 1).toLowerCase() : "";
}

/** errcode 为 0（或数字字符串 "0"）视为成功，其余抛 DingTalkApiError。 */
function assertAccepted(response: Record<string, unknown> | undefined, action: string): void {
  const errcode = response?.errcode;
  if (errcode === undefined) return;
  if (toNumber(errcode) !== 0) {
    throw new DingTalkApiError(`钉钉${action}被拒绝：${str(response?.errmsg) ?? String(errcode)}。`, {
      providerCode: typeof errcode === "number" || typeof errcode === "string" ? errcode : undefined,
    });
  }
}

function asRecord(value: unknown): Record<string, unknown> | undefined {
  return isRecord(value) ? value : undefined;
}

function cardData(markdown: string, flowStatus: string): Record<string, unknown> {
  return {
    cardParamMap: {
      flowStatus,
      msgContent: markdown,
      staticMsgContent: "",
      sys_full_json_obj: JSON.stringify({ order: ["msgContent"] }),
      config: JSON.stringify({ autoLayout: true }),
    },
  };
}

/** 纯文本转卡片 Markdown：围栏外的单换行转 <br>（ported from dsh）。 */
export function cardMarkdown(text: string): string {
  const normalized = text.replace(/\r\n?/g, "\n");
  const lines = normalized.split("\n");
  let inFence = false;
  return lines
    .map((line, index) => {
      const fenced = /^\s{0,3}```/.test(line);
      const currentInFence = inFence;
      if (fenced) inFence = !inFence;
      if (index === lines.length - 1) return line;
      if (currentInFence || fenced || inFence || !line || !lines[index + 1]) return `${line}\n`;
      if (/^\s{0,3}(?:[-*+] |\d+[.)] |#{1,6} |\||> )/.test(lines[index + 1] ?? "")) return `${line}\n`;
      return `${line}<br>`;
    })
    .join("");
}

/** REST/卡片协议封装：令牌缓存、webhook 与机器人发送、媒体上传下载地址、AI 卡片流。 */
export class DingTalkApi {
  private readonly rest: DingtalkDeps["rest"];
  private readonly webhook: DingtalkDeps["webhook"];
  private readonly nowFn: () => number;
  private readonly clientId: string;
  private readonly clientSecret: string;
  private readonly signal: AbortSignal;
  private tokenCache: { value: string; expiresAt: number } | undefined;
  private tokenInflight: Promise<string> | undefined;
  private cardTail: Promise<unknown> = Promise.resolve();
  private nextCardAt = 0;
  private readonly cardRetryDelayMs: number;

  constructor(
    deps: Pick<DingtalkDeps, "rest" | "webhook" | "now" | "cardRetryDelayMs">,
    clientId: string,
    clientSecret: string,
    signal: AbortSignal,
  ) {
    this.rest = deps.rest;
    this.webhook = deps.webhook;
    this.nowFn = deps.now ?? (() => Date.now());
    this.cardRetryDelayMs = deps.cardRetryDelayMs ?? CARD_RETRY_DELAY_MS;
    this.clientId = clientId;
    this.clientSecret = clientSecret;
    this.signal = signal;
  }

  private async call(
    path: string,
    body?: unknown,
    token?: string,
    method: "POST" | "PUT" = "POST",
  ): Promise<Record<string, unknown> | undefined> {
    const response = await this.rest(path, body, DINGTALK_API_BASE, method, token);
    return asRecord(response);
  }

  /** access_token：带 60s 余量缓存与并发单飞。 */
  async token(): Promise<string> {
    const cached = this.tokenCache;
    if (cached && cached.expiresAt > this.nowFn()) return cached.value;
    const inflight = this.tokenInflight ?? this.requestToken();
    this.tokenInflight = inflight;
    try {
      return await inflight;
    } finally {
      if (this.tokenInflight === inflight) this.tokenInflight = undefined;
    }
  }

  private async requestToken(): Promise<string> {
    const response = await this.call("v1.0/oauth2/accessToken", {
      appKey: this.clientId,
      appSecret: this.clientSecret,
    });
    const value = str(response?.accessToken);
    if (!value) throw new DingTalkApiError("钉钉服务没有返回访问令牌。");
    const expireIn = toNumber(response?.expireIn ?? response?.expiresIn);
    const seconds = Number.isFinite(expireIn) && expireIn > 0 ? expireIn : 7200;
    this.tokenCache = { value, expiresAt: this.nowFn() + Math.max(1_000, (seconds - 60) * 1_000) };
    return value;
  }

  async sendWebhookText(url: string, content: string): Promise<void> {
    const response = asRecord(await this.webhook(url, { msgtype: "text", text: { content } }));
    assertAccepted(response, "回复消息");
  }

  async sendWebhookMedia(url: string, kind: "image" | "file", mediaId: string, fileName: string): Promise<void> {
    const body =
      kind === "image"
        ? { msgtype: "image", image: { media_id: mediaId } }
        : { msgtype: "file", file: { media_id: mediaId, fileName } };
    const response = asRecord(await this.webhook(url, body));
    assertAccepted(response, "媒体消息");
  }

  async robotSendText(target: RobotTarget, content: string): Promise<void> {
    await this.robotSend(target, "sampleText", { content });
  }

  async robotSendMedia(target: RobotTarget, kind: "image" | "file", mediaId: string, fileName: string): Promise<void> {
    const msgParam =
      kind === "image" ? { photoURL: mediaId } : { mediaId, fileName, fileType: extOf(fileName) };
    await this.robotSend(target, kind === "image" ? "sampleImageMsg" : "sampleFile", msgParam);
  }

  private async robotSend(target: RobotTarget, msgKey: string, msgParam: Record<string, unknown>): Promise<void> {
    const token = await this.token();
    const path =
      target.kind === "group" ? "v1.0/robot/groupMessages/send" : "v1.0/robot/oToMessages/batchSend";
    const body = {
      robotCode: target.robotCode,
      msgKey,
      msgParam: JSON.stringify(msgParam),
      ...(target.kind === "group"
        ? { openConversationId: target.chatId }
        : { userIds: [target.userId ?? ""] }),
    };
    const response = await this.call(path, body, token);
    if (response?.errcode !== undefined) assertAccepted(response, "主动消息");
  }

  /** oapi /media/upload（multipart FieldType file/image）→ media_id。 */
  async uploadMedia(kind: "image" | "file", bytes: Uint8Array, fileName: string, mime?: string): Promise<string> {
    const token = await this.token();
    const form = new FormData();
    form.append("media", new Blob([bytes], { type: mime ?? "application/octet-stream" }), fileName);
    const path = `media/upload?access_token=${encodeURIComponent(token)}&type=${kind}`;
    const response = asRecord(await this.rest(path, form, DINGTALK_OAPI_BASE));
    assertAccepted(response, "媒体上传");
    const mediaId = str(response?.media_id);
    if (!mediaId) throw new DingTalkApiError("钉钉服务没有返回 media_id。");
    return mediaId;
  }

  /** downloadCode → 下载地址（需机器人“企业内机器人发送消息”权限）。 */
  async downloadUrl(downloadCode: string, robotCode: string): Promise<string> {
    const token = await this.token();
    const response = await this.call("v1.0/robot/messageFiles/download", { downloadCode, robotCode }, token);
    const url = str(response?.downloadUrl) ?? str(response?.download_url);
    if (!url) throw new DingTalkApiError("钉钉服务没有返回媒体下载地址。");
    return url;
  }

  async createCard(target: CardTarget, initialMarkdown: string): Promise<string> {
    const instanceId = `paseo_${randomUUID()}`;
    const body: Record<string, unknown> = {
      outTrackId: instanceId,
      userIdType: 1,
      openSpaceId:
        target.type === "group"
          ? `dtv1.card//IM_GROUP.${target.openConversationId ?? ""}`
          : `dtv1.card//IM_ROBOT.${target.userId ?? ""}`,
      cardTemplateId: AI_CARD_TEMPLATE_ID,
      cardData: cardData(initialMarkdown, "2"),
      callbackType: "STREAM",
      imGroupOpenSpaceModel: { supportForward: true },
      imRobotOpenSpaceModel: { supportForward: true },
    };
    if (target.type === "group") {
      body.imGroupOpenDeliverModel = { robotCode: target.robotCode };
    } else {
      body.imRobotOpenDeliverModel = {
        spaceType: "IM_ROBOT",
        robotCode: target.robotCode,
        extension: { dynamicSummary: "true" },
      };
    }
    await this.cardRequest("v1.0/card/instances/createAndDeliver", body);
    try {
      await this.streaming(instanceId, initialMarkdown, false);
    } catch {
      // createAndDeliver 已让首帧可见；失败留给 finish 流式覆盖。
    }
    return instanceId;
  }

  async updateCard(instanceId: string, markdown: string): Promise<void> {
    await this.streaming(instanceId, markdown, false);
  }

  async finishCard(instanceId: string, markdown: string): Promise<void> {
    await this.streaming(instanceId, markdown, true);
    await this.cardRequest("v1.0/card/instances", {
      outTrackId: instanceId,
      cardData: cardData(markdown, "3"),
      cardUpdateOptions: { updateCardDataByKey: true },
    }, "PUT");
  }

  private async streaming(instanceId: string, markdown: string, finalize: boolean): Promise<void> {
    await this.cardRequest("v1.0/card/streaming", {
      outTrackId: instanceId,
      guid: randomUUID(),
      key: "msgContent",
      content: markdown,
      isFull: true,
      isFinalize: finalize,
      isError: false,
    }, "PUT");
  }

  private async cardRequest(
    path: string,
    body: unknown,
    method: "POST" | "PUT" = "POST",
  ): Promise<Record<string, unknown> | undefined> {
    // 卡片三接口必须携带访问令牌，缺失会被网关直接拒绝。
    const token = await this.token();
    await this.acquireCardSlot();
    try {
      return await this.call(path, body, token, method);
    } catch (error) {
      // 403（HTTP 状态或业务 errcode）视为限流/冲突，退避后重试一次。
      const forbidden =
        error instanceof DingTalkApiError &&
        (error.status === 403 || error.providerCode === 403 || error.providerCode === "403");
      if (!forbidden) throw error;
      await sleep(this.cardRetryDelayMs, this.signal);
      await this.acquireCardSlot();
      return this.call(path, body, token, method);
    }
  }

  private async acquireCardSlot(): Promise<void> {
    const acquire = async (): Promise<void> => {
      const wait = Math.max(0, this.nextCardAt - this.nowFn());
      if (wait > 0) await sleep(wait, this.signal);
      this.nextCardAt = Math.max(this.nextCardAt, this.nowFn()) + CARD_MIN_INTERVAL_MS;
    };
    const slot = this.cardTail.then(acquire, acquire);
    this.cardTail = slot.catch(() => undefined);
    await slot;
  }
}
