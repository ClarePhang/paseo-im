/** Matrix channel adapter: long-poll sync loop, inbound normalization, and
 * outbound send/edit/typing/media over the CS API v3. Protocol facts ported
 * from xmanrui/dsh-im (MIT); E2EE rooms are skipped (unsupported in v1). */
import { randomUUID } from "node:crypto";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { basename, join } from "node:path";
import type { AdapterContext, ChannelAdapter } from "../types.ts";
import type {
  NormalizedMessage,
  OutboundMedia,
  OutboundText,
  SendResult,
} from "../../../shared/model.ts";
import { UserFacingError } from "../../../shared/errors.ts";
import { atomicWrite, readJson, WriteChain } from "../../../server/files.ts";
import {
  MAX_MEDIA_BYTES,
  MatrixApiError,
  TYPING_RENEW_MS,
  TYPING_TIMEOUT_MS,
  defaultDeps,
  homeserverUrl,
  isMatrixUserId,
  type MatrixConnection,
  type MatrixDeps,
} from "./api.ts";
import { asRecord, projectEvent } from "./normalize.ts";
import { MAX_CHUNK_CHARS, editContent, splitFenceAware, textContent } from "./format.ts";

const RECONNECT_DELAYS_MS = [1_000, 3_000, 5_000, 10_000, 30_000];
const SEEN_CAP = 500;
const RECENT_CAP = 200;
const OWN_CAP = 1_000;
/** 非永久 join 失败的退避窗口：期间该邀请跳过本批，后续批次照常摄取。 */
const JOIN_RETRY_DELAY_MS = 30_000;
const SYNC_FILTER = JSON.stringify({ room: { timeline: { limit: 20 } } });
const MEDIA_EXTENSIONS: Record<string, string> = {
  ".png": "image/png",
  ".jpg": "image/jpeg",
  ".jpeg": "image/jpeg",
  ".gif": "image/gif",
  ".webp": "image/webp",
  ".svg": "image/svg+xml",
  ".pdf": "application/pdf",
  ".txt": "text/plain",
  ".mp4": "video/mp4",
  ".mp3": "audio/mpeg",
  ".zip": "application/zip",
};

interface MatrixState {
  version: 1;
  nextBatch?: string;
  dmRooms: string[];
  seen: string[];
  /** 机器人自己发出的 message 事件 id（replyToBot 启发式），重启后仍可用。 */
  own?: string[];
  session?: { accessToken: string; deviceId?: string; userId?: string };
}

interface StreamSession {
  chatId: string;
  lastText: string;
}

export class MatrixAdapter implements ChannelAdapter {
  readonly channel = "matrix" as const;
  readonly capabilities = {
    progress: "edit" as const,
    markdown: "html" as const,
    uploadImage: true,
    uploadFile: true,
    reactions: false,
    typing: true,
    maxChunkChars: MAX_CHUNK_CHARS,
    threads: true,
  };
  remembersOwnMessages = true;

  private readonly ctx: AdapterContext<MatrixConnection>;
  private readonly deps: MatrixDeps;
  private readonly statePath: string;
  private readonly stateChain = new WriteChain();
  private state: MatrixState = { version: 1, dmRooms: [], seen: [] };
  private stateSignature = "";
  private readonly dmRooms = new Set<string>();
  private readonly seenIds = new Set<string>();
  private readonly seenOrder: string[] = [];
  private readonly ownIds = new Set<string>();
  private readonly ownOrder: string[] = [];
  private readonly recentEvents = new Map<string, { sender: string; text: string }>();
  private readonly streams = new Map<string, StreamSession>();
  private readonly typingTimers = new Map<string, NodeJS.Timeout>();
  private readonly warnedEncrypted = new Set<string>();
  private readonly inviteHandled = new Set<string>();
  /** roomId → 下次允许重试 join 的时间；非永久失败退避，避免毒丸阻塞摄取。 */
  private readonly inviteRetryAt = new Map<string, number>();
  private readonly lifecycle = new AbortController();
  private accessToken = "";
  private botUserId = "";
  private started = false;
  private stopped = false;
  private loopTask: Promise<void> | null = null;
  private backoffIndex = 0;
  private authFailed = false;

  constructor(ctx: AdapterContext<MatrixConnection>, deps?: MatrixDeps) {
    this.ctx = ctx;
    this.deps = deps ?? defaultDeps({
      homeserver: ctx.config.homeserver,
      token: () => this.accessToken,
    });
    this.statePath = join(ctx.dataDir, "matrix-state.json");
  }

  async start(): Promise<void> {
    if (this.started) return;
    const cfg = this.ctx.config;
    if (!homeserverUrl(cfg.homeserver)) {
      throw new UserFacingError("Homeserver 地址无效，请填写形如 https://matrix.example.org 的地址。");
    }
    if (!isMatrixUserId(cfg.userId)) {
      throw new UserFacingError("用户 ID 需形如 @bot:example.org，请核对连接设置。");
    }
    this.state = (await readJson<MatrixState>(this.statePath, parseState)) ?? { version: 1, dmRooms: [], seen: [] };
    for (const roomId of this.state.dmRooms) this.dmRooms.add(roomId);
    for (const eventId of this.state.seen) this.markSeen(eventId);
    for (const eventId of this.state.own ?? []) this.rememberOwnMessage(eventId);
    await this.resolveAuth();
    this.started = true;
    this.stopped = false;
    this.ctx.signal.addEventListener("abort", () => void this.stop(), { once: true });
    await mkdir(this.ctx.mediaDir, { recursive: true }).catch(() => {});
    if (!this.state.nextBatch) {
      try {
        await this.syncOnce();
      } catch (error) {
        if (error instanceof UserFacingError) throw error;
        this.report(error);
      }
    }
    if (this.stopped) return;
    this.loopTask = this.loop();
  }

  async stop(): Promise<void> {
    if (this.stopped) return;
    this.stopped = true;
    this.started = false;
    this.lifecycle.abort();
    for (const timer of this.typingTimers.values()) clearTimeout(timer);
    this.typingTimers.clear();
    await this.loopTask?.catch(() => {});
    this.loopTask = null;
  }

  private async resolveAuth(): Promise<void> {
    const cfg = this.ctx.config;
    const tokenSecret = (this.ctx.secrets.accessToken ?? "").trim();
    const password = this.ctx.secrets.password ?? "";
    if (cfg.authMode === "token") {
      if (!tokenSecret) {
        throw new UserFacingError("缺少 Access Token：请在 Element 客户端「设置 → 帮助与关于 → 访问令牌」中获取后填入连接凭据。");
      }
      this.accessToken = tokenSecret;
    } else {
      // password 模式：优先复用持久化会话；whoami 永久失败（401/M_UNKNOWN_TOKEN
      // 类，会话已被服务器吊销）时清掉 session 用密码重登一次再验证。token 模式不变。
      const savedToken = this.state.session?.accessToken;
      if (savedToken) {
        this.accessToken = savedToken;
        try {
          await this.assertIdentity();
          return;
        } catch (error) {
          if (!(error instanceof MatrixApiError && error.permanent)) throw this.asCredentialError(error);
          this.state.session = undefined;
          this.accessToken = "";
          this.stateSignature = ""; // 会话变更必须落盘
          await this.persistState();
          this.ctx.log("warn", "已保存的 Matrix 会话已失效，正在用密码重新登录。");
        }
      }
      if (!password) {
        throw new UserFacingError("缺少密码：请在连接凭据中填写密码，或改用 Access Token 登录。");
      }
      let login: Record<string, unknown>;
      try {
        login = await this.deps.rest("POST", "/login", {
          type: "m.login.password",
          identifier: { type: "m.id.user", user: cfg.userId },
          password,
          initial_device_display_name: "Paseo IM",
        }, this.requestSignal());
      } catch (error) {
        throw this.asCredentialError(error);
      }
      const accessToken = typeof login.access_token === "string" ? login.access_token : "";
      if (!accessToken) throw new UserFacingError("Matrix 登录未返回 access_token，请检查用户名与密码。");
      this.accessToken = accessToken;
      this.state.session = {
        accessToken,
        deviceId: typeof login.device_id === "string" ? login.device_id : undefined,
        userId: typeof login.user_id === "string" ? login.user_id : undefined,
      };
      this.stateSignature = ""; // 会话变更必须落盘
      await this.persistState();
    }
    await this.assertIdentity().catch((error) => {
      throw this.asCredentialError(error);
    });
  }

  /** whoami 验证并记录机器人身份；错误原样上抛，由 resolveAuth 统一转用户可读。 */
  private async assertIdentity(): Promise<void> {
    const whoami = await this.deps.rest("GET", "/account/whoami", undefined, this.requestSignal());
    const verified = typeof whoami.user_id === "string" ? whoami.user_id : "";
    if (!verified) throw new UserFacingError("Matrix whoami 未返回 user_id，请检查凭据是否正确。");
    if (verified.toLowerCase() !== this.ctx.config.userId.trim().toLowerCase()) {
      throw new UserFacingError(`凭据对应的用户 ${verified} 与配置的用户 ${this.ctx.config.userId} 不一致，请核对连接设置。`);
    }
    this.botUserId = verified;
  }

  private asCredentialError(error: unknown): UserFacingError {
    if (error instanceof UserFacingError) return error;
    const detail = error instanceof Error ? error.message : String(error);
    if (error instanceof MatrixApiError && error.permanent) {
      return new UserFacingError(`Matrix 认证失败（${error.errcode ?? "认证错误"}）：请更新 Access Token 或密码后重试。`);
    }
    return new UserFacingError(`Matrix 连接失败：${detail}`);
  }

  private requestSignal(): AbortSignal {
    return AbortSignal.any([this.ctx.signal, this.lifecycle.signal]);
  }

  private async loop(): Promise<void> {
    while (!this.stopped && !this.ctx.signal.aborted) {
      try {
        await this.syncOnce();
        this.backoffIndex = 0;
        this.authFailed = false;
      } catch (error) {
        if (this.stopped || this.ctx.signal.aborted) return;
        if (error instanceof MatrixApiError && error.permanent) {
          if (!this.authFailed) {
            this.authFailed = true;
            this.ctx.onError(new UserFacingError(
              `Matrix 访问令牌已失效（${error.errcode ?? "认证错误"}），请在连接设置中更新凭据。`,
            ));
          }
          return;
        }
        this.report(error);
        const delayMs = RECONNECT_DELAYS_MS[Math.min(this.backoffIndex, RECONNECT_DELAYS_MS.length - 1)]!;
        this.backoffIndex += 1;
        await this.deps.sleep(delayMs, this.requestSignal()).catch(() => {});
      }
    }
  }

  private report(error: unknown): void {
    this.ctx.onError(error instanceof Error ? error : new Error(String(error)));
  }

  private async syncOnce(): Promise<void> {
    const since = this.state.nextBatch;
    const query = new URLSearchParams({ filter: SYNC_FILTER });
    if (since) {
      query.set("since", since);
      query.set("timeout", "30000");
    } else {
      query.set("timeout", "0");
    }
    const data = await this.deps.rest("GET", `/sync?${query.toString()}`, undefined, this.requestSignal());
    await this.handleSync(data, !since);
  }

  private async handleSync(data: Record<string, unknown>, initial: boolean): Promise<void> {
    const directContent = asRecord(data.account_data) ?? {};
    const accountEvents = Array.isArray(directContent.events) ? directContent.events : [];
    for (const raw of accountEvents) {
      const event = asRecord(raw);
      if (event?.type !== "m.direct") continue;
      const roomsByUser = asRecord(event.content) ?? {};
      for (const roomList of Object.values(roomsByUser)) {
        if (!Array.isArray(roomList)) continue;
        for (const roomId of roomList) {
          if (typeof roomId === "string" && roomId) this.dmRooms.add(roomId);
        }
      }
    }
    const rooms = asRecord(data.rooms) ?? {};
    const join = asRecord(rooms.join) ?? {};
    for (const [roomId, roomRaw] of Object.entries(join)) {
      const room = asRecord(roomRaw);
      if (!room || initial) continue;
      const timeline = asRecord(room.timeline) ?? {};
      const events = Array.isArray(timeline.events) ? timeline.events : [];
      for (const raw of events) {
        const event = asRecord(raw);
        if (event) await this.handleEvent(roomId, event);
      }
    }
    const invite = asRecord(rooms.invite) ?? {};
    for (const [roomId, roomRaw] of Object.entries(invite)) {
      const room = asRecord(roomRaw);
      if (room) await this.handleInvite(roomId, room);
    }
    if (!initial) {
      const leave = asRecord(rooms.leave) ?? {};
      for (const roomId of Object.keys(leave)) {
        this.ctx.onGroups([{ chatId: roomId, added: false }]);
      }
    }
    const nextBatch = data.next_batch;
    if (typeof nextBatch === "string" && nextBatch) {
      this.state.nextBatch = nextBatch;
      await this.persistState();
    }
  }

  private async handleInvite(roomId: string, room: Record<string, unknown>): Promise<void> {
    if (this.inviteHandled.has(roomId)) return;
    const now = this.deps.now();
    const retryAt = this.inviteRetryAt.get(roomId);
    if (retryAt !== undefined && now < retryAt) return; // 退避窗口内跳过本批
    const inviteState = asRecord(room.invite_state) ?? {};
    const events = Array.isArray(inviteState.events) ? inviteState.events : [];
    let isDirect = false;
    let title: string | undefined;
    for (const raw of events) {
      const event = asRecord(raw);
      if (!event) continue;
      const content = asRecord(event.content) ?? {};
      if (event.type === "m.room.member" && content.membership === "invite" && content.is_direct === true) isDirect = true;
      if (event.type === "m.room.name" && typeof content.name === "string" && content.name.trim()) {
        title = content.name.trim();
      }
    }
    try {
      await this.deps.rest("POST", `/join/${encodeURIComponent(roomId)}`, {}, this.requestSignal());
    } catch (error) {
      // join 失败绝不向上抛：抛出会中断 sync、next_batch 不落盘，单个坏邀请会
      // 无限阻塞全部摄取。永久失败标记已处理；其余退避后由后续 sync 重试。
      const permanent = error instanceof MatrixApiError && error.permanent;
      this.ctx.log("warn", `Matrix 加入房间 ${roomId} 失败：${error instanceof Error ? error.message : String(error)}`);
      if (permanent) {
        this.inviteHandled.add(roomId);
        this.inviteRetryAt.delete(roomId);
        return;
      }
      this.inviteRetryAt.set(roomId, now + JOIN_RETRY_DELAY_MS);
      return;
    }
    this.inviteHandled.add(roomId);
    this.inviteRetryAt.delete(roomId);
    if (isDirect) this.dmRooms.add(roomId);
    this.ctx.onGroups([{ chatId: roomId, title, added: true }]);
  }

  private async handleEvent(roomId: string, event: Record<string, unknown>): Promise<void> {
    const eventId = typeof event.event_id === "string" ? event.event_id.trim() : "";
    if (eventId && !this.markSeen(eventId)) return;
    if (event.type === "m.room.encrypted") {
      if (!this.warnedEncrypted.has(roomId)) {
        this.warnedEncrypted.add(roomId);
        this.ctx.log("warn", `房间 ${roomId} 的消息已加密，暂不支持 E2EE，已跳过。`);
      }
      return;
    }
    const outcome = projectEvent({
      event,
      roomId,
      botUserId: this.botUserId,
      isDirect: this.dmRooms.has(roomId),
      connectionId: this.ctx.connectionId,
      now: () => this.deps.now(),
    });
    const draft = outcome.message;
    if (!draft) return;
    if (outcome.download) {
      try {
        const settings = await this.ctx.settings();
        const capMb = outcome.download.kind === "image"
          ? Math.min(100, settings.imageMaxDownloadMb || 100)
          : 100;
        const { bytes, mime } = await this.deps.mediaDownload(
          outcome.download.mxc,
          Math.min(capMb * 1024 * 1024, MAX_MEDIA_BYTES),
          this.requestSignal(),
        );
        const path = await this.writeMediaFile(eventId, outcome.download, bytes);
        draft.media = [{ kind: outcome.download.kind, path, mime: mime ?? outcome.download.mime, name: outcome.download.name }];
      } catch (error) {
        this.ctx.log("warn", `Matrix 媒体下载失败：${error instanceof Error ? error.message : String(error)}`);
        draft.media = undefined;
      }
    }
    if (!draft.text && !draft.media?.length) return;
    const replied = draft.repliedEventId;
    draft.trigger.replyToBot = replied ? this.ownIds.has(replied) : false;
    if (replied) {
      const cached = this.recentEvents.get(replied);
      draft.replyTo = { id: replied, text: cached?.text, sender: cached?.sender };
    }
    const message: NormalizedMessage = {
      channel: "matrix",
      connectionId: this.ctx.connectionId,
      kind: draft.kind,
      chatId: draft.chatId,
      threadId: draft.threadId,
      senderId: draft.senderId,
      messageId: draft.messageId,
      timestamp: draft.timestamp,
      text: draft.text,
      media: draft.media,
      replyTo: draft.replyTo,
      trigger: draft.trigger,
    };
    this.cacheRecent(message.messageId, message.senderId, message.text ?? "");
    this.ctx.onMessage(message);
  }

  private async writeMediaFile(
    eventId: string,
    download: { name?: string; mime?: string },
    bytes: Uint8Array,
  ): Promise<string> {
    const rawName = (download.name ?? "file").replaceAll(/[/\\]+/g, "_");
    const safeName = rawName.replaceAll(/[^A-Za-z0-9._-]+/g, "_").slice(-80) || "file";
    const path = join(this.ctx.mediaDir, `${this.deps.now()}-${randomUUID().slice(0, 8)}-${safeName}`);
    await writeFile(path, bytes);
    return path;
  }

  private markSeen(eventId: string): boolean {
    if (this.seenIds.has(eventId)) return false;
    this.seenIds.add(eventId);
    this.seenOrder.push(eventId);
    if (this.seenOrder.length > SEEN_CAP) {
      for (const evicted of this.seenOrder.splice(0, this.seenOrder.length - SEEN_CAP)) {
        this.seenIds.delete(evicted);
      }
    }
    return true;
  }

  private cacheRecent(eventId: string, sender: string, text: string): void {
    this.recentEvents.set(eventId, { sender, text });
    if (this.recentEvents.size > RECENT_CAP) {
      const oldest = this.recentEvents.keys().next().value;
      if (oldest) this.recentEvents.delete(oldest);
    }
  }

  rememberOwnMessage(messageId: string): void {
    if (this.ownIds.has(messageId)) return;
    this.ownIds.add(messageId);
    this.ownOrder.push(messageId);
    if (this.ownOrder.length > OWN_CAP) {
      for (const evicted of this.ownOrder.splice(0, this.ownOrder.length - OWN_CAP)) {
        this.ownIds.delete(evicted);
      }
    }
  }

  isOwnMessage(messageId: string): boolean {
    return this.ownIds.has(messageId);
  }

  private async sendContent(roomId: string, content: Record<string, unknown>): Promise<string | undefined> {
    const path = `/rooms/${encodeURIComponent(roomId)}/send/m.room.message/${encodeURIComponent(this.deps.txnId())}`;
    const result = await this.deps.rest("PUT", path, content, this.requestSignal());
    return typeof result.event_id === "string" ? result.event_id : undefined;
  }

  async sendText(outbound: OutboundText): Promise<SendResult> {
    if (!outbound.text.trim()) return {};
    const chunks = splitFenceAware(outbound.text, this.capabilities.maxChunkChars);
    await this.clearTyping(outbound.chatId);
    let messageId: string | undefined;
    let partial = false;
    for (const [index, chunk] of chunks.entries()) {
      const content: Record<string, unknown> = { ...textContent(chunk) };
      if (index === 0) {
        if (outbound.threadId) {
          content["m.relates_to"] = {
            rel_type: "m.thread",
            event_id: outbound.threadId,
            is_falling_back: true,
            "m.in_reply_to": { event_id: outbound.replyToId ?? outbound.threadId },
          };
        } else if (outbound.replyToId) {
          content["m.relates_to"] = { "m.in_reply_to": { event_id: outbound.replyToId } };
        }
      }
      try {
        const eventId = await this.sendContent(outbound.chatId, content);
        if (eventId) {
          messageId = eventId;
          this.rememberOwnMessage(eventId);
          this.cacheRecent(eventId, this.botUserId, chunk);
        }
      } catch (error) {
        if (!messageId) throw error;
        partial = true;
        break;
      }
    }
    return { messageId, uncertain: partial };
  }

  async sendMedia(outbound: OutboundMedia): Promise<SendResult> {
    const bytes = new Uint8Array(await readFile(outbound.path));
    const extension = basename(outbound.path).toLowerCase().match(/\.[^.]+$/)?.[0] ?? "";
    const mime = outbound.mime || MEDIA_EXTENSIONS[extension] || "application/octet-stream";
    const name = outbound.name || basename(outbound.path);
    const contentUri = await this.deps.mediaUpload(bytes, mime, name, this.requestSignal());
    const content = {
      msgtype: outbound.kind === "image" ? "m.image" : "m.file",
      body: name,
      url: contentUri,
      info: { mimetype: mime, size: bytes.byteLength },
    };
    await this.clearTyping(outbound.chatId);
    const eventId = await this.sendContent(outbound.chatId, content);
    if (eventId) this.rememberOwnMessage(eventId);
    return { messageId: eventId };
  }

  async editMessage(messageId: string, chatId: string, text: string): Promise<void> {
    await this.sendContent(chatId, editContent(text, messageId));
  }

  async streamStart(chatId: string, threadId?: string): Promise<{ streamId: string }> {
    const content: Record<string, unknown> = { ...textContent("正在处理…") };
    if (threadId) {
      content["m.relates_to"] = {
        rel_type: "m.thread",
        event_id: threadId,
        is_falling_back: true,
        "m.in_reply_to": { event_id: threadId },
      };
    }
    const eventId = await this.sendContent(chatId, content);
    if (!eventId) throw new UserFacingError("Matrix 未返回消息 ID，无法开始进度更新。");
    this.rememberOwnMessage(eventId);
    this.cacheRecent(eventId, this.botUserId, "正在处理…");
    this.streams.set(eventId, { chatId, lastText: "" });
    return { streamId: eventId };
  }

  async streamAppend(streamId: string, text: string): Promise<void> {
    const session = this.streams.get(streamId);
    if (!session) throw new UserFacingError("进度流不存在或已结束，无法继续编辑。");
    if (text === session.lastText) return;
    await this.sendContent(session.chatId, editContent(text, streamId));
    session.lastText = text;
  }

  async streamFinish(streamId: string, text: string): Promise<{ messageId?: string }> {
    const session = this.streams.get(streamId);
    if (!session) throw new UserFacingError("进度流不存在或已结束，无法写回最终文本。");
    if (text !== session.lastText) {
      await this.sendContent(session.chatId, editContent(text, streamId));
      session.lastText = text;
    }
    this.streams.delete(streamId);
    await this.clearTyping(session.chatId);
    return { messageId: streamId };
  }

  async sendTyping(chatId: string): Promise<void> {
    await this.putTyping(chatId, true);
    this.scheduleTypingRenewal(chatId);
  }

  private scheduleTypingRenewal(chatId: string): void {
    clearTimeout(this.typingTimers.get(chatId));
    const timer = setTimeout(() => {
      this.typingTimers.delete(chatId);
      void this.putTyping(chatId, true)
        .then(() => this.scheduleTypingRenewal(chatId))
        .catch(() => {});
    }, TYPING_RENEW_MS);
    timer.unref?.();
    this.typingTimers.set(chatId, timer);
  }

  private async clearTyping(chatId: string): Promise<void> {
    clearTimeout(this.typingTimers.get(chatId));
    this.typingTimers.delete(chatId);
    await this.putTyping(chatId, false).catch(() => {});
  }

  private async putTyping(chatId: string, typing: boolean): Promise<void> {
    await this.deps.rest(
      "PUT",
      `/rooms/${encodeURIComponent(chatId)}/typing/${encodeURIComponent(this.botUserId)}`,
      typing ? { typing: true, timeout: TYPING_TIMEOUT_MS } : { typing: false },
      this.requestSignal(),
    );
  }

  async leaveChat(chatId: string): Promise<void> {
    await this.clearTyping(chatId);
    await this.deps.rest("POST", `/leave/${encodeURIComponent(chatId)}`, {}, this.requestSignal());
    this.dmRooms.delete(chatId);
    await this.persistState();
  }

  async isAdmin(chatId: string, senderId: string): Promise<boolean> {
    try {
      const event = await this.deps.rest(
        "GET",
        `/rooms/${encodeURIComponent(chatId)}/state/m.room.power_levels`,
        undefined,
        this.requestSignal(),
      );
      const content = asRecord(event.content) ?? {};
      const users = asRecord(content.users) ?? {};
      const raw = users[senderId];
      const userLevel = typeof raw === "number" ? raw : typeof content.users_default === "number" ? content.users_default : 0;
      return userLevel >= 50;
    } catch {
      return false;
    }
  }

  async testConnection(): Promise<{ ok: boolean; detail?: string }> {
    try {
      const cfg = this.ctx.config;
      const token = (this.ctx.secrets.accessToken ?? "").trim() || this.state.session?.accessToken || "";
      if (!token) {
        // token 缺失但有密码：试一次 /login 验证凭据有效性。
        const password = this.ctx.secrets.password ?? "";
        if (!password) return { ok: false, detail: "尚未配置 Access Token，且无已保存的登录会话或密码。" };
        const login = await this.deps.rest("POST", "/login", {
          type: "m.login.password",
          identifier: { type: "m.id.user", user: cfg.userId },
          password,
          initial_device_display_name: "Paseo IM",
        }, this.requestSignal());
        const loginToken = typeof login.access_token === "string" ? login.access_token : "";
        if (!loginToken) return { ok: false, detail: "Matrix 登录未返回 access_token，请检查用户名与密码。" };
        this.accessToken = loginToken;
      } else {
        this.accessToken = token;
      }
      const whoami = await this.deps.rest("GET", "/account/whoami", undefined, this.requestSignal());
      const userId = typeof whoami.user_id === "string" ? whoami.user_id : "";
      if (!userId) return { ok: false, detail: "whoami 未返回 user_id。" };
      if (userId.toLowerCase() !== cfg.userId.trim().toLowerCase()) {
        return { ok: false, detail: `凭据对应用户 ${userId} 与配置用户 ${cfg.userId} 不一致。` };
      }
      return { ok: true, detail: `已连接 ${userId}` };
    } catch (error) {
      return { ok: false, detail: error instanceof Error ? error.message : String(error) };
    }
  }

  private persistState(): Promise<void> {
    const signature = JSON.stringify([
      this.state.nextBatch,
      [...this.dmRooms].sort(),
      this.seenOrder.length,
      this.ownOrder.length,
    ]);
    if (signature === this.stateSignature) return Promise.resolve();
    return this.stateChain.run(async () => {
      this.stateSignature = signature;
      this.state.dmRooms = [...this.dmRooms];
      this.state.seen = this.seenOrder.slice(-SEEN_CAP);
      this.state.own = this.ownOrder.slice(-OWN_CAP);
      await atomicWrite(this.statePath, this.state);
    });
  }
}

function parseState(value: unknown): MatrixState {
  const record = asRecord(value) ?? {};
  const session = asRecord(record.session);
  const accessToken = typeof session?.accessToken === "string" ? session.accessToken : undefined;
  return {
    version: 1,
    nextBatch: typeof record.nextBatch === "string" ? record.nextBatch : undefined,
    dmRooms: Array.isArray(record.dmRooms) ? record.dmRooms.filter((id): id is string => typeof id === "string") : [],
    seen: Array.isArray(record.seen) ? record.seen.filter((id): id is string => typeof id === "string") : [],
    own: Array.isArray(record.own) ? record.own.filter((id): id is string => typeof id === "string") : [],
    session: accessToken ? {
      accessToken,
      deviceId: typeof session?.deviceId === "string" ? session.deviceId : undefined,
      userId: typeof session?.userId === "string" ? session.userId : undefined,
    } : undefined,
  };
}
