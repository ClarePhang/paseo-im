/** Discord channel adapter: Gateway v10 over a bare WebSocket plus REST v10.
 * Protocol facts ported from xmanrui/dsh-im (MIT). */
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { basename, join } from "node:path";
import type { Connection } from "../../../shared/config.ts";
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
import { atomicWrite, readJson, WriteChain } from "../../files.ts";
import { defaultConfigure, requireFields } from "../configure.ts";
import type { AdapterContext, ChannelAdapter, ChannelDefinition, ConfiguredFields } from "../types.ts";
import { createRestClient } from "./rest.ts";
import {
  CHANNEL_CACHE_CAP,
  CONNECT_TIMEOUT_MS,
  DISCORD_INTENTS,
  DISCORD_MAX_CHUNK_CHARS,
  DISCORD_TOKEN_PATTERN,
  MEDIA_PER_MESSAGE_CAP,
  RECONNECT_DELAYS_MS,
  SENT_CACHE_CAP,
  SEEN_CAP,
  chunkText,
  describeError,
  fieldOf,
  gatewayCloseError,
  isTextChannelType,
  isThreadChannelType,
  messageTimestamp,
  safeFileName,
  senderName,
  socketEventData,
  str,
  stripBotMention,
  threadName,
  toRestStartError,
  type DiscordAttachment,
  type DiscordChannel,
  type DiscordMessagePayload,
  type DiscordMessageReference,
  type DiscordUser,
  type GatewayPacket,
  type GatewaySocket,
} from "./protocol.ts";

export * from "./protocol.ts";
export { createRestClient, type RestCall } from "./rest.ts";

export type DiscordConnection = Extract<Connection, { channel: "discord" }>;

/** Transport surface injectable for tests; defaults hit the real Discord API. */
export interface DiscordDeps {
  rest(method: string, path: string, body?: unknown): Promise<unknown>;
  openSocket(url: string): GatewaySocket | Promise<GatewaySocket>;
  download(url: string): Promise<Uint8Array>;
  random(): number;
}

export const discordCapabilities: ChannelCapabilities = {
  progress: "edit",
  markdown: "native",
  uploadImage: true,
  uploadFile: true,
  reactions: true,
  typing: true,
  maxChunkChars: DISCORD_MAX_CHUNK_CHARS,
  threads: true,
};

interface DiscordStateFile {
  version: 1;
  seen: string[];
}

interface Deferred {
  promise: Promise<void>;
  resolve: () => void;
  reject: (reason?: unknown) => void;
}

export function defaultDeps(ctx: AdapterContext<DiscordConnection>): DiscordDeps {
  const token = (ctx.secrets.token ?? "").trim();
  return {
    rest: createRestClient({ token, signal: ctx.signal }),
    openSocket: (url) => new WebSocket(url),
    download: async (url) => {
      const response = await fetch(url, { signal: ctx.signal });
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      return new Uint8Array(await response.arrayBuffer());
    },
    random: Math.random,
  };
}

export function createAdapter(
  ctx: AdapterContext<DiscordConnection>,
  deps: DiscordDeps = defaultDeps(ctx),
): ChannelAdapter {
  return new DiscordAdapter(ctx, deps);
}

export const definition: ChannelDefinition<DiscordConnection> = {
  channel: "discord",
  create: (ctx) => createAdapter(ctx),
  configure(fields: Record<string, string>): ConfiguredFields {
    requireFields(CHANNEL_SPECS.discord.fields, fields);
    return defaultConfigure("discord", fields);
  },
};

export class DiscordAdapter implements ChannelAdapter {
  readonly channel = "discord" as const;
  readonly capabilities = discordCapabilities;
  readonly remembersOwnMessages = true;

  private readonly ctx: AdapterContext<DiscordConnection>;
  private readonly deps: DiscordDeps;
  private readonly token: string;
  private readonly statePath: string;
  private readonly stateChain = new WriteChain();
  private readonly channels = new Map<string, DiscordChannel>();
  private readonly sentChannels = new Map<string, string>();
  private readonly sentOrder: string[] = [];
  private readonly seen = new Set<string>();
  private stopped = true;
  private generation = 0;
  private attempt = 0;
  private seq: number | null = null;
  private socket: GatewaySocket | null = null;
  private heartbeatTimer: NodeJS.Timeout | undefined;
  private heartbeatAcked = true;
  private reconnectTimer: NodeJS.Timeout | undefined;
  private ready: Deferred | null = null;
  private failureHandledFor = -1;
  private abortListener: (() => void) | null = null;

  constructor(ctx: AdapterContext<DiscordConnection>, deps: DiscordDeps) {
    this.ctx = ctx;
    this.deps = deps;
    this.token = (ctx.secrets.token ?? "").trim();
    this.statePath = join(ctx.dataDir, "discord-state.json");
  }

  // ---------------------------------------------------------------- lifecycle

  async start(): Promise<void> {
    if (this.ctx.signal.aborted) return;
    this.assertTokenConfigured();
    await this.loadState();
    if (!this.stopped) return; // Already running (or retrying in background).
    this.stopped = false;
    this.attempt = 0;
    if (this.abortListener === null) {
      this.abortListener = () => {
        void this.stop();
      };
      this.ctx.signal.addEventListener("abort", this.abortListener, { once: true });
    }
    try {
      await this.establish();
      this.attempt = 0;
    } catch (error) {
      if (this.stopped) return;
      this.disposeSocket();
      if (error instanceof UserFacingError) {
        this.stopped = true; // Allow a later start() retry after the config is fixed.
        throw error;
      }
      // Transient: surface and keep retrying with backoff until ctx.signal aborts.
      this.handleConnectionFailure(error as Error);
    }
  }

  async stop(): Promise<void> {
    this.stopped = true;
    this.generation += 1;
    if (this.abortListener !== null) {
      this.ctx.signal.removeEventListener("abort", this.abortListener);
      this.abortListener = null;
    }
    if (this.reconnectTimer !== undefined) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = undefined;
    }
    this.clearHeartbeat();
    const pending = this.ready;
    this.ready = null;
    if (pending) pending.reject(new Error("Discord 适配器已停止。"));
    const socket = this.socket;
    this.socket = null;
    try {
      socket?.close(1000, "paseo-im 停止");
    } catch {
      // Socket already gone.
    }
    await this.stateChain.run(async () => {});
  }

  private assertTokenConfigured(): void {
    if (this.token === "") {
      throw new UserFacingError("缺少 Discord Bot Token，请在连接设置的 Token 字段填写后重试。");
    }
    if (!DISCORD_TOKEN_PATTERN.test(this.token)) {
      throw new UserFacingError(
        "Discord Bot Token 格式不正确，请在 Discord Developer Portal → Bot 页面重新复制完整 Token 后填写。",
      );
    }
  }

  /** One full connect cycle: identity check, gateway URL, then the WebSocket. */
  private async establish(): Promise<void> {
    const me = (await this.callRest("GET", "/users/@me")) as DiscordUser | undefined;
    const meId = str(me?.id);
    if (meId === "") {
      throw new UserFacingError("Discord 未返回机器人身份信息，请检查 Token 后重试。");
    }
    if (meId !== this.ctx.config.botUserId) {
      throw new UserFacingError(
        `Discord Token 对应的机器人 ID（${meId}）与连接配置中的 ID（${this.ctx.config.botUserId}）不一致，请核对后重新保存连接。`,
      );
    }
    const gateway = await this.callRest("GET", "/gateway/bot");
    const url = str(fieldOf(gateway, "url"));
    if (url === "") {
      throw new UserFacingError("Discord 未返回可用的 Gateway 地址，请稍后重试或检查 Discord 服务状态。");
    }
    await this.connect(url);
  }

  private async connect(url: string): Promise<void> {
    if (this.stopped) throw new Error("Discord 适配器已停止。");
    const generation = ++this.generation;
    const socket = await this.deps.openSocket(url);
    if (this.stopped || generation !== this.generation) {
      try {
        socket.close(1000, "stale connect");
      } catch {
        // Ignore.
      }
      throw new Error("Discord 适配器已停止。");
    }
    this.socket = socket;
    this.heartbeatAcked = true;
    const ready: Deferred = Promise.withResolvers<void>();
    this.ready = ready;
    let identified = false;

    socket.addEventListener("message", (event) => {
      if (this.stopped || generation !== this.generation) return;
      const data = socketEventData(event);
      if (data === null) return;
      let packet: GatewayPacket;
      try {
        packet = JSON.parse(data) as GatewayPacket;
      } catch {
        this.ctx.log("warn", "Discord Gateway 收到无法解析的消息帧，已忽略。");
        return;
      }
      if (Number.isSafeInteger(packet.s)) this.seq = packet.s as number;
      if (packet.op === 10) {
        this.startHeartbeat(socket, fieldOf(packet.d, "heartbeat_interval"), generation);
        if (!identified) {
          identified = true;
          this.sendGateway(socket, {
            op: 2,
            d: {
              token: this.token,
              intents: DISCORD_INTENTS,
              properties: { os: "linux", browser: "paseo-im", device: "paseo-im" },
            },
          });
        }
        return;
      }
      if (packet.op === 11) {
        this.heartbeatAcked = true;
        return;
      }
      if (packet.op === 1) {
        this.sendGateway(socket, { op: 1, d: this.seq });
        return;
      }
      if (packet.op === 7 || packet.op === 9) {
        try {
          socket.close(4000, packet.op === 7 ? "reconnect requested" : "invalid session");
        } catch {
          // Ignore.
        }
        return;
      }
      if (packet.op !== 0) return;
      if (packet.t === "READY") {
        ready.resolve();
        return;
      }
      if (packet.t === "GUILD_CREATE") {
        const channels = fieldOf(packet.d, "channels");
        const threads = fieldOf(packet.d, "threads");
        const list = [
          ...(Array.isArray(channels) ? (channels as DiscordChannel[]) : []),
          ...(Array.isArray(threads) ? (threads as DiscordChannel[]) : []),
        ];
        for (const channel of list) this.cacheChannel(channel);
        return;
      }
      if (
        packet.t === "CHANNEL_CREATE" ||
        packet.t === "CHANNEL_UPDATE" ||
        packet.t === "THREAD_CREATE" ||
        packet.t === "THREAD_UPDATE"
      ) {
        this.cacheChannel(packet.d as DiscordChannel);
        return;
      }
      if (packet.t === "CHANNEL_DELETE" || packet.t === "THREAD_DELETE") {
        const id = str(fieldOf(packet.d, "id"));
        if (id !== "") this.channels.delete(id);
        return;
      }
      if (packet.t === "MESSAGE_CREATE") {
        void this.handleMessage(packet.d).catch((error) => {
          this.ctx.log("error", `Discord 消息处理失败：${describeError(error)}`);
        });
      }
      // MESSAGE_UPDATE and other dispatches are intentionally ignored: edits
      // must not re-trigger the engine.
    });

    socket.addEventListener("close", (event) => {
      if (this.stopped || generation !== this.generation) return;
      this.clearHeartbeat();
      if (this.socket === socket) this.socket = null;
      const error = gatewayCloseError(Number(event?.code) || 0);
      const pending = this.ready;
      this.ready = null;
      if (pending) {
        // Whoever awaited connect() reports and schedules.
        pending.reject(error);
        return;
      }
      this.handleConnectionFailure(error);
    });

    socket.addEventListener("error", (event) => {
      if (this.stopped || generation !== this.generation) return;
      this.ctx.log("warn", `Discord Gateway 连接错误：${describeError(event?.error)}`);
    });

    const timeoutTimer = setTimeout(() => {
      ready.reject(
        new Error(`Discord Gateway 连接超时（${CONNECT_TIMEOUT_MS / 1000} 秒未收到 READY），请检查网络后重试。`),
      );
    }, CONNECT_TIMEOUT_MS);
    try {
      await ready.promise;
    } finally {
      clearTimeout(timeoutTimer);
      if (this.ready === ready) this.ready = null;
    }
  }

  private handleConnectionFailure(error: Error): void {
    if (this.stopped || this.failureHandledFor === this.generation) return;
    this.failureHandledFor = this.generation;
    this.ctx.onError(error);
    if (error instanceof UserFacingError) return; // Permanent config problem; no auto retry.
    this.scheduleReconnect();
  }

  private disposeSocket(): void {
    this.clearHeartbeat();
    const socket = this.socket;
    this.socket = null;
    try {
      socket?.close(4000, "paseo-im reconnect");
    } catch {
      // Ignore.
    }
  }

  private scheduleReconnect(): void {
    if (this.stopped || this.reconnectTimer !== undefined) return;
    const delay = RECONNECT_DELAYS_MS[Math.min(this.attempt, RECONNECT_DELAYS_MS.length - 1)];
    this.attempt += 1;
    const timer = setTimeout(() => {
      this.reconnectTimer = undefined;
      void this.establishOnce();
    }, delay);
    this.reconnectTimer = timer;
    timer.unref?.();
  }

  private async establishOnce(): Promise<void> {
    try {
      await this.establish();
      this.attempt = 0;
    } catch (error) {
      if (this.stopped) return;
      if (error instanceof UserFacingError) {
        this.ctx.onError(error);
        return;
      }
      this.disposeSocket();
      this.handleConnectionFailure(error as Error);
    }
  }

  private startHeartbeat(socket: GatewaySocket, rawInterval: unknown, generation: number): void {
    this.clearHeartbeat();
    const interval = Number(rawInterval);
    if (!Number.isFinite(interval) || interval < 1000) {
      try {
        socket.close(4000, "invalid heartbeat interval");
      } catch {
        // Ignore.
      }
      return;
    }
    this.heartbeatAcked = true;
    const jitter = Math.floor(interval * this.deps.random());
    const tick = (): void => {
      if (this.stopped || generation !== this.generation || this.socket !== socket) return;
      if (!this.heartbeatAcked) {
        try {
          socket.close(4000, "heartbeat not acknowledged");
        } catch {
          // Ignore.
        }
        return;
      }
      this.heartbeatAcked = false;
      this.sendGateway(socket, { op: 1, d: this.seq });
      const timer = setTimeout(tick, interval);
      timer.unref?.();
      this.heartbeatTimer = timer;
    };
    const timer = setTimeout(tick, jitter);
    timer.unref?.();
    this.heartbeatTimer = timer;
  }

  private clearHeartbeat(): void {
    clearTimeout(this.heartbeatTimer);
    this.heartbeatTimer = undefined;
    this.heartbeatAcked = true;
  }

  private sendGateway(socket: GatewaySocket, payload: unknown): void {
    if (socket.readyState !== 1) return;
    try {
      socket.send(JSON.stringify(payload));
    } catch (error) {
      this.ctx.log("warn", `Discord Gateway 发送失败：${describeError(error)}`);
    }
  }

  // ------------------------------------------------------------------ inbound

  private async handleMessage(raw: unknown): Promise<void> {
    const message = raw as DiscordMessagePayload;
    const messageId = str(message?.id);
    const channelId = str(message?.channel_id);
    const authorId = str(message?.author?.id);
    if (messageId === "" || channelId === "" || authorId === "") return;
    if (this.seen.has(messageId)) return;
    if (message?.author?.bot === true) return;
    if (Number(message?.type) === 21) return; // No conversation content (ported from dsh-im).
    const content = typeof message?.content === "string" ? message.content : "";
    const attachments = Array.isArray(message?.attachments)
      ? (message.attachments as DiscordAttachment[])
      : [];
    const stickers = Array.isArray(message?.sticker_items) ? message.sticker_items.length : 0;
    if (content.trim() === "" && attachments.length === 0 && stickers === 0) return;

    if (this.seen.has(messageId)) return;
    this.seen.add(messageId);
    this.trimSeen();

    const botId = this.ctx.config.botUserId;
    const mention =
      Array.isArray(message?.mentions) &&
      (message.mentions as DiscordUser[]).some((user) => str(user?.id) === botId);
    const reference = message?.message_reference ?? null;
    const referenced = (message?.referenced_message ?? null) as DiscordMessagePayload | null;
    const replyToBot = Number(reference?.type) === 0 && str(referenced?.author?.id) === botId;

    const media = await this.downloadAttachments(attachments);

    let kind: "dm" | "group";
    let chatId = channelId;
    let threadId: string | undefined;
    let managed = false;
    if (!message?.guild_id) {
      // No guild: DM or group DM (channel.type 1 / dm_recipients).
      kind = "dm";
    } else {
      const channel = await this.lookupChannel(channelId);
      const type = Number(channel?.type);
      if (channel === undefined) {
        kind = "group"; // Lookup failed: degrade to a plain guild channel.
      } else if (isThreadChannelType(type)) {
        kind = "group";
        threadId = channelId;
        chatId = str(channel?.parent_id) || channelId;
        managed = str(channel?.owner_id) === botId;
      } else if (isTextChannelType(type)) {
        kind = "group";
        if (mention || replyToBot) {
          // First addressed message in a text channel starts a native thread.
          threadId = await this.createThread(channelId, messageId, content, botId);
        }
      } else {
        return; // Voice/stage/forum/category channels carry no chat for us.
      }
    }

    const text = stripBotMention(content, botId);
    const replyTo = this.replySnapshot(referenced, reference);
    const normalized: NormalizedMessage = {
      channel: "discord",
      connectionId: this.ctx.connectionId,
      kind,
      chatId,
      ...(threadId !== undefined ? { threadId } : {}),
      senderId: authorId,
      senderName: senderName(message),
      messageId,
      timestamp: messageTimestamp(message),
      ...(text !== "" ? { text } : {}),
      ...(media.length > 0 ? { media } : {}),
      ...(replyTo !== undefined ? { replyTo } : {}),
      trigger: { mention: mention || managed, replyToBot, command: false },
    };

    await this.persistState();
    this.ctx.onMessage(normalized);
  }

  private async lookupChannel(channelId: string): Promise<DiscordChannel | undefined> {
    const cached = this.channels.get(channelId);
    if (cached !== undefined) return cached;
    let channel: DiscordChannel | undefined;
    try {
      channel = (await this.callRest("GET", `/channels/${channelId}`)) as DiscordChannel | undefined;
    } catch (error) {
      this.ctx.log("warn", `Discord 频道信息获取失败：${describeError(error)}`);
      return undefined;
    }
    this.cacheChannel(channel);
    return channel;
  }

  private cacheChannel(channel: DiscordChannel | undefined | null): void {
    const id = str(channel?.id);
    if (id === "") return;
    this.channels.set(id, channel as DiscordChannel);
    while (this.channels.size > CHANNEL_CACHE_CAP) {
      const oldest = this.channels.keys().next().value;
      if (oldest === undefined) break;
      this.channels.delete(oldest);
    }
  }

  private async createThread(
    channelId: string,
    messageId: string,
    content: string,
    botId: string,
  ): Promise<string | undefined> {
    try {
      const created = (await this.callRest(
        "POST",
        `/channels/${channelId}/messages/${messageId}/threads`,
        { name: threadName(content, botId), auto_archive_duration: 1440 },
      )) as DiscordChannel | undefined;
      const id = str(created?.id);
      if (id === "") throw new Error("Discord 未返回线程 ID。");
      this.cacheChannel({
        ...(created as DiscordChannel),
        id,
        parent_id: str(created?.parent_id) || channelId,
        owner_id: str(created?.owner_id) || botId,
      });
      return id;
    } catch (error) {
      this.ctx.log("warn", `Discord 创建线程失败，将直接在频道内回复：${describeError(error)}`);
      return undefined;
    }
  }

  private replySnapshot(
    referenced: DiscordMessagePayload | null,
    reference: DiscordMessageReference | null,
  ): NormalizedMessage["replyTo"] {
    const refId = str(reference?.message_id) || str(referenced?.id);
    if (refId === "") return undefined;
    const reply: { id: string; text?: string; sender?: string } = { id: refId };
    const text = typeof referenced?.content === "string" ? referenced.content : "";
    if (text.trim() !== "") reply.text = text;
    const sender = str(referenced?.author?.id);
    if (sender !== "") reply.sender = sender;
    return reply;
  }

  private async downloadAttachments(attachments: DiscordAttachment[]): Promise<NormalizedMedia[]> {
    const usable = attachments.filter(
      (attachment) => typeof attachment?.url === "string" && (attachment.url as string) !== "",
    );
    if (usable.length === 0) return [];
    await mkdir(this.ctx.mediaDir, { recursive: true });
    const media: NormalizedMedia[] = [];
    for (const attachment of usable.slice(0, MEDIA_PER_MESSAGE_CAP)) {
      const url = attachment.url as string;
      const mime =
        typeof attachment.content_type === "string"
          ? (attachment.content_type.split(";", 1)[0] ?? "").trim().toLowerCase()
          : "";
      const kind: "image" | "file" = mime.startsWith("image/") ? "image" : "file";
      const name = safeFileName(attachment.filename) || "file";
      const target = join(this.ctx.mediaDir, `${str(attachment.id) || String(media.length)}-${name}`);
      try {
        const bytes = await this.deps.download(url);
        await writeFile(target, bytes);
        media.push({ kind, path: target, ...(mime !== "" ? { mime } : {}), name });
      } catch (error) {
        this.ctx.log("warn", `Discord 附件下载失败（${name}）：${describeError(error)}`);
      }
    }
    return media;
  }

  // ----------------------------------------------------------------- outbound

  async sendText(outbound: OutboundText): Promise<SendResult> {
    const text = outbound.text;
    if (typeof text !== "string" || text.trim() === "") return {};
    const channelId = this.resolveTarget(outbound.chatId, outbound.threadId);
    const chunks = chunkText(text, this.capabilities.maxChunkChars);
    let messageId: string | undefined;
    for (let index = 0; index < chunks.length; index += 1) {
      const body: Record<string, unknown> = {
        content: chunks[index],
        allowed_mentions: { parse: [] },
      };
      if (index === 0 && typeof outbound.replyToId === "string" && outbound.replyToId !== "") {
        body.message_reference = { message_id: outbound.replyToId, fail_if_not_exists: false };
      }
      const result = await this.callRest("POST", `/channels/${channelId}/messages`, body);
      const id = str(fieldOf(result, "id"));
      if (id !== "") {
        messageId = id;
        this.rememberSent(id, channelId);
      }
    }
    return messageId !== undefined ? { messageId } : {};
  }

  async sendMedia(outbound: OutboundMedia): Promise<SendResult> {
    const channelId = this.resolveTarget(outbound.chatId, outbound.threadId);
    const bytes = await readFile(outbound.path);
    const name =
      typeof outbound.name === "string" && outbound.name.trim() !== ""
        ? outbound.name.trim()
        : basename(outbound.path) || "file";
    const payload: Record<string, unknown> = {
      attachments: [{ id: 0, filename: name }],
      allowed_mentions: { parse: [] },
    };
    if (typeof outbound.caption === "string" && outbound.caption.trim() !== "") {
      payload.content = outbound.caption.slice(0, this.capabilities.maxChunkChars);
    }
    if (typeof outbound.replyToId === "string" && outbound.replyToId !== "") {
      payload.message_reference = { message_id: outbound.replyToId, fail_if_not_exists: false };
    }
    const form = new FormData();
    form.append(
      "file",
      outbound.mime !== undefined && outbound.mime !== ""
        ? new File([bytes], name, { type: outbound.mime })
        : new File([bytes], name),
      name,
    );
    form.append("payload_json", JSON.stringify(payload));
    const result = await this.callRest("POST", `/channels/${channelId}/messages`, form);
    const id = str(fieldOf(result, "id"));
    if (id !== "") this.rememberSent(id, channelId);
    return id !== "" ? { messageId: id } : {};
  }

  async editMessage(messageId: string, chatId: string, text: string): Promise<void> {
    const channelId = this.resolveEditChannel(messageId, chatId);
    await this.callRest("PATCH", `/channels/${channelId}/messages/${messageId}`, { content: text });
  }

  async sendTyping(chatId: string): Promise<void> {
    await this.callRest("POST", `/channels/${chatId}/typing`, undefined);
  }

  async addReaction(messageId: string, chatId: string, emoji: string): Promise<void> {
    const channelId = this.resolveEditChannel(messageId, chatId);
    await this.callRest(
      "PUT",
      `/channels/${channelId}/messages/${messageId}/reactions/${encodeURIComponent(emoji)}/@me`,
      undefined,
    );
  }

  async leaveChat(chatId: string): Promise<void> {
    const channel = await this.lookupChannel(chatId);
    const guildId = str(channel?.guild_id);
    if (guildId === "") return; // DM / group DM: nothing to leave.
    await this.callRest("DELETE", `/users/@me/guilds/${guildId}`);
  }

  async testConnection(): Promise<{ ok: boolean; detail?: string }> {
    try {
      const me = (await this.callRest("GET", "/users/@me")) as DiscordUser | undefined;
      const id = str(me?.id);
      if (id === "") return { ok: false, detail: "Discord 未返回机器人身份信息。" };
      if (id !== this.ctx.config.botUserId) {
        return {
          ok: false,
          detail: `Token 对应的机器人 ID（${id}）与连接配置（${this.ctx.config.botUserId}）不一致。`,
        };
      }
      await this.callRest("GET", "/gateway/bot");
      const username = str(me?.username);
      return { ok: true, detail: `Discord 连接正常${username === "" ? "。" : `（@${username}）。`}` };
    } catch (error) {
      return {
        ok: false,
        detail: error instanceof UserFacingError ? error.message : describeError(error),
      };
    }
  }

  isOwnMessage(messageId: string): boolean {
    return this.sentChannels.has(messageId);
  }

  rememberOwnMessage(messageId: string): void {
    this.rememberSent(messageId, "");
  }

  private resolveTarget(chatId: string, threadId?: string): string {
    const id = typeof threadId === "string" && threadId !== "" ? threadId : chatId;
    if (typeof id !== "string" || id === "") {
      throw new UserFacingError("Discord 发送目标缺失，无法发送消息。");
    }
    return id;
  }

  private resolveEditChannel(messageId: string, chatId: string): string {
    const mapped = this.sentChannels.get(messageId);
    return mapped !== undefined && mapped !== "" ? mapped : chatId;
  }

  private rememberSent(messageId: string, channelId: string): void {
    if (this.sentChannels.has(messageId)) return;
    this.sentChannels.set(messageId, channelId);
    this.sentOrder.push(messageId);
    while (this.sentOrder.length > SENT_CACHE_CAP) {
      const oldest = this.sentOrder.shift();
      if (oldest !== undefined) this.sentChannels.delete(oldest);
    }
  }

  private async callRest(method: string, path: string, body?: unknown): Promise<unknown> {
    try {
      return await this.deps.rest(method, path, body);
    } catch (error) {
      throw toRestStartError(error);
    }
  }

  // -------------------------------------------------------------------- state

  private trimSeen(): void {
    while (this.seen.size > SEEN_CAP) {
      const oldest = this.seen.values().next().value;
      if (oldest === undefined) break;
      this.seen.delete(oldest);
    }
  }

  private async loadState(): Promise<void> {
    const loaded = await readJson<DiscordStateFile>(this.statePath, (value) => {
      if (value === null || typeof value !== "object") {
        throw new Error("状态文件格式不正确");
      }
      return value as DiscordStateFile;
    });
    const seen = Array.isArray(loaded?.seen) ? loaded.seen : [];
    for (const id of seen) {
      if (typeof id === "string" && id !== "") this.seen.add(id);
    }
    this.trimSeen();
  }

  private async persistState(): Promise<void> {
    const snapshot: DiscordStateFile = { version: 1, seen: [...this.seen].slice(-SEEN_CAP) };
    await this.stateChain.run(() => atomicWrite(this.statePath, snapshot));
  }
}
