/** WhatsApp channel adapter on top of @whiskeysockets/baileys.
 * Protocol facts ported from xmanrui/dsh-im (MIT). */

import { chmod, mkdir, readFile, readdir, stat, writeFile } from "node:fs/promises";
import { basename, join } from "node:path";
import { randomUUID } from "node:crypto";
import qrcode from "qrcode";
import { baileys, baileysSync, type Baileys } from "./baileys.ts";
import type {
  AnyMessageContent,
  BaileysEventMap,
  AuthenticationState,
  ConnectionState,
  MediaDownloadOptions,
  MiscMessageGenerationOptions,
  UserFacingSocketConfig,
  WAMessage,
  WAMessageKey,
  WAPresence,
} from "@whiskeysockets/baileys";
import type { ILogger } from "@whiskeysockets/baileys/lib/Utils/logger.js";
import { sleep, WriteChain } from "../../files.ts";
import { splitChunks } from "../../format/chunks.ts";
import type { Connection } from "../../../shared/config.ts";
import { UserFacingError } from "../../../shared/errors.ts";
import type { ChannelCapabilities, NormalizedMedia, NormalizedMessage, OutboundMedia, OutboundText, SendResult } from "../../../shared/model.ts";
import { AdapterStartError, type AdapterContext, type ChannelAdapter } from "../types.ts";
import { markdownToWhatsapp } from "./markdown.ts";
import { messageTimestampMs, normalizeWhatsappMessage, type AccountJids, type PendingMedia } from "./normalize.ts";
import { WhatsappStateStore, type MessageKeyRef } from "./state.ts";

/** WhatsApp hard limit for a single text message. */
const MAX_CHUNK_CHARS = 4_000;
/** 统一媒体上限：入站文档下载与出站 sendMedia 都按 100MB 前置拒绝，避免
 * 无界文件读入内存。 */
const MAX_MEDIA_BYTES = 100 * 1024 * 1024;
/** messages.upsert type "append" only replays history this recently. */
const APPEND_GRACE_MS = 60_000;
const BACKOFF_SECONDS = [1, 3, 5, 10, 30];
/** composing presence expires fast on WhatsApp; renew while a task runs. */
const TYPING_RENEW_MS = 20_000;
const TYPING_TOTAL_MS = 120_000;
const SILENT_LOGGER: ILogger = {
  level: "silent",
  child: () => SILENT_LOGGER,
  trace: () => {},
  debug: () => {},
  info: () => {},
  warn: () => {},
  error: () => {},
};

/** Socket configuration derived from the dsh-im runtime: suppress history
 * sync noise, never mark the account online, keep link previews cheap. */
export function socketConfigPreset(b: Baileys, auth: AuthenticationState, version?: readonly [number, number, number]): UserFacingSocketConfig {
  const config: UserFacingSocketConfig = {
    auth,
    browser: b.Browsers.macOS("Paseo IM"),
    logger: SILENT_LOGGER,
    markOnlineOnConnect: false,
    syncFullHistory: false,
    shouldSyncHistoryMessage: () => false,
    getMessage: async () => undefined,
    generateHighQualityLinkPreview: false,
  };
  if (version) config.version = [...version];
  return config;
}

export const AUTH_DIR_NAME = "whatsapp-auth";

/** Minimal structural surface of a Baileys socket; the fake transport in
 * tests implements exactly this. */
export interface WhatsappSocketUser {
  id?: string;
  name?: string;
  lid?: string;
  phoneNumber?: string;
}

export interface WhatsappSocket {
  user?: WhatsappSocketUser | undefined;
  ev: {
    on(type: string, handler: (payload: unknown) => void): void;
  };
  sendMessage(jid: string, content: AnyMessageContent, options?: MiscMessageGenerationOptions): Promise<WAMessage | undefined>;
  readMessages(keys: WAMessageKey[]): Promise<void>;
  sendPresenceUpdate(type: WAPresence, toJid?: string): Promise<void>;
  updateMediaMessage?(message: WAMessage): Promise<WAMessage>;
  end(error?: Error): Promise<void> | void;
  logout(reason?: string): Promise<void> | void;
}

export interface WhatsappDeps {
  createSocket(config: UserFacingSocketConfig): WhatsappSocket;
  loadAuthState(folder: string): Promise<{ state: AuthenticationState; saveCreds: () => Promise<void> }>;
  download(message: WAMessage, options?: MediaDownloadOptions): Promise<Buffer>;
  makeQr(text: string): Promise<string>;
  fetchVersion(): Promise<readonly [number, number, number] | undefined>;
}

function wrapBaileysSocket(socket: ReturnType<Baileys["makeWASocket"]>): WhatsappSocket {
  return {
    get user() {
      return socket.user;
    },
    ev: {
      on: (type, handler) => {
        socket.ev.on(
          type as keyof BaileysEventMap,
          handler as unknown as (arg: BaileysEventMap[keyof BaileysEventMap]) => void,
        );
      },
    },
    sendMessage: (jid, content, options) => socket.sendMessage(jid, content, options),
    readMessages: (keys) => socket.readMessages(keys),
    sendPresenceUpdate: (type, toJid) => socket.sendPresenceUpdate(type, toJid),
    updateMediaMessage: (message) => socket.updateMediaMessage(message),
    end: (error) => socket.end(error),
    logout: (reason) => socket.logout(reason),
  };
}

async function withTimeout<T>(promise: Promise<T>, ms: number): Promise<T | undefined> {
  const { promise: deadline, resolve } = Promise.withResolvers<undefined>();
  const timer = setTimeout(() => resolve(undefined), ms);
  try {
    return await Promise.race([promise, deadline]);
  } catch {
    return undefined;
  } finally {
    clearTimeout(timer);
  }
}

export async function makeDefaultDeps(): Promise<WhatsappDeps> {
  const b = await baileys();
  return {
    createSocket: (config) => wrapBaileysSocket(b.makeWASocket(config)),
    loadAuthState: (folder) => b.useMultiFileAuthState(folder),
    download: (message, options) => b.downloadMediaMessage(message, "buffer", options ?? {}),
    makeQr: (text) => qrcode.toDataURL(text),
    fetchVersion: async () => {
      const result = await withTimeout(b.fetchLatestBaileysVersion(), 5_000);
      return result?.version;
    },
  };
}

/** 0700 directory / 0600 files so linked-device keys are not world readable. */
export async function hardenAuthDirectory(path: string): Promise<void> {
  await mkdir(path, { recursive: true, mode: 0o700 });
  await chmod(path, 0o700).catch(() => {});
  const entries = await readdir(path, { withFileTypes: true }).catch(() => []);
  await Promise.all(
    entries
      .filter((entry) => entry.isFile())
      .map((entry) => chmod(join(path, entry.name), 0o600).catch(() => {})),
  );
}

/** Boom errors carry the disconnect status in output.statusCode. */
export function disconnectStatus(error: unknown): number | null {
  const candidate = error as
    | { output?: { statusCode?: number }; data?: { statusCode?: number }; statusCode?: number }
    | undefined;
  return candidate?.output?.statusCode ?? candidate?.data?.statusCode ?? candidate?.statusCode ?? null;
}

const MEDIA_EXTENSIONS: Record<string, string> = {
  "image/jpeg": "jpg",
  "image/png": "png",
  "image/webp": "webp",
  "image/gif": "gif",
  "application/pdf": "pdf",
};

function mediaExtension(mimetype: string | undefined, fallback: string): string {
  if (mimetype && MEDIA_EXTENSIONS[mimetype]) return MEDIA_EXTENSIONS[mimetype];
  if (mimetype && mimetype.startsWith("image/")) return mimetype.slice("image/".length).replace(/[^a-z0-9]/g, "") || fallback;
  return fallback;
}

function sanitizeFileName(name: string): string {
  const base = basename(name).replace(/[/\\:*?"<>|\x00-\x1f]/g, "_").trim();
  return base.slice(0, 120);
}

export const whatsappCapabilities: ChannelCapabilities = {
  progress: "edit",
  markdown: "custom",
  uploadImage: true,
  uploadFile: true,
  reactions: false,
  typing: true,
  maxChunkChars: MAX_CHUNK_CHARS,
  threads: false,
};

export type WhatsappConnection = Extract<Connection, { channel: "whatsapp" }>;

/** ChannelAdapter factory with injectable transport for tests. */
export function createAdapter(ctx: AdapterContext<WhatsappConnection>, deps?: WhatsappDeps): ChannelAdapter {
  return new WhatsappAdapter(ctx, deps);
}

export class WhatsappAdapter implements ChannelAdapter {
  readonly channel = "whatsapp" as const;
  readonly capabilities = whatsappCapabilities;
  readonly remembersOwnMessages = true;

  private readonly ctx: AdapterContext<WhatsappConnection>;
  private deps!: WhatsappDeps;
  private readonly store: WhatsappStateStore;
  /** Serializes ingest so seen-ring dedupe cannot race between upserts. */
  private readonly ingestChain = new WriteChain();
  private readonly typingTimers = new Map<string, ReturnType<typeof setInterval>>();

  private authDir: string;
  private creds?: { state: AuthenticationState; saveCreds: () => Promise<void> };
  private socket?: WhatsappSocket;
  private generation = 0;
  private sessionStartedAt = 0;
  private reconnectAttempt = 0;
  private reconnecting = false;
  private stopped = false;
  private accountJids: AccountJids;
  #b!: Baileys;
  private mediaSeq = 0;

  constructor(ctx: AdapterContext<WhatsappConnection>, deps?: WhatsappDeps) {
    this.ctx = ctx;
    if (deps) this.deps = deps;
    this.store = new WhatsappStateStore(ctx.dataDir, (error) => ctx.log("error", `写入 WhatsApp 状态失败: ${error.message}`));
    this.authDir = join(ctx.dataDir, AUTH_DIR_NAME);
    this.accountJids = [ctx.config.accountJid];
  }

  async start(): Promise<void> {
    if (this.stopped) throw new AdapterStartError("WhatsApp 适配器已停止，无法再次启动。");
    this.deps ??= await makeDefaultDeps();
    this.#b = await baileys();
    let entries: string[];
    try {
      entries = await readdir(this.authDir);
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code === "ENOENT") {
        throw new UserFacingError("WhatsApp 尚未配对：请先在 Paseo → IM 设置中扫码关联设备，再启用此连接。");
      }
      throw new AdapterStartError(`无法读取 WhatsApp 认证目录 ${this.authDir}: ${(error as Error).message}`);
    }
    if (entries.length === 0) {
      throw new UserFacingError("WhatsApp 凭据为空：请先在 Paseo → IM 设置中扫码关联设备，再启用此连接。");
    }
    await this.store.load();
    await this.spawnSocket();
  }

  async stop(): Promise<void> {
    if (this.stopped) return;
    this.stopped = true;
    this.generation += 1;
    for (const timer of this.typingTimers.values()) clearInterval(timer);
    this.typingTimers.clear();
    const socket = this.socket;
    this.socket = undefined;
    await this.store.flush().catch(() => {});
    if (socket) await Promise.resolve(socket.end(undefined)).catch(() => {});
  }

  // ---- connection lifecycle ----

  private async spawnSocket(): Promise<void> {
    const { state, saveCreds } = await this.deps.loadAuthState(this.authDir);
    // 对齐 pairing 侧：baileys 装载/落盘凭据用默认 umask，链接设备的 key 文件
    // 可能 world-readable；每次起 socket 前先收敛目录与文件权限。
    await hardenAuthDirectory(this.authDir);
    this.creds = { state, saveCreds };
    this.authDir = join(this.ctx.dataDir, AUTH_DIR_NAME);
    const generation = ++this.generation;
    this.sessionStartedAt = Date.now();
    this.socket = undefined;

    const config = socketConfigPreset(this.#b, state, await this.resolveVersion());
    const socket = this.deps.createSocket(config);
    this.socket = socket;
    this.refreshAccountIdentity();

    socket.ev.on("creds.update", () => void this.persistCreds(generation));
    socket.ev.on("connection.update", (raw) => this.onConnectionUpdate(generation, raw as Partial<ConnectionState>));
    socket.ev.on("messages.upsert", (raw) => void this.onUpsert(generation, raw as { messages?: WAMessage[]; type?: string }));
  }

  private async resolveVersion(): Promise<readonly [number, number, number] | undefined> {
    try {
      return await this.deps.fetchVersion();
    } catch {
      return undefined;
    }
  }

  /** PN and LID are separate identities; keep every known form of the account. */
  private refreshAccountIdentity(): void {
    const jids = new Set<string>([this.ctx.config.accountJid]);
    const sources = [this.socket?.user, this.creds?.state.creds.me];
    for (const source of sources) {
      for (const jid of [source?.id, source?.lid, source?.phoneNumber]) {
        if (typeof jid === "string" && jid) jids.add(this.#b!.jidNormalizedUser(jid));
      }
    }
    jids.delete("");
    this.accountJids = [...jids];
  }

  private async persistCreds(generation: number): Promise<void> {
    if (generation !== this.generation || this.stopped || !this.creds) return;
    try {
      await this.creds.saveCreds();
      await hardenAuthDirectory(this.authDir);
    } catch (error) {
      this.ctx.log("error", `保存 WhatsApp 凭据失败: ${(error as Error).message}`);
    }
    this.refreshAccountIdentity();
  }

  private onConnectionUpdate(generation: number, update: Partial<ConnectionState>): void {
    if (generation !== this.generation || this.stopped) return;
    if (update.connection === "open") {
      this.reconnectAttempt = 0;
      this.refreshAccountIdentity();
      this.ctx.log("info", "WhatsApp 已连接。");
      return;
    }
    if (update.connection === "close") {
      const status = disconnectStatus(update.lastDisconnect?.error);
      if (status === this.#b!.DisconnectReason.loggedOut) {
        // 账号在此设备上已登出：重连无意义，需要用户重新扫码。
        this.generation += 1;
        const socket = this.socket;
        this.socket = undefined;
        void Promise.resolve(socket?.end(undefined)).catch(() => {});
        this.ctx.onError(new UserFacingError("WhatsApp 设备已被移除或已退出登录，请在 Paseo → IM 设置中重新扫码配对。"));
        return;
      }
      void this.scheduleReconnect(status);
    }
  }

  private async scheduleReconnect(status: number | null): Promise<void> {
    if (this.reconnecting || this.stopped || this.ctx.signal.aborted) return;
    this.reconnecting = true;
    try {
      while (!this.stopped && !this.ctx.signal.aborted) {
        const seconds = BACKOFF_SECONDS[Math.min(this.reconnectAttempt, BACKOFF_SECONDS.length - 1)];
        this.reconnectAttempt = Math.min(this.reconnectAttempt + 1, BACKOFF_SECONDS.length - 1);
        this.ctx.log("warn", `WhatsApp 连接断开（status=${status ?? "未知"}），${seconds} 秒后重连。`);
        await sleep(seconds * 1_000, this.ctx.signal);
        if (this.stopped) return;
        try {
          await this.spawnSocket();
          return;
        } catch (error) {
          this.ctx.onError(error instanceof Error ? error : new Error(String(error)));
        }
      }
    } catch {
      // sleep() aborted via ctx.signal — shutdown in progress.
    } finally {
      this.reconnecting = false;
    }
  }

  // ---- inbound ----

  private async onUpsert(generation: number, payload: { messages?: WAMessage[]; type?: string }): Promise<void> {
    if (generation !== this.generation || this.stopped) return;
    if (payload.type !== "notify" && payload.type !== "append") return;
    // "append" replays history on (re)connect; only messages newer than a
    // 60s window before session start are relevant.
    const staleAppend = payload.type === "append";
    await this.ingestChain.run(async () => {
      for (const message of Array.isArray(payload.messages) ? payload.messages : []) {
        if (staleAppend && messageTimestampMs(message.messageTimestamp) < this.sessionStartedAt - APPEND_GRACE_MS) {
          continue;
        }
        await this.ingest(message);
      }
    });
  }

  /** Test seam: resolves once every ingest queued so far has settled. */
  async waitForIngest(): Promise<void> {
    await this.ingestChain.run(async () => {});
  }

  private async ingest(message: WAMessage): Promise<void> {
    try {
      const key = message.key;
      const remoteJid = typeof key?.remoteJid === "string" ? key.remoteJid : "";
      const platformId = typeof key?.id === "string" ? key.id : "";
      if (!remoteJid || !platformId) return;
      const dedupeId = `${remoteJid}:${platformId}`;
      if (this.store.hasSeen(dedupeId)) return;

      const keyRef = this.messageKeyRef(key);
      this.store.markSeen(dedupeId, keyRef);

      // 自聊回声：机器人自己发出的消息不再当作用户指令。
      if (key.fromMe === true && this.store.isOwn(platformId)) return;

      const normalized = normalizeWhatsappMessage(message, {
        connectionId: this.ctx.connectionId,
        accountJids: this.accountJids,
        isOwnMessage: (id) => this.store.isOwn(id),
      });
      if (!normalized) return;

      const media: NormalizedMedia[] = [];
      for (const source of normalized.pendingMedia) {
        try {
          media.push(await this.downloadMedia(message, source));
        } catch (error) {
          this.ctx.log("warn", `WhatsApp 媒体下载失败: ${(error as Error).message}`);
        }
      }
      const out: NormalizedMessage = { ...normalized.base };
      if (media.length > 0) out.media = media;
      if (!out.text && media.length === 0) return;
      this.ctx.onMessage(out);
    } catch (error) {
      this.ctx.log("error", `处理 WhatsApp 消息失败: ${(error as Error).message}`);
    }
  }

  private messageKeyRef(key: WAMessageKey | undefined): MessageKeyRef | null {
    if (!key || typeof key.id !== "string" || !key.id || typeof key.remoteJid !== "string" || !key.remoteJid) return null;
    return {
      remoteJid: key.remoteJid,
      fromMe: key.fromMe === true,
      id: key.id,
      participant: typeof key.participant === "string" && key.participant ? key.participant : undefined,
    };
  }

  private async downloadMedia(message: WAMessage, source: PendingMedia): Promise<NormalizedMedia> {
    if (source.kind === "image") {
      const settings = await this.ctx.settings();
      const maxBytes = settings.imageMaxDownloadMb * 1024 * 1024;
      const options: MediaDownloadOptions = { options: { signal: this.ctx.signal } };
      const buffer = await this.deps.download(message, options);
      if (buffer.length > maxBytes) {
        throw new Error(`图片超过 ${settings.imageMaxDownloadMb} MB 下载上限`);
      }
      return this.writeMediaFile(buffer, source);
    }
    // 文档没有平台侧配额：先按声明大小前置拒绝（sender 可谎报，下载后再校验）。
    if (source.size !== undefined && source.size > MAX_MEDIA_BYTES) {
      throw new UserFacingError(`文件超过 WhatsApp 100MB 上限（${Math.ceil(source.size / (1024 * 1024))}MB），未下载。`);
    }
    const options: MediaDownloadOptions = { options: { signal: this.ctx.signal } };
    const buffer = await this.deps.download(message, options);
    if (buffer.length > MAX_MEDIA_BYTES) {
      throw new UserFacingError(`文件超过 WhatsApp 100MB 上限，未保存。`);
    }
    return this.writeMediaFile(buffer, source);
  }

  private async writeMediaFile(buffer: Buffer, source: PendingMedia): Promise<NormalizedMedia> {
    await mkdir(this.ctx.mediaDir, { recursive: true, mode: 0o700 });
    const fallbackExt = source.kind === "image" ? "jpg" : "bin";
    const extension = mediaExtension(source.mimetype, fallbackExt);
    const original = source.name ? sanitizeFileName(source.name) : "";
    const stem = original ? original.replace(/\.[a-z0-9]{1,8}$/i, "") : `wa-${this.mediaSeq++}`;
    const name = `${stem}.${extension}`;
    const path = join(this.ctx.mediaDir, name);
    await writeFile(path, buffer, { mode: 0o600 });
    return {
      kind: source.kind,
      path,
      mime: source.mimetype,
      name: source.name ? sanitizeFileName(source.name) : name,
    };
  }

  // ---- outbound ----

  private requireSocket(): WhatsappSocket {
    if (this.stopped || !this.socket) {
      throw new UserFacingError("WhatsApp 未连接：请检查网络或重新启用此连接。");
    }
    return this.socket;
  }

  private render(text: string): string {
    try {
      return markdownToWhatsapp(text);
    } catch {
      return text;
    }
  }

  private reserveOutboundId(jid: string): string {
    const platformId = `paseo${randomUUID().replaceAll("-", "").slice(0, 18)}`;
    this.store.rememberOwn(platformId, jid);
    return platformId;
  }

  async sendText(outbound: OutboundText): Promise<SendResult> {
    const socket = this.requireSocket();
    const rendered = this.render(outbound.text);
    const chunks = splitChunks(rendered, this.capabilities.maxChunkChars);
    if (chunks.length === 0) return {};
    if (outbound.replyToId) await this.markRead(outbound.replyToId).catch(() => {});
    let firstId: string | undefined;
    for (const [index, chunk] of chunks.entries()) {
      // Reserve before dispatch: a self-chat echo of this message must not
      // re-enter the bridge even if sendMessage later fails.
      const platformId = this.reserveOutboundId(outbound.chatId);
      if (index === 0) firstId = platformId;
      const result = await socket.sendMessage(outbound.chatId, { text: chunk }, { messageId: platformId });
      this.store.attachOwnKey(platformId, result?.key as Partial<MessageKeyRef> | undefined);
    }
    return { messageId: firstId };
  }

  async editMessage(messageId: string, chatId: string, text: string): Promise<void> {
    const socket = this.requireSocket();
    const record = this.store.ownKey(messageId);
    if (!record?.key) {
      throw new UserFacingError(`无法编辑消息：未找到消息 ${messageId} 的原始 key（可能已超出记忆范围）。`);
    }
    // Baileys edits need the original full message key, not an edit envelope id.
    await socket.sendMessage(chatId, { text: this.render(text), edit: record.key });
  }

  async sendMedia(outbound: OutboundMedia): Promise<SendResult> {
    const socket = this.requireSocket();
    // 前置拒绝超大媒体，避免整文件读入内存后才失败。
    const { size } = await stat(outbound.path).catch((error: NodeJS.ErrnoException) => {
      throw new UserFacingError(`无法读取待发送的${outbound.kind === "image" ? "图片" : "文件"} ${outbound.path}：${error.message}`);
    });
    if (size > MAX_MEDIA_BYTES) {
      throw new UserFacingError(`待发送的${outbound.kind === "image" ? "图片" : "文件"}超过 WhatsApp 100MB 上限，未发送。`);
    }
    const buffer = await readFile(outbound.path);
    const platformId = this.reserveOutboundId(outbound.chatId);
    const content: AnyMessageContent =
      outbound.kind === "image"
        ? { image: buffer, caption: outbound.caption, mimetype: outbound.mime }
        : {
            document: buffer,
            mimetype: outbound.mime ?? "application/octet-stream",
            fileName: outbound.name ?? sanitizeFileName(basename(outbound.path)),
            caption: outbound.caption,
          };
    const result = await socket.sendMessage(outbound.chatId, content, { messageId: platformId });
    this.store.attachOwnKey(platformId, result?.key as Partial<MessageKeyRef> | undefined);
    return { messageId: platformId };
  }

  /** Best-effort read receipt for the inbound message being replied to. */
  private async markRead(messageId: string): Promise<void> {
    const socket = this.requireSocket();
    const separator = messageId.indexOf(":");
    if (separator <= 0) return;
    const key = this.store.seenKey(messageId);
    if (!key) return;
    try {
      await socket.readMessages([key]);
    } catch (error) {
      this.ctx.log("warn", `标记已读失败: ${(error as Error).message}`);
    }
  }

  async sendTyping(chatId: string): Promise<void> {
    if (this.stopped || !this.socket) {
      this.ctx.log("warn", "WhatsApp 未连接，无法发送输入状态。");
      return;
    }
    const socket = this.socket;
    const previous = this.typingTimers.get(chatId);
    if (previous) {
      clearInterval(previous);
      this.typingTimers.delete(chatId);
    }
    try {
      await socket.sendPresenceUpdate("composing", chatId);
    } catch (error) {
      this.ctx.log("warn", `发送输入状态失败: ${(error as Error).message}`);
      return;
    }
    const startedAt = Date.now();
    const timer = setInterval(() => {
      if (this.stopped || Date.now() - startedAt >= TYPING_TOTAL_MS) {
        clearInterval(timer);
        this.typingTimers.delete(chatId);
        if (!this.stopped) void socket.sendPresenceUpdate("paused", chatId).catch(() => {});
        return;
      }
      void socket.sendPresenceUpdate("composing", chatId).catch(() => {});
    }, TYPING_RENEW_MS);
    timer.unref?.();
    this.typingTimers.set(chatId, timer);
  }

  async testConnection(): Promise<{ ok: boolean; detail?: string }> {
    try {
      const entries = await readdir(this.authDir);
      if (entries.length === 0) return { ok: false, detail: "尚未扫码配对。" };
      return { ok: true, detail: "已配对，凭据就绪。" };
    } catch {
      return { ok: false, detail: "尚未扫码配对：请先在 Paseo → IM 设置中扫码关联设备。" };
    }
  }

  isOwnMessage(messageId: string): boolean {
    return this.store.isOwn(messageId);
  }

  rememberOwnMessage(messageId: string): void {
    this.store.rememberOwn(messageId, this.ctx.config.accountJid);
  }
}
