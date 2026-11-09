/** iMessage channel adapter: polls the macOS Messages chat.db with
 * `sqlite3 -json` and sends through `osascript` driving the Messages app.
 *
 * Protocol facts ported from xmanrui/dsh-im (MIT): chat.db polling SQL,
 * max-ROWID cursor bootstrap, AppleScript escaping, permission-error
 * classification, and the bot-echo prefix marker. */
import { execFile } from "node:child_process";
import { join } from "node:path";
import type { Connection } from "../../../shared/config.ts";
import type {
  ChannelCapabilities,
  NormalizedMessage,
  OutboundText,
  SendResult,
} from "../../../shared/model.ts";
import { readJson, atomicWrite } from "../../files.ts";
import { UserFacingError, userMessage } from "../../../shared/errors.ts";
import { CHANNEL_SPECS } from "../../../shared/channels.ts";
import type { AdapterContext, ChannelAdapter, ChannelDefinition } from "../types.ts";
import { defaultConfigure, requireFields } from "../configure.ts";
import { decodeAttributedBodyText } from "./typedstream.ts";

export type ImessageConnection = Extract<Connection, { channel: "imessage" }>;

/** Bot-echo marker: outbound messages carry it, inbound copies starting with
 * it are our own replies echoed back and must not be re-emitted. */
export const IMESSAGE_BOT_PREFIX = "🤖 Paseo\n";

const DEFAULT_POLL_INTERVAL_MS = 2_000;
const DEFAULT_BACKOFF_MS = [1_000, 3_000, 5_000, 10_000, 30_000];
const POLL_LIMIT = 100;
const SEEN_LIMIT = 512;
const STATE_FILE = "imessage-state.json";
const COMMAND_TIMEOUT_MS = 15_000;
const DEFAULT_DB_PATH = `${process.env.HOME ?? ""}/Library/Messages/chat.db`;

const DB_PERMISSION_PATTERN = /authorization denied|not authorized|unable to open database/i;
const AUTOMATION_PERMISSION_PATTERN = /not authorized|(-1743)|assistive/i;
/** sendScript 内无可用 iMessage 服务时抛出的标记错误。 */
const NO_IMESSAGE_SERVICE_PATTERN = /No iMessage service is available/i;

/** Injectable transport so tests never touch sqlite3/osascript. */
export interface ImessageDeps {
  /** Run one read-only query against chat.db, returning JSON rows. */
  runSql(query: string): Promise<Record<string, unknown>[]>;
  /** Run one AppleScript through `osascript -e`. */
  runOsa(script: string): Promise<string>;
  /** Healthy-poll interval in ms (default 2s). */
  pollIntervalMs?: number;
  /** Reconnect backoff for transient errors in ms (default [1,3,5,10,30]s). */
  backoffMs?: number[];
}

function cleanString(value: unknown): string | null {
  return typeof value === "string" && value.trim() ? value.trim() : null;
}

function messageOf(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

export function assertDarwin(): void {
  if (process.platform !== "darwin") {
    throw new UserFacingError(
      "iMessage 渠道仅支持 macOS：请在 Mac 上运行 Paseo（依赖「信息」App 与 ~/Library/Messages/chat.db）。",
    );
  }
}

/** C0 控制字符里 \b\t\n\f\r 之外全部剔除：AppleScript 字面量没有 \uXXXX
 * 转义，其余控制字符序列化进脚本会直接破坏字面量（JSON 转义覆盖 " 与 \）。 */
const APPLESCRIPT_STRIP_CONTROLS = /[\u0000-\u0007\u000b\u000e-\u001f]/g;

/** Messages 在多数系统上并没有名为 "iMessage" 的服务：动态取第一个 iMessage
 * 类型服务（ported from dsh whose-clause）；无服务时脚本内显式报错，适配层
 * 再转成中文提示。 */
export function sendScript(recipient: string, text: string): string {
  return `tell application "Messages"
  set serviceList to every service whose service type = iMessage
  if (count of serviceList) is 0 then error "No iMessage service is available"
  set targetService to item 1 of serviceList
  set targetBuddy to buddy ${JSON.stringify(recipient.replace(APPLESCRIPT_STRIP_CONTROLS, ""))} of targetService
  send ${JSON.stringify(text.replace(APPLESCRIPT_STRIP_CONTROLS, ""))} to targetBuddy
end tell`;
}

/** Poll query ported faithfully from dsh listMessages: outgoing rows excluded,
 * self-chat incoming copies read once and filtered by the bot prefix marker,
 * macOS 26 attributedBody accepted as a text fallback via hex(). */
export function pollQuery(cursor: number, limit = POLL_LIMIT): string {
  return `SELECT m.ROWID AS rowid, m.guid AS guid, m.text AS text,
      hex(m.attributedBody) AS attributedBodyHex,
      h.id AS sender, c.guid AS chatGuid, c.service_name AS serviceName,
      m.is_from_me AS isFromMe,
      datetime((m.date / 1000000000) + 978307200, 'unixepoch') AS receivedAt
      FROM message m
      JOIN chat_message_join cmj ON cmj.message_id = m.ROWID
      JOIN chat c ON c.ROWID = cmj.chat_id
      LEFT JOIN handle h ON h.ROWID = m.handle_id
      WHERE m.ROWID > ${cursor} AND m.is_from_me = 0
        AND ((m.text IS NOT NULL AND m.text != '') OR m.attributedBody IS NOT NULL)
        AND c.service_name = 'iMessage'
      ORDER BY m.ROWID ASC LIMIT ${limit};`;
}

function maxRowIdQuery(): string {
  return `SELECT COALESCE(MAX(m.ROWID), 0) AS rowid
      FROM message m
      JOIN chat_message_join cmj ON cmj.message_id = m.ROWID
      JOIN chat c ON c.ROWID = cmj.chat_id
      WHERE c.service_name = 'iMessage';`;
}

/** Parse one sqlite3 -json stdout payload (ported from dsh decodeRows). */
function decodeRows(stdout: string): Record<string, unknown>[] {
  const text = String(stdout ?? "").trim();
  if (!text) return [];
  try {
    const rows = JSON.parse(text);
    return Array.isArray(rows) ? rows : [];
  } catch (error) {
    throw new Error("iMessage 数据库返回了无法解析的输出。", { cause: error });
  }
}

/** sqlite3 -json / osascript transport over spawn (ported from dsh
 * MacOSMessagesApi command runners). */
export function defaultDeps(options: { dbPath?: string } = {}): ImessageDeps {
  const dbPath = options.dbPath ?? DEFAULT_DB_PATH;
  const run = (file: string, args: string[]) => {
    let resolve!: (value: string) => void;
    let reject!: (error: Error) => void;
    const promise = new Promise<string>((res, rej) => {
      resolve = res;
      reject = rej;
    });
    execFile(
      file,
      args,
      { timeout: COMMAND_TIMEOUT_MS, maxBuffer: 4 * 1024 * 1024, encoding: "utf8" },
      (error, stdout, stderr) => {
        if (error) {
          // Surface stderr first so permission patterns match (dsh behavior).
          reject(new Error(String(stderr || error.message), { cause: error }));
        } else {
          resolve(String(stdout));
        }
      },
    );
    return promise;
  };
  return {
    runSql: async (query) => decodeRows(await run("/usr/bin/sqlite3", ["-json", dbPath, query])),
    runOsa: (script) => run("/usr/bin/osascript", ["-e", script]),
  };
}

/** chat.db row shape (SQL aliases above) → NormalizedMessage. DM only: the
 * chat id is the buddy handle; every DM message is inherently addressed to the
 * bot. Returns null for noise: bot echoes, own messages, attachment-only rows,
 * non-iMessage service rows, and rows without text/sender. */
export function normalizeImessageRow(
  row: Record<string, unknown>,
  connectionId: string,
  now: () => number = Date.now,
): NormalizedMessage | null {
  if (!row || typeof row !== "object") return null;
  if (row.serviceName !== undefined && row.serviceName !== "iMessage") return null;
  const guid = cleanString(row.guid ?? row.id);
  const sender = cleanString(row.sender ?? row.handle_id);
  const text = cleanString(row.text)
    ?? decodeAttributedBodyText(row.attributedBodyHex ?? row.attributedBody);
  if (!guid || !sender || !text) return null;
  if (row.isFromMe === 1 || row.isFromMe === true) return null;
  // 自我回环过滤：Messages 会把自发消息的副本送回来，按前缀标记识别。
  if (text.startsWith(IMESSAGE_BOT_PREFIX)) return null;
  return {
    channel: "imessage",
    connectionId,
    kind: "dm",
    chatId: sender,
    senderId: sender,
    senderName: sender,
    messageId: guid,
    timestamp: parseReceivedAt(row.receivedAt) ?? now(),
    text,
    trigger: { mention: false, replyToBot: false, command: false },
  };
}

/** datetime(..., 'unixepoch') → "YYYY-MM-DD HH:MM:SS" in UTC. */
function parseReceivedAt(value: unknown): number | undefined {
  if (typeof value !== "string") return undefined;
  const parsed = Date.parse(`${value.trim().replace(" ", "T")}Z`);
  return Number.isFinite(parsed) ? parsed : undefined;
}

function normalizeRecipient(value: string): string {
  const address = cleanString(value);
  if (!address || address.length > 512 || /[\r\n]/.test(address)) {
    throw new UserFacingError("iMessage 收件人地址无效：请使用对方在「信息」中的手机号或 Apple ID 邮箱。");
  }
  return address;
}

interface ImessageState {
  cursor: number;
  seen: string[];
}

function parseState(value: unknown): ImessageState | undefined {
  if (!value || typeof value !== "object") return undefined;
  const row = value as { cursor?: unknown; seen?: unknown };
  if (typeof row.cursor !== "number" || !Number.isSafeInteger(row.cursor) || row.cursor < 0) {
    return undefined;
  }
  const seen = Array.isArray(row.seen)
    ? row.seen.filter((item): item is string => typeof item === "string")
    : [];
  return { cursor: row.cursor, seen };
}

class ImessageAdapter implements ChannelAdapter {
  readonly channel = "imessage" as const;
  readonly capabilities: ChannelCapabilities = {
    progress: "none",
    markdown: "plain",
    uploadImage: false,
    uploadFile: false,
    reactions: false,
    typing: false,
    maxChunkChars: 0,
    threads: false,
  };

  readonly #ctx: AdapterContext<ImessageConnection>;
  readonly #deps: ImessageDeps;
  readonly #statePath: string;
  readonly #pollIntervalMs: number;
  readonly #backoffMs: number[];

  #cursor: number | null = null;
  readonly #seen = new Set<string>();
  #seenOrder: string[] = [];
  #timer: NodeJS.Timeout | null = null;
  #polling: Promise<void> | null = null;
  #stopped = true;
  #backoffIndex = 0;

  constructor(ctx: AdapterContext<ImessageConnection>, deps: ImessageDeps) {
    this.#ctx = ctx;
    this.#deps = deps;
    this.#statePath = join(ctx.dataDir, STATE_FILE);
    this.#pollIntervalMs = deps.pollIntervalMs ?? DEFAULT_POLL_INTERVAL_MS;
    this.#backoffMs = deps.backoffMs ?? DEFAULT_BACKOFF_MS;
  }

  async start(): Promise<void> {
    if (!this.#stopped) return;
    // macOS 独占渠道：非 darwin 直接拒绝启动。
    assertDarwin();
    await this.#probeDatabase();
    const persisted = await readJson<ImessageState | undefined>(this.#statePath, parseState);
    if (persisted) {
      this.#cursor = persisted.cursor;
      for (const guid of persisted.seen) this.#rememberGuid(guid);
    } else {
      // 首次启动：用 max(ROWID) 初始化游标，跳过全部历史消息。
      this.#cursor = await this.#latestRowId();
      await this.#persistState();
    }
    this.#stopped = false;
    this.#schedule(0);
  }

  async stop(): Promise<void> {
    this.#stopped = true;
    if (this.#timer !== null) {
      clearTimeout(this.#timer);
      this.#timer = null;
    }
    await this.#polling?.catch(() => {});
    this.#polling = null;
  }

  /** maxChunkChars 为 0：不做分块，整段发送（文本前加机器人回环标记）。 */
  async sendText(outbound: OutboundText): Promise<SendResult> {
    const recipient = normalizeRecipient(outbound.chatId);
    const text = cleanString(outbound.text);
    if (!text) throw new UserFacingError("iMessage 消息内容不能为空。");
    const reply = text.startsWith(IMESSAGE_BOT_PREFIX) ? text : `${IMESSAGE_BOT_PREFIX}${text}`;
    try {
      await this.#deps.runOsa(sendScript(recipient, reply));
    } catch (error) {
      if (NO_IMESSAGE_SERVICE_PATTERN.test(messageOf(error))) {
        throw new UserFacingError("未找到可用的 iMessage 服务：请确认「信息」App 已登录 Apple ID 并启用 iMessage 后重试。");
      }
      if (AUTOMATION_PERMISSION_PATTERN.test(messageOf(error))) {
        throw new UserFacingError(
          "无法通过「信息」App 发送：请在「系统设置 → 隐私与安全性 → 自动化」中允许 Paseo 控制 Messages。",
        );
      }
      throw error instanceof Error ? error : new Error(String(error));
    }
    // osascript 不返回平台消息 id。
    return {};
  }

  /** 设置页「测试连接」：依次探测 chat.db 可读性与 Messages 自动化权限
   * （移植 dsh getPermissions 的探测语义）。 */
  async testConnection(): Promise<{ ok: boolean; detail?: string }> {
    try {
      assertDarwin();
      await this.#probeDatabase();
    } catch (error) {
      return { ok: false, detail: userMessage(error) };
    }
    try {
      await this.#deps.runOsa('tell application "Messages" to get name');
      return { ok: true, detail: "chat.db 可读，Messages 自动化可用。" };
    } catch (error) {
      if (AUTOMATION_PERMISSION_PATTERN.test(messageOf(error))) {
        return {
          ok: false,
          detail: "请在「系统设置 → 隐私与安全性 → 自动化」中允许 Paseo 控制 Messages。",
        };
      }
      return { ok: false, detail: `无法控制「信息」App：${messageOf(error)}` };
    }
  }

  async #probeDatabase(): Promise<void> {
    try {
      await this.#deps.runSql("SELECT 1 AS ok LIMIT 1;");
    } catch (error) {
      const message = messageOf(error);
      if (DB_PERMISSION_PATTERN.test(message)) {
        throw new UserFacingError(
          "无法读取 iMessage 数据库（chat.db）：请在「系统设置 → 隐私与安全性 → 完全磁盘访问权限」中为运行 Paseo 的程序授权，然后重试。",
        );
      }
      throw new UserFacingError(
        `无法读取 iMessage 数据库（chat.db）：${message}。请确认本机是 macOS、已安装 sqlite3，并至少打开过一次「信息」App。`,
      );
    }
  }

  async #latestRowId(): Promise<number> {
    let rows: Record<string, unknown>[];
    try {
      rows = await this.#deps.runSql(maxRowIdQuery());
    } catch (error) {
      throw new UserFacingError(
        `无法初始化 iMessage 游标（读取 chat.db 失败）：${messageOf(error)}。请确认「信息」App 可用后重试。`,
      );
    }
    const rowid = Number(rows[0]?.rowid ?? 0);
    return Number.isSafeInteger(rowid) && rowid >= 0 ? rowid : 0;
  }

  #schedule(delayMs: number): void {
    if (this.#stopped) return;
    this.#timer = setTimeout(() => {
      this.#timer = null;
      this.#polling = this.#poll().finally(() => {
        if (this.#stopped) return;
        this.#schedule(this.#nextDelayMs());
      });
    }, delayMs);
    this.#timer.unref();
  }

  #nextDelayMs(): number {
    if (this.#backoffIndex > 0) {
      return this.#backoffMs[Math.min(this.#backoffIndex - 1, this.#backoffMs.length - 1)];
    }
    return this.#pollIntervalMs;
  }

  async #poll(): Promise<void> {
    const startCursor = this.#cursor;
    if (this.#stopped || startCursor === null) return;
    // 游标在本地推进；emit 失败时不越过该 rowid（下轮重发，seen 防进程内重复）。
    let cursor = startCursor;
    try {
      const rows = await this.#deps.runSql(pollQuery(cursor));
      let emitFailedAt: number | null = null;
      for (const row of rows) {
        const rowid = Number(row.rowid);
        if (!Number.isSafeInteger(rowid) || rowid <= 0) continue;
        const message = normalizeImessageRow(row, this.#ctx.connectionId);
        if (message && !this.#seen.has(message.messageId)) {
          try {
            this.#ctx.onMessage(message);
            this.#rememberGuid(message.messageId);
          } catch (error) {
            // emit 失败：游标停在该 rowid 之前并终止本批（保序）；已成功 emit
            // 的行已记入 seen，下轮重取时只会被跳过、不会重复投递。
            this.#ctx.onError(error instanceof Error ? error : new Error(String(error)));
            emitFailedAt = rowid;
            break;
          }
        }
        if (rowid > cursor) cursor = rowid;
      }
      if (emitFailedAt !== null) cursor = Math.min(cursor, emitFailedAt - 1);
      if (cursor > startCursor) {
        this.#cursor = cursor;
        await this.#persistState();
      }
      this.#backoffIndex = 0;
    } catch (error) {
      if (this.#stopped) return;
      // 瞬时错误：交给 ctx.onError，并按退避表延长下一次轮询间隔。
      this.#backoffIndex += 1;
      const delayMs = this.#backoffMs[Math.min(this.#backoffIndex - 1, this.#backoffMs.length - 1)];
      this.#ctx.log(
        "error",
        `iMessage 轮询失败：${messageOf(error)}；${Math.round(delayMs / 1000)} 秒后重试。`,
      );
      this.#ctx.onError(error instanceof Error ? error : new Error(String(error)));
    }
  }

  #rememberGuid(guid: string): void {
    if (this.#seen.has(guid)) return;
    this.#seen.add(guid);
    this.#seenOrder.push(guid);
    while (this.#seenOrder.length > SEEN_LIMIT) {
      const evicted = this.#seenOrder.shift();
      if (evicted !== undefined) this.#seen.delete(evicted);
    }
  }

  async #persistState(): Promise<void> {
    await atomicWrite(this.#statePath, {
      cursor: this.#cursor ?? 0,
      seen: this.#seenOrder,
    });
  }
}

/** Test/manager entry point: inject `deps` to fake sqlite3/osascript. */
export function createAdapter(
  ctx: AdapterContext<ImessageConnection>,
  deps: ImessageDeps = defaultDeps(),
): ChannelAdapter {
  return new ImessageAdapter(ctx, deps);
}

export const definition: ChannelDefinition<ImessageConnection> = {
  channel: "imessage",
  create: (ctx) => createAdapter(ctx),
  // iMessage 无凭据字段（macOS 系统权限即凭据）；沿用统一的拆分逻辑兜底。
  configure: (fields) => {
    requireFields(CHANNEL_SPECS.imessage.fields, fields);
    return defaultConfigure("imessage", fields);
  },
  checkPlatform: () => assertDarwin(),
};
