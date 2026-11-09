/** WhatsApp channel adapter tests: fake Baileys transport, no real network. */

import { strict as assert } from "node:assert/strict";
import { test, before } from "node:test";
import { existsSync, mkdirSync, mkdtempSync, readFileSync, writeFileSync } from "node:fs";
import { mkdtemp, stat, truncate } from "node:fs/promises";
import { tmpdir } from "node:os";
import { basename, join } from "node:path";
import { randomUUID } from "node:crypto";
import { setTimeout as delay } from "node:timers/promises";
import type {
  AnyMessageContent,
  AuthenticationState,
  ConnectionState,
  MiscMessageGenerationOptions,
  WAMessage,
  WAMessageKey,
  WAPresence,
} from "@whiskeysockets/baileys";
import { connectionSchema, emptyPolicy } from "../../shared/config.ts";
import { UserFacingError } from "../../shared/errors.ts";
import type { NormalizedMessage } from "../../shared/model.ts";
import { imSettings, type ImSettings } from "../../shared/settings.ts";
import { baileys } from "../../server/channels/whatsapp/baileys.ts";

// normalize 直调用例依赖 Baileys 模块（LID 匹配/消息内容展开），文件级装载一次。
before(async () => {
  await baileys();
});
import type { AdapterContext, PairingStatus } from "../../server/channels/types.ts";
import {
  createAdapter,
  definition,
  type WhatsappConnection,
  type WhatsappDeps,
  type WhatsappSocket,
  type WhatsappSocketUser,
} from "../../server/channels/whatsapp/index.ts";
import { markdownToWhatsapp } from "../../server/channels/whatsapp/markdown.ts";
import {
  normalizeWhatsappMessage,
  type NormalizeOptions,
} from "../../server/channels/whatsapp/normalize.ts";
import { WhatsappAdapter } from "../../server/channels/whatsapp/adapter.ts";
import { WhatsappPairingSession } from "../../server/channels/whatsapp/pairing.ts";

const PN_JID = "15551234567@s.whatsapp.net";
const LID_JID = "209851234567890@lid";
const FRIEND_JID = "15559998888@s.whatsapp.net";
const GROUP_JID = "120363021234567890@g.us";

/** Let the fire-and-forget ingest chain settle deterministically. */
async function drain(ticks = 10): Promise<void> {
  for (let i = 0; i < ticks; i++) await new Promise<void>((resolve) => setImmediate(resolve));
}

/** Pairing status transitions settle across real fs/microtask work; poll
 * bounded by immediate ticks instead of wall-clock sleeps. */
async function pollUntil(
  session: Pick<WhatsappPairingSession, "poll">,
  predicate: (status: PairingStatus) => boolean,
): Promise<PairingStatus> {
  let status = await session.poll();
  for (let i = 0; i < 200 && !predicate(status); i++) {
    await new Promise<void>((resolve) => setImmediate(resolve));
    status = await session.poll();
  }
  return status;
}

class FakeSocket implements WhatsappSocket {
  user: WhatsappSocketUser = { id: PN_JID, lid: LID_JID, name: "Clare" };
  readonly sent: Array<{ jid: string; content: AnyMessageContent; options?: MiscMessageGenerationOptions }> = [];
  readonly presence: Array<{ type: WAPresence; toJid?: string }> = [];
  readonly reads: WAMessageKey[] = [];
  ended = false;
  private readonly handlers = new Map<string, Array<(payload: unknown) => void>>();

  readonly ev = {
    on: (type: string, handler: (payload: unknown) => void): void => {
      const list = this.handlers.get(type) ?? [];
      list.push(handler);
      this.handlers.set(type, list);
    },
  };

  on(type: string, payload: unknown): void {
    for (const handler of [...(this.handlers.get(type) ?? [])]) handler(payload);
  }

  /** Test fixture: only the Boom statusCode matters to disconnectStatus(). */
  emitConnection(
    update: Partial<Omit<ConnectionState, "lastDisconnect">> & {
      lastDisconnect?: { error: unknown; date?: Date };
    },
  ): void {
    this.on("connection.update", update);
  }

  emitCredsUpdate(): void {
    this.on("creds.update", {});
  }

  emitUpsert(messages: WAMessage[], type: "notify" | "append" = "notify"): void {
    this.on("messages.upsert", { messages, type });
  }

  async sendMessage(jid: string, content: AnyMessageContent, options?: MiscMessageGenerationOptions): Promise<WAMessage | undefined> {
    this.sent.push({ jid, content, options });
    return {
      key: { remoteJid: jid, fromMe: true, id: options?.messageId ?? `fake-${this.sent.length}` },
      message: {},
    } as unknown as WAMessage;
  }

  async readMessages(keys: WAMessageKey[]): Promise<void> {
    this.reads.push(...keys);
  }

  async sendPresenceUpdate(type: WAPresence, toJid?: string): Promise<void> {
    this.presence.push({ type, toJid });
  }

  async end(): Promise<void> {
    this.ended = true;
  }

  async logout(): Promise<void> {
    this.ended = true;
  }
}

interface Harness {
  ctx: AdapterContext<WhatsappConnection>;
  deps: WhatsappDeps;
  sockets: FakeSocket[];
  state: AuthenticationState;
  savedCreds: number[];
  messages: NormalizedMessage[];
  errors: Error[];
  logs: string[];
  dataDir: string;
  mediaDir: string;
  root: string;
}

function fakeAuthState(): AuthenticationState {
  return {
    creds: { me: { id: PN_JID, lid: LID_JID, name: "Clare" }, registered: false },
    keys: { set: async () => {} },
  } as unknown as AuthenticationState;
}

function makeHarness(options: { settings?: ImSettings } = {}): Harness {
  const root = mkdtempSync(join(tmpdir(), "paseo-wa-test-"));
  const dataDir = join(root, "data");
  const mediaDir = join(root, "media");
  mkdirSync(dataDir, { recursive: true });
  mkdirSync(mediaDir, { recursive: true });
  const config = connectionSchema.parse({
    channel: "whatsapp",
    id: randomUUID(),
    name: "WhatsApp 测试",
    enabled: true,
    target: null,
    policy: emptyPolicy(),
    bindings: {},
    revision: 0,
    accountJid: PN_JID,
  }) as WhatsappConnection;
  const messages: NormalizedMessage[] = [];
  const errors: Error[] = [];
  const logs: string[] = [];
  const sockets: FakeSocket[] = [];
  const savedCreds: number[] = [];
  const state = fakeAuthState();
  const deps: WhatsappDeps = {
    createSocket: () => {
      const socket = new FakeSocket();
      sockets.push(socket);
      return socket;
    },
    loadAuthState: async () => ({
      state,
      saveCreds: async () => {
        savedCreds.push(1);
      },
    }),
    download: async () => Buffer.from("fake-media-bytes"),
    makeQr: async (text) => `data:image/png;base64,${Buffer.from(text).toString("base64")}`,
    fetchVersion: async () => [2, 3000, 1043857760],
  };
  const ctx: AdapterContext<WhatsappConnection> = {
    connectionId: config.id,
    config,
    secrets: {},
    dataDir,
    mediaDir,
    signal: new AbortController().signal,
    settings: async () => options.settings ?? imSettings.schema.parse({}),
    log: (level, message) => logs.push(`${level}: ${message}`),
    onMessage: (message) => messages.push(message),
    onGroups: () => {},
    onError: (error) => errors.push(error),
  };
  return { ctx, deps, sockets, state, savedCreds, messages, errors, logs, dataDir, mediaDir, root };
}

async function startAdapter(harness: Harness) {
  const authDir = join(harness.dataDir, "whatsapp-auth");
  mkdirSync(authDir, { recursive: true });
  writeFileSync(join(authDir, "creds.json"), "{}");
  const adapter = new WhatsappAdapter(harness.ctx, harness.deps);
  await adapter.start();
  return adapter;
}

interface WaMessageSpec {
  remoteJid: string;
  id: string;
  fromMe?: boolean;
  participant?: string;
  participantAlt?: string;
  content?: Record<string, unknown>;
  timestamp?: number;
  pushName?: string;
}

function waMessage(spec: WaMessageSpec): WAMessage {
  return {
    key: {
      remoteJid: spec.remoteJid,
      fromMe: spec.fromMe ?? false,
      id: spec.id,
      participant: spec.participant,
      participantAlt: spec.participantAlt,
    },
    messageTimestamp: spec.timestamp ?? 1_700_000_000,
    pushName: spec.pushName,
    message: spec.content,
  } as unknown as WAMessage;
}

const SELF_CHAT: WaMessageSpec = {
  remoteJid: PN_JID,
  id: "SELF1",
  fromMe: true,
  content: { conversation: "帮我总结" },
};

test("definition：configure 校验 accountJid 并把非机密字段留在 config", () => {
  assert.equal(definition.channel, "whatsapp");
  const fields = definition.configure({ accountJid: PN_JID });
  assert.deepEqual(fields.config, { accountJid: PN_JID });
  assert.deepEqual(fields.secrets, {});
  assert.throws(() => definition.configure({ accountJid: "not-a-jid" }), UserFacingError);
  // 工厂与 definition.create 走同一构造路径
  const harness = makeHarness();
  const adapter = createAdapter(harness.ctx, harness.deps);
  assert.equal(adapter.channel, "whatsapp");
  assert.equal(adapter.capabilities.progress, "edit");
  assert.equal(adapter.remembersOwnMessages, true);
});

test("normalize：自聊 dm 转换为 NormalizedMessage", () => {
  const options: NormalizeOptions = {
    connectionId: "conn-1",
    accountJids: [PN_JID, LID_JID],
    isOwnMessage: () => false,
  };
  const normalized = normalizeWhatsappMessage(waMessage(SELF_CHAT), options);
  assert.ok(normalized);
  assert.equal(normalized.base.kind, "dm");
  assert.equal(normalized.base.chatId, PN_JID);
  assert.equal(normalized.base.senderId, PN_JID);
  assert.equal(normalized.base.text, "帮我总结");
  assert.equal(normalized.base.timestamp, 1_700_000_000_000);
  assert.deepEqual(normalized.base.trigger, { mention: false, replyToBot: false, command: false });
  assert.equal(normalized.base.senderName, undefined);
  assert.deepEqual(normalized.pendingMedia, []);
});

test("normalize：群聊 @提及命中 LID 双身份，普通群消息不触发", () => {
  const options: NormalizeOptions = {
    connectionId: "conn-1",
    accountJids: [PN_JID, LID_JID],
    isOwnMessage: () => false,
  };
  const mentioned = normalizeWhatsappMessage(
    waMessage({
      remoteJid: GROUP_JID,
      id: "G1",
      participant: FRIEND_JID,
      pushName: "Alice",
      content: {
        extendedTextMessage: {
          text: "@15551234567 你好",
          contextInfo: { mentionedJid: [LID_JID] },
        },
      },
    }),
    options,
  );
  assert.ok(mentioned);
  assert.equal(mentioned.base.kind, "group");
  assert.equal(mentioned.base.chatId, GROUP_JID);
  assert.equal(mentioned.base.senderId, FRIEND_JID);
  assert.equal(mentioned.base.senderName, "Alice");
  assert.equal(mentioned.base.trigger.mention, true);

  const plain = normalizeWhatsappMessage(
    waMessage({
      remoteJid: GROUP_JID,
      id: "G2",
      participant: FRIEND_JID,
      content: { conversation: "随便聊聊" },
    }),
    options,
  );
  assert.ok(plain);
  assert.equal(plain.base.trigger.mention, false);
  assert.equal(plain.base.trigger.replyToBot, false);
});

test("normalize：回复 bot 消息仅在 ring 命中时触发", () => {
  const options: NormalizeOptions = {
    connectionId: "conn-1",
    accountJids: [PN_JID, LID_JID],
    isOwnMessage: (id) => id === "BOT1",
  };
  const reply = normalizeWhatsappMessage(
    waMessage({
      remoteJid: FRIEND_JID,
      id: "R1",
      content: {
        extendedTextMessage: {
          text: "继续",
          contextInfo: {
            participant: PN_JID,
            stanzaId: "BOT1",
            quotedMessage: { conversation: "原话" },
          },
        },
      },
    }),
    options,
  );
  assert.ok(reply);
  assert.equal(reply.base.trigger.replyToBot, true);
  assert.deepEqual(reply.base.replyTo, { id: "BOT1", text: "原话", sender: PN_JID });

  const unknownId = normalizeWhatsappMessage(
    waMessage({
      remoteJid: FRIEND_JID,
      id: "R2",
      content: {
        extendedTextMessage: {
          text: "继续",
          contextInfo: { participant: PN_JID, stanzaId: "NOT-MINE" },
        },
      },
    }),
    options,
  );
  assert.ok(unknownId);
  assert.equal(unknownId.base.trigger.replyToBot, false);

  const strangerQuoted = normalizeWhatsappMessage(
    waMessage({
      remoteJid: FRIEND_JID,
      id: "R3",
      content: {
        extendedTextMessage: {
          text: "引用别人",
          contextInfo: { participant: FRIEND_JID, stanzaId: "BOT1" },
        },
      },
    }),
    options,
  );
  assert.ok(strangerQuoted);
  assert.equal(strangerQuoted.base.trigger.replyToBot, false);
});

test("normalize：图片/文档待下载媒体，噪音消息返回 null", () => {
  const options: NormalizeOptions = {
    connectionId: "conn-1",
    accountJids: [PN_JID, LID_JID],
    isOwnMessage: () => false,
  };
  const image = normalizeWhatsappMessage(
    waMessage({
      remoteJid: PN_JID,
      id: "IMG1",
      fromMe: true,
      content: { imageMessage: { mimetype: "image/jpeg", caption: "看看" } },
    }),
    options,
  );
  assert.ok(image);
  assert.equal(image.base.text, "看看");
  assert.deepEqual(image.pendingMedia, [{ kind: "image", mimetype: "image/jpeg" }]);

  const document = normalizeWhatsappMessage(
    waMessage({
      remoteJid: PN_JID,
      id: "DOC1",
      fromMe: true,
      content: { documentMessage: { mimetype: "application/pdf", fileName: "报告.pdf" } },
    }),
    options,
  );
  assert.ok(document);
  assert.deepEqual(document.pendingMedia, [{ kind: "file", mimetype: "application/pdf", name: "报告.pdf" }]);

  const imageDocument = normalizeWhatsappMessage(
    waMessage({
      remoteJid: PN_JID,
      id: "DOC2",
      fromMe: true,
      content: { documentMessage: { mimetype: "image/png", fileName: "截图.png" } },
    }),
    options,
  );
  assert.ok(imageDocument);
  assert.equal(imageDocument.pendingMedia[0]?.kind, "image");

  // 协议外的音频消息：无文本无待下载媒体（适配层将其按噪音跳过）
  const audio = normalizeWhatsappMessage(
    waMessage({
      remoteJid: PN_JID,
      id: "AUD1",
      fromMe: true,
      content: { audioMessage: { mimetype: "audio/ogg; codecs=opus" } },
    }),
    options,
  );
  assert.ok(audio);
  assert.equal(audio.base.text, undefined);
  assert.deepEqual(audio.pendingMedia, []);
  // 机器人发给联系人的回声（fromMe 且非自聊）→ null
  assert.equal(
    normalizeWhatsappMessage(
      waMessage({ remoteJid: FRIEND_JID, id: "ECHO", fromMe: true, content: { conversation: "hi" } }),
      options,
    ),
    null,
  );
});

test("start：缺失或空凭据目录给出中文可执行提示", async () => {
  const harness = makeHarness();
  await assert.rejects(
    createAdapter(harness.ctx, harness.deps).start(),
    (error: unknown) => error instanceof UserFacingError && /扫码/.test(error.message),
  );
  mkdirSync(join(harness.dataDir, "whatsapp-auth"), { recursive: true });
  await assert.rejects(
    createAdapter(harness.ctx, harness.deps).start(),
    (error: unknown) => error instanceof UserFacingError && /扫码/.test(error.message),
  );
});

test("ingest：自聊 emit、去重、append 时间窗、机器人回声抑制", async () => {
  const harness = makeHarness();
  const adapter = await startAdapter(harness);
  const socket = harness.sockets[0];

  socket.emitUpsert([waMessage(SELF_CHAT)]);
  await adapter.waitForIngest();
  assert.equal(harness.messages.length, 1);
  assert.equal(harness.messages[0]?.messageId, `${PN_JID}:SELF1`);

  // 重复推送同一消息 → 去重
  socket.emitUpsert([waMessage(SELF_CHAT)]);
  await adapter.waitForIngest();
  assert.equal(harness.messages.length, 1);

  // append：早于会话窗口的历史 → 跳过；窗口内的 → emit
  socket.emitUpsert([waMessage({ ...SELF_CHAT, id: "OLD", timestamp: 1_690_000_000 })], "append");
  await adapter.waitForIngest();
  assert.equal(harness.messages.length, 1);
  socket.emitUpsert([{ ...waMessage({ ...SELF_CHAT, id: "FRESH" }), messageTimestamp: Math.floor(Date.now() / 1000) } as unknown as WAMessage], "append");
  await adapter.waitForIngest();
  assert.equal(harness.messages.length, 2);
  assert.equal(harness.messages[1]?.messageId, `${PN_JID}:FRESH`);

  // 机器人自己发的消息（sendText 已记入 ring）→ 自聊回声抑制
  const sent = await adapter.sendText({ chatId: PN_JID, text: "进度更新" });
  assert.ok(sent.messageId);
  socket.emitUpsert([
    waMessage({ remoteJid: PN_JID, id: sent.messageId, fromMe: true, content: { conversation: "进度更新" } }),
  ]);
  await adapter.waitForIngest();
  assert.equal(harness.messages.length, 2);
  assert.ok(adapter.isOwnMessage(sent.messageId));

  // 来自另一个联系人的 fromMe 消息（非自聊）→ 忽略
  socket.emitUpsert([waMessage({ remoteJid: FRIEND_JID, id: "ECHO2", fromMe: true, content: { conversation: "hi" } })]);
  await adapter.waitForIngest();
  assert.equal(harness.messages.length, 2);

  await adapter.stop();
});

test("ingest：媒体下载落盘 mediaDir，协议外媒体按噪音跳过", async () => {
  const harness = makeHarness();
  const adapter = await startAdapter(harness);
  const socket = harness.sockets[0];

  socket.emitUpsert([
    waMessage({
      remoteJid: PN_JID,
      id: "IMG1",
      fromMe: true,
      content: { imageMessage: { mimetype: "image/jpeg", caption: "看看" } },
    }),
    waMessage({
      remoteJid: PN_JID,
      id: "DOC1",
      fromMe: true,
      content: { documentMessage: { mimetype: "application/pdf", fileName: "报告.pdf" } },
    }),
  ]);
  await adapter.waitForIngest();
  assert.equal(harness.messages.length, 2);

  const image = harness.messages[0];
  assert.equal(image?.text, "看看");
  assert.equal(image?.media?.length, 1);
  assert.equal(image?.media?.[0]?.kind, "image");
  assert.equal(image?.media?.[0]?.mime, "image/jpeg");
  assert.ok(image?.media?.[0]?.path);
  assert.equal(readFileSync(image.media[0].path, "utf8"), "fake-media-bytes");

  const document = harness.messages[1];
  assert.equal(document?.media?.[0]?.kind, "file");
  assert.equal(document?.media?.[0]?.name, "报告.pdf");
  assert.ok(existsSync(document?.media?.[0]?.path ?? ""));

  // 音频消息：无文本无媒体 → 不 emit
  socket.emitUpsert([
    waMessage({ remoteJid: PN_JID, id: "AUD1", fromMe: true, content: { audioMessage: { mimetype: "audio/ogg" } } }),
  ]);
  await adapter.waitForIngest();
  assert.equal(harness.messages.length, 2);

  await adapter.stop();
});

test("ingest：图片超过 settings 上限时降级为纯文本并告警", async () => {
  const settings = imSettings.schema.parse({ imageMaxDownloadMb: 1 });
  const harness = makeHarness({ settings });
  harness.deps = { ...harness.deps, download: async () => Buffer.alloc(2 * 1024 * 1024, 1) };
  const adapter = await startAdapter(harness);
  harness.sockets[0].emitUpsert([
    waMessage({ remoteJid: PN_JID, id: "BIG", fromMe: true, content: { imageMessage: { mimetype: "image/png", caption: "大图" } } }),
  ]);
  await adapter.waitForIngest();
  assert.equal(harness.messages.length, 1);
  assert.equal(harness.messages[0]?.text, "大图");
  assert.equal(harness.messages[0]?.media, undefined);
  assert.ok(harness.logs.some((entry) => entry.startsWith("warn:") && entry.includes("上限")));
  await adapter.stop();
});

/** The channels under test only send text/media envelopes; narrow safely. */
function sentText(content: AnyMessageContent): string {
  assert.ok("text" in content);
  return content.text;
}

test("sendText：markdown 渲染为 WhatsApp 方言", async () => {
  const harness = makeHarness();
  const adapter = await startAdapter(harness);
  await adapter.sendText({
    chatId: PN_JID,
    text: "# 标题\n\n**粗体** *斜体* ~~删除~~ `代码`\n\n[链接](https://example.com/x)",
  });
  assert.equal(harness.sockets[0]?.sent.length, 1);
  assert.equal(
    sentText(harness.sockets[0]?.sent[0]?.content ?? { text: "" }),
    "*标题*\n\n*粗体* _斜体_ ~删除~ `代码`\n\n链接 (https://example.com/x)",
  );
  await adapter.stop();
});

test("markdownToWhatsapp：保留无法表达的嵌套围栏", () => {
  const text = "前文\n````\n内嵌 ``` 围栏\n````\n后文";
  assert.equal(markdownToWhatsapp(text), text);
});

test("sendText：超过 4000 字符时 fence-aware 分块且保序", async () => {
  const harness = makeHarness();
  const adapter = await startAdapter(harness);
  const result = await adapter.sendText({
    chatId: PN_JID,
    text: `${"a".repeat(3990)}\n\n\`\`\`\ncode-line\n\`\`\`\n尾部`,
  });
  const sends = harness.sockets[0]?.sent ?? [];
  assert.equal(sends.length, 2);
  const first = sentText(sends[0]?.content ?? { text: "" });
  const second = sentText(sends[1]?.content ?? { text: "" });
  assert.ok(first.length <= 4_000);
  assert.ok(second.length <= 4_000);
  // 段尾空行跟随第一段：3990 个 a + 换行
  assert.equal(first.length, 3991);
  assert.equal(first.trim(), "a".repeat(3990));
  assert.ok(second.startsWith("```"));
  assert.ok(second.includes("尾部"));
  assert.ok(second.indexOf("```", 3) < second.indexOf("尾部"), "围栏在分块中保持完整");
  assert.ok(result.messageId);
  assert.ok(adapter.isOwnMessage(result.messageId));
  await adapter.stop();
});

test("editMessage：使用原消息 key 发送 edit 信封", async () => {
  const harness = makeHarness();
  const adapter = await startAdapter(harness);
  const sent = await adapter.sendText({ chatId: PN_JID, text: "第一版" });
  assert.ok(sent.messageId);
  await adapter.editMessage(sent.messageId, PN_JID, "更新 **了**");
  const edit = harness.sockets[0]?.sent.at(-1);
  assert.ok(edit);
  assert.ok("text" in edit.content);
  assert.ok("edit" in edit.content);
  assert.equal(edit.content.text, "更新 *了*");
  const key = edit.content.edit;
  assert.ok(key);
  assert.equal(key.remoteJid, PN_JID);
  assert.equal(key.id, sent.messageId);
  assert.equal(key.fromMe, true);
  // ring 外的消息不可编辑
  await assert.rejects(adapter.editMessage("unknown-id", PN_JID, "x"), UserFacingError);
  await adapter.stop();
});

test("sendMedia：图片与文档各按渠道信封发送", async () => {
  const harness = makeHarness();
  const adapter = await startAdapter(harness);
  const imagePath = join(harness.root, "out.png");
  const docPath = join(harness.root, "手册.pdf");
  writeFileSync(imagePath, "png-bytes");
  writeFileSync(docPath, "pdf-bytes");

  const imageResult = await adapter.sendMedia({ chatId: PN_JID, kind: "image", path: imagePath, mime: "image/png", caption: "图" });
  assert.ok(imageResult.messageId);
  const imageSend = harness.sockets[0]?.sent[0];
  assert.equal(imageSend?.jid, PN_JID);
  assert.ok(imageSend && "image" in imageSend.content);
  assert.deepEqual(imageSend.content.image, Buffer.from("png-bytes"));
  assert.ok("caption" in imageSend.content);
  assert.equal(imageSend.content.caption, "图");
  assert.ok("mimetype" in imageSend.content);
  assert.equal(imageSend.content.mimetype, "image/png");

  const docResult = await adapter.sendMedia({ chatId: PN_JID, kind: "file", path: docPath, mime: "application/pdf", name: "手册.pdf" });
  assert.ok(docResult.messageId);
  const docSend = harness.sockets[0]?.sent[1];
  assert.ok(docSend && "document" in docSend.content);
  assert.deepEqual(docSend.content.document, Buffer.from("pdf-bytes"));
  assert.ok("fileName" in docSend.content);
  assert.equal(docSend.content.fileName, "手册.pdf");
  assert.ok("mimetype" in docSend.content);
  assert.equal(docSend.content.mimetype, "application/pdf");
  await adapter.stop();
});

test("sendTyping/readMessages：composing 上报且回复前标记已读", async () => {
  const harness = makeHarness();
  const adapter = await startAdapter(harness);
  const socket = harness.sockets[0];
  socket.emitUpsert([waMessage({ remoteJid: FRIEND_JID, id: "IN1", content: { conversation: "问题" } })]);
  await adapter.waitForIngest();

  await adapter.sendTyping(FRIEND_JID);
  assert.ok(socket.presence.some((entry) => entry.type === "composing" && entry.toJid === FRIEND_JID));

  await adapter.sendText({ chatId: FRIEND_JID, text: "回答", replyToId: `${FRIEND_JID}:IN1` });
  const read = socket.reads.at(-1);
  assert.ok(read);
  assert.equal(read.remoteJid, FRIEND_JID);
  assert.equal(read.id, "IN1");
  assert.equal(read.fromMe, false);

  assert.ok(socket.presence.every((entry) => entry.type !== "paused"));
  await adapter.stop();
  // stop 后不再有 presence 残留定时器抛错
  await adapter.sendTyping(FRIEND_JID);
});

test("close(loggedOut)：不再重连并上报可执行错误", async () => {
  const harness = makeHarness();
  const adapter = await startAdapter(harness);
  harness.sockets[0]?.emitConnection({
    connection: "close",
    lastDisconnect: { error: { output: { statusCode: 401 } }, date: new Date() },
  });
  await adapter.waitForIngest();
  assert.equal(harness.errors.length, 1);
  assert.ok(harness.errors[0] instanceof UserFacingError);
  assert.match(harness.errors[0].message, /重新扫码/);
  assert.equal(harness.sockets[0]?.ended, true);
  await adapter.stop();
});

test("close(瞬时错误)：按退避重连生成新 socket", async () => {
  const harness = makeHarness();
  const adapter = await startAdapter(harness);
  assert.equal(harness.sockets.length, 1);
  harness.sockets[0]?.emitConnection({
    connection: "close",
    lastDisconnect: { error: { output: { statusCode: 428 } }, date: new Date() },
  });
  await delay(1_300);
  assert.ok(harness.sockets.length >= 2, "退避后应生成新的 socket");
  assert.ok(harness.logs.some((entry) => entry.startsWith("warn:") && entry.includes("重连")));
  await adapter.stop();
});

test("pairing：二维码 → scanned → ok 并写入凭据目录", async () => {
  const harness = makeHarness();
  const session = new WhatsappPairingSession(harness.ctx.connectionId, harness.dataDir, harness.deps);
  await session.start();
  const socket = harness.sockets[0];
  assert.ok(socket);

  socket.emitConnection({ qr: "QRTEXT" });
  const pending = await pollUntil(session, (status) => status.status === "pending" && Boolean(status.qrDataUrl));
  assert.equal(pending.status, "pending");
  assert.ok(pending.qrDataUrl);
  assert.ok(pending.qrDataUrl.startsWith("data:image/png;base64,"));
  assert.ok(pending.qrDataUrl.includes(Buffer.from("QRTEXT").toString("base64")));

  harness.state.creds.registered = true;
  socket.emitCredsUpdate();
  assert.equal((await pollUntil(session, (status) => status.status === "scanned")).status, "scanned");

  socket.emitConnection({ connection: "open" });
  const ok = await pollUntil(session, (status) => status.status === "ok" || status.status === "failed");
  assert.equal(ok.status, "ok");
  assert.deepEqual(ok.fields, { accountJid: PN_JID });
  assert.ok(existsSync(join(harness.dataDir, "whatsapp-auth")));
  assert.ok(harness.savedCreds.length >= 1);
  assert.equal(socket.ended, true);
  await session.stop();
});

test("pairing：登出式关闭 → failed 提示重新发起", async () => {
  const harness = makeHarness();
  const session = new WhatsappPairingSession(harness.ctx.connectionId, harness.dataDir, harness.deps);
  await session.start();
  harness.sockets[0]?.emitConnection({
    connection: "close",
    lastDisconnect: { error: { output: { statusCode: 401 } }, date: new Date() },
  });
  const failed = await pollUntil(session, (status) => status.status === "failed");
  assert.equal(failed.status, "failed");
  assert.match(failed.detail, /重新发起/);
  await session.stop();
});

test("pairing：restartRequired 后重建 socket 继续等待扫码", async () => {
  const harness = makeHarness();
  const session = new WhatsappPairingSession(harness.ctx.connectionId, harness.dataDir, harness.deps);
  await session.start();
  assert.equal(harness.sockets.length, 1);
  harness.sockets[0]?.emitConnection({
    connection: "close",
    lastDisconnect: { error: { output: { statusCode: 515 } }, date: new Date() },
  });
  for (let i = 0; i < 200 && harness.sockets.length < 2; i++) {
    await new Promise<void>((resolve) => setImmediate(resolve));
  }
  assert.equal(harness.sockets.length, 2);
  assert.equal((await session.poll()).status, "pending");
  await session.stop();
  assert.equal(harness.sockets.at(-1)?.ended, true);
});

test("normalize：群聊 LID senderId 归一为 participantAlt 的 PN 形态", () => {
  const options: NormalizeOptions = {
    connectionId: "conn-1",
    accountJids: [PN_JID, LID_JID],
    isOwnMessage: () => false,
  };
  const lidSender = `${FRIEND_JID.split("@")[0]}@lid`;
  const normalized = normalizeWhatsappMessage(
    waMessage({
      remoteJid: GROUP_JID,
      id: "LID1",
      participant: lidSender,
      participantAlt: FRIEND_JID,
      content: { conversation: "@bot 处理一下" },
    }),
    options,
  );
  assert.ok(normalized);
  assert.equal(normalized.base.senderId, FRIEND_JID, "提供 PN alt 时归一到 PN");

  // 无 participantAlt：保持 LID 原样（无法归一不猜）。
  const kept = normalizeWhatsappMessage(
    waMessage({ remoteJid: GROUP_JID, id: "LID2", participant: lidSender, content: { conversation: "普通消息" } }),
    options,
  );
  assert.ok(kept);
  assert.equal(kept.base.senderId, lidSender);

  // 参与者本身就是 PN：保持不变。
  const pn = normalizeWhatsappMessage(
    waMessage({ remoteJid: GROUP_JID, id: "PN1", participant: FRIEND_JID, participantAlt: lidSender, content: { conversation: "普通消息" } }),
    options,
  );
  assert.ok(pn);
  assert.equal(pn.base.senderId, FRIEND_JID);
});

test("start：spawnSocket 装载凭据后立即收敛认证目录权限", async () => {
  const harness = makeHarness();
  const authDir = join(harness.dataDir, "whatsapp-auth");
  mkdirSync(authDir, { recursive: true, mode: 0o755 });
  writeFileSync(join(authDir, "creds.json"), "{}", { mode: 0o644 });
  const adapter = new WhatsappAdapter(harness.ctx, harness.deps);
  await adapter.start();

  const dirMode = await stat(authDir);
  assert.equal(dirMode.mode & 0o777, 0o700, "目录应收敛为 0700");
  const fileMode = await stat(join(authDir, "creds.json"));
  assert.equal(fileMode.mode & 0o777, 0o600, "凭据文件应收敛为 0600");
  await adapter.stop();
});

test("ingest：文档声明超 100MB 前置拒绝，不发起下载", async () => {
  const harness = makeHarness();
  let downloads = 0;
  harness.deps = { ...harness.deps, download: async () => { downloads += 1; return Buffer.from("small"); } };
  const adapter = await startAdapter(harness);
  harness.sockets[0].emitUpsert([
    waMessage({
      remoteJid: PN_JID,
      id: "BIGDOC",
      fromMe: true,
      content: { documentMessage: { mimetype: "application/zip", fileName: "huge.zip", fileLength: 101 * 1024 * 1024 } },
    }),
  ]);
  await adapter.waitForIngest();
  assert.equal(downloads, 0, "声明超限应在下载前拒绝");
  assert.equal(harness.messages.length, 0, "无文本无媒体按噪音跳过");
  assert.ok(harness.logs.some((entry) => entry.startsWith("warn:") && entry.includes("100MB")));
  await adapter.stop();
});

test("ingest：文档实际字节超 100MB 下载后拒绝保存", async () => {
  const harness = makeHarness();
  // 发送方谎报大小：声明 1MB，实际 101MB。
  harness.deps = { ...harness.deps, download: async () => Buffer.alloc(101 * 1024 * 1024, 1) };
  const adapter = await startAdapter(harness);
  harness.sockets[0].emitUpsert([
    waMessage({
      remoteJid: PN_JID,
      id: "LIARDOC",
      fromMe: true,
      content: { documentMessage: { mimetype: "application/zip", fileName: "liar.zip", fileLength: 1024 * 1024 } },
    }),
  ]);
  await adapter.waitForIngest();
  assert.equal(harness.messages.length, 0);
  assert.ok(harness.logs.some((entry) => entry.startsWith("warn:") && entry.includes("100MB")));
  await adapter.stop();
});

test("sendMedia：超过 100MB 前置拒绝且中文 UserFacingError", async () => {
  const harness = makeHarness();
  const adapter = await startAdapter(harness);
  const bigPath = join(harness.root, "huge.bin");
  writeFileSync(bigPath, "");
  await truncate(bigPath, 100 * 1024 * 1024 + 1);
  await assert.rejects(
    adapter.sendMedia({ chatId: PN_JID, kind: "file", path: bigPath, name: "huge.bin" }),
    (error: unknown) => error instanceof UserFacingError && error.message.includes("100MB"),
  );
  assert.equal(harness.sockets[0]?.sent.length, 0, "超限媒体不得发出");
  await adapter.stop();
});
