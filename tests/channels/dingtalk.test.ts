/** Tests for the DingTalk channel adapter: frame normalize, media download,
 * dedupe/persistence, chunked sends with webhook→robot fallback, AI Card
 * streaming and reconnect backoff. All transports are faked; no network. */
import assert from "node:assert/strict";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { basename, join } from "node:path";
import { test } from "node:test";
import { setTimeout as delay } from "node:timers/promises";

import { TOPIC_ROBOT } from "dingtalk-stream";
import { UserFacingError } from "../../shared/errors.ts";
import type { NormalizedMessage } from "../../shared/model.ts";
import type { ImSettings } from "../../shared/settings.ts";
import {
  AI_CARD_TEMPLATE_ID,
  DINGTALK_OAPI_BASE,
  DingTalkApiError,
  cardMarkdown,
  isRecord,
  type DingTalkFrame,
  type DingtalkDeps,
  type DWClientLike,
  type DWDownStream,
} from "../../server/channels/dingtalk/api.ts";
import {
  createAdapter,
  definition,
  isFrameEmit,
  normalizeFrame,
  type DingTalkConnection,
  type DingtalkAdapter,
  type FrameOutcome,
} from "../../server/channels/dingtalk/index.ts";
import type { AdapterContext } from "../../server/channels/types.ts";

const CLIENT_ID = "ding0123456789abcdef";
const CONNECTION_ID = "9a1b2c3d-4e5f-4a6b-8c7d-123456789abc";
const FRAME_CONTEXT = { connectionId: CONNECTION_ID, clientId: CLIENT_ID };

type Listener = (downstream: DWDownStream) => unknown;
type RestCall = {
  path: string;
  body: unknown;
  base?: string;
  method?: string;
  token?: string;
};

interface FakeDw {
  client: DWClientLike;
  listeners: Map<string, Listener>;
  acks: Array<{ messageId: string; result: unknown }>;
  state: { connected: boolean; connectCalls: number };
  fail: { connectError?: Error };
}

function fakeDw(): FakeDw {
  const listeners = new Map<string, Listener>();
  const acks: Array<{ messageId: string; result: unknown }> = [];
  const state = { connected: false, connectCalls: 0 };
  const fail: { connectError?: Error } = {};
  const client: DWClientLike = {
    registerCallbackListener(topic, listener) {
      listeners.set(topic, listener);
      return client;
    },
    async connect() {
      state.connectCalls += 1;
      if (fail.connectError) throw fail.connectError;
      state.connected = true;
    },
    disconnect() {
      state.connected = false;
    },
    socketCallBackResponse(messageId, result) {
      acks.push({ messageId, result });
    },
    get connected() {
      return state.connected;
    },
    socket: null,
  };
  return { client, listeners, acks, state, fail };
}

interface FakeTransport {
  deps: DingtalkDeps;
  restCalls: RestCall[];
  webhookCalls: Array<{ url: string; body: unknown }>;
}

function fakeDeps(dw: FakeDw, overrides: Partial<DingtalkDeps> = {}): FakeTransport {
  const restCalls: RestCall[] = [];
  const webhookCalls: Array<{ url: string; body: unknown }> = [];
  const userWebhook = overrides.webhook;
  const deps: DingtalkDeps = {
    rest: overrides.rest ?? (async (path, body, base, method, token) => {
      restCalls.push({ path, body, base, method, token });
      if (path.startsWith("v1.0/oauth2/accessToken")) return { accessToken: "token-1", expireIn: 7200 };
      if (path.startsWith("v1.0/robot/messageFiles/download")) {
        return { downloadUrl: "https://download.dingtalk.com/file-1" };
      }
      if (path.startsWith("media/upload")) return { media_id: "media-1" };
      return {};
    }),
    async webhook(url, body) {
      webhookCalls.push({ url, body });
      return userWebhook ? userWebhook(url, body) : { errcode: 0 };
    },
    createDw: overrides.createDw ?? (() => dw.client),
    fetchUrl:
      overrides.fetchUrl ?? (async () => new Response(new Uint8Array([1, 2, 3]), { status: 200 })),
    backoffMs: overrides.backoffMs ?? [5, 5, 5],
    monitorIntervalMs: overrides.monitorIntervalMs ?? 5,
    now: overrides.now,
    waitConnected: overrides.waitConnected,
    cardRetryDelayMs: overrides.cardRetryDelayMs,
  };
  return { deps, restCalls, webhookCalls };
}

const TEST_SETTINGS: ImSettings = {
  imageMaxDownloadMb: 30,
  imageMaxSingleMb: 5,
  imageMaxTotalMb: 100,
  imageMaxCount: 20,
  busyMode: "queue",
  attachmentTtlHours: 168,
  deferTimeoutMinutes: 30,
};

async function makeCtx(overrides: Partial<AdapterContext<DingTalkConnection>> = {}) {
  const messages: NormalizedMessage[] = [];
  const errors: Error[] = [];
  const logs: string[] = [];
  const dataDir = await mkdtemp(join(tmpdir(), "paseo-dingtalk-data-"));
  const mediaDir = await mkdtemp(join(tmpdir(), "paseo-dingtalk-media-"));
  const ctx: AdapterContext<DingTalkConnection> = {
    connectionId: CONNECTION_ID,
    config: {
      channel: "dingtalk",
      id: CONNECTION_ID,
      name: "钉钉测试",
      enabled: true,
      target: null,
      policy: {
        groupAdmission: "approval",
        directMessages: "everyone",
        groupUsers: "operators",
        operators: [],
        invocation: "mentions",
      },
      bindings: {},
      revision: 0,
      clientId: CLIENT_ID,
    },
    secrets: { clientSecret: "client-secret" },
    dataDir,
    mediaDir,
    signal: new AbortController().signal,
    settings: async () => TEST_SETTINGS,
    log: (level, message) => logs.push(`${level}: ${message}`),
    onMessage: (message) => messages.push(message),
    onGroups: () => {},
    onError: (error) => errors.push(error),
    ...overrides,
  };
  return { ctx, messages, errors, logs, dataDir, mediaDir };
}

interface Harness {
  ctx: AdapterContext<DingTalkConnection>;
  adapter: DingtalkAdapter;
  dw: FakeDw;
  restCalls: RestCall[];
  webhookCalls: Array<{ url: string; body: unknown }>;
  messages: NormalizedMessage[];
  errors: Error[];
  logs: string[];
  dataDir: string;
  mediaDir: string;
}

async function makeHarness(
  overrides: Partial<DingtalkDeps> = {},
  ctxOverrides: Partial<AdapterContext<DingTalkConnection>> = {},
  dw: FakeDw = fakeDw(),
): Promise<Harness> {
  const env = await makeCtx(ctxOverrides);
  const transport = fakeDeps(dw, overrides);
  const adapter = createAdapter(env.ctx, transport.deps);
  return { ...env, ...transport, adapter, dw };
}

function cleanup(...dirs: string[]): () => Promise<void> {
  return async () => {
    for (const dir of dirs) await rm(dir, { recursive: true, force: true });
  };
}

async function waitFor(predicate: () => boolean, timeoutMs = 3000): Promise<void> {
  const start = Date.now();
  while (!predicate()) {
    if (Date.now() - start > timeoutMs) throw new Error("waitFor 超时");
    await delay(5);
  }
}

function bodyRecord(body: unknown): Record<string, unknown> {
  if (!isRecord(body)) throw new Error(`消息体不是对象：${JSON.stringify(body)}`);
  return body;
}

function field(record: Record<string, unknown>, key: string): Record<string, unknown> {
  if (!isRecord(record[key])) throw new Error(`消息体缺少对象字段 ${key}`);
  return record[key];
}

function sentText(body: unknown): string {
  const text = field(bodyRecord(body), "text");
  if (typeof text.content !== "string") throw new Error(`消息体缺少 text.content：${JSON.stringify(body)}`);
  return text.content;
}

function assertEmit(outcome: FrameOutcome): Extract<FrameOutcome, { action: "emit" }> {
  assert.equal(outcome.action, "emit", JSON.stringify(outcome));
  if (!isFrameEmit(outcome)) throw new Error("unreachable");
  return outcome;
}

function baseFrame(overrides: Record<string, unknown> = {}): DingTalkFrame {
  return {
    conversationId: "cidDM0001",
    conversationType: "1",
    msgId: "msg0001",
    msgtype: "text",
    senderStaffId: "staff001",
    senderNick: "张三",
    text: { content: "你好" },
    sessionWebhook: "https://oapi.dingtalk.com/robot/send?sessionToken=abc",
    sessionWebhookExpiredTime: 4102444800000,
    createAt: 1730980800000,
    robotCode: CLIENT_ID,
    ...overrides,
  } as DingTalkFrame;
}

function groupFrame(overrides: Record<string, unknown> = {}): DingTalkFrame {
  return baseFrame({
    conversationId: "cidGRP0001",
    conversationType: "2",
    msgId: "grp0001",
    atUsers: [{ staffId: "u-mate", dingtalkId: CLIENT_ID }],
    text: { content: "@小助手 帮我查天气" },
    ...overrides,
  });
}

async function deliver(dw: FakeDw, frame: DingTalkFrame, messageId = "cb-1"): Promise<void> {
  const listener = dw.listeners.get(TOPIC_ROBOT);
  assert.ok(listener, "未注册机器人回调监听器");
  listener({ headers: { messageId, topic: TOPIC_ROBOT }, data: JSON.stringify(frame) });
}

// ---------------------------------------------------------------- definition

test("definition 暴露钉钉能力并按 SECRET_KEYS 拆分 configure", async (t) => {
  assert.equal(definition.channel, "dingtalk");
  const env = await makeCtx();
  t.after(cleanup(env.dataDir, env.mediaDir));
  const adapter = createAdapter(env.ctx);
  assert.equal(adapter.capabilities.progress, "stream");
  assert.equal(adapter.capabilities.markdown, "plain");
  assert.equal(adapter.capabilities.uploadImage, true);
  assert.equal(adapter.capabilities.uploadFile, true);
  assert.equal(adapter.capabilities.maxChunkChars, 4000);
  assert.equal(adapter.capabilities.reactions, false);
  assert.equal(adapter.capabilities.threads, false);

  const configured = definition.configure({ clientId: "  app-key ", clientSecret: " secret " });
  assert.deepEqual(configured, { config: { clientId: "app-key" }, secrets: { clientSecret: "secret" } });
  assert.throws(
    () => definition.configure({ clientId: "app-key" }),
    (error: unknown) => error instanceof UserFacingError && /Client Secret/.test((error as Error).message),
  );
});

// ---------------------------------------------------------------- normalize

test("normalize 私聊文本真实形状", () => {
  const outcome = assertEmit(normalizeFrame(baseFrame(), FRAME_CONTEXT));
  const { message, conversation, media } = outcome;
  assert.equal(message.channel, "dingtalk");
  assert.equal(message.connectionId, CONNECTION_ID);
  assert.equal(message.kind, "dm");
  assert.equal(message.chatId, "cidDM0001");
  assert.equal(message.senderId, "staff001");
  assert.equal(message.senderName, "张三");
  assert.equal(message.messageId, "msg0001");
  assert.equal(message.timestamp, 1730980800000);
  assert.equal(message.text, "你好");
  assert.equal(message.trigger.mention, true);
  assert.equal(message.trigger.replyToBot, false);
  assert.equal(message.trigger.command, false);
  assert.deepEqual(media, []);
  assert.equal(conversation.kind, "dm");
  assert.equal(conversation.webhook, "https://oapi.dingtalk.com/robot/send?sessionToken=abc");
  assert.equal(conversation.webhookExpiresAt, 4102444800000);
  assert.equal(conversation.senderId, "staff001");
});

test("normalize 群聊 @机器人（dingtalkId 匹配），并剥离首部 @", () => {
  const byDingtalkId = assertEmit(normalizeFrame(groupFrame(), FRAME_CONTEXT));
  assert.equal(byDingtalkId.message.kind, "group");
  assert.equal(byDingtalkId.message.trigger.mention, true);
  assert.equal(byDingtalkId.message.text, "帮我查天气");

  // staffId 恒为员工 ID，不会等于 robotCode：死码分支已删，仅命中不触发。
  const staffOnly = assertEmit(
    normalizeFrame(groupFrame({ atUsers: [{ staffId: CLIENT_ID }] }), FRAME_CONTEXT),
  );
  assert.equal(staffOnly.message.trigger.mention, false);
});

test("normalize 群聊仅 @ 他人不触发，isAtAll 与 isInAtList 触发", () => {
  const othersOnly = assertEmit(
    normalizeFrame(
      groupFrame({ atUsers: [{ staffId: "u1", dingtalkId: "someone" }], isAtAll: false }),
      FRAME_CONTEXT,
    ),
  );
  assert.equal(othersOnly.message.trigger.mention, false);

  const atAll = assertEmit(normalizeFrame(groupFrame({ atUsers: [], isAtAll: true }), FRAME_CONTEXT));
  assert.equal(atAll.message.trigger.mention, true);

  const inAtList = assertEmit(normalizeFrame(groupFrame({ atUsers: [], isInAtList: true }), FRAME_CONTEXT));
  assert.equal(inAtList.message.trigger.mention, true);
});

test("normalize 回复机器人真实形状（repliedMsg）", () => {
  const outcome = assertEmit(
    normalizeFrame(
      groupFrame({
        atUsers: [],
        text: {
          content: "好的",
          repliedMsg: {
            msgId: "bot-1",
            senderStaffId: CLIENT_ID,
            senderNick: "小助手",
            content: JSON.stringify({ text: { content: "原始问题" } }),
          },
        },
      }),
      FRAME_CONTEXT,
    ),
  );
  assert.equal(outcome.message.trigger.replyToBot, true);
  assert.equal(outcome.message.replyTo?.id, "bot-1");
  assert.equal(outcome.message.replyTo?.text, "原始问题");
  assert.equal(outcome.message.replyTo?.sender, "小助手");

  const others = assertEmit(
    normalizeFrame(
      groupFrame({
        atUsers: [],
        text: { content: "好的", repliedMsg: { msgId: "m-2", senderId: "staff009", content: "闲聊" } },
      }),
      FRAME_CONTEXT,
    ),
  );
  assert.equal(others.message.trigger.replyToBot, false);
  assert.equal(others.message.replyTo?.text, "闲聊");
});

test("normalize 机器人自身消息与噪音被跳过", () => {
  const self = normalizeFrame(baseFrame({ chatbotUserId: "bot-u1", senderId: "bot-u1" }), FRAME_CONTEXT);
  assert.equal(self.action, "skip");
  if (self.action === "skip") assert.match(self.reason, /机器人自身/);

  const mentionOnly = normalizeFrame(groupFrame({ text: { content: "@小助手" } }), FRAME_CONTEXT);
  assert.equal(mentionOnly.action, "skip");

  const audio = normalizeFrame(baseFrame({ msgtype: "audio", text: undefined }), FRAME_CONTEXT);
  assert.equal(audio.action, "skip");

  const noSender = normalizeFrame(baseFrame({ senderStaffId: undefined, senderId: undefined }), FRAME_CONTEXT);
  assert.equal(noSender.action, "skip");
});

test("normalize 富文本（richtext）提取文本与图片引用", () => {
  const outcome = assertEmit(
    normalizeFrame(
      baseFrame({
        msgtype: "richText",
        text: undefined,
        content: {
          richText: [
            { type: "text", text: { content: "看这张图" } },
            { type: "picture", downloadCode: "pic-9" },
          ],
        },
      }),
      FRAME_CONTEXT,
    ),
  );
  assert.equal(outcome.message.text, "看这张图");
  assert.equal(outcome.media.length, 1);
  assert.equal(outcome.media[0]?.kind, "image");
  assert.equal(outcome.media[0]?.downloadCode, "pic-9");
});

// ------------------------------------------------------- inbound + download

test("入站媒体：自带 messageFileDownloadUrl 直接下载图片", async (t) => {
  const fetched: string[] = [];
  const harness = await makeHarness({
    fetchUrl: async (url) => {
      fetched.push(url);
      return new Response(new Uint8Array([1, 2, 3]), { status: 200 });
    },
  });
  t.after(cleanup(harness.dataDir, harness.mediaDir));
  t.after(() => harness.adapter.stop());
  await harness.adapter.start();
  await deliver(harness.dw, baseFrame({ msgtype: "picture", messageFileDownloadUrl: "https://download.dingtalk.com/pic-1", text: undefined }));
  await harness.adapter.settle();
  assert.equal(harness.messages.length, 1);
  const media = harness.messages[0]?.media?.[0];
  assert.ok(media, "媒体未下载");
  assert.equal(media.kind, "image");
  assert.equal(media.mime, "image/png");
  assert.ok(media.path.startsWith(harness.mediaDir));
  // 文件名带 message_id 前缀，防不同消息同名覆盖。
  assert.equal(basename(media.path), "msg0001-dingtalk.png");
  assert.deepEqual(new Uint8Array(await readFile(media.path)), new Uint8Array([1, 2, 3]));
  assert.deepEqual(fetched, ["https://download.dingtalk.com/pic-1"]);
  assert.equal(harness.restCalls.some((call) => call.path.startsWith("v1.0/robot/messageFiles/download")), false);
});

test("入站媒体：downloadCode 换取下载地址后下载文件", async (t) => {
  const harness = await makeHarness();
  t.after(cleanup(harness.dataDir, harness.mediaDir));
  t.after(() => harness.adapter.stop());
  await harness.adapter.start();
  await deliver(
    harness.dw,
    baseFrame({ msgtype: "file", text: undefined, content: { downloadCode: "code-1", fileName: "报表.xlsx" } }),
  );
  await harness.adapter.settle();
  assert.equal(harness.messages.length, 1);
  const media = harness.messages[0]?.media?.[0];
  assert.ok(media);
  assert.equal(media.kind, "file");
  assert.equal(media.name, "报表.xlsx");
  assert.equal(basename(media.path), "msg0001-报表.xlsx", "落盘文件名须带 message_id 前缀");
  assert.equal(media.mime, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet");
  const exchange = harness.restCalls.find((call) => call.path === "v1.0/robot/messageFiles/download");
  assert.ok(exchange, "未换取下载地址");
  assert.deepEqual(exchange.body, { downloadCode: "code-1", robotCode: CLIENT_ID });
  assert.equal(exchange.token, "token-1");
  assert.deepEqual(new Uint8Array(await readFile(media.path)), new Uint8Array([1, 2, 3]));
});

test("去重与会话记录持久化到 dingtalk-state.json", async (t) => {
  const harness = await makeHarness();
  t.after(cleanup(harness.dataDir, harness.mediaDir));
  t.after(() => harness.adapter.stop());
  await harness.adapter.start();
  await deliver(harness.dw, baseFrame());
  await harness.adapter.settle();
  await deliver(harness.dw, baseFrame(), "cb-2");
  await harness.adapter.settle();
  assert.equal(harness.messages.length, 1);
  const stored = bodyRecord(JSON.parse(await readFile(join(harness.dataDir, "dingtalk-state.json"), "utf8")));
  assert.deepEqual(stored.seen, ["msg0001"]);
  const dmRecord = field(field(stored, "conversations"), "cidDM0001");
  assert.equal(dmRecord.kind, "dm");
  assert.equal(dmRecord.senderId, "staff001");
  assert.ok(dmRecord.webhook);
});

test("回调帧 ack success 并进入 onMessage", async (t) => {
  const harness = await makeHarness();
  t.after(cleanup(harness.dataDir, harness.mediaDir));
  t.after(() => harness.adapter.stop());
  await harness.adapter.start();
  await deliver(harness.dw, baseFrame(), "cb-77");
  await harness.adapter.settle();
  assert.deepEqual(harness.dw.acks[0], { messageId: "cb-77", result: { status: "success", message: "OK" } });
  assert.equal(harness.messages.length, 1);
});

// ---------------------------------------------------------------- sendText

test("状态文件损坏时重置空状态并告警，消息仍投递", async (t) => {
  const harness = await makeHarness();
  t.after(cleanup(harness.dataDir, harness.mediaDir));
  t.after(() => harness.adapter.stop());
  await writeFile(join(harness.dataDir, "dingtalk-state.json"), "{ 损坏的 JSON", "utf8");
  await harness.adapter.start();
  assert.ok(harness.logs.some((entry) => entry.includes("状态文件损坏")), "应记录损坏告警");
  await deliver(harness.dw, baseFrame());
  await harness.adapter.settle();
  assert.equal(harness.messages.length, 1, "重置后消息不应被误判为已见过");
});

test("sendText 按 4000 字分块且代码围栏不拆", async (t) => {
  const harness = await makeHarness();
  t.after(cleanup(harness.dataDir, harness.mediaDir));
  t.after(() => harness.adapter.stop());
  await harness.adapter.start();
  await deliver(harness.dw, baseFrame());
  await harness.adapter.settle();
  harness.webhookCalls.length = 0;

  const fence = "```js\n" + Array.from({ length: 200 }, (_, i) => `const v${i} = ${i};`).join("\n") + "\n```";
  const text = `${"开头段落。\n".repeat(300)}\n${fence}\n结尾说明。`;
  const result = await harness.adapter.sendText({ chatId: "cidDM0001", text });
  assert.deepEqual(result, {});
  assert.ok(harness.webhookCalls.length >= 2, `分块数不足：${harness.webhookCalls.length}`);
  for (const call of harness.webhookCalls) {
    const content = sentText(call.body);
    assert.ok(content.length <= 4000);
    assert.equal(((content.match(/```/g) ?? []).length) % 2, 0, "围栏被拆散");
  }
  const joined = harness.webhookCalls.map((call) => sentText(call.body)).join("\n");
  assert.ok(joined.includes("```js\nconst v0 = 0;"));
  assert.ok(joined.includes("结尾说明。"));
});

test("sendText webhook 过期回退机器人接口（私聊与群聊分别路由）", async (t) => {
  const harness = await makeHarness();
  t.after(cleanup(harness.dataDir, harness.mediaDir));
  t.after(() => harness.adapter.stop());
  await harness.adapter.start();
  await deliver(
    harness.dw,
    baseFrame({ sessionWebhookExpiredTime: Date.now() - 3_600_000 }),
  );
  await deliver(
    harness.dw,
    groupFrame({ sessionWebhookExpiredTime: Date.now() - 3_600_000 }),
    "cb-2",
  );
  await harness.adapter.settle();

  await harness.adapter.sendText({ chatId: "cidDM0001", text: "私聊回复" });
  const dm = harness.restCalls.find((call) => call.path === "v1.0/robot/oToMessages/batchSend");
  assert.ok(dm, "私聊未走机器人接口");
  const dmBody = bodyRecord(dm.body);
  assert.deepEqual(dmBody.userIds, ["staff001"]);
  assert.equal(dmBody.robotCode, CLIENT_ID);
  assert.equal(dmBody.msgKey, "sampleText");
  if (typeof dmBody.msgParam !== "string") throw new Error("msgParam 不是 JSON 字符串");
  assert.deepEqual(JSON.parse(dmBody.msgParam), { content: "私聊回复" });

  await harness.adapter.sendText({ chatId: "cidGRP0001", text: "群聊回复" });
  const group = harness.restCalls.find((call) => call.path === "v1.0/robot/groupMessages/send");
  assert.ok(group, "群聊未走机器人接口");
  assert.equal(bodyRecord(group.body).openConversationId, "cidGRP0001");
  assert.equal(harness.webhookCalls.length, 0);
});

test("sendText webhook 被拒（errcode 非 0）回退机器人接口", async (t) => {
  const harness = await makeHarness({
    webhook: async () => ({ errcode: 310000, errmsg: "session expired" }),
  });
  t.after(cleanup(harness.dataDir, harness.mediaDir));
  t.after(() => harness.adapter.stop());
  await harness.adapter.start();
  await deliver(harness.dw, baseFrame());
  await harness.adapter.settle();

  await harness.adapter.sendText({ chatId: "cidDM0001", text: "换条路发" });
  assert.equal(harness.webhookCalls.length, 1);
  assert.ok(harness.restCalls.some((call) => call.path === "v1.0/robot/oToMessages/batchSend"));
  assert.ok(harness.logs.some((entry) => entry.includes("webhook 回复失败")));
});

test("sendText 无会话记录时给出可执行错误", async (t) => {
  const harness = await makeHarness();
  t.after(cleanup(harness.dataDir, harness.mediaDir));
  t.after(() => harness.adapter.stop());
  await harness.adapter.start();
  await assert.rejects(
    harness.adapter.sendText({ chatId: "cidUnknown", text: "hi" }),
    (error: unknown) => error instanceof UserFacingError && /接收用户/.test((error as Error).message),
  );
});

// ---------------------------------------------------------------- sendMedia

test("sendMedia 上传后经 webhook 发送 file 与 image 消息体", async (t) => {
  const harness = await makeHarness();
  t.after(cleanup(harness.dataDir, harness.mediaDir));
  t.after(() => harness.adapter.stop());
  await harness.adapter.start();
  await deliver(harness.dw, baseFrame());
  await harness.adapter.settle();

  const filePath = join(harness.mediaDir, "src.bin");
  await writeFile(filePath, Buffer.from("hello"));
  await harness.adapter.sendMedia({
    chatId: "cidDM0001",
    kind: "file",
    path: filePath,
    name: "附件.txt",
    mime: "text/plain",
  });
  const upload = harness.restCalls.find((call) => call.path.startsWith("media/upload"));
  assert.ok(upload, "未上传媒体");
  assert.equal(upload.base, DINGTALK_OAPI_BASE);
  assert.match(upload.path, /access_token=token-1&type=file/);
  assert.ok(upload.body instanceof FormData);
  assert.deepEqual(harness.webhookCalls[0]?.body, {
    msgtype: "file",
    file: { media_id: "media-1", fileName: "附件.txt" },
  });

  await harness.adapter.sendMedia({ chatId: "cidDM0001", kind: "image", path: filePath });
  const imageUpload = harness.restCalls.filter((call) => call.path.startsWith("media/upload"))[1];
  assert.match(imageUpload?.path ?? "", /type=image/);
  assert.deepEqual(harness.webhookCalls[1]?.body, {
    msgtype: "image",
    image: { media_id: "media-1" },
  });
});

test("sendMedia 源文件缺失抛中文 UserFacingError", async (t) => {
  const harness = await makeHarness();
  t.after(cleanup(harness.dataDir, harness.mediaDir));
  t.after(() => harness.adapter.stop());
  await harness.adapter.start();
  await assert.rejects(
    harness.adapter.sendMedia({
      chatId: "cidDM0001",
      kind: "file",
      path: join(harness.mediaDir, "不存在的文件.bin"),
    }),
    (error: unknown) =>
      error instanceof UserFacingError && /无法读取待发送的文件/.test((error as Error).message),
  );
  assert.equal(harness.restCalls.some((call) => call.path.startsWith("media/upload")), false);
});

// ---------------------------------------------------------------- start/stop

test("start 缺少凭据抛 UserFacingError", async (t) => {
  const noSecret = await makeCtx({ secrets: {} });
  t.after(cleanup(noSecret.dataDir, noSecret.mediaDir));
  const dw = fakeDw();
  const adapterA = createAdapter(noSecret.ctx, fakeDeps(dw).deps);
  await assert.rejects(
    adapterA.start(),
    (error: unknown) => error instanceof UserFacingError && /Client Secret/.test((error as Error).message),
  );

  const noId = await makeCtx({
    config: { ...noSecret.ctx.config, clientId: "" } as DingTalkConnection,
  });
  t.after(cleanup(noId.dataDir, noId.mediaDir));
  const adapterB = createAdapter(noId.ctx, fakeDeps(dw).deps);
  await assert.rejects(
    adapterB.start(),
    (error: unknown) => error instanceof UserFacingError && /Client ID/.test((error as Error).message),
  );
  assert.equal(dw.state.connectCalls, 0);
});

test("start 连接失败走 onError 并按退避重连直到成功", async (t) => {
  const harness = await makeHarness();
  t.after(cleanup(harness.dataDir, harness.mediaDir));
  t.after(() => harness.adapter.stop());
  harness.dw.fail.connectError = new Error("gateway down");
  await harness.adapter.start();
  assert.ok(harness.errors.length >= 1);
  assert.equal(harness.errors[0]?.message, "gateway down");
  await waitFor(() => harness.dw.state.connectCalls >= 3);
  harness.dw.fail.connectError = undefined;
  await waitFor(() => harness.dw.state.connected === true);
  const attemptsAtSuccess = harness.dw.state.connectCalls;
  await delay(20);
  assert.equal(harness.dw.state.connectCalls, attemptsAtSuccess);
});

test("掉线由监视器重连，stop 清理 socket 与定时器", async (t) => {
  const harness = await makeHarness();
  t.after(cleanup(harness.dataDir, harness.mediaDir));
  await harness.adapter.start();
  await waitFor(() => harness.dw.state.connected === true);
  harness.dw.state.connected = false;
  await waitFor(() => harness.dw.state.connectCalls >= 2);
  await waitFor(() => harness.dw.state.connected === true);

  await harness.adapter.stop();
  assert.equal(harness.dw.state.connected, false);
  const callsAtStop = harness.dw.state.connectCalls;
  await delay(20);
  assert.equal(harness.dw.state.connectCalls, callsAtStop);
  await harness.adapter.stop();
  assert.equal(harness.messages.length, 0);
});

test("connect 同步 resolve 但 socket 未就绪时等待，不堆积连接", async (t) => {
  const dw = fakeDw();
  // 模拟真实 SDK：connect() 先返回，socket 稍后才 open。
  dw.client.connect = async () => {
    dw.state.connectCalls += 1;
  };
  let waitCalls = 0;
  const opened = Promise.withResolvers<void>();
  const release = opened.resolve;
  const harness = await makeHarness(
    {
      waitConnected: async () => {
        waitCalls += 1;
        await opened.promise;
        return dw.state.connected;
      },
    },
    {},
    dw,
  );
  t.after(cleanup(harness.dataDir, harness.mediaDir));
  t.after(() => harness.adapter.stop());
  await harness.adapter.start();
  // start 的 connect 已返回但 socket 未 open；ensureConnected 应停在等待函数上，
  // 而不是立刻重入 connect 造成连接风暴。
  await delay(30);
  assert.equal(dw.state.connectCalls, 2, "等待就绪期间不应重复 connect");
  assert.ok(waitCalls >= 1, "应经由等待函数确认 socket 就绪");
  dw.state.connected = true;
  release();
  await delay(20);
  assert.equal(dw.state.connectCalls, 2, "就绪后不应再 connect");
});

// ---------------------------------------------------------------- AI Card

test("AI 卡片流：create/append/finish 调用序列与渲染", async (t) => {
  const harness = await makeHarness();
  t.after(cleanup(harness.dataDir, harness.mediaDir));
  t.after(() => harness.adapter.stop());
  await harness.adapter.start();
  await deliver(harness.dw, groupFrame());
  await harness.adapter.settle();

  const { streamId } = await harness.adapter.streamStart("cidGRP0001");
  const create = harness.restCalls.find((call) => call.path === "v1.0/card/instances/createAndDeliver");
  assert.ok(create, "未创建卡片");
  const createBody = bodyRecord(create.body);
  assert.equal(createBody.openSpaceId, "dtv1.card//IM_GROUP.cidGRP0001");
  assert.equal(createBody.cardTemplateId, AI_CARD_TEMPLATE_ID);
  assert.equal(createBody.callbackType, "STREAM");
  assert.equal(field(createBody, "imGroupOpenDeliverModel").robotCode, CLIENT_ID);
  assert.equal(field(field(createBody, "cardData"), "cardParamMap").flowStatus, "2");
  const firstStream = harness.restCalls.find((call) => call.path === "v1.0/card/streaming");
  assert.ok(firstStream, "未启动卡片流式");
  assert.equal(firstStream.method, "PUT");
  const firstStreamBody = bodyRecord(firstStream.body);
  assert.equal(firstStreamBody.isFinalize, false);
  assert.equal(firstStreamBody.content, "正在思考…");

  await harness.adapter.streamAppend(streamId, "第一段\n第二段");
  const appended = bodyRecord(harness.restCalls.filter((call) => call.path === "v1.0/card/streaming")[1]?.body);
  assert.equal(appended.content, "第一段<br>第二段");

  const finished = await harness.adapter.streamFinish(streamId, "完成");
  assert.equal(finished.messageId, streamId);
  const finishStream = bodyRecord(harness.restCalls.filter((call) => call.path === "v1.0/card/streaming")[2]?.body);
  assert.equal(finishStream.isFinalize, true);
  const finalize = harness.restCalls.find((call) => call.path === "v1.0/card/instances");
  assert.ok(finalize, "未收口卡片状态");
  assert.equal(finalize.method, "PUT");
  const finalizeBody = bodyRecord(finalize.body);
  assert.equal(field(field(finalizeBody, "cardData"), "cardParamMap").flowStatus, "3");
  assert.equal(field(finalizeBody, "cardUpdateOptions").updateCardDataByKey, true);
  // 防回归：卡片三接口都必须携带访问令牌（缺失会全量 401）。
  const cardCalls = harness.restCalls.filter((call) => call.path.startsWith("v1.0/card/"));
  assert.ok(cardCalls.length >= 5, `卡片调用数不足：${cardCalls.length}`);
  for (const call of cardCalls) {
    assert.equal(call.token, "token-1", `卡片调用缺令牌：${call.path}`);
  }
});

test("AI 卡片创建失败降级：streamStart 不抛错，finish 直接 sendText 送达", async (t) => {
  const harness = await makeHarness({
    rest: async (path, body, base, method, token) => {
      if (path.startsWith("v1.0/card/instances/createAndDeliver")) {
        throw new DingTalkApiError("卡片服务不可用。", { status: 500 });
      }
      if (path.startsWith("v1.0/oauth2/accessToken")) return { accessToken: "token-1", expireIn: 7200 };
      return {};
    },
  });
  t.after(cleanup(harness.dataDir, harness.mediaDir));
  t.after(() => harness.adapter.stop());
  await harness.adapter.start();
  await deliver(harness.dw, groupFrame());
  await harness.adapter.settle();

  // 降级：不抛错（抛错会中断引擎投递分支，最终回复永远发不出去）。
  const { streamId } = await harness.adapter.streamStart("cidGRP0001");
  assert.match(harness.logs.join("\n"), /AI 卡片不可用/);
  await harness.adapter.streamAppend(streamId, "进度行"); // 降级流 no-op，不抛错
  const result = await harness.adapter.streamFinish(streamId, "最终答案");
  const webhook = harness.webhookCalls.at(-1);
  assert.ok(webhook, "最终回复未发送");
  assert.equal(sentText(webhook.body), "最终答案");
  assert.equal(result.messageId, undefined);
  await assert.rejects(harness.adapter.streamAppend("missing", "x"), /未知的钉钉卡片流/);
});

test("AI 卡片收口失败不残留流记录（finally 清理）", async (t) => {
  const harness = await makeHarness({
    rest: async (path, body, base, method, token) => {
      if (path.startsWith("v1.0/card/streaming") && bodyRecord(body).isFinalize === true) {
        throw new DingTalkApiError("收口失败。", { status: 500 });
      }
      if (path.startsWith("v1.0/oauth2/accessToken")) return { accessToken: "token-1", expireIn: 7200 };
      return {};
    },
  });
  t.after(cleanup(harness.dataDir, harness.mediaDir));
  t.after(() => harness.adapter.stop());
  await harness.adapter.start();
  await deliver(harness.dw, groupFrame());
  await harness.adapter.settle();

  const { streamId } = await harness.adapter.streamStart("cidGRP0001");
  await assert.rejects(harness.adapter.streamFinish(streamId, "完成"), /收口失败/);
  // 流记录已被 finally 清理：再次 finish 视为未知流，而非复活旧卡片。
  await assert.rejects(harness.adapter.streamFinish(streamId, "重试"), /未知的钉钉卡片流/);
});

test("卡片请求业务码 403 同样退避重试一次", async (t) => {
  let cardCreates = 0;
  const harness = await makeHarness({
    cardRetryDelayMs: 1,
    rest: async (path, body, base, method, token) => {
      if (path.startsWith("v1.0/card/instances/createAndDeliver")) {
        cardCreates += 1;
        if (cardCreates === 1) {
          // 生产 rest 层对 errcode≠0 抛 DingTalkApiError（providerCode=errcode）。
          throw new DingTalkApiError("钉钉服务拒绝了请求：flow status reject。", {
            status: 200,
            providerCode: 403,
          });
        }
        return { errcode: 0 };
      }
      if (path.startsWith("v1.0/oauth2/accessToken")) return { accessToken: "token-1", expireIn: 7200 };
      return {};
    },
  });
  t.after(cleanup(harness.dataDir, harness.mediaDir));
  t.after(() => harness.adapter.stop());
  await harness.adapter.start();
  await deliver(harness.dw, groupFrame());
  await harness.adapter.settle();

  const { streamId } = await harness.adapter.streamStart("cidGRP0001");
  assert.equal(cardCreates, 2, "providerCode 403 应触发一次重试");
  assert.match(streamId, /^paseo_/);
});

// ---------------------------------------------------------------- misc

test("testConnection 校验凭据", async (t) => {
  const harness = await makeHarness();
  t.after(cleanup(harness.dataDir, harness.mediaDir));
  t.after(() => harness.adapter.stop());
  assert.deepEqual(await harness.adapter.testConnection(), { ok: true, detail: "钉钉凭据校验通过。" });

  const broken = await makeHarness({
    rest: async () => {
      throw new DingTalkApiError("钉钉服务请求失败（HTTP 403）。", { status: 403 });
    },
  });
  t.after(cleanup(broken.dataDir, broken.mediaDir));
  t.after(() => broken.adapter.stop());
  const result = await broken.adapter.testConnection();
  assert.equal(result.ok, false);
  assert.match(result.detail ?? "", /403/);
});

test("cardMarkdown：围栏外单换行转 <br>，围栏与列表保留", () => {
  assert.equal(cardMarkdown("第一段\n第二段"), "第一段<br>第二段");
  assert.equal(cardMarkdown("第一段\n\n第二段"), "第一段\n\n第二段");
  assert.equal(cardMarkdown("```\ncode\n```"), "```\ncode\n```");
  assert.equal(cardMarkdown("- a\n- b"), "- a\n- b");
  assert.equal(cardMarkdown("标题\n## 小节"), "标题\n## 小节");
});
