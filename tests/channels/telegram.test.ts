/** Tests for the Telegram channel adapter: normalization, HTML rendering,
 * chunked sending, rate limits, polling lifecycle and start error paths.
 * All transports are faked; no real network access. */
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test, type TestContext } from "node:test";
import { setTimeout as delay } from "node:timers/promises";

import { UserFacingError } from "../../shared/errors.ts";
import type { NormalizedMessage } from "../../shared/model.ts";
import type { ImSettings } from "../../shared/settings.ts";
import {
  configureTelegram,
  createAdapter,
  definition,
  isRecord,
  normalizeUpdate,
  type NormalizeOptions,
  type TelegramApi,
  type TelegramConnection,
  type TelegramDeps,
} from "../../server/channels/telegram/index.ts";
import { renderTelegramHtml } from "../../server/channels/telegram/html.ts";
import type { AdapterContext, ChannelAdapter } from "../../server/channels/types.ts";

const BOT_ID = "123456789";
const BOT_USERNAME = "paseo_test_bot";
const TOKEN = "123456789:AAFakeTestTokenForUnitTests0000000";
const CONNECTION_ID = "6f1c2a34-9b8e-4d5f-8a7c-1e2d3f4a5b6c";

const TEST_SETTINGS: ImSettings = {
  imageMaxDownloadMb: 30,
  imageMaxSingleMb: 5,
  imageMaxTotalMb: 100,
  imageMaxCount: 20,
  busyMode: "queue",
  attachmentTtlHours: 168,
  deferTimeoutMinutes: 30,
};

// ------------------------------------------------------------------- fixtures

function dmTextUpdate(updateId: number, messageId: number, text: string): unknown {
  return {
    update_id: updateId,
    message: {
      message_id: messageId,
      date: 1_700_000_000,
      chat: { id: 4242, type: "private", first_name: "Alice" },
      from: { id: 4242, is_bot: false, first_name: "Alice", username: "alice" },
      text,
    },
  };
}

const SUPERGROUP = { id: -1_001_234_567_890, type: "supergroup", title: "测试群" };

function groupMessage(extra: Record<string, unknown>): unknown {
  return {
    message_id: 12,
    date: 1_700_000_001,
    chat: SUPERGROUP,
    from: { id: 7, is_bot: false, first_name: "Bob" },
    ...extra,
  };
}

function normalizeOptions(
  overrides: Partial<Omit<NormalizeOptions, "connectionId">> = {},
): NormalizeOptions {
  return {
    botId: BOT_ID,
    botUsername: BOT_USERNAME,
    connectionId: CONNECTION_ID,
    ...overrides,
  };
}

// ------------------------------------------------------------------- recorder

type Handler = (payload: Record<string, unknown>) => unknown;

interface Recorder {
  deps: TelegramDeps;
  calls: Array<{ method: string; payload: unknown }>;
  sleeps: number[];
  downloads: string[];
  queue(method: string, ...handlers: Handler[]): void;
  feedPollUpdates(updates: unknown): void;
}

function makeRecorder(): Recorder {
  const calls: Array<{ method: string; payload: unknown }> = [];
  const sleeps: number[] = [];
  const downloads: string[] = [];
  const handlers = new Map<string, Handler[]>();
  const pollQueue: unknown[] = [];
  let pendingPolls: Array<(value: unknown) => void> = [];

  const isPollCall = (payload: unknown): boolean => isRecord(payload) && payload.timeout === 25;

  const api: TelegramApi = {
    call: async (method, payload) => {
      calls.push({ method, payload });
      const handler = handlers.get(method)?.shift();
      if (handler) return await handler(isRecord(payload) ? payload : {});
      if (method === "getUpdates" && isPollCall(payload)) {
        if (pollQueue.length > 0) return pollQueue.shift();
        const { promise, resolve } = Promise.withResolvers<unknown>();
        pendingPolls.push(resolve);
        return await promise;
      }
      throw new Error(`意外的 Telegram API 调用：${method}`);
    },
  };

  const deps: TelegramDeps = {
    api,
    download: async (fileId: string) => {
      downloads.push(fileId);
      return new TextEncoder().encode(`bytes-of-${fileId}`);
    },
    sleep: async (ms: number) => {
      sleeps.push(ms);
    },
    abort: () => {
      const resolvers = pendingPolls;
      pendingPolls = [];
      for (const resolve of resolvers) resolve([]);
    },
  };

  return {
    deps,
    calls,
    sleeps,
    downloads,
    queue: (method, ...list) => {
      const existing = handlers.get(method) ?? [];
      existing.push(...list);
      handlers.set(method, existing);
    },
    feedPollUpdates: (updates) => {
      const resolve = pendingPolls.shift();
      if (resolve) resolve(updates);
      else pollQueue.push(updates);
    },
  };
}

// ----------------------------------------------------------------- ctx helper

interface CtxEnv {
  ctx: AdapterContext<TelegramConnection>;
  messages: NormalizedMessage[];
  errors: Error[];
  logs: string[];
  dataDir: string;
  mediaDir: string;
  controller: AbortController;
  cleanup(): Promise<void>;
}

async function makeCtx(overrides: Partial<AdapterContext<TelegramConnection>> = {}): Promise<CtxEnv> {
  const messages: NormalizedMessage[] = [];
  const errors: Error[] = [];
  const logs: string[] = [];
  const dataDir = await mkdtemp(join(tmpdir(), "paseo-telegram-data-"));
  const mediaDir = await mkdtemp(join(tmpdir(), "paseo-telegram-media-"));
  const controller = new AbortController();
  const ctx: AdapterContext<TelegramConnection> = {
    connectionId: CONNECTION_ID,
    config: {
      channel: "telegram",
      id: CONNECTION_ID,
      name: "测试连接",
      enabled: true,
      target: null,
      policy: {
        groupAdmission: "approval",
        directMessages: "operators",
        groupUsers: "operators",
        operators: [],
        invocation: "mentions",
      },
      bindings: {},
      revision: 0,
      username: BOT_USERNAME,
      botUserId: BOT_ID,
    },
    secrets: { botToken: TOKEN },
    dataDir,
    mediaDir,
    signal: controller.signal,
    settings: async () => TEST_SETTINGS,
    log: (level, message) => logs.push(`${level}: ${message}`),
    onMessage: (message) => messages.push(message),
    onGroups: () => {},
    onError: (error) => errors.push(error),
    ...overrides,
  };
  return {
    ctx,
    messages,
    errors,
    logs,
    dataDir,
    mediaDir,
    controller,
    cleanup: async () => {
      await rm(dataDir, { recursive: true, force: true });
      await rm(mediaDir, { recursive: true, force: true });
    },
  };
}

function registerConnectHandlers(recorder: Recorder): void {
  recorder.queue("getMe", () => ({
    id: Number(BOT_ID),
    is_bot: true,
    username: BOT_USERNAME,
    first_name: "Paseo",
  }));
  recorder.queue("getWebhookInfo", () => ({ url: "" }));
  // 游标初始化（offset:-1）返回空；首个 25s 长轮询由各测试自行排队或挂起。
  recorder.queue("getUpdates", () => []);
  recorder.queue("setMyCommands", () => true);
}

async function startConnected(
  t: TestContext,
  recorder: Recorder,
  env: CtxEnv,
  beforeStart?: (recorder: Recorder) => void,
): Promise<ChannelAdapter> {
  registerConnectHandlers(recorder);
  beforeStart?.(recorder);
  const adapter = createAdapter(env.ctx, recorder.deps);
  t.after(() => adapter.stop());
  t.after(() => env.cleanup());
  await adapter.start();
  return adapter;
}

async function waitFor(predicate: () => boolean, timeoutMs = 3000): Promise<void> {
  const start = Date.now();
  while (!predicate()) {
    if (Date.now() - start > timeoutMs) throw new Error("waitFor 超时");
    await delay(5);
  }
}

function callsOf(recorder: Recorder, method: string): Array<Record<string, unknown>> {
  return recorder.calls
    .filter((entry) => entry.method === method)
    .map((entry) => (isRecord(entry.payload) ? entry.payload : {}));
}

// ------------------------------------------------------------------ normalize

test("normalize：私聊文本 → dm，mention 视为已点名", async () => {
  const message = await normalizeUpdate(dmTextUpdate(100, 11, "你好，帮我看看日志"), normalizeOptions());
  assert.ok(message);
  assert.equal(message.channel, "telegram");
  assert.equal(message.connectionId, CONNECTION_ID);
  assert.equal(message.kind, "dm");
  assert.equal(message.chatId, "4242");
  assert.equal(message.senderId, "4242");
  assert.equal(message.senderName, "Alice");
  assert.equal(message.messageId, "11");
  assert.equal(message.timestamp, 1_700_000_000_000);
  assert.equal(message.text, "你好，帮我看看日志");
  assert.deepEqual(message.trigger, { mention: true, replyToBot: false, command: false });
});

test("normalize：群聊 /cmd@bot 命中 → command+mention，剥离 @用户名，保留 topic", async () => {
  const update = groupMessage({
    message_thread_id: 7,
    text: "/status@paseo_test_bot 帮我查下状态",
    entities: [{ offset: 0, length: "/status@paseo_test_bot".length, type: "bot_command" }],
  });
  const message = await normalizeUpdate({ update_id: 101, message: update }, normalizeOptions());
  assert.ok(message);
  assert.equal(message.kind, "group");
  assert.equal(message.chatId, "-1001234567890");
  assert.equal(message.threadId, "7");
  assert.equal(message.text, "/status 帮我查下状态");
  assert.deepEqual(message.trigger, { mention: true, replyToBot: false, command: true });
});

test("normalize：群聊 @mention 实体命中 → mention；未命中 → 非点名", async () => {
  const mentioned = await normalizeUpdate(
    {
      update_id: 102,
      message: groupMessage({
        text: "@paseo_test_bot 在吗",
        entities: [{ offset: 0, length: BOT_USERNAME.length + 1, type: "mention" }],
      }),
    },
    normalizeOptions(),
  );
  assert.ok(mentioned);
  assert.equal(mentioned.text, "在吗");
  assert.equal(mentioned.trigger.mention, true);

  const unqualified = await normalizeUpdate(
    {
      update_id: 103,
      message: groupMessage({
        text: "/status hello",
        entities: [{ offset: 0, length: "/status".length, type: "bot_command" }],
      }),
    },
    normalizeOptions(),
  );
  assert.ok(unqualified);
  assert.equal(unqualified.trigger.mention, false);
  assert.equal(unqualified.trigger.command, false);
});

test("normalize：回复机器人消息 → replyToBot + replyTo 引用", async () => {
  const update = groupMessage({
    message_id: 13,
    text: "继续",
    reply_to_message: {
      message_id: 5,
      date: 1_699_999_000,
      chat: SUPERGROUP,
      from: { id: Number(BOT_ID), is_bot: true, username: BOT_USERNAME },
      text: "上一条回答",
    },
  });
  const message = await normalizeUpdate({ update_id: 104, message: update }, normalizeOptions());
  assert.ok(message);
  assert.equal(message.trigger.replyToBot, true);
  assert.equal(message.trigger.mention, true);
  assert.deepEqual(message.replyTo, { id: "5", text: "上一条回答", sender: BOT_ID });
});

test("normalize：photo 取最大 file_id，document 按 mime/name，caption 作为文本", async () => {
  const loads: Array<[string, string]> = [];
  const options = normalizeOptions({
    loadMedia: async (fileId, name) => {
      loads.push([fileId, name]);
      return `/media/${name}`;
    },
  });
  const update = groupMessage({
    message_id: 14,
    caption: "看这张图",
    photo: [
      { file_id: "photo_small", file_unique_id: "s1", width: 90, height: 90, file_size: 100 },
      { file_id: "photo_big", file_unique_id: "b1", width: 1280, height: 720, file_size: 5000 },
    ],
    document: {
      file_id: "doc1",
      file_unique_id: "d1",
      file_name: "报告.pdf",
      mime_type: "application/pdf",
      file_size: 1024,
    },
  });
  const message = await normalizeUpdate({ update_id: 105, message: update }, options);
  assert.ok(message);
  assert.equal(message.text, "看这张图");
  assert.deepEqual(loads, [
    ["photo_big", "photo-14.jpg"],
    ["doc1", "报告.pdf"],
  ]);
  assert.equal(message.media?.length, 2);
  assert.deepEqual(message.media?.[0], {
    kind: "image",
    path: "/media/photo-14.jpg",
    mime: "image/jpeg",
    name: "photo-14.jpg",
  });
  assert.deepEqual(message.media?.[1], {
    kind: "file",
    path: "/media/报告.pdf",
    mime: "application/pdf",
    name: "报告.pdf",
  });
});

test("normalize：机器人自身消息、无文本无媒体噪音、非目标 chat 类型 → 不产出", async () => {
  const options = normalizeOptions();
  const ownBot = await normalizeUpdate(
    {
      update_id: 106,
      message: groupMessage({
        from: { id: Number(BOT_ID), is_bot: true, username: BOT_USERNAME },
        text: "我自己说的话",
      }),
    },
    options,
  );
  assert.equal(ownBot, null);

  const serviceNoise = await normalizeUpdate(
    {
      update_id: 107,
      message: groupMessage({
        message_id: 16,
        new_chat_members: [{ id: 9, is_bot: false, first_name: "新成员" }],
      }),
    },
    options,
  );
  assert.equal(serviceNoise, null);

  const channelPost = await normalizeUpdate(
    {
      update_id: 108,
      channel_post: { message_id: 17, date: 1_700_000_000, chat: { id: -1_000, type: "channel" }, text: "公告" },
    },
    options,
  );
  assert.equal(channelPost, null);

  const callbackWithoutData = await normalizeUpdate(
    {
      update_id: 109,
      callback_query: {
        id: "cbq",
        from: { id: 4242, is_bot: false, first_name: "Alice" },
        message: { message_id: 77, date: 1_700_000_000, chat: { id: 4242, type: "private" } },
      },
    },
    options,
  );
  assert.equal(callbackWithoutData, null);
});

test("normalize：callback_query 归一为 addressed 文本", async () => {
  const message = await normalizeUpdate(
    {
      update_id: 110,
      callback_query: {
        id: "cbq1",
        from: { id: 4242, is_bot: false, first_name: "Alice", username: "alice" },
        message: {
          message_id: 77,
          date: 1_700_000_002,
          chat: { id: 4242, type: "private" },
          text: "选择操作",
        },
        data: "/new",
      },
    },
    normalizeOptions(),
  );
  assert.ok(message);
  assert.equal(message.kind, "dm");
  assert.equal(message.chatId, "4242");
  assert.equal(message.messageId, "cbq1"); // messageId 优先用全局唯一的 callback.id
  assert.equal(message.timestamp, 1_700_000_002_000);
  assert.equal(message.text, "/new");
  assert.deepEqual(message.trigger, { mention: true, replyToBot: false, command: true });
});

test("normalize：callback_query 的 messageId 取 callback.id，多用户点击不被去重吞", async () => {
  const base = {
    from: { id: 4242, is_bot: false, first_name: "Alice" },
    message: { message_id: 77, date: 1_700_000_002, chat: { id: 4242, type: "private" }, text: "选择操作" },
    data: "/new",
  };
  const first = await normalizeUpdate({ update_id: 111, callback_query: { id: "cbq-a", ...base } }, normalizeOptions());
  const second = await normalizeUpdate({ update_id: 112, callback_query: { id: "cbq-b", ...base } }, normalizeOptions());
  assert.ok(first && second);
  assert.equal(first.messageId, "cbq-a");
  assert.equal(second.messageId, "cbq-b");
  assert.notEqual(first.messageId, second.messageId);

  // callback.id 缺失时回退 message_id
  const fallback = await normalizeUpdate(
    { update_id: 113, callback_query: { from: base.from, message: base.message, data: "/new" } },
    normalizeOptions(),
  );
  assert.equal(fallback?.messageId, "77");
});

test("normalize：群聊仅有 @bot（剥离后无文本无媒体）→ 视为噪音不产出", async () => {
  const bareMention = await normalizeUpdate(
    {
      update_id: 114,
      message: groupMessage({
        text: "@paseo_test_bot",
        entities: [{ offset: 0, length: BOT_USERNAME.length + 1, type: "mention" }],
      }),
    },
    normalizeOptions(),
  );
  assert.equal(bareMention, null);

  // 带媒体时保留消息，但剥离后无文本则不带 text
  const withMedia = await normalizeUpdate(
    {
      update_id: 115,
      message: groupMessage({
        caption: "@paseo_test_bot",
        entities: [{ offset: 0, length: BOT_USERNAME.length + 1, type: "mention" }],
        photo: [{ file_id: "photo_x", file_unique_id: "x", width: 10, height: 10 }],
      }),
    },
    normalizeOptions({ loadMedia: async () => "/media/photo-x.jpg" }),
  );
  assert.ok(withMedia);
  assert.equal(withMedia.text, undefined);
  assert.equal(withMedia.media?.length, 1);
});

// -------------------------------------------------------------------- render

test("renderTelegramHtml：粗体/斜体/行内代码/链接", () => {
  const html = renderTelegramHtml("**粗** *斜* `c` [页](https://example.com)");
  assert.equal(html, '<b>粗</b> <i>斜</i> <code>c</code> <a href="https://example.com">页</a>');
});

test("renderTelegramHtml：预块保留语言标注", () => {
  const html = renderTelegramHtml("```js\nconst a = 1;\n```");
  assert.equal(html, '<pre><code class="language-js">const a = 1;</code></pre>');
});

test("renderTelegramHtml：HTML 特殊字符与原始 HTML 一律转义", () => {
  assert.equal(renderTelegramHtml("1 < 2 & 3 > 2"), "1 &lt; 2 &amp; 3 &gt; 2");
  assert.equal(renderTelegramHtml("<b>hi</b>"), "&lt;b&gt;hi&lt;/b&gt;");
});

test("renderTelegramHtml：标题加粗、列表、引用", () => {
  assert.equal(renderTelegramHtml("## 标题"), "<b>标题</b>");
  assert.equal(renderTelegramHtml("- 一\n- 二"), "• 一\n• 二");
  assert.equal(renderTelegramHtml("> 引用内容"), "<blockquote>引用内容</blockquote>");
});

test("renderTelegramHtml：非 http 链接不生成 <a>", () => {
  const html = renderTelegramHtml("[点我](javascript:alert(1))");
  assert.ok(!html.includes("<a "));
  assert.ok(html.includes("点我"));
});

test("renderTelegramHtml：表格渲染表头（加粗）且数据行不丢列", () => {
  const html = renderTelegramHtml("| 名称 | 数量 |\n| --- | --- |\n| 苹果 | 3 |\n| 香蕉 | 5 |");
  assert.ok(html.includes("<b>名称 · 数量</b>"), "表头行应加粗渲染");
  assert.ok(html.includes("苹果 · 3"), "数据行不应丢失");
  assert.ok(html.includes("香蕉 · 5"));
});

// ----------------------------------------------------- lifecycle & transport

test("start：缺 Token / Token 格式错误 → UserFacingError", async (t) => {
  const missing = await makeCtx({ secrets: {} });
  t.after(() => missing.cleanup());
  const missingAdapter = createAdapter(missing.ctx, makeRecorder().deps);
  await assert.rejects(
    () => missingAdapter.start(),
    (error: unknown) => error instanceof UserFacingError && error.message.includes("Bot Token"),
  );

  const malformed = await makeCtx({ secrets: { botToken: "not-a-token" } });
  t.after(() => malformed.cleanup());
  const malformedAdapter = createAdapter(malformed.ctx, makeRecorder().deps);
  await assert.rejects(
    () => malformedAdapter.start(),
    (error: unknown) => error instanceof UserFacingError && error.message.includes("格式不正确"),
  );
});

test("start：getMe 401 → UserFacingError 提示重新获取 Token", async (t) => {
  const recorder = makeRecorder();
  recorder.queue("getMe", () => {
    throw Object.assign(new Error("Unauthorized"), { status: 401 });
  });
  const env = await makeCtx();
  t.after(() => env.cleanup());
  const adapter = createAdapter(env.ctx, recorder.deps);
  await assert.rejects(
    () => adapter.start(),
    (error: unknown) => error instanceof UserFacingError && error.message.includes("401"),
  );
  assert.equal(env.errors.length, 0); // 凭据问题直接抛出，不走 onError
});

test("start：已设置 Webhook → UserFacingError 拒绝启动", async (t) => {
  const recorder = makeRecorder();
  recorder.queue("getMe", () => ({ id: Number(BOT_ID), is_bot: true, username: BOT_USERNAME }));
  recorder.queue("getWebhookInfo", () => ({ url: "https://example.com/hook" }));
  const env = await makeCtx();
  t.after(() => env.cleanup());
  const adapter = createAdapter(env.ctx, recorder.deps);
  await assert.rejects(
    () => adapter.start(),
    (error: unknown) => error instanceof UserFacingError && error.message.includes("Webhook"),
  );
});

test("start：瞬时错误退避重试后成功连接", async (t) => {
  const recorder = makeRecorder();
  recorder.queue("getMe", () => {
    throw new TypeError("fetch failed");
  });
  recorder.queue("getMe", () => ({ id: Number(BOT_ID), is_bot: true, username: BOT_USERNAME }));
  recorder.queue("getWebhookInfo", () => ({ url: "" }));
  recorder.queue("getUpdates", () => []);
  recorder.queue("setMyCommands", () => true);
  const env = await makeCtx();
  t.after(() => env.cleanup());
  const adapter = createAdapter(env.ctx, recorder.deps);
  t.after(() => adapter.stop());
  await adapter.start();
  assert.deepEqual(recorder.sleeps, [1_000]); // 首档退避
  assert.equal(env.errors.length, 1);
});

test("收发主链路：批量更新去重入库、媒体落盘、发送与编辑", async (t) => {
  const recorder = makeRecorder();
  const batch = () => [
    dmTextUpdate(200, 11, "第一条"),
    // 机器人自身消息：跳过
    {
      update_id: 201,
      message: groupMessage({
        message_id: 15,
        from: { id: Number(BOT_ID), is_bot: true, username: BOT_USERNAME },
        text: "自己",
      }),
    },
    // 同批次重复 update_id：去重
    dmTextUpdate(200, 11, "第一条"),
    // 无文本无媒体噪音：不产出
    { update_id: 203, message: groupMessage({ message_id: 18, pinned_chat_message: {} }) },
    // 媒体消息：下载落盘
    {
      update_id: 204,
      message: groupMessage({
        message_id: 19,
        photo: [{ file_id: "photo_big", file_unique_id: "b", width: 1280, height: 720 }],
      }),
    },
    // callback_query：addressed 文本
    {
      update_id: 205,
      callback_query: {
        id: "cbq1",
        from: { id: 4242, is_bot: false, first_name: "Alice" },
        message: { message_id: 77, date: 1_700_000_000, chat: { id: 4242, type: "private" } },
        data: "/status",
      },
    },
  ];
  const env = await makeCtx();
  const adapter = await startConnected(t, recorder, env, (r) => r.queue("getUpdates", batch));

  await waitFor(() => env.messages.length === 3);
  assert.equal(env.messages[0]?.text, "第一条");
  assert.equal(env.messages[1]?.media?.[0]?.kind, "image");
  const storedPath = env.messages[1]?.media?.[0]?.path ?? "";
  assert.ok(storedPath.startsWith(env.mediaDir));
  assert.equal(await readFile(storedPath, "utf8"), "bytes-of-photo_big");
  assert.deepEqual(recorder.downloads, ["photo_big"]);
  assert.equal(env.messages[2]?.text, "/status");

  // 游标持久化：cursor = 最后一个 update_id + 1，seen 覆盖本批全部 update_id
  // （落盘是异步的：轮询文件直到游标推进完成，避免与 emit 的时序竞态）
  const statePath = join(env.dataDir, "telegram-state.json");
  let state: { cursor: number; seen: Record<string, number> } | undefined;
  await waitFor(() => {
    try {
      const raw = readFileSync(statePath, "utf8");
      const parsed = JSON.parse(raw) as { cursor: number; seen: Record<string, number> };
      if (parsed.cursor === 206) {
        state = parsed;
        return true;
      }
      return false;
    } catch {
      return false;
    }
  });
  assert.ok(state?.seen["200"] !== undefined);
  assert.ok(state?.seen["205"] !== undefined);

  // 重复投喂旧 update：seen 去重，不重复 emit
  recorder.feedPollUpdates([dmTextUpdate(200, 11, "第一条")]);
  await delay(20);
  assert.equal(env.messages.length, 3);

  // 发送：HTML 渲染 + 分块 + messageId
  recorder.queue("sendMessage", () => ({ message_id: 55 }));
  const sent = await adapter.sendText({ chatId: "4242", text: "**你好**" });
  assert.equal(sent.messageId, "55");
  const sendMessageCalls = callsOf(recorder, "sendMessage");
  assert.equal(sendMessageCalls.length, 1);
  assert.deepEqual(sendMessageCalls[0], {
    chat_id: "4242",
    text: "<b>你好</b>",
    parse_mode: "HTML",
    link_preview_options: { is_disabled: true },
  });

  // 回复参数
  recorder.queue("sendMessage", () => ({ message_id: 56 }));
  await adapter.sendText({ chatId: "4242", text: "回复你", replyToId: "11" });
  const replyPayload = callsOf(recorder, "sendMessage").at(-1);
  assert.deepEqual(replyPayload?.reply_parameters, { message_id: 11, allow_sending_without_reply: true });

  // 编辑：进度占位改写 + 幂等编辑视为成功
  const editMessage = adapter.editMessage?.bind(adapter);
  const sendTyping = adapter.sendTyping?.bind(adapter);
  const sendMedia = adapter.sendMedia?.bind(adapter);
  assert.ok(editMessage);
  assert.ok(sendTyping);
  assert.ok(sendMedia);
  recorder.queue("editMessageText", () => true);
  await editMessage("55", "4242", "**新内容**");
  const editPayload = callsOf(recorder, "editMessageText").at(-1);
  assert.equal(editPayload?.message_id, 55);
  assert.equal(editPayload?.text, "<b>新内容</b>");
  recorder.queue("editMessageText", () => {
    throw new Error("Bad Request: message is not modified");
  });
  await editMessage("55", "4242", "再次编辑");
  assert.equal(env.errors.length, 0);

  // typing
  recorder.queue("sendChatAction", () => true);
  await sendTyping("4242");
  assert.deepEqual(callsOf(recorder, "sendChatAction").at(-1), { chat_id: "4242", action: "typing" });

  // 媒体发送：multipart 字段
  const mediaPath = join(env.mediaDir, "outgoing.png");
  await writeFile(mediaPath, Buffer.from([1, 2, 3]));
  recorder.queue("sendPhoto", () => ({ message_id: 60 }));
  const mediaSent = await sendMedia({
    chatId: "4242",
    kind: "image",
    path: mediaPath,
    mime: "image/png",
    name: "pic.png",
    caption: "**图**",
  });
  assert.equal(mediaSent.messageId, "60");
  const photoPayload = recorder.calls.find((entry) => entry.method === "sendPhoto")?.payload;
  assert.ok(photoPayload instanceof FormData);
  const form = photoPayload;
  assert.equal(form.get("chat_id"), "4242");
  assert.equal(form.get("caption"), "<b>图</b>");
  assert.equal(form.get("parse_mode"), "HTML");
  const photo = form.get("photo");
  assert.ok(photo instanceof File);
  assert.equal(photo.name, "pic.png");
});

test("sendText：超长文本按 maxChunkChars 分块，段落优先", async (t) => {
  const recorder = makeRecorder();
  const env = await makeCtx();
  const adapter = await startConnected(t, recorder, env);
  recorder.queue("sendMessage", () => ({ message_id: 1 }));
  recorder.queue("sendMessage", () => ({ message_id: 2 }));

  await adapter.sendText({ chatId: "4242", text: "a".repeat(2_000) + "\n\n" + "b".repeat(2_000) });
  const messages = callsOf(recorder, "sendMessage");
  assert.equal(messages.length, 2);
  assert.equal(messages[0]?.text, "a".repeat(2_000));
  assert.equal(messages[1]?.text, "b".repeat(2_000));

  recorder.queue("sendMessage", () => ({ message_id: 3 }));
  recorder.queue("sendMessage", () => ({ message_id: 4 }));
  await adapter.sendText({ chatId: "4242", text: "c".repeat(5_000) });
  const hardSplit = callsOf(recorder, "sendMessage").slice(-2);
  assert.equal(hardSplit[0]?.text, "c".repeat(adapter.capabilities.maxChunkChars));
  assert.equal(hardSplit[1]?.text, "c".repeat(5_000 - adapter.capabilities.maxChunkChars));
});

test("sendText：429 按 retry_after 暂停该 chat 后重试；同 chat 强制 1100ms 间隔", async (t) => {
  const recorder = makeRecorder();
  const env = await makeCtx();
  const adapter = await startConnected(t, recorder, env);
  recorder.queue("sendMessage", () => {
    throw Object.assign(new Error("Too Many Requests: retry after 2"), { status: 429, retryAfter: 2 });
  });
  recorder.queue("sendMessage", () => ({ message_id: 10 }));
  recorder.queue("sendMessage", () => ({ message_id: 11 }));

  await adapter.sendText({ chatId: "4242", text: "一" });
  await adapter.sendText({ chatId: "4242", text: "二" });
  assert.equal(callsOf(recorder, "sendMessage").length, 3); // 首次失败 + 重试成功 + 第二条
  assert.ok(recorder.sleeps.some((ms) => ms >= 1_990), "应等待 retry_after 暂停");
  assert.ok(
    recorder.sleeps.some((ms) => ms >= 1_090),
    "第二次发送应等待每 chat 1100ms 间隔（只约束下限，调度抖动允许超时）",
  );
});

test("sendText：429 按 chat 隔离暂停，A 群被限不阻塞 B 私聊；群聊间隔放宽 3000ms", async (t) => {
  const recorder = makeRecorder();
  const env = await makeCtx();
  const adapter = await startConnected(t, recorder, env);
  recorder.queue("sendMessage", () => {
    throw Object.assign(new Error("Too Many Requests: retry after 30"), { status: 429, retryAfter: 30 });
  });
  recorder.queue("sendMessage", () => ({ message_id: 20 }));
  recorder.queue("sendMessage", () => ({ message_id: 21 }));
  recorder.queue("sendMessage", () => ({ message_id: 22 }));

  const groupSend = adapter.sendText({ chatId: "-100999", text: "群消息" });
  const dmSend = adapter.sendText({ chatId: "4242", text: "私聊消息" });
  await Promise.all([groupSend, dmSend]);
  const sends = callsOf(recorder, "sendMessage");
  assert.equal(sends.length, 3); // A 群首试 429 + 重试成功 + B 私聊
  assert.ok(sends.some((payload) => payload.chat_id === "4242"), "B 私聊应已发出");
  assert.ok(sends.some((payload) => payload.chat_id === "-100999"));
  // 暂停只落在 A 群：仅一条 ~30s 等待；B 私聊零等待（旧全局暂停会多出一条 ~30s）
  assert.equal(recorder.sleeps.length, 1);
  assert.ok(recorder.sleeps[0] >= 29_000);

  // 另一个群的两连发：第二次等待 3000ms 群聊间隔（私聊口径为 1100ms）
  recorder.queue("sendMessage", () => ({ message_id: 22 }));
  recorder.queue("sendMessage", () => ({ message_id: 23 }));
  await adapter.sendText({ chatId: "-100888", text: "群一" });
  await adapter.sendText({ chatId: "-100888", text: "群二" });
  assert.equal(recorder.sleeps.length, 2);
  assert.ok(
    recorder.sleeps[1] >= 2_990 && recorder.sleeps[1] < 3_100,
    "群聊发送间隔应为 3000ms",
  );
});

test("媒体下载：瞬时失败按退避重试，重试成功后消息正常投递", async (t) => {
  const recorder = makeRecorder();
  let downloadAttempts = 0;
  recorder.deps.download = async (fileId: string) => {
    downloadAttempts += 1;
    if (downloadAttempts === 1) throw new TypeError("fetch failed");
    return new TextEncoder().encode(`bytes-of-${fileId}`);
  };
  const env = await makeCtx();
  await startConnected(t, recorder, env, (r) =>
    r.queue("getUpdates", () => [
      {
        update_id: 400,
        message: groupMessage({
          message_id: 30,
          photo: [{ file_id: "photo_retry", file_unique_id: "r", width: 10, height: 10 }],
        }),
      },
    ]),
  );
  await waitFor(() => env.messages.length === 1);
  assert.equal(downloadAttempts, 2); // 1 次失败 + 1 次重试成功
  assert.deepEqual(recorder.sleeps, [1_000]); // 退避表首档
  assert.equal(env.errors.length, 0);
  const media = env.messages[0]?.media?.[0];
  assert.equal(media?.kind, "image");
  assert.equal(await readFile(media?.path ?? "", "utf8"), "bytes-of-photo_retry");
});

test("媒体下载：重试耗尽仍失败 → 消息丢弃但游标照常推进", async (t) => {
  const recorder = makeRecorder();
  let downloadAttempts = 0;
  recorder.deps.download = async () => {
    downloadAttempts += 1;
    throw new TypeError("fetch failed");
  };
  const env = await makeCtx();
  await startConnected(t, recorder, env, (r) =>
    r.queue("getUpdates", () => [
      {
        update_id: 400,
        message: groupMessage({
          message_id: 30,
          photo: [{ file_id: "photo_dead", file_unique_id: "d", width: 10, height: 10 }],
        }),
      },
      dmTextUpdate(401, 31, "下一条"),
    ]),
  );
  await waitFor(() => env.messages.length === 1 && env.messages[0]?.text === "下一条");
  assert.equal(downloadAttempts, 3); // 1 次原始 + 2 次有界重试
  assert.deepEqual(recorder.sleeps, [1_000, 3_000]);
  assert.ok(env.logs.some((line) => line.includes("处理 Telegram 更新 400 失败")));
  // 游标推进语义不变：失败的 400 也被标记已处理
  const statePath = join(env.dataDir, "telegram-state.json");
  await waitFor(() => {
    try {
      const parsed = JSON.parse(readFileSync(statePath, "utf8")) as {
        cursor: number;
        seen: Record<string, number>;
      };
      return parsed.cursor === 402 && parsed.seen["400"] !== undefined;
    } catch {
      return false;
    }
  });
});

test("sendText：409 → 发送失败、onError 上报并终止轮询", async (t) => {
  const recorder = makeRecorder();
  const env = await makeCtx();
  const adapter = await startConnected(t, recorder, env);
  recorder.queue("sendMessage", () => {
    throw Object.assign(new Error("Conflict: terminated by other getUpdates request"), { status: 409 });
  });
  await assert.rejects(() => adapter.sendText({ chatId: "4242", text: "x" }));
  await waitFor(() => env.errors.length === 1);
  assert.ok(env.errors[0] instanceof UserFacingError);
  assert.ok(env.errors[0]?.message.includes("409"));
  // 终止后不再轮询
  recorder.feedPollUpdates([dmTextUpdate(300, 90, "不应处理")]);
  await delay(20);
  assert.equal(env.messages.length, 0);
});

test("poll：401/403/409 致命错误 → onError 后停止轮询", async (t) => {
  const recorder = makeRecorder();
  const env = await makeCtx();
  const adapter = await startConnected(t, recorder, env, (r) =>
    r.queue("getUpdates", () => {
      throw Object.assign(new Error("Unauthorized"), { status: 401 });
    }),
  );
  await waitFor(() => env.errors.length === 1);
  assert.ok(env.errors[0] instanceof UserFacingError);
  assert.ok(env.errors[0]?.message.includes("401"));
  recorder.feedPollUpdates([dmTextUpdate(301, 91, "不应处理")]);
  await delay(20);
  assert.equal(env.messages.length, 0);
  await adapter.stop();
});

test("stop：挂起的长轮询被解除，pollLoop 退出", async (t) => {
  const recorder = makeRecorder();
  const env = await makeCtx();
  const adapter = await startConnected(t, recorder, env);
  // 无处理器的 25s 轮询处于挂起状态
  recorder.feedPollUpdates([dmTextUpdate(302, 92, "停前处理")]);
  await waitFor(() => env.messages.length === 1);
  await adapter.stop();
  recorder.feedPollUpdates([dmTextUpdate(303, 93, "停后不应处理")]);
  await delay(20);
  assert.equal(env.messages.length, 1);
});

// ----------------------------------------------------------------- configure

test("configureTelegram：拆分机密、从 Token 前缀派生 botUserId", () => {
  const configured = configureTelegram({ botToken: `  ${TOKEN}  ` });
  assert.equal(configured.secrets.botToken, TOKEN);
  assert.equal(configured.config.botUserId, BOT_ID);
  assert.equal(configured.config.username, "telegram_bot");
});

test("configureTelegram：缺 Token 或格式错误 → UserFacingError", () => {
  assert.throws(
    () => configureTelegram({}),
    (error: unknown) => error instanceof UserFacingError && error.message.includes("必填"),
  );
  assert.throws(
    () => configureTelegram({ botToken: "abc" }),
    (error: unknown) => error instanceof UserFacingError && error.message.includes("格式不正确"),
  );
});

test("definition：渠道元数据与工厂", () => {
  assert.equal(definition.channel, "telegram");
  assert.equal(definition.configure, configureTelegram);
  // 仅验证工厂产物形状；start() 依赖传输层，由 createAdapter 测试覆盖。
  const adapter = definition.create({
    connectionId: CONNECTION_ID,
    config: {} as TelegramConnection,
    secrets: {},
    dataDir: ".",
    mediaDir: ".",
    signal: new AbortController().signal,
    settings: async () => TEST_SETTINGS,
    log: () => {},
    onMessage: () => {},
    onGroups: () => {},
    onError: () => {},
  });
  assert.equal(adapter.channel, "telegram");
  assert.equal(adapter.capabilities.progress, "edit");
  assert.equal(adapter.capabilities.markdown, "html");
  assert.equal(adapter.capabilities.typing, true);
  assert.equal(adapter.capabilities.threads, true);
  assert.ok(adapter.capabilities.maxChunkChars > 0);
  assert.equal(typeof adapter.start, "function");
  assert.equal(typeof adapter.stop, "function");
  assert.equal(typeof adapter.sendText, "function");
  assert.equal(typeof adapter.editMessage, "function");
  assert.equal(typeof adapter.sendTyping, "function");
});
