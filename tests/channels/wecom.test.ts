/** WeCom channel adapter tests: fake transport injected via createAdapter deps,
 * no real network. Covers normalize (dm/group/@mention/quote/media), fence-aware
 * chunking, send paths, stream lifecycle with fallback, dedup persistence,
 * reconnect backoff, and start error paths. Timers use node:test mock.timers;
 * the only real-clock waits are bounded condition polls on the state file. */
import assert from "node:assert/strict";
import { mock, test } from "node:test";
import { mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { WSAuthFailureError } from "@wecom/aibot-node-sdk";
import { connectionSchema } from "../../shared/config.ts";
import { UserFacingError } from "../../shared/errors.ts";
import type { NormalizedMessage } from "../../shared/model.ts";
import { imSettings } from "../../shared/settings.ts";
import { chunkText } from "../../server/channels/wecom/chunk.ts";
import {
  createAdapter,
  defaultDeps,
  definition,
  type WecomAdapterDeps,
  type WecomClientLike,
  type WecomConnection,
} from "../../server/channels/wecom/index.ts";
import type { AdapterContext, ChannelAdapter } from "../../server/channels/types.ts";

type Listener = (...args: unknown[]) => void;

class FakeWecomClient {
  readonly calls: Array<{ method: string; args: unknown[] }> = [];
  readonly listeners = new Map<string, Listener[]>();
  linkUp = false;
  authOnConnect = true;
  authErrorOnConnect: Error | null = null;
  connectShouldThrow: Error | null = null;
  sendMessageError: unknown = null;
  replyFinishError: unknown = null;
  downloadResult: { buffer: Buffer; filename?: string } | Error = {
    buffer: Buffer.from([0x89, 0x50, 0x4e, 0x47]),
    filename: "photo.png",
  };
  #msgCount = 0;
  #streamCount = 0;
  #uploadCount = 0;
  #mediaSentCount = 0;
  #reqCount = 0;

  on(event: string, listener: Listener): this {
    const list = this.listeners.get(event) ?? [];
    list.push(listener);
    this.listeners.set(event, list);
    return this;
  }

  emit(event: string, ...args: unknown[]): void {
    for (const listener of [...(this.listeners.get(event) ?? [])]) listener(...args);
  }

  callsOf(method: string): Array<{ method: string; args: unknown[] }> {
    return this.calls.filter((call) => call.method === method);
  }

  connect(): this {
    this.calls.push({ method: "connect", args: [] });
    if (this.connectShouldThrow) throw this.connectShouldThrow;
    this.linkUp = true;
    // 真实 SDK 的事件在连接建立后异步到达；微任务即可让 start() 的等待先完成注册。
    queueMicrotask(() => {
      if (this.authErrorOnConnect) this.emit("error", this.authErrorOnConnect);
      else if (this.authOnConnect) this.emit("authenticated");
    });
    return this;
  }

  disconnect(): void {
    this.calls.push({ method: "disconnect", args: [] });
    this.linkUp = false;
  }

  get isConnected(): boolean {
    return this.linkUp;
  }

  async sendMessage(chatid: string, body: unknown): Promise<unknown> {
    this.calls.push({ method: "sendMessage", args: [chatid, body] });
    if (this.sendMessageError) throw this.sendMessageError;
    this.#msgCount += 1;
    return { headers: { req_id: `ack-${this.#msgCount}` }, body: { msgid: `sent-${this.#msgCount}` } };
  }

  async replyStream(frame: unknown, streamId: string, content: string, finish?: boolean): Promise<unknown> {
    this.calls.push({ method: "replyStream", args: [frame, streamId, content, finish === true] });
    if (finish && this.replyFinishError) throw this.replyFinishError;
    if (finish) {
      this.#streamCount += 1;
      return { headers: { req_id: `ack-${this.#streamCount}` }, body: { msgid: `stream-${this.#streamCount}` } };
    }
    return { headers: { req_id: "ack" }, body: {} };
  }

  async replyStreamNonBlocking(frame: unknown, streamId: string, content: string, finish?: boolean): Promise<unknown> {
    this.calls.push({ method: "replyStreamNonBlocking", args: [frame, streamId, content, finish === true] });
    return { headers: { req_id: "ack" }, body: {} };
  }

  async uploadMedia(fileBuffer: Buffer, options: { type: string; filename: string }): Promise<unknown> {
    this.calls.push({ method: "uploadMedia", args: [fileBuffer.length, options] });
    this.#uploadCount += 1;
    return { type: options.type, media_id: `media-${this.#uploadCount}`, created_at: "" };
  }

  async sendMediaMessage(chatid: string, mediaType: string, mediaId: string): Promise<unknown> {
    this.calls.push({ method: "sendMediaMessage", args: [chatid, mediaType, mediaId] });
    this.#mediaSentCount += 1;
    return { headers: { req_id: `ack-${this.#mediaSentCount}` }, body: { msgid: `mediaSent-${this.#mediaSentCount}` } };
  }

  async downloadFile(url: string, aesKey?: string): Promise<{ buffer: Buffer; filename?: string }> {
    this.calls.push({ method: "downloadFile", args: [url, aesKey] });
    if (this.downloadResult instanceof Error) throw this.downloadResult;
    return this.downloadResult;
  }

  frame(body: Record<string, unknown>): void {
    this.#reqCount += 1;
    this.emit("message", { headers: { req_id: `req-${this.#reqCount}` }, body });
  }
}

interface Harness {
  client: FakeWecomClient;
  adapter: ChannelAdapter;
  ctx: AdapterContext<WecomConnection>;
  messages: NormalizedMessage[];
  errors: Error[];
  logs: Array<{ level: string; message: string }>;
  root: string;
  dataDir: string;
  mediaDir: string;
  nextMessage: () => Promise<NormalizedMessage>;
  cleanup: () => Promise<void>;
}

function wecomConnection(overrides: Partial<WecomConnection> = {}): WecomConnection {
  const parsed = connectionSchema.parse({
    channel: "wecom",
    id: "00000000-0000-4000-8000-000000000001",
    name: "测试企业微信机器人",
    enabled: true,
    target: null,
    policy: {},
    revision: 0,
    botId: "bot-1",
  });
  return { ...parsed, ...overrides } as WecomConnection;
}

async function setup(
  deps: Partial<WecomAdapterDeps> = {},
  dirs?: { dataDir?: string; mediaDir?: string },
): Promise<Harness> {
  const root = !dirs?.dataDir && !dirs?.mediaDir
    ? await mkdtemp(join(tmpdir(), "paseo-wecom-"))
    : await mkdtemp(join(tmpdir(), "paseo-wecom-helper-"));
  const dataDir = dirs?.dataDir ?? join(root, "data");
  const mediaDir = dirs?.mediaDir ?? join(root, "media");
  const client = new FakeWecomClient();
  const messages: NormalizedMessage[] = [];
  const waiters: Array<(message: NormalizedMessage) => void> = [];
  const errors: Error[] = [];
  const logs: Array<{ level: string; message: string }> = [];
  const ctx: AdapterContext<WecomConnection> = {
    connectionId: "conn-1",
    config: wecomConnection(),
    secrets: { secret: "s3cret" },
    dataDir,
    mediaDir,
    signal: new AbortController().signal,
    settings: async () => imSettings.schema.parse({}),
    log: (level, message) => logs.push({ level, message }),
    onMessage: (message) => {
      messages.push(message);
      for (const resolve of waiters.splice(0)) resolve(message);
    },
    onGroups: () => {},
    onError: (error) => errors.push(error),
  };
  // 伪传输层按语义实现 WecomClientLike；宽松签名无法被结构化校验，在注入点
  // 做一次显式断言，其余全部走类型化接口。
  const adapter = createAdapter(ctx, { client: client as unknown as WecomClientLike, ...deps });
  return {
    client,
    adapter,
    ctx,
    messages,
    errors,
    logs,
    root,
    dataDir,
    mediaDir,
    nextMessage: () => new Promise((resolve) => waiters.push(resolve)),
    cleanup: async () => {
      await adapter.stop();
      await rm(root, { recursive: true, force: true });
    },
  };
}

/** Bounded poll on the persisted dedup state; only used to order assertions
 * behind fs completion where no event is exposed. */
async function untilStateIncludes(h: Harness, msgid: string): Promise<void> {
  const statePath = join(h.dataDir, "wecom-state.json");
  const deadline = Date.now() + 2_000;
  for (;;) {
    const raw = await readFile(statePath, "utf8").catch(() => "");
    if (raw.includes(msgid)) return;
    if (Date.now() > deadline) throw new Error(`状态文件未包含 ${msgid}`);
    await new Promise((resolve) => setTimeout(resolve, 5));
  }
}

test("normalize: 单聊文本消息", async () => {
  const h = await setup();
  try {
    await h.adapter.start();
    const pending = h.nextMessage();
    h.client.frame({
      msgid: "m1",
      chattype: "single",
      from: { userid: "u-100" },
      create_time: 1_700_000_000,
      msgtype: "text",
      text: { content: "你好，机器人" },
    });
    const message = await pending;
    assert.equal(message.channel, "wecom");
    assert.equal(message.connectionId, "conn-1");
    assert.equal(message.kind, "dm");
    assert.equal(message.chatId, "u-100");
    assert.equal(message.senderId, "u-100");
    assert.equal(message.messageId, "m1");
    assert.equal(message.text, "你好，机器人");
    assert.equal(message.timestamp, 1_700_000_000_000);
    assert.deepEqual(message.trigger, { mention: true, replyToBot: false, command: false });
  } finally {
    await h.cleanup();
  }
});

test("normalize: 群聊 @提及 前缀剥离", async () => {
  const h = await setup();
  try {
    await h.adapter.start();
    const pending = h.nextMessage();
    h.client.frame({
      msgid: "m2",
      chattype: "group",
      chatid: "g-9",
      from: { userid: "u-200" },
      msgtype: "text",
      text: { content: "@小助手 帮我写个函数" },
    });
    const message = await pending;
    assert.equal(message.kind, "group");
    assert.equal(message.chatId, "g-9");
    assert.equal(message.text, "帮我写个函数");
    assert.deepEqual(message.trigger, { mention: true, replyToBot: false, command: false });
  } finally {
    await h.cleanup();
  }
});

test("normalize: 引用消息进入 replyTo", async () => {
  const h = await setup();
  try {
    await h.adapter.start();
    const pending = h.nextMessage();
    h.client.frame({
      msgid: "m3",
      chattype: "single",
      from: { userid: "u-100" },
      msgtype: "text",
      text: { content: "再说一遍" },
      quote: { msgtype: "text", text: { content: "原始问题" } },
    });
    const message = await pending;
    assert.deepEqual(message.replyTo, { text: "原始问题" });
    assert.equal(message.text, "再说一遍");
  } finally {
    await h.cleanup();
  }
});

test("normalize: 图片消息下载进 mediaDir", async () => {
  const h = await setup();
  try {
    await h.adapter.start();
    const pending = h.nextMessage();
    h.client.frame({
      msgid: "m4",
      chattype: "single",
      from: { userid: "u-100" },
      msgtype: "image",
      image: { url: "https://example.com/enc-image", aeskey: "k-1" },
    });
    await pending;
    const downloads = h.client.callsOf("downloadFile");
    assert.equal(downloads.length, 1);
    assert.deepEqual(downloads[0].args, ["https://example.com/enc-image", "k-1"]);
    const media = h.messages[0].media;
    assert.equal(media?.length, 1);
    assert.equal(media![0].kind, "image");
    assert.equal(media![0].name, "photo.png");
    assert.equal(media![0].mime, "image/png");
    assert.ok(media![0].path.startsWith(h.mediaDir));
    const content = await readFile(media![0].path);
    assert.deepEqual([...content], [0x89, 0x50, 0x4e, 0x47]);
  } finally {
    await h.cleanup();
  }
});

test("normalize: 文件消息保留文件名", async () => {
  const h = await setup();
  try {
    await h.adapter.start();
    h.client.downloadResult = { buffer: Buffer.from("csv-data"), filename: "报表.xlsx" };
    const pending = h.nextMessage();
    h.client.frame({
      msgid: "m5",
      chattype: "group",
      chatid: "g-9",
      from: { userid: "u-200" },
      msgtype: "file",
      file: { url: "https://example.com/f", aeskey: "k-2" },
    });
    await pending;
    const media = h.messages[0].media;
    assert.equal(media?.length, 1);
    assert.equal(media![0].kind, "file");
    assert.equal(media![0].name, "报表.xlsx");
    assert.equal(media![0].mime, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet");
  } finally {
    await h.cleanup();
  }
});

test("normalize: 图文混排消息合并文本与图片", async () => {
  const h = await setup();
  try {
    await h.adapter.start();
    const pending = h.nextMessage();
    h.client.frame({
      msgid: "m6",
      chattype: "single",
      from: { userid: "u-100" },
      msgtype: "mixed",
      mixed: {
        msg_item: [
          { msgtype: "text", text: { content: "看这张图" } },
          { msgtype: "image", image: { url: "https://example.com/i2", aeskey: "k-3" } },
        ],
      },
    });
    const message = await pending;
    assert.equal(message.text, "看这张图");
    assert.equal(message.media?.length, 1);
  } finally {
    await h.cleanup();
  }
});

test("normalize: 语音消息取平台转写占位文本", async () => {
  const h = await setup();
  try {
    await h.adapter.start();
    const pending = h.nextMessage();
    h.client.frame({
      msgid: "m7",
      chattype: "single",
      from: { userid: "u-100" },
      msgtype: "voice",
      voice: { content: "帮我查一下天气" },
    });
    const message = await pending;
    assert.equal(message.text, "帮我查一下天气");
  } finally {
    await h.cleanup();
  }
});

test("normalize: 语音无转写时使用占位符", async () => {
  const h = await setup();
  try {
    await h.adapter.start();
    const pending = h.nextMessage();
    h.client.frame({
      msgid: "m8",
      chattype: "single",
      from: { userid: "u-100" },
      msgtype: "voice",
      voice: {},
    });
    const message = await pending;
    assert.equal(message.text, "[语音]");
  } finally {
    await h.cleanup();
  }
});

test("normalize: msgid 去重且状态持久化", async () => {
  const h = await setup();
  try {
    await h.adapter.start();
    const first = h.nextMessage();
    const frameBody = {
      msgid: "dup-1",
      chattype: "single",
      from: { userid: "u-100" },
      msgtype: "text",
      text: { content: "只发一次" },
    };
    h.client.frame(frameBody);
    await first;
    h.client.frame(frameBody);
    await untilStateIncludes(h, "dup-1");
    assert.equal(h.messages.length, 1);
  } finally {
    await h.cleanup();
  }
});

test("normalize: 重启后依据持久化状态继续去重", async () => {
  const first = await setup();
  let second: Harness | null = null;
  try {
    await first.adapter.start();
    const pending = first.nextMessage();
    first.client.frame({
      msgid: "old-1",
      chattype: "single",
      from: { userid: "u-100" },
      msgtype: "text",
      text: { content: "第一条" },
    });
    await pending;
    await first.adapter.stop(); // 仅停止，保留目录供“重启”后的实例复用

    // 模拟重启：沿用同一 dataDir（持久化的 seen 记录）与新客户端实例。
    second = await setup({}, { dataDir: first.dataDir, mediaDir: first.mediaDir });
    await second.adapter.start();
    second.client.frame({
      msgid: "old-1",
      chattype: "single",
      from: { userid: "u-100" },
      msgtype: "text",
      text: { content: "第一条" },
    });
    const fresh = second.nextMessage();
    second.client.frame({
      msgid: "new-2",
      chattype: "single",
      from: { userid: "u-100" },
      msgtype: "text",
      text: { content: "第二条" },
    });
    const message = await fresh;
    assert.equal(message.messageId, "new-2");
    assert.equal(second.messages.length, 1);
  } finally {
    await second?.cleanup();
    await rm(first.root, { recursive: true, force: true });
  }
});

test("normalize: 无文本无媒体的噪音不 emit", async () => {
  const h = await setup();
  try {
    await h.adapter.start();
    h.client.frame({
      msgid: "noise-1",
      chattype: "single",
      from: { userid: "u-100" },
      msgtype: "video",
      video: { url: "https://example.com/v" },
    });
    h.client.frame({
      msgid: "noise-2",
      chattype: "group",
      chatid: "g-9",
      from: { userid: "u-200" },
      msgtype: "text",
      text: { content: "   " },
    });
    // 噪音帧同样会被记录去重；等两帧的处理都落盘后再断言零投递。
    await untilStateIncludes(h, "noise-1");
    await untilStateIncludes(h, "noise-2");
    assert.equal(h.messages.length, 0);
  } finally {
    await h.cleanup();
  }
});

test("chunkText: 字符与字节预算分块", () => {
  assert.deepEqual(chunkText("", 6000), []);
  assert.deepEqual(chunkText("  ", 6000), []);
  assert.deepEqual(chunkText("hello", 6000), ["hello"]);

  const cjk = "字".repeat(7000);
  const chunks = chunkText(cjk, 6000);
  assert.equal(chunks.length, 2);
  for (const chunk of chunks) {
    assert.ok(chunk.length <= 6000);
    assert.ok(Buffer.byteLength(chunk) <= 18_000);
  }
  assert.equal(chunks.join(""), cjk);

  // 6000 个汉字恰好 18000 字节；再多一个字必须触发字节预算分块。
  const bytesSplit = chunkText("字".repeat(6001), 6000);
  assert.equal(bytesSplit.length, 2);
  assert.equal(Buffer.byteLength(bytesSplit[0]), 18_000);
  assert.equal(bytesSplit[1], "字");

  const longLine = "a".repeat(25);
  assert.deepEqual(chunkText(longLine, 10), ["a".repeat(10), "a".repeat(10), "a".repeat(5)]);
});

test("chunkText: 代码围栏跨块闭合重开", () => {
  const text = [
    "```python",
    ...Array.from({ length: 30 }, (_, i) => `line ${i} = ${i}`),
    "```",
    "结尾说明",
  ].join("\n");
  const chunks = chunkText(text, 60);
  assert.ok(chunks.length > 1);
  for (const chunk of chunks) {
    const fences = chunk.split("\n").filter((line) => line.trimStart().startsWith("```")).length;
    assert.equal(fences % 2, 0, `围栏未闭合：${chunk}`);
  }
  for (const chunk of chunks) {
    if (chunk.includes("line 0 =") || chunk.includes("line 29 =")) {
      assert.ok(chunk.split("\n").some((line) => line.trimStart().startsWith("```")));
    }
  }
  assert.ok(chunks.at(-1)!.includes("结尾说明"));
});

test("sendText: markdown 发送并返回消息 ID", async () => {
  const h = await setup();
  try {
    await h.adapter.start();
    const result = await h.adapter.sendText({ chatId: "u-100", text: "你好\n第二行" });
    const sends = h.client.callsOf("sendMessage");
    assert.equal(sends.length, 1);
    assert.equal(sends[0].args[0], "u-100");
    assert.deepEqual(sends[0].args[1], { msgtype: "markdown", markdown: { content: "你好\n第二行" } });
    assert.equal(result.messageId, "sent-1");
  } finally {
    await h.cleanup();
  }
});

test("sendText: 超长文本分块且每块不超限", async () => {
  const h = await setup();
  try {
    await h.adapter.start();
    const result = await h.adapter.sendText({ chatId: "g-9", text: "字".repeat(13_000) });
    const sends = h.client.callsOf("sendMessage");
    assert.equal(sends.length, 3);
    for (const send of sends) {
      const body = send.args[1] as { markdown: { content: string } };
      assert.ok(body.markdown.content.length <= 6000);
      assert.ok(Buffer.byteLength(body.markdown.content) <= 18_000);
    }
    assert.equal(result.messageId, "sent-3");
  } finally {
    await h.cleanup();
  }
});

test("sendText: 平台拒绝帧包装为中文用户可读错误", async () => {
  const h = await setup();
  try {
    await h.adapter.start();
    h.client.sendMessageError = { errcode: 45009, errmsg: "api forbidden" };
    await assert.rejects(
      h.adapter.sendText({ chatId: "u-100", text: "hi" }),
      (error: unknown) => error instanceof UserFacingError && error.message.includes("45009"),
    );
  } finally {
    await h.cleanup();
  }
});

test("sendMedia: 上传临时素材后发送媒体消息", async () => {
  const h = await setup();
  try {
    await h.adapter.start();
    const filePath = join(h.mediaDir, "out.png");
    await mkdir(h.mediaDir, { recursive: true });
    await writeFile(filePath, Buffer.from("image-bytes"));
    const result = await h.adapter.sendMedia!({ chatId: "g-9", kind: "image", path: filePath, name: "out.png" });
    const uploads = h.client.callsOf("uploadMedia");
    assert.equal(uploads.length, 1);
    assert.deepEqual(uploads[0].args[1], { type: "image", filename: "out.png" });
    const sends = h.client.callsOf("sendMediaMessage");
    assert.deepEqual(sends[0].args, ["g-9", "image", "media-1"]);
    assert.equal(result.messageId, "mediaSent-1");
  } finally {
    await h.cleanup();
  }
});

test("sendMedia: 空文件与超限文件拒绝发送", async () => {
  const h = await setup();
  try {
    await h.adapter.start();
    await mkdir(h.mediaDir, { recursive: true });
    const emptyPath = join(h.mediaDir, "empty.bin");
    await writeFile(emptyPath, Buffer.alloc(0));
    await assert.rejects(
      h.adapter.sendMedia!({ chatId: "u-100", kind: "file", path: emptyPath }),
      UserFacingError,
    );
    const bigPath = join(h.mediaDir, "big.bin");
    await writeFile(bigPath, Buffer.alloc(30 * 1024 * 1024 + 1));
    await assert.rejects(
      h.adapter.sendMedia!({ chatId: "u-100", kind: "file", path: bigPath }),
      (error: unknown) => error instanceof UserFacingError && error.message.includes("30MB"),
    );
    assert.equal(h.client.callsOf("uploadMedia").length, 0);
  } finally {
    await h.cleanup();
  }
});

test("stream: 进度流使用缓存帧并以 finish 帧结束", async () => {
  const h = await setup();
  try {
    await h.adapter.start();
    const pending = h.nextMessage();
    h.client.frame({
      msgid: "s1",
      chattype: "group",
      chatid: "g-9",
      from: { userid: "u-200" },
      msgtype: "text",
      text: { content: "@bot 开始任务" },
    });
    await pending;

    const { streamId } = await h.adapter.streamStart!("g-9");
    await h.adapter.streamAppend!(streamId, "正在思考…");
    await h.adapter.streamAppend!(streamId, "正在思考…第二步");
    const result = await h.adapter.streamFinish!(streamId, "最终答案");
    assert.equal(result.messageId, "stream-1");

    const streams = h.client.callsOf("replyStream");
    const nonBlocking = h.client.callsOf("replyStreamNonBlocking");
    assert.equal(streams.length, 2);
    const firstFrame = streams[0].args[0] as { headers: { req_id: string } };
    assert.equal(firstFrame.headers.req_id, "req-1");
    assert.equal(streams[0].args[1], streamId);
    assert.equal(streams[0].args[2], "正在思考…");
    assert.equal(streams[0].args[3], false);
    assert.equal(streams[1].args[2], "最终答案");
    assert.equal(streams[1].args[3], true);
    assert.equal(nonBlocking.length, 1);
  } finally {
    await h.cleanup();
  }
});

test("stream: 无缓存帧时完成回退为主动发送", async () => {
  const h = await setup();
  try {
    await h.adapter.start();
    const { streamId } = await h.adapter.streamStart!("unknown-chat");
    await h.adapter.streamAppend!(streamId, "进度"); // 无帧：静默跳过
    const result = await h.adapter.streamFinish!(streamId, "完整答案");
    assert.equal(h.client.callsOf("replyStream").length, 0);
    const sends = h.client.callsOf("sendMessage");
    assert.equal(sends.length, 1);
    assert.deepEqual(sends[0].args[1], { msgtype: "markdown", markdown: { content: "完整答案" } });
    assert.equal(result.messageId, "sent-1");
  } finally {
    await h.cleanup();
  }
});

test("stream: 卡片过期完成失败时回退为主动发送", async () => {
  const h = await setup();
  try {
    await h.adapter.start();
    const pending = h.nextMessage();
    h.client.frame({
      msgid: "s2",
      chattype: "single",
      from: { userid: "u-100" },
      msgtype: "text",
      text: { content: "hi" },
    });
    await pending;
    h.client.replyFinishError = { errcode: 846608, errmsg: "stream message update expired" };
    const { streamId } = await h.adapter.streamStart!("u-100");
    const result = await h.adapter.streamFinish!(streamId, "答案正文");
    const sends = h.client.callsOf("sendMessage");
    assert.equal(sends.length, 1);
    assert.equal(result.messageId, "sent-1");
  } finally {
    await h.cleanup();
  }
});

test("start: 缺少 Secret 时抛出中文可执行错误", async () => {
  const h = await setup();
  try {
    h.ctx.secrets = {};
    await assert.rejects(
      h.adapter.start(),
      (error: unknown) => error instanceof UserFacingError && error.message.includes("Secret"),
    );
  } finally {
    await h.cleanup();
  }
});

test("start: 缺少 Bot ID 时抛出中文可执行错误", async () => {
  const h = await setup();
  try {
    h.ctx.config = wecomConnection({ botId: "" });
    await assert.rejects(
      h.adapter.start(),
      (error: unknown) => error instanceof UserFacingError && error.message.includes("Bot ID"),
    );
  } finally {
    await h.cleanup();
  }
});

test("start: 认证失败立即失败且不再重连", async () => {
  const h = await setup({ reconnectDelaysMs: [10] });
  try {
    mock.timers.enable({ apis: ["setTimeout"] });
    h.client.authErrorOnConnect = new WSAuthFailureError(3);
    const pending = h.adapter.start();
    await assert.rejects(
      pending,
      (error: unknown) => error instanceof UserFacingError && error.message.includes("认证失败"),
    );
    mock.timers.tick(60_000);
    assert.equal(h.client.callsOf("connect").length, 1);
  } finally {
    mock.timers.reset();
    await h.cleanup();
  }
});

test("start: 就绪超时抛出超时错误并断开", async () => {
  // Real platform-clock behavior: the ready window races the fs state load for
  // timer registration, so a mocked clock would tick before it exists.
  const h = await setup({ readyTimeoutMs: 250 });
  try {
    h.client.authOnConnect = false;
    await assert.rejects(
      h.adapter.start(),
      (error: unknown) => error instanceof UserFacingError && error.message.includes("超时"),
    );
    assert.ok(h.client.callsOf("disconnect").length >= 1);
  } finally {
    await h.cleanup();
  }
});

test("reconnect: 断开后按退避计划重连并在认证成功后重置", async () => {
  const h = await setup({ reconnectDelaysMs: [10, 20] });
  try {
    await h.adapter.start();
    assert.equal(h.client.callsOf("connect").length, 1);
    mock.timers.enable({ apis: ["setTimeout"] });
    h.client.linkUp = false;
    h.client.emit("disconnected", "connection lost");
    mock.timers.tick(10);
    assert.ok(h.client.callsOf("connect").length >= 2);
    assert.ok(h.logs.some((entry) => entry.level === "warn" && entry.message.includes("重连")));
    await Promise.resolve(); // 让重连后的 authenticated 微任务先重置退避计数
    // 认证成功后计数器已重置：再次断开仍走最小退避。
    h.client.linkUp = false;
    h.client.emit("disconnected", "connection lost again");
    mock.timers.tick(10);
    assert.ok(h.client.callsOf("connect").length >= 3);
  } finally {
    mock.timers.reset();
    await h.cleanup();
  }
});

test("reconnect: stop 后不再重连", async () => {
  const h = await setup({ reconnectDelaysMs: [10] });
  try {
    await h.adapter.start();
    await h.adapter.stop();
    mock.timers.enable({ apis: ["setTimeout"] });
    h.client.emit("disconnected", "manual");
    mock.timers.tick(60_000);
    assert.equal(h.client.callsOf("connect").length, 1);
  } finally {
    mock.timers.reset();
    await h.cleanup();
  }
});

test("testConnection: 连接状态反映结果", async () => {
  const h = await setup();
  try {
    assert.deepEqual(await h.adapter.testConnection!(), { ok: false, detail: "适配器尚未启动。" });
    await h.adapter.start();
    assert.deepEqual(await h.adapter.testConnection!(), { ok: true });
    await h.adapter.stop();
    assert.equal((await h.adapter.testConnection!()).ok, false);
  } finally {
    await h.cleanup();
  }
});

test("configure: 拆分机密并校验必填字段", () => {
  const fields = definition.configure({ botId: " bot-9 ", secret: " sec-9 " });
  assert.deepEqual(fields.config, { botId: "bot-9" });
  assert.deepEqual(fields.secrets, { secret: "sec-9" });

  assert.throws(
    () => definition.configure({ botId: "bot-9" }),
    (error: unknown) => error instanceof UserFacingError && error.message.includes("Secret"),
  );
  assert.throws(
    () => definition.configure({ secret: "sec-9" }),
    (error: unknown) => error instanceof UserFacingError && error.message.includes("Bot ID"),
  );
});

test("definition: create 工厂与默认依赖可用", async () => {
  const h = await setup();
  try {
    assert.equal(h.adapter.channel, "wecom");
    assert.equal(h.adapter.capabilities.progress, "stream");
    assert.equal(h.adapter.capabilities.markdown, "native");
    assert.equal(h.adapter.capabilities.maxChunkChars, 6000);
    assert.equal(typeof defaultDeps().createClient, "function");
    const adapter = definition.create(h.ctx);
    assert.equal(adapter.channel, "wecom");
    await adapter.stop();
  } finally {
    await h.cleanup();
  }
});

test("stream: 卡片临近 510s 上限时轮换新卡且不重复送达", async () => {
  // TTL 0 是退化配置：第二次 append 必然越过轮换线，无需真实等待。
  const h = await setup({ streamCardTtlMs: 0 });
  try {
    await h.adapter.start();
    const pending = h.nextMessage();
    h.client.frame({
      msgid: "s3",
      chattype: "single",
      from: { userid: "u-100" },
      msgtype: "text",
      text: { content: "开始长任务" },
    });
    await pending;

    const { streamId } = await h.adapter.streamStart!("u-100");
    await h.adapter.streamAppend!(streamId, "第一段进度");
    await h.adapter.streamAppend!(streamId, "第二段进度");
    const result = await h.adapter.streamFinish!(streamId, "最终答案");

    // 序列：开卡 → 旧卡以最后已知文本 finish → 同一 frame 开新卡（（续）前缀）→ 新卡 finish。
    const streams = h.client.callsOf("replyStream");
    const newCardId = streams[2]?.args[1];
    assert.ok(typeof newCardId === "string" && newCardId !== streamId, "轮换应打开新卡 id");
    assert.deepEqual(
      streams.map((call) => [call.args[1], call.args[2], call.args[3]]),
      [
        [streamId, "第一段进度", false],
        [streamId, "第一段进度", true],
        [newCardId, "（续）第二段进度", false],
        [newCardId, "最终答案", true],
      ],
    );
    // 全程无 sendText 兜底：内容只在卡片上送达一次，不重复。
    assert.equal(h.client.callsOf("sendMessage").length, 0);
    // 引擎拿到的 messageId 来自最终 finish 帧（fake 按收尾计数）。
    assert.equal(result.messageId, "stream-2");
  } finally {
    await h.cleanup();
  }
});

test("stream: 记录被逐出后完成抛出中文用户可读错误", async () => {
  const h = await setup();
  try {
    await h.adapter.start();
    const { streamId } = await h.adapter.streamStart!("u-100");
    // STREAMS_CAP = 32：再开满一页流把最早一条挤出。
    for (let i = 0; i < 32; i += 1) await h.adapter.streamStart!("g-9");
    await assert.rejects(
      h.adapter.streamFinish!(streamId, "答案"),
      (error: unknown) => error instanceof UserFacingError && error.message.includes("已被清理"),
    );
  } finally {
    await h.cleanup();
  }
});

test("markSeen: 状态写失败回滚去重记录，恢复后同一条消息可再次处理", async () => {
  const root = await mkdtemp(join(tmpdir(), "paseo-wecom-rollback-"));
  const dataDir = join(root, "data");
  const mediaDir = join(root, "media");
  await mkdir(dataDir, { recursive: true });
  // 同名目录阻断 atomicWrite：#markSeen 写盘必然失败。
  await mkdir(join(dataDir, "wecom-state.json"));
  const h = await setup({}, { dataDir, mediaDir });
  await h.adapter.start();
  try {
    const frameBody = {
      msgid: "m-rollback",
      chattype: "single",
      from: { userid: "u-100" },
      msgtype: "text",
      text: { content: "写盘失败的消息" },
    };
    h.client.frame(frameBody);
    
    // 有界条件轮询：错误经 onError 同步入列，但到达依赖 fs 完成事件（与
    // untilStateIncludes 同一让步理由）。
    const deadline = Date.now() + 2_000;
    while (h.errors.length === 0 && Date.now() <= deadline) {
      await new Promise((resolve) => setTimeout(resolve, 5));
    }
    assert.ok(h.errors.length >= 1, "写盘失败应上报错误");
    assert.equal(h.messages.length, 0);

    // 解除阻断后，同一条消息（平台重推）必须能被再次处理——证明 msgid 未残留。
    await rm(join(dataDir, "wecom-state.json"), { recursive: true, force: true });
    const delivered = h.nextMessage();
    h.client.frame(frameBody);
    const message = await delivered;
    assert.equal(message.messageId, "m-rollback");
    assert.equal(h.messages.length, 1);
  } finally {
    await h.cleanup();
    await rm(root, { recursive: true, force: true, maxRetries: 10, retryDelay: 20 });
  }
});

test("stop: 解除未完成的就绪等待，失败路径不留已启动假象", async () => {
  const h = await setup({ readyTimeoutMs: 20_000 });
  try {
    h.client.authOnConnect = false; // 永不就绪，start 卡在 waitForReady
    const starting = h.adapter.start();
    // 有界轮询 connect 调用：connect() 与就绪门注册之间无 await 间隙，
    // 记录到调用即说明 start 已进入 waitForReady 等待。
    const deadline = Date.now() + 2_000;
    while (h.client.callsOf("connect").length === 0 && Date.now() <= deadline) {
      await new Promise((resolve) => setTimeout(resolve, 5));
    }
    assert.equal(h.client.callsOf("connect").length, 1);
    await h.adapter.stop();
    await assert.rejects(
      starting,
      (error: unknown) => error instanceof UserFacingError && error.message.includes("已停止"),
    );
    // 失败路径 #client 置空：testConnection 不再误报。
    assert.deepEqual(await h.adapter.testConnection!(), { ok: false, detail: "适配器尚未启动。" });
  } finally {
    await h.cleanup();
  }
});
