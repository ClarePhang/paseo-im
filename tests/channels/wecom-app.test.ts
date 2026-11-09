/** wecom-app channel adapter tests: real crypto envelopes over a loopback
 * HTTP callback listener and a fake cgi-bin upstream; no external network. */
import { strict as assert } from "node:assert";
import { createServer, type IncomingMessage, type Server, type ServerResponse } from "node:http";
import { mkdir, mkdtemp, writeFile, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test, { type TestContext } from "node:test";

import { WecomCrypto } from "@wecom/aibot-node-sdk";

import { definition, createAdapter, type WecomAppConnection } from "../../server/channels/wecom-app/index.ts";
import { parseWecomXmlBody } from "../../server/channels/wecom-app/protocol.ts";
import { acquireCallbackServer, releaseCallbackServer } from "../../server/channels/wecom-app/callback.ts";
import { WecomAppApi } from "../../server/channels/wecom-app/api.ts";
import { WecomAppApiError } from "../../server/channels/wecom-app/protocol.ts";
import type { AdapterContext, ChannelAdapter } from "../../server/channels/types.ts";
import { emptyPolicy } from "../../shared/config.ts";
import { UserFacingError } from "../../shared/errors.ts";
import { imSettings } from "../../shared/settings.ts";
import type { NormalizedMessage } from "../../shared/model.ts";

const CORP_ID = "corp123456";
const AGENT_ID = "1000002";
const TOKEN = "testtoken";
const AES_KEY = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRS".slice(0, 43); // 43 base64-ish chars → 32 bytes
const CALLBACK_SECRET = "0123456789abcdef0123456789abcdef";
const CONNECTION_ID = "11111111-2222-4333-8444-555555555555";
const CONNECTION_ID_B = "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee";
const PNG_BYTES = Buffer.from([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a, 0x00, 0x01]);

/** ES2023 lib has no Promise.withResolvers; this is its local equivalent. */
function deferred<T>(): { promise: Promise<T>; resolve: (value: T) => void } {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((res) => {
    resolve = res;
  });
  return { promise, resolve };
}

function makeConnection(overrides: Partial<WecomAppConnection> = {}): WecomAppConnection {
  return {
    id: CONNECTION_ID,
    channel: "wecom-app",
    name: "测试企微应用",
    enabled: true,
    target: null,
    policy: emptyPolicy(),
    bindings: {},
    revision: 0,
    corpId: CORP_ID,
    agentId: AGENT_ID,
    callbackSecret: CALLBACK_SECRET,
    streamEnabled: true,
    callbackBaseUrl: "https://wecom.example.com",
    ...overrides,
  };
}

const BASE_SECRETS = { secret: "app-secret-1", token: TOKEN, encodingAESKey: AES_KEY };

interface Upstream {
  port: number;
  close(): Promise<void>;
  state: {
    tokenRequests: number;
    failToken: number;
    sends: { payload: Record<string, unknown>; agentid: unknown }[];
    uploads: { contentType: string | null; body: string }[];
    mediaDownloads: string[];
  };
}

async function startUpstream(): Promise<Upstream> {
  const state: Upstream["state"] = { tokenRequests: 0, failToken: 0, sends: [], uploads: [], mediaDownloads: [] };
  const readBody = (req: IncomingMessage) =>
    new Promise<string>((resolve) => {
      const chunks: Buffer[] = [];
      req.on("data", (chunk: Buffer) => chunks.push(chunk));
      req.on("end", () => resolve(Buffer.concat(chunks).toString("utf8")));
    });
  const server: Server = createServer(async (req: IncomingMessage, res: ServerResponse) => {
    const url = new URL(req.url ?? "/", "http://upstream");
    const json = (payload: unknown) => {
      res.writeHead(200, { "content-type": "application/json" });
      res.end(JSON.stringify(payload));
    };
    if (url.pathname === "/cgi-bin/gettoken") {
      state.tokenRequests += 1;
      if (state.failToken > 0) {
        state.failToken -= 1;
        res.writeHead(500, { "content-type": "text/plain" });
        res.end("boom");
        return;
      }
      json({ access_token: "TEST_TOKEN", expires_in: 7200 });
      return;
    }
    if (url.pathname === "/cgi-bin/message/send") {
      const payload = JSON.parse(await readBody(req)) as Record<string, unknown>;
      state.sends.push({ payload, agentid: payload.agentid });
      json({ errcode: 0, errmsg: "ok" });
      return;
    }
    if (url.pathname === "/cgi-bin/media/upload") {
      state.uploads.push({ contentType: req.headers["content-type"] ?? null, body: await readBody(req) });
      json({ errcode: 0, media_id: "MEDIA_1" });
      return;
    }
    if (url.pathname === "/cgi-bin/media/get") {
      state.mediaDownloads.push(url.searchParams.get("media_id") ?? "");
      res.writeHead(200, { "content-type": "image/png", "content-disposition": 'attachment; filename="photo.png"' });
      res.end(PNG_BYTES);
      return;
    }
    if (url.pathname === "/media/pic.png") {
      res.writeHead(200, { "content-type": "image/png" });
      res.end(PNG_BYTES);
      return;
    }
    if (url.pathname === "/media/slow.png") {
      // 永不响应：客户端超时中止时连接被销毁（pic_url 超时降级用例）。
      req.on("close", () => res.destroy());
      return;
    }
    res.writeHead(404);
    res.end();
  });
  const listening = deferred<void>();
  server.once("listening", () => listening.resolve());
  server.listen(0, "127.0.0.1");
  await listening.promise;
  const port = (server.address() as { port: number }).port;
  return {
    port,
    state,
    close: () =>
      new Promise<void>((resolve) => {
        server.close(() => resolve());
        server.closeAllConnections();
      }),
  };
}

interface Harness {
  adapter: ChannelAdapter;
  ctx: AdapterContext<WecomAppConnection>;
  connection: WecomAppConnection;
  dir: string;
  messages: NormalizedMessage[];
  errors: Error[];
  logs: [string, string][];
  nextMessage(): Promise<NormalizedMessage>;
}

async function makeHarness(t: TestContext, {
  overrides = {},
  secrets = BASE_SECRETS,
  upstreamPort,
  deps = {},
}: {
  overrides?: Partial<WecomAppConnection>;
  secrets?: Record<string, string>;
  upstreamPort?: number;
  deps?: Parameters<typeof createAdapter>[1];
}): Promise<Harness> {
  const dir = await mkdtemp(join(tmpdir(), "wecom-app-test-"));
  // Adapter state writes race teardown; retry ENOTEMPTY.
  t.after(() => rm(dir, { recursive: true, force: true, maxRetries: 10, retryDelay: 20 }));
  const connection = makeConnection(overrides);
  // Must precede createAdapter: the API client snapshots apiBaseUrl at
  // construction, and tests must never reach the real WeCom API.
  if (upstreamPort !== undefined) {
    connection.apiBaseUrl = `http://127.0.0.1:${upstreamPort}`;
  }
  const messages: NormalizedMessage[] = [];
  const errors: Error[] = [];
  const logs: [string, string][] = [];
  let delivered = 0;
  const waiters: ((message: NormalizedMessage) => void)[] = [];
  const ctx: AdapterContext<WecomAppConnection> = {
    connectionId: connection.id,
    config: connection,
    secrets,
    dataDir: join(dir, "data"),
    mediaDir: join(dir, "media"),
    signal: new AbortController().signal,
    settings: async () => imSettings.schema.parse({}),
    log: (level, message) => logs.push([level, message]),
    onMessage: (message) => {
      messages.push(message);
      waiters.shift()?.(message);
    },
    onGroups: () => {},
    onError: (error) => errors.push(error),
  };
  const adapter = createAdapter(ctx, { port: 0, fetch_: fetch, replyWindowMs: 400, ...deps });
  t.after(() => adapter.stop().catch(() => {}));
  return {
    adapter,
    ctx,
    connection,
    dir,
    messages,
    errors,
    logs,
    nextMessage: () => {
      if (delivered < messages.length) {
        return Promise.resolve(messages[delivered++]!);
      }
      return new Promise((resolve) => {
        waiters.push((message) => {
          delivered += 1;
          resolve(message);
        });
      });
    },
  };
}

function postCallback({
  base,
  path,
  crypto,
  plaintext,
  format = "json",
  timestamp = "1700000000",
  nonce = "nonce1",
  signatureOverride,
}: {
  base: string;
  path: string;
  crypto: WecomCrypto;
  plaintext: unknown;
  format?: "json" | "xml";
  timestamp?: string;
  nonce?: string;
  signatureOverride?: string;
}): Promise<Response> {
  const plain = typeof plaintext === "string" ? plaintext : JSON.stringify(plaintext);
  const { encrypt, signature } = crypto.encrypt(plain, timestamp, nonce);
  const body = format === "json"
    ? JSON.stringify({ encrypt, msgsignature: signatureOverride ?? signature, timestamp, nonce })
    : `<xml><Encrypt><![CDATA[${encrypt}]]></Encrypt><MsgSignature><![CDATA[${signatureOverride ?? signature}]]></MsgSignature><TimeStamp>${timestamp}</TimeStamp><Nonce><![CDATA[${nonce}]]></Nonce></xml>`;
  return fetch(`${base}${path}`, {
    method: "POST",
    headers: { "content-type": format === "json" ? "application/json" : "text/xml" },
    body,
  });
}

async function replyPayload(response: Response, crypto: WecomCrypto): Promise<Record<string, unknown>> {
  const contentType = response.headers.get("content-type") ?? "";
  const text = await response.text();
  assert.equal(response.status, 200);
  const encrypt = contentType.includes("xml")
    ? parseWecomXmlBody(text).Encrypt!
    : (JSON.parse(text) as { encrypt: string }).encrypt;
  return JSON.parse(crypto.decrypt(encrypt)) as Record<string, unknown>;
}

test("configure 校验必填字段并拆分机密、派生 callbackSecret", () => {
  assert.throws(() => definition.configure({}), (error: unknown) => {
    assert.ok(error instanceof UserFacingError);
    assert.match(error.message, /缺少必填字段：企业 ID/);
    return true;
  });
  const fields = {
    corpId: ` ${CORP_ID} `,
    agentId: AGENT_ID,
    secret: "  app-secret  ",
    token: TOKEN,
    encodingAESKey: AES_KEY,
    callbackBaseUrl: "https://gw.example.com/",
  };
  const { config, secrets } = definition.configure(fields);
  assert.equal(config.corpId, CORP_ID);
  assert.equal(secrets.secret, "app-secret");
  assert.equal(secrets.token, TOKEN);
  assert.equal(secrets.encodingAESKey, AES_KEY);
  assert.match(config.callbackSecret as string, /^[0-9a-f]{32}$/);
  assert.equal(config.callbackBaseUrl, "https://gw.example.com/");
  // Preserved callbackSecret survives reconfigure; invalid AES key rejected.
  const again = definition.configure({ ...fields, callbackSecret: "ABCDEF0123456789ABCDEF0123456789" });
  assert.equal(again.config.callbackSecret, "abcdef0123456789abcdef0123456789");
  assert.throws(
    () => definition.configure({ ...fields, encodingAESKey: "short-key" }),
    UserFacingError,
  );
  assert.throws(
    () => definition.configure({ ...fields, callbackBaseUrl: "not-a-url" }),
    (error: unknown) => {
      assert.ok(error instanceof UserFacingError);
      assert.match(error.message, /回调基址/);
      return true;
    },
  );
});

test("start 校验凭据：缺回调基址、缺 Secret、坏 EncodingAESKey 均报 UserFacingError", async (t) => {
  const cases: { secrets?: Record<string, string>; overrides?: Partial<WecomAppConnection>; match: RegExp }[] = [
    { overrides: { callbackBaseUrl: undefined }, match: /公网可达的回调基址/ },
    { secrets: { token: TOKEN, encodingAESKey: AES_KEY }, match: /缺少应用 Secret/ },
    { secrets: { secret: "s", token: TOKEN, encodingAESKey: "bad" }, match: /EncodingAESKey 不合法/ },
  ];
  for (const testCase of cases) {
    const harness = await makeHarness(t, { overrides: testCase.overrides, secrets: testCase.secrets });
    await assert.rejects(
      () => harness.adapter.start(),
      (error: unknown) => {
        assert.ok(error instanceof UserFacingError);
        assert.match(error.message, testCase.match);
        return true;
      },
    );
  }
});

test("GET URL 验证：验签解密回明明文，坏签名 401", async (t) => {
  const upstream = await startUpstream();
  t.after(() => upstream.close());
  const harness = await makeHarness(t, { upstreamPort: upstream.port });
  await harness.adapter.start();
  const adapter = harness.adapter as unknown as { callbackPort: number; callbackPath: string };
  const base = `http://127.0.0.1:${adapter.callbackPort}`;
  const crypto = new WecomCrypto(TOKEN, AES_KEY, CORP_ID);
  const timestamp = "1700000001";
  const nonce = "nonce-verify";
  const { encrypt, signature } = crypto.encrypt("echo-plain-text", timestamp, nonce);
  const url = `${base}${adapter.callbackPath}?msg_signature=${signature}&timestamp=${timestamp}&nonce=${nonce}&echostr=${encodeURIComponent(encrypt)}`;
  const ok = await fetch(url);
  assert.equal(ok.status, 200);
  assert.match(ok.headers.get("content-type") ?? "", /text\/plain/);
  assert.equal(await ok.text(), "echo-plain-text");
  const bad = await fetch(
    `${base}${adapter.callbackPath}?msg_signature=${"0".repeat(64)}&timestamp=${timestamp}&nonce=${nonce}&echostr=${encodeURIComponent(encrypt)}`,
  );
  assert.equal(bad.status, 401);
});

test("单聊文本：normalize 并通过被动回复返回引擎文本（无主动发送）", async (t) => {
  const upstream = await startUpstream();
  t.after(() => upstream.close());
  const harness = await makeHarness(t, { upstreamPort: upstream.port });
  await harness.adapter.start();
  const adapter = harness.adapter as unknown as { callbackPort: number; callbackPath: string };
  const crypto = new WecomCrypto(TOKEN, AES_KEY, CORP_ID);
  const pendingReply = postCallback({
    base: `http://127.0.0.1:${adapter.callbackPort}`,
    path: adapter.callbackPath,
    crypto,
    plaintext: {
      msgid: "MSG_DM_1",
      msgtype: "text",
      from: { userid: "zhangsan", name: "张三" },
      chattype: "single",
      text: { content: "帮我看看这个报表" },
      createTime: "1700000000",
    },
  });
  const message = await harness.nextMessage();
  assert.equal(message.kind, "dm");
  assert.equal(message.chatId, "zhangsan");
  assert.equal(message.senderId, "zhangsan");
  assert.equal(message.senderName, "张三");
  assert.equal(message.text, "帮我看看这个报表");
  assert.equal(message.messageId, "MSG_DM_1");
  assert.equal(message.timestamp, 1_700_000_000_000);
  assert.equal(message.trigger.mention, true);
  assert.equal(message.trigger.replyToBot, false);
  assert.equal(message.trigger.command, false);
  await harness.adapter.sendText({ chatId: "zhangsan", text: "已收到，正在处理。" });
  const response = await pendingReply;
  const payload = await replyPayload(response, crypto);
  assert.deepEqual(payload, { msgtype: "text", text: { content: "已收到，正在处理。" } });
  assert.equal(upstream.state.sends.length, 0);
});

test("回复引用（quote）：replyToBot 触发且 replyTo 带引用文本", async (t) => {
  const upstream = await startUpstream();
  t.after(() => upstream.close());
  const harness = await makeHarness(t, { upstreamPort: upstream.port });
  await harness.adapter.start();
  const adapter = harness.adapter as unknown as { callbackPort: number; callbackPath: string };
  const crypto = new WecomCrypto(TOKEN, AES_KEY, CORP_ID);
  const pendingReply = postCallback({
    base: `http://127.0.0.1:${adapter.callbackPort}`,
    path: adapter.callbackPath,
    crypto,
    plaintext: {
      msgid: "MSG_QUOTE_1",
      msgtype: "text",
      from: { userid: "lisi" },
      chattype: "single",
      text: { content: "再说一遍" },
      quote: { msgid: "BOT_OLD", content: "上一条回答", userid: "assistant" },
    },
  });
  const message = await harness.nextMessage();
  assert.equal(message.trigger.replyToBot, true);
  assert.equal(message.trigger.command, false);
  assert.deepEqual(message.replyTo, { id: "BOT_OLD", text: "上一条回答", sender: "assistant" });
  await harness.adapter.sendText({ chatId: "lisi", text: "好的" });
  await replyPayload(await pendingReply, crypto);
});

test("群聊文本：chatid 作为 chatId，超时后走主动发送（chatid + agentid + safe:0）", async (t) => {
  const upstream = await startUpstream();
  t.after(() => upstream.close());
  const harness = await makeHarness(t, { upstreamPort: upstream.port });
  await harness.adapter.start();
  const adapter = harness.adapter as unknown as { callbackPort: number; callbackPath: string };
  const crypto = new WecomCrypto(TOKEN, AES_KEY, CORP_ID);
  const pendingReply = postCallback({
    base: `http://127.0.0.1:${adapter.callbackPort}`,
    path: adapter.callbackPath,
    crypto,
    plaintext: {
      msgid: "MSG_GROUP_1",
      msgtype: "text",
      from: { userid: "wangwu" },
      chatid: "wrJk8dEAAAxxxxxxxx",
      chattype: "group",
      text: { content: "@助手 会一下" },
    },
  });
  const message = await harness.nextMessage();
  assert.equal(message.kind, "group");
  assert.equal(message.chatId, "wrJk8dEAAAxxxxxxxx");
  assert.equal(message.senderId, "wangwu");
  assert.equal(message.trigger.mention, true);
  const response = await pendingReply;
  assert.equal(response.status, 200);
  assert.equal(await response.text(), ""); // reply window expired → empty ack
  await harness.adapter.sendText({ chatId: "wrJk8dEAAAxxxxxxxx", text: "群聊回复" });
  assert.equal(upstream.state.sends.length, 1);
  const { payload } = upstream.state.sends[0]!;
  assert.equal(payload.chatid, "wrJk8dEAAAxxxxxxxx");
  assert.equal(payload.msgtype, "text");
  assert.deepEqual(payload.text, { content: "群聊回复" });
  assert.equal(payload.safe, 0);
  assert.equal(payload.agentid, Number(AGENT_ID));
});

test("XML 信封入站：解密、解析 XML 明文并回 XML 加密应答", async (t) => {
  const upstream = await startUpstream();
  t.after(() => upstream.close());
  const harness = await makeHarness(t, { upstreamPort: upstream.port });
  await harness.adapter.start();
  const adapter = harness.adapter as unknown as { callbackPort: number; callbackPath: string };
  const crypto = new WecomCrypto(TOKEN, AES_KEY, CORP_ID);
  const xmlPlain = "<xml><ToUserName><![CDATA[corp123456]]></ToUserName><FromUserName><![CDATA[zhaoliu]]></FromUserName><CreateTime>1700000002</CreateTime><MsgType><![CDATA[text]]></MsgType><Content><![CDATA[XML 你好]]></Content><MsgId>70001</MsgId><AgentID>1000002</AgentID></xml>";
  const pendingReply = postCallback({
    base: `http://127.0.0.1:${adapter.callbackPort}`,
    path: adapter.callbackPath,
    crypto,
    plaintext: xmlPlain,
    format: "xml",
    nonce: "nonce-xml",
  });
  const message = await harness.nextMessage();
  assert.equal(message.kind, "dm");
  assert.equal(message.chatId, "zhaoliu");
  assert.equal(message.text, "XML 你好");
  assert.equal(message.messageId, "70001");
  await harness.adapter.sendText({ chatId: "zhaoliu", text: "XML 收到" });
  const response = await pendingReply;
  assert.match(response.headers.get("content-type") ?? "", /xml/);
  const fields = parseWecomXmlBody(await response.text());
  assert.match(String(fields.Encrypt), /\S/);
  assert.deepEqual(JSON.parse(crypto.decrypt(fields.Encrypt!)), { msgtype: "text", text: { content: "XML 收到" } });
  assert.equal(upstream.state.sends.length, 0);
});

test("图片消息：pic_url 下载进 mediaDir，media_id 走鉴权下载", async (t) => {
  const upstream = await startUpstream();
  t.after(() => upstream.close());
  const harness = await makeHarness(t, { upstreamPort: upstream.port });
  await harness.adapter.start();
  const adapter = harness.adapter as unknown as { callbackPort: number; callbackPath: string };
  const crypto = new WecomCrypto(TOKEN, AES_KEY, CORP_ID);
  const base = `http://127.0.0.1:${adapter.callbackPort}`;
  const first = postCallback({
    base,
    path: adapter.callbackPath,
    crypto,
    plaintext: {
      msgid: "MSG_PIC_URL",
      msgtype: "image",
      from: { userid: "zhangsan" },
      chattype: "single",
      image: { pic_url: `http://127.0.0.1:${upstream.port}/media/pic.png` },
    },
  });
  const message = await harness.nextMessage();
  assert.equal(message.media?.length, 1);
  const media = message.media![0]!;
  assert.equal(media.kind, "image");
  assert.equal(media.mime, "image/png");
  const stored = await readFile(media.path);
  assert.deepEqual([...stored.subarray(0, 4)], [...PNG_BYTES.subarray(0, 4)]);
  const firstResponse = await first;
  assert.equal(firstResponse.status, 200);
  await firstResponse.text();
  // media_id flavor: authenticated download via cgi-bin media/get.
  const second = postCallback({
    base,
    path: adapter.callbackPath,
    crypto,
    nonce: "nonce2",
    plaintext: {
      msgid: "MSG_PIC_MEDIA",
      msgtype: "image",
      from: { userid: "zhangsan" },
      chattype: "single",
      image: { media_id: "IMG_9" },
    },
  });
  const message2 = await harness.nextMessage();
  assert.equal(message2.media?.[0]?.name, "photo.png");
  assert.equal(upstream.state.mediaDownloads[0], "IMG_9");
  await second;
});

test("语音消息：识别文本作为正文；事件与噪音不 emit", async (t) => {
  const upstream = await startUpstream();
  t.after(() => upstream.close());
  const harness = await makeHarness(t, { upstreamPort: upstream.port });
  await harness.adapter.start();
  const adapter = harness.adapter as unknown as { callbackPort: number; callbackPath: string };
  const crypto = new WecomCrypto(TOKEN, AES_KEY, CORP_ID);
  const base = `http://127.0.0.1:${adapter.callbackPort}`;
  const voice = postCallback({
    base,
    path: adapter.callbackPath,
    crypto,
    plaintext: {
      msgid: "MSG_VOICE_1",
      msgtype: "voice",
      from: { userid: "zhangsan" },
      chattype: "single",
      recognition: "明天提醒我开会",
    },
  });
  const message = await harness.nextMessage();
  assert.equal(message.text, "明天提醒我开会");
  assert.equal(message.media, undefined);
  await voice;
  // Voice without recognition, unknown msgtype and event payloads are noise.
  for (const [index, plaintext] of [
    { msgid: "MSG_VOICE_2", msgtype: "voice", from: { userid: "zhangsan" }, chattype: "single" },
    { msgid: "MSG_CARD_1", msgtype: "template_card", from: { userid: "zhangsan" }, chattype: "single" },
    { msgid: "MSG_EVENT_1", msgtype: "event", event: "subscribe", from: { userid: "zhangsan" } },
  ].entries()) {
    const response = await postCallback({
      base,
      path: adapter.callbackPath,
      crypto,
      nonce: `noise-${index}`,
      plaintext,
    });
    assert.equal(response.status, 200);
    assert.equal(await response.text(), "");
  }
  assert.equal(harness.messages.length, 1);
});

test("MsgId 重试去重：同一消息只 emit 一次，重试回空响应", async (t) => {
  const upstream = await startUpstream();
  t.after(() => upstream.close());
  const harness = await makeHarness(t, { upstreamPort: upstream.port });
  await harness.adapter.start();
  const adapter = harness.adapter as unknown as { callbackPort: number; callbackPath: string };
  const crypto = new WecomCrypto(TOKEN, AES_KEY, CORP_ID);
  const base = `http://127.0.0.1:${adapter.callbackPort}`;
  const plaintext = {
    msgid: "MSG_DUP_1",
    msgtype: "text",
    from: { userid: "zhangsan" },
    chattype: "single",
    text: { content: "只处理一次" },
  };
  const first = postCallback({ base, path: adapter.callbackPath, crypto, plaintext });
  await harness.nextMessage();
  await first;
  const retry = await postCallback({
    base,
    path: adapter.callbackPath,
    crypto,
    plaintext,
    nonce: "nonce-retry",
    timestamp: "1700000010",
  });
  assert.equal(retry.status, 200);
  assert.equal(await retry.text(), "");
  assert.equal(harness.messages.length, 1);
  assert.equal(upstream.state.sends.length, 0);
});

test("签名不符与畸形载荷被拒绝", async (t) => {
  const upstream = await startUpstream();
  t.after(() => upstream.close());
  const harness = await makeHarness(t, { upstreamPort: upstream.port });
  await harness.adapter.start();
  const adapter = harness.adapter as unknown as { callbackPort: number; callbackPath: string };
  const crypto = new WecomCrypto(TOKEN, AES_KEY, CORP_ID);
  const base = `http://127.0.0.1:${adapter.callbackPort}`;
  const bad = await postCallback({
    base,
    path: adapter.callbackPath,
    crypto,
    signatureOverride: "f".repeat(64),
    plaintext: { msgid: "X", msgtype: "text", text: { content: "hi" } },
  });
  assert.equal(bad.status, 401);
  const malformed = await fetch(`${base}${adapter.callbackPath}`, { method: "POST", body: "not-json" });
  assert.equal(malformed.status, 400);
  const unknownRoute = await fetch(`${base}/wecom-app-callback/other/${"a".repeat(32)}`);
  assert.equal(unknownRoute.status, 404);
  assert.equal(harness.messages.length, 0);
});

test("分块：超长文本按 2048 字节切分且代码围栏不被截断；token 跨请求缓存", async (t) => {
  const upstream = await startUpstream();
  t.after(() => upstream.close());
  const harness = await makeHarness(t, { upstreamPort: upstream.port });
  await harness.adapter.start();
  const longText = [
    "开头说明",
    "```js",
    "x".repeat(1500),
    "```",
    "结尾说明",
  ].join("\n");
  await harness.adapter.sendText({ chatId: "zhangsan", text: longText });
  const sends = upstream.state.sends;
  assert.ok(sends.length >= 2);
  for (const { payload } of sends) {
    assert.equal(payload.msgtype, "text");
    assert.ok(Buffer.byteLength((payload.text as { content: string }).content, "utf8") <= 2048);
  }
  const joined = sends.map(({ payload }) => (payload.text as { content: string }).content).join("\n");
  assert.ok(joined.includes("开头说明"));
  assert.ok(joined.includes("结尾说明"));
  // A short fenced block stays intact inside a single chunk.
  await harness.adapter.sendText({ chatId: "zhangsan", text: "a\n```\nshort\n```\nb" });
  const last = upstream.state.sends.at(-1)!;
  const lastContent = (last.payload.text as { content: string }).content;
  assert.match(lastContent, /```\nshort\n```/);
  // CJK text splits on UTF-8 byte budget without breaking code points.
  await harness.adapter.sendText({ chatId: "zhangsan", text: "好".repeat(1000) });
  const cjkSends = upstream.state.sends.slice(-2);
  for (const { payload } of cjkSends) {
    const content = (payload.text as { content: string }).content;
    assert.ok(/^[好的]*$/.test(content));
    assert.ok(Buffer.byteLength(content, "utf8") <= 2048);
  }
  assert.equal(Buffer.byteLength((cjkSends[0]!.payload.text as { content: string }).content, "utf8"), 2040);
  // One token fetch serves all sends.
  assert.equal(upstream.state.tokenRequests, 1);
  assert.equal(upstream.state.sends.every(({ payload }) => payload.touser === "zhangsan"), true);
});

test("被动回复超长文本：首块被动回，剩余主动发送", async (t) => {
  const upstream = await startUpstream();
  t.after(() => upstream.close());
  const harness = await makeHarness(t, { upstreamPort: upstream.port });
  await harness.adapter.start();
  const adapter = harness.adapter as unknown as { callbackPort: number; callbackPath: string };
  const crypto = new WecomCrypto(TOKEN, AES_KEY, CORP_ID);
  const pending = postCallback({
    base: `http://127.0.0.1:${adapter.callbackPort}`,
    path: adapter.callbackPath,
    crypto,
    plaintext: { msgid: "MSG_LONG_1", msgtype: "text", from: { userid: "zhangsan" }, chattype: "single", text: { content: "讲个长故事" } },
  });
  await harness.nextMessage();
  await harness.adapter.sendText({ chatId: "zhangsan", text: "好".repeat(1000) });
  const payload = await replyPayload(await pending, crypto);
  const passive = (payload.text as { content: string }).content;
  assert.equal(Buffer.byteLength(passive, "utf8"), 2040);
  assert.equal(upstream.state.sends.length, 1);
  assert.equal(Buffer.byteLength((upstream.state.sends[0]!.payload.text as { content: string }).content, "utf8"), 960);
});

test("sendMedia：multipart 上传后按 media_id 发送", async (t) => {
  const upstream = await startUpstream();
  t.after(() => upstream.close());
  const harness = await makeHarness(t, { upstreamPort: upstream.port });
  await harness.adapter.start();
  const imagePath = join(harness.dir, "cat.png");
  await writeFile(imagePath, PNG_BYTES);
  await harness.adapter.sendMedia!({ chatId: "zhangsan", kind: "image", path: imagePath, name: "cat.png" });
  assert.equal(upstream.state.uploads.length, 1);
  assert.match(upstream.state.uploads[0]!.contentType ?? "", /multipart\/form-data/);
  assert.ok(upstream.state.uploads[0]!.body.includes("cat.png"));
  const send = upstream.state.sends.at(-1)!;
  assert.equal(send.payload.msgtype, "image");
  assert.deepEqual(send.payload.image, { media_id: "MEDIA_1" });
  assert.equal(send.payload.touser, "zhangsan");
});

test("启动健康检查：瞬时失败退避重试后恢复；testConnection 反映状态", async (t) => {
  const upstream = await startUpstream();
  upstream.state.failToken = 3;
  t.after(() => upstream.close());
  const harness = await makeHarness(t, {
    upstreamPort: upstream.port,
    deps: { healthRetryDelays: [5, 5, 5] },
  });
  const recovered = deferred<void>();
  const originalLog = harness.ctx.log;
  harness.ctx.log = (level, message) => {
    originalLog(level, message);
    if (message.includes("凭据校验通过")) recovered.resolve();
  };
  await harness.adapter.start();
  const failing = await harness.adapter.testConnection!();
  assert.equal(failing.ok, false);
  await recovered.promise;
  assert.ok(harness.errors.length >= 2);
  assert.ok(harness.errors.every((error) => /HTTP 500|网络请求失败|无法解析/.test(error.message)));
  const ok = await harness.adapter.testConnection!();
  assert.equal(ok.ok, true);
  await harness.adapter.sendText({ chatId: "zhangsan", text: "恢复后发送" });
  assert.equal(upstream.state.sends.length, 1);
});

test("多连接共享同一回调监听；stop 后路由移除", async (t) => {
  const upstream = await startUpstream();
  t.after(() => upstream.close());
  const harnessA = await makeHarness(t, { upstreamPort: upstream.port });
  const harnessB = await makeHarness(t, {
    upstreamPort: upstream.port,
    overrides: { id: CONNECTION_ID_B },
  });
  await harnessA.adapter.start();
  await harnessB.adapter.start();
  const portA = (harnessA.adapter as unknown as { callbackPort: number }).callbackPort;
  const portB = (harnessB.adapter as unknown as { callbackPort: number }).callbackPort;
  assert.equal(portA, portB); // same shared listener instance
  const pathB = (harnessB.adapter as unknown as { callbackPath: string }).callbackPath;
  assert.notEqual(pathB, (harnessA.adapter as unknown as { callbackPath: string }).callbackPath);
  const probe = await fetch(`http://127.0.0.1:${portB}${pathB}`);
  assert.equal(probe.status, 400); // GET without params on a live route
  await harnessB.adapter.stop();
  const gone = await fetch(`http://127.0.0.1:${portA}${pathB}`);
  assert.equal(gone.status, 404);
  const alive = await fetch(`http://127.0.0.1:${portA}${(harnessA.adapter as unknown as { callbackPath: string }).callbackPath}`);
  assert.equal(alive.status, 400); // A's route survives B's stop
});

test("图片下载挂死：超时后降级为无图文本，不静默丟消息", async (t) => {
  const upstream = await startUpstream();
  t.after(() => upstream.close());
  const harness = await makeHarness(t, {
    upstreamPort: upstream.port,
    deps: { picDownloadTimeoutMs: 50 },
  });
  await harness.adapter.start();
  const adapter = harness.adapter as unknown as { callbackPort: number; callbackPath: string };
  const crypto = new WecomCrypto(TOKEN, AES_KEY, CORP_ID);
  const pending = postCallback({
    base: `http://127.0.0.1:${adapter.callbackPort}`,
    path: adapter.callbackPath,
    crypto,
    plaintext: {
      msgid: "MSG_PIC_SLOW",
      msgtype: "image",
      from: { userid: "zhangsan" },
      chattype: "single",
      image: { pic_url: `http://127.0.0.1:${upstream.port}/media/slow.png` },
    },
  });
  const message = await harness.nextMessage();
  assert.equal(message.text, "[图片]", "超时降级为无图占位文本");
  assert.equal(message.media, undefined);
  assert.ok(harness.logs.some(([level, m]) => level === "warn" && m.includes("图片下载失败")));
  const response = await pending;
  assert.equal(response.status, 200);
  await response.text();
});

test("回调监听日志脱敏：callbackSecret 段不进日志", async (t) => {
  const upstream = await startUpstream();
  t.after(() => upstream.close());
  const harness = await makeHarness(t, { upstreamPort: upstream.port });
  await harness.adapter.start();
  const joined = harness.logs.map(([level, message]) => message).join("\n");
  assert.ok(joined.includes("企业微信应用回调已监听"));
  assert.ok(joined.includes("/****"), "路由密钥段应打码");
  assert.ok(!joined.includes(CALLBACK_SECRET), "日志不得包含完整 callbackSecret");
});

test("api：200 + 非 JSON 响应按 bad-response 抛错", async (t) => {
  const server: Server = createServer((_req: IncomingMessage, res: ServerResponse) => {
    res.writeHead(200, { "content-type": "text/html" });
    res.end("<html>gateway busy</html>");
  });
  const listening = deferred<void>();
  server.once("listening", () => listening.resolve());
  server.listen(0, "127.0.0.1");
  await listening.promise;
  const port = (server.address() as { port: number }).port;
  t.after(() => new Promise<void>((resolve) => server.close(() => resolve())));
  const api = new WecomAppApi({
    corpId: CORP_ID,
    corpSecret: "secret",
    agentId: AGENT_ID,
    apiBaseUrl: `http://127.0.0.1:${port}`,
  });
  await assert.rejects(
    api.sendText({ userId: "zhangsan", content: "hi" }),
    (error: unknown) => error instanceof WecomAppApiError && error.code === "bad-response" && /无法解析/.test(error.message),
  );
});

test("回调监听冷端口并发 acquire 串行化：共享同一实例且无 EADDRINUSE", async (t) => {
  // 先占住再释放一个端口，得到确定空闲的端口号。
  const probe: Server = createServer();
  const listening = deferred<void>();
  probe.once("listening", () => listening.resolve());
  probe.listen(0, "127.0.0.1");
  await listening.promise;
  const port = (probe.address() as { port: number }).port;
  await new Promise<void>((resolve) => probe.close(() => resolve()));

  const noopLog = () => {};
  const [a, b] = await Promise.all([
    acquireCallbackServer({ port, log: noopLog }),
    acquireCallbackServer({ port, log: noopLog }),
  ]);
  assert.equal(a, b, "并发 acquire 应复用同一监听实例");
  assert.equal(a.actualPort, port);
  await releaseCallbackServer(port);
  await releaseCallbackServer(port); // 幂等
  // 释放后可重新绑定同端口（确认引用计数正确关闭）。
  const again = await acquireCallbackServer({ port, log: noopLog });
  assert.equal(again.actualPort, port);
  await releaseCallbackServer(port);
});

test("状态文件损坏：重置空状态并告警，入站消息照常处理", async (t) => {
  const upstream = await startUpstream();
  t.after(() => upstream.close());
  const harness = await makeHarness(t, { upstreamPort: upstream.port });
  await mkdir(join(harness.dir, "data"), { recursive: true });
  await writeFile(join(harness.dir, "data", "wecom-app-state.json"), "{{{not-json");
  await harness.adapter.start();
  assert.ok(
    harness.logs.some(([level, m]) => level === "warn" && m.includes("状态文件损坏")),
    `应告警状态损坏 logs=${JSON.stringify(harness.logs)}`,
  );
  // 去重与会话记录已重置：入站消息不被旧状态阻断。
  const adapter = harness.adapter as unknown as { callbackPort: number; callbackPath: string };
  const crypto = new WecomCrypto(TOKEN, AES_KEY, CORP_ID);
  const pending = postCallback({
    base: `http://127.0.0.1:${adapter.callbackPort}`,
    path: adapter.callbackPath,
    crypto,
    plaintext: { msgid: "MSG_AFTER_CORRUPT", msgtype: "text", from: { userid: "zhangsan" }, chattype: "single", text: { content: "还能收到吗" } },
  });
  const message = await harness.nextMessage();
  assert.equal(message.text, "还能收到吗");
  await pending;
});
