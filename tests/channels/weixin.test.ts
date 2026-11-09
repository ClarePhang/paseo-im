/** Weixin channel adapter tests. All transport is faked through deps; no
 * real network access happens.
 *
 * Timing: assertions are driven by observable conditions (emitted messages,
 * recorder calls, persisted state) via waitFor; the remaining fixed waits are
 * negative assertions (nothing more may happen after a stop), which have no
 * observable completion signal to await. */
import assert from "node:assert/strict";
import { createCipheriv, createDecipheriv, randomBytes } from "node:crypto";
import { mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, it } from "node:test";
import { emptyPolicy } from "../../shared/config.ts";
import { UserFacingError } from "../../shared/errors.ts";
import type { NormalizedMessage } from "../../shared/model.ts";
import { imSettings } from "../../shared/settings.ts";
import {
  BASE_INFO,
  TEXT_SEND_RETRY_DELAYS_MS,
  WEIXIN_MAX_CHUNK_CHARS,
  WeixinApiError,
} from "../../server/channels/weixin/api.ts";
import type { RawRecord, WeixinHttp, WeixinHttpOptions } from "../../server/channels/weixin/api.ts";
import type { PairingStatus } from "../../server/channels/types.ts";
import type { WeixinConnection, WeixinDeps } from "../../server/channels/weixin/index.ts";
import { createAdapter, createPairingSession, definition } from "../../server/channels/weixin/index.ts";
import type { AdapterContext } from "../../server/channels/types.ts";
import { splitChunks } from "../../server/format/chunks.ts";

interface HttpCall {
  path: string;
  body: unknown;
  init?: WeixinHttpOptions;
}

function recorder(handler: (call: HttpCall) => unknown): { calls: HttpCall[]; http: WeixinHttp } {
  const calls: HttpCall[] = [];
  const http: WeixinHttp = async (path, body, init) => {
    const call: HttpCall = { path, body, init };
    calls.push(call);
    return (await handler(call)) as RawRecord;
  };
  return { calls, http };
}

/** Never-resolving long poll that rejects on abort, like a real socket. */
function hang(init?: WeixinHttpOptions): Promise<RawRecord> {
  // Promise.withResolvers is absent from the project's ES2023 lib target.
  return new Promise((_, reject) => {
    init?.signal?.addEventListener(
      "abort",
      () => reject(new DOMException("Aborted", "AbortError")),
      { once: true },
    );
  });
}

/** Fixed wait; only used for negative assertions with no observable signal. */
const delay = (ms: number): Promise<void> => {
  // See hang(): ES2023 lib has no Promise.withResolvers.
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
};

async function waitFor(
  condition: () => boolean | Promise<boolean>,
  timeoutMs = 2_000,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!(await condition())) {
    if (Date.now() > deadline) throw new Error("waitFor: condition not met in time");
    await delay(5);
  }
}

/** 轮询配对快照直至命中条件并返回该快照。后台单飞推进状态机，快照轮询只读，
 * 直接返回值避免闭包赋值导致的 TS 窄化失效。 */
async function pollUntil(
  session: { poll(): Promise<PairingStatus> },
  match: (status: PairingStatus) => boolean,
  timeoutMs = 3_000,
): Promise<PairingStatus> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const snapshot = await session.poll();
    if (match(snapshot)) return snapshot;
    if (Date.now() > deadline) throw new Error("pollUntil: condition not met in time");
    await delay(5);
  }
}

/** Wraps a per-endpoint handler: scripted answers win, lifecycle endpoints
 * default to an accepted empty response. */
function weixinHandler(respond: (call: HttpCall) => unknown) {
  return (call: HttpCall): unknown => {
    const scripted = respond(call);
    return scripted !== undefined ? scripted : {};
  };
}

/** Scripted getupdates: queued responses (or thrown errors), then hang. */
function updatesScript(responses: unknown[]) {
  let index = 0;
  return (call: HttpCall): unknown => {
    if (call.path !== "ilink/bot/getupdates") return {};
    const next = responses[index];
    index += 1;
    if (next === undefined) return hang(call.init);
    if (next instanceof Error) throw next;
    return next;
  };
}

/** Recorder request bodies are fixtures written by these tests. */
function sendBody(call: HttpCall): RawRecord {
  const envelope = call.body as RawRecord;
  return envelope.msg as RawRecord;
}

function adapterDeps(http: WeixinHttp, extra: Partial<WeixinDeps> = {}): Partial<WeixinDeps> {
  return {
    http,
    sleep: async () => {},
    download: async () => {
      throw new Error("unexpected media download");
    },
    upload: async () => {
      throw new Error("unexpected CDN upload");
    },
    ...extra,
  };
}

interface Env {
  dir: string;
  dataDir: string;
  mediaDir: string;
  ctx: AdapterContext<WeixinConnection>;
  messages: NormalizedMessage[];
  errors: Error[];
  logs: string[];
}

async function makeEnv(options: { secrets?: Record<string, string>; baseUrl?: string } = {}): Promise<Env> {
  const dir = await mkdtemp(join(tmpdir(), "paseo-weixin-test-"));
  const dataDir = join(dir, "data");
  const mediaDir = join(dir, "media");
  await mkdir(dataDir, { recursive: true });
  await mkdir(mediaDir, { recursive: true });
  const connection: WeixinConnection = {
    id: "1b8f4c3e-0000-4000-8000-000000000001",
    name: "微信测试连接",
    enabled: true,
    target: null,
    policy: emptyPolicy(),
    bindings: {},
    revision: 0,
    channel: "weixin",
    accountId: "acct-1",
    ownerUserId: "owner-1",
    ...(options.baseUrl ? { baseUrl: options.baseUrl } : {}),
  };
  const messages: NormalizedMessage[] = [];
  const errors: Error[] = [];
  const logs: string[] = [];
  const ctx: AdapterContext<WeixinConnection> = {
    connectionId: connection.id,
    config: connection,
    secrets: options.secrets ?? { token: "token-abc" },
    dataDir,
    mediaDir,
    signal: new AbortController().signal,
    settings: async () => imSettings.schema.parse({}),
    log: (level, message) => logs.push(`${level}:${message}`),
    onMessage: (message) => messages.push(message),
    onGroups: () => {},
    onError: (error) => errors.push(error),
  };
  return { dir, dataDir, mediaDir, ctx, messages, errors, logs };
}

async function readStateDoc(dataDir: string): Promise<RawRecord> {
  const raw = await readFile(join(dataDir, "weixin-state.json"), "utf8");
  return JSON.parse(raw) as RawRecord;
}

const IMAGE_KEY_HEX = "00112233445566778899aabbccddeeff";

function aesEncrypt(plain: Buffer, keyHex: string): Buffer {
  const cipher = createCipheriv("aes-128-ecb", Buffer.from(keyHex, "hex"), null);
  return Buffer.concat([cipher.update(plain), cipher.final()]);
}

function aesDecrypt(ciphertext: Buffer, keyHex: string): Buffer {
  const decipher = createDecipheriv("aes-128-ecb", Buffer.from(keyHex, "hex"), null);
  return Buffer.concat([decipher.update(ciphertext), decipher.final()]);
}

function textMessage(overrides: RawRecord = {}): RawRecord {
  return {
    message_id: "7260000000000000001",
    from_user_id: "user-1",
    to_user_id: "bot-1",
    message_type: 1,
    context_token: "ctx-token-1",
    run_id: "run-1",
    seq: 42,
    create_time_ms: 1_700_000_000_000,
    item_list: [{ type: 1, text_item: { text: "你好" } }],
    ...overrides,
  };
}

function updatesResponse(msgs: unknown[], getUpdatesBuf = "BUF-1"): RawRecord {
  return { ret: 0, msgs, get_updates_buf: getUpdatesBuf };
}

describe("weixin adapter", () => {
  it("definition 暴露渠道能力", async (t) => {
    const env = await makeEnv();
    t.after(() => rm(env.dir, { recursive: true, force: true }));
    assert.equal(definition.channel, "weixin");
    assert.equal(typeof definition.create, "function");
    assert.equal(typeof definition.pairing, "function");
    assert.equal(typeof definition.configure, "function");
    const adapter = createAdapter(env.ctx, adapterDeps(recorder(() => ({})).http));
    assert.equal(adapter.channel, "weixin");
    assert.equal(adapter.capabilities.maxChunkChars, WEIXIN_MAX_CHUNK_CHARS);
    assert.equal(adapter.capabilities.maxChunkChars, 1_800);
    assert.equal(adapter.capabilities.typing, true);
    assert.equal(adapter.capabilities.markdown, "plain");
    assert.equal(adapter.capabilities.progress, "typing");
    assert.equal(adapter.capabilities.threads, false);
    assert.equal(adapter.capabilities.reactions, false);
    assert.equal(adapter.capabilities.uploadImage, true);
    assert.equal(adapter.capabilities.uploadFile, true);
  });

  it("configure：拆分机密、校验必填与 baseUrl 白名单", () => {
    const configured = definition.configure({
      token: " t1 ",
      accountId: " A1 ",
      ownerUserId: " O1 ",
      baseUrl: "https://ilinkai.weixin.qq.com/",
    });
    assert.deepEqual(configured.secrets, { token: "t1" });
    assert.deepEqual(configured.config, {
      accountId: "A1",
      ownerUserId: "O1",
      baseUrl: "https://ilinkai.weixin.qq.com/",
    });
    assert.throws(() => definition.configure({}), /缺少微信登录凭据/);
    assert.throws(() => definition.configure({ token: "t" }), /accountId/);
    assert.throws(
      () => definition.configure({ token: "t", accountId: "A", baseUrl: "http://evil.example/" }),
      /不受信任/,
    );
    assert.throws(
      () => definition.configure({ token: "t", accountId: "A", baseUrl: "https://proxy.example.com/" }),
      /不受信任/,
    );
  });

  it("normalize：私聊文本消息与游标持久化", async (t) => {
    const env = await makeEnv();
    t.after(() => rm(env.dir, { recursive: true, force: true }));
    const { calls, http } = recorder(weixinHandler(updatesScript([
      updatesResponse([textMessage()]),
    ])));
    const adapter = createAdapter(env.ctx, adapterDeps(http));
    await adapter.start();
    t.after(() => adapter.stop().catch(() => {}));
    await waitFor(() => env.messages.length === 1);
    await waitFor(() => calls.filter((call) => call.path === "ilink/bot/getupdates").length >= 2);
    const message = env.messages[0]!;
    assert.equal(message.channel, "weixin");
    assert.equal(message.kind, "dm");
    assert.equal(message.chatId, "user-1");
    assert.equal(message.senderId, "user-1");
    assert.equal(message.messageId, "7260000000000000001");
    assert.equal(message.text, "你好");
    assert.equal(message.timestamp, 1_700_000_000_000);
    assert.equal(message.media, undefined);
    assert.equal(message.replyTo, undefined);
    assert.deepEqual(message.trigger, { mention: false, replyToBot: false, command: false });
    const secondPoll = calls.filter((call) => call.path === "ilink/bot/getupdates")[1]!;
    assert.deepEqual(secondPoll.body, {
      get_updates_buf: "BUF-1",
      base_info: BASE_INFO,
    });
    const doc = await readStateDoc(env.dataDir);
    assert.equal(doc.getUpdatesBuf, "BUF-1");
    assert.ok((doc.seenMessageIds as string[]).includes("7260000000000000001"));
    await adapter.stop();
  });

  it("normalize：命令消息 trigger.command 为 true", async (t) => {
    const env = await makeEnv();
    t.after(() => rm(env.dir, { recursive: true, force: true }));
    const { http } = recorder(weixinHandler(updatesScript([
      updatesResponse([textMessage({ item_list: [{ type: 1, text_item: { text: "/状态" } }] })]),
    ])));
    const adapter = createAdapter(env.ctx, adapterDeps(http));
    await adapter.start();
    t.after(() => adapter.stop().catch(() => {}));
    await waitFor(() => env.messages.length === 1);
    assert.equal(env.messages[0]!.trigger.command, true);
    await adapter.stop();
  });

  it("normalize：同一消息只 emit 一次", async (t) => {
    const env = await makeEnv();
    t.after(() => rm(env.dir, { recursive: true, force: true }));
    const { calls, http } = recorder(weixinHandler(updatesScript([
      updatesResponse([textMessage()]),
      updatesResponse([textMessage()], "BUF-2"),
    ])));
    const adapter = createAdapter(env.ctx, adapterDeps(http));
    await adapter.start();
    t.after(() => adapter.stop().catch(() => {}));
    // Both scripted batches are consumed once the loop hangs on poll #3.
    await waitFor(() => calls.filter((call) => call.path === "ilink/bot/getupdates").length >= 3);
    assert.equal(env.messages.length, 1);
    await adapter.stop();
  });

  it("normalize：机器人自身消息与噪音不 emit", async (t) => {
    const env = await makeEnv();
    t.after(() => rm(env.dir, { recursive: true, force: true }));
    const own = textMessage({
      message_id: "7260000000000000009",
      message_type: 2,
      item_list: [{ type: 1, text_item: { text: "机器人回声" } }],
    });
    const noise = textMessage({
      message_id: "7260000000000000008",
      context_token: undefined,
      run_id: undefined,
      item_list: [],
    });
    const { http } = recorder(weixinHandler(updatesScript([
      updatesResponse([own, noise, textMessage()]),
    ])));
    const adapter = createAdapter(env.ctx, adapterDeps(http));
    await adapter.start();
    t.after(() => adapter.stop().catch(() => {}));
    await waitFor(() => env.messages.length === 1);
    await adapter.stop();
    assert.equal(env.messages[0]!.messageId, "7260000000000000001");
    const doc = await readStateDoc(env.dataDir);
    // The own echo is skipped before dedupe; the noise message is marked seen.
    assert.equal((doc.seenMessageIds as string[]).length, 2);
    assert.ok((doc.seenMessageIds as string[]).includes("7260000000000000008"));
    assert.ok(!(doc.seenMessageIds as string[]).includes("7260000000000000009"));
  });

  it("normalize：语音转写文本", async (t) => {
    const env = await makeEnv();
    t.after(() => rm(env.dir, { recursive: true, force: true }));
    const { http } = recorder(weixinHandler(updatesScript([
      updatesResponse([textMessage({
        message_id: "7260000000000000002",
        item_list: [{ type: 3, voice_item: { text: "语音转写内容" } }],
      })]),
    ])));
    const adapter = createAdapter(env.ctx, adapterDeps(http));
    await adapter.start();
    t.after(() => adapter.stop().catch(() => {}));
    await waitFor(() => env.messages.length === 1);
    assert.equal(env.messages[0]!.text, "语音转写内容");
    await adapter.stop();
  });

  it("normalize：图片媒体 AES 解密并落盘 mediaDir", async (t) => {
    const env = await makeEnv();
    t.after(() => rm(env.dir, { recursive: true, force: true }));
    const plain = Buffer.concat([
      Buffer.from([0xff, 0xd8, 0xff, 0xe0]),
      Buffer.from("0123456789abcdef".repeat(4)),
    ]);
    const ciphertext = aesEncrypt(plain, IMAGE_KEY_HEX);
    const downloadUrls: string[] = [];
    const { http } = recorder(weixinHandler(updatesScript([
      updatesResponse([textMessage({
        message_id: "7260000000000000003",
        context_token: undefined,
        run_id: undefined,
        item_list: [{
          type: 2,
          image_item: {
            media: {
              encrypt_query_param: "EQP-1",
              aes_key: Buffer.from(IMAGE_KEY_HEX).toString("base64"),
            },
          },
        }],
      })]),
    ])));
    const adapter = createAdapter(env.ctx, adapterDeps(http, {
      download: async (url) => {
        downloadUrls.push(String(url));
        return ciphertext;
      },
    }));
    await adapter.start();
    t.after(() => adapter.stop().catch(() => {}));
    await waitFor(() => env.messages.length === 1);
    assert.deepEqual(downloadUrls, [
      "https://novac2c.cdn.weixin.qq.com/c2c/download?encrypted_query_param=EQP-1",
    ]);
    const media = env.messages[0]!.media!;
    assert.equal(media.length, 1);
    assert.equal(media[0]!.kind, "image");
    assert.equal(media[0]!.mime, "image/jpeg");
    assert.equal(media[0]!.name, "image");
    assert.ok(media[0]!.path.startsWith(env.mediaDir));
    assert.ok((await readFile(media[0]!.path)).equals(plain));
    await adapter.stop();
  });

  it("normalize：文件媒体（hex aeskey + full_url）", async (t) => {
    const env = await makeEnv();
    t.after(() => rm(env.dir, { recursive: true, force: true }));
    const plain = Buffer.from("文件内容-0123456789abcdef".repeat(2));
    const ciphertext = aesEncrypt(plain, IMAGE_KEY_HEX);
    const { http } = recorder(weixinHandler(updatesScript([
      updatesResponse([textMessage({
        message_id: "7260000000000000004",
        context_token: undefined,
        run_id: undefined,
        item_list: [{
          type: 4,
          file_item: {
            file_name: "报表.xlsx",
            len: String(plain.byteLength),
            aeskey: IMAGE_KEY_HEX,
            media: { full_url: "https://novac2c.cdn.weixin.qq.com/c2c/download?encrypted_query_param=ABC" },
          },
        }],
      })]),
    ])));
    const adapter = createAdapter(env.ctx, adapterDeps(http, {
      download: async () => ciphertext,
    }));
    await adapter.start();
    t.after(() => adapter.stop().catch(() => {}));
    await waitFor(() => env.messages.length === 1);
    const media = env.messages[0]!.media!;
    assert.equal(media.length, 1);
    assert.equal(media[0]!.kind, "file");
    assert.equal(media[0]!.name, "报表.xlsx");
    assert.equal(media[0]!.mime, undefined);
    assert.ok(media[0]!.path.endsWith("报表.xlsx"));
    assert.ok((await readFile(media[0]!.path)).equals(plain));
    await adapter.stop();
  });

  it("normalize：ref_msg 一级引用快照", async (t) => {
    const env = await makeEnv();
    t.after(() => rm(env.dir, { recursive: true, force: true }));
    const { http } = recorder(weixinHandler(updatesScript([
      updatesResponse([textMessage({
        message_id: "7260000000000000005",
        context_token: undefined,
        run_id: undefined,
        item_list: [{
          type: 1,
          text_item: { text: "回复内容" },
          ref_msg: {
            title: "原消息标题",
            message_item: { msg_id: "7260000000000000000", type: 1, text_item: { text: "被引用原文" } },
          },
        }],
      })]),
    ])));
    const adapter = createAdapter(env.ctx, adapterDeps(http));
    await adapter.start();
    t.after(() => adapter.stop().catch(() => {}));
    await waitFor(() => env.messages.length === 1);
    assert.deepEqual(env.messages[0]!.replyTo, { id: "7260000000000000000", text: "被引用原文" });
    await adapter.stop();
  });

  it("normalize：不受信任的媒体地址被跳过且不 emit", async (t) => {
    const env = await makeEnv();
    t.after(() => rm(env.dir, { recursive: true, force: true }));
    const { http } = recorder(weixinHandler(updatesScript([
      updatesResponse([textMessage({
        message_id: "7260000000000000006",
        context_token: undefined,
        run_id: undefined,
        item_list: [{ type: 2, image_item: { media: { full_url: "https://evil.example/c2c/download" } } }],
      })]),
    ])));
    const adapter = createAdapter(env.ctx, adapterDeps(http));
    await adapter.start();
    t.after(() => adapter.stop().catch(() => {}));
    // Processing completes once the skipped message is persisted as seen.
    await waitFor(async () => {
      try {
        const doc = await readStateDoc(env.dataDir);
        return (doc.seenMessageIds as string[]).includes("7260000000000000006");
      } catch {
        return false; // state file not written yet
      }
    });
    assert.equal(env.messages.length, 0);
    assert.ok(
      env.logs.some((line) => line.startsWith("warn:") && line.includes("跳过不可解析的微信媒体项")),
      "不受信任的媒体地址按策略性跳过记 warn",
    );
    await adapter.stop();
  });

  it("sendText：fence-aware 分块、context_token 与 reply run_id", async (t) => {
    const env = await makeEnv();
    t.after(() => rm(env.dir, { recursive: true, force: true }));
    const { calls, http } = recorder(weixinHandler(updatesScript([
      updatesResponse([textMessage()]),
    ])));
    const adapter = createAdapter(env.ctx, adapterDeps(http));
    await adapter.start();
    t.after(() => adapter.stop().catch(() => {}));
    await waitFor(() => env.messages.length === 1);
    const prose = "段落内容。".repeat(100); // 500 chars
    const code = "x".repeat(1500);
    // Blank line keeps the fenced block its own segment so it must survive intact.
    const text = `${prose}\n\n\`\`\`js\n${code}\n\`\`\``;
    const expected = splitChunks(text, WEIXIN_MAX_CHUNK_CHARS);
    assert.equal(expected.length, 2);
    const result = await adapter.sendText({
      chatId: "user-1",
      text,
      replyToId: env.messages[0]!.messageId,
    });
    assert.equal(result.uncertain, undefined);
    assert.match(result.messageId!, /^paseo-weixin-/);
    const sends = calls.filter((call) => call.path === "ilink/bot/sendmessage");
    assert.equal(sends.length, expected.length);
    const clientIds = sends.map((call) => sendBody(call).client_id);
    assert.deepEqual(clientIds, [...new Set(clientIds)]);
    const chunkTexts: string[] = [];
    for (const [index, call] of sends.entries()) {
      const body = sendBody(call);
      assert.equal(body.to_user_id, "user-1");
      assert.equal(body.from_user_id, "");
      assert.equal(body.message_type, 2);
      assert.equal(body.message_state, 2);
      assert.equal(body.context_token, "ctx-token-1");
      assert.equal(body.run_id, "run-1");
      const items = body.item_list as RawRecord[]; // fixture: single iLink item
      assert.equal(items[0]!.type, 1);
      const chunk = (items[0]!.text_item as RawRecord).text as string; // fixture text
      assert.equal(chunk, expected[index]);
      assert.ok(chunk.length <= WEIXIN_MAX_CHUNK_CHARS);
      chunkTexts.push(chunk);
    }
    // The fence block survives intact in one chunk.
    assert.ok(chunkTexts.some((chunk) => chunk.includes("```js\n") && chunk.endsWith("```")));
    await adapter.stop();
  });

  it("sendText：网络错误短暂重试且保持同一 client_id", async (t) => {
    const env = await makeEnv();
    t.after(() => rm(env.dir, { recursive: true, force: true }));
    let attempts = 0;
    const sleeps: number[] = [];
    const { calls, http } = recorder(weixinHandler((call) => {
      if (call.path === "ilink/bot/getupdates") return hang(call.init);
      if (call.path === "ilink/bot/sendmessage") {
        attempts += 1;
        if (attempts === 1) throw new WeixinApiError("network-error", "暂时无法访问微信服务。");
        return {};
      }
      return {};
    }));
    const adapter = createAdapter(env.ctx, adapterDeps(http, {
      sleep: async (ms) => {
        sleeps.push(ms);
      },
    }));
    await adapter.start();
    t.after(() => adapter.stop().catch(() => {}));
    await adapter.sendText({ chatId: "user-1", text: "短消息" });
    assert.equal(attempts, 2);
    assert.deepEqual(sleeps, [TEXT_SEND_RETRY_DELAYS_MS[0]]);
    const sends = calls.filter((call) => call.path === "ilink/bot/sendmessage");
    assert.equal(sendBody(sends[0]!).client_id, sendBody(sends[1]!).client_id);
    await adapter.stop();
  });

  it("sendText：平台拒绝时抛出中文错误", async (t) => {
    const env = await makeEnv();
    t.after(() => rm(env.dir, { recursive: true, force: true }));
    const { http } = recorder(weixinHandler((call) => {
      if (call.path === "ilink/bot/getupdates") return hang(call.init);
      if (call.path === "ilink/bot/sendmessage") return { ret: -1 };
      return {};
    }));
    const adapter = createAdapter(env.ctx, adapterDeps(http));
    await adapter.start();
    t.after(() => adapter.stop().catch(() => {}));
    await assert.rejects(
      adapter.sendText({ chatId: "user-1", text: "你好" }),
      (error: Error) => error instanceof UserFacingError && /微信服务拒绝了回复消息/.test(error.message),
    );
    await adapter.stop();
  });

  it("sendText：超时等未决错误返回 uncertain", async (t) => {
    const env = await makeEnv();
    t.after(() => rm(env.dir, { recursive: true, force: true }));
    const { http } = recorder(weixinHandler((call) => {
      if (call.path === "ilink/bot/getupdates") return hang(call.init);
      if (call.path === "ilink/bot/sendmessage") {
        throw new WeixinApiError("timeout", "微信服务请求超时。");
      }
      return {};
    }));
    const adapter = createAdapter(env.ctx, adapterDeps(http));
    await adapter.start();
    t.after(() => adapter.stop().catch(() => {}));
    const result = await adapter.sendText({ chatId: "user-1", text: "你好" });
    assert.equal(result.uncertain, true);
    await adapter.stop();
  });

  it("context_token 按用户持久化，重启后仍然携带，换凭据后清空", async (t) => {
    const env = await makeEnv();
    t.after(() => rm(env.dir, { recursive: true, force: true }));
    const first = recorder(weixinHandler(updatesScript([updatesResponse([textMessage()])])));
    const adapterA = createAdapter(env.ctx, adapterDeps(first.http));
    await adapterA.start();
    t.after(() => adapterA.stop().catch(() => {}));
    await waitFor(() => env.messages.length === 1);
    await adapterA.stop();
    const doc = await readStateDoc(env.dataDir);
    const tokens = doc.contextTokens as RawRecord;
    const users = tokens.users as RawRecord;
    assert.equal((users["user-1"] as RawRecord).token, "ctx-token-1");
    assert.match(tokens.credentialHash as string, /^[a-f0-9]{64}$/);

    const second = recorder(weixinHandler(updatesScript([])));
    const adapterB = createAdapter(env.ctx, adapterDeps(second.http));
    await adapterB.start();
    t.after(() => adapterB.stop().catch(() => {}));
    await adapterB.sendText({ chatId: "user-1", text: "第二条" });
    const sendB = second.calls.filter((call) => call.path === "ilink/bot/sendmessage");
    assert.equal(sendBody(sendB[0]!).context_token, "ctx-token-1");
    await adapterB.stop();

    const ctxC = { ...env.ctx, secrets: { token: "token-xyz" } };
    const third = recorder(weixinHandler(updatesScript([])));
    const adapterC = createAdapter(ctxC, adapterDeps(third.http));
    await adapterC.start();
    t.after(() => adapterC.stop().catch(() => {}));
    await adapterC.sendText({ chatId: "user-1", text: "第三条" });
    const sendC = third.calls.filter((call) => call.path === "ilink/bot/sendmessage");
    assert.equal("context_token" in sendBody(sendC[0]!), false);
    await adapterC.stop();
  });

  it("typing：getconfig → sendtyping(1) → 保活 → 发送前 sendtyping(2)", async (t) => {
    const env = await makeEnv();
    t.after(() => rm(env.dir, { recursive: true, force: true }));
    const { calls, http } = recorder(weixinHandler((call) => {
      if (call.path === "ilink/bot/getupdates") return hang(call.init);
      if (call.path === "ilink/bot/getconfig") return { typing_ticket: "TICKET-1" };
      return {};
    }));
    const adapter = createAdapter(env.ctx, adapterDeps(http, { typingKeepaliveMs: 15 }));
    await adapter.start();
    t.after(() => adapter.stop().catch(() => {}));
    await adapter.sendTyping("user-1");
    const configCall = calls.find((call) => call.path === "ilink/bot/getconfig");
    assert.equal((configCall!.body as RawRecord).ilink_user_id, "user-1");
    const typingCalls = (): HttpCall[] => calls.filter((call) => call.path === "ilink/bot/sendtyping");
    await waitFor(() => typingCalls().length >= 3); // initial + two keepalives
    // Sending a reply must cancel the typing indicator first.
    await adapter.sendText({ chatId: "user-1", text: "回复" });
    const stopIndex = calls.findIndex((call) => call.path === "ilink/bot/sendtyping"
      && (call.body as RawRecord).status === 2);
    const sendIndex = calls.findIndex((call) => call.path === "ilink/bot/sendmessage");
    assert.ok(stopIndex >= 0 && sendIndex >= 0 && stopIndex < sendIndex);
    const stopCall = calls[stopIndex]!;
    const stopBody = stopCall.body as RawRecord; // fixture: sendtyping request
    assert.equal(stopBody.typing_ticket, "TICKET-1");
    assert.equal(stopBody.ilink_user_id, "user-1");
    assert.equal(stopBody.status, 2);
    assert.ok(calls.slice(0, stopIndex).every((call) => call.path !== "ilink/bot/sendmessage"));
    const typingCount = typingCalls().length;
    await adapter.stop();
    // Negative assertion: after stop no keepalive may fire again (no signal to await).
    await delay(60);
    assert.equal(typingCalls().length, typingCount);
    assert.ok(calls.some((call) => call.path === "ilink/bot/msg/notifystop"));
  });

  it("start：缺少凭据抛出中文 UserFacingError 且不发起请求", async (t) => {
    const env = await makeEnv({ secrets: {} });
    t.after(() => rm(env.dir, { recursive: true, force: true }));
    const { calls, http } = recorder(() => ({}));
    const adapter = createAdapter(env.ctx, adapterDeps(http));
    await assert.rejects(
      adapter.start(),
      (error: Error) => error instanceof UserFacingError && /缺少微信登录凭据/.test(error.message),
    );
    assert.equal(calls.length, 0);
  });

  it("start：baseUrl 不受信任时抛出 UserFacingError", async (t) => {
    const env = await makeEnv({ baseUrl: "http://evil.example/" });
    t.after(() => rm(env.dir, { recursive: true, force: true }));
    const { calls, http } = recorder(() => ({}));
    const adapter = createAdapter(env.ctx, adapterDeps(http));
    await assert.rejects(
      adapter.start(),
      (error: Error) => error instanceof UserFacingError && /不受信任/.test(error.message),
    );
    assert.equal(calls.length, 0);
  });

  it("start：瞬时错误按 [1,3,5,10,30]s 退避重试", async (t) => {
    const env = await makeEnv();
    t.after(() => rm(env.dir, { recursive: true, force: true }));
    let starts = 0;
    const sleeps: number[] = [];
    const { http } = recorder(weixinHandler((call) => {
      if (call.path === "ilink/bot/msg/notifystart") {
        starts += 1;
        if (starts <= 2) throw new WeixinApiError("network-error", "暂时无法访问微信服务。");
        return {};
      }
      if (call.path === "ilink/bot/getupdates") return hang(call.init);
      return {};
    }));
    const adapter = createAdapter(env.ctx, adapterDeps(http, {
      sleep: async (ms) => {
        sleeps.push(ms);
      },
    }));
    await adapter.start();
    t.after(() => adapter.stop().catch(() => {}));
    assert.deepEqual(sleeps, [1_000, 3_000]);
    assert.equal(env.errors.length, 2);
    await adapter.stop();
  });

  it("start：凭据失效（ret -14）抛出 UserFacingError 且 onError 收到", async (t) => {
    const env = await makeEnv();
    t.after(() => rm(env.dir, { recursive: true, force: true }));
    const { calls, http } = recorder(weixinHandler((call) => {
      if (call.path === "ilink/bot/msg/notifystart") return { ret: "-14" };
      return {};
    }));
    const adapter = createAdapter(env.ctx, adapterDeps(http));
    await assert.rejects(
      adapter.start(),
      (error: Error) => error instanceof UserFacingError && /重新扫码配对/.test(error.message),
    );
    assert.equal(env.errors.length, 1);
    assert.ok(env.errors[0] instanceof UserFacingError);
    assert.equal(calls.filter((call) => call.path === "ilink/bot/msg/notifystart").length, 1);
  });

  it("监控循环：瞬时错误退避后恢复", async (t) => {
    const env = await makeEnv();
    t.after(() => rm(env.dir, { recursive: true, force: true }));
    const sleeps: number[] = [];
    const { http } = recorder(weixinHandler(updatesScript([
      new WeixinApiError("network-error", "暂时无法访问微信服务。"),
      updatesResponse([textMessage()], "BUF-9"),
    ])));
    const adapter = createAdapter(env.ctx, adapterDeps(http, {
      sleep: async (ms) => {
        sleeps.push(ms);
      },
    }));
    await adapter.start();
    t.after(() => adapter.stop().catch(() => {}));
    await waitFor(() => env.messages.length === 1);
    await waitFor(async () => (await readStateDoc(env.dataDir)).getUpdatesBuf === "BUF-9");
    assert.deepEqual(sleeps, [1_000]);
    assert.equal(env.errors.length, 1);
    await adapter.stop();
  });

  it("监控循环：凭据失效（ret -14）终止且不再轮询", async (t) => {
    const env = await makeEnv();
    t.after(() => rm(env.dir, { recursive: true, force: true }));
    const { calls, http } = recorder(weixinHandler(updatesScript([{ ret: -14 }])));
    const adapter = createAdapter(env.ctx, adapterDeps(http));
    await adapter.start();
    t.after(() => adapter.stop().catch(() => {}));
    await waitFor(() => env.errors.length === 1);
    assert.ok(env.errors[0] instanceof UserFacingError);
    assert.match(env.errors[0]!.message, /重新扫码配对/);
    assert.equal(calls.filter((call) => call.path === "ilink/bot/getupdates").length, 1);
    await adapter.stop();
  });

  it("sendMedia：图片上传全流程（getuploadurl → CDN 加密上传 → sendmessage）", async (t) => {
    const env = await makeEnv();
    t.after(() => rm(env.dir, { recursive: true, force: true }));
    const plain = Buffer.concat([Buffer.from([0x89, 0x50, 0x4e, 0x47]), randomBytes(100)]);
    const imagePath = join(env.dir, "upload.png");
    await writeFile(imagePath, plain);
    let uploadSeen: { url: string; contentLength: string; body: Buffer } | null = null;
    const { calls, http } = recorder(weixinHandler((call) => {
      if (call.path === "ilink/bot/getupdates") return hang(call.init);
      if (call.path === "ilink/bot/getuploadurl") {
        return { upload_full_url: "https://novac2c.cdn.weixin.qq.com/c2c/upload?encrypted_query_param=UP" };
      }
      return {};
    }));
    const adapter = createAdapter(env.ctx, adapterDeps(http, {
      upload: async (url, init) => {
        const headers = init.headers as Record<string, string>; // fixture: plain header object
        const stream = init.body as ReadableStream<Uint8Array>; // the encrypted stream we produced
        const reader = stream.getReader();
        const chunks: Buffer[] = [];
        for (;;) {
          const { done, value } = await reader.read();
          if (done) break;
          chunks.push(Buffer.from(value));
        }
        uploadSeen = { url: String(url), contentLength: headers["content-length"], body: Buffer.concat(chunks) };
        return new Response(null, { status: 200, headers: { "x-encrypted-param": "DL-PARAM" } });
      },
    }));
    await adapter.start();
    t.after(() => adapter.stop().catch(() => {}));
    const result = await adapter.sendMedia({
      chatId: "user-1",
      kind: "image",
      path: imagePath,
      caption: "看这张图",
    });
    assert.match(result.messageId!, /^paseo-weixin-/);
    const getupload = calls.find((call) => call.path === "ilink/bot/getuploadurl");
    const uploadBody = getupload!.body as RawRecord; // fixture: getuploadurl request
    assert.equal(uploadBody.media_type, 1);
    assert.equal(uploadBody.no_need_thumb, true);
    assert.equal(uploadBody.to_user_id, "user-1");
    assert.equal(uploadBody.rawsize, plain.byteLength);
    assert.equal(uploadBody.filesize, 112);
    const rawMd5 = uploadBody.rawfilemd5 as string;
    const aesKeyHex = uploadBody.aeskey as string;
    assert.match(rawMd5, /^[a-f0-9]{32}$/);
    assert.match(aesKeyHex, /^[0-9a-f]{32}$/);
    const seen = uploadSeen!; // populated before sendMedia resolved
    assert.equal(seen.url, "https://novac2c.cdn.weixin.qq.com/c2c/upload?encrypted_query_param=UP");
    assert.equal(seen.contentLength, "112");
    assert.ok(aesDecrypt(seen.body, aesKeyHex).equals(plain));
    const sends = calls.filter((call) => call.path === "ilink/bot/sendmessage");
    assert.equal(sends.length, 2);
    const mediaItems = sendBody(sends[0]!).item_list as RawRecord[];
    const mediaItem = mediaItems[0]!;
    assert.equal(mediaItem.type, 2);
    const imageItem = mediaItem.image_item as RawRecord;
    const media = imageItem.media as RawRecord;
    assert.equal(media.encrypt_query_param, "DL-PARAM");
    assert.equal(media.encrypt_type, 1);
    assert.equal(Buffer.from(media.aes_key as string, "base64").toString("ascii"), aesKeyHex);
    assert.equal(imageItem.mid_size, 112);
    assert.equal(sendBody(sends[0]!).to_user_id, "user-1");
    // Caption travels as a follow-up text message.
    const captionItems = sendBody(sends[1]!).item_list as RawRecord[];
    const captionItem = captionItems[0]!;
    assert.equal(captionItem.type, 1);
    assert.equal((captionItem.text_item as RawRecord).text, "看这张图");
    await adapter.stop();
  });

  it("sendMedia：文件 item 形状（file_name/len/media_type 3）", async (t) => {
    const env = await makeEnv();
    t.after(() => rm(env.dir, { recursive: true, force: true }));
    const filePath = join(env.dir, "附件.bin");
    await writeFile(filePath, Buffer.from("0123456789abcdef")); // 16 bytes → padded 32
    const { calls, http } = recorder(weixinHandler((call) => {
      if (call.path === "ilink/bot/getupdates") return hang(call.init);
      if (call.path === "ilink/bot/getuploadurl") return { upload_param: "PARAM-1" };
      return {};
    }));
    const adapter = createAdapter(env.ctx, adapterDeps(http, {
      upload: async () => new Response(null, { status: 200, headers: { "x-encrypted-param": "DL-2" } }),
    }));
    await adapter.start();
    t.after(() => adapter.stop().catch(() => {}));
    await adapter.sendMedia({ chatId: "user-1", kind: "file", path: filePath, name: "附件.bin" });
    const getupload = calls.find((call) => call.path === "ilink/bot/getuploadurl");
    assert.equal((getupload!.body as RawRecord).media_type, 3);
    const sends = calls.filter((call) => call.path === "ilink/bot/sendmessage");
    assert.equal(sends.length, 1);
    const items = sendBody(sends[0]!).item_list as RawRecord[];
    const fileItem = items[0]!.file_item as RawRecord;
    assert.equal(items[0]!.type, 4);
    assert.equal(fileItem.file_name, "附件.bin");
    assert.equal(fileItem.len, "16");
    assert.equal((fileItem.media as RawRecord).encrypt_query_param, "DL-2");
    await adapter.stop();
  });

  it("sendMedia：上传请求被平台拒绝时抛出中文错误", async (t) => {
    const env = await makeEnv();
    t.after(() => rm(env.dir, { recursive: true, force: true }));
    const filePath = join(env.dir, "a.bin");
    await writeFile(filePath, Buffer.from("data"));
    const { http } = recorder(weixinHandler((call) => {
      if (call.path === "ilink/bot/getupdates") return hang(call.init);
      if (call.path === "ilink/bot/getuploadurl") return { ret: -2 };
      return {};
    }));
    const adapter = createAdapter(env.ctx, adapterDeps(http));
    await adapter.start();
    t.after(() => adapter.stop().catch(() => {}));
    await assert.rejects(
      adapter.sendMedia({ chatId: "user-1", kind: "file", path: filePath }),
      (error: Error) => error instanceof UserFacingError && /文件上传请求/.test(error.message),
    );
    await adapter.stop();
  });

  it("sendMedia：sendmessage 模糊失败返回 uncertain，CDN 上传请求禁用重定向", async (t) => {
    const env = await makeEnv();
    t.after(() => rm(env.dir, { recursive: true, force: true }));
    const filePath = join(env.dir, "a.bin");
    await writeFile(filePath, Buffer.from("data"));
    let uploadInit: RequestInit | null = null;
    const { http } = recorder(weixinHandler((call) => {
      if (call.path === "ilink/bot/getupdates") return hang(call.init);
      if (call.path === "ilink/bot/getuploadurl") {
        return { upload_full_url: "https://novac2c.cdn.weixin.qq.com/c2c/upload?encrypted_query_param=UP" };
      }
      if (call.path === "ilink/bot/sendmessage") {
        throw new WeixinApiError("timeout", "微信服务请求超时。");
      }
      return {};
    }));
    const adapter = createAdapter(env.ctx, adapterDeps(http, {
      upload: async (_url, init) => {
        uploadInit = init;
        return new Response(null, { status: 200, headers: { "x-encrypted-param": "DL-PARAM" } });
      },
    }));
    await adapter.start();
    t.after(() => adapter.stop().catch(() => {}));
    const result = await adapter.sendMedia({ chatId: "user-1", kind: "file", path: filePath });
    assert.equal(result.uncertain, true, "sendmessage 超时属于模糊结果，不上抛");
    assert.equal(uploadInit!.redirect, "error", "CDN 上传 fetch 必须带 redirect:error");
    await adapter.stop();
  });

  it("sendMedia：CDN 上传失败仍抛出中文错误（文件未送达）", async (t) => {
    const env = await makeEnv();
    t.after(() => rm(env.dir, { recursive: true, force: true }));
    const filePath = join(env.dir, "a.bin");
    await writeFile(filePath, Buffer.from("data"));
    const { calls, http } = recorder(weixinHandler((call) => {
      if (call.path === "ilink/bot/getupdates") return hang(call.init);
      if (call.path === "ilink/bot/getuploadurl") {
        return { upload_full_url: "https://novac2c.cdn.weixin.qq.com/c2c/upload?encrypted_query_param=UP" };
      }
      return {};
    }));
    const adapter = createAdapter(env.ctx, adapterDeps(http, {
      upload: async () => {
        throw new Error("boom");
      },
    }));
    await adapter.start();
    t.after(() => adapter.stop().catch(() => {}));
    await assert.rejects(
      adapter.sendMedia({ chatId: "user-1", kind: "file", path: filePath }),
      (error: Error) => error instanceof UserFacingError && /微信文件上传失败/.test(error.message),
    );
    assert.equal(
      calls.some((call) => call.path === "ilink/bot/sendmessage"),
      false,
      "上传失败后不得调用 sendmessage",
    );
    await adapter.stop();
  });
});

describe("weixin 扫码配对", () => {
  interface PairingEnv {
    dir: string;
    calls: HttpCall[];
    http: WeixinHttp;
  }

  async function makePairingEnv(handler: (call: HttpCall) => unknown): Promise<PairingEnv> {
    const dir = await mkdtemp(join(tmpdir(), "paseo-weixin-pair-"));
    const { calls, http } = recorder(handler);
    return { dir, calls, http };
  }

  const makeQr = async (): Promise<string> => "data:image/png;base64,FAKE";

  it("start 拉取二维码，poll 返回 pending + qrDataUrl", async (t) => {
    const env = await makePairingEnv((call) => {
      if (call.path === "ilink/bot/get_bot_qrcode?bot_type=3") {
        return { qrcode: "QR-1", qrcode_img_content: "https://ilinkai.weixin.qq.com/qr/QR-1.png" };
      }
      if (call.path === "ilink/bot/get_qrcode_status?qrcode=QR-1") return { status: "wait" };
      return {};
    });
    t.after(async () => {
      await session.stop().catch(() => {});
      await rm(env.dir, { recursive: true, force: true });
    });
    const session = createPairingSession("conn-1", env.dir, { http: env.http, makeQr });
    t.after(() => session.stop().catch(() => {})); // fake 恒返 wait：不 stop 则后台轮询循环挂住事件循环
    await session.start();
    assert.ok(env.calls.some((call) => call.path === "ilink/bot/get_bot_qrcode?bot_type=3"));
    const status = await session.poll();
    assert.equal(status.status, "pending");
    if (status.status === "pending") {
      // 可渲染断言：必须 image/* data URL（曾回归出 text/html 落地页冒充图片，手机端空白）
      assert.match(status.qrDataUrl ?? "", /^data:image\/(png|jpe?g);base64,/);
      assert.equal(status.qrDataUrl, "data:image/png;base64,FAKE");
    }
  });

  it("状态机：scaned → redirect → confirmed 返回凭据 fields（后台单飞推进，快照收敛）", async (t) => {
    let poll = 0;
    const env = await makePairingEnv((call) => {
      if (call.path === "ilink/bot/get_bot_qrcode?bot_type=3") {
        return { qrcode: "QR-2", qrcode_img_content: "https://ilinkai.weixin.qq.com/qr/QR-2.png" };
      }
      if (call.path.startsWith("ilink/bot/get_qrcode_status")) {
        poll += 1;
        if (poll === 1) return { status: "scaned" };
        if (poll === 2) return { status: "scaned_but_redirect", redirect_host: "backup.weixin.qq.com" };
        return {
          status: "confirmed",
          bot_token: "T-1",
          ilink_bot_id: "ACC-1",
          ilink_user_id: "USER-1",
        };
      }
      return {};
    });
    t.after(async () => {
      await session.stop().catch(() => {});
      await rm(env.dir, { recursive: true, force: true });
    });
    const session = createPairingSession("conn-1", env.dir, { http: env.http, makeQr });
    await session.start();
    const first = await session.poll();
    assert.equal(first.status, "pending"); // 快照立即可用：pending + 二维码
    if (first.status === "pending") {
      assert.equal(first.qrDataUrl, "data:image/png;base64,FAKE");
    }
    // 后台单飞推进 scaned → scaned_but_redirect → confirmed；快照轮询等待收敛
    const ok = await pollUntil(session, (snapshot) => snapshot.status === "ok");
    assert.equal(ok.status, "ok");
    if (ok.status === "ok") {
      assert.deepEqual(ok.fields, {
        token: "T-1",
        accountId: "ACC-1",
        ownerUserId: "USER-1",
        baseUrl: "https://backup.weixin.qq.com/",
      });
    }
    assert.ok(
      poll >= 3,
      `应依次经历 scaned → scaned_but_redirect → confirmed，实际 poll=${poll}`,
    );
  });

  it("need_verifycode 后提交配对码，后续轮询携带 verify_code（后台暂停等码，提交后恢复）", async (t) => {
    let poll = 0;
    const env = await makePairingEnv((call) => {
      if (call.path === "ilink/bot/get_bot_qrcode?bot_type=3") {
        return { qrcode: "QR-3", qrcode_img_content: "https://ilinkai.weixin.qq.com/qr/QR-3.png" };
      }
      if (call.path.startsWith("ilink/bot/get_qrcode_status")) {
        poll += 1;
        if (poll === 1) return { status: "need_verifycode" };
        return { status: "confirmed", bot_token: "T-2", ilink_bot_id: "ACC-2", ilink_user_id: "USER-2" };
      }
      return {};
    });
    t.after(async () => {
      await session.stop().catch(() => {});
      await rm(env.dir, { recursive: true, force: true });
    });
    const session = createPairingSession("conn-1", env.dir, { http: env.http, makeQr });
    await session.start();
    // 后台命中 need_verifycode 后暂停：快照等到「配对码」提示，且不再空转打微信
    const pending = await pollUntil(
      session,
      (snapshot) => snapshot.status === "pending" && /配对码/.test(snapshot.detail ?? ""),
    );
    assert.ok(pending.status === "pending", "暂停点必须停在 pending（等待配对码）");
    assert.match(pending.detail ?? "", /配对码/);
    const statusCallsBeforeSubmit = env.calls.filter((call) =>
      call.path.startsWith("ilink/bot/get_qrcode_status")
    ).length;
    await session.submitVerification!("4321");
    const ok = await pollUntil(session, (snapshot) => snapshot.status === "ok");
    assert.equal(ok.status, "ok");
    const statusCall = env.calls.find((call) => call.path.startsWith("ilink/bot/get_qrcode_status")
      && call.path.includes("verify_code=4321"));
    assert.ok(statusCall, "提交配对码后的恢复轮询必须携带 verify_code=4321");
    assert.equal(
      env.calls.filter((call) => call.path.startsWith("ilink/bot/get_qrcode_status")).length,
      statusCallsBeforeSubmit + 1,
      "等待配对码期间后台不得空转轮询",
    );
  });

  it("expired / verify_code_blocked / binded_redirect 状态映射（快照收敛）", async (t) => {
    const cases: Array<{ response: RawRecord; expected: string; pattern: RegExp }> = [
      { response: { status: "expired" }, expected: "expired", pattern: /^$/ },
      {
        response: { status: "verify_code_blocked" },
        expected: "failed",
        pattern: /配对码多次错误/,
      },
      {
        response: { status: "binded_redirect" },
        expected: "failed",
        pattern: /已绑定/,
      },
    ];
    for (const [index, testCase] of cases.entries()) {
      const env = await makePairingEnv((call) => {
        if (call.path === "ilink/bot/get_bot_qrcode?bot_type=3") {
          return { qrcode: `QR-BLOCK-${index}`, qrcode_img_content: "https://ilinkai.weixin.qq.com/qr/x.png" };
        }
        if (call.path.startsWith("ilink/bot/get_qrcode_status")) return testCase.response;
        return {};
      });
      try {
        const session = createPairingSession("conn-1", env.dir, { http: env.http, makeQr });
        await session.start();
        const status = await pollUntil(session, (snapshot) => snapshot.status === testCase.expected);
        assert.equal(status.status, testCase.expected);
        if (status.status === "failed") {
          assert.match(status.detail, testCase.pattern);
        }
      } finally {
        await rm(env.dir, { recursive: true, force: true });
      }
    }
  });

  it("不受信任的二维码地址抛出 UserFacingError", async (t) => {
    const env = await makePairingEnv(() => ({
      qrcode: "QR-EVIL",
      qrcode_img_content: "https://evil.example/x.png",
    }));
    t.after(async () => {
      await session.stop().catch(() => {});
      await rm(env.dir, { recursive: true, force: true });
    });
    const session = createPairingSession("conn-1", env.dir, { http: env.http, makeQr });
    await assert.rejects(
      session.start(),
      (error: Error) => error instanceof UserFacingError && /不受信任的扫码地址/.test(error.message),
    );
  });

  it("stop 取消进行中的长轮询，poll 以 pending 收尾", async (t) => {
    const env = await makePairingEnv((call) => {
      if (call.path === "ilink/bot/get_bot_qrcode?bot_type=3") {
        return { qrcode: "QR-STOP", qrcode_img_content: "https://ilinkai.weixin.qq.com/qr/x.png" };
      }
      if (call.path.startsWith("ilink/bot/get_qrcode_status")) return hang(call.init);
      return {};
    });
    t.after(async () => {
      await session.stop().catch(() => {});
      await rm(env.dir, { recursive: true, force: true });
    });
    const session = createPairingSession("conn-1", env.dir, { http: env.http, makeQr });
    await session.start();
    const polling = session.poll();
    await waitFor(() => env.calls.some((call) => call.path.startsWith("ilink/bot/get_qrcode_status")));
    await session.stop();
    const status = await polling;
    assert.equal(status.status, "pending");
  });
});
