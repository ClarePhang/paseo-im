/** QQ 渠道适配器测试：全部通过注入 fake 传输层运行，无真实网络。 */
import assert from "node:assert/strict";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { setTimeout as sleep } from "node:timers/promises";
import { describe, it } from "node:test";
import { connectionSchema } from "../../shared/config.ts";
import type { Connection, Policy } from "../../shared/config.ts";
import type { GroupMembershipEvent, NormalizedMessage } from "../../shared/model.ts";
import { UserFacingError } from "../../shared/errors.ts";
import type { AdapterContext } from "../../server/channels/types.ts";
import type { QqConnection, QqDeps, QqSocket, QqSocketEvent } from "../../server/channels/qq/index.ts";
import {
  definition,
  isMarkdownRejection,
  QqApiError,
  createAdapter,
} from "../../server/channels/qq/index.ts";

const TOKEN_PATH = "/app/getAppAccessToken";
const INTENT_GROUP_AND_C2C = 1 << 25;

interface ApiCall {
  path: string;
  method: string;
  body?: unknown;
}

function asRecord(value: unknown): Record<string, unknown> {
  return value !== null && typeof value === "object" ? (value as Record<string, unknown>) : {};
}

class FakeSocket implements QqSocket {
  readonly url: string;
  readonly sent: string[] = [];
  readonly closed: Array<{ code?: number; reason?: string }> = [];
  private readonly listeners = new Map<string, Array<(event: QqSocketEvent) => void>>();

  constructor(url: string) {
    this.url = url;
  }

  send(data: string): void {
    this.sent.push(data);
  }

  close(code?: number, reason?: string): void {
    this.closed.push({ code, reason });
  }

  addEventListener(type: "message" | "error" | "close", listener: (event: QqSocketEvent) => void): void {
    const list = this.listeners.get(type) ?? [];
    list.push(listener);
    this.listeners.set(type, list);
  }

  emit(event: QqSocketEvent): void {
    for (const listener of this.listeners.get(event.type) ?? []) listener(event);
  }

  emitJson(frame: unknown): void {
    this.emit({ type: "message", data: JSON.stringify(frame) });
  }

  emitClose(code: number, reason = ""): void {
    this.emit({ type: "close", code, reason });
  }
}

function fakeDeps(overrides: { api?: QqDeps["api"]; fetchImpl?: typeof fetch; backoffMs?: number[] } = {}) {
  const calls: ApiCall[] = [];
  const sockets: FakeSocket[] = [];
  const api = overrides.api ?? (async (path: string, method: string, body?: unknown) => {
    calls.push({ path, method, body });
    if (path === TOKEN_PATH) return "TOKEN1";
    if (path === "/gateway") return { url: "wss://gateway.example" };
    if (path.startsWith("/v2/")) return { id: `SENT-${calls.length}` };
    return {};
  });
  const deps: QqDeps = {
    api,
    createWs: (url: string) => {
      const socket = new FakeSocket(url);
      sockets.push(socket);
      return socket;
    },
    ...(overrides.fetchImpl ? { fetchImpl: overrides.fetchImpl } : {}),
    backoffMs: overrides.backoffMs ?? [10, 10, 10, 10, 10],
  };
  return { deps, calls, sockets };
}

async function makeCtx(overrides: { secrets?: Record<string, string> } = {}) {
  const dataDir = await mkdtemp(join(tmpdir(), "paseo-im-qq-data-"));
  const mediaDir = await mkdtemp(join(tmpdir(), "paseo-im-qq-media-"));
  const messages: NormalizedMessage[] = [];
  const groupEvents: GroupMembershipEvent[] = [];
  const errors: Error[] = [];
  const logs: string[] = [];
  const controller = new AbortController();
  const policy: Policy = {
    groupAdmission: "everyone",
    directMessages: "everyone",
    groupUsers: "everyone",
    operators: [],
    invocation: "mentions",
  };
  const ctx: AdapterContext<QqConnection> = {
    connectionId: "conn-1",
    config: {
      channel: "qq",
      id: "9b1deb4d-3b7d-4bad-9bdd-2b0d7b3dcb6d",
      name: "QQ 机器人",
      enabled: true,
      target: null,
      policy,
      bindings: {},
      revision: 0,
      appId: "1111",
    },
    secrets: overrides.secrets ?? { appSecret: "secret-1" },
    dataDir,
    mediaDir,
    signal: controller.signal,
    settings: async () => ({
      imageMaxDownloadMb: 30,
      imageMaxSingleMb: 5,
      imageMaxTotalMb: 20,
      imageMaxCount: 20,
      busyMode: "queue",
      attachmentTtlHours: 168,
      deferTimeoutMinutes: 30,
    }),
    log: (level, message) => logs.push(`${level}:${message}`),
    onMessage: (message) => messages.push(message),
    onGroups: (events) => groupEvents.push(...events),
    onError: (error) => errors.push(error),
  };
  const cleanup = async () => {
    // 状态写入走 WriteChain 异步落盘；留出收尾窗口并容忍一次并发删除竞争。
    await sleep(20);
    for (let attempt = 0; attempt < 5; attempt += 1) {
      try {
        await rm(dataDir, { recursive: true, force: true });
        break;
      } catch {
        await sleep(20);
      }
    }
    await rm(mediaDir, { recursive: true, force: true });
  };
  return { ctx, messages, groupEvents, errors, logs, dataDir, mediaDir, controller, cleanup };
}

async function waitFor(condition: () => boolean | Promise<boolean>, label: string, timeoutMs = 1_000): Promise<void> {
  const start = Date.now();
  while (!(await condition())) {
    if (Date.now() - start > timeoutMs) throw new Error(`waitFor timeout: ${label}`);
    await sleep(2);
  }
}

function c2cFrame(overrides: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    id: "MSG1",
    content: "你好",
    timestamp: "2026-11-07T10:00:00+08:00",
    author: { user_openid: "U1" },
    ...overrides,
  };
}

function groupFrame(overrides: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    id: "G1",
    content: "<@!1111> 帮我查一下",
    timestamp: "2026-11-07T10:00:00+08:00",
    author: { member_openid: "M2" },
    group_openid: "GRP1",
    ...overrides,
  };
}

function dispatch(socket: FakeSocket, t: string, d: unknown, s?: number): void {
  socket.emitJson(s === undefined ? { op: 0, t, d } : { op: 0, s, t, d });
}

async function startConnected(
  harness: { ctx: AdapterContext<QqConnection> },
  fake: ReturnType<typeof fakeDeps>,
): Promise<{ adapter: ReturnType<typeof createAdapter>; socket: FakeSocket }> {
  const adapter = createAdapter(harness.ctx, fake.deps);
  await adapter.start();
  const socket = fake.sockets[fake.sockets.length - 1]!;
  socket.emitJson({ op: 10, d: { heartbeat_interval: 600_000 } });
  return { adapter, socket };
}

describe("qq definition", () => {
  it("exposes the qq channel with v1 capabilities", async () => {
    const harness = await makeCtx();
    assert.equal(definition.channel, "qq");
    const adapter = definition.create(harness.ctx);
    assert.equal(adapter.channel, "qq");
    assert.equal(adapter.capabilities.maxChunkChars, 3800);
    assert.equal(adapter.capabilities.markdown, "custom");
    assert.equal(adapter.capabilities.progress, "typing");
    assert.equal(adapter.capabilities.uploadImage, false);
    assert.equal(adapter.capabilities.uploadFile, false);
    assert.equal(adapter.capabilities.threads, false);
    assert.equal(adapter.remembersOwnMessages, true);
    await harness.cleanup();
  });

  it("configure trims fields and splits secrets", () => {
    const { config, secrets } = definition.configure({ appId: " 2222 ", appSecret: " sec ", note: "" });
    assert.deepEqual(config, { appId: "2222" });
    assert.deepEqual(secrets, { appSecret: "sec" });
    assert.throws(() => definition.configure({ appId: "2222" }), UserFacingError);
    assert.throws(() => definition.configure({ appSecret: "sec" }), UserFacingError);
  });
});

describe("qq start", () => {
  it("fails fast with a Chinese hint when AppSecret is missing", async () => {
    const harness = await makeCtx({ secrets: {} });
    const fake = fakeDeps(); const { deps } = fake;
    const adapter = createAdapter(harness.ctx, deps);
    await assert.rejects(adapter.start(), (error: unknown) => {
      assert.ok(error instanceof UserFacingError);
      assert.match(error.message, /AppSecret/);
      return true;
    });
    await harness.cleanup();
  });

  it("identifies with the group/c2c intent after gateway connect", async () => {
    const harness = await makeCtx();
    const fake = fakeDeps(); const { deps, calls, sockets } = fake;
    const adapter = createAdapter(harness.ctx, deps);
    await adapter.start();
    assert.equal(sockets.length, 1);
    assert.equal(sockets[0]!.url, "wss://gateway.example");
    const paths = calls.map((call) => call.path);
    assert.ok(paths.includes(TOKEN_PATH));
    assert.ok(paths.includes("/gateway"));
    assert.equal(sockets[0]!.sent.length, 0, "HELLO 前不应发送任何帧");
    sockets[0]!.emitJson({ op: 10, d: { heartbeat_interval: 600_000 } });
    const identify = JSON.parse(sockets[0]!.sent[0]!) as { op: number; d: { token: string; intents: number; shard: number[] } };
    assert.equal(identify.op, 2);
    assert.equal(identify.d.token, "QQBot TOKEN1");
    assert.equal(identify.d.intents, INTENT_GROUP_AND_C2C);
    assert.deepEqual(identify.d.shard, [0, 1]);
    await adapter.stop();
    await harness.cleanup();
  });

  it("sends heartbeats at the interval delivered in HELLO", async () => {
    const harness = await makeCtx();
    const fake = fakeDeps(); const { deps, sockets } = fake;
    const adapter = createAdapter(harness.ctx, deps);
    await adapter.start();
    const socket = sockets[0]!;
    socket.emitJson({ op: 10, d: { heartbeat_interval: 20 } });
    await sleep(90);
    await adapter.stop();
    const heartbeats = socket.sent
      .map((raw) => JSON.parse(raw) as { op: number })
      .filter((frame) => frame.op === 1);
    assert.ok(heartbeats.length >= 1, `期望至少一条心跳帧，实际 ${socket.sent.length} 帧`);
    await harness.cleanup();
  });

  it("resumes the session with seq after an unexpected close", async () => {
    const harness = await makeCtx();
    const { deps, sockets, calls } = fakeDeps();
    const adapter = createAdapter(harness.ctx, deps);
    await adapter.start();
    const first = sockets[0]!;
    first.emitJson({ op: 10, d: { heartbeat_interval: 600_000 } });
    dispatch(first, "READY", { session_id: "S1" }, 7);
    first.emitClose(1006, "drop");
    await waitFor(() => sockets.length >= 2, "reconnect");
    const second = sockets[1]!;
    second.emitJson({ op: 10, d: { heartbeat_interval: 600_000 } });
    const resume = JSON.parse(second.sent[0]!) as { op: number; d: { session_id: string; seq: number; token: string } };
    assert.equal(resume.op, 6);
    assert.equal(resume.d.session_id, "S1");
    assert.equal(resume.d.seq, 7);
    assert.equal(resume.d.token, "QQBot TOKEN1");
    assert.ok(calls.filter((call) => call.path === "/gateway").length >= 2);
    await adapter.stop();
    await harness.cleanup();
  });

  it("stops reconnecting after an auth-failure close (4004)", async () => {
    const harness = await makeCtx();
    const { deps, sockets, calls } = fakeDeps();
    const adapter = createAdapter(harness.ctx, deps);
    await adapter.start();
    sockets[0]!.emitClose(4004, "invalid token");
    await waitFor(() => harness.errors.length >= 1, "auth error");
    assert.match(harness.errors[0]!.message, /4004/);
    await sleep(60);
    assert.equal(sockets.length, 1, "鉴权失败后不应重连");
    assert.equal(calls.filter((call) => call.path === "/gateway").length, 1);
    await adapter.stop();
    await harness.cleanup();
  });

  it("retries transient token failures with backoff", async () => {
    const harness = await makeCtx();
    let tokenCalls = 0;
    const fake = fakeDeps({
      api: async (path, method, body) => {
        calls.push({ path, method, body });
        if (path === TOKEN_PATH) {
          tokenCalls += 1;
          if (tokenCalls === 1) throw new TypeError("fetch failed");
          return "TOKEN1";
        }
        if (path === "/gateway") return { url: "wss://gateway.example" };
        void body;
        return {};
      },
      backoffMs: [10, 10, 10, 10, 10],
    });
    const { deps, calls } = fake;
    const adapter = createAdapter(harness.ctx, deps);
    await adapter.start();
    await waitFor(() => calls.some((call) => call.path === "/gateway"), "gateway after retry");
    assert.ok(harness.errors.length >= 1, "瞬时错误应上报 onError");
    await adapter.stop();
    await harness.cleanup();
  });
});

describe("qq normalize", () => {
  it("emits a c2c dm with mention trigger and parsed timestamp", async () => {
    const harness = await makeCtx();
    const fake = fakeDeps(); const { deps } = fake;
    const { adapter, socket } = await startConnected(harness, fake);
    dispatch(socket, "C2C_MESSAGE_CREATE", c2cFrame());
    await waitFor(() => harness.messages.length === 1, "c2c message");
    const message = harness.messages[0]!;
    assert.deepEqual(
      { ...message, timestamp: 0 },
      {
        channel: "qq",
        connectionId: "conn-1",
        kind: "dm",
        chatId: "U1",
        senderId: "U1",
        messageId: "MSG1",
        timestamp: 0,
        text: "你好",
        trigger: { mention: true, replyToBot: false, command: false },
      },
    );
    assert.equal(message.timestamp, Date.parse("2026-11-07T10:00:00+08:00"));
    await adapter.stop();
    await harness.cleanup();
  });

  it("emits a group @ message with the mention marker stripped and quote attached", async () => {
    const harness = await makeCtx();
    const fake = fakeDeps(); const { deps } = fake;
    const { adapter, socket } = await startConnected(harness, fake);
    dispatch(socket, "GROUP_AT_MESSAGE_CREATE", groupFrame({
      msg_elements: [{ msg_idx: "IDX9", content: "之前的结果" }],
    }));
    await waitFor(() => harness.messages.length === 1, "group message");
    const message = harness.messages[0]!;
    assert.equal(message.kind, "group");
    assert.equal(message.chatId, "GRP1");
    assert.equal(message.senderId, "M2");
    assert.equal(message.text, "帮我查一下");
    assert.deepEqual(message.replyTo, { text: "之前的结果" });
    assert.equal(message.trigger.mention, true);
    assert.equal(message.trigger.replyToBot, false);
    await adapter.stop();
    await harness.cleanup();
  });

  it("marks replyToBot when the quote matches text this adapter sent", async () => {
    const harness = await makeCtx();
    const fake = fakeDeps(); const { deps, calls } = fake;
    const { adapter, socket } = await startConnected(harness, fake);
    dispatch(socket, "GROUP_AT_MESSAGE_CREATE", groupFrame());
    await waitFor(() => harness.messages.length === 1, "first group message");
    await adapter.sendText({ chatId: "GRP1", text: "今天天气晴" });
    assert.ok(calls.some((call) => asRecord(call.body).markdown !== undefined));
    dispatch(socket, "GROUP_AT_MESSAGE_CREATE", groupFrame({
      id: "G2",
      content: "<@!1111> 谢谢",
      msg_elements: [{ msg_idx: "IDX10", content: "今天天气晴" }],
    }));
    await waitFor(() => harness.messages.length === 2, "quoted message");
    assert.equal(harness.messages[1]!.trigger.replyToBot, true);
    await adapter.stop();
    await harness.cleanup();
  });

  it("downloads whitelisted attachments into mediaDir and skips foreign hosts", async () => {
    const harness = await makeCtx();
    const fetchedUrls: string[] = [];
    const fetchImpl = async (input: Parameters<typeof fetch>[0]): Promise<Response> => {
      fetchedUrls.push(String(input));
      return new Response(Uint8Array.of(1, 2, 3), { status: 200, headers: { "content-type": "image/png" } });
    };
    const fake = fakeDeps({ fetchImpl });
    const { adapter, socket } = await startConnected(harness, fake);
    dispatch(socket, "C2C_MESSAGE_CREATE", c2cFrame({
      id: "MSG-IMG",
      content: "看图",
      attachments: [
        { content_type: "image/png", url: "https://multimedia.qq.com/a/shot.png", filename: "shot.png" },
        { content_type: "image/png", url: "https://evil.example/x.png", filename: "x.png" },
      ],
    }));
    await waitFor(() => harness.messages.length === 1, "media message");
    const message = harness.messages[0]!;
    assert.equal(fetchedUrls.length, 1);
    assert.equal(fetchedUrls[0], "https://multimedia.qq.com/a/shot.png");
    assert.equal(message.media?.length, 1);
    const media = message.media![0]!;
    assert.equal(media.kind, "image");
    assert.equal(media.mime, "image/png");
    assert.equal(media.name, "shot.png");
    assert.ok(media.path.startsWith(harness.mediaDir));
    assert.deepEqual([...await readFile(media.path)], [1, 2, 3]);
    assert.ok(harness.logs.some((entry) => entry.includes("白名单")));
    // 官方多 CDN 域（qq.com.cn / tencentcos.com / ugcimg.cn）同样放行。
    dispatch(socket, "C2C_MESSAGE_CREATE", c2cFrame({
      id: "MSG-IMG2",
      content: "再看图",
      attachments: [
        { content_type: "image/png", url: "https://dl.gtimg.qq.com.cn/a/cn.png", filename: "cn.png" },
        { content_type: "image/png", url: "https://cos.ap-guangzhou.tencentcos.com/b/t.png", filename: "t.png" },
        { content_type: "image/png", url: "https://p3-sign.ugcimg.cn/c/u.png", filename: "u.png" },
        { content_type: "image/png", url: "https://fake-ugcimg.cn.evil.example/c2.png", filename: "c2.png" },
      ],
    }));
    await waitFor(() => harness.messages.length === 2, "second media message");
    assert.deepEqual(
      fetchedUrls.slice(1),
      [
        "https://dl.gtimg.qq.com.cn/a/cn.png",
        "https://cos.ap-guangzhou.tencentcos.com/b/t.png",
        "https://p3-sign.ugcimg.cn/c/u.png",
      ],
      "仅官方 CDN 后缀放行，伪后缀域名（evil.example）必须拒绝",
    );
    assert.equal(harness.messages[1]!.media?.length, 3);
    await adapter.stop();
    await harness.cleanup();
  });

  it("deduplicates seen message ids across restarts via the state file", async () => {
    const harness = await makeCtx();
    const fake = fakeDeps(); const { deps } = fake;
    const first = await startConnected(harness, fake);
    dispatch(first.socket, "GROUP_AT_MESSAGE_CREATE", groupFrame());
    await waitFor(() => harness.messages.length === 1, "first emit");
    await first.adapter.stop();
    await waitFor(async () => {
      try {
        await readFile(join(harness.dataDir, "qq-state.json"), "utf8");
        return true;
      } catch {
        return false;
      }
    }, "state file");
    const state = JSON.parse(await readFile(join(harness.dataDir, "qq-state.json"), "utf8")) as {
      scopes: Record<string, string>;
      seen: Record<string, number>;
    };
    assert.equal(state.scopes["GRP1"], "group");
    assert.ok(state.seen["G1"] > 0);

    const second = await startConnected(harness, fake);
    dispatch(second.socket, "GROUP_AT_MESSAGE_CREATE", groupFrame());
    await sleep(30);
    assert.equal(harness.messages.length, 1, "重启后同一条消息不应重复投递");
    await second.adapter.stop();
    await harness.cleanup();
  });

  it("suppresses bot echoes, noise, and marks commands", async () => {
    const harness = await makeCtx();
    const fake = fakeDeps(); const { deps } = fake;
    const { adapter, socket } = await startConnected(harness, fake);
    dispatch(socket, "GROUP_AT_MESSAGE_CREATE", groupFrame({ id: "OWN1", author: { member_openid: "M9", bot: true } }));
    dispatch(socket, "C2C_MESSAGE_CREATE", c2cFrame({ id: "N1", content: "" }));
    dispatch(socket, "C2C_MESSAGE_CREATE", c2cFrame({ id: "N2", content: "<@!1111>" }));
    dispatch(socket, "C2C_MESSAGE_CREATE", c2cFrame({ id: "CMD1", content: "/状态" }));
    await waitFor(() => harness.messages.length === 1, "command message");
    assert.deepEqual(
      { text: harness.messages[0]!.text, trigger: harness.messages[0]!.trigger },
      { text: "/状态", trigger: { mention: true, replyToBot: false, command: true } },
    );
    await sleep(30);
    assert.equal(harness.messages.length, 1);
    await adapter.stop();
    await harness.cleanup();
  });

  it("forwards group robot membership events", async () => {
    const harness = await makeCtx();
    const fake = fakeDeps(); const { deps } = fake;
    const { adapter, socket } = await startConnected(harness, fake);
    dispatch(socket, "GROUP_ADD_ROBOT", { group_openid: "GRP9" });
    dispatch(socket, "GROUP_DEL_ROBOT", { group_openid: "GRP9" });
    await waitFor(() => harness.groupEvents.length === 2, "group events");
    assert.deepEqual(harness.groupEvents, [
      { chatId: "GRP9", added: true },
      { chatId: "GRP9", added: false },
    ]);
    await adapter.stop();
    await harness.cleanup();
  });

  it("reports acceptMessage failures through onError instead of dropping them", async () => {
    const harness = await makeCtx();
    const fake = fakeDeps();
    const { adapter, socket } = await startConnected(harness, fake);
    harness.ctx.settings = async () => {
      throw new Error("设置读取失败");
    };
    // 附件存在才会走到 settings 读取，确保异常真实发生在 acceptMessage 内。
    dispatch(socket, "C2C_MESSAGE_CREATE", c2cFrame({
      content: "看图",
      attachments: [{ content_type: "image/png", url: "https://multimedia.qq.com/a/shot.png", filename: "shot.png" }],
    }));
    await waitFor(() => harness.errors.length >= 1, "onError");
    assert.match(harness.errors[0]!.message, /设置读取失败/);
    await adapter.stop();
    await harness.cleanup();
  });
});

describe("qq sendText", () => {
  it("sends markdown chunks passively with msg_id and returns the platform id", async () => {
    const harness = await makeCtx();
    const fake = fakeDeps(); const { deps, calls } = fake;
    const { adapter, socket } = await startConnected(harness, fake);
    dispatch(socket, "C2C_MESSAGE_CREATE", c2cFrame());
    await waitFor(() => harness.messages.length === 1, "trigger message");
    const result = await adapter.sendText({ chatId: "U1", text: "早上好", replyToId: "MSG1" });
    const sends = calls.filter((call) => call.path === "/v2/users/U1/messages");
    assert.equal(sends.length, 1);
    const body = asRecord(sends[0]!.body);
    assert.equal(body.msg_type, 2);
    assert.deepEqual(body.markdown, { content: "早上好" });
    assert.equal(body.msg_id, "MSG1");
    assert.equal(typeof body.msg_seq, "number");
    assert.equal(result.messageId, "SENT-3");
    await adapter.stop();
    await harness.cleanup();
  });

  it("falls back to plain text after markdown rejection 40034090", async () => {
    const harness = await makeCtx();
    const fake = fakeDeps({
      api: async (path, method, body) => {
        fake.calls.push({ path, method, body });
        if (path === TOKEN_PATH) return "TOKEN1";
        if (path === "/gateway") return { url: "wss://gateway.example" };
        const record = asRecord(body);
        if (record.msg_type === 2) throw new QqApiError("markdown 未授权", 400, 40034090);
        return { id: "PLAIN-OK" };
      },
    });
    const { deps, calls } = fake;
    const { adapter, socket } = await startConnected(harness, fake);
    dispatch(socket, "C2C_MESSAGE_CREATE", c2cFrame());
    await waitFor(() => harness.messages.length === 1, "trigger message");
    const result = await adapter.sendText({ chatId: "U1", text: "**重点**", replyToId: "MSG1" });
    const sends = calls.filter((call) => call.path === "/v2/users/U1/messages");
    assert.equal(sends.length, 2);
    assert.equal(asRecord(sends[0]!.body).msg_type, 2);
    const fallback = asRecord(sends[1]!.body);
    assert.equal(fallback.msg_type, 0);
    assert.equal(fallback.content, "**重点**");
    assert.equal(fallback.msg_id, "MSG1");
    assert.equal(fallback.msg_seq, asRecord(sends[0]!.body).msg_seq, "回退沿用同一 msg_seq");
    assert.equal(result.messageId, "PLAIN-OK");
    await adapter.stop();
    await harness.cleanup();
  });

  it("splits long text into fence-aware chunks under 3800 chars", async () => {
    const harness = await makeCtx();
    const fake = fakeDeps(); const { deps, calls } = fake;
    const { adapter, socket } = await startConnected(harness, fake);
    dispatch(socket, "C2C_MESSAGE_CREATE", c2cFrame());
    await waitFor(() => harness.messages.length === 1, "trigger message");
    const longText = `${"a".repeat(99)}\n`.repeat(101);
    const result = await adapter.sendText({ chatId: "U1", text: longText, replyToId: "MSG1" });
    const sends = calls.filter((call) => call.path === "/v2/users/U1/messages");
    assert.equal(sends.length, 3, `10100 字符应切成 3 片，实际 ${sends.length}`);
    const seqs: number[] = [];
    for (const call of sends) {
      const body = asRecord(call.body);
      assert.equal(body.msg_id, "MSG1", "4 片以内全部走被动回复");
      assert.equal(body.msg_type, 2);
      const content = asRecord(body.markdown).content as string;
      assert.ok(content.length <= 3800, `分片长度 ${content.length} 超限`);
      seqs.push(body.msg_seq as number);
    }
    assert.equal(new Set(seqs).size, seqs.length, "msg_seq 不得重复");
    assert.ok(result.messageId);
    await adapter.stop();
    await harness.cleanup();
  });

  it("switches overflow chunks to proactive sends with a notice", async () => {
    const harness = await makeCtx();
    const fake = fakeDeps(); const { deps, calls } = fake;
    const { adapter, socket } = await startConnected(harness, fake);
    dispatch(socket, "C2C_MESSAGE_CREATE", c2cFrame());
    await waitFor(() => harness.messages.length === 1, "trigger message");
    const paragraphs = Array.from({ length: 6 }, () => "x".repeat(2000));
    await adapter.sendText({ chatId: "U1", text: paragraphs.join("\n\n"), replyToId: "MSG1" });
    const sends = calls.filter((call) => call.path === "/v2/users/U1/messages");
    assert.equal(sends.length, 7, "3 被动 + 1 提示 + 3 主动");
    for (const [index, call] of sends.entries()) {
      const body = asRecord(call.body);
      if (index < 3) {
        assert.equal(body.msg_id, "MSG1", `分片 ${index} 应为被动回复`);
        assert.equal(body.msg_type, 2);
      } else if (index === 3) {
        assert.equal(body.msg_id, "MSG1", "提示占用最后一个被动额度");
        assert.equal(body.msg_type, 0);
        assert.ok(String(body.content).includes("继续"));
      } else {
        assert.equal(body.msg_id, undefined, `分片 ${index} 应转为主动发送`);
        assert.equal(body.msg_type, 2);
      }
      assert.ok(String(asRecord(body.markdown).content ?? body.content ?? "").length <= 3800);
    }
    await adapter.stop();
    await harness.cleanup();
  });

  it("sends proactively when the reply target is unknown or outside the window", async () => {
    const harness = await makeCtx();
    const fake = fakeDeps(); const { deps, calls } = fake;
    const { adapter, socket } = await startConnected(harness, fake);
    dispatch(socket, "C2C_MESSAGE_CREATE", c2cFrame());
    await waitFor(() => harness.messages.length === 1, "trigger message");
    await adapter.sendText({ chatId: "U1", text: "稍后提醒", replyToId: "UNKNOWN-MSG" });
    const sends = calls.filter((call) => call.path === "/v2/users/U1/messages");
    assert.equal(sends.length, 1);
    assert.equal(asRecord(sends[0]!.body).msg_id, undefined, "无有效 msg_id 记录时走主动发送");
    await adapter.stop();
    await harness.cleanup();
  });

  it("routes group sends through the group messages path and enforces the proactive quota", async () => {
    const harness = await makeCtx();
    const fake = fakeDeps(); const { deps, calls } = fake;
    const { adapter, socket } = await startConnected(harness, fake);
    dispatch(socket, "GROUP_AT_MESSAGE_CREATE", groupFrame());
    await waitFor(() => harness.messages.length === 1, "trigger message");
    for (let index = 0; index < 5; index += 1) {
      await adapter.sendText({ chatId: "GRP1", text: `主动消息 ${index}` });
    }
    await assert.rejects(
      () => adapter.sendText({ chatId: "GRP1", text: "第 6 条" }),
      (error: unknown) => {
        assert.ok(error instanceof UserFacingError);
        assert.match(error.message, /5 条\/5 分钟/);
        return true;
      },
    );
    const sends = calls.filter((call) => call.path === "/v2/groups/GRP1/messages");
    assert.equal(sends.length, 5);
    for (const call of sends) {
      assert.equal(asRecord(call.body).msg_type, 2);
      assert.equal(asRecord(call.body).msg_id, undefined);
    }
    await adapter.stop();
    await harness.cleanup();
  });
});

describe("qq typing and own messages", () => {
  it("sends the c2c input-state notify and skips groups", async () => {
    const harness = await makeCtx();
    const fake = fakeDeps(); const { deps, calls } = fake;
    const { adapter, socket } = await startConnected(harness, fake);
    dispatch(socket, "GROUP_AT_MESSAGE_CREATE", groupFrame());
    await waitFor(() => harness.messages.length === 1, "group scope recorded");
    await adapter.sendTyping?.("GRP1");
    assert.equal(calls.filter((call) => call.path === "/v2/groups/GRP1/messages").length, 0);
    dispatch(socket, "C2C_MESSAGE_CREATE", c2cFrame());
    await waitFor(() => harness.messages.length === 2, "c2c scope recorded");
    await adapter.sendTyping?.("U1");
    const typing = calls.filter((call) => call.path === "/v2/users/U1/messages");
    assert.equal(typing.length, 1);
    const body = asRecord(typing[0]!.body);
    assert.equal(body.msg_type, 6);
    assert.deepEqual(body.input_notify, { input_type: 1, input_second: 60 });
    await adapter.stop();
    await harness.cleanup();
  });

  it("tracks own sent message ids and skips their echo events", async () => {
    const harness = await makeCtx();
    const fake = fakeDeps(); const { deps } = fake;
    const { adapter, socket } = await startConnected(harness, fake);
    dispatch(socket, "C2C_MESSAGE_CREATE", c2cFrame());
    await waitFor(() => harness.messages.length === 1, "scope recorded");
    const result = await adapter.sendText({ chatId: "U1", text: "机器人回复" });
    assert.equal(result.messageId, "SENT-3");
    assert.equal(adapter.isOwnMessage?.("SENT-3"), true);
    assert.equal(adapter.isOwnMessage?.("SENT-999"), false);
    adapter.rememberOwnMessage?.("REMEMBERED");
    assert.equal(adapter.isOwnMessage?.("REMEMBERED"), true);
    dispatch(socket, "C2C_MESSAGE_CREATE", c2cFrame({ id: "SENT-3", content: "机器人回复" }));
    await sleep(30);
    assert.equal(harness.messages.length, 1, "自身消息回声不应再投递");
    await adapter.stop();
    await harness.cleanup();
  });
});

describe("qq misc", () => {
  it("reports markdown rejection only for the known business code", () => {
    assert.equal(isMarkdownRejection(new QqApiError("no md", 400, 40034090)), true);
    assert.equal(isMarkdownRejection(new QqApiError("other", 400, 500)), false);
    assert.equal(isMarkdownRejection(new QqApiError("server", 500, 40034090)), false);
    assert.equal(isMarkdownRejection(new Error("plain")), false);
  });

  it("testConnection verifies the token endpoint", async () => {
    const harness = await makeCtx();
    const working = fakeDeps();
    const okAdapter = createAdapter(harness.ctx, working.deps);
    assert.deepEqual(await okAdapter.testConnection?.(), { ok: true });
    const failing = fakeDeps({
      api: async () => {
        throw new TypeError("fetch failed");
      },
    });
    const badAdapter = createAdapter(harness.ctx, failing.deps);
    const result = await badAdapter.testConnection?.();
    assert.equal(result?.ok, false);
    assert.match(result?.detail ?? "", /fetch failed/);
    await harness.cleanup();
  });

  it("connection config satisfies the shared connection schema", async () => {
    const harness = await makeCtx();
    const parsed = connectionSchema.parse(harness.ctx.config satisfies Connection);
    assert.equal(parsed.channel, "qq");
    await harness.cleanup();
  });
});
