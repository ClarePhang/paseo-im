/** Tests for the Discord channel adapter: gateway handshake, normalize, routing,
 * chunking, outbound calls and state dedupe. All transports are faked. */
import assert from "node:assert/strict";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test, type TestContext } from "node:test";
import { setTimeout as delay } from "node:timers/promises";

import { UserFacingError } from "../../shared/errors.ts";
import type { NormalizedMessage, OutboundMedia, OutboundText } from "../../shared/model.ts";
import type { ImSettings } from "../../shared/settings.ts";
import {
  createAdapter,
  definition,
  type DiscordConnection,
  type DiscordDeps,
} from "../../server/channels/discord/index.ts";
import {
  DISCORD_INTENTS,
  DiscordRestError,
  chunkText,
  snowflakeTimestamp,
  stripBotMention,
  threadName,
  type GatewaySocket,
  type GatewaySocketEvent,
} from "../../server/channels/discord/protocol.ts";
import { createRestClient } from "../../server/channels/discord/rest.ts";
import type { AdapterContext } from "../../server/channels/types.ts";

const BOT_ID = "900000000000000001";
const TOKEN = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAA.BBBBBB.CCCCCCCCCCCCCCCCCCCCCCCCCCCC";
const GATEWAY_URL = "wss://gateway.discord.gg/?v=10&encodings=json";
const CONNECTION_ID = "8f0e3a2c-1b4d-4e5f-9a6b-7c8d9e0f1a2b";

type RestHandler = (method: string, path: string, body: unknown) => unknown;

class FakeSocket implements GatewaySocket {
  readonly url: string;
  readyState = 0;
  readonly sent: string[] = [];
  readonly closed: Array<{ code: number; reason: string }> = [];
  private readonly listeners = new Map<string, Array<(event: GatewaySocketEvent) => void>>();
  private closeEmitted = false;

  constructor(url: string) {
    this.url = url;
  }

  addEventListener(type: string, listener: (event: GatewaySocketEvent) => void): void {
    const list = this.listeners.get(type) ?? [];
    list.push(listener);
    this.listeners.set(type, list);
  }

  send(data: string): void {
    this.sent.push(data);
  }

  close(code = 1000, reason = ""): void {
    if (this.closeEmitted) return;
    this.closeEmitted = true;
    this.readyState = 3;
    this.closed.push({ code, reason });
    this.emit("close", { code, reason });
  }

  open(): void {
    this.readyState = 1;
  }

  receive(packet: unknown): void {
    this.emit("message", { data: JSON.stringify(packet) });
  }

  emit(type: string, event: GatewaySocketEvent): void {
    for (const listener of [...(this.listeners.get(type) ?? [])]) listener(event);
  }

  sentPackets(): unknown[] {
    return this.sent.map((raw) => JSON.parse(raw) as unknown);
  }
}

interface TestEnv {
  deps: DiscordDeps;
  handlers: Map<string, RestHandler>;
  calls: Array<{ method: string; path: string; body: unknown }>;
  sockets: FakeSocket[];
  downloads: string[];
}

function makeDeps(): TestEnv {
  const calls: Array<{ method: string; path: string; body: unknown }> = [];
  const handlers = new Map<string, RestHandler>();
  const sockets: FakeSocket[] = [];
  const downloads: string[] = [];
  const deps: DiscordDeps = {
    rest: async (method, path, body) => {
      calls.push({ method, path, body });
      const handler = handlers.get(`${method} ${path}`);
      if (!handler) throw new Error(`意外的 REST 调用：${method} ${path}`);
      return await handler(method, path, body);
    },
    openSocket: (url: string) => {
      const socket = new FakeSocket(url);
      sockets.push(socket);
      return socket;
    },
    download: async (url: string) => {
      downloads.push(url);
      return new TextEncoder().encode(`bytes-of-${url}`);
    },
    random: () => 0,
  };
  return { deps, handlers, calls, sockets, downloads };
}

interface CtxEnv {
  ctx: AdapterContext<DiscordConnection>;
  messages: NormalizedMessage[];
  errors: Error[];
  logs: string[];
  dataDir: string;
  mediaDir: string;
  controller: AbortController;
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

async function makeCtx(overrides: Partial<AdapterContext<DiscordConnection>> = {}): Promise<CtxEnv> {
  const messages: NormalizedMessage[] = [];
  const errors: Error[] = [];
  const logs: string[] = [];
  const dataDir = await mkdtemp(join(tmpdir(), "paseo-discord-data-"));
  const mediaDir = await mkdtemp(join(tmpdir(), "paseo-discord-media-"));
  const controller = new AbortController();
  const ctx: AdapterContext<DiscordConnection> = {
    connectionId: CONNECTION_ID,
    config: {
      channel: "discord",
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
      botUserId: BOT_ID,
      username: "paseo-bot",
    },
    secrets: { token: TOKEN },
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
  return { ctx, messages, errors, logs, dataDir, mediaDir, controller };
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
    await delay(10);
  }
}

function registerStartHandlers(handlers: Map<string, RestHandler>): void {
  handlers.set("GET /users/@me", () => ({ id: BOT_ID, username: "paseo-bot", bot: true }));
  handlers.set("GET /gateway/bot", () => ({ url: GATEWAY_URL }));
}

/** Full handshake: start, open socket, HELLO → identify, READY, first heartbeat. */
async function startConnected(
  t: TestContext,
  env: TestEnv,
  ctx: AdapterContext<DiscordConnection>,
): Promise<{ adapter: ReturnType<typeof createAdapter>; socket: FakeSocket }> {
  registerStartHandlers(env.handlers);
  const adapter = createAdapter(ctx, env.deps);
  t.after(() => adapter.stop());
  const started = adapter.start();
  await waitFor(() => env.sockets.length > 0);
  const socket = env.sockets[env.sockets.length - 1];
  socket.open();
  socket.receive({ op: 10, d: { heartbeat_interval: 1000 }, s: null });
  socket.receive({
    op: 0,
    t: "READY",
    s: 1,
    d: { session_id: "sess-1", resume_gateway_url: GATEWAY_URL, user: { id: BOT_ID, username: "paseo-bot" } },
  });
  await started;
  await delay(5); // first heartbeat tick (random() === 0 → zero jitter)
  socket.receive({ op: 11 }); // ack so the watchdog stays quiet
  return { adapter, socket };
}

function sendMessage(socket: FakeSocket, payload: unknown): void {
  socket.receive({ op: 0, t: "MESSAGE_CREATE", s: 2, d: payload });
}

function dmMessage(overrides: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    id: "3000000000000000001",
    channel_id: "5500000000000000001",
    author: { id: "2200000000000000001", username: "alice", global_name: "Alice" },
    content: "你好",
    timestamp: "2026-11-07T08:00:00.000000+00:00",
    type: 0,
    ...overrides,
  };
}

// ---------------------------------------------------------------- definition

test("definition exposes discord capabilities and configure", async (t) => {
  assert.equal(definition.channel, "discord");
  const env = makeDeps();
  const { ctx, dataDir, mediaDir } = await makeCtx();
  t.after(cleanup(dataDir, mediaDir));
  const adapter = createAdapter(ctx, env.deps);
  assert.deepEqual(adapter.capabilities, {
    progress: "edit",
    markdown: "native",
    uploadImage: true,
    uploadFile: true,
    reactions: true,
    typing: true,
    maxChunkChars: 2000,
    threads: true,
  });
  for (const name of [
    "start",
    "stop",
    "sendText",
    "sendMedia",
    "editMessage",
    "sendTyping",
    "addReaction",
    "leaveChat",
    "testConnection",
    "isOwnMessage",
    "rememberOwnMessage",
  ] as const) {
    assert.equal(typeof adapter[name], "function", `${name} 应该是函数`);
  }
});

test("configure splits secrets and validates required fields", async (t) => {
  const fields = definition.configure({ token: ` ${TOKEN} `, note: " 备注 " });
  assert.deepEqual(fields.secrets, { token: TOKEN });
  assert.deepEqual(fields.config, { note: "备注" });
  assert.throws(
    () => definition.configure({}),
    (error: unknown) => error instanceof UserFacingError && /Bot Token/.test(error.message),
  );
});

// ------------------------------------------------------------------- startup

test("start completes handshake, identifies and heartbeats", async (t) => {
  const env = makeDeps();
  const { ctx, dataDir, mediaDir } = await makeCtx();
  t.after(cleanup(dataDir, mediaDir));
  registerStartHandlers(env.handlers);
  const adapter = createAdapter(ctx, env.deps);
  t.after(() => adapter.stop());
  const started = adapter.start();
  await waitFor(() => env.sockets.length > 0);
  const socket = env.sockets[0];
  socket.open();
  socket.receive({ op: 10, d: { heartbeat_interval: 1000 }, s: null });
  await delay(5); // first heartbeat fires before READY → d stays null
  assert.deepEqual(
    env.calls.slice(0, 2).map((call) => `${call.method} ${call.path}`),
    ["GET /users/@me", "GET /gateway/bot"],
  );
  assert.equal(socket.url, GATEWAY_URL);
  const packets = socket.sentPackets();
  assert.deepEqual(packets[0], {
    op: 2,
    d: {
      token: TOKEN,
      intents: DISCORD_INTENTS,
      properties: { os: "linux", browser: "paseo-im", device: "paseo-im" },
    },
  });
  assert.equal(DISCORD_INTENTS, 1 | 512 | 4096 | 32768);
  assert.deepEqual(packets[1], { op: 1, d: null });
  socket.receive({
    op: 0,
    t: "READY",
    s: 1,
    d: { session_id: "sess-1", resume_gateway_url: GATEWAY_URL, user: { id: BOT_ID, username: "paseo-bot" } },
  });
  await started;
  socket.receive({ op: 11 });
  socket.receive({ op: 1 }); // server-requested heartbeat uses the latest seq
  const packetsAfter = socket.sentPackets();
  assert.deepEqual(packetsAfter[packetsAfter.length - 1], { op: 1, d: 1 });

  await adapter.stop();
  assert.deepEqual(socket.closed, [{ code: 1000, reason: "paseo-im 停止" }]);
});

test("start rejects when token is missing or malformed", async (t) => {
  const env = makeDeps();
  const { ctx, dataDir, mediaDir } = await makeCtx({ secrets: {} });
  t.after(cleanup(dataDir, mediaDir));
  await assert.rejects(
    createAdapter(ctx, env.deps).start(),
    (error: unknown) => error instanceof UserFacingError && /缺少 Discord Bot Token/.test(error.message),
  );
  const { ctx: ctx2, dataDir: dataDir2, mediaDir: mediaDir2 } = await makeCtx({ secrets: { token: "abc" } });
  t.after(cleanup(dataDir2, mediaDir2));
  await assert.rejects(
    createAdapter(ctx2, env.deps).start(),
    (error: unknown) => error instanceof UserFacingError && /格式不正确/.test(error.message),
  );
  assert.equal(env.calls.length, 0);
});

test("start rejects on 401 from the REST API", async (t) => {
  const env = makeDeps();
  const { ctx, dataDir, mediaDir } = await makeCtx();
  t.after(cleanup(dataDir, mediaDir));
  env.handlers.set("GET /users/@me", () => {
    throw new DiscordRestError(401, 0, "401: Unauthorized");
  });
  const adapter = createAdapter(ctx, env.deps);
  t.after(() => adapter.stop());
  await assert.rejects(
    adapter.start(),
    (error: unknown) => error instanceof UserFacingError && /Token/.test(error.message),
  );
  assert.equal(env.sockets.length, 0);
});

test("start rejects when token identity mismatches the saved bot", async (t) => {
  const env = makeDeps();
  const { ctx, dataDir, mediaDir } = await makeCtx();
  t.after(cleanup(dataDir, mediaDir));
  env.handlers.set("GET /users/@me", () => ({ id: "111111111111111111", username: "other", bot: true }));
  const adapter = createAdapter(ctx, env.deps);
  t.after(() => adapter.stop());
  await assert.rejects(
    adapter.start(),
    (error: unknown) => error instanceof UserFacingError && /不一致/.test(error.message),
  );
  assert.equal(env.sockets.length, 0);
});

test("start rejects when gateway url is absent", async (t) => {
  const env = makeDeps();
  const { ctx, dataDir, mediaDir } = await makeCtx();
  t.after(cleanup(dataDir, mediaDir));
  env.handlers.set("GET /users/@me", () => ({ id: BOT_ID, username: "paseo-bot", bot: true }));
  env.handlers.set("GET /gateway/bot", () => ({}));
  const adapter = createAdapter(ctx, env.deps);
  t.after(() => adapter.stop());
  await assert.rejects(
    adapter.start(),
    (error: unknown) => error instanceof UserFacingError && /Gateway 地址/.test(error.message),
  );
  assert.equal(env.sockets.length, 0);
});

test("transient close before READY reports onError and reconnects", async (t) => {
  const env = makeDeps();
  const { ctx, errors, dataDir, mediaDir } = await makeCtx();
  t.after(cleanup(dataDir, mediaDir));
  registerStartHandlers(env.handlers);
  const adapter = createAdapter(ctx, env.deps);
  t.after(() => adapter.stop());
  const started = adapter.start();
  await waitFor(() => env.sockets.length > 0);
  const socket = env.sockets[0];
  socket.open();
  socket.receive({ op: 10, d: { heartbeat_interval: 1000 }, s: null });
  socket.close(4001, "network blip");
  await started; // transient failures resolve start(); onError carries the report
  assert.equal(errors.length, 1);
  assert.match(errors[0].message, /Discord Gateway 连接关闭（4001）/);
  await waitFor(() => env.sockets.length === 2, 3000); // backoff [1s] fired
  assert.match(env.calls[env.calls.length - 1].path, /\/gateway\/bot/);
});

test("permanent close 4004 reports once and never reconnects", async (t) => {
  const env = makeDeps();
  const { ctx, errors, dataDir, mediaDir } = await makeCtx();
  t.after(cleanup(dataDir, mediaDir));
  const { adapter, socket } = await startConnected(t, env, ctx);
  socket.close(4004, "Authentication failed");
  await delay(20);
  assert.equal(errors.length, 1);
  assert.ok(errors[0] instanceof UserFacingError);
  assert.match(errors[0].message, /Token/);
  await delay(1150); // reconnect backoff window
  assert.equal(env.sockets.length, 1); // no reconnect for a config error
  await adapter.stop();
});

test("unacknowledged heartbeat closes the socket", async (t) => {
  const env = makeDeps();
  const { ctx, errors, dataDir, mediaDir } = await makeCtx();
  t.after(cleanup(dataDir, mediaDir));
  registerStartHandlers(env.handlers);
  const adapter = createAdapter(ctx, env.deps);
  t.after(() => adapter.stop());
  const started = adapter.start();
  await waitFor(() => env.sockets.length > 0);
  const socket = env.sockets[0];
  socket.open();
  socket.receive({ op: 10, d: { heartbeat_interval: 1000 }, s: null });
  await started;
  await delay(20); // first heartbeat sent, never acked
  assert.deepEqual(socket.sentPackets()[1], { op: 1, d: null });
  await delay(1100); // next tick: watchdog closes with 4000
  await started;
  assert.deepEqual(socket.closed, [{ code: 4000, reason: "heartbeat not acknowledged" }]);
  assert.equal(errors.length, 1);
});

test("stop cancels a pending reconnect", async (t) => {
  const env = makeDeps();
  const { ctx, errors, dataDir, mediaDir } = await makeCtx();
  t.after(cleanup(dataDir, mediaDir));
  registerStartHandlers(env.handlers);
  const adapter = createAdapter(ctx, env.deps);
  t.after(() => adapter.stop());
  const started = adapter.start();
  await waitFor(() => env.sockets.length > 0);
  const socket = env.sockets[0];
  socket.open();
  socket.receive({ op: 10, d: { heartbeat_interval: 1000 }, s: null });
  socket.close(4001, "network blip");
  await started;
  await adapter.stop();
  await delay(1150);
  assert.equal(env.sockets.length, 1);
  assert.ok(errors.length >= 1);
});

test("stop is idempotent", async (t) => {
  const env = makeDeps();
  const { ctx, dataDir, mediaDir } = await makeCtx();
  t.after(cleanup(dataDir, mediaDir));
  const { adapter, socket } = await startConnected(t, env, ctx);
  await adapter.stop();
  await adapter.stop();
  assert.deepEqual(socket.closed, [{ code: 1000, reason: "paseo-im 停止" }]);
});

test("aborting ctx.signal stops the adapter", async (t) => {
  const env = makeDeps();
  const { ctx, controller, dataDir, mediaDir } = await makeCtx();
  t.after(cleanup(dataDir, mediaDir));
  const { socket } = await startConnected(t, env, ctx);
  controller.abort();
  await waitFor(() => socket.closed.length > 0);
  assert.deepEqual(socket.closed, [{ code: 1000, reason: "paseo-im 停止" }]);
});

// ----------------------------------------------------------------- normalize

test("normalizes a DM message and dedupes repeats", async (t) => {
  const env = makeDeps();
  const { ctx, messages, dataDir, mediaDir } = await makeCtx();
  t.after(cleanup(dataDir, mediaDir));
  const { socket } = await startConnected(t, env, ctx);
  sendMessage(socket, dmMessage());
  await waitFor(() => messages.length === 1);
  const message = messages[0];
  assert.deepEqual(message, {
    channel: "discord",
    connectionId: CONNECTION_ID,
    kind: "dm",
    chatId: "5500000000000000001",
    senderId: "2200000000000000001",
    senderName: "Alice",
    messageId: "3000000000000000001",
    timestamp: Date.parse("2026-11-07T08:00:00.000000+00:00"),
    text: "你好",
    trigger: { mention: false, replyToBot: false, command: false },
  });
  sendMessage(socket, dmMessage());
  await delay(30);
  assert.equal(messages.length, 1);
});

test("strips bot mention tags from text", async (t) => {
  const env = makeDeps();
  const { ctx, messages, dataDir, mediaDir } = await makeCtx();
  t.after(cleanup(dataDir, mediaDir));
  const { socket } = await startConnected(t, env, ctx);
  sendMessage(socket, dmMessage({ content: `<@${BOT_ID}> 帮忙 <@!${BOT_ID}> 查天气` }));
  await waitFor(() => messages.length === 1);
  assert.equal(messages[0].text, "帮忙  查天气");
});

test("normalizes a DM with an image attachment into mediaDir", async (t) => {
  const env = makeDeps();
  const { ctx, messages, dataDir, mediaDir } = await makeCtx();
  t.after(cleanup(dataDir, mediaDir));
  const { socket } = await startConnected(t, env, ctx);
  const url = "https://cdn.discordapp.com/attachments/a/b/cat.png";
  sendMessage(
    socket,
    dmMessage({
      content: "",
      attachments: [
        { id: "7000000000000000001", filename: "cat.png", content_type: "image/png", url, size: 12 },
      ],
    }),
  );
  await waitFor(() => messages.length === 1);
  assert.deepEqual(env.downloads, [url]);
  const path = join(mediaDir, "7000000000000000001-cat.png");
  assert.deepEqual(messages[0].media, [{ kind: "image", path, mime: "image/png", name: "cat.png" }]);
  assert.deepEqual(messages[0].text, undefined);
  assert.equal(
    new TextDecoder().decode(await readFile(path)),
    `bytes-of-${url}`,
  );
});

test("guild mention creates a native thread and routes into it", async (t) => {
  const env = makeDeps();
  const { ctx, messages, dataDir, mediaDir } = await makeCtx();
  t.after(cleanup(dataDir, mediaDir));
  const { socket } = await startConnected(t, env, ctx);
  let threadBody: unknown;
  env.handlers.set("GET /channels/4444000000000000001", () => ({
    id: "4444000000000000001",
    type: 0,
    name: "general",
    guild_id: "6600000000000000001",
  }));
  env.handlers.set("POST /channels/4444000000000000001/messages/3000000000000000002/threads", (_m, _p, body) => {
    threadBody = body;
    return { id: "8888000000000000001", type: 11, name: "帮我查天气", owner_id: BOT_ID, parent_id: "4444000000000000001" };
  });
  sendMessage(
    socket,
    dmMessage({
      id: "3000000000000000002",
      guild_id: "6600000000000000001",
      channel_id: "4444000000000000001",
      author: { id: "2200000000000000002", username: "bob" },
      content: `<@${BOT_ID}> 帮我查天气`,
      mentions: [{ id: BOT_ID, username: "paseo-bot" }],
    }),
  );
  await waitFor(() => messages.length === 1);
  assert.deepEqual(threadBody, { name: "帮我查天气", auto_archive_duration: 1440 });
  const message = messages[0];
  assert.equal(message.kind, "group");
  assert.equal(message.chatId, "4444000000000000001");
  assert.equal(message.threadId, "8888000000000000001");
  assert.equal(message.text, "帮我查天气");
  assert.deepEqual(message.trigger, { mention: true, replyToBot: false, command: false });

  const state = JSON.parse(await readFile(join(dataDir, "discord-state.json"), "utf8")) as {
    seen: string[];
  };
  assert.ok(state.seen.includes("3000000000000000002"));
});

test("messages inside the bot's managed thread route to the thread", async (t) => {
  const env = makeDeps();
  const { ctx, messages, dataDir, mediaDir } = await makeCtx();
  t.after(cleanup(dataDir, mediaDir));
  const { socket } = await startConnected(t, env, ctx);
  env.handlers.set("GET /channels/8888000000000000001", () => ({
    id: "8888000000000000001",
    type: 11,
    name: "帮我查天气",
    owner_id: BOT_ID,
    parent_id: "4444000000000000001",
    guild_id: "6600000000000000001",
  }));
  sendMessage(
    socket,
    dmMessage({
      id: "3000000000000000003",
      guild_id: "6600000000000000001",
      channel_id: "8888000000000000001",
      author: { id: "2200000000000000002", username: "bob" },
      content: "跟进一下",
    }),
  );
  await waitFor(() => messages.length === 1);
  const message = messages[0];
  assert.equal(message.kind, "group");
  assert.equal(message.chatId, "4444000000000000001");
  assert.equal(message.threadId, "8888000000000000001");
  assert.deepEqual(message.trigger, { mention: true, replyToBot: false, command: false });
  assert.equal(env.calls.some((call) => call.path.includes("/threads")), false);
});

test("messages in a member-created thread are not force-mentioned", async (t) => {
  const env = makeDeps();
  const { ctx, messages, dataDir, mediaDir } = await makeCtx();
  t.after(cleanup(dataDir, mediaDir));
  const { socket } = await startConnected(t, env, ctx);
  env.handlers.set("GET /channels/8888000000000000002", () => ({
    id: "8888000000000000002",
    type: 12,
    name: "别人的线程",
    owner_id: "2200000000000000009",
    parent_id: "4444000000000000001",
    guild_id: "6600000000000000001",
  }));
  sendMessage(
    socket,
    dmMessage({
      id: "3000000000000000004",
      guild_id: "6600000000000000001",
      channel_id: "8888000000000000002",
      author: { id: "2200000000000000002", username: "bob" },
      content: "普通聊天",
    }),
  );
  await waitFor(() => messages.length === 1);
  assert.equal(messages[0].chatId, "4444000000000000001");
  assert.equal(messages[0].threadId, "8888000000000000002");
  assert.deepEqual(messages[0].trigger, { mention: false, replyToBot: false, command: false });
});

test("reply to the bot in a text channel starts a thread with reply snapshot", async (t) => {
  const env = makeDeps();
  const { ctx, messages, dataDir, mediaDir } = await makeCtx();
  t.after(cleanup(dataDir, mediaDir));
  const { socket } = await startConnected(t, env, ctx);
  env.handlers.set("GET /channels/4444000000000000001", () => ({
    id: "4444000000000000001",
    type: 0,
    guild_id: "6600000000000000001",
  }));
  env.handlers.set("POST /channels/4444000000000000001/messages/3000000000000000005/threads", () => ({
    id: "8888000000000000003",
    type: 11,
    owner_id: BOT_ID,
    parent_id: "4444000000000000001",
  }));
  sendMessage(
    socket,
    dmMessage({
      id: "3000000000000000005",
      guild_id: "6600000000000000001",
      channel_id: "4444000000000000001",
      author: { id: "2200000000000000002", username: "bob" },
      content: "再帮我看看",
      type: 19,
      message_reference: { type: 0, message_id: "3000000000000000002", channel_id: "4444000000000000001" },
      referenced_message: {
        id: "3000000000000000002",
        channel_id: "4444000000000000001",
        author: { id: BOT_ID, username: "paseo-bot", bot: true },
        content: "好的",
      },
    }),
  );
  await waitFor(() => messages.length === 1);
  const message = messages[0];
  assert.deepEqual(message.trigger, { mention: false, replyToBot: true, command: false });
  assert.equal(message.threadId, "8888000000000000003");
  assert.deepEqual(message.replyTo, {
    id: "3000000000000000002",
    text: "好的",
    sender: BOT_ID,
  });
});

test("reply to another member is not addressed", async (t) => {
  const env = makeDeps();
  const { ctx, messages, dataDir, mediaDir } = await makeCtx();
  t.after(cleanup(dataDir, mediaDir));
  const { socket } = await startConnected(t, env, ctx);
  env.handlers.set("GET /channels/4444000000000000001", () => ({
    id: "4444000000000000001",
    type: 0,
    guild_id: "6600000000000000001",
  }));
  sendMessage(
    socket,
    dmMessage({
      id: "3000000000000000006",
      guild_id: "6600000000000000001",
      channel_id: "4444000000000000001",
      author: { id: "2200000000000000002", username: "bob" },
      content: "普通回复",
      message_reference: { type: 0, message_id: "3000000000000000001", channel_id: "4444000000000000001" },
      referenced_message: {
        id: "3000000000000000001",
        channel_id: "4444000000000000001",
        author: { id: "2200000000000000001", username: "alice" },
        content: "原始消息",
      },
    }),
  );
  await waitFor(() => messages.length === 1);
  const message = messages[0];
  assert.deepEqual(message.trigger, { mention: false, replyToBot: false, command: false });
  assert.equal(message.threadId, undefined);
  assert.deepEqual(message.replyTo, {
    id: "3000000000000000001",
    text: "原始消息",
    sender: "2200000000000000001",
  });
  assert.equal(env.calls.some((call) => call.path.includes("/threads")), false);
});

test("bot own messages, noise and edits are not emitted", async (t) => {
  const env = makeDeps();
  const { ctx, messages, dataDir, mediaDir } = await makeCtx();
  t.after(cleanup(dataDir, mediaDir));
  const { socket } = await startConnected(t, env, ctx);
  sendMessage(socket, dmMessage({ author: { id: BOT_ID, username: "paseo-bot", bot: true } }));
  sendMessage(socket, dmMessage({ content: "" }));
  sendMessage(socket, { op: 0, t: "MESSAGE_UPDATE", s: 3, d: dmMessage({ content: "编辑后的内容" }) });
  sendMessage(socket, dmMessage({ id: "3000000000000000009", type: 21, content: "系统消息" }));
  await delay(40);
  assert.equal(messages.length, 0);
});

test("unsupported guild channel types are skipped", async (t) => {
  const env = makeDeps();
  const { ctx, messages, dataDir, mediaDir } = await makeCtx();
  t.after(cleanup(dataDir, mediaDir));
  const { socket } = await startConnected(t, env, ctx);
  env.handlers.set("GET /channels/7777000000000000001", () => ({
    id: "7777000000000000001",
    type: 2, // voice
    guild_id: "6600000000000000001",
  }));
  sendMessage(
    socket,
    dmMessage({
      guild_id: "6600000000000000001",
      channel_id: "7777000000000000001",
      content: "语音频道文本",
    }),
  );
  await delay(40);
  assert.equal(messages.length, 0);
});

test("channel lookup failure degrades to a plain group", async (t) => {
  const env = makeDeps();
  const { ctx, messages, dataDir, mediaDir } = await makeCtx();
  t.after(cleanup(dataDir, mediaDir));
  const { socket } = await startConnected(t, env, ctx);
  env.handlers.set("GET /channels/4444000000000000001", () => {
    throw new DiscordRestError(500, 0, "internal");
  });
  sendMessage(
    socket,
    dmMessage({
      guild_id: "6600000000000000001",
      channel_id: "4444000000000000001",
      content: `<@${BOT_ID}> 在吗`,
      mentions: [{ id: BOT_ID }],
    }),
  );
  await waitFor(() => messages.length === 1);
  const message = messages[0];
  assert.equal(message.kind, "group");
  assert.equal(message.chatId, "4444000000000000001");
  assert.equal(message.threadId, undefined);
  assert.equal(message.text, "在吗");
});

test("timestamp falls back to the snowflake epoch", async (t) => {
  const env = makeDeps();
  const { ctx, messages, dataDir, mediaDir } = await makeCtx();
  t.after(cleanup(dataDir, mediaDir));
  const { socket } = await startConnected(t, env, ctx);
  const payload = dmMessage({ timestamp: undefined });
  delete (payload as { timestamp?: unknown }).timestamp;
  sendMessage(socket, payload);
  await waitFor(() => messages.length === 1);
  assert.equal(messages[0].timestamp, snowflakeTimestamp("3000000000000000001"));
});

// ------------------------------------------------------------------ outbound

test("sendText chunks long text and returns the last message id", async (t) => {
  const env = makeDeps();
  const { ctx, dataDir, mediaDir } = await makeCtx();
  t.after(cleanup(dataDir, mediaDir));
  const { adapter } = await startConnected(t, env, ctx);
  let sent = 0;
  env.handlers.set("POST /channels/8888000000000000001/messages", () => {
    sent += 1;
    return { id: `sent-${sent}` };
  });
  const result = await adapter.sendText({ chatId: "4444000000000000001", threadId: "8888000000000000001", text: "a".repeat(4500) });
  assert.equal(result.messageId, "sent-3");
  const posts = env.calls.filter((call) => call.method === "POST" && call.path.includes("/messages"));
  assert.equal(posts.length, 3);
  for (const post of posts) {
    const body = post.body as { content: string; allowed_mentions: { parse: string[] } };
    assert.ok(body.content.length <= 2000);
    assert.deepEqual(body.allowed_mentions, { parse: [] });
  }
  assert.deepEqual(
    posts.map((post) => (post.body as { content: string }).content.length),
    [2000, 2000, 500],
  );
});

test("sendText attaches message_reference only to the first chunk", async (t) => {
  const env = makeDeps();
  const { ctx, dataDir, mediaDir } = await makeCtx();
  t.after(cleanup(dataDir, mediaDir));
  const { adapter } = await startConnected(t, env, ctx);
  env.handlers.set("POST /channels/4444000000000000001/messages", () => ({ id: "sent-1" }));
  await adapter.sendText({ chatId: "4444000000000000001", text: "b".repeat(2500), replyToId: "3000000000000000001" });
  const posts = env.calls.filter((call) => call.method === "POST" && call.path.includes("/messages"));
  assert.equal(posts.length, 2);
  assert.deepEqual((posts[0].body as { message_reference?: unknown }).message_reference, {
    message_id: "3000000000000000001",
    fail_if_not_exists: false,
  });
  assert.equal((posts[1].body as { message_reference?: unknown }).message_reference, undefined);
});

test("sendText skips empty text without calls", async (t) => {
  const env = makeDeps();
  const { ctx, dataDir, mediaDir } = await makeCtx();
  t.after(cleanup(dataDir, mediaDir));
  const { adapter } = await startConnected(t, env, ctx);
  assert.deepEqual(await adapter.sendText({ chatId: "4444000000000000001", text: "   " }), {});
  assert.deepEqual(await adapter.sendText({ chatId: "4444000000000000001", text: "" }), {});
  assert.equal(env.calls.filter((call) => call.path.includes("/messages")).length, 0);
});

test("editMessage prefers the channel the message was sent to", async (t) => {
  const env = makeDeps();
  const { ctx, dataDir, mediaDir } = await makeCtx();
  t.after(cleanup(dataDir, mediaDir));
  const { adapter } = await startConnected(t, env, ctx);
  env.handlers.set("POST /channels/8888000000000000001/messages", () => ({ id: "sent-1" }));
  env.handlers.set("PATCH /channels/8888000000000000001/messages/sent-1", () => ({ id: "sent-1" }));
  env.handlers.set("PATCH /channels/4444000000000000001/messages/unknown-id", () => ({ id: "unknown-id" }));
  await adapter.sendText({ chatId: "4444000000000000001", threadId: "8888000000000000001", text: "初稿" });
  await adapter.editMessage!("sent-1", "4444000000000000001", "修改稿");
  await adapter.editMessage!("unknown-id", "4444000000000000001", "回退路径");
  const patches = env.calls.filter((call) => call.method === "PATCH");
  assert.deepEqual(
    patches.map((call) => call.path),
    [
      "/channels/8888000000000000001/messages/sent-1",
      "/channels/4444000000000000001/messages/unknown-id",
    ],
  );
  assert.deepEqual((patches[0].body as { content: string }).content, "修改稿");
});

test("sendTyping and addReaction hit the documented endpoints", async (t) => {
  const env = makeDeps();
  const { ctx, dataDir, mediaDir } = await makeCtx();
  t.after(cleanup(dataDir, mediaDir));
  const { adapter } = await startConnected(t, env, ctx);
  env.handlers.set("POST /channels/4444000000000000001/typing", () => undefined);
  env.handlers.set("POST /channels/8888000000000000001/typing", () => undefined);
  env.handlers.set(
    "PUT /channels/8888000000000000001/messages/sent-1/reactions/%F0%9F%91%8D/@me",
    () => undefined,
  );
  env.handlers.set("POST /channels/8888000000000000001/messages", () => ({ id: "sent-1" }));
  await adapter.sendTyping!("4444000000000000001");
  await adapter.addReaction!("sent-1", "8888000000000000001", "👍");
  const paths = env.calls.map((call) => `${call.method} ${call.path}`);
  assert.ok(paths.includes("POST /channels/4444000000000000001/typing"));
  assert.ok(paths.includes("PUT /channels/8888000000000000001/messages/sent-1/reactions/%F0%9F%91%8D/@me"));
});

test("sendMedia uploads multipart payload_json plus file", async (t) => {
  const env = makeDeps();
  const { ctx, dataDir, mediaDir } = await makeCtx();
  t.after(cleanup(dataDir, mediaDir));
  const { adapter } = await startConnected(t, env, ctx);
  const imagePath = join(mediaDir, "upload.png");
  await writeFile(imagePath, Buffer.from("png-bytes"));
  let form: FormData | undefined;
  env.handlers.set("POST /channels/8888000000000000001/messages", (_m, _p, body) => {
    form = body as FormData;
    return { id: "media-1" };
  });
  const outbound: OutboundMedia = {
    chatId: "4444000000000000001",
    threadId: "8888000000000000001",
    kind: "image",
    path: imagePath,
    mime: "image/png",
    name: "pic.png",
    caption: "看图",
  };
  const result = await adapter.sendMedia!(outbound);
  assert.equal(result.messageId, "media-1");
  assert.ok(form instanceof FormData);
  const payload = JSON.parse(form.get("payload_json") as string) as {
    content?: string;
    attachments: Array<{ id: number; filename: string }>;
  };
  assert.deepEqual(payload.attachments, [{ id: 0, filename: "pic.png" }]);
  assert.equal(payload.content, "看图");
  const file = form.get("file");
  assert.ok(file instanceof File);
  assert.equal(file.name, "pic.png");
  assert.equal(file.type, "image/png");
  assert.equal(file.size, Buffer.from("png-bytes").length);
  assert.equal(adapter.isOwnMessage!("media-1"), true);
});

test("testConnection reports ok and failure details", async (t) => {
  const env = makeDeps();
  const { ctx, dataDir, mediaDir } = await makeCtx();
  t.after(cleanup(dataDir, mediaDir));
  const adapter = createAdapter(ctx, env.deps);
  registerStartHandlers(env.handlers);
  const ok = await adapter.testConnection!();
  assert.equal(ok.ok, true);
  assert.match(ok.detail ?? "", /@paseo-bot/);

  env.handlers.set("GET /users/@me", () => ({ id: "111111111111111111", username: "other", bot: true }));
  const mismatch = await adapter.testConnection!();
  assert.equal(mismatch.ok, false);
  assert.match(mismatch.detail ?? "", /不一致/);

  env.handlers.set("GET /users/@me", () => {
    throw new DiscordRestError(401, 0, "401: Unauthorized");
  });
  const bad = await adapter.testConnection!();
  assert.equal(bad.ok, false);
  assert.match(bad.detail ?? "", /Token 无效/);
});

test("leaveChat leaves the owning guild and ignores DMs", async (t) => {
  const env = makeDeps();
  const { ctx, dataDir, mediaDir } = await makeCtx();
  t.after(cleanup(dataDir, mediaDir));
  const { adapter } = await startConnected(t, env, ctx);
  env.handlers.set("GET /channels/4444000000000000001", () => ({
    id: "4444000000000000001",
    type: 0,
    guild_id: "6600000000000000001",
  }));
  env.handlers.set("DELETE /users/@me/guilds/6600000000000000001", () => undefined);
  await adapter.leaveChat!("4444000000000000001");
  assert.ok(env.calls.some((call) => call.path === "/users/@me/guilds/6600000000000000001" && call.method === "DELETE"));
  const before = env.calls.length;
  await adapter.leaveChat!("5500000000000000001"); // unknown → not a guild channel → no-op
  assert.equal(env.calls.length, before + 1); // only the channel lookup
});

// --------------------------------------------------------------- state dedupe

test("seen state dedupes across restarts", async (t) => {
  const env = makeDeps();
  const { ctx, messages, dataDir, mediaDir } = await makeCtx();
  t.after(cleanup(dataDir, mediaDir));
  const first = await startConnected(t, env, ctx);
  sendMessage(first.socket, dmMessage());
  await waitFor(() => messages.length === 1);
  await first.adapter.stop();

  const second = makeDeps();
  const secondCtx: AdapterContext<DiscordConnection> = { ...ctx, onMessage: messages.push.bind(messages) } as AdapterContext<DiscordConnection>;
  const restarted = await startConnected(t, second, secondCtx);
  sendMessage(restarted.socket, dmMessage());
  await delay(40);
  assert.equal(messages.length, 1); // duplicate suppressed from persisted state
  sendMessage(restarted.socket, dmMessage({ id: "3000000000000000042", content: "新消息" }));
  await waitFor(() => messages.length === 2);
});

// --------------------------------------------------------------- rest client

interface ScriptedFetch {
  fetchImpl: typeof fetch;
  calls: Array<{ url: string; init: RequestInit }>;
}

function scriptedFetch(responses: Array<() => Response>): ScriptedFetch {
  const calls: Array<{ url: string; init: RequestInit }> = [];
  const fetchImpl = (async (input: string | URL | Request, init?: RequestInit): Promise<Response> => {
    calls.push({ url: String(input), init: init ?? {} });
    const next = responses.shift();
    if (!next) throw new Error("no scripted response");
    return next();
  }) as unknown as typeof fetch;
  return { fetchImpl, calls };
}

test("rest client sends bot auth, json body and parses responses", async () => {
  const { fetchImpl, calls } = scriptedFetch([() => new Response(JSON.stringify({ id: "m1" }), { status: 200 })]);
  const rest = createRestClient({ token: TOKEN, fetchImpl });
  const result = await rest("POST", "/channels/1/messages", { content: "hi" });
  assert.deepEqual(result, { id: "m1" });
  assert.equal(calls[0].url, "https://discord.com/api/v10/channels/1/messages");
  assert.equal(calls[0].init.method, "POST");
  const headers = calls[0].init.headers as Record<string, string>;
  assert.equal(headers.Authorization, `Bot ${TOKEN}`);
  assert.equal(headers["Content-Type"], "application/json");
  assert.equal(calls[0].init.body, JSON.stringify({ content: "hi" }));
});

test("rest client honors retry_after on 429 then succeeds", async () => {
  const { fetchImpl, calls } = scriptedFetch([
    () => new Response(JSON.stringify({ retry_after: 0.03 }), { status: 429 }),
    () => new Response(JSON.stringify({ id: "ok" }), { status: 200 }),
  ]);
  const rest = createRestClient({ token: TOKEN, fetchImpl });
  const startedAt = Date.now();
  const result = (await rest("POST", "/channels/1/messages", { content: "x" })) as { id?: string };
  assert.equal(result.id, "ok");
  assert.ok(Date.now() - startedAt >= 25);
  assert.equal(calls.length, 2);
});

test("rest client exhausts 429 retries into a DiscordRestError", async () => {
  const { fetchImpl, calls } = scriptedFetch([
    () => new Response(JSON.stringify({ retry_after: 0.01 }), { status: 429 }),
    () => new Response(JSON.stringify({ retry_after: 0.01 }), { status: 429 }),
    () => new Response(JSON.stringify({ retry_after: 0.01 }), { status: 429 }),
  ]);
  const rest = createRestClient({ token: TOKEN, fetchImpl });
  await assert.rejects(
    rest("POST", "/channels/1/messages", {}),
    (error: unknown) => error instanceof DiscordRestError && error.status === 429,
  );
  assert.equal(calls.length, 3);
});

test("rest client maps provider errors and passes FormData through", async () => {
  const { fetchImpl, calls } = scriptedFetch([
    () => new Response(JSON.stringify({ message: "Missing Permissions", code: 50013 }), { status: 403 }),
    () => new Response(null, { status: 204 }),
  ]);
  const rest = createRestClient({ token: TOKEN, fetchImpl });
  await assert.rejects(
    rest("POST", "/channels/1/messages", {}),
    (error: unknown) => error instanceof DiscordRestError && error.status === 403 && error.providerCode === 50013,
  );
  const form = new FormData();
  form.append("payload_json", "{}");
  form.append("file", new File(["x"], "a.png"));
  assert.equal(await rest("POST", "/channels/1/messages", form), undefined);
  const formCall = calls[1];
  assert.equal(formCall.init.body, form);
  const headers = formCall.init.headers as Record<string, string>;
  assert.equal(headers["Content-Type"], undefined);
});

// ---------------------------------------------------------------- chunking

test("chunkText splits plain text at the limit", () => {
  assert.deepEqual(chunkText("short", 2000), ["short"]);
  assert.deepEqual(chunkText("a".repeat(2000), 2000), ["a".repeat(2000)]);
  assert.deepEqual(chunkText("a".repeat(2001), 2000), ["a".repeat(2000), "a"]);
  assert.deepEqual(chunkText("a".repeat(150) + "\n" + "b".repeat(50), 100), [
    "a".repeat(100),
    "a".repeat(50),
    "b".repeat(50),
  ]);
  assert.deepEqual(chunkText("行1\n行2\n行3", 100), ["行1\n行2\n行3"]);
  assert.throws(() => chunkText("x", 7), RangeError);
});

test("chunkText keeps code fences balanced across chunks", () => {
  const text = `\`\`\`js\n${"x".repeat(1990)}\nmore\n${"y".repeat(40)}\n\`\`\``;
  const chunks = chunkText(text, 2000);
  assert.equal(chunks.length, 2);
  for (const chunk of chunks) assert.ok(chunk.length <= 2000);
  assert.equal(chunks[0], `\`\`\`js\n${"x".repeat(1990)}\n\`\`\``);
  assert.equal(chunks[1], `\`\`\`js\nmore\n${"y".repeat(40)}\n\`\`\``);
});

test("chunkText hard-splits oversized lines inside fences", () => {
  const zs = "z".repeat(5000);
  const chunks = chunkText(`\`\`\`ts\n${zs}\n\`\`\``, 2000);
  for (const chunk of chunks) assert.ok(chunk.length <= 2000, "分块超长");
  const zTotal = chunks.reduce((sum, chunk) => sum + (chunk.match(/z/g)?.length ?? 0), 0);
  assert.equal(zTotal, 5000);
  assert.equal(chunks[0], "```ts\n```");
  assert.equal(chunks[1], `\`\`\`ts\n${"z".repeat(1990)}\n\`\`\``);
  for (const chunk of chunks.slice(1)) {
    assert.ok(chunk.startsWith("```ts\n"), "重开围栏");
    assert.ok(chunk.endsWith("\n```"), "闭合围栏");
  }
});

test("protocol helpers: mention stripping, thread names, snowflakes", () => {
  assert.equal(stripBotMention(`<@${BOT_ID}>hi<@!${BOT_ID}>`, BOT_ID), "hi");
  assert.equal(threadName(`  <@${BOT_ID}> 多  空格 `, BOT_ID), "多 空格");
  assert.equal(threadName("", BOT_ID), "Paseo 会话");
  assert.equal(threadName("长".repeat(150), BOT_ID).length, 100);
  assert.equal(snowflakeTimestamp("1000000000000000001"), Number((BigInt("1000000000000000001") >> 22n) + 1_420_070_400_000n));
  assert.equal(snowflakeTimestamp("nope"), undefined);
});
