/** bots-contract/v1 集成测试：loader 热载/容错/锁 + 契约路由 + F23 发送侧拦截。 */
import assert from "node:assert/strict";
import { test } from "node:test";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { mkdir, rename, readFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { once } from "node:events";
import { spawn } from "node:child_process";
import type { PaseoApi } from "@getpaseo/client";
import {
  BotsContractLoader,
  CONTRACT_VERSION,
  DEFAULT_SENSITIVE_PATTERNS,
  paseoImTargetsV1,
  type PaseoImTargetEntry,
} from "../server/bots-contract.ts";
import { Bridge, type BridgeDeps } from "../server/bridge.ts";
import { Router, resolveTarget } from "../server/routing.ts";
import { AgentQueue } from "../server/queue.ts";
import { StateStore } from "../server/state-store.ts";
import { DeferredRegistry } from "../server/deferred.ts";
import { sleep } from "../server/files.ts";
import { emptyPolicy, connectionSchema, type Connection, type Target } from "../shared/config.ts";
import { imSettings } from "../shared/settings.ts";
import type { ChannelAdapter } from "../server/channels/types.ts";
import type { NormalizedMessage, OutboundText, SendResult } from "../shared/model.ts";

const CONNECTION_ID = "00000000-0000-4000-8000-000000000002";
const AGENT_ID = "agent-contract-1";
const FILE_NAME = "paseo-im-targets.json";

// ------------------------------------------------------------------ fixtures

function entry(overrides: Partial<PaseoImTargetEntry> = {}): PaseoImTargetEntry {
  return {
    id: "bots-plus:bot_1",
    agentId: AGENT_ID,
    sensitiveCheck: false,
    patterns: [],
    ...overrides,
  };
}

function doc(targets: PaseoImTargetEntry[]): { version: string; targets: PaseoImTargetEntry[] } {
  return { version: CONTRACT_VERSION, targets };
}

async function tempContractDir(): Promise<{ dir: string; file: string }> {
  const dir = await mkdtemp(join(tmpdir(), "paseo-im-contract-"));
  return { dir, file: join(dir, FILE_NAME) };
}

/** 与 bots+ 写入侧一致的原子写（tmp + rename），确保目录 watch 的 rename 事件触发热载。 */
async function writeAtomic(file: string, value: unknown): Promise<void> {
  const tmp = `${file}.tmp-${Date.now()}`;
  await writeFile(tmp, JSON.stringify(value, null, 2), "utf8");
  await rename(tmp, file);
}

/** 轮询断言：loader 热载走真实 fs.watch + debounce（平台时钟），无法用假定时器
 * 驱动；此处等待的是真实条件达成而非固定时长，10ms 探测、2s 即败。 */
async function until(check: () => boolean, timeoutMs = 2_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!check()) {
    if (Date.now() > deadline) throw new Error("until：条件等待超时");
    await sleep(10);
  }
}

interface LoaderLogs {
  logs: string[];
  log: (level: "info" | "warn" | "error", message: string) => void;
}

function captureLogs(): LoaderLogs {
  const logs: string[] = [];
  return { logs, log: (level, message) => logs.push(`${level}: ${message}`) };
}

// ---------------------------------------------------------- loader：热载/容错

test("loader：合法契约文件载入，原子改写后热更新（debounce）", async () => {
  const { dir, file } = await tempContractDir();
  const captured = captureLogs();
  const loader = new BotsContractLoader({ file, log: captured.log, debounceMs: 25 });
  try {
    await writeAtomic(file, doc([entry()]));
    await loader.start();
    assert.equal(loader.all.length, 1);
    assert.equal(loader.all[0]?.agentId, AGENT_ID);

    await writeAtomic(file, doc([entry({ agentId: "agent-next", sensitiveCheck: true, patterns: DEFAULT_SENSITIVE_PATTERNS })]));
    await until(() => loader.all[0]?.agentId === "agent-next");
    assert.equal(loader.all[0]?.sensitiveCheck, true);
    assert.deepEqual(loader.all[0]?.patterns, DEFAULT_SENSITIVE_PATTERNS);
    assert.ok(captured.logs.some((line) => line.includes("已载入 1 条契约 target")));
  } finally {
    await loader.stop();
    await rm(dir, { recursive: true, force: true });
  }
});

test("loader：patterns 缺省为空数组（schema default）", () => {
  const parsed = paseoImTargetsV1.parse({ version: CONTRACT_VERSION, targets: [{ id: "x", agentId: "a", sensitiveCheck: false }] });
  assert.deepEqual(parsed.targets[0]?.patterns, []);
});

test("loader：version 非 bots-contract/v1 忽略（空注册表 + breadcrumb）", async () => {
  const { dir, file } = await tempContractDir();
  const captured = captureLogs();
  const loader = new BotsContractLoader({ file, log: captured.log });
  try {
    await writeFile(file, JSON.stringify({ version: "bots-contract/v2", targets: [entry()] }), "utf8");
    await loader.start();
    assert.equal(loader.all.length, 0);
    assert.ok(captured.logs.some((line) => line.startsWith("warn:") && line.includes("已忽略")));
  } finally {
    await loader.stop();
    await rm(dir, { recursive: true, force: true });
  }
});

test("loader：坏 JSON 容错为空注册表，修复后热载恢复", async () => {
  const { dir, file } = await tempContractDir();
  const captured = captureLogs();
  const loader = new BotsContractLoader({ file, log: captured.log, debounceMs: 25 });
  try {
    await writeFile(file, "{ not json", "utf8");
    await loader.start();
    assert.equal(loader.all.length, 0);
    assert.ok(captured.logs.some((line) => line.includes("已忽略")));

    await writeAtomic(file, doc([entry()]));
    await until(() => loader.all.length === 1);
    assert.equal(loader.all[0]?.agentId, AGENT_ID);
  } finally {
    await loader.stop();
    await rm(dir, { recursive: true, force: true });
  }
});

test("loader：watch 失败退固定轮询，轮询仍能热载", async () => {
  const { dir, file } = await tempContractDir();
  const captured = captureLogs();
  const loader = new BotsContractLoader({
    file,
    log: captured.log,
    debounceMs: 25,
    pollMs: 50,
    watchImpl: () => {
      throw new Error("watch unsupported");
    },
  });
  try {
    await loader.start();
    assert.ok(captured.logs.some((line) => line.includes("轮询")));
    await writeAtomic(file, doc([entry()]));
    await until(() => loader.all.length === 1);
  } finally {
    await loader.stop();
    await rm(dir, { recursive: true, force: true });
  }
});

test("loader：锁文件防重复热载实例；陈旧锁接管；让位者不动他人锁", async () => {
  const { dir, file } = await tempContractDir();
  const lockPath = join(dir, ".paseo-im-loader.lock");
  const first = new BotsContractLoader({ file, log: captureLogs().log, debounceMs: 25 });
  try {
    // 1) 空闲目录：持锁 + 锁内容为本进程 pid
    await first.start();
    assert.equal(first.holdsLock, true);
    assert.equal(Number.parseInt((await readFile(lockPath, "utf8")).trim(), 10), process.pid);
    await first.stop();
    assert.ok(
      await readFile(lockPath, "utf8").then(
        () => false,
        () => true,
      ),
    );

    // 2) 存活外部进程持锁 → 本实例让位（仅静态加载），停止时不删他人锁
    const holder = spawn(process.execPath, ["-e", "setInterval(() => {}, 1e6)"]);
    try {
      await writeFile(lockPath, `${holder.pid}\n`, "utf8");
      const passiveLogs = captureLogs();
      const passive = new BotsContractLoader({ file, log: passiveLogs.log, debounceMs: 25 });
      try {
        await passive.start();
        assert.equal(passive.holdsLock, false);
        assert.ok(passiveLogs.logs.some((line) => line.includes("仅静态加载")));
        assert.equal(passive.all.length, 0);
      } finally {
        await passive.stop();
      }
      assert.ok(await readFile(lockPath, "utf8").then(() => true, () => false));
    } finally {
      holder.kill();
      await once(holder, "exit");
    }

    // 3) 陈旧锁（持有进程已退出）→ 接管
    await writeFile(lockPath, "999999999\n", "utf8");
    const staleLogs = captureLogs();
    const stale = new BotsContractLoader({ file, log: staleLogs.log, debounceMs: 25 });
    try {
      await stale.start();
      assert.equal(stale.holdsLock, true);
      assert.ok(!staleLogs.logs.some((line) => line.includes("仅静态加载")));
    } finally {
      await stale.stop();
    }
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});

// ------------------------------------------------- 路由：契约条目 → agent target

function connection(target: Target | null): Connection {
  return connectionSchema.parse({
    id: CONNECTION_ID,
    channel: "telegram",
    name: "契约测试",
    enabled: true,
    target,
    policy: emptyPolicy(),
    bindings: {},
    revision: 0,
    username: "paseo_bot",
    botUserId: "1",
  }) as Connection;
}

const contractEntry = (): PaseoImTargetEntry[] => [entry({ agentId: AGENT_ID, sensitiveCheck: false })];

test("resolveTarget：显式绑定 > 默认去向 > 契约回退", () => {
  const targets = contractEntry();
  const bound = connection(null);
  bound.bindings = { "dm:100": { kind: "agent", agentId: "bound-agent", sharedHistory: false } };
  const viaBinding = resolveTarget(bound, "dm:100", targets);
  assert.ok(viaBinding?.kind === "agent");
  assert.equal(viaBinding.agentId, "bound-agent");

  const defaulted = connection({ kind: "agent", agentId: "default-agent", sharedHistory: false });
  const viaDefault = resolveTarget(defaulted, "dm:100", targets);
  assert.ok(viaDefault?.kind === "agent");
  assert.equal(viaDefault.agentId, "default-agent");

  const none = connection(null);
  const fallback = resolveTarget(none, "dm:100", targets);
  assert.deepEqual(fallback, { kind: "agent", agentId: AGENT_ID, sharedHistory: true });

  assert.equal(resolveTarget(none, "dm:100", []), null);
});

interface TestHarness {
  bridge: Bridge;
  queue: AgentQueue;
  state: StateStore;
  deferred: DeferredRegistry;
  sent: (OutboundText & { messageId?: string })[];
  agentCalls: { send: { text: string; options?: unknown }[]; respond: unknown[] };
  logs: string[];
}

async function harness(options: {
  target?: Target | null;
  contractTargets?: PaseoImTargetEntry[];
  waitResult?: () => unknown;
}): Promise<TestHarness> {
  const root = await mkdtemp(join(tmpdir(), "paseo-im-contract-bridge-"));
  const sent: (OutboundText & { messageId?: string })[] = [];
  const agentCalls = { send: [] as { text: string; options?: unknown }[], respond: [] as unknown[] };
  const logs: string[] = [];
  const state = new StateStore(CONNECTION_ID, root);
  await state.load();
  const queue = new AgentQueue();
  const deferred = new DeferredRegistry({ sweep: async () => false, intervalMs: 60_000 });
  const config = connection(options.target === undefined ? { kind: "agent", agentId: AGENT_ID, sharedHistory: false } : options.target);
  const contractTargets = options.contractTargets ?? [];
  const adapter: ChannelAdapter = {
    channel: "telegram",
    capabilities: {
      progress: "none",
      markdown: "plain",
      uploadImage: false,
      uploadFile: false,
      reactions: false,
      typing: false,
      maxChunkChars: 4000,
      threads: false,
    },
    async sendText(outbound: OutboundText): Promise<SendResult> {
      const messageId = `out-${sent.length + 1}`;
      sent.push({ ...outbound, messageId });
      return { messageId };
    },
    async stop() {},
    async start() {},
  };
  const handle = {
    id: AGENT_ID,
    async refresh() {
      return {
        agent: {
          id: AGENT_ID,
          status: "idle",
          archivedAt: null,
          activeTurn: false,
          workspaceId: "ws-1",
          cwd: "/tmp",
          pendingPermissions: null,
        },
      };
    },
    async send(text: string, sendOptions?: unknown) {
      agentCalls.send.push({ text, options: sendOptions });
    },
    waitForFinish: async () =>
      options.waitResult?.() ?? {
        status: "idle",
        final: null,
        error: null,
        lastMessage: "模型回答",
      },
    async respondToPermission(args: unknown) {
      agentCalls.respond.push(args);
    },
    timeline: {
      refetch: async () => ({ epoch: "e1", entries: [], error: null }),
    },
  };
  const api = { agents: { ref: (_id: string) => handle } } as unknown as PaseoApi;
  const deps: BridgeDeps = {
    connectionId: CONNECTION_ID,
    config: () => config,
    adapter: () => adapter,
    state,
    queue,
    daemon: () => Promise.resolve(api),
    settings: async () => imSettings.schema.parse({}),
    deferred: { track: (id) => deferred.track(id), resolve: (id) => deferred.resolve(id) },
    signal: new AbortController().signal,
    log: (level, message) => logs.push(`${level}: ${message}`),
    contractTargets: () => contractTargets,
  };
  const bridge = new Bridge(deps, new Router(deps.daemon, undefined, () => contractTargets));
  return { bridge, queue, state, deferred, sent, agentCalls, logs };
}

function dmMessage(overrides: Partial<NormalizedMessage> = {}): NormalizedMessage {
  return {
    channel: "telegram",
    connectionId: CONNECTION_ID,
    kind: "dm",
    chatId: "100",
    senderId: "user-1",
    messageId: "m1",
    timestamp: Date.now(),
    text: "帮我看看构建日志",
    trigger: { mention: false, replyToBot: false, command: false },
    ...overrides,
  };
}

test("契约 agent 绑定路由：无显式去向时入站消息路由到契约条目", async () => {
  const h = await harness({ target: null, contractTargets: contractEntry() });
  try {
    await h.bridge.handle(dmMessage());
    await h.queue.idle();
    assert.equal(h.agentCalls.send.length, 1);
    assert.ok(h.agentCalls.send[0]!.text.includes("帮我看看构建日志"));
    assert.ok(h.sent.some((out) => out.text === "模型回答"));
    const job = h.state.job("m1");
    assert.equal(job?.delivery, "done");
    assert.equal(job?.agentId, AGENT_ID);
  } finally {
    h.deferred.stop();
  }
});

// ------------------------------------------------------- F23 发送侧敏感检查

test("F23 命中：出站回复整条拦截不发送 + breadcrumb + 计数", async () => {
  const secret = "密钥是 sk-abcdefghijklmnopqrst1234";
  const h = await harness({
    target: { kind: "agent", agentId: AGENT_ID, sharedHistory: false },
    contractTargets: [entry({ sensitiveCheck: true, patterns: ["sk-[A-Za-z0-9]{20,}"] })],
    waitResult: () => ({ status: "idle", final: null, error: null, lastMessage: secret }),
  });
  try {
    await h.bridge.handle(dmMessage());
    await h.queue.idle();
    assert.equal(h.sent.filter((out) => out.text.includes("sk-")).length, 0);
    assert.equal(h.sent.length, 0);
    const job = h.state.job("m1");
    assert.equal(job?.delivery, "done");
    assert.equal(job?.answer, "");
    assert.equal(h.bridge.blockedOutboundCount, 1);
    assert.ok(
      h.logs.some((line) => line.startsWith("warn:") && line.includes("F23") && line.includes("sk-[A-Za-z0-9]{20,}")),
      `logs=${JSON.stringify(h.logs)}`,
    );
  } finally {
    h.deferred.stop();
  }
});

test("F23 未命中：正常放行且不计数", async () => {
  const h = await harness({
    target: { kind: "agent", agentId: AGENT_ID, sharedHistory: false },
    contractTargets: [entry({ sensitiveCheck: true, patterns: DEFAULT_SENSITIVE_PATTERNS })],
    waitResult: () => ({ status: "idle", final: null, error: null, lastMessage: "构建日志已修复，见详情。" }),
  });
  try {
    await h.bridge.handle(dmMessage());
    await h.queue.idle();
    assert.ok(h.sent.some((out) => out.text.includes("构建日志已修复")));
    assert.equal(h.bridge.blockedOutboundCount, 0);
  } finally {
    h.deferred.stop();
  }
});

test("F23 仅约束 sensitiveCheck=true 的契约条目；无契约注册表不做检查", async () => {
  const secret = "内网地址 10.1.2.3 请查看";
  const off = await harness({
    target: { kind: "agent", agentId: AGENT_ID, sharedHistory: false },
    contractTargets: [entry({ sensitiveCheck: false, patterns: DEFAULT_SENSITIVE_PATTERNS })],
    waitResult: () => ({ status: "idle", final: null, error: null, lastMessage: secret }),
  });
  try {
    await off.bridge.handle(dmMessage());
    await off.queue.idle();
    assert.ok(off.sent.some((out) => out.text.includes("10.1.2.3")));
    assert.equal(off.bridge.blockedOutboundCount, 0);
  } finally {
    off.deferred.stop();
  }

  const noContract = await harness({
    target: { kind: "agent", agentId: AGENT_ID, sharedHistory: false },
    contractTargets: [],
    waitResult: () => ({ status: "idle", final: null, error: null, lastMessage: secret }),
  });
  try {
    await noContract.bridge.handle(dmMessage());
    await noContract.queue.idle();
    assert.ok(noContract.sent.some((out) => out.text.includes("10.1.2.3")));
    assert.equal(noContract.bridge.blockedOutboundCount, 0);
  } finally {
    noContract.deferred.stop();
  }
});

test("F23 超时补发路径同样整条拦截", async () => {
  let waitResult: unknown = { status: "idle", final: null, error: null, lastMessage: "模型回答" };
  const h = await harness({
    target: { kind: "agent", agentId: AGENT_ID, sharedHistory: false },
    contractTargets: [entry({ sensitiveCheck: true, patterns: DEFAULT_SENSITIVE_PATTERNS })],
    waitResult: () => waitResult,
  });
  try {
    // 先走一次正常投递，再把任务回拨成「超时待补发」并换成敏感回复，走
    // resumeDeferred → deliverAnswer。
    await h.bridge.handle(dmMessage());
    await h.queue.idle();
    assert.ok(h.sent.some((out) => out.text === "模型回答"));
    assert.equal(h.state.job("m1")?.delivery, "done");
    waitResult = { status: "idle", final: null, error: null, lastMessage: "token ghp_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" };
    await h.state.mutate((draft) => {
      const job = draft.jobs["m1"];
      if (job) {
        job.delivery = null;
        job.dispatch = "sent";
        job.answer = null;
      }
    });
    assert.equal(await h.bridge.resumeDeferred("m1"), true);
    assert.equal(h.sent.some((out) => out.text.includes("ghp_")), false);
    assert.equal(h.bridge.blockedOutboundCount, 1);
  } finally {
    h.deferred.stop();
  }
});
