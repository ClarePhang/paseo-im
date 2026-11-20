import assert from "node:assert/strict";
import { test } from "node:test";
import { mkdtemp, mkdir, rm, readFile, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import type { PaseoApi } from "@getpaseo/client";
import { Bridge, DeferredTimeoutError, type BridgeDeps } from "../server/bridge.ts";
import { Router } from "../server/routing.ts";
import { AgentQueue } from "../server/queue.ts";
import { StateStore } from "../server/state-store.ts";
import { DeferredRegistry } from "../server/deferred.ts";
import { emptyPolicy, connectionSchema, type Connection, type Policy, type Target } from "../shared/config.ts";
import { imSettings } from "../shared/settings.ts";
import type { AdapterContext, ChannelAdapter } from "../server/channels/types.ts";
import { decisionsPath, outboxPath, readWatermark } from "../server/bots-plus-outbox.ts";
import { applyOwnerPairing, OwnerPairingBook } from "../server/owner-pairing.ts";
import type { NormalizedMessage, OutboundText, SendResult } from "../shared/model.ts";

const CONNECTION_ID = "00000000-0000-4000-8000-000000000001";
const AGENT_ID = "agent-1";

interface TestHarness {
  bridge: Bridge;
  queue: AgentQueue;
  state: StateStore;
  deferred: DeferredRegistry;
  sent: (OutboundText & { messageId?: string })[];
  edits: { messageId: string; chatId: string; text: string }[];
  agentCalls: { send: { text: string; options?: unknown }[]; respond: unknown[] };
  deps: BridgeDeps;
  waitResults: unknown[];
  logs: string[];
}

function makeAdapter(
  sent: (OutboundText & { messageId?: string })[],
  noReceipt = false,
  edits?: { messageId: string; chatId: string; text: string }[],
  failEdit = false,
): ChannelAdapter {
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
      if (noReceipt) {
        // wecom-app 类渠道：sendText 无平台回执 id。
        sent.push({ ...outbound });
        return {};
      }
      const messageId = `out-${sent.length + 1}`;
      sent.push({ ...outbound, messageId });
      return { messageId };
    },
    async stop() {},
    async start() {},
  };
  if (edits) {
    adapter.editMessage = async (messageId: string, chatId: string, text: string): Promise<void> => {
      if (failEdit) throw new Error("edit 失败（测试注入）");
      edits.push({ messageId, chatId, text });
    };
  }
  return adapter;
}

function makeApi(agentCalls: { send: { text: string; options?: unknown }[]; respond: unknown[] }, timelineEntries: unknown[] = []) {
  let refetchCount = 0;
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
          pendingPermissions: pendingPermission(),
        },
      };
    },
    async send(text: string, options?: unknown) {
      agentCalls.send.push({ text, options });
    },
    waitForFinish: async () => nextWaitResult(),
    async respondToPermission(args: unknown) {
      agentCalls.respond.push(args);
    },
    timeline: {
      refetch: async () => {
        refetchCount += 1;
        const entries =
          timelineEntries.length === 0
            ? []
            : timelineEntries.map((entry, index) => {
                const base = entry as { seqEnd?: number; item?: unknown };
                return index === 0 ? { ...base, seqEnd: 5 + refetchCount } : entry;
              });
        return { epoch: "e1", entries, error: null };
      },
    },
  };
  const api = {
    agents: { ref: (_id: string) => handle },
  } as unknown as PaseoApi;
  return api;
}

let pendingPermission: () => unknown = () => null;
let nextWaitResult: () => unknown = () => ({
  status: "idle",
  final: null,
  error: null,
  lastMessage: "模型回答",
});

function connection(target: Target | null): Connection {
  return connectionSchema.parse({
    id: CONNECTION_ID,
    channel: "telegram",
    name: "测试",
    enabled: true,
    target,
    policy: emptyPolicy(),
    bindings: {},
    revision: 0,
    username: "paseo_bot",
    botUserId: "1",
  }) as Connection;
}

async function harness(options: {
  target?: Target | null;
  policyOperators?: string[];
  policyOverrides?: Partial<Policy>;
  timelineEntries?: unknown[];
  noReceipt?: boolean;
  editable?: boolean;
  failEdit?: boolean;
  pairingBook?: OwnerPairingBook;
}): Promise<TestHarness> {
  const root = await mkdtemp(join(tmpdir(), "paseo-im-bridge-"));
  const sent: (OutboundText & { messageId?: string })[] = [];
  const edits: { messageId: string; chatId: string; text: string }[] = [];
  const agentCalls = { send: [] as { text: string; options?: unknown }[], respond: [] as unknown[] };
  const logs: string[] = [];
  const state = new StateStore(CONNECTION_ID, root);
  await state.load();
  const queue = new AgentQueue();
  const deferred = new DeferredRegistry({ sweep: async () => false, intervalMs: 60_000 });
  const config = connection(options.target === undefined ? { kind: "agent", agentId: AGENT_ID, sharedHistory: false } : options.target);
  if (options.policyOperators || options.policyOverrides) {
    config.policy = {
      ...config.policy,
      ...(options.policyOperators ? { operators: options.policyOperators } : {}),
      ...(options.policyOverrides ?? {}),
    };
  }
  const deps: BridgeDeps = {
    connectionId: CONNECTION_ID,
    config: () => config,
    adapter: () => makeAdapter(sent, options.noReceipt === true, options.editable === true ? edits : undefined, options.failEdit === true),
    state,
    queue,
    daemon: () => Promise.resolve(makeApi(agentCalls, options.timelineEntries ?? [])),
    settings: async () => imSettings.schema.parse({}),
    deferred: { track: (id) => deferred.track(id), resolve: (id) => deferred.resolve(id) },
    signal: new AbortController().signal,
    log: (level, message) => logs.push(`${level}: ${message}`),
    tryOwnerPairing: options.pairingBook
      ? (senderId, chatId, code) =>
          applyOwnerPairing({
            book: options.pairingBook!,
            state,
            connectionId: CONNECTION_ID,
            senderId,
            chatId,
            code,
          })
      : undefined,
  };
  const bridge = new Bridge(deps, new Router(deps.daemon));
  return { bridge, queue, state, deferred, sent, edits, agentCalls, deps, waitResults: [], logs };
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

test("私聊消息全链路：派发→等待→分段回复", async () => {
  const h = await harness({});
  await h.bridge.handle(dmMessage());
  await h.queue.idle();
  assert.equal(h.agentCalls.send.length, 1);
  assert.ok(h.agentCalls.send[0]!.text.includes("帮我看看构建日志"));
  assert.ok(
    h.sent.some((out) => out.text === "模型回答"),
    `sent=${JSON.stringify(h.sent.map((out) => out.text))} logs=${JSON.stringify(h.logs)} job=${JSON.stringify(h.state.job("m1"))}`,
  );
  const job = h.state.job("m1");
  assert.equal(job?.delivery, "done");
  assert.equal(job?.agentId, AGENT_ID);
  h.deferred.stop();
});

test("同 messageId 幂等去重", async () => {
  const h = await harness({});
  await h.bridge.handle(dmMessage());
  await h.queue.idle();
  await h.bridge.handle(dmMessage());
  await h.queue.idle();
  assert.equal(h.agentCalls.send.length, 1);
  h.deferred.stop();
});

test("未授权私聊不派发", async () => {
  const h = await harness({
    policyOperators: ["someone-else"],
    policyOverrides: { directMessages: "operators" },
  });
  await h.bridge.handle(dmMessage());
  await h.queue.idle();
  assert.equal(h.agentCalls.send.length, 0);
  assert.ok(h.sent.length === 0);
  h.deferred.stop();
});

test("未配置去向提示可读错误", async () => {
  const h = await harness({ target: null });
  await h.bridge.handle(dmMessage());
  await h.queue.idle();
  assert.ok(
    h.sent.some((out) => out.text.includes("尚未配置消息去向")),
    `sent=${JSON.stringify(h.sent.map((out) => out.text))} logs=${JSON.stringify(h.logs)}`,
  );
  const job = h.state.job("m1");
  assert.ok(job?.failure);
  h.deferred.stop();
});

test(
  "异步提问：通知→回复→代答",
  { timeout: 20_000 },
  async () => {
  const questionRequest = {
    id: "req-1",
    kind: "question",
    name: "request_user_input_async",
    input: { questions: [{ header: "选择", question: "用哪个方案？", options: [{ label: "方案 A" }, { label: "方案 B" }] }] },
  };
  let calls = 0;
  pendingPermission = () => [questionRequest];
  nextWaitResult = () => {
    calls += 1;
    if (calls === 1) {
      return { status: "permission", final: { pendingPermissions: [questionRequest], status: "running" }, error: null, lastMessage: null };
    }
    if (calls === 2) {
      return { status: "permission", final: { pendingPermissions: [questionRequest], status: "running" }, error: null, lastMessage: null };
    }
    return { status: "idle", final: null, error: null, lastMessage: "已按方案 A 继续" };
  };
  // The question reply path reads the timeline for the follow-up turn boundary.
  const timelineEntries = [{ seqEnd: 5, item: { type: "user_message", messageId: "later" } }];
  const h = await harness({ timelineEntries });
  await h.bridge.handle(dmMessage());
  await new Promise<void>((resolve) => {
    queueMicrotask(() => resolve());
  });
  // waitForTask polls at 1s between async-question rounds; two rounds run.
  const pending = h.queue.idle();
  await pending;
  const questionMessage = h.sent.find((out) => out.text.includes("需要澄清"));
  assert.ok(questionMessage, "应向 IM 通知问题");
  assert.equal(questionMessage?.messageId, "out-1");

  await h.bridge.handle(
    dmMessage({
      messageId: "reply-1",
      text: "1",
      replyTo: { id: "out-1" },
    }),
  );
  await h.queue.idle();
  const respond = h.agentCalls.respond[0] as { requestId?: string; response?: { updatedInput?: { answers?: Record<string, string> } } } | undefined;
  assert.ok(
    respond,
    `respond=${JSON.stringify(h.agentCalls.respond)} sent=${JSON.stringify(h.sent.map((out) => ({ text: out.text, id: out.messageId })))} jobs=${JSON.stringify(Object.values(h.state.get().jobs))} logs=${JSON.stringify(h.logs)}`,
  );
  assert.equal(respond?.requestId, "req-1");
  assert.deepEqual(respond?.response?.updatedInput?.answers, { 选择: "方案 A" });
  const followUp = h.sent.find((out) => out.text.includes("已按方案 A 继续"));
  assert.ok(followUp, "应把后续回合的最终回答送达 IM");
  const sourceJob = h.state.job("m1");
  assert.equal(sourceJob?.delivery, "done");
  pendingPermission = () => null;
  nextWaitResult = () => ({ status: "idle", final: null, error: null, lastMessage: "模型回答" });
  h.deferred.stop();
  },
);

test("等待超时进入补发登记", async (t) => {
  t.mock.timers.enable({ apis: ["setTimeout", "setInterval"] });
  nextWaitResult = () => Promise.withResolvers<unknown>().promise;
  const h = await harness({});
  await h.bridge.handle(dmMessage());
  // Flush microtasks so the delivery (and its timeout timer) has started.
  await new Promise<void>((resolve) => {
    setImmediate(() => resolve());
  });
  t.mock.timers.tick(31 * 60_000);
  await h.queue.idle();
  const job = h.state.job("m1");
  assert.ok(job?.deadlineAt, "应登记补发时限");
  assert.equal(h.deferred.count(), 1);
  assert.ok(h.sent.some((out) => out.text.includes("等待模型回复超时")));
  nextWaitResult = () => ({ status: "idle", final: null, error: null, lastMessage: "模型回答" });
  h.deferred.stop();
});

test("DeferredTimeoutError 可识别", () => {
  const error = new DeferredTimeoutError();
  assert.ok(error instanceof DeferredTimeoutError);
});

test(
  "异步提问兜底：无回执 id 渠道回复引用按唯一未答问题代答",
  { timeout: 20_000 },
  async () => {
    const questionRequest = {
      id: "req-1",
      kind: "question",
      name: "request_user_input_async",
      input: { questions: [{ header: "选择", question: "用哪个方案？", options: [{ label: "方案 A" }, { label: "方案 B" }] }] },
    };
    let calls = 0;
    pendingPermission = () => [questionRequest];
    nextWaitResult = () => {
      calls += 1;
      if (calls <= 2) {
        return { status: "permission", final: { pendingPermissions: [questionRequest], status: "running" }, error: null, lastMessage: null };
      }
      return { status: "idle", final: null, error: null, lastMessage: "已按方案 A 继续" };
    };
    const timelineEntries = [{ seqEnd: 5, item: { type: "user_message", messageId: "later" } }];
    // noReceipt：sendText 不返回平台 id → question.messageId 为 null。
    const h = await harness({ timelineEntries, noReceipt: true });
    await h.bridge.handle(dmMessage());
    await h.queue.idle();
    const questionMessage = h.sent.find((out) => out.text.includes("需要澄清"));
    assert.ok(questionMessage, "应向 IM 通知问题");
    assert.equal(questionMessage?.messageId, undefined, "该渠道无平台回执 id");

    // 用户引用回复卡片：平台侧引用 id 不在 jobs 表内 → 直接匹配未命中 → 兜底。
    await h.bridge.handle(
      dmMessage({
        messageId: "reply-nr-1",
        text: "1",
        replyTo: { id: "platform-quote-id-not-in-jobs" },
      }),
    );
    await h.queue.idle();
    const respond = h.agentCalls.respond[0] as { requestId?: string; response?: { updatedInput?: { answers?: Record<string, string> } } } | undefined;
    assert.ok(respond, `兜底应代答唯一未答问题 respond=${JSON.stringify(h.agentCalls.respond)} jobs=${JSON.stringify(Object.values(h.state.get().jobs))}`);
    assert.equal(respond?.requestId, "req-1");
    assert.deepEqual(respond?.response?.updatedInput?.answers, { 选择: "方案 A" });
    pendingPermission = () => null;
    nextWaitResult = () => ({ status: "idle", final: null, error: null, lastMessage: "模型回答" });
    h.deferred.stop();
  },
);

test("异步提问兜底：多于一个未答问题时不猜，按新任务处理", async () => {
  const h = await harness({});
  // 直接放置两个未答问题 job，破坏“同连接唯一未答问题”前置条件。
  h.state.ensureJob("q-job-1", {
    messageId: "q-job-1",
    conversationKey: "dm:100",
    chatId: "100",
    senderId: "user-1",
    createdAt: Date.now() - 1_000,
    agentId: AGENT_ID,
    question: {
      requestId: "req-q1",
      fields: [{ header: "选择", text: "用哪个方案？", options: ["方案 A", "方案 B"] }],
      messageId: null,
      notified: true,
      status: "pending",
    },
  });
  h.state.ensureJob("q-job-2", {
    messageId: "q-job-2",
    conversationKey: "dm:100",
    chatId: "100",
    senderId: "user-1",
    createdAt: Date.now(),
    agentId: AGENT_ID,
    question: {
      requestId: "req-q2",
      fields: [{ header: "选择", text: "用哪个方案？", options: ["方案 A", "方案 B"] }],
      messageId: null,
      notified: true,
      status: "pending",
    },
  });

  await h.bridge.handle(
    dmMessage({
      messageId: "reply-multi-1",
      text: "1",
      replyTo: { id: "platform-quote-id-not-in-jobs" },
    }),
  );
  await h.queue.idle();
  assert.equal(h.agentCalls.respond.length, 0, "多个未答问题不得兜底");
  const job = h.state.job("reply-multi-1");
  assert.ok(job);
  assert.equal(job.questionReply, null, "未命中兜底，按新任务建 job");
  h.deferred.stop();
});

// ── P2b 收尾：outbox decision-receipt 编辑原审批通知 ──

const outboxEntry = (seq: number, overrides: Record<string, unknown> = {}): Record<string, unknown> => ({
  seq,
  kind: "approval-notify",
  botId: "b1",
  botName: "Probe",
  ref: `card-${seq}`,
  title: "审批：memory-write",
  agentId: null,
  routineId: null,
  failures: null,
  expiresAt: null,
  ts: "2026-11-13T00:00:00.000Z",
  ...overrides,
});

async function seedOutboxFile(entries: Array<Record<string, unknown>>): Promise<string> {
  const home = await mkdtemp(join(tmpdir(), "paseo-im-outbox-"));
  const file = outboxPath(home);
  await mkdir(dirname(file), { recursive: true });
  await writeFile(
    file,
    JSON.stringify({ version: "bots-plus-outbox/v1", seq: Math.max(0, ...entries.map((e) => e.seq as number)), entries }),
    "utf8",
  );
  return home;
}

/** pollOutbox 经 readNewEntries/readWatermark 走默认 PASEO_HOME：测试窗口内指向临时目录，结束后还原。 */
async function withOutboxHome<T>(home: string, run: () => Promise<T>): Promise<T> {
  const prev = process.env.PASEO_HOME;
  process.env.PASEO_HOME = home;
  try {
    return await run();
  } finally {
    if (prev === undefined) delete process.env.PASEO_HOME;
    else process.env.PASEO_HOME = prev;
  }
}

async function pairOwner(h: TestHarness): Promise<void> {
  await h.state.mutate((s) => {
    s.ownerPairing = { hash: "a".repeat(64), expiresAt: Date.now() + 60_000, ownerId: "owner-1", ownerChatId: "chat-1" };
  });
}

test("pollOutbox：decision-receipt 命中 ref→messageId 编辑原审批通知，不发新消息", async () => {
  const h = await harness({ editable: true });
  const home = await seedOutboxFile([
    outboxEntry(1),
    outboxEntry(2, { kind: "decision-receipt", ref: "card-1", title: "批准 card-1（已执行）" }),
  ]);
  await withOutboxHome(home, async () => {
    await pairOwner(h);
    await h.bridge.pollOutbox();
  });
  assert.deepEqual(h.sent.map((out) => out.text), ["[bots+] 审批等待：Probe · 审批：memory-write · ref card-1"], "回执不得发新消息");
  assert.deepEqual(h.edits, [{ messageId: "out-1", chatId: "chat-1", text: "[bots+] ✅ 批准 card-1（已执行）" }]);
  assert.equal(await readWatermark(home), 2, "回执消费后 watermark 推进");
  await rm(home, { recursive: true, force: true });
  h.deferred.stop();
});

test("pollOutbox：adapter 无编辑面 → 回执回退发新消息（现行为）", async () => {
  const h = await harness({});
  const home = await seedOutboxFile([
    outboxEntry(1),
    outboxEntry(2, { kind: "decision-receipt", ref: "card-1", title: "批准 card-1（已执行）" }),
  ]);
  await withOutboxHome(home, async () => {
    await pairOwner(h);
    await h.bridge.pollOutbox();
  });
  assert.deepEqual(h.sent.map((out) => out.text), [
    "[bots+] 审批等待：Probe · 审批：memory-write · ref card-1",
    "[bots+] 批准 card-1（已执行）",
  ]);
  assert.deepEqual(h.edits, []);
  assert.equal(await readWatermark(home), 2);
  await rm(home, { recursive: true, force: true });
  h.deferred.stop();
});

test("pollOutbox：ref 无 map 命中（重启场景）或编辑失败 → 回执回退发新消息", async () => {
  // 场景 A：可编辑 adapter，但 map 无 ref（approval-notify 未经过本进程投递）。
  const a = await harness({ editable: true });
  const homeA = await seedOutboxFile([
    outboxEntry(1, { kind: "decision-receipt", ref: "card-1", title: "拒绝 card-1（已执行）" }),
  ]);
  await withOutboxHome(homeA, async () => {
    await pairOwner(a);
    await a.bridge.pollOutbox();
  });
  assert.deepEqual(a.sent.map((out) => out.text), ["[bots+] 拒绝 card-1（已执行）"], "无 map 命中回退发新消息");
  assert.deepEqual(a.edits, []);
  assert.equal(await readWatermark(homeA), 1);
  await rm(homeA, { recursive: true, force: true });
  a.deferred.stop();

  // 场景 B：map 命中但编辑抛错 → 降级 breadcrumb + 回退发新消息，watermark 仍推进。
  const b = await harness({ editable: true, failEdit: true });
  const homeB = await seedOutboxFile([
    outboxEntry(1),
    outboxEntry(2, { kind: "decision-receipt", ref: "card-1", title: "批准 card-1（已执行）" }),
  ]);
  await withOutboxHome(homeB, async () => {
    await pairOwner(b);
    await b.bridge.pollOutbox();
  });
  assert.deepEqual(b.sent.map((out) => out.text), [
    "[bots+] 审批等待：Probe · 审批：memory-write · ref card-1",
    "[bots+] 批准 card-1（已执行）",
  ]);
  assert.deepEqual(b.edits, []);
  assert.ok(b.logs.some((line) => line.includes("回执编辑失败")), `应记录编辑失败降级：${JSON.stringify(b.logs)}`);
  assert.equal(await readWatermark(homeB), 2);
  await rm(homeB, { recursive: true, force: true });
  b.deferred.stop();
});

test("pollOutbox：permission-notify 投递短 TTL 审批文案并登记 ref→messageId；回执编辑原消息", async () => {
  const h = await harness({ editable: true });
  const home = await seedOutboxFile([
    outboxEntry(1, { kind: "permission-notify", ref: "perm-req-1", title: "Allow tool: Bash", agentId: "agent-1" }),
    outboxEntry(2, { kind: "decision-receipt", ref: "perm-req-1", title: "批准 perm-req-1（已执行）" }),
  ]);
  await withOutboxHome(home, async () => {
    await pairOwner(h);
    await h.bridge.pollOutbox();
  });
  assert.deepEqual(
    h.sent.map((out) => out.text),
    ["[bots+] 审批等待：Probe · Allow tool: Bash · 5分钟内回复 批 perm-req-1 或 拒 perm-req-1"],
    "回执不得发新消息（permission ref 同走编辑联动）",
  );
  assert.deepEqual(h.edits, [{ messageId: "out-1", chatId: "chat-1", text: "[bots+] ✅ 批准 perm-req-1（已执行）" }]);
  assert.equal(await readWatermark(home), 2);
  await rm(home, { recursive: true, force: true });
  h.deferred.stop();
});

test("owner 私聊审批命令（requestId 形态）：写 decisions 回执不派发；非 owner 同形文本走正常流程", async () => {
  const h = await harness({});
  const home = await mkdtemp(join(tmpdir(), "paseo-im-decisions-"));
  await withOutboxHome(home, async () => {
    await pairOwner(h);
    await h.bridge.handle(dmMessage({ messageId: "perm-cmd-1", chatId: "chat-1", senderId: "owner-1", text: "批 perm-req-1" }));
    const doc = JSON.parse(await readFile(decisionsPath(home), "utf8")) as {
      decisions: Array<{ ref: string; behavior: string; channel: string; channelUser: string }>;
    };
    assert.equal(doc.decisions.length, 1);
    assert.equal(doc.decisions[0]!.ref, "perm-req-1");
    assert.equal(doc.decisions[0]!.behavior, "allow");
    assert.equal(doc.decisions[0]!.channel, "telegram");
    assert.equal(doc.decisions[0]!.channelUser.length, 16);
  });
  assert.equal(h.agentCalls.send.length, 0, "审批命令不得派发 agent");
  assert.deepEqual(h.sent.map((out) => out.text), ["已记录：批准 perm-req-1（bots+ 将在下一轮询执行）"]);
  await h.bridge.handle(dmMessage({ messageId: "perm-cmd-2", text: "批 perm-req-1" }));
  await h.queue.idle();
  assert.equal(h.agentCalls.send.length, 1, "非 owner 同形文本走正常流程派发");
  await rm(home, { recursive: true, force: true });
  h.deferred.stop();
});

test("owner 私聊审批命令（omp:permission 冒号形态）：parse→decisions 落盘 ref=原文（P1-M1 全链）", async () => {
  const h = await harness({});
  const home = await mkdtemp(join(tmpdir(), "paseo-im-decisions-"));
  await withOutboxHome(home, async () => {
    await pairOwner(h);
    await h.bridge.handle(dmMessage({ messageId: "perm-cmd-colon", chatId: "chat-1", senderId: "owner-1", text: "批 omp:permission:ns:42" }));
    const doc = JSON.parse(await readFile(decisionsPath(home), "utf8")) as {
      decisions: Array<{ ref: string; behavior: string; channel: string }>;
    };
    assert.equal(doc.decisions.length, 1);
    assert.equal(doc.decisions[0]!.ref, "omp:permission:ns:42", "ref 必须与 TG 原文逐字一致");
    assert.equal(doc.decisions[0]!.behavior, "allow");
    assert.equal(doc.decisions[0]!.channel, "telegram");
  });
  assert.equal(h.agentCalls.send.length, 0, "审批命令不得派发 agent");
  assert.deepEqual(h.sent.map((out) => out.text), ["已记录：批准 omp:permission:ns:42（bots+ 将在下一轮询执行）"]);
  await rm(home, { recursive: true, force: true });
  h.deferred.stop();
});

// ── P2c：owner 配对命令拦截（「配对 <64hex>」dm） ──

test("配对命令命中：写 ownerPairing + 回执，不派发 agent", async () => {
  const book = new OwnerPairingBook();
  const h = await harness({ pairingBook: book });
  const { attemptId, code } = book.start(CONNECTION_ID);
  await h.bridge.handle(dmMessage({ text: `配对 ${code}` }));
  assert.equal(h.agentCalls.send.length, 0, "配对命令不得派发 agent");
  assert.deepEqual(h.sent.map((out) => out.text), ["配对成功 ✅ 本渠道已绑定为 owner 通道"]);
  const pairing = h.state.get().ownerPairing;
  assert.equal(pairing?.ownerId, "user-1");
  assert.equal(pairing?.ownerChatId, "100");
  assert.match(pairing?.hash ?? "", /^[a-f0-9]{64}$/);
  assert.equal(pairing?.hash, book.attempt(attemptId)?.nonceHash, "落盘 hash=发码摘要");
  h.deferred.stop();
});

test("配对命令错码：回执无效，不写不消费账本；随后正确码仍可配对", async () => {
  const book = new OwnerPairingBook();
  const h = await harness({ pairingBook: book });
  const { attemptId, code } = book.start(CONNECTION_ID);
  await h.bridge.handle(dmMessage({ text: `配对 ${"ab".repeat(32)}` }));
  assert.deepEqual(h.sent.map((out) => out.text), ["配对码无效或已过期"]);
  assert.equal(h.state.get().ownerPairing, null);
  assert.equal(h.agentCalls.send.length, 0, "错码不得派发 agent");
  assert.deepEqual(book.poll(attemptId, h.state.get().ownerPairing), { status: "pending" }, "错码不得消费账本");
  await h.bridge.handle(dmMessage({ text: `配对 ${code}` }));
  assert.deepEqual(h.sent.map((out) => out.text), ["配对码无效或已过期", "配对成功 ✅ 本渠道已绑定为 owner 通道"]);
  h.deferred.stop();
});

test("无 pending 配对：同形文本不拦不回显，走正常流程派发", async () => {
  const h = await harness({ pairingBook: new OwnerPairingBook() });
  await h.bridge.handle(dmMessage({ text: `配对 ${"cd".repeat(32)}` }));
  await h.queue.idle();
  assert.deepEqual(h.sent.map((out) => out.text), ["模型回答"], "静默：无配对回执，正常答复");
  assert.equal(h.agentCalls.send.length, 1, "走正常流程派发");
  assert.ok(h.agentCalls.send[0]!.text.includes("cd".repeat(32)));
  const job = h.state.job("m1");
  assert.equal(job?.delivery, "done");
  h.deferred.stop();
});
