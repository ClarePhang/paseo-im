/** bots-plus-outbox 只读侧测试：新条目过滤/watermark/容错降级（P2b 第一步）。 */
import assert from "node:assert/strict";
import { test } from "node:test";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { appendDecision, botsPlusOutboxV1, DECISIONS_MAX_BYTES, decisionsPath, outboxPath, parseDecisionCommand, readNewEntries, readWatermark, writeWatermark } from "../server/bots-plus-outbox.ts";

const entry = (seq: number) => ({
  seq,
  kind: "approval-notify" as const,
  botId: "b1",
  botName: "Probe",
  ref: `card-${seq}`,
  title: "审批：memory-write",
  agentId: null,
  routineId: null,
  failures: null,
  expiresAt: null,
  ts: "2026-11-13T00:00:00.000Z",
});

async function tmpHome(): Promise<string> {
  return fs.mkdtemp(path.join(os.tmpdir(), "im-outbox-"));
}

async function seedOutbox(home: string, seqs: number[]): Promise<void> {
  const file = outboxPath(home);
  await fs.mkdir(path.dirname(file), { recursive: true });
  await fs.writeFile(file, JSON.stringify({ version: "bots-plus-outbox/v1", seq: Math.max(...seqs, 0), entries: seqs.map(entry) }), "utf8");
}

test("readNewEntries：seq 过滤（只回新于 watermark）+契约顺序", async () => {
  const home = await tmpHome();
  await seedOutbox(home, [1, 2, 3]);
  assert.deepEqual((await readNewEntries(0, home)).map((e) => e.seq), [1, 2, 3]);
  assert.deepEqual((await readNewEntries(2, home)).map((e) => e.seq), [3]);
  assert.deepEqual(await readNewEntries(3, home), []);
});

test("watermark：持久化往返；缺文件=0", async () => {
  const home = await tmpHome();
  assert.equal(await readWatermark(home), 0);
  await writeWatermark(7, home);
  assert.equal(await readWatermark(home), 7);
});

test("降级：缺文件/坏 JSON/坏版本 → 空数组不抛（不阻塞渠道环）", async () => {
  const home = await tmpHome();
  assert.deepEqual(await readNewEntries(0, home), []);
  const file = outboxPath(home);
  await fs.mkdir(path.dirname(file), { recursive: true });
  await fs.writeFile(file, "{broken", "utf8");
  assert.deepEqual(await readNewEntries(0, home), []);
  await fs.writeFile(file, JSON.stringify({ version: "bots-contract/v1", seq: 1, entries: [entry(1)] }), "utf8");
  assert.deepEqual(await readNewEntries(0, home), [], "坏版本字面量拒收");
  // 好文件往返：schema parse 正常
  await seedOutbox(home, [1]);
  assert.equal((botsPlusOutboxV1.parse(JSON.parse(await fs.readFile(file, "utf8").then((r) => r)))).entries.length, 1);
});

test("schema 同步：permission-notify 条目（含 agentId）往返；缺 agentId 可解析（nullish 兼容旧文件——P1-1 同步）", async () => {
  const home = await tmpHome();
  const file = outboxPath(home);
  await fs.mkdir(path.dirname(file), { recursive: true });
  const permEntry = { ...entry(1), kind: "permission-notify", ref: "perm-req-1", agentId: "agent-1", title: "Allow tool: Bash" };
  await fs.writeFile(file, JSON.stringify({ version: "bots-plus-outbox/v1", seq: 1, entries: [permEntry] }), "utf8");
  const fresh = await readNewEntries(0, home);
  assert.equal(fresh.length, 1);
  assert.equal(fresh[0]!.kind, "permission-notify");
  assert.equal(fresh[0]!.agentId, "agent-1");
  const missing = { ...permEntry } as Record<string, unknown>;
  delete missing.agentId;
  // P1-1（bots+ 侧重做同步）：agentId nullish——旧文件缺键可解析（拒读历史数据=上轮生产回归根因，禁回退）。
  const legacy = botsPlusOutboxV1.parse({ version: "bots-plus-outbox/v1", seq: 1, entries: [missing] });
  assert.equal(legacy.entries[0]!.agentId, undefined, "缺 agentId 键可解析（旧文件兼容）");
  const withNull = botsPlusOutboxV1.parse({ version: "bots-plus-outbox/v1", seq: 1, entries: [{ ...missing, agentId: null }] });
  assert.equal(withNull.entries[0]!.agentId, null, "agentId=null 亦可");
});

test("parseDecisionCommand：card 短码与 daemon requestId（含 omp:permission 冒号形态）均接受", () => {
  assert.deepEqual(parseDecisionCommand("批 card-1"), { behavior: "allow", ref: "card-1" });
  assert.deepEqual(parseDecisionCommand("拒 card-abc9"), { behavior: "deny", ref: "card-abc9" });
  assert.deepEqual(parseDecisionCommand("批 a1b2c3d4"), { behavior: "allow", ref: "a1b2c3d4" });
  assert.deepEqual(parseDecisionCommand("拒 550e8400-e29b-41d4-a716-446655440000"), {
    behavior: "deny",
    ref: "550e8400-e29b-41d4-a716-446655440000",
  });
  assert.deepEqual(parseDecisionCommand(`批 ${"a".repeat(64)}`), { behavior: "allow", ref: "a".repeat(64) }, "64 位上限内接受");
  assert.equal(parseDecisionCommand("批 ab1"), null, "短于 4 位拒收");
  assert.equal(parseDecisionCommand(`批 ${"a".repeat(65)}`), null, "超 64 位拒收");
  assert.equal(parseDecisionCommand("批 card-1 理由"), null, "锚定行尾，理由尾参不再接受");
  assert.equal(parseDecisionCommand("批准 card-1"), null, "命令词仅 批/拒");
  // P1-M1：permission requestId 含冒号（omp:permission:<ns>:<seq>）——通知文案原样回复必须可解析
  assert.deepEqual(parseDecisionCommand("批 omp:permission:ns:42"), { behavior: "allow", ref: "omp:permission:ns:42" }, "冒号形态接受");
  assert.deepEqual(parseDecisionCommand("拒 omp:permission:ns:42"), { behavior: "deny", ref: "omp:permission:ns:42" });
  assert.deepEqual(parseDecisionCommand("批 ns_1:ref-9"), { behavior: "allow", ref: "ns_1:ref-9" }, "下划线/冒号混合接受");
  assert.deepEqual(parseDecisionCommand(`批 omp:permission:ns:${"a".repeat(46)}`), { behavior: "allow", ref: `omp:permission:ns:${"a".repeat(46)}` }, "含冒号恰 64 位接受");
  assert.equal(parseDecisionCommand("批 ::::"), null, "冒号不可为首字符（仍字母数字开头）");
  assert.equal(parseDecisionCommand("批 omp:permission:ns:42!"), null, "非法尾字符拒收");
  assert.equal(parseDecisionCommand(`批 omp:permission:ns:${"a".repeat(47)}`), null, "含冒号超 64 位拒收");
});

test("32KiB 自修剪按缩进形态测量：修剪后写盘 ≤ DECISIONS_MAX_BYTES 且保留最新条目", async (t) => {
  const home = await tmpHome();
  t.after(() => fs.rm(home, { recursive: true, force: true }));
  const filler = "x".repeat(9 * 1024);
  await appendDecision(home, { ref: "r1", behavior: "allow", channel: "qq", channelUser: filler });
  await appendDecision(home, { ref: "r2", behavior: "allow", channel: "qq", channelUser: filler });
  await appendDecision(home, { ref: "r3", behavior: "allow", channel: "qq", channelUser: filler });
  const raw = await fs.readFile(decisionsPath(home), "utf8");
  assert.ok(Buffer.byteLength(raw, "utf8") <= DECISIONS_MAX_BYTES, `写盘 ${Buffer.byteLength(raw)} 字节超出 32KiB 上限`);
  const doc = JSON.parse(raw);
  assert.ok(doc.decisions.some((d: { ref: string }) => d.ref === "r3"), "最新条目必须保留");
  assert.ok(!doc.decisions.some((d: { ref: string }) => d.ref === "r1"), "最旧条目应被修剪");
  assert.ok(doc.decisions.some((d: { ref: string }) => d.ref === "r2") || doc.decisions.length === 1);
});
