/** bots-plus-outbox 只读侧测试：新条目过滤/watermark/容错降级（P2b 第一步）。 */
import assert from "node:assert/strict";
import { test } from "node:test";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { botsPlusOutboxV1, outboxPath, readNewEntries, readWatermark, writeWatermark } from "../server/bots-plus-outbox.ts";

const entry = (seq: number) => ({
  seq,
  kind: "approval-notify" as const,
  botId: "b1",
  botName: "Probe",
  ref: `card-${seq}`,
  title: "审批：memory-write",
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
