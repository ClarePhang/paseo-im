import assert from "node:assert/strict";
import { test } from "node:test";
import { mkdtemp, readFile, stat, writeFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { atomicWrite, readJson, WriteChain, sleep } from "../server/files.ts";

test("atomicWrite 产出 0600 文件与 0700 目录", async () => {
  const root = await mkdtemp(join(tmpdir(), "paseo-im-files-"));
  try {
    const path = join(root, "nested", "doc.json");
    await atomicWrite(path, { version: 1 });
    const fileStat = await stat(path);
    assert.equal(fileStat.mode & 0o777, 0o600);
    const dirStat = await stat(join(root, "nested"));
    assert.equal(dirStat.mode & 0o777, 0o700);
    const raw = JSON.parse(await readFile(path, "utf8"));
    assert.deepEqual(raw, { version: 1 });
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("atomicWrite 原子替换不留临时文件", async () => {
  const root = await mkdtemp(join(tmpdir(), "paseo-im-files-"));
  try {
    const path = join(root, "doc.json");
    await atomicWrite(path, { a: 1 });
    await atomicWrite(path, { a: 2 });
    const raw = JSON.parse(await readFile(path, "utf8"));
    assert.equal(raw.a, 2);
    const { readdir } = await import("node:fs/promises");
    const entries = await readdir(root);
    assert.deepEqual(entries, ["doc.json"]);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("readJson 缺失文件返回 undefined", async () => {
  const missing = join(tmpdir(), `paseo-im-missing-${Date.now()}.json`);
  assert.equal(await readJson(missing, (value) => value), undefined);
});

test("readJson 损坏文档抛用户可读错误", async () => {
  const root = await mkdtemp(join(tmpdir(), "paseo-im-files-"));
  try {
    const path = join(root, "broken.json");
    await writeFile(path, "{not json", "utf8");
    await assert.rejects(() => readJson(path, (value) => value), /无法解析/);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("WriteChain 严格按提交顺序执行", async () => {
  const chain = new WriteChain();
  const order: number[] = [];
  const gate = Promise.withResolvers<void>();
  const first = chain.run(async () => {
    await gate.promise;
    order.push(1);
    return "first";
  });
  const second = chain.run(async () => {
    order.push(2);
    return "second";
  });
  await Promise.resolve();
  gate.resolve();
  assert.deepEqual(await Promise.all([first, second]), ["first", "second"]);
  assert.deepEqual(order, [1, 2]);
});

test("WriteChain 前序失败不阻塞后续", async () => {
  const chain = new WriteChain();
  await assert.rejects(
    chain.run(async () => {
      throw new Error("boom");
    }),
    /boom/,
  );
  const result = await chain.run(async () => "recovered");
  assert.equal(result, "recovered");
});

test("sleep 可被 signal 打断", async () => {
  const controller = new AbortController();
  const pending = sleep(60_000, controller.signal);
  queueMicrotask(() => controller.abort(new Error("stop")));
  await assert.rejects(() => pending, /stop/);
});
