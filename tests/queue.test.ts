import assert from "node:assert/strict";
import { test } from "node:test";
import { AgentQueue } from "../server/queue.ts";

test("同 key 严格串行", async () => {
  const queue = new AgentQueue();
  const order: string[] = [];
  const gate = Promise.withResolvers<void>();
  const first = queue.run("conv:a", async () => {
    await gate.promise;
    order.push("a1");
  });
  const second = queue.run("conv:a", async () => {
    order.push("a2");
  });
  await Promise.resolve();
  assert.equal(queue.size(), 2);
  gate.resolve();
  await Promise.all([first, second]);
  assert.deepEqual(order, ["a1", "a2"]);
});

test("不同 key 并发", async () => {
  const queue = new AgentQueue();
  const order: string[] = [];
  const gate = Promise.withResolvers<void>();
  const first = queue.run("conv:a", async () => {
    await gate.promise;
    order.push("a");
  });
  const second = queue.run("conv:b", async () => {
    order.push("b");
  });
  await second;
  assert.deepEqual(order, ["b"]);
  gate.resolve();
  await first;
  assert.deepEqual(order, ["b", "a"]);
});

test("前序失败不阻塞后续", async () => {
  const queue = new AgentQueue();
  await assert.rejects(
    queue.run("conv:a", async () => {
      throw new Error("boom");
    }),
    /boom/,
  );
  const result = await queue.run("conv:a", async () => "recovered");
  assert.equal(result, "recovered");
});

test("signal 在排队时即中止", async () => {
  const queue = new AgentQueue();
  const gate = Promise.withResolvers<void>();
  const controller = new AbortController();
  const blocked = queue.run("conv:a", async () => {
    await gate.promise;
  });
  const cancelled = queue.run("conv:a", async () => assert.fail("不应执行"), controller.signal);
  controller.abort(new Error("stopped"));
  await assert.rejects(() => cancelled, /stopped/);
  gate.resolve();
  await blocked;
});
