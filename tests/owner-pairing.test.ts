import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { test } from "node:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
  applyOwnerPairing,
  ownerPairingBindingTtlMs,
  ownerPairingTtlMs,
  OwnerPairingBook,
  parsePairingCommand,
} from "../server/owner-pairing.ts";
import { StateStore } from "../server/state-store.ts";

const CONNECTION_ID = "00000000-0000-4000-8000-000000000001";
const NOW = 1_700_000_000_000;

async function stateStore(): Promise<{ state: StateStore; cleanup: () => Promise<void> }> {
  const root = await mkdtemp(join(tmpdir(), "paseo-im-owner-pairing-"));
  const state = new StateStore(CONNECTION_ID, root);
  await state.load();
  return { state, cleanup: () => rm(root, { recursive: true, force: true }) };
}

test("start：64hex 码 + sha256 摘要 + 5min TTL；重发码作废旧账本", () => {
  const book = new OwnerPairingBook();
  const first = book.start(CONNECTION_ID, NOW);
  assert.match(first.code, /^[a-f0-9]{64}$/);
  assert.equal(first.nonceHash, createHash("sha256").update(first.code).digest("hex"));
  assert.equal(first.expiresAt, NOW + ownerPairingTtlMs);
  assert.match(first.attemptId, /^[a-z0-9]{24}$/);
  const second = book.start(CONNECTION_ID, NOW);
  assert.equal(book.attempt(first.attemptId), undefined, "重发码作废旧 attempt");
  assert.equal(book.poll(first.attemptId, null, NOW).status, "none");
  assert.equal(
    book.verify({ connectionId: CONNECTION_ID, senderId: "u", chatId: "c", code: first.code, now: NOW }).outcome,
    "rejected",
    "旧码失效",
  );
  assert.ok(second.attemptId !== first.attemptId);
});

test("TTL 过期：verify 与 poll 均拒绝并清账本", () => {
  const book = new OwnerPairingBook();
  const { attemptId, code } = book.start(CONNECTION_ID, NOW);
  const after = NOW + ownerPairingTtlMs + 1;
  assert.equal(
    book.verify({ connectionId: CONNECTION_ID, senderId: "u", chatId: "c", code, now: after }).outcome,
    "rejected",
    "过期码拒绝",
  );
  assert.equal(book.attempt(attemptId), undefined, "过期清账本");
  const { attemptId: id2 } = book.start(CONNECTION_ID, NOW);
  assert.equal(book.poll(id2, null, after).status, "expired");
});

test("命中：applyOwnerPairing 写 ownerPairing（10y 绑定），poll ok 消费 attempt", async () => {
  const { state, cleanup } = await stateStore();
  try {
    const book = new OwnerPairingBook();
    const { attemptId, nonceHash, code } = book.start(CONNECTION_ID, NOW);
    const outcome = await applyOwnerPairing({ book, state, connectionId: CONNECTION_ID, senderId: "owner-1", chatId: "chat-1", code, now: NOW });
    assert.equal(outcome, "paired");
    const pairing = state.get().ownerPairing;
    assert.deepEqual(
      pairing && { ownerId: pairing.ownerId, ownerChatId: pairing.ownerChatId, hash: pairing.hash, expiresAt: pairing.expiresAt },
      { ownerId: "owner-1", ownerChatId: "chat-1", hash: nonceHash, expiresAt: NOW + ownerPairingBindingTtlMs },
    );
    assert.deepEqual(book.poll(attemptId, state.get().ownerPairing, NOW), { status: "ok", ownerId: "owner-1" });
    assert.equal(book.poll(attemptId, state.get().ownerPairing, NOW).status, "none", "ok 消费 attempt");
  } finally {
    await cleanup();
  }
});

test("错码拒绝不写；无 pending 配对静默 ignored；先绑优先不得改绑", async () => {
  const { state, cleanup } = await stateStore();
  try {
    const book = new OwnerPairingBook();
    // 无 pending：ignored（bridge 据此不拦，走正常流程）
    assert.equal(
      await applyOwnerPairing({ book, state, connectionId: CONNECTION_ID, senderId: "u", chatId: "c", code: "0".repeat(64), now: NOW }),
      "ignored",
    );
    // 错码：rejected 且不写 ownerPairing
    const { code } = book.start(CONNECTION_ID, NOW);
    assert.equal(
      await applyOwnerPairing({ book, state, connectionId: CONNECTION_ID, senderId: "u", chatId: "c", code: "1".repeat(64), now: NOW }),
      "rejected",
    );
    assert.equal(state.get().ownerPairing, null);
    // 命中落盘后：先绑优先——同绑双方可重复确认，他人携同码被拒且不改绑
    assert.equal(
      await applyOwnerPairing({ book, state, connectionId: CONNECTION_ID, senderId: "owner-1", chatId: "chat-1", code, now: NOW }),
      "paired",
    );
    assert.equal(
      await applyOwnerPairing({ book, state, connectionId: CONNECTION_ID, senderId: "attacker", chatId: "chat-evil", code, now: NOW }),
      "rejected",
      "后到者不得改绑",
    );
    assert.equal(state.get().ownerPairing?.ownerId, "owner-1");
    assert.equal(state.get().ownerPairing?.ownerChatId, "chat-1");
  } finally {
    await cleanup();
  }
});

test("parsePairingCommand：仅接受「配对 <64小写hex>」形态", () => {
  const code = "ab".repeat(32);
  assert.equal(parsePairingCommand(`配对 ${code}`), code);
  assert.equal(parsePairingCommand(`配对   ${code}`), code, "多空格容忍（\\s+）");
  assert.equal(parsePairingCommand(`配对 ${"AB".repeat(32)}`), null, "大写不匹配");
  assert.equal(parsePairingCommand(`配对 ${"ab".repeat(31)}`), null, "长度不足");
  assert.equal(parsePairingCommand(`请配对 ${code}`), null, "前缀不匹配");
  assert.equal(parsePairingCommand(`配对 ${code} 谢谢`), null, "尾缀不匹配");
});
