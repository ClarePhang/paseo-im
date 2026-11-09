import assert from "node:assert/strict";
import { test } from "node:test";
import { emptyPolicy, type Policy } from "../shared/config.ts";
import { isAuthorized } from "../server/policy.ts";

const message = (overrides: { kind?: "dm" | "group"; senderId?: string; mention?: boolean; replyToBot?: boolean; command?: boolean }) => ({
  kind: overrides.kind ?? "dm",
  senderId: overrides.senderId ?? "user-1",
  trigger: {
    mention: overrides.mention ?? false,
    replyToBot: overrides.replyToBot ?? false,
    command: overrides.command ?? false,
  },
});

const policy = (overrides: Partial<Policy>): Policy => ({ ...emptyPolicy(), ...overrides });

test("私聊：operators 模式仅放行名单", () => {
  const p = policy({ directMessages: "operators", operators: ["user-1"] });
  assert.ok(isAuthorized({ message: message({}) as never, policy: p, isAdmin: false }));
  assert.ok(!isAuthorized({ message: message({ senderId: "user-2" }) as never, policy: p, isAdmin: false }));
});

test("私聊：everyone 放行所有", () => {
  const p = policy({ directMessages: "everyone" });
  assert.ok(isAuthorized({ message: message({ senderId: "anyone" }) as never, policy: p, isAdmin: false }));
});

test("群聊：mentions 触发模式要求 @ 或回复", () => {
  const p = policy({ invocation: "mentions", groupUsers: "everyone" });
  assert.ok(isAuthorized({ message: message({ kind: "group", mention: true }) as never, policy: p, isAdmin: false }));
  assert.ok(isAuthorized({ message: message({ kind: "group", replyToBot: true }) as never, policy: p, isAdmin: false }));
  assert.ok(!isAuthorized({ message: message({ kind: "group" }) as never, policy: p, isAdmin: false }));
});

test("群聊：all 触发模式不要求 @", () => {
  const p = policy({ invocation: "all", groupUsers: "everyone" });
  assert.ok(isAuthorized({ message: message({ kind: "group" }) as never, policy: p, isAdmin: false }));
});

test("群聊：admins 模式校验管理员", () => {
  const p = policy({ groupUsers: "admins" });
  assert.ok(isAuthorized({ message: message({ kind: "group", mention: true }) as never, policy: p, isAdmin: true }));
  assert.ok(!isAuthorized({ message: message({ kind: "group", mention: true }) as never, policy: p, isAdmin: false }));
  assert.ok(isAuthorized({ message: message({ kind: "group", mention: true, senderId: "boss" }) as never, policy: policy({ groupUsers: "admins", operators: ["boss"] }), isAdmin: false }));
});

test("群聊：operators 模式仅名单", () => {
  const p = policy({ groupUsers: "operators", operators: ["a", "b"] });
  assert.ok(isAuthorized({ message: message({ kind: "group", mention: true, senderId: "a" }) as never, policy: p, isAdmin: false }));
  assert.ok(!isAuthorized({ message: message({ kind: "group", mention: true, senderId: "c" }) as never, policy: p, isAdmin: false }));
});
