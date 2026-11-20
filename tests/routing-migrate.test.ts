#!/usr/bin/env node
/** routing 迁移纯函数：改连接去向时跟随旧 agent 的会话/绑定迁移规则。 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { migrateBindingTargets, migrateConversationTargets } from "../server/routing.ts";
import type { Target } from "../shared/config.ts";

const agent = (id: string) => ({ kind: "agent", agentId: id, sharedHistory: true }) as Target;
const workspace = (id: string) => ({ kind: "workspace", workspaceId: id }) as Target;

describe("migrateConversationTargets", () => {
  it("agent→agent：跟随旧 agent 的会话改绑新 agent", () => {
    const conversations = {
      "dm:a": { agentId: "old-agent" },
      "dm:b": { agentId: "other-agent" },
      "dm:c": { agentId: null },
    };
    migrateConversationTargets(conversations, "old-agent", agent("new-agent"));
    assert.deepEqual(
      { a: conversations["dm:a"].agentId, b: conversations["dm:b"].agentId, c: conversations["dm:c"].agentId },
      { a: "new-agent", b: "other-agent", c: null },
    );
  });

  it("agent→workspace：跟随旧 agent 的会话解除绑定（回归默认去向）", () => {
    const conversations = {
      "dm:a": { agentId: "old-agent" },
      "dm:b": { agentId: "other-agent" },
    };
    migrateConversationTargets(conversations, "old-agent", workspace("wks-1"));
    assert.equal(conversations["dm:a"].agentId, null);
    assert.equal(conversations["dm:b"].agentId, "other-agent");
  });

  it("next 为 null（清空去向）：跟随旧 agent 的会话解绑", () => {
    const conversations = { "dm:a": { agentId: "old-agent" } };
    migrateConversationTargets(conversations, "old-agent", null);
    assert.equal(conversations["dm:a"].agentId, null);
  });
});

describe("migrateBindingTargets", () => {
  it("agent→agent：指向旧 agent 的绑定迁移到新 agent，其余不动", () => {
    const result = migrateBindingTargets(
      { "dm:a": agent("old-agent"), "dm:b": agent("other-agent"), "dm:c": workspace("wks-1") },
      "old-agent",
      agent("new-agent"),
    );
    assert.deepEqual(result, {
      "dm:a": agent("new-agent"),
      "dm:b": agent("other-agent"),
      "dm:c": workspace("wks-1"),
    });
  });

  it("agent→workspace：解除指向旧 agent 的绑定，回归默认去向", () => {
    const result = migrateBindingTargets(
      { "dm:a": agent("old-agent"), "dm:b": agent("other-agent") },
      "old-agent",
      workspace("wks-1"),
    );
    assert.deepEqual(result, { "dm:b": agent("other-agent") });
  });
});
