import assert from "node:assert/strict";
import { test } from "node:test";
import { mapProviderChoices } from "../server/choices.ts";

test("过滤 unavailable 与无模型 provider", () => {
  const providers = mapProviderChoices([
    { provider: "omp", label: "Oh My Pi", status: "ready", enabled: true, models: [{ id: "opencode-zen/big-pickle", label: "Big Pickle" }] },
    { provider: "muse", label: "Muse Code", status: "unavailable", enabled: false, models: [] },
    { provider: "empty", label: "空模型", status: "ready", models: [] },
    { provider: "no-models-key", label: "无 models 键", status: "ready" },
  ]);
  assert.equal(providers.length, 1);
  assert.equal(providers[0]?.id, "omp");
  assert.deepEqual(providers[0]?.models.map((m) => m.id), ["opencode-zen/big-pickle"]);
});

test("模型元素缺 id 时丢弃该模型", () => {
  const providers = mapProviderChoices([
    { provider: "x", models: [{ id: "m1" }, { label: "无 id" }, { model: "m2" }] },
  ]);
  assert.deepEqual(providers[0]?.models.map((m) => m.id), ["m1", "m2"]);
});

test("modes 落到模型级，provider.modes 作兜底；thinkingOptions 映射 thinking", () => {
  const providers = mapProviderChoices([
    {
      provider: "omp",
      modes: [{ id: "full", label: "Full Access" }],
      models: [
        { id: "m-a", thinkingOptions: [{ id: "auto", label: "Auto" }] },
        { id: "m-b", modes: [{ id: "write", label: "Write" }] },
      ],
    },
  ]);
  const [provider] = providers;
  assert.deepEqual(provider?.models[0]?.modes, [{ id: "full", label: "Full Access" }]); // record.modes 缺省回退 provider.modes
  assert.deepEqual(provider?.models[0]?.thinking, [{ id: "auto", label: "Auto" }]);
  assert.deepEqual(provider?.models[1]?.modes, [{ id: "write", label: "Write" }]);
});

test("label 缺省回退 id/provider", () => {
  const providers = mapProviderChoices([
    { provider: "p1", models: [{ id: "m1" }] },
    { provider: "p2", label: "P2 标签", models: [{ id: "m2", label: "M2 标签" }] },
  ]);
  assert.equal(providers[0]?.label, "p1");
  assert.equal(providers[0]?.models[0]?.label, "m1");
  assert.equal(providers[1]?.label, "P2 标签");
  assert.equal(providers[1]?.models[0]?.label, "M2 标签");
});
