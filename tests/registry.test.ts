import assert from "node:assert/strict";
import { test } from "node:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Registry } from "../server/registry.ts";
import { UserFacingError } from "../shared/errors.ts";
import { emptyPolicy, connectionSchema, type Connection } from "../shared/config.ts";

function telegramConnection(id: string, revision = 0): Connection {
  return connectionSchema.parse({
    id,
    channel: "telegram",
    name: "测试机器人",
    enabled: false,
    target: null,
    policy: emptyPolicy(),
    bindings: {},
    revision,
    username: "paseo_bot",
    botUserId: "10086",
  }) as Connection;
}

const uuid = (n: number) =>
  `${String(n).padStart(8, "0")}-0000-4000-8000-000000000000`;

async function withRegistry(run: (registry: Registry, path: string) => Promise<void>) {
  const root = await mkdtemp(join(tmpdir(), "paseo-im-registry-"));
  const path = join(root, "connections.json");
  try {
    await run(new Registry(path), path);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
}

test("add 与 list 往返持久化", async () => {
  await withRegistry(async (registry, path) => {
    await registry.add(telegramConnection(uuid(1)));
    const reloaded = new Registry(path);
    await reloaded.load();
    assert.equal(reloaded.list().length, 1);
    const loaded = reloaded.get(uuid(1));
    assert.equal(loaded?.channel === "telegram" ? loaded.username : undefined, "paseo_bot");
  });
});

test("replace 乐观锁：revision 冲突拒绝", async () => {
  await withRegistry(async (registry) => {
    await registry.add(telegramConnection(uuid(1), 0));
    const stale = telegramConnection(uuid(1), 0);
    const fresh = await registry.replace({ ...stale, name: "新名字" });
    assert.equal(fresh.revision, 1);
    await assert.rejects(
      () => registry.replace({ ...stale, name: "旧 revision" }),
      (error) => error instanceof UserFacingError && /已在其他客户端修改/.test(error.message),
    );
    assert.equal(registry.get(uuid(1))?.name, "新名字");
  });
});

test("remove 删除并持久化", async () => {
  await withRegistry(async (registry, path) => {
    await registry.add(telegramConnection(uuid(1)));
    await registry.add(telegramConnection(uuid(2)));
    assert.deepEqual(await registry.remove(uuid(1)), true);
    assert.deepEqual(await registry.remove(uuid(1)), false);
    const reloaded = new Registry(path);
    await reloaded.load();
    assert.deepEqual(reloaded.list().map((entry) => entry.id), [uuid(2)]);
  });
});

test("mutate 静默推进 revision", async () => {
  await withRegistry(async (registry) => {
    await registry.add(telegramConnection(uuid(1), 3));
    await registry.mutate(uuid(1), (entry) => ({ ...entry, botUserId: "999" }));
    const mutated = registry.get(uuid(1));
    const botUserId = mutated?.channel === "telegram" ? mutated.botUserId : undefined;
    assert.equal(botUserId, "999");
    assert.equal(registry.get(uuid(1))?.revision, 3);
  });
});
