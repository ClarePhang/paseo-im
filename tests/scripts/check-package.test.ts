import { test } from "node:test";
import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { cp, mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";

const exec = promisify(execFile);
const repoRoot = join(dirname(fileURLToPath(import.meta.url)), "..", "..");

interface GuardOptions {
  dependencies: Record<string, string>;
  devDependencies?: Record<string, string>;
  server?: string;
  shared?: string;
}

async function fixture(options: GuardOptions): Promise<string> {
  const dir = await mkdtemp(join(tmpdir(), "paseo-im-package-guard-"));
  await mkdir(join(dir, "scripts"), { recursive: true });
  await mkdir(join(dir, "client"), { recursive: true });
  await mkdir(join(dir, "server"), { recursive: true });
  await mkdir(join(dir, "shared"), { recursive: true });
  await cp(join(repoRoot, "scripts/check-package.mjs"), join(dir, "scripts/check-package.mjs"));
  await writeFile(
    join(dir, "package.json"),
    JSON.stringify(
      {
        name: "paseo-im",
        version: "0.1.0",
        dependencies: options.dependencies,
        devDependencies: options.devDependencies ?? {},
      },
      null,
      2,
    ),
  );
  await writeFile(
    join(dir, "paseo-plugin.json"),
    JSON.stringify({
      id: "paseo-im",
      requirements: { paseo: ">=0.15.2" },
      build: [["npm", "ci", "--omit=dev", "--ignore-scripts"]],
    }),
  );
  await writeFile(join(dir, "OVERVIEW.md"), "# overview\n");
  await writeFile(join(dir, "index.client.tsx"), "export {};\n");
  await writeFile(join(dir, "index.server.ts"), "export {};\n");
  await writeFile(join(dir, "client/channel.ts"), "export {};\n");
  await writeFile(join(dir, "server/channel.ts"), options.server ?? "export {};\n");
  await writeFile(join(dir, "shared/contracts.ts"), options.shared ?? "export {};\n");
  return dir;
}

async function runGuard(dir: string): Promise<{ code: number; stdout: string; stderr: string }> {
  try {
    const { stdout, stderr } = await exec(process.execPath, [join(dir, "scripts/check-package.mjs")]);
    return { code: 0, stdout, stderr };
  } catch (error) {
    const failure = error as { code?: number; stdout?: string; stderr?: string };
    return { code: failure.code ?? 1, stdout: failure.stdout ?? "", stderr: failure.stderr ?? "" };
  }
}

test("check-package：宿主 SDK 在 dependencies 时通过", async () => {
  const dir = await fixture({
    dependencies: { ws: "8.22.0", "@getpaseo/client": "0.16.0" },
    server: 'import type { PaseoApi } from "@getpaseo/client";\nexport {};\n',
  });
  try {
    const result = await runGuard(dir);
    assert.equal(result.code, 0, result.stderr);
    assert.match(result.stdout, /@getpaseo\/client 在 dependencies/);
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});

test("check-package：宿主 SDK 退回 devDependencies 时失败", async () => {
  const dir = await fixture({
    dependencies: { ws: "8.22.0" },
    devDependencies: { "@getpaseo/client": "0.16.0" },
    server: 'import type { PaseoApi } from "@getpaseo/client";\nexport {};\n',
  });
  try {
    const result = await runGuard(dir);
    assert.equal(result.code, 1);
    assert.match(result.stderr, /@getpaseo\/client 被 server\/channel\.ts 引用/);
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});

test("check-package：副作用导入与 require 同样受守卫", async () => {
  const sideEffect = await fixture({
    dependencies: { ws: "8.22.0" },
    devDependencies: { "@getpaseo/register": "1.0.0" },
    server: 'import "@getpaseo/register";\nexport {};\n',
  });
  const required = await fixture({
    dependencies: { ws: "8.22.0" },
    devDependencies: { "@getpaseo/legacy": "1.0.0" },
    shared: 'const legacy = require("@getpaseo/legacy");\nexport { legacy };\n',
  });
  try {
    const first = await runGuard(sideEffect);
    assert.equal(first.code, 1);
    assert.match(first.stderr, /@getpaseo\/register 被 server\/channel\.ts 引用/);
    const second = await runGuard(required);
    assert.equal(second.code, 1);
    assert.match(second.stderr, /@getpaseo\/legacy 被 shared\/contracts\.ts 引用/);
  } finally {
    await rm(sideEffect, { recursive: true, force: true });
    await rm(required, { recursive: true, force: true });
  }
});

test("check-package：宿主注入的 @getpaseo/plugin 无需声明", async () => {
  const dir = await fixture({
    dependencies: { ws: "8.22.0" },
    server: 'import { defineRpc } from "@getpaseo/plugin";\nexport { defineRpc };\n',
  });
  try {
    const result = await runGuard(dir);
    assert.equal(result.code, 0, result.stderr);
    assert.doesNotMatch(result.stdout, /@getpaseo\/plugin 在 dependencies/);
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});
