#!/usr/bin/env node
/** Publish-shape guard: manifest fields, entries present, dependency hygiene. */
import { readFileSync, existsSync, readdirSync } from "node:fs";
import { join } from "node:path";

const root = new URL("..", import.meta.url).pathname;
let failures = 0;
const fail = (message) => {
  failures += 1;
  console.error(`✖ ${message}`);
};
const ok = (message) => console.log(`✔ ${message}`);

const manifest = JSON.parse(readFileSync(join(root, "paseo-plugin.json"), "utf8"));
if (manifest.id !== "paseo-im") fail("manifest id 必须是 paseo-im");
else ok("manifest id");
if (!/^>=\d+\.\d+\.\d+/.test(manifest.requirements?.paseo ?? "")) fail("requirements.paseo 必须是 >=x.y.z");
else ok(`requirements.paseo ${manifest.requirements.paseo}`);
if (!Array.isArray(manifest.build) || !manifest.build.some((cmd) => cmd[0] === "npm" && cmd[1] === "ci")) {
  fail("manifest build 必须包含 npm ci --omit=dev --ignore-scripts");
} else ok("manifest build");

const pkg = JSON.parse(readFileSync(join(root, "package.json"), "utf8"));
for (const entry of ["index.client.tsx", "index.server.ts", "OVERVIEW.md"]) {
  if (!existsSync(join(root, entry))) fail(`缺少 ${entry}`);
  else ok(`存在 ${entry}`);
}
const clientFiles = readdirSync(join(root, "client"));
const serverFiles = readdirSync(join(root, "server"));
if (!clientFiles.length) fail("client/ 为空");
if (!serverFiles.length) fail("server/ 为空");

const forbiddenRuntimeDeps = ["react", "react-native", "@getpaseo/plugin"];
for (const dep of forbiddenRuntimeDeps) {
  if (pkg.dependencies?.[dep]) fail(`${dep} 必须在 devDependencies`);
  else ok(`${dep} 不在 dependencies`);
}
if (!pkg.dependencies?.ws) fail("缺少 ws 依赖");

process.exit(failures ? 1 : 0);
