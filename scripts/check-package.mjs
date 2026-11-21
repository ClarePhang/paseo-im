#!/usr/bin/env node
/** Publish-shape guard: manifest fields, entries present, dependency hygiene. */
import { readFileSync, existsSync, readdirSync, statSync } from "node:fs";
import { join, relative } from "node:path";

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

// The daemon installs with `npm ci --omit=dev` and only injects
// @getpaseo/plugin (+ zod) into author bundles. Every other @getpaseo/*
// import must therefore be a production dependency, or the install build
// fails with "Could not resolve type dependency" (issue #1).
const HOST_INJECTED = ["@getpaseo/plugin"];
const hostImportFiles = [];
const walk = (directory) => {
  for (const entry of readdirSync(directory, { withFileTypes: true })) {
    const path = join(directory, entry.name);
    if (entry.isDirectory()) walk(path);
    else if (/\.[cm]?[jt]sx?$/.test(entry.name)) hostImportFiles.push(path);
  }
};
for (const target of ["index.server.ts", "index.client.tsx", "server", "shared", "client"]) {
  const path = join(root, target);
  if (!existsSync(path)) continue;
  if (statSync(path).isDirectory()) walk(path);
  else hostImportFiles.push(path);
}
const HOST_IMPORT_RE =
  /(?:import|export)\s+(type\s+)?[^;]*?from\s+["'](@getpaseo\/[^"']+)["']|import\s*\(\s*["'](@getpaseo\/[^"']+)["']/g;
const hostImports = new Map();
for (const file of hostImportFiles) {
  for (const match of readFileSync(file, "utf8").matchAll(HOST_IMPORT_RE)) {
    const typeOnly = Boolean(match[1]);
    const specifier = match[2] ?? match[3];
    const packageName = specifier.split("/").slice(0, 2).join("/");
    if (HOST_INJECTED.includes(packageName)) continue;
    // Erased protocol types are host contracts and need no local install.
    if (typeOnly && packageName === "@getpaseo/protocol") continue;
    const importers = hostImports.get(packageName) ?? new Set();
    importers.add(relative(root, file));
    hostImports.set(packageName, importers);
  }
}
for (const [packageName, importers] of hostImports) {
  if (pkg.dependencies?.[packageName]) ok(`${packageName} 在 dependencies`);
  else fail(`${packageName} 被 ${[...importers].join(", ")} 引用，但不在 dependencies（npm ci --omit=dev 后构建无法解析）`);
}

process.exit(failures ? 1 : 0);
