#!/usr/bin/env node
/** Runtime boundary guard: client code must not import Node or server modules;
 * root must stay free of code modules; shared must not import runtimes. */
import { readFileSync, readdirSync, statSync } from "node:fs";
import { join, relative } from "node:path";

const root = new URL("..", import.meta.url).pathname;
let violations = 0;

function walk(dir, callback) {
  for (const entry of readdirSync(dir, { withFileTypes: true })) {
    const path = join(dir, entry.name);
    if (entry.isDirectory()) walk(path, callback);
    else if (/\.(ts|tsx)$/.test(entry.name)) callback(path);
  }
}

const CLIENT_BANNED = [
  /^node:/,
  /^@types\//,
  /(^|[\/])server\//,
  /^\.\.\/server\//,
  /^ws$/,
  /^["']fs["']/,
  /^["']path["']/,
  /^["']os["']/,
  /^["']crypto["']/,
];
const SHARED_BANNED = [/^node:/, /react/, /["']ws["']/];

function check(dir, rules, label) {
  walk(join(root, dir), (path) => {
    const source = readFileSync(path, "utf8");
    for (const match of source.matchAll(/from\s+["']([^"']+)["']/g)) {
      const specifier = match[1];
      for (const rule of rules) {
        if (rule.test(specifier)) {
          violations += 1;
          console.error(`✖ ${label} ${relative(root, path)} 导入了 ${specifier}`);
        }
      }
    }
  });
}

check("client", CLIENT_BANNED, "[client]");
check("shared", SHARED_BANNED, "[shared]");
for (const entry of readdirSync(root)) {
  if (statSync(join(root, entry)).isFile() && /\.(ts|tsx)$/.test(entry) && entry !== "index.client.tsx" && entry !== "index.server.ts") {
    violations += 1;
    console.error(`✖ 根目录存在代码模块 ${entry}（只允许两个入口）`);
  }
}

if (violations) {
  console.error(`共 ${violations} 处边界违规`);
  process.exit(1);
}
console.log("✔ 运行时边界检查通过");
