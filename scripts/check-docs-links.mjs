#!/usr/bin/env node
/** 文档链接守卫：校验全部 markdown 相对链接（[text](path)）指向存在的文件。
 * http(s)/mailto/anchor 链接跳过；只校验仓库内相对路径与根相对路径。
 * 用法：node scripts/check-docs-links.mjs → 断链打印文件:行号，退出码 1。
 */
import { execSync } from "node:child_process";
import { readFileSync, existsSync, statSync } from "node:fs";
import { dirname, resolve } from "node:path";

const LINK = /\[[^\]]+\]\(([^)]+)\)/g;

const files = execSync("git ls-files", { encoding: "utf8", maxBuffer: 16 * 1024 * 1024 })
  .split("\n")
  .filter((file) => file.endsWith(".md"));

let broken = 0;
for (const file of files) {
  const base = dirname(resolve(file));
  const content = readFileSync(file, "utf8");
  const lines = content.split("\n");
  lines.forEach((line, index) => {
    for (const match of line.matchAll(LINK)) {
      const target = match[1].trim();
      if (/^(https?:|mailto:|#)/.test(target)) continue;
      const path = target.startsWith("/") ? target.slice(1) : target;
      const abs = resolve(base, path);
      if (!existsSync(abs) || !statSync(abs).isFile()) {
        broken += 1;
        console.error(`BROKEN-LINK ${file}:${index + 1}: ${target}`);
      }
    }
  });
}

if (broken > 0) {
  console.error(`\n文档链接守卫：${broken} 处断链。`);
  process.exit(1);
}
console.log("文档链接守卫：全部相对链接有效");
