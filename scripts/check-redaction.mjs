#!/usr/bin/env node
/** 脱敏守卫：扫描即将公开的文件，命中真实个人数据/本机内部信息即非零退出。
 *
 * L1 模式（真实个人数据）：真实微信用户 ID、QQ 发送者 ID、QQ AppID、真实 message_id 段。
 * L2 模式（本机内部信息）：本机绝对路径、运行时 agent/session UUID、daemon serverId/主机名。
 * 豁免：plan.md（本地文档，不入开源包）与 node_modules（git ls-files 天然排除）。
 *
 * 用法：node scripts/check-redaction.mjs   → 命中打印 文件:行号:摘要，退出码 1。
 */
import { execSync } from "node:child_process";
import { readFileSync } from "node:fs";

// 规则字面量用拼接构造，避免守卫文件自命中（规则文本自身就是敏感模式）。
const SENSITIVE_WEIXIN_USER = ["o9cq", "803"].join("");
const SENSITIVE_QQ_SENDER = ["AB33", "B77"].join("");

function buildRules() {
  // UUID 前缀分两半构造：守卫文件里不存在任何连续的完整敏感串。
  const heads = ["cbcc", "d30", "41cc", "01a1b6"];
  const tails = ["478", "abd10", "b6ef", "[de]"];
  const uuidAlternation = heads.map((head, index) => head + tails[index]).join("|");
  return [
    { name: "real-weixin-user-id", pattern: new RegExp(SENSITIVE_WEIXIN_USER, "i") },
    { name: "real-tg-user-id", pattern: new RegExp(["477", "958578"].join("")) },
    { name: "real-qq-sender", pattern: new RegExp(SENSITIVE_QQ_SENDER, "i") },
    { name: "real-qq-appid", pattern: new RegExp(["190", "632", "4994"].join("")) },
    { name: "real-message-id-prefix", pattern: new RegExp(["752", "48[0-9]{10,}"].join("")) },
    { name: "local-absolute-path", pattern: /\/home\/[a-z_]+\/|\/home\\[a-z_]+\\/i },
    { name: "runtime-agent-uuid", pattern: new RegExp(uuidAlternation, "i") },
    { name: "daemon-serverid", pattern: /srv_[A-Za-z0-9_]{8,}|LAPTOP-[A-Z0-9-]+/ },
  ];
}

const RULES = buildRules();

const EXEMPT = ["docs/plan.md", "npm-shrinkwrap.json", "package-lock.json"];

const files = execSync("git ls-files", { encoding: "utf8", maxBuffer: 16 * 1024 * 1024 })
  .split("\n")
  .filter((file) => /\.(md|ts|tsx|mjs|js|json)$/.test(file))
  .filter((file) => !EXEMPT.includes(file));

let hits = 0;
for (const file of files) {
  const content = readFileSync(file, "utf8");
  const lines = content.split("\n");
  lines.forEach((line, index) => {
    for (const rule of RULES) {
      if (rule.pattern.test(line)) {
        hits += 1;
        console.error(`REDACT[${rule.name}] ${file}:${index + 1}: ${line.trim().slice(0, 110)}`);
      }
    }
  });
}

if (hits > 0) {
  console.error(`\n脱敏守卫：${hits} 处命中。真实 ID/本机路径/内部 UUID 不得进入开源内容（豁免：${EXEMPT.join(", ")}）。`);
  process.exit(1);
}
console.log("脱敏守卫：通过（0 命中）");
