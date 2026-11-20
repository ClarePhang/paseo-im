#!/usr/bin/env node
/** 脱敏守卫：扫描即将公开的文件与提交信息，命中即非零退出。
 *
 * 三层规则：
 *   PUBLIC（内嵌）— 可公开的模式定义（真实个人数据、运行时 UUID、本机路径）。
 *   IDENTITY（`.redaction-identity-rules.json`，git-ignored）— 雇主/贡献者身份模式
 *     （公司名、内部网关/模型路径、主机名、内部账号）。文件存在时加载并入；
 *     不存在则跳过该层（新 clone 环境守卫仍覆盖 PUBLIC 层）。
 *   ALLOWLIST（同规则文件 `allowlist` 字段）— 显式放行的占位变量
 *     （$harness $model $thinkingMode）。扫描前先从待测文本剔除白名单命中
 *     片段，再做规则判定——防止未来规则误伤合法占位变量引用。
 *
 * 扫描范围：
 *   默认：全部跟踪文件内容 + 未推送提交信息（origin/main..HEAD）。
 *   --files-only：跳过提交信息扫描（仅文件）。
 *
 * 规则文件在每条命中处输出规则 id，便于定位策略条目。
 * 用法：
 *   node scripts/check-redaction.mjs             # 文件 + 未推送提交信息
 *   node scripts/check-redaction.mjs --files-only
 */
import { execSync } from "node:child_process";
import { readFileSync, existsSync } from "node:fs";

// —— PUBLIC 规则（本文件可公开；模式值用拼接构造防自命中）——
const join = (parts) => parts.join("");
const RULES = [
  {
    name: "real-weixin-user-id",
    pattern: new RegExp(join(["o9cq", "803"]), "i"),
  },
  {
    name: "real-tg-user-id",
    pattern: new RegExp(join(["477", "958578"])),
  },
  {
    name: "real-qq-sender",
    pattern: new RegExp(join(["AB33", "B77"]), "i"),
  },
  {
    name: "real-qq-appid",
    pattern: new RegExp(join(["190", "632", "4994"])),
  },
  {
    name: "real-message-id-prefix",
    pattern: new RegExp(join(["752", "48[0-9]{10,}"])),
  },
  {
    name: "local-absolute-path",
    pattern: /\/home\/[a-z_]+\//i,
  },
  {
    name: "runtime-agent-uuid",
    pattern: new RegExp(
      join(["cbcc", "478|", "d30", "abd10|", "41cc", "b6ef|", "01a1b6[de]"]),
      "i",
    ),
  },
  {
    name: "daemon-serverid",
    pattern: /srv_[A-Za-z0-9_]{8,}|LAPTOP-[A-Z0-9-]+/,
  },
];

// —— IDENTITY 规则（外置 git-ignored 文件，存在则并入）——
const IDENTITY_RULES_PATH = ".redaction-identity-rules.json";
function loadIdentityRules() {
  if (!existsSync(IDENTITY_RULES_PATH)) return [];
  try {
    const raw = JSON.parse(readFileSync(IDENTITY_RULES_PATH, "utf8"));
    const rules = Object.entries(raw)
      .filter(([name]) => name !== "allowlist")
      .map(([name, entry]) => {
        const patterns = Array.isArray(entry.patterns) ? entry.patterns : [entry.patterns];
        const flags = entry.flags ?? "";
        return { name: `identity:${name}`, pattern: new RegExp(patterns.join("|"), flags) };
      });
    const allowlist = Array.isArray(raw.allowlist) ? raw.allowlist : [];
    return { rules, allowlist: allowlist.map((p) => new RegExp(p, (raw.allowlistFlags ?? "") + "g")) };
  } catch (error) {
    console.error(`警告：身份规则文件解析失败，仅运行 PUBLIC 规则：${error.message}`);
    return { rules: [], allowlist: [] };
  }
}

const { rules: identityRules, allowlist } = loadIdentityRules();
const RULES_ALL = [...RULES, ...identityRules];

/** 扫描前剔除白名单片段（如 $harness $model $thinkingMode 占位变量引用）。 */
function stripAllowlist(text) {
  let out = text;
  for (const allow of allowlist) out = out.replace(allow, "");
  return out;
}

/** 对一段文本跑全部规则，返回命中描述数组。 */
function scan(text, label) {
  const hits = [];
  for (const rule of RULES_ALL) {
    const lines = stripAllowlist(text).split("\n");
    lines.forEach((line, index) => {
      if (rule.pattern.test(stripAllowlist(line))) {
        hits.push(`${label}:${index + 1} [${rule.name}] ${line.trim().slice(0, 110)}`);
      }
    });
  }
  return hits;
}

const filesOnly = process.argv.includes("--files-only");

let hits = 0;
const report = (hit) => {
  hits += 1;
  console.error(`REDACT ${hit}`);
};

// —— 1. 文件内容扫描 ——
const EXEMPT = ["docs/plan.md", "npm-shrinkwrap.json", "package-lock.json"];
const files = execSync("git ls-files", { encoding: "utf8", maxBuffer: 16 * 1024 * 1024 })
  .split("\n")
  .filter((file) => /\.(md|ts|tsx|mjs|js|json)$/.test(file))
  .filter((file) => !EXEMPT.includes(file));

for (const file of files) {
  for (const hit of scan(readFileSync(file, "utf8"), `${file}`)) report(hit);
}

// —— 2. 未推送提交信息扫描（推送前门禁：提交信息与文件同标准）——
if (!filesOnly) {
  let unpushed = "";
  try {
    unpushed = execSync("git log origin/main..HEAD --format=%B", {
      encoding: "utf8",
      maxBuffer: 8 * 1024 * 1024,
    });
  } catch {
    console.error("提示：origin/main 不可达，跳过提交信息扫描（仅文件扫描已执行）。");
  }
  if (unpushed.trim()) {
    for (const hit of scan(unpushed, "commit-message(unpushed)")) report(hit);
  }
}

if (hits > 0) {
  const identityCount = RULES_ALL.length - RULES.length;
  console.error(
    `\n脱敏守卫：${hits} 处命中（规则 PUBLIC ${RULES.length} + IDENTITY ${identityCount}` +
      ` + ALLOWLIST ${allowlist.length}；豁免文件：${EXEMPT.join(", ")}）。`,
  );
  process.exit(1);
}
console.log(
  `脱敏守卫：通过（0 命中；规则 PUBLIC ${RULES.length} + IDENTITY ${identityRules.length}` +
    ` + ALLOWLIST ${allowlist.length}${filesOnly ? "；仅文件扫描" : "；含未推送提交信息"}）`,
);
