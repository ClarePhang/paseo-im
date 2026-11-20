/** bots-plus-outbox/v1 只读侧（paseo-im）：P2b 第一步通知条目读取 + watermark（已消费 seq）。
 * 契约单一事实源在 paseo-bots-plus/shared/bots-contract.ts；本文件为 paseo-im 侧的
 * zod 复刻（写入方是 bots+，本仓只读）。降级语义：文件缺失/坏 JSON/坏版本
 * = 无新条目 + breadcrumb，不阻塞渠道核心环（与 bots-contract.ts 同口径）。
 * decisions 回写（bots-plus-outbox 第二步）未含在本文件。 */
import { readFile, writeFile, mkdir } from "node:fs/promises";
import { homedir } from "node:os";
import { join } from "node:path";
import { z } from "zod";
import { CHANNEL_IDS } from "../shared/model.ts";
import { botsContractDir } from "./bots-contract.ts";

export const OUTBOX_CONTRACT_VERSION = "bots-plus-outbox/v1";

/** 通知 allowlist 字段（与 bots+ 写侧逐字对照；仅元数据——正文/命令文本/工具输出不在此契约）。 */
export const outboxEntryV1 = z
  .object({
    seq: z.number().int().min(1),
    kind: z.enum(["approval-notify", "routine-failure", "decision-receipt", "permission-notify"]),
    botId: z.string(),
    botName: z.string().max(80),
    ref: z.string().max(64).nullable(),
    title: z.string().max(120),
    /** permission-notify 专用：挂起工具调用的 agent id。 */
    agentId: z.string().max(64).nullish(),
    routineId: z.string().max(128).nullable(),
    failures: z.number().int().min(0).nullable(),
    expiresAt: z.string().nullable(),
    ts: z.string(),
  })
  .strict();

export const botsPlusOutboxV1 = z
  .object({
    version: z.literal(OUTBOX_CONTRACT_VERSION),
    seq: z.number().int().min(0),
    entries: z.array(outboxEntryV1),
  })
  .strict();

export type OutboxEntry = z.output<typeof outboxEntryV1>;

export function outboxPath(home: string = process.env.PASEO_HOME || join(homedir(), ".paseo")): string {
  return join(botsContractDir(home), "bots-plus-outbox.json");
}

function watermarkPath(home: string): string {
  return join(botsContractDir(home), ".paseo-im-outbox-seq");
}

/** 已消费 watermark（持久化；缺文件/坏内容=0 从头重放——通知幂等靠文本，重放无害）。 */
export async function readWatermark(home: string = process.env.PASEO_HOME || join(homedir(), ".paseo")): Promise<number> {
  try {
    const raw = await readFile(watermarkPath(home), "utf8");
    const n = Number.parseInt(raw.trim(), 10);
    return Number.isFinite(n) && n >= 0 ? n : 0;
  } catch {
    return 0;
  }
}

export async function writeWatermark(seq: number, home: string = process.env.PASEO_HOME || join(homedir(), ".paseo")): Promise<void> {
  const file = watermarkPath(home);
  await mkdir(join(file, ".."), { recursive: true });
  await writeFile(file, String(seq), "utf8");
}

/** 读出 seq > lastSeq 的新条目（契约顺序）；文件缺失/坏 JSON/坏版本 → []（降级不阻塞）。 */
export async function readNewEntries(lastSeq: number, home: string = process.env.PASEO_HOME || join(homedir(), ".paseo")): Promise<OutboxEntry[]> {
  try {
    const raw = await readFile(outboxPath(home), "utf8");
    const doc = botsPlusOutboxV1.parse(JSON.parse(raw));
    return doc.entries.filter((e) => e.seq > lastSeq);
  } catch {
    return [];
  }
}

// ── P2b 第二步：decisions 回写（paseo-im → bots+；审批命令解析后落此文件，bots+ 读方代批）──

export const DECISIONS_CONTRACT_VERSION = "paseo-im-decisions/v1";
/** size 上限（design §3.5）：超限拒写新条目并自修剪最旧。 */
export const DECISIONS_MAX_BYTES = 32 * 1024;

export const decisionEntryV1 = z
  .object({
    decisionId: z.string().min(8).max(64),
    ref: z.string().min(4).max(64),
    behavior: z.enum(["allow", "deny"]),
    // channel 写方更严为有意（im 渠道 id 枚举白名单；bots+ 契约侧 shared/bots-contract.ts 为 z.string().max(32)），非逐字对照。
    channel: z.enum(CHANNEL_IDS),
    channelUser: z.string().max(32),
    ts: z.string(),
  })
  .strict();

export const decisionsFileV1 = z
  .object({
    version: z.literal(DECISIONS_CONTRACT_VERSION),
    watermark: z.number().int().min(0),
    decisions: z.array(decisionEntryV1),
  })
  .strict();

export type DecisionEntry = z.output<typeof decisionEntryV1>;

export function decisionsPath(home: string = process.env.PASEO_HOME || join(homedir(), ".paseo")): string {
  return join(botsContractDir(home), "paseo-im-decisions.json");
}

/** 审批命令形态（owner 私聊回复通知：「批 <ref>」/「拒 <ref>」；ref=card- 短码或 daemon requestId（如 omp:permission:<ns>:<seq>），字母数字开头，可含冒号/下划线/连字符，4-64 位）。 */
export function parseDecisionCommand(text: string): { ref: string; behavior: "allow" | "deny" } | null {
  const m = /^(批|拒)\s+([A-Za-z0-9][A-Za-z0-9:_-]{3,63})$/.exec(text.trim());
  if (!m) return null;
  return { behavior: m[1] === "批" ? "allow" : "deny", ref: m[2]! };
}

/** 决策落盘（0600；decisions 文件 watermark v1 未启用——processed 集合+ref 防重+already-decided 三重去重兜底；写方只追加+32KiB 自修剪）。 */
export async function appendDecision(
  home: string | undefined,
  input: { ref: string; behavior: "allow" | "deny"; channel: z.input<typeof decisionEntryV1>["channel"]; channelUser: string },
): Promise<DecisionEntry> {
  const file = decisionsPath(home);
  let doc: { version: string; watermark: number; decisions: DecisionEntry[] } = { version: DECISIONS_CONTRACT_VERSION, watermark: 0, decisions: [] };
  try {
    const raw = await readFile(file, "utf8");
    doc = decisionsFileV1.parse(JSON.parse(raw));
  } catch {
    // 缺文件/坏内容 → 从空开始（写方容错；watermark v1 未启用，去重兜底=processed 集合+ref 防重+already-decided）
  }
  const decisionId = `${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;
  const entry: DecisionEntry = { decisionId, ref: input.ref, behavior: input.behavior, channel: input.channel, channelUser: input.channelUser, ts: new Date().toISOString() };
  let decisions = [...doc.decisions, entry];
  // 32KiB 自修剪：最旧先出，直至含新条目 ≤ 上限（测量与写出必须同一序列化形态——
  // 缩进开销会让「无缩进测量 + 缩进写出」的组合令写盘超出上限，全量审查 P3-4）
  const serialize = (decisions: DecisionEntry[]) =>
    JSON.stringify({ version: DECISIONS_CONTRACT_VERSION, watermark: doc.watermark, decisions }, null, 2);
  while (serialize(decisions).length > DECISIONS_MAX_BYTES && decisions.length > 1) {
    decisions = decisions.slice(1);
  }
  const payload = serialize(decisions);
  const tmp = `${file}.tmp-${process.pid}-${Date.now()}`;
  await mkdir(join(file, ".."), { recursive: true });
  await writeFile(tmp, payload, { encoding: "utf8", mode: 0o600 });
  await (await import("node:fs/promises")).rename(tmp, file);
  return entry;
}
