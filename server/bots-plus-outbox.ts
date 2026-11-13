/** bots-plus-outbox/v1 只读侧（paseo-im）：P2b 第一步通知条目读取 + watermark（已消费 seq）。
 * 契约单一事实源在 paseo-bots-plus/shared/bots-contract.ts；本文件为 paseo-im 侧的
 * zod 复刻（写入方是 bots+，本仓只读）。降级语义：文件缺失/坏 JSON/坏版本
 * = 无新条目 + breadcrumb，不阻塞渠道核心环（与 bots-contract.ts 同口径）。
 * decisions 回写（bots-plus-outbox 第二步）未含在本文件。 */
import { readFile, writeFile, mkdir } from "node:fs/promises";
import { homedir } from "node:os";
import { join } from "node:path";
import { z } from "zod";
import { botsContractDir } from "./bots-contract.ts";

export const OUTBOX_CONTRACT_VERSION = "bots-plus-outbox/v1";

/** 通知 allowlist 字段（与 bots+ 写侧逐字对照；仅元数据——正文/命令文本/工具输出不在此契约）。 */
export const outboxEntryV1 = z
  .object({
    seq: z.number().int().min(1),
    kind: z.enum(["approval-notify", "routine-failure"]),
    botId: z.string(),
    botName: z.string().max(80),
    ref: z.string().max(64).nullable(),
    title: z.string().max(120),
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
