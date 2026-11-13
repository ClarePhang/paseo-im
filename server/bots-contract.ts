/** bots-contract/v1 契约只读侧（paseo-im）：paseo-im-targets.json 加载 + 热载 + 单实例锁。
 * 契约单一事实源在 paseo-bots-plus/shared/bots-contract.ts；本文件为 paseo-im 侧的
 * zod 复刻（写入方是 bots+，本仓只读）。降级语义：桥故障/文件缺失/坏 JSON/坏版本
 * = 空注册表 + breadcrumb 日志，不阻塞核心环（fail-closed 到无 contract target）。 */
import { homedir } from "node:os";
import { basename, dirname, join } from "node:path";
import { watch } from "node:fs";
import { mkdir, open, readFile, unlink, writeFile } from "node:fs/promises";
import { z } from "zod";

export const CONTRACT_VERSION = "bots-contract/v1";

/** F23 默认敏感模式集（与 bots+ DEFAULT_SENSITIVE_PATTERNS 逐字对照，仅供测试
 * 校验两侧一致；运行时 patterns 一律以契约文件携带的为准，本仓不下发规则）。 */
export const DEFAULT_SENSITIVE_PATTERNS: string[] = [
  "sk-[A-Za-z0-9]{20,}",
  "ghp_[A-Za-z0-9]{36}",
  "AKIA[A-Z0-9]{16}",
  "-----BEGIN [A-Z ]*PRIVATE KEY-----",
  "\\b(?:192\\.168|10\\.\\d{1,3}|172\\.(?:1[6-9]|2\\d|3[01]))\\.\\d{1,3}\\.\\d{1,3}\\b",
];

/** bots+ → paseo-im（bot 主聊天 target 注册表；sensitiveCheck 由 F23 开关+默认规则集驱动）。 */
export const paseoImTargetsV1 = z
  .object({
    version: z.literal(CONTRACT_VERSION),
    targets: z
      .array(
        z
          .object({
            id: z.string(),
            agentId: z.string(),
            sensitiveCheck: z.boolean(),
            patterns: z.array(z.string()).default([]),
          })
          .strict(),
      )
      .default([]),
  })
  .strict();
export type PaseoImTargetsV1 = z.output<typeof paseoImTargetsV1>;
export type PaseoImTargetEntry = PaseoImTargetsV1["targets"][number];

export type BreadcrumbLog = (level: "info" | "warn" | "error", message: string) => void;

/** 契约目录：$PASEO_HOME/plugin-data/bots-contract/（home 为空时回退 env/默认 HOME）。 */
export function botsContractDir(home: string = process.env.PASEO_HOME || join(homedir(), ".paseo")): string {
  return join(home || process.env.PASEO_HOME || join(homedir(), ".paseo"), "plugin-data", "bots-contract");
}

export function paseoImTargetsPath(home?: string): string {
  return join(botsContractDir(home), "paseo-im-targets.json");
}

const LOCK_FILE_NAME = ".paseo-im-loader.lock";
const DEFAULT_DEBOUNCE_MS = 500;
const DEFAULT_POLL_MS = 30_000;

export interface WatchHandle {
  close(): void;
}

/** 目录监听 seam：生产用 node:fs.watch，测试注入故障实现验证轮询回退。
 * onError 覆盖创建失败（同步抛出）与运行期 error 事件两种情况。 */
export type WatchImpl = (
  dir: string,
  listener: (event: string, filename: string | null) => void,
  onError: (reason: string) => void,
) => WatchHandle;

function defaultWatchImpl(
  dir: string,
  listener: (event: string, filename: string | null) => void,
  onError: (reason: string) => void,
): WatchHandle {
  const watcher = watch(dir, { persistent: false }, (event, filename) => {
    listener(event, typeof filename === "string" ? filename : null);
  });
  watcher.on("error", (error) => onError((error as Error).message));
  return watcher;
}

function pidAlive(pid: number): boolean {
  try {
    process.kill(pid, 0);
    return true;
  } catch (error) {
    // EPERM 同样说明进程存在（他人所属）；ESRCH 才是不存在。
    return (error as NodeJS.ErrnoException).code === "EPERM";
  }
}

export interface BotsContractLoaderOptions {
  log: BreadcrumbLog;
  /** 契约文件完整路径；缺省按 home/env 推导 paseo-im-targets.json。 */
  file?: string;
  home?: string;
  /** 热载去抖（生产默认 500ms）。 */
  debounceMs?: number;
  /** watch 故障时的轮询间隔（生产默认 30s）。 */
  pollMs?: number;
  watchImpl?: WatchImpl;
}

/** 契约 target 注册表加载器。start() 后持有契约目录锁并挂目录监听（去抖热载）；
 * 监听失败退固定轮询。锁被其他存活进程持有时本实例让位：仅静态加载，不重复热载。 */
export class BotsContractLoader {
  private readonly file: string;
  private readonly log: BreadcrumbLog;
  private readonly debounceMs: number;
  private readonly pollMs: number;
  private readonly watchImpl: WatchImpl;

  private targets: PaseoImTargetEntry[] = [];
  private lastSnapshot: string | null = null;
  private lastProblem: string | null = null;
  private started = false;
  private ownsLock = false;
  private watcher: WatchHandle | null = null;
  private pollTimer: NodeJS.Timeout | undefined;
  private debounceTimer: NodeJS.Timeout | undefined;
  private reloadChain: Promise<void> = Promise.resolve();

  constructor(options: BotsContractLoaderOptions) {
    this.log = options.log;
    this.file = options.file ?? paseoImTargetsPath(options.home);
    this.debounceMs = Math.max(500, options.debounceMs ?? DEFAULT_DEBOUNCE_MS);
    this.pollMs = Math.max(1_000, options.pollMs ?? DEFAULT_POLL_MS);
    this.watchImpl = options.watchImpl ?? defaultWatchImpl;
  }

  /** 当前契约 target 注册表（只读快照；空数组 = 无契约路由/无 F23 检查）。 */
  get all(): readonly PaseoImTargetEntry[] {
    return this.targets;
  }

  get path(): string {
    return this.file;
  }

  /** 本实例是否持有契约目录锁（即是否负责热载）。 */
  get holdsLock(): boolean {
    return this.ownsLock;
  }

  async start(): Promise<void> {
    if (this.started) return;
    this.started = true;
    try {
      this.ownsLock = await this.acquireLock();
    } catch (error) {
      this.ownsLock = false;
      this.log("warn", `[bots-contract] 契约锁初始化失败（${(error as Error).message}），本实例仅静态加载。`);
    }
    await this.reload();
    if (this.ownsLock) this.startWatch();
  }

  /** 释放监听与锁；重复调用安全。 */
  async stop(): Promise<void> {
    clearTimeout(this.debounceTimer);
    this.debounceTimer = undefined;
    clearInterval(this.pollTimer);
    this.pollTimer = undefined;
    if (this.watcher) {
      const watcher = this.watcher;
      this.watcher = null;
      try {
        watcher.close();
      } catch {
        // 关闭失败无需处理：进程退出后句柄随之释放。
      }
    }
    if (this.ownsLock) {
      this.ownsLock = false;
      await unlink(join(dirname(this.file), LOCK_FILE_NAME)).catch(() => {});
    }
  }

  /** 重读契约文件；任何失败都降级为空注册表 + breadcrumb，绝不抛出。 */
  reload(): Promise<void> {
    const next = this.reloadChain.then(() => this.reloadOnce());
    this.reloadChain = next.catch(() => {});
    return next;
  }

  private async reloadOnce(): Promise<void> {
    let raw: string;
    try {
      raw = await readFile(this.file, "utf8");
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code === "ENOENT") {
        this.apply([], "契约文件缺失（等待 bots+ 首次写入）");
        return;
      }
      this.apply([], `契约文件读取失败：${(error as Error).message}`);
      return;
    }
    let parsed: PaseoImTargetsV1;
    try {
      parsed = paseoImTargetsV1.parse(JSON.parse(raw));
    } catch (error) {
      this.apply([], `契约文件无效，已忽略：${(error as Error).message.slice(0, 200)}`);
      return;
    }
    this.apply(parsed.targets, null);
  }

  private apply(next: PaseoImTargetEntry[], problem: string | null): void {
    const snapshot = JSON.stringify(next);
    if (snapshot === this.lastSnapshot && problem === this.lastProblem) return;
    const changed = snapshot !== this.lastSnapshot;
    this.lastSnapshot = snapshot;
    this.lastProblem = problem;
    if (problem) this.log("warn", `[bots-contract] ${problem}；按空 target 注册表处理。`);
    this.targets = next;
    if (changed) this.log("info", `[bots-contract] 已载入 ${next.length} 条契约 target（${basename(this.file)}）。`);
  }

  /** 契约目录锁：写 pid 防重复热载实例。陈旧锁（持有进程已死）接管，
   * 同进程重复启动视为重入。 */
  private async acquireLock(): Promise<boolean> {
    const dir = dirname(this.file);
    const lockPath = join(dir, LOCK_FILE_NAME);
    await mkdir(dir, { recursive: true, mode: 0o700 });
    const write = async (pid: number): Promise<void> => {
      const handle = await open(lockPath, "wx");
      try {
        await handle.writeFile(`${pid}\n`, "utf8");
      } finally {
        await handle.close();
      }
    };
    try {
      await write(process.pid);
      return true;
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error;
    }
    let owner: number | null = null;
    try {
      owner = Number.parseInt((await readFile(lockPath, "utf8")).trim(), 10);
    } catch {
      owner = null;
    }
    if (owner === process.pid) return true;
    if (owner !== null && Number.isFinite(owner) && pidAlive(owner)) {
      this.log("warn", `[bots-contract] 契约热载已由进程 ${owner} 持有（${lockPath}），本实例仅静态加载。`);
      return false;
    }
    // 陈旧锁：接管（写入非原子，双进程竞抢时后写者胜，代价只是重复热载日志）。
    await unlink(lockPath).catch(() => {});
    try {
      await write(process.pid);
      return true;
    } catch (error) {
      this.log("warn", `[bots-contract] 契约锁接管失败（${(error as Error).message}），本实例仅静态加载。`);
      return false;
    }
  }

  private startWatch(): void {
    const dir = dirname(this.file);
    const onEvent = (_event: string, filename: string | null): void => {
      // 只关心契约文件本身；锁文件/临时文件的 churn 忽略。
      if (filename !== null && filename !== basename(this.file)) return;
      this.scheduleReload();
    };
    const onWatchError = (reason: string): void => {
      if (!this.watcher) return;
      this.watcher = null;
      this.fallbackToPoll(`契约目录监听故障（${reason}）`);
    };
    try {
      this.watcher = this.watchImpl(dir, onEvent, onWatchError);
    } catch (error) {
      this.fallbackToPoll(`契约目录监听失败（${(error as Error).message}）`);
    }
  }

  private fallbackToPoll(reason: string): void {
    if (this.pollTimer) return;
    this.log("warn", `[bots-contract] ${reason}，改为 ${Math.round(this.pollMs / 1000)}s 轮询热载。`);
    this.pollTimer = setInterval(() => void this.reload(), this.pollMs);
    this.pollTimer.unref?.();
  }

  private scheduleReload(): void {
    clearTimeout(this.debounceTimer);
    this.debounceTimer = setTimeout(() => {
      this.debounceTimer = undefined;
      void this.reload();
    }, this.debounceMs);
    this.debounceTimer.unref?.();
  }
}
