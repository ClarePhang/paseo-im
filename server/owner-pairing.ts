/** owner 配对（bots+ owner 通道绑定）：im.pairing.start 发码 → 用户在渠道 dm 里发
 * 「配对 <64hex>」→ bridge 拦截裁决 → state.ownerPairing 落盘 → im.pairing.poll 确认。
 * 防线：配对码（明文）只由本机 UI 经 start RPC 获得，state.json 只落 sha256 摘要
 * （nonce hash）；dm 里任何人可发命令，但码不匹配一律拒绝。账本仅进程内存，
 * 5min TTL——重启丢账本=未配对码自然失效，无持久化副作用。 */
import { createHash, randomBytes, randomUUID, timingSafeEqual } from "node:crypto";
import type { ConnectionState, StateStore } from "./state-store.ts";

/** 配对码有效期（start → 用户发命令的窗口）。 */
export const ownerPairingTtlMs = 5 * 60_000;
/** 配对绑定是长期身份（owner 通道），10 年=随连接生命周期而非短期凭据。 */
export const ownerPairingBindingTtlMs = 10 * 365 * 24 * 60 * 60 * 1000;

export interface OwnerPairingAttempt {
  attemptId: string;
  connectionId: string;
  /** sha256(配对码) hex——明文码不落任何存储。 */
  nonceHash: string;
  expiresAt: number;
}

export type OwnerPairingVerify =
  | { outcome: "ignored" }
  | { outcome: "rejected" }
  | { outcome: "paired"; pairing: NonNullable<ConnectionState["ownerPairing"]> };

export type OwnerPairingPoll =
  | { status: "none" }
  | { status: "expired" }
  | { status: "pending" }
  | { status: "ok"; ownerId: string };

/** owner 配对码账本：start 发码、verify 拦截裁决、poll 确认三面共用；
 * 同连接仅保留最新待验配对（重发码作废旧码）。 */
export class OwnerPairingBook {
  private readonly attempts = new Map<string, OwnerPairingAttempt>();

  start(connectionId: string, now: number = Date.now()): OwnerPairingAttempt & { code: string } {
    for (const [attemptId, attempt] of this.attempts) {
      if (attempt.connectionId === connectionId) this.attempts.delete(attemptId);
    }
    const code = randomBytes(32).toString("hex");
    const attempt: OwnerPairingAttempt = {
      attemptId: randomUUID().replace(/-/g, "").slice(0, 24),
      connectionId,
      nonceHash: createHash("sha256").update(code).digest("hex"),
      expiresAt: now + ownerPairingTtlMs,
    };
    this.attempts.set(attempt.attemptId, attempt);
    return { ...attempt, code };
  }

  attempt(attemptId: string): OwnerPairingAttempt | undefined {
    return this.attempts.get(attemptId);
  }

  /** 拦截裁决：无 pending 配对=ignored（不拦，走正常流程）；错码/过期/改绑=rejected；
   * 命中=paired（调用方负责写 ownerPairing）。先绑优先：同码后到者不得改绑他人通道。 */
  verify(input: {
    connectionId: string;
    senderId: string;
    chatId: string;
    code: string;
    /** state 里已有的绑定（本 attempt 之前落盘的）：命中码时仅允许原绑双方重复确认。 */
    bound?: { ownerId: string; ownerChatId: string } | null;
    now?: number;
  }): OwnerPairingVerify {
    const attempt = [...this.attempts.values()].find((candidate) => candidate.connectionId === input.connectionId);
    if (!attempt) return { outcome: "ignored" };
    const now = input.now ?? Date.now();
    if (now > attempt.expiresAt) {
      this.attempts.delete(attempt.attemptId);
      return { outcome: "rejected" };
    }
    const computed = createHash("sha256").update(input.code).digest();
    const expected = Buffer.from(attempt.nonceHash, "hex");
    if (expected.length !== computed.length || !timingSafeEqual(computed, expected)) {
      return { outcome: "rejected" };
    }
    if (input.bound && (input.bound.ownerId !== input.senderId || input.bound.ownerChatId !== input.chatId)) {
      return { outcome: "rejected" };
    }
    return {
      outcome: "paired",
      pairing: {
        hash: attempt.nonceHash,
        expiresAt: now + ownerPairingBindingTtlMs,
        ownerId: input.senderId,
        ownerChatId: input.chatId,
      },
    };
  }

  /** UI 确认轮询：绑定落盘（hash=本 attempt 的 nonce）才算 ok 并消费 attempt；
   * 命中前反复 poll=pending，attempt 不存在=none，超时=expired。 */
  poll(attemptId: string, pairing: ConnectionState["ownerPairing"], now: number = Date.now()): OwnerPairingPoll {
    const attempt = this.attempts.get(attemptId);
    if (!attempt) return { status: "none" };
    if (now > attempt.expiresAt) {
      this.attempts.delete(attemptId);
      return { status: "expired" };
    }
    if (pairing && pairing.hash === attempt.nonceHash && pairing.ownerId) {
      this.attempts.delete(attemptId);
      return { status: "ok", ownerId: pairing.ownerId };
    }
    return { status: "pending" };
  }

  clear(): void {
    this.attempts.clear();
  }
}

/** bridge 依赖注入的生产实现：裁决 + 命中写 ownerPairing（StateStore 写链串行落盘）。 */
export async function applyOwnerPairing(input: {
  book: OwnerPairingBook;
  state: Pick<StateStore, "get" | "mutate">;
  connectionId: string;
  senderId: string;
  chatId: string;
  code: string;
  now?: number;
}): Promise<"ignored" | "paired" | "rejected"> {
  const bound = input.state.get().ownerPairing;
  const outcome = input.book.verify({
    connectionId: input.connectionId,
    senderId: input.senderId,
    chatId: input.chatId,
    code: input.code,
    bound: bound?.ownerId && bound.ownerChatId ? { ownerId: bound.ownerId, ownerChatId: bound.ownerChatId } : null,
    now: input.now,
  });
  if (outcome.outcome !== "paired") return outcome.outcome;
  await input.state.mutate((draft) => {
    draft.ownerPairing = outcome.pairing;
  });
  return "paired";
}

/** 「配对 <64hex>」dm 命令解析（bridge 拦截面；小写 hex，与 start 下发逐字匹配）。 */
export function parsePairingCommand(text: string): string | null {
  return /^配对\s+([a-f0-9]{64})$/.exec(text)?.[1] ?? null;
}
