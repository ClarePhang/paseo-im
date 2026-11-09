/** Durable Weixin adapter state: long-poll cursor, seen-message dedupe,
 * per-user context tokens (bound to the login credential), and recent inbound
 * run ids for reply threading. Persisted via server/files.ts atomicWrite. */
import { createHash } from "node:crypto";
import { atomicWrite, readJson, WriteChain } from "../../files.ts";
import { asRecord, nonEmptyString } from "./api.ts";

export interface WeixinContextTokenEntry {
  token: string;
  seq: string | null;
  messageTimeMs: number | null;
  receivedAt: number;
}

export interface WeixinStateDoc {
  version: 1;
  getUpdatesBuf: string;
  seenMessageIds: string[];
  contextTokens: { credentialHash: string; users: Record<string, WeixinContextTokenEntry> } | null;
  inboundRuns: Record<string, { toUserId: string; runId: string }>;
}

const SEEN_CAP = 1_000;
const CONTEXT_TOKEN_CAP = 1_000;
const INBOUND_RUN_CAP = 200;

function seqString(value: unknown): string | null {
  const text = typeof value === "string"
    ? value
    : typeof value === "number" && Number.isSafeInteger(value) ? String(value) : "";
  return /^\d+$/.test(text) ? text : null;
}

function timestampNum(value: unknown): number | null {
  return typeof value === "number" && Number.isFinite(value) ? value : null;
}

function emptyDoc(): WeixinStateDoc {
  return {
    version: 1,
    getUpdatesBuf: "",
    seenMessageIds: [],
    contextTokens: null,
    inboundRuns: {},
  };
}

/** Lenient normalization: corrupt or older files load to what still applies. */
function normalizeState(value: unknown): WeixinStateDoc {
  const record = asRecord(value);
  if (!record || record.version !== 1) return emptyDoc();
  const seenMessageIds = Array.isArray(record.seenMessageIds)
    ? record.seenMessageIds.filter((id): id is string => typeof id === "string").slice(-SEEN_CAP)
    : [];
  const getUpdatesBuf = typeof record.getUpdatesBuf === "string" ? record.getUpdatesBuf : "";

  let contextTokens: WeixinStateDoc["contextTokens"] = null;
  const rawTokens = asRecord(record.contextTokens);
  const credentialHash = rawTokens !== null
    && typeof rawTokens.credentialHash === "string"
    && /^[a-f0-9]{64}$/.test(rawTokens.credentialHash)
    ? rawTokens.credentialHash
    : null;
  if (credentialHash) {
    const users: Record<string, WeixinContextTokenEntry> = {};
    const rawUsers = asRecord(rawTokens?.users);
    if (rawUsers) {
      for (const [userId, entry] of Object.entries(rawUsers).slice(-CONTEXT_TOKEN_CAP)) {
        const parsed = asRecord(entry);
        const token = nonEmptyString(parsed?.token);
        if (!userId || !token) continue;
        users[userId] = {
          token,
          seq: seqString(parsed?.seq),
          messageTimeMs: timestampNum(parsed?.messageTimeMs),
          receivedAt: timestampNum(parsed?.receivedAt) ?? 0,
        };
      }
    }
    contextTokens = { credentialHash, users };
  }

  const inboundRuns: WeixinStateDoc["inboundRuns"] = {};
  const rawRuns = asRecord(record.inboundRuns);
  if (rawRuns) {
    for (const [messageId, entry] of Object.entries(rawRuns).slice(-INBOUND_RUN_CAP)) {
      const parsed = asRecord(entry);
      const toUserId = nonEmptyString(parsed?.toUserId);
      const runId = nonEmptyString(parsed?.runId);
      if (!messageId || !toUserId || !runId) continue;
      inboundRuns[messageId] = { toUserId, runId };
    }
  }

  return { version: 1, getUpdatesBuf, seenMessageIds, contextTokens, inboundRuns };
}

export class WeixinState {
  private readonly path: string;
  private doc: WeixinStateDoc = emptyDoc();
  private chain = new WriteChain();

  constructor(path: string) {
    this.path = path;
  }

  async load(): Promise<void> {
    const loaded = await readJson(this.path, normalizeState);
    if (loaded) this.doc = loaded;
  }

  getUpdatesBuf(): string {
    return this.doc.getUpdatesBuf;
  }

  async setGetUpdatesBuf(value: string): Promise<void> {
    if (value === this.doc.getUpdatesBuf) return;
    this.doc.getUpdatesBuf = value;
    await this.persist();
  }

  hasSeen(messageId: string): boolean {
    return this.doc.seenMessageIds.includes(messageId);
  }

  async markSeen(messageId: string): Promise<void> {
    if (this.hasSeen(messageId)) return;
    this.doc.seenMessageIds.push(messageId);
    if (this.doc.seenMessageIds.length > SEEN_CAP) {
      this.doc.seenMessageIds.splice(0, this.doc.seenMessageIds.length - SEEN_CAP);
    }
    await this.persist();
  }

  /** Context tokens belong to one bot login: a new credential starts fresh. */
  async bindContextTokens(token: string): Promise<void> {
    const credentialHash = createHash("sha256").update(token).digest("hex");
    if (this.doc.contextTokens?.credentialHash === credentialHash) return;
    this.doc.contextTokens = { credentialHash, users: {} };
    await this.persist();
  }

  contextTokenFor(userId: string): string | undefined {
    return this.doc.contextTokens?.users[userId]?.token ?? undefined;
  }

  /** Remember the newest context token per user; late or duplicate batches
   * must not replace a newer conversation capability. */
  async rememberContextToken(entry: {
    userId: string;
    contextToken: string;
    seq?: unknown;
    messageTimeMs?: number | null;
  }): Promise<void> {
    const users = this.doc.contextTokens?.users;
    if (!users) return;
    const previous = users[entry.userId];
    const next: WeixinContextTokenEntry = {
      token: entry.contextToken,
      seq: seqString(entry.seq),
      messageTimeMs: timestampNum(entry.messageTimeMs ?? null),
      receivedAt: Date.now(),
    };
    if (previous) {
      if (previous.seq !== null && next.seq !== null
        && BigInt(next.seq) <= BigInt(previous.seq)) return;
      if (previous.messageTimeMs !== null && next.messageTimeMs !== null
        && next.messageTimeMs < previous.messageTimeMs) return;
    }
    delete users[entry.userId];
    users[entry.userId] = next;
    const keys = Object.keys(users);
    if (keys.length > CONTEXT_TOKEN_CAP) delete users[keys[0]];
    await this.persist();
  }

  async rememberInboundRun(messageId: string, toUserId: string, runId: string): Promise<void> {
    const runs = this.doc.inboundRuns;
    delete runs[messageId];
    runs[messageId] = { toUserId, runId };
    const keys = Object.keys(runs);
    if (keys.length > INBOUND_RUN_CAP) delete runs[keys[0]];
    await this.persist();
  }

  runForReply(toUserId: string, replyToId: string): string | undefined {
    const entry = this.doc.inboundRuns[replyToId];
    return entry && entry.toUserId === toUserId ? entry.runId : undefined;
  }

  /** Wait until every queued state write has landed. */
  async flush(): Promise<void> {
    await this.chain.run(async () => {});
  }

  private async persist(): Promise<void> {
    await this.chain.run(() => atomicWrite(this.path, this.doc));
  }
}
