/** Durable WhatsApp adapter state: inbound dedupe ring (with reply keys) and
 * the outbound own-message ring used for edit/reply matching. Persisted as
 * whatsapp-state.json inside the connection dataDir. */

import { join } from "node:path";
import { z } from "zod";
import { atomicWrite, readJson, WriteChain } from "../../files.ts";

export const WHATSAPP_STATE_FILE = "whatsapp-state.json";

const RING_CAP = 1000;
const ID_MAX = 300;

export const messageKeySchema = z.object({
  remoteJid: z.string().min(1).max(ID_MAX),
  fromMe: z.boolean(),
  id: z.string().min(1).max(ID_MAX),
  participant: z.string().max(ID_MAX).optional(),
});
export type MessageKeyRef = z.output<typeof messageKeySchema>;

const seenSchema = z.object({
  id: z.string().min(1).max(400),
  key: messageKeySchema.nullable().default(null),
});
export type SeenRecord = z.output<typeof seenSchema>;

const ownSchema = z.object({
  id: z.string().min(1).max(ID_MAX),
  jid: z.string().min(1).max(ID_MAX),
  key: messageKeySchema.nullable().default(null),
});
export type OwnRecord = z.output<typeof ownSchema>;

const stateSchema = z.object({
  seen: z.array(seenSchema).max(RING_CAP).default([]),
  own: z.array(ownSchema).max(RING_CAP).default([]),
});
export type WhatsappState = z.output<typeof stateSchema>;

/** Rings are rotate-latest arrays; lookups stay O(n) which is fine at this cap. */
export class WhatsappStateStore {
  private state: WhatsappState = stateSchema.parse({});
  private readonly chain = new WriteChain();
  readonly path: string;
  private readonly report: ((error: Error) => void) | undefined;

  constructor(dataDir: string, report?: (error: Error) => void) {
    this.path = join(dataDir, WHATSAPP_STATE_FILE);
    this.report = report;
  }

  async load(): Promise<void> {
    this.state = (await readJson(this.path, stateSchema.parse)) ?? stateSchema.parse({});
  }

  /** Fire-and-forget persistence; write errors surface via the report hook. */
  persist(): void {
    const snapshot = this.state;
    void this.chain
      .run(() => atomicWrite(this.path, snapshot))
      .catch((error) => this.report?.(error instanceof Error ? error : new Error(String(error))));
  }

  async flush(): Promise<void> {
    const snapshot = this.state;
    await this.chain.run(() => atomicWrite(this.path, snapshot));
  }

  hasSeen(dedupeId: string): boolean {
    return this.state.seen.some((entry) => entry.id === dedupeId);
  }

  markSeen(dedupeId: string, key: MessageKeyRef | null): void {
    this.state.seen = [...this.state.seen.filter((entry) => entry.id !== dedupeId), { id: dedupeId, key }];
    if (this.state.seen.length > RING_CAP) this.state.seen = this.state.seen.slice(-RING_CAP);
    this.persist();
  }

  /** Full Baileys key of an inbound message, for readMessages. */
  seenKey(dedupeId: string): MessageKeyRef | undefined {
    return this.state.seen.find((entry) => entry.id === dedupeId)?.key ?? undefined;
  }

  isOwn(platformId: string): boolean {
    return this.state.own.some((entry) => entry.id === platformId);
  }

  /** Reserve an outbound id BEFORE dispatch so a server-side echo can never
   * re-enter the bridge as a user message. */
  rememberOwn(platformId: string, jid: string): void {
    if (this.isOwn(platformId)) return;
    this.state.own = [...this.state.own, { id: platformId, jid, key: null }];
    if (this.state.own.length > RING_CAP) this.state.own = this.state.own.slice(-RING_CAP);
    this.persist();
  }

  attachOwnKey(platformId: string, key: Partial<MessageKeyRef> | undefined | null): void {
    if (!key || typeof key.id !== "string" || !key.id) return;
    const entry = this.state.own.find((candidate) => candidate.id === platformId);
    const ref: MessageKeyRef = {
      remoteJid: typeof key.remoteJid === "string" && key.remoteJid ? key.remoteJid : entry?.jid ?? "",
      fromMe: key.fromMe === true,
      id: key.id,
      participant: typeof key.participant === "string" && key.participant ? key.participant : undefined,
    };
    if (!ref.remoteJid) return;
    if (entry) entry.key = ref;
    else this.rememberOwn(platformId, ref.remoteJid);
    this.persist();
  }

  ownKey(platformId: string): OwnRecord | undefined {
    return this.state.own.find((entry) => entry.id === platformId);
  }
}
