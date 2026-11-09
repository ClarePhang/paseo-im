/** Inbound WhatsApp message normalization.
 * Protocol facts ported from xmanrui/dsh-im (MIT). */

import { baileysSync, type Baileys } from "./baileys.ts";
import type { proto, WAMessage, WAMessageContent } from "@whiskeysockets/baileys";
import type { NormalizedMessage } from "../../../shared/model.ts";

function requireBaileys(): Baileys {
  const b = baileysSync();
  if (!b) throw new Error("Baileys 模块尚未装载（适配器未启动）。");
  return b;
}

/** Identity of the linked account: PN (@s.whatsapp.net) and LID (@lid) are
 * separate identities; both must be considered when matching mentions. */
export type AccountJids = readonly string[];

export function sameJidUser(left: string | undefined, right: string | undefined): boolean {
  if (!left || !right) return false;
  const b = baileysSync();
  if (b) {
    try {
      return b.areJidsSameUser(left, right) === true;
    } catch {
      return false;
    }
  }
  // String fallback before the module is loaded (never hit on live paths:
  // adapter start awaits baileys() before any ingest).
  const bare = (value: string) => value.split("@")[0]?.split(".")[0] ?? value;
  return bare(left) === bare(right);
}

export function matchesAccount(accountJids: AccountJids, jid: string | undefined): boolean {
  return typeof jid === "string" && jid !== "" && accountJids.some((account) => sameJidUser(jid, account));
}

/** Baileys message timestamps are epoch seconds (Long or number). */
export function messageTimestampMs(value: unknown): number {
  let seconds: number = Number.NaN;
  if (typeof value === "number") seconds = value;
  else if (value && typeof value === "object" && "toNumber" in value && typeof value.toNumber === "function") {
    seconds = value.toNumber();
  }
  if (!Number.isFinite(seconds) || seconds <= 0) return Date.now();
  return seconds < 1e12 ? Math.round(seconds * 1000) : Math.round(seconds);
}

/** protobufjs keeps u64 fields as Long or number depending on the runtime. */
function declaredByteLength(value: unknown): number | undefined {
  if (typeof value === "number" && Number.isFinite(value) && value > 0) return value;
  if (value && typeof value === "object" && "toNumber" in value && typeof value.toNumber === "function") {
    const converted = value.toNumber();
    return Number.isFinite(converted) && converted > 0 ? converted : undefined;
  }
  return undefined;
}

function contentText(content: WAMessageContent | undefined): string {
  const value =
    content?.conversation ??
    content?.extendedTextMessage?.text ??
    content?.imageMessage?.caption ??
    content?.videoMessage?.caption ??
    content?.documentMessage?.caption;
  return typeof value === "string" ? value : "";
}

function contentContext(content: WAMessageContent | undefined): proto.IContextInfo | undefined {
  return (
    content?.extendedTextMessage?.contextInfo ??
    content?.imageMessage?.contextInfo ??
    content?.videoMessage?.contextInfo ??
    content?.documentMessage?.contextInfo ??
    undefined
  );
}

function optionalString(value: unknown): string | undefined {
  return typeof value === "string" && value ? value : undefined;
}

export interface PendingMedia {
  kind: "image" | "file";
  mimetype?: string;
  name?: string;
  /** Sender-declared byte size (documentMessage.fileLength); advisory only. */
  size?: number;
}

export interface NormalizeOptions {
  connectionId: string;
  accountJids: AccountJids;
  /** Whether a platform message id belongs to a message this bot sent. */
  isOwnMessage: (platformId: string) => boolean;
}

export interface NormalizedWhatsAppInbound {
  /** Complete NormalizedMessage minus downloaded media. */
  base: Omit<NormalizedMessage, "media">;
  pendingMedia: PendingMedia[];
}

/** Map one real Baileys inbound message to the channel-neutral shape.
 * Returns null for bot echoes, broadcasts, newsletters and empty noise. */
export function normalizeWhatsappMessage(
  message: WAMessage,
  options: NormalizeOptions,
): NormalizedWhatsAppInbound | null {
  const key = message.key;
  const remoteJid = typeof key?.remoteJid === "string" ? key.remoteJid : "";
  const platformId = typeof key?.id === "string" ? key.id : "";
  if (!remoteJid || !platformId) return null;
  if (remoteJid === "status@broadcast" || remoteJid.endsWith("@newsletter")) return null;
  if (options.accountJids.length === 0) return null;

  const group = remoteJid.endsWith("@g.us");
  const fromMe = key.fromMe === true;
  const alternate = typeof key.remoteJidAlt === "string" ? key.remoteJidAlt : undefined;
  const selfChat = fromMe && !group && (matchesAccount(options.accountJids, remoteJid) || matchesAccount(options.accountJids, alternate));
  // Bot's own outbound messages (non self-chat) are echoes, never triggers.
  if (fromMe && !selfChat && !group) return null;

  let senderId: unknown = fromMe ? options.accountJids[0] : group ? key.participant : remoteJid;
  // 群聊参与者越来越多地以 LID 形态推送，而访问策略（operators）按用户可读的
  // PN 形态录入；平台提供 participantAlt（同一参与者的对侧形态）时归一到 PN，
  // 否则保持原样（策略匹配退化为精确相等，无法归一就不猜）。
  if (
    group &&
    !fromMe &&
    typeof senderId === "string" &&
    senderId.endsWith("@lid") &&
    typeof key.participantAlt === "string" &&
    key.participantAlt.endsWith("@s.whatsapp.net")
  ) {
    senderId = key.participantAlt;
  }
  if (typeof senderId !== "string" || !senderId) return null;

  const content = requireBaileys().normalizeMessageContent(message.message ?? undefined);
  const context = contentContext(content);
  const text = contentText(content);

  const mention = Array.isArray(context?.mentionedJid) && context.mentionedJid.some((jid) => matchesAccount(options.accountJids, jid ?? undefined));
  const stanzaId = optionalString(context?.stanzaId);
  // Only replies quoting a message this bot actually sent count as triggers.
  const replyToBot = matchesAccount(options.accountJids, context?.participant ?? undefined) && stanzaId !== undefined && options.isOwnMessage(stanzaId);

  const pendingMedia: PendingMedia[] = [];
  if (content?.imageMessage && typeof content.imageMessage === "object") {
    pendingMedia.push({ kind: "image", mimetype: optionalString(content.imageMessage.mimetype) });
  }
  if (content?.documentMessage && typeof content.documentMessage === "object") {
    const mimetype = optionalString(content.documentMessage.mimetype);
    const size = declaredByteLength(content.documentMessage.fileLength);
    pendingMedia.push({
      kind: mimetype && mimetype.startsWith("image/") ? "image" : "file",
      mimetype,
      name: optionalString(content.documentMessage.fileName),
      // size 缺省时保持字段缺席：pendingMedia 会被测试按精确形状断言。
      ...(size !== undefined ? { size } : {}),
    });
  }

  const quoted = context?.quotedMessage ? requireBaileys().normalizeMessageContent(context.quotedMessage) : undefined;
  const replyTo = stanzaId || quoted
    ? {
        id: stanzaId,
        text: contentText(quoted) || undefined,
        sender: optionalString(context?.participant),
      }
    : undefined;

  const base: Omit<NormalizedMessage, "media"> = {
    channel: "whatsapp",
    connectionId: options.connectionId,
    kind: group ? "group" : "dm",
    chatId: remoteJid,
    senderId,
    senderName: optionalString(message.pushName?.trim().slice(0, 100)),
    messageId: `${remoteJid}:${platformId}`,
    timestamp: messageTimestampMs(message.messageTimestamp),
    text: text || undefined,
    replyTo: replyTo && (replyTo.id || replyTo.text || replyTo.sender) ? replyTo : undefined,
    trigger: {
      mention: mention === true,
      replyToBot,
      command: /^\/[A-Za-z]/.test(text),
    },
  };
  return { base, pendingMedia };
}
