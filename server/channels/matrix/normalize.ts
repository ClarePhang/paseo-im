/** Pure Matrix inbound event projection helpers. Protocol facts ported from
 * xmanrui/dsh-im (MIT). */
import type { NormalizedMedia } from "../../../shared/model.ts";
import { isMatrixRoomId, isMatrixUserId } from "./api.ts";

/** Runtime guard for untrusted JSON objects; reads stay `unknown` afterwards. */
export function asRecord(value: unknown): Record<string, unknown> | null {
  return value && typeof value === "object" && !Array.isArray(value) ? (value as Record<string, unknown>) : null;
}

const MEDIA_MSGTYPES: Record<string, true> = {
  "m.image": true,
  "m.file": true,
  "m.audio": true,
  "m.video": true,
};

export interface NormalizeInput {
  event: Record<string, unknown>;
  roomId: string;
  botUserId: string;
  isDirect: boolean;
  connectionId: string;
  now: () => number;
}

export interface NormalizeOutcome {
  drop?: "self" | "type" | "edit" | "notice" | "malformed" | "noise" | "encrypted";
  message?: {
    kind: "dm" | "group";
    chatId: string;
    threadId?: string;
    senderId: string;
    messageId: string;
    timestamp: number;
    text?: string;
    media?: NormalizedMedia[];
    replyTo?: { id?: string; text?: string; sender?: string };
    trigger: { mention: boolean; replyToBot: boolean; command: boolean };
    repliedEventId?: string;
  };
  download?: { mxc: string; kind: "image" | "file"; name?: string; mime?: string };
}

function cleanEventId(value: unknown): string | null {
  return typeof value === "string" && value.startsWith("$") && value.trim().length > 1 ? value.trim() : null;
}

function lower(value: unknown): string {
  return typeof value === "string" ? value.trim().toLowerCase() : "";
}

function relatesOf(content: Record<string, unknown>): Record<string, unknown> | null {
  return asRecord(content["m.relates_to"]);
}

/** MSC3952 `m.mentions.user_ids`, then the pill link in formatted_body, then a
 * full MXID in the body. */
export function detectMention(content: Record<string, unknown>, botUserId: string): boolean {
  const own = lower(botUserId);
  if (!own) return false;
  const mentionRecord = asRecord(content["m.mentions"]);
  const userIds = mentionRecord?.user_ids;
  if (Array.isArray(userIds) && userIds.some((candidate) => lower(candidate) === own)) return true;
  const formatted = typeof content.formatted_body === "string" ? content.formatted_body : "";
  if (formatted.toLowerCase().includes(`matrix.to/#/${own}`)) return true;
  const body = typeof content.body === "string" ? content.body : "";
  return body.toLowerCase().includes(own);
}

/** Remove bot-directed mention markup so the prompt text stays clean: pill
 * forms, bare `<@user:server>` and full `@user:server` tokens. */
export function stripMention(body: string, botUserId: string): string {
  const own = botUserId.trim().toLowerCase();
  if (!own) return body.trim();
  const escaped = own.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  const pill = new RegExp(`\\[\\s*<${escaped}>\\s*\\]\\([^)]*\\)`, "gi");
  return body
    .replace(pill, "")
    .replaceAll(new RegExp(`<${escaped}>`, "gi"), "")
    .replaceAll(new RegExp(escaped, "gi"), "")
    .trim();
}

/** The target of a genuine reply relation; `is_falling_back` marks the
 * thread-membership statement clients attach automatically, never a human reply. */
export function replyEventIdOf(content: Record<string, unknown>): string | null {
  const relates = relatesOf(content);
  const hasFallback = (holder: unknown): boolean => asRecord(holder)?.is_falling_back === true;
  if (hasFallback(relates) || hasFallback(content["m.in_reply_to"])) return null;
  const replyTarget = (holder: unknown): string | null => cleanEventId(asRecord(holder)?.event_id);
  if (relates?.rel_type === "m.reply") {
    const target = cleanEventId(relates.event_id);
    if (target) return target;
  }
  return replyTarget(content["m.in_reply_to"]) ?? replyTarget(relates?.["m.in_reply_to"]);
}

export function threadEventIdOf(content: Record<string, unknown>): string | null {
  const relates = relatesOf(content);
  if (!relates) return null;
  const chain = Array.isArray(relates.chain) ? relates.chain : [];
  const threaded = chain.map((entry) => asRecord(entry)).find((entry) => entry?.rel_type === "m.thread");
  if (threaded) return cleanEventId(threaded.event_id);
  return relates.rel_type === "m.thread" ? cleanEventId(relates.event_id) : null;
}

export function isEditEvent(content: Record<string, unknown>): boolean {
  return relatesOf(content)?.rel_type === "m.replace";
}

/** Project one timeline event; media bytes are fetched by the adapter so this
 * stays transport-free. */
export function projectEvent(input: NormalizeInput): NormalizeOutcome {
  const { event, roomId, botUserId } = input;
  const senderId = typeof event.sender === "string" ? event.sender.trim() : "";
  const messageId = typeof event.event_id === "string" ? event.event_id.trim() : "";
  if (!isMatrixRoomId(roomId) || !messageId || !isMatrixUserId(senderId)) return { drop: "malformed" };
  if (senderId.toLowerCase() === botUserId.toLowerCase()) return { drop: "self" };
  if (event.type === "m.room.encrypted") return { drop: "encrypted" };
  if (event.type !== "m.room.message") return { drop: "type" };
  const content = asRecord(event.content) ?? {};
  if (isEditEvent(content)) return { drop: "edit" };
  const msgtype = typeof content.msgtype === "string" ? content.msgtype : "m.text";
  if (msgtype === "m.notice") return { drop: "notice" };

  const rawBody = typeof content.body === "string" ? content.body : "";
  const isMedia = MEDIA_MSGTYPES[msgtype] === true;
  let media: NormalizedMedia[] | undefined;
  let download: NormalizeOutcome["download"];
  if (isMedia && typeof content.url === "string") {
    const kind = msgtype === "m.image" ? "image" : "file";
    const fileName = typeof content.filename === "string" ? content.filename : "";
    const name = fileName || rawBody || undefined;
    const infoMime = asRecord(content.info)?.mimetype;
    const mime = typeof infoMime === "string" ? infoMime : undefined;
    download = { mxc: content.url, kind, name, mime };
    media = [{ kind, path: "", mime, name }];
  } else if (isMedia) {
    return { drop: "noise" };
  }

  const trimmed = rawBody.trim();
  if (!trimmed && !media) return { drop: "noise" };
  const mention = detectMention(content, botUserId);
  const text = mention && !isMedia ? stripMention(rawBody, botUserId) : trimmed || undefined;
  const threadId = threadEventIdOf(content) ?? undefined;
  const repliedEventId = replyEventIdOf(content) ?? undefined;
  const ts = Number(event.origin_server_ts);
  return {
    message: {
      kind: input.isDirect ? "dm" : "group",
      chatId: roomId,
      threadId,
      senderId,
      messageId,
      timestamp: Number.isSafeInteger(ts) && ts > 0 ? (ts < 1e12 ? ts * 1000 : ts) : input.now(),
      text: text && text.trim() ? text : undefined,
      media,
      replyTo: repliedEventId ? { id: repliedEventId } : undefined,
      trigger: { mention: input.isDirect || mention, replyToBot: false, command: /^\/\S/.test(text ?? "") },
      repliedEventId,
    },
    download,
  };
}
