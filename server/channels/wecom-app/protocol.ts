/** WeCom self-built application protocol layer: signature/encryption envelope
 * handling, callback payload parsing and passive-reply encoding.
 * Protocol facts ported from xmanrui/dsh-im (MIT). */
import { timingSafeEqual } from "node:crypto";

export const WECOM_APP_DEFAULT_API_BASE = "https://qyapi.weixin.qq.com";
export const WECOM_APP_TEXT_MAX_BYTES = 2048;

/** Minimal shape of @wecom/aibot-node-sdk's WecomCrypto used by this channel. */
export interface WecomCryptoLike {
  computeSignature(timestamp: string, nonce: string, encrypt: string): string;
  verifySignature(signature: string, timestamp: string, nonce: string, encrypt: string): boolean;
  decrypt(encryptText: string): string;
  encrypt(plainText: string, timestamp: string, nonce: string): { encrypt: string; signature: string };
}

export class WecomAppApiError extends Error {
  readonly code: string;
  readonly providerCode?: string;
  /** Transient failures are safe to retry with backoff; credential/config
   * errors are not. */
  readonly transient: boolean;

  constructor(
    code: string,
    message: string,
    options: { providerCode?: string; transient?: boolean } = {},
  ) {
    super(message);
    this.name = "WecomAppApiError";
    this.code = code;
    this.providerCode = options.providerCode;
    this.transient = options.transient ?? false;
  }
}

export function cleanString(value: unknown): string | null {
  return typeof value === "string" && value.trim() ? value.trim() : null;
}

/** Canonical object guard for this channel's network payload parsing. */
export function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

export function normalizeApiBaseUrl(value: unknown): string {
  const raw = cleanString(value);
  if (!raw) return WECOM_APP_DEFAULT_API_BASE;
  if (!/^https?:\/\//iu.test(raw)) {
    throw new WecomAppApiError("invalid-api-base", "API 代理地址必须是 http(s) 地址");
  }
  return raw.replace(/\/+$/u, "");
}

export function byteLength(value: string): number {
  return Buffer.byteLength(value, "utf8");
}

/** WeCom text messages accept at most 2048 UTF-8 bytes per message; split on
 * code-point boundaries so multi-byte characters are never cut in half. */
export function splitUtf8ByBytes(text: string, maxBytes = WECOM_APP_TEXT_MAX_BYTES): string[] {
  const value = typeof text === "string" ? text : "";
  if (value === "") return [""];
  const chunks: string[] = [];
  let current = "";
  let currentBytes = 0;
  for (const char of value) {
    const size = byteLength(char);
    if (currentBytes + size > maxBytes && current.length > 0) {
      chunks.push(current);
      current = char;
      currentBytes = size;
      continue;
    }
    current += char;
    currentBytes += size;
  }
  if (current.length > 0) chunks.push(current);
  return chunks;
}

/** Parses the two XML shapes WeCom uses (CDATA-wrapped and plain values). */
export function parseWecomXmlBody(xml: string): Record<string, string> {
  const result: Record<string, string> = {};
  const source = typeof xml === "string" ? xml : "";
  const cdataPattern = /<([\w:-]+)><!\[CDATA\[([\s\S]*?)\]\]><\/\1>/gu;
  let match: RegExpExecArray | null;
  while ((match = cdataPattern.exec(source)) !== null) {
    result[match[1]] = match[2];
  }
  const simplePattern = /<([\w:-]+)>([^<>]*)<\/\1>/gu;
  while ((match = simplePattern.exec(source)) !== null) {
    if (result[match[1]] === undefined) result[match[1]] = match[2];
  }
  return result;
}

export function isXmlFormat(raw: string): boolean {
  const trimmed = typeof raw === "string" ? raw.trim() : "";
  return trimmed.startsWith("<") && trimmed.endsWith(">");
}

/** Normalized inbound callback message; JSON field names from the WeCom
 * documentation are canonical, XML callbacks are mapped onto them. */
export interface WecomAppInboundMessage {
  msgid?: string;
  msgtype: string;
  createTime?: string;
  agentId?: string;
  fromUserId?: string;
  fromName?: string;
  toUserName?: string;
  text?: string;
  imageUrl?: string;
  imageMediaId?: string;
  recognition?: string;
  event?: string;
  chatid?: string;
  chattype?: string;
  quote?: { msgid?: string; text?: string; sender?: string };
}

function quoteFrom(value: unknown): WecomAppInboundMessage["quote"] | undefined {
  if (!isRecord(value)) return undefined;
  const msgid = cleanString(value.msgid) ?? cleanString(value.message_id) ?? undefined;
  const text = cleanString(value.content) ?? (isRecord(value.text) ? cleanString(value.text.content) : null) ?? undefined;
  const sender = cleanString(value.userid) ?? cleanString(value.from) ?? undefined;
  if (msgid === undefined && text === undefined && sender === undefined) return undefined;
  return { msgid, text, sender };
}

/** Normalizes the decrypted plaintext into the internal inbound message shape.
 * Accepts both the JSON and XML callback formats. */
export function parseWecomPlainMessage(raw: string): WecomAppInboundMessage | null {
  const source = typeof raw === "string" ? raw.trim() : "";
  if (!source) return null;
  if (isXmlFormat(source)) {
    const xml = parseWecomXmlBody(source);
    return {
      msgid: cleanString(xml.MsgId) ?? cleanString(xml.msgid) ?? undefined,
      msgtype: (cleanString(xml.MsgType) ?? cleanString(xml.msgtype) ?? "").toLowerCase(),
      createTime: cleanString(xml.CreateTime) ?? cleanString(xml.createTime) ?? undefined,
      agentId: cleanString(xml.AgentID) ?? cleanString(xml.agentid) ?? undefined,
      fromUserId: cleanString(xml.FromUserName) ?? undefined,
      fromName: undefined,
      toUserName: cleanString(xml.ToUserName) ?? undefined,
      text: xml.Content !== undefined ? String(xml.Content) : undefined,
      imageUrl: cleanString(xml.PicUrl) ?? undefined,
      imageMediaId: cleanString(xml.MediaId) ?? cleanString(xml.mediaid) ?? undefined,
      recognition: cleanString(xml.Recognition) ?? undefined,
      event: (cleanString(xml.Event) ?? cleanString(xml.event) ?? "").toLowerCase() || undefined,
      chatid: cleanString(xml.ChatId) ?? cleanString(xml.chatid) ?? undefined,
      chattype: (cleanString(xml.ChatType) ?? cleanString(xml.chattype) ?? "").toLowerCase() || undefined,
    };
  }
  let data: unknown;
  try {
    data = JSON.parse(source);
  } catch {
    return null;
  }
  if (!isRecord(data)) return null;
  const from = isRecord(data.from)
    ? cleanString(data.from.userid)
    : cleanString(data.from) ?? cleanString(data.FromUserName);
  const fromName = isRecord(data.from) ? cleanString(data.from.name) ?? undefined : undefined;
  const image = isRecord(data.image) ? data.image : undefined;
  return {
    msgid: cleanString(data.msgid) ?? cleanString(data.MsgId) ?? undefined,
    msgtype: String(cleanString(data.msgtype) ?? cleanString(data.MsgType) ?? "").toLowerCase(),
    createTime: cleanString(data.createTime) ?? cleanString(data.CreateTime) ?? undefined,
    agentId: cleanString(data.agentid) ?? cleanString(data.AgentID) ?? undefined,
    fromUserId: from ?? undefined,
    fromName,
    toUserName: cleanString(data.to) ?? undefined,
    text: isRecord(data.text) ? cleanString(data.text.content) ?? undefined : undefined,
    imageUrl: image ? cleanString(image.pic_url) ?? cleanString(image.url) ?? undefined : undefined,
    imageMediaId: image ? cleanString(image.media_id) ?? cleanString(image.mediaId) ?? undefined : undefined,
    recognition: cleanString(data.recognition) ?? undefined,
    event: String(cleanString(data.event) ?? cleanString(data.eventtype) ?? "").toLowerCase() || undefined,
    chatid: cleanString(data.chatid) ?? undefined,
    chattype: (cleanString(data.chattype) ?? "").toLowerCase() || undefined,
    quote: quoteFrom(data.quote),
  };
}

export type CallbackFormat = "json" | "xml";

export interface CallbackEnvelope {
  encrypt: string;
  signature: string;
  timestamp: string;
  nonce: string;
  format: CallbackFormat;
}

/** Extracts the encrypted envelope from a POST body (JSON or XML), falling
 * back to query-string parameters the way WeCom sometimes sends them. */
export function extractEnvelope(body: string, query: URLSearchParams): CallbackEnvelope | null {
  if (isXmlFormat(body)) {
    const xml = parseWecomXmlBody(body);
    const envelope: CallbackEnvelope = {
      encrypt: xml.Encrypt ?? "",
      signature: xml.MsgSignature ?? query.get("msg_signature") ?? query.get("msgsignature") ?? "",
      timestamp: xml.TimeStamp ?? query.get("timestamp") ?? "",
      nonce: xml.Nonce ?? query.get("nonce") ?? "",
      format: "xml",
    };
    return envelope.encrypt && envelope.signature && envelope.timestamp && envelope.nonce ? envelope : null;
  }
  let parsed: Record<string, unknown> = {};
  try {
    const value: unknown = JSON.parse(body);
    if (isRecord(value)) parsed = value;
  } catch {
    return null;
  }
  const envelope: CallbackEnvelope = {
    encrypt: cleanString(parsed.encrypt) ?? cleanString(parsed.Encrypt) ?? "",
    signature: cleanString(parsed.msgsignature)
      ?? cleanString(parsed.MsgSignature)
      ?? query.get("msg_signature")
      ?? query.get("msgsignature")
      ?? "",
    timestamp: String(cleanString(parsed.timestamp) ?? cleanString(parsed.TimeStamp) ?? query.get("timestamp") ?? ""),
    nonce: String(cleanString(parsed.nonce) ?? cleanString(parsed.Nonce) ?? query.get("nonce") ?? ""),
    format: "json",
  };
  return envelope.encrypt && envelope.signature && envelope.timestamp && envelope.nonce ? envelope : null;
}

export function safeEqualHex(expected: string, provided: string): boolean {
  const a = Buffer.from(String(expected ?? ""), "utf8");
  const b = Buffer.from(String(provided ?? ""), "utf8");
  if (a.length === 0 || a.length !== b.length) return false;
  return timingSafeEqual(a, b);
}

/** Builds the encrypted passive-reply body in the same JSON or XML envelope
 * the callback was received with. */
export function buildEncryptedReply({
  format,
  crypto,
  plaintext,
  timestamp,
  nonce,
}: {
  format: CallbackFormat;
  crypto: WecomCryptoLike;
  plaintext: unknown;
  timestamp: string;
  nonce: string;
}): string {
  const plain = typeof plaintext === "string" ? plaintext : JSON.stringify(plaintext ?? {});
  const { encrypt, signature } = crypto.encrypt(plain, String(timestamp), String(nonce));
  if (format === "json") {
    return JSON.stringify({
      encrypt,
      msgsignature: signature,
      timestamp: String(timestamp),
      nonce: String(nonce),
    });
  }
  return [
    "<xml>",
    `<Encrypt><![CDATA[${encrypt}]]></Encrypt>`,
    `<MsgSignature><![CDATA[${signature}]]></MsgSignature>`,
    `<TimeStamp>${String(timestamp)}</TimeStamp>`,
    `<Nonce><![CDATA[${String(nonce)}]]></Nonce>`,
    "</xml>",
  ].join("");
}
