/** macOS 26 (Tahoe) stopped populating message.text for received iMessages:
 * the body is only stored in message.attributedBody, an NSArchiver typedstream
 * blob (`streamtyped` + archived NSAttributedString).
 *
 * Protocol facts ported from xmanrui/dsh-im (MIT). */

const ATTRIBUTED_BODY_STRING_CLASS = Buffer.from("NSString", "utf8");
const ATTRIBUTED_BODY_STRING_TAG = 0x2b; // '+': inline NSString payload follows
// Attachment-only messages carry U+FFFC (object replacement character) instead
// of text. The channel is text-only, so such runs must keep normalizing to null.
const OBJECT_REPLACEMENT_CHARACTER = "\ufffc";
/** 单段文本长度的硬上限：iMessage 正文远小于此，防御性拒绝伪造的大长度。 */
const MAX_TYPEDSTREAM_RUN_BYTES = 1 << 20;

// Apple NXTypedStream integer encoding: a signed byte is a literal length,
// 0x81/0x82/0x83 introduce a little-endian 2/4/8-byte length.
function readTypedStreamLength(
  buffer: Buffer,
  offset: number,
): { length: number; offset: number } | null {
  if (offset >= buffer.length) return null;
  const tag = buffer[offset];
  let cursor = offset + 1;
  let length: number;
  if (tag === 0x81) {
    if (cursor + 2 > buffer.length) return null;
    // 无符号读：32KB-64KB 的正文按有符号读会变成负数而被误判损坏。
    length = buffer.readUInt16LE(cursor);
    cursor += 2;
  } else if (tag === 0x82) {
    if (cursor + 4 > buffer.length) return null;
    length = buffer.readUInt32LE(cursor);
    cursor += 4;
  } else if (tag === 0x83) {
    if (cursor + 8 > buffer.length) return null;
    const value = buffer.readBigInt64LE(cursor);
    if (value < 0n || value > BigInt(Number.MAX_SAFE_INTEGER)) return null;
    length = Number(value);
    cursor += 8;
  } else {
    length = tag > 0x7f ? tag - 0x100 : tag;
  }
  if (!Number.isInteger(length) || length < 0 || length > MAX_TYPEDSTREAM_RUN_BYTES
    || cursor + length > buffer.length) {
    return null;
  }
  return { length, offset: cursor };
}

/** Accepts either the hex string selected via hex(m.attributedBody) or the raw
 * blob; returns the decoded UTF-8 text, or null when no inline string run can
 * be read. */
export function decodeAttributedBodyText(value: unknown): string | null {
  const buffer = Buffer.isBuffer(value)
    ? value
    : typeof value === "string" && value.trim() ? Buffer.from(value.trim(), "hex") : null;
  if (!buffer?.length) return null;
  const marker = buffer.indexOf(ATTRIBUTED_BODY_STRING_CLASS);
  if (marker < 0) return null;
  let tagIndex = marker + ATTRIBUTED_BODY_STRING_CLASS.length;
  while (tagIndex < buffer.length && buffer[tagIndex] !== ATTRIBUTED_BODY_STRING_TAG) tagIndex += 1;
  if (tagIndex >= buffer.length) return null;
  const run = readTypedStreamLength(buffer, tagIndex + 1);
  if (!run || run.length === 0) return null;
  let text: string;
  try {
    text = new TextDecoder("utf-8", { fatal: true }).decode(
      buffer.subarray(run.offset, run.offset + run.length),
    );
  } catch {
    return null;
  }
  const stripped = text.replaceAll(OBJECT_REPLACEMENT_CHARACTER, "").trim();
  return stripped || null;
}
