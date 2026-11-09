import { SECRET_KEYS, type FieldSpec } from "../../shared/channels.ts";
import { UserFacingError } from "../../shared/errors.ts";
import type { ChannelId } from "../../shared/model.ts";
import type { ConfiguredFields } from "./types.ts";

/** Split raw user fields into config entries and secrets, trimmed. */
export function defaultConfigure(channel: ChannelId, fields: Record<string, string>): ConfiguredFields {
  const secretKeys = SECRET_KEYS[channel];
  const config: Record<string, unknown> = {};
  const secrets: Record<string, string> = {};
  for (const [key, raw] of Object.entries(fields)) {
    const value = raw.trim();
    if (!value) continue;
    if (secretKeys.includes(key)) secrets[key] = value;
    else config[key] = value;
  }
  return { config, secrets };
}

export function requireFields(specs: FieldSpec[], fields: Record<string, string>): void {
  for (const spec of specs) {
    if (spec.required && !(fields[spec.key]?.trim())) {
      throw new UserFacingError(`缺少必填字段：${spec.label}`);
    }
  }
}
