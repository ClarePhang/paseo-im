import { z } from "zod";
import { atomicWrite, readJson, WriteChain } from "./files.ts";
import { secretPath } from "./paths.ts";
import { SECRET_KEYS, type FieldSpec } from "../shared/channels.ts";
import type { ChannelId } from "../shared/model.ts";
import { UserFacingError } from "../shared/errors.ts";

const secretDocumentSchema = z.record(z.string(), z.string().max(20_000));
type SecretDocument = z.output<typeof secretDocumentSchema>;

/** Split user-entered fields into non-secret config entries and secrets. */
export function splitFields(
  channel: ChannelId,
  specs: FieldSpec[],
  fields: Record<string, string>,
): { config: Record<string, string>; secrets: Record<string, string> } {
  const secretKeys = SECRET_KEYS[channel];
  const config: Record<string, string> = {};
  const secrets: Record<string, string> = {};
  for (const field of specs) {
    const value = fields[field.key]?.trim() ?? "";
    if (!value && field.required) {
      throw new UserFacingError(`缺少必填字段：${field.label}`);
    }
    if (!value) continue;
    if (secretKeys.includes(field.key)) secrets[field.key] = value;
    else config[field.key] = value;
  }
  return { config, secrets };
}

/** One 0600 JSON file per connection holding only that connection's secrets. */
export class SecretStore {
  private readonly chain = new WriteChain();

  read(id: string): Promise<SecretDocument | undefined> {
    return readJson(secretPath(id), secretDocumentSchema.parse);
  }

  write(id: string, secrets: SecretDocument): Promise<void> {
    return this.chain.run(() => atomicWrite(secretPath(id), secrets));
  }

  merge(id: string, secrets: SecretDocument): Promise<void> {
    return this.chain.run(async () => {
      const current = (await readJson(secretPath(id), secretDocumentSchema.parse)) ?? {};
      await atomicWrite(secretPath(id), { ...current, ...secrets });
    });
  }

  async remove(id: string): Promise<void> {
    await this.chain.run(async () => {
      await atomicWrite(secretPath(id), {}).catch(() => {});
    });
  }
}
