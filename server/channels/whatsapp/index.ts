/** WhatsApp channel: QR pairing over Baileys, self-chat-first personal bot.
 * Protocol facts ported from xmanrui/dsh-im (MIT). */

import type { Connection } from "../../../shared/config.ts";
import { UserFacingError } from "../../../shared/errors.ts";
import { defaultConfigure } from "../configure.ts";
import type { ChannelDefinition, ConfiguredFields } from "../types.ts";
import { WhatsappAdapter, createAdapter, whatsappCapabilities, type WhatsappDeps } from "./adapter.ts";
import { WhatsappPairingSession } from "./pairing.ts";

export type WhatsappConnection = Extract<Connection, { channel: "whatsapp" }>;

export { createAdapter, whatsappCapabilities, type WhatsappDeps, type WhatsappSocket, type WhatsappSocketUser } from "./adapter.ts";
export { WhatsappPairingSession } from "./pairing.ts";
export { WHATSAPP_STATE_FILE } from "./state.ts";

/** WhatsApp linked accounts are numeric jids on the PN or LID server. */
const ACCOUNT_JID_PATTERN = /^\d{5,32}@(s\.whatsapp\.net|lid)$/;

export const definition: ChannelDefinition<WhatsappConnection> = {
  channel: "whatsapp",
  create: (ctx) => new WhatsappAdapter(ctx),
  configure(fields: Record<string, string>): ConfiguredFields {
    const accountJid = fields.accountJid?.trim();
    if (accountJid && !ACCOUNT_JID_PATTERN.test(accountJid)) {
      throw new UserFacingError(`accountJid 格式无效：${accountJid}（应为 数字@s.whatsapp.net 或 数字@lid）。`);
    }
    return defaultConfigure("whatsapp", fields);
  },
  pairing: (connectionId, dataDir) => new WhatsappPairingSession(connectionId, dataDir),
};
