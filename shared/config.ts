/** Connection configuration documents. Secrets never appear here; they live in
 * server-side per-connection secret files. */
import { z } from "zod";
import { CHANNEL_IDS, type ChannelId } from "./model.ts";

export const idSchema = z.string().min(1).max(256);
export const maxConnections = 50;

export const profileSchema = z
  .object({
    provider: z.string().regex(/^[^/]+\/.+/).max(256),
    modeId: z.string().max(128).optional(),
    thinkingOptionId: z.string().max(128).optional(),
    systemPrompt: z.string().max(32_000).default(""),
  })
  .strict();

export const targetSchema = z.discriminatedUnion("kind", [
  z
    .object({
      kind: z.literal("agent"),
      agentId: idSchema,
      sharedHistory: z.boolean().default(false),
    })
    .strict(),
  z
    .object({
      kind: z.literal("workspace"),
      workspaceId: idSchema,
      profile: profileSchema,
    })
    .strict(),
]);
export type Target = z.output<typeof targetSchema>;

export const policySchema = z
  .object({
    groupAdmission: z.enum(["operators", "approval", "everyone"]).default("approval"),
    directMessages: z.enum(["operators", "everyone"]).default("everyone"),
    groupUsers: z.enum(["operators", "admins", "everyone"]).default("operators"),
    operators: z.array(z.string().min(1).max(256)).max(100).default([]),
    invocation: z.enum(["mentions", "all"]).default("mentions"),
  })
  .strict();
export type Policy = z.output<typeof policySchema>;

export const convKeySchema = z.string().min(1).max(400);

const common = {
  id: z.uuid(),
  name: z.string().trim().min(1).max(80),
  enabled: z.boolean(),
  target: targetSchema.nullable(),
  policy: policySchema,
  bindings: z.record(convKeySchema, targetSchema).default({}),
  revision: z.number().int().nonnegative(),
};

const strictObject = <T extends z.ZodRawShape>(shape: T) => z.object(shape).strict();

export const connectionSchema = z.discriminatedUnion("channel", [
  strictObject({ ...common, channel: z.literal("telegram"), username: z.string().regex(/^[A-Za-z0-9_]{4,64}$/), botUserId: z.string().min(1).max(64) }),
  strictObject({ ...common, channel: z.literal("slack"), teamId: z.string().min(1).max(64), botUserId: z.string().min(1).max(64) }),
  strictObject({ ...common, channel: z.literal("discord"), botUserId: z.string().min(1).max(64), username: z.string().min(1).max(80) }),
  strictObject({ ...common, channel: z.literal("feishu"), appId: z.string().min(1).max(64), domain: z.enum(["feishu", "lark"]).default("feishu") }),
  strictObject({ ...common, channel: z.literal("dingtalk"), clientId: z.string().min(1).max(64) }),
  strictObject({ ...common, channel: z.literal("qq"), appId: z.string().min(1).max(64) }),
  strictObject({ ...common, channel: z.literal("wecom"), botId: z.string().max(128).default("") }),
  strictObject({
    ...common,
    channel: z.literal("wecom-app"),
    corpId: z.string().min(1).max(64),
    agentId: z.string().min(1).max(64),
    apiBaseUrl: z.string().url().max(200).optional(),
    callbackBaseUrl: z.string().url().max(200).optional(),
    callbackSecret: z.string().regex(/^[0-9a-f]{32}$/),
    streamEnabled: z.boolean().default(true),
  }),
  strictObject({ ...common, channel: z.literal("whatsapp"), accountJid: z.string().max(128).default("") }),
  strictObject({ ...common, channel: z.literal("weixin"), accountId: z.string().max(128).default(""), ownerUserId: z.string().max(128).optional(), baseUrl: z.string().url().max(200).optional() }),
  strictObject({ ...common, channel: z.literal("imessage") }),
  strictObject({ ...common, channel: z.literal("matrix"), homeserver: z.string().url().max(200), userId: z.string().regex(/^@.+:.+$/).max(256), authMode: z.enum(["token", "password"]) }),
]);
export type Connection = z.output<typeof connectionSchema>;

export const registryDocumentSchema = z
  .object({
    version: z.literal(1),
    connections: z.array(connectionSchema).max(maxConnections).default([]),
  })
  .strict();

export type RegistryDocument = z.output<typeof registryDocumentSchema>;

export function channelOf(value: unknown): ChannelId | null {
  const parsed = z.object({ channel: z.enum(CHANNEL_IDS) }).safeParse(value);
  return parsed.success ? parsed.data.channel : null;
}

export function emptyPolicy(): Policy {
  return policySchema.parse({});
}
