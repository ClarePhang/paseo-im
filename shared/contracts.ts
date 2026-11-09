/** Plugin RPC contracts. */
import { defineRpc } from "@getpaseo/plugin";
import { z } from "zod";
import { connectionSchema, policySchema, targetSchema } from "./config.ts";
import { CHANNEL_IDS } from "./model.ts";

const empty = z.object({}).strict();
const connectionId = z.object({ id: z.uuid() }).strict();

export const conversationViewSchema = z
  .object({
    key: z.string(),
    title: z.string().nullable(),
    agentId: z.string().nullable(),
    workspaceId: z.string().nullable(),
    current: z.boolean(),
    active: z.boolean(),
  })
  .strict();

export const connectionViewSchema = z
  .object({
    config: connectionSchema,
    running: z.boolean(),
    lastError: z.string().nullable(),
    ready: z.boolean(),
    queued: z.number().int(),
    ownerPaired: z.boolean(),
    conversations: z.array(conversationViewSchema).default([]),
    groups: z.array(z.object({ id: z.string(), title: z.string().nullable() })).default([]),
    pendingGroups: z.array(z.object({ id: z.string(), title: z.string().nullable() })).default([]),
  })
  .strict();
export type ConnectionView = z.output<typeof connectionViewSchema>;

export const listRpc = defineRpc({
  name: "im.list",
  input: empty,
  output: z
    .object({
      channels: z.array(
        z.object({
          channel: z.enum(CHANNEL_IDS),
          connections: z.array(connectionViewSchema),
        }),
      ),
    })
    .strict(),
});

export const choicesSchema = z
  .object({
    agents: z.array(z.object({ id: z.string(), title: z.string() })),
    workspaces: z.array(z.object({ id: z.string(), name: z.string() })),
    providers: z.array(
      z.object({
        id: z.string(),
        label: z.string(),
        models: z.array(
          z.object({
            id: z.string(),
            label: z.string(),
            modes: z.array(z.object({ id: z.string(), label: z.string() })),
            thinking: z.array(z.object({ id: z.string(), label: z.string() })),
          }),
        ),
      }),
    ),
  })
  .strict();
export type Choices = z.output<typeof choicesSchema>;

export const choicesRpc = defineRpc({ name: "im.choices", input: empty, output: choicesSchema });

export const connectRpc = defineRpc({
  name: "im.connect",
  input: z
    .object({
      channel: z.enum(CHANNEL_IDS),
      name: z.string().trim().min(1).max(80),
      fields: z.record(z.string(), z.string().max(20_000)),
    })
    .strict(),
  output: z.object({ id: z.uuid(), detail: z.string().nullable() }).strict(),
});

export const saveRpc = defineRpc({
  name: "im.save",
  input: z
    .object({
      id: z.uuid(),
      revision: z.number().int().nonnegative(),
      name: z.string().trim().min(1).max(80).optional(),
      enabled: z.boolean().optional(),
      target: targetSchema.nullable().optional(),
      bindings: z.record(z.string().min(1).max(400), targetSchema).optional(),
      policy: policySchema.optional(),
    })
    .strict(),
  output: z.object({ connection: connectionSchema }).strict(),
});

export const disconnectRpc = defineRpc({
  name: "im.disconnect",
  input: connectionId,
  output: z.object({ removed: z.boolean() }).strict(),
});

export const revokeRpc = defineRpc({
  name: "im.revoke",
  input: connectionId.extend({ chatId: z.string().min(1).max(256) }),
  output: z.object({ revoked: z.boolean() }).strict(),
});

export const reviewRpc = defineRpc({
  name: "im.review",
  input: connectionId.extend({ chatId: z.string().min(1).max(256), approve: z.boolean() }),
  output: z.object({ approved: z.boolean() }).strict(),
});

export const pairingStartRpc = defineRpc({
  name: "im.pairing.start",
  input: z.object({ channel: z.enum(CHANNEL_IDS), connectionId: z.uuid().optional() }).strict(),
  output: z.object({ attemptId: z.string().min(1).max(64) }).strict(),
});

export const pairingPollRpc = defineRpc({
  name: "im.pairing.poll",
  input: z.object({ attemptId: z.string().min(1).max(64) }).strict(),
  output: z
    .object({
      status: z.enum(["pending", "scanned", "ok", "expired", "failed"]),
      qrDataUrl: z.string().max(100_000).optional(),
      detail: z.string().nullable().optional(),
      connectionId: z.uuid().optional(),
    })
    .strict(),
});

export const testRpc = defineRpc({
  name: "im.test",
  input: connectionId,
  output: z.object({ ok: z.boolean(), detail: z.string().nullable() }).strict(),
});
