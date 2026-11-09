/** Global plugin settings document, persisted by the Paseo host. */
import { defineSettings } from "@getpaseo/plugin";
import { z } from "zod";

export const imSettings = defineSettings({
  id: "global",
  scope: "host",
  version: 1,
  schema: z
    .object({
      imageMaxDownloadMb: z.number().min(1).max(200).default(30),
      imageMaxSingleMb: z.number().min(1).max(20).default(5),
      imageMaxTotalMb: z.number().min(1).max(100).default(20),
      imageMaxCount: z.number().int().min(1).max(20).default(20),
      busyMode: z.enum(["queue", "steer"]).default("queue"),
      attachmentTtlHours: z.number().min(-1).max(8760).default(168),
      deferTimeoutMinutes: z.number().int().min(5).max(720).default(30),
    })
    .strict(),
});

export type ImSettings = z.output<(typeof imSettings)["schema"]>;
