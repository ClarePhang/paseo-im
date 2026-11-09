import type { ChannelDefinition } from "./types.ts";
import { definition as dingtalk } from "./dingtalk/index.ts";
import { definition as discord } from "./discord/index.ts";
import { definition as feishu } from "./feishu/index.ts";
import { definition as imessage } from "./imessage/index.ts";
import { definition as matrix } from "./matrix/index.ts";
import { definition as qq } from "./qq/index.ts";
import { definition as slack } from "./slack/index.ts";
import { definition as telegram } from "./telegram/index.ts";
import { definition as wecom } from "./wecom/index.ts";
import { definition as wecomApp } from "./wecom-app/index.ts";
import { definition as weixin } from "./weixin/index.ts";
import { definition as whatsapp } from "./whatsapp/index.ts";
import type { ChannelId } from "../../shared/model.ts";

export const CHANNEL_DEFINITIONS: Record<ChannelId, ChannelDefinition> = {
  dingtalk,
  discord,
  feishu,
  imessage,
  matrix,
  qq,
  slack,
  telegram,
  wecom,
  "wecom-app": wecomApp,
  weixin,
  whatsapp,
};
