/** Per-channel UI field specs and display metadata, shared by client forms and
 * server-side credential splitting/validation. */
import { CHANNEL_IDS, type ChannelId } from "./model.ts";

export interface FieldSpec {
  key: string;
  label: string;
  secure?: boolean;
  required?: boolean;
  placeholder?: string;
  help?: string;
}

export interface ChannelSpec {
  id: ChannelId;
  title: string;
  icon: string;
  hint: string;
  fields: FieldSpec[];
  pairing: boolean;
}

export const SECRET_KEYS: Record<ChannelId, string[]> = {
  telegram: ["botToken"],
  slack: ["botToken", "appToken"],
  discord: ["token"],
  feishu: ["appSecret"],
  dingtalk: ["clientSecret"],
  qq: ["appSecret"],
  wecom: ["secret"],
  "wecom-app": ["secret", "token", "encodingAESKey"],
  whatsapp: [],
  weixin: ["token"],
  imessage: [],
  matrix: ["accessToken", "password"],
};

function spec(
  id: ChannelId,
  title: string,
  icon: string,
  hint: string,
  fields: FieldSpec[],
  pairing = false,
): ChannelSpec {
  return { id, title, icon, hint, fields, pairing };
}

export const CHANNEL_SPECS: Record<ChannelId, ChannelSpec> = {
  telegram: spec(
    "telegram",
    "Telegram",
    "Send",
    "使用 @BotFather 生成的 Bot Token 接入；私聊直接响应，群聊被提及或回复时响应。",
    [
      {
        key: "botToken",
        label: "Bot Token",
        secure: true,
        required: true,
        placeholder: "123456:ABC-DEF...",
      },
    ],
  ),
  slack: spec(
    "slack",
    "Slack",
    "Slack",
    "使用预置 App Manifest 创建应用并开启 Socket Mode，然后填写 Bot Token 与 App Token。",
    [
      {
        key: "botToken",
        label: "Bot Token (xoxb-)",
        secure: true,
        required: true,
        placeholder: "xoxb-...",
      },
      {
        key: "appToken",
        label: "App Token (xapp-)",
        secure: true,
        required: true,
        placeholder: "xapp-...",
      },
    ],
  ),
  discord: spec(
    "discord",
    "Discord",
    "Gamepad2",
    "使用 Developer Portal 生成的 Bot Token；需开启 Message Content Intent。",
    [
      {
        key: "token",
        label: "Bot Token",
        secure: true,
        required: true,
      },
    ],
  ),
  feishu: spec(
    "feishu",
    "飞书",
    "Bird",
    "填写飞书开放平台应用的 App ID 与 App Secret，需要 im:message 读取与 im:resource 上传权限。",
    [
      { key: "appId", label: "App ID", required: true },
      {
        key: "appSecret",
        label: "App Secret",
        secure: true,
        required: true,
      },
      {
        key: "domain",
        label: "域名",
        placeholder: "feishu 或 lark，默认 feishu",
      },
    ],
  ),
  dingtalk: spec(
    "dingtalk",
    "钉钉",
    "Bolt",
    "填写钉钉开放平台企业内部应用的 Client ID 与 Client Secret，开通机器人能力。",
    [
      { key: "clientId", label: "Client ID", required: true },
      {
        key: "clientSecret",
        label: "Client Secret",
        secure: true,
        required: true,
      },
    ],
  ),
  qq: spec(
    "qq",
    "QQ",
    "MessageSquare",
    "填写 QQ 开放平台机器人的 AppID 与 AppSecret，使用 WebSocket 长连接。",
    [
      { key: "appId", label: "AppID", required: true },
      { key: "appSecret", label: "AppSecret", secure: true, required: true },
    ],
  ),
  wecom: spec(
    "wecom",
    "企业微信",
    "Briefcase",
    "填写企业微信智能机器人的 Bot ID 与 Secret，或使用扫码配对自动获取。",
    [
      { key: "botId", label: "Bot ID", required: true },
      { key: "secret", label: "Secret", secure: true, required: true },
    ],
    true,
  ),
  "wecom-app": spec(
    "wecom-app",
    "企业微信应用",
    "Building2",
    "填写企业微信自建应用凭据；回调需要公网可达的 callbackBaseUrl。",
    [
      { key: "corpId", label: "企业 ID", required: true },
      { key: "agentId", label: "AgentId", required: true },
      { key: "secret", label: "应用 Secret", secure: true, required: true },
      { key: "token", label: "回调 Token", secure: true, required: true },
      {
        key: "encodingAESKey",
        label: "EncodingAESKey",
        secure: true,
        required: true,
      },
      { key: "callbackBaseUrl", label: "回调基址 (公网 URL)" },
      { key: "apiBaseUrl", label: "API 代理地址" },
    ],
  ),
  whatsapp: spec(
    "whatsapp",
    "WhatsApp",
    "Phone",
    "使用手机 WhatsApp 扫码关联设备；默认仅响应账号自聊。",
    [],
    true,
  ),
  weixin: spec(
    "weixin",
    "微信",
    "MessageCircle",
    "使用微信扫码登录机器人并配对；仅支持私聊。协议为未公开接口，存在稳定性风险。",
    [],
    true,
  ),
  imessage: spec(
    "imessage",
    "iMessage",
    "Smartphone",
    "仅 macOS：需要本机 Messages.app 登录并授予完全磁盘访问与自动化权限；Linux 不可用。",
    [],
  ),
  matrix: spec(
    "matrix",
    "Matrix",
    "Network",
    "填写 homeserver 与访问令牌，或用户 ID 与密码二选一。E2EE 暂不支持。",
    [
      { key: "homeserver", label: "Homeserver", required: true, placeholder: "https://matrix.example.org" },
      { key: "userId", label: "用户 ID", required: true, placeholder: "@bot:example.org" },
      { key: "accessToken", label: "Access Token", secure: true, placeholder: "syt_..." },
      { key: "password", label: "密码", secure: true },
    ],
  ),
};

export const CHANNEL_LIST: ChannelSpec[] = CHANNEL_IDS.map(
  (id) => CHANNEL_SPECS[id],
);
