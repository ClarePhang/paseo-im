#!/usr/bin/env node
/** 微信 sendText 三连重复自测：mock http 计数 sendmessage 调用次数。
 * 判定「一条回复 → 微信显示三条」发生在 adapter 内还是上层。
 * 运行：node --experimental-strip-types scripts/selftest-weixin-send.ts
 */
import { mkdtemp, mkdir, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createAdapter } from "../server/channels/weixin/index.ts";
import { emptyPolicy } from "../shared/config.ts";

const calls = [];
const dir = await mkdtemp(join(tmpdir(), "wx-send-test-"));
const dataDir = join(dir, "data");
const mediaDir = join(dir, "media");
await mkdir(dataDir, { recursive: true });
await mkdir(mediaDir, { recursive: true });

const http = async (path, body, init) => {
  calls.push({ path, clientId: body?.msg?.client_id, state: body?.msg?.message_state });
  if (path.includes("getupdates")) return { status: "wait" };
  return { ret: 0 };
};

const connection = {
  id: "1b8f4c3e-0000-4000-8000-000000000001",
  channel: "weixin",
  name: "微信测试连接",
  enabled: true,
  target: null,
  bindings: {},
  policy: emptyPolicy(),
  revision: 0,
  accountId: "acct-1",
  ownerUserId: "owner-1",
};

const adapter = createAdapter({
  connectionId: connection.id,
  config: connection,
  secrets: { token: "token-abc" },
  dataDir,
  mediaDir,
  signal: new AbortController().signal,
  settings: async () => ({
    imageMaxDownloadMb: 20,
    imageMaxSingleMb: 10,
    imageMaxTotalMb: 20,
    imageMaxCount: 4,
    attachmentTtlHours: 24,
    deferTimeoutMinutes: 5,
    busyMode: "queue",
  }),
  log: () => {},
  onMessage: () => {},
  onGroups: () => {},
  onError: () => {},
}, { http, sleep: async () => {} });

await adapter.start();
const result = await adapter.sendText({
  chatId: "user-1@im.wechat",
  text: "收到第二轮：wechat test（weixin，发送者同 user-1）。解析正常，本条回复验证回程。",
});
console.log("sendText result:", JSON.stringify(result));
const sends = calls.filter((call) => call.path.includes("sendmessage"));
console.log("sendmessage HTTP 调用次数:", sends.length);
console.log("client_ids:", sends.map((call) => call.clientId?.slice(0, 12)));
console.log("message_states:", sends.map((call) => call.state));
await adapter.stop().catch(() => {});
await rm(dir, { recursive: true, force: true });
process.exit(0);
