#!/usr/bin/env node
/** 微信 sendText 三连重复自测：mock http 计数 sendmessage 调用。 */
import (await import("node:url")).pathToFileURL;

const { createAdapter } = await import("../server/channels/weixin/index.ts").then((m) => m.default ?? m).catch(() => import("../server/channels/weixin/index.ts"));

const calls = [];
const mkdtemp = (await import("node:fs/promises")).mkdtemp;
const join = (await import("node:path")).join;
const tmpdir = (await import("node:os")).tmpdir;
const dir = await mkdtemp(join(tmpdir(), "wx-send-test-"));

const http = async (path, body, init) => {
  calls.push({ path, body });
  if (path.includes("getupdates")) return { status: "wait", get_updates_buf: "BUF" };
  if (path.includes("get_bot_qrcode")) return { qrcode: "Q", qrcode_img_content: "https://liteapp.weixin.qq.com/q/x" };
  return { ret: 0 };
};

const ctx = {
  connection: { id: "c1", channel: "weixin", name: "t", enabled: true, target: null, bindings: {}, policy: { directMessages: "everyone", groupAdmission: "approval", groupUsers: "operators", operators: [], invocation: "mentions" }, revision: 0 },
  dataDir: dir,
  secrets: {},
  log: () => {},
  signal: new AbortController().signal,
  daemon: async () => ({}),
  settings: async () => ({ imageMaxDownloadMb: 20, imageMaxSingleMb: 10, imageMaxTotalMb: 20, imageMaxCount: 4, attachmentTtlHours: 24, deferTimeoutMinutes: 5, busyMode: "queue" }),
};

const adapter = createAdapter(ctx);
await adapter.start();
const result = await adapter.sendText({ chatId: "user-1", text: "收到第二轮：wechat test（weixin，发送者同 user-1）。解析正常，本条回复验证回程。" });
console.log("sendText result:", JSON.stringify(result));
const sends = calls.filter((c) => c.path.includes("sendmessage"));
console.log("sendmessage HTTP 调用次数:", sends.length);
console.log("client_ids:", sends.map((c) => c.body?.msg?.client_id?.slice(0, 12)));
await adapter.stop().catch(() => {});
process.exit(0);
