import dns from "node:dns";
import net from "node:net";
import type { PluginServerContext } from "@getpaseo/plugin/server";
import { imSettings } from "./shared/settings.ts";
import {
  choicesRpc,
  connectRpc,
  disconnectRpc,
  listRpc,
  pairingPollRpc,
  pairingStartRpc,
  reviewRpc,
  revokeRpc,
  saveRpc,
  testRpc,
} from "./shared/contracts.ts";
import { Manager } from "./server/manager.ts";
import { dataRoot, paseoHome } from "./server/paths.ts";

// 双栈域名在本机 IPv6 路由黑洞时（如 WSL2 下 api.telegram.org：AAAA 可解析但 SYN 无响应，
// node 连接竞速 ~400ms 内以 ETIMEDOUT 失败且不回退 v4）所有渠道出站全部瘫痪。
// 实测（node v24）：dns ipv4first + 关闭 autoSelectFamily 竞速后 fetch api.telegram.org 200。
// 只影响解析顺序偏好：仅有 AAAA 的域名仍走 IPv6，无副作用。
dns.setDefaultResultOrder("ipv4first");
net.setDefaultAutoSelectFamily(false);

export default function contribute(server: PluginServerContext) {
  const manager = new Manager(paseoHome, dataRoot);
  const settings = server.registerSettings(imSettings);
  manager.setSettingsReader(async () => {
    const state = await settings.read();
    return state.status === "ready" ? state.values : imSettings.schema.parse({});
  });

  server.handle(listRpc, () => manager.list());
  server.handle(choicesRpc, () => manager.choices());
  server.handle(connectRpc, (input) => manager.connect(input));
  server.handle(saveRpc, (input) => manager.save(input));
  server.handle(disconnectRpc, ({ id }) => manager.disconnect(id));
  server.handle(revokeRpc, ({ id, chatId }) => manager.revoke(id, chatId));
  server.handle(reviewRpc, ({ id, chatId, approve }) => manager.review(id, chatId, approve));
  server.handle(pairingStartRpc, ({ channel, connectionId }) => manager.pairingStart(channel, connectionId));
  server.handle(pairingPollRpc, ({ attemptId }) => manager.pairingPoll(attemptId));
  server.handle(testRpc, ({ id }) => manager.test(id));
  const removePermissionHook = server.addHook("agent.permission.after", async ({ agent, requestId }) => {
    await manager.onPermissionResolved(agent.id, requestId).catch(() => {});
  });

  manager.start();
  return () => {
    removePermissionHook();
    void manager.stop();
  };
}
