/** Matrix channel definition: credential configure/splitting plus the adapter
 * factory. Protocol facts ported from xmanrui/dsh-im (MIT). */
import { CHANNEL_SPECS } from "../../../shared/channels.ts";
import { UserFacingError } from "../../../shared/errors.ts";
import type { AdapterContext, ChannelAdapter, ChannelDefinition } from "../types.ts";
import { defaultConfigure, requireFields } from "../configure.ts";
import { MatrixAdapter } from "./adapter.ts";
import { defaultDeps, homeserverUrl, isMatrixUserId, type MatrixConnection, type MatrixDeps } from "./api.ts";

export type { MatrixConnection };
export { defaultDeps, MatrixApiError } from "./api.ts";
export type { MatrixDeps };

export function createAdapter(ctx: AdapterContext<MatrixConnection>, deps?: MatrixDeps): ChannelAdapter {
  return new MatrixAdapter(ctx, deps);
}

export const definition: ChannelDefinition<MatrixConnection> = {
  channel: "matrix",
  create: createAdapter,
  configure(fields) {
    requireFields(CHANNEL_SPECS.matrix.fields, fields);
    const homeserver = homeserverUrl(fields.homeserver ?? "");
    if (!homeserver) {
      throw new UserFacingError("Homeserver 地址无效，请填写形如 https://matrix.example.org 的地址。");
    }
    const userId = (fields.userId ?? "").trim();
    if (!isMatrixUserId(userId)) {
      throw new UserFacingError("用户 ID 需形如 @bot:example.org，请核对后重新填写。");
    }
    const { config, secrets } = defaultConfigure("matrix", fields);
    if (!secrets.accessToken && !secrets.password) {
      throw new UserFacingError("Access Token 与密码至少填写一项（二选一）。");
    }
    return {
      config: { ...config, homeserver, userId, authMode: secrets.accessToken ? "token" : "password" },
      secrets,
    };
  },
};
