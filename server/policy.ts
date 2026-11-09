import type { Policy } from "../shared/config.ts";
import type { NormalizedMessage } from "../shared/model.ts";

export interface AccessInput {
  message: Pick<NormalizedMessage, "kind" | "senderId" | "trigger">;
  policy: Policy;
  isAdmin: boolean;
}

/** One gate for every inbound message. "operators" is the owner set edited in
 * the plugin screen; there is no separate pairing flow in v1. */
export function isAuthorized({ message, policy, isAdmin }: AccessInput): boolean {
  const operator = policy.operators.includes(message.senderId);
  if (message.kind === "dm") {
    return policy.directMessages === "everyone" || operator;
  }
  if (
    policy.invocation === "mentions" &&
    !message.trigger.mention &&
    !message.trigger.replyToBot
  ) {
    return false;
  }
  switch (policy.groupUsers) {
    case "everyone":
      return true;
    case "admins":
      return isAdmin || operator;
    case "operators":
      return operator;
  }
}

export function deniedHint(policy: Policy): string {
  return `当前账号未被授权使用此机器人（directMessages=${policy.directMessages}, groupUsers=${policy.groupUsers}）。请在 Paseo → IM 的访问策略中添加你的用户 ID。`;
}
