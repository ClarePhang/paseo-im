import type { PluginClientContext, PluginSidebarItemProps } from "@getpaseo/plugin/client";
import { SidebarRow } from "@getpaseo/plugin/client/ui";
import { ImScreen } from "./client/screen.tsx";

function ImItem(props: PluginSidebarItemProps) {
  return (
    <SidebarRow
      icon="MessagesSquare"
      label="IM"
      active={props.currentScreen?.screenId === "im"}
      onPress={() => props.openScreen({ screenId: "im" })}
    />
  );
}

export default function contribute(client: PluginClientContext) {
  const removers = [
    client.addScreen({
      id: "im",
      title: "IM",
      Component: (props) => <ImScreen {...props} client={client} />,
    }),
    client.addSidebarHeaderItem({ id: "im", title: "IM", Component: ImItem }),
    client.addCommandCenterItem({
      id: "im",
      title: "IM 机器人",
      icon: "MessagesSquare",
      context: "global",
      keywords: ["im", "bot", "telegram", "feishu", "wechat", "dingtalk", "slack", "discord", "whatsapp", "matrix"],
      onSelect: (context) => context.openScreen({ screenId: "im" }),
    }),
  ];
  return () => {
    for (const remove of removers) remove();
  };
}
