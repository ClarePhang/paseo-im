import type { PluginSurfaceProps } from "@getpaseo/plugin/client";
import type { Target, Policy, Connection } from "../shared/config.ts";
import type { Choices } from "../shared/contracts.ts";
import { SettingsCard, SettingsRow, SettingsSelect, SettingsSwitch, SettingsInput } from "@getpaseo/plugin/client/ui";
import { useState } from "react";
import { Pressable, ScrollView, Text, View } from "react-native";

interface RouteEditorProps {
  theme: PluginSurfaceProps["theme"];
  choices: Choices | null;
  target: Target | null;
  onChange: (target: Target | null) => void;
}

function AgentTargetEditor({
  theme,
  choices,
  target,
  onChange,
}: { target: Extract<Target, { kind: "agent" }> } & Omit<RouteEditorProps, "target"> & { target: Target }) {
  const agentTarget = target.kind === "agent" ? target : null;
  return (
    <View style={{ gap: 8 }}>
      <SettingsSelect
        label="Paseo 聊天"
        value={agentTarget?.agentId ?? ""}
        options={(choices?.agents ?? []).map((agent) => ({ value: agent.id, label: agent.title }))}
        onValueChange={(agentId) =>
          onChange({ kind: "agent", agentId, sharedHistory: agentTarget?.sharedHistory ?? false })
        }
      />
      <SettingsSwitch
        label="多会话共享此聊天历史"
        hint="默认一个 IM 会话独占一个聊天；开启后多个 IM 会话写入同一历史。"
        value={agentTarget?.sharedHistory ?? false}
        onValueChange={(sharedHistory) => {
          if (agentTarget?.agentId) onChange({ kind: "agent", agentId: agentTarget.agentId, sharedHistory });
        }}
      />
      <Text style={{ color: theme.colors.foregroundMuted, fontSize: 12 }}>
        未在列表中的聊天可先在 Paseo 中打开后再选择。
      </Text>
    </View>
  );
}

function WorkspaceTargetEditor({
  theme,
  choices,
  target,
  onChange,
}: { target: Extract<Target, { kind: "workspace" }> } & Omit<RouteEditorProps, "target"> & { target: Target }) {
  const workspaceTarget = target.kind === "workspace" ? target : null;
  const profile = workspaceTarget?.profile;
  // 模型 ID 本身可含斜杠（如 "omp/opencode-zen/big-pickle"）：provider 取首段，其余全部是 model
  const providerValue = profile?.provider ?? "";
  const firstSlash = providerValue.indexOf("/");
  const providerId = firstSlash >= 0 ? providerValue.slice(0, firstSlash) : providerValue;
  const modelId = firstSlash >= 0 ? providerValue.slice(firstSlash + 1) : "";
  const provider = choices?.providers.find((entry) => entry.id === providerId) ?? null;
  const model = provider?.models.find((entry) => entry.id === modelId) ?? null;

  return (
    <View style={{ gap: 8 }}>
      <SettingsSelect
        label="工作区"
        value={workspaceTarget?.workspaceId ?? ""}
        options={(choices?.workspaces ?? []).map((workspace) => ({ value: workspace.id, label: workspace.name }))}
        onValueChange={(workspaceId) =>
          onChange({
            kind: "workspace",
            workspaceId,
            profile: profile ?? { provider: "", modeId: undefined, thinkingOptionId: undefined, systemPrompt: "" },
          })
        }
      />
      <OptionPicker
        theme={theme}
        label="提供方"
        value={providerId ?? ""}
        options={(choices?.providers ?? []).map((entry) => ({ value: entry.id, label: entry.label }))}
        onChange={(nextProviderId) => {
          if (!workspaceTarget) return;
          const nextProvider = choices?.providers.find((entry) => entry.id === nextProviderId) ?? null;
          onChange({
            kind: "workspace",
            workspaceId: workspaceTarget.workspaceId,
            profile: {
              provider: `${nextProviderId}/${nextProvider?.models[0]?.id ?? ""}`,
              modeId: undefined,
              thinkingOptionId: undefined,
              systemPrompt: profile?.systemPrompt ?? "",
            },
          });
        }}
      />
      {provider ? (
        <OptionPicker
          theme={theme}
          label="模型"
          value={modelId ?? ""}
          options={provider.models.map((entry) => ({ value: entry.id, label: entry.label }))}
          onChange={(nextModelId) => {
            if (!workspaceTarget) return;
            onChange({
              kind: "workspace",
              workspaceId: workspaceTarget.workspaceId,
              profile: {
                provider: `${providerId}/${nextModelId}`,
                modeId: profile?.modeId,
                thinkingOptionId: profile?.thinkingOptionId,
                systemPrompt: profile?.systemPrompt ?? "",
              },
            });
          }}
        />
      ) : null}
      {model && model.modes.length ? (
        <SettingsSelect
          label="模式"
          value={profile?.modeId ?? ""}
          options={[{ value: "", label: "默认" }, ...model.modes.map((mode) => ({ value: mode.id, label: mode.label }))]}
          onValueChange={(modeId) => {
            if (!workspaceTarget) return;
            onChange({
              kind: "workspace",
              workspaceId: workspaceTarget.workspaceId,
              profile: {
                provider: providerValue,
                modeId: modeId || undefined,
                thinkingOptionId: profile?.thinkingOptionId,
                systemPrompt: profile?.systemPrompt ?? "",
              },
            });
          }}
        />
      ) : null}
      {model && model.thinking.length ? (
        <SettingsSelect
          label="思考等级"
          value={profile?.thinkingOptionId ?? ""}
          options={[{ value: "", label: "默认" }, ...model.thinking.map((option) => ({ value: option.id, label: option.label }))]}
          onValueChange={(thinkingOptionId) => {
            if (!workspaceTarget) return;
            onChange({
              kind: "workspace",
              workspaceId: workspaceTarget.workspaceId,
              profile: {
                provider: providerValue,
                modeId: profile?.modeId,
                thinkingOptionId: thinkingOptionId || undefined,
                systemPrompt: profile?.systemPrompt ?? "",
              },
            });
          }}
        />
      ) : null}
      <SettingsInput
          label="系统提示词"
          hint="可选；仅用于新创建的会话"
          initialValue={profile?.systemPrompt ?? ""}
          placeholder="可选"
          onChangeText={(systemPrompt) => {
            if (!workspaceTarget) return;
            onChange({
              kind: "workspace",
              workspaceId: workspaceTarget.workspaceId,
              profile: {
                provider: providerValue,
                modeId: profile?.modeId,
                thinkingOptionId: profile?.thinkingOptionId,
                systemPrompt,
              },
            });
          }}
        />
      <Text style={{ color: theme.colors.foregroundMuted, fontSize: 12 }}>
        每个 IM 会话在该工作区创建独立聊天与历史。
      </Text>
    </View>
  );
}

/** Destination editor: bind an existing chat or create per-conversation agents
 * in a workspace, for the default route or one conversation override. */
export function RouteEditor({ theme, choices, target, onChange }: RouteEditorProps) {
  const kind = target?.kind ?? "none";
  return (
    <View style={{ gap: 8 }}>
      <SettingsSelect
        label="去向类型"
        value={kind}
        options={[
          { value: "none", label: "未设置（不响应）" },
          { value: "agent", label: "已有 Paseo 聊天" },
          { value: "workspace", label: "工作区（每会话新建聊天）" },
        ]}
        onValueChange={(value) => {
          if (value === "agent") {
            const first = choices?.agents[0]?.id;
            if (first) onChange({ kind: "agent", agentId: first, sharedHistory: false });
          } else if (value === "workspace") {
            const first = choices?.workspaces[0]?.id;
            const provider = choices?.providers[0];
            if (first && provider) {
              onChange({
                kind: "workspace",
                workspaceId: first,
                profile: { provider: `${provider.id}/${provider.models[0]?.id ?? ""}`, modeId: undefined, thinkingOptionId: undefined, systemPrompt: "" },
              });
            }
          } else {
            onChange(null);
          }
        }}
      />
      {target?.kind === "agent" ? (
        <AgentTargetEditor theme={theme} choices={choices} target={target} onChange={onChange} />
      ) : null}
      {target?.kind === "workspace" ? (
        <WorkspaceTargetEditor theme={theme} choices={choices} target={target} onChange={onChange} />
      ) : null}
    </View>
  );
}

export function PolicyEditor({
  policy,
  onChange,
}: {
  policy: Policy;
  onChange: (policy: Policy) => void;
}) {
  return (
    <SettingsCard>
      <SettingsSelect
        label="私聊权限"
        value={policy.directMessages}
        options={[
          { value: "operators", label: "仅名单用户" },
          { value: "everyone", label: "所有人" },
        ]}
        onValueChange={(value) => onChange({ ...policy, directMessages: value as Policy["directMessages"] })}
      />
      <SettingsSelect
        label="群聊触发"
        value={policy.invocation}
        options={[
          { value: "mentions", label: "被 @ 或回复时" },
          { value: "all", label: "全部消息" },
        ]}
        onValueChange={(value) => onChange({ ...policy, invocation: value as Policy["invocation"] })}
      />
      <SettingsSelect
        label="群聊请求权限"
        hint="「群管理员」需渠道支持管理员查询（QQ/钉钉/企业微信应用/iMessage 不支持，按名单用户处理）"
        value={policy.groupUsers}
        options={[
          { value: "operators", label: "仅名单用户" },
          { value: "admins", label: "群管理员" },
          { value: "everyone", label: "所有群成员" },
        ]}
        onValueChange={(value) => onChange({ ...policy, groupUsers: value as Policy["groupUsers"] })}
      />
      <SettingsSelect
        label="群接入方式"
        value={policy.groupAdmission}
        options={[
          { value: "approval", label: "接入需审批" },
          { value: "operators", label: "名单用户可接入（仍需审批）" },
          { value: "everyone", label: "自动接入" },
        ]}
        onValueChange={(value) => onChange({ ...policy, groupAdmission: value as Policy["groupAdmission"] })}
      />
      <SettingsInput
        label="用户名单"
        hint="逗号分隔的用户 ID；私聊/群聊选中“名单用户”时生效"
        initialValue={policy.operators.join(", ")}
        placeholder="如 10086, ou_abc123, @user:example.org"
        onChangeText={(text) =>
          onChange({
            ...policy,
            operators: text
              .split(/[,，\n]/)
              .map((entry) => entry.trim())
              .filter(Boolean)
              .slice(0, 100),
          })
        }
      />
    </SettingsCard>
  );
}

export function statusText(connection: {
  config: Connection;
  running: boolean;
  lastError: string | null;
  ready: boolean;
  pendingGroups: unknown[];
}): { label: string; tone: "ok" | "warn" | "idle" } {
  if (!connection.config.enabled) return { label: "已暂停", tone: "idle" };
  if (connection.lastError) return { label: "需要关注", tone: "warn" };
  if (connection.pendingGroups.length > 0) return { label: "有群待审批", tone: "warn" };
  if (!connection.config.target && Object.keys(connection.config.bindings).length === 0) {
    return { label: "待选择去向", tone: "warn" };
  }
  if (!connection.running) return { label: "连接中…", tone: "idle" };
  return { label: "就绪", tone: "ok" };
}

/** 长列表选择器：宿主自适应菜单选项超高时无法滚动，这里用屏内 ScrollView
 * 自绘（web 滚轮 / 移动触屏均可滚动），仅用于选项较多的下拉。 */
export function OptionPicker({
  theme,
  label,
  value,
  options,
  onChange,
}: {
  theme: { colors: { foreground: string; foregroundMuted: string; accent: string; accentForeground: string; surface1: string; border: string } };
  label: string;
  value: string;
  options: readonly { label: string; value: string }[];
  onChange: (value: string) => void;
}) {
  const [open, setOpen] = useState(false);
  const current = options.find((option) => option.value === value);
  return (
    <View>
      <Pressable
        accessibilityRole="button"
        accessibilityLabel={label}
        onPress={() => setOpen((prev) => !prev)}
        style={{ paddingVertical: 10 }}
      >
        <View style={{ flexDirection: "row", alignItems: "center" }}>
          <Text style={{ color: theme.colors.foreground, flex: 1 }}>{label}</Text>
          <Text
            style={{
              color: current ? theme.colors.foreground : theme.colors.foregroundMuted,
              marginRight: 6,
              maxWidth: 200,
            }}
            numberOfLines={1}
          >
            {current?.label ?? "未选择"}
          </Text>
          <Text style={{ color: theme.colors.foregroundMuted }}>{open ? "▾" : "›"}</Text>
        </View>
      </Pressable>
      {open ? (
        <View
          style={{
            maxHeight: 260,
            borderWidth: 1,
            borderColor: theme.colors.border,
            borderRadius: 8,
            overflow: "hidden",
            backgroundColor: theme.colors.surface1,
          }}
        >
          <ScrollView style={{ maxHeight: 258 }} nestedScrollEnabled>
            {options.map((option) => (
              <Pressable
                key={option.value}
                accessibilityRole="button"
                onPress={() => {
                  onChange(option.value);
                  setOpen(false);
                }}
                style={{
                  paddingVertical: 9,
                  paddingHorizontal: 12,
                  backgroundColor: option.value === value ? theme.colors.accent : "transparent",
                }}
              >
                <Text
                  style={{
                    color: option.value === value ? theme.colors.accentForeground : theme.colors.foreground,
                  }}
                  numberOfLines={1}
                >
                  {option.label}
                </Text>
              </Pressable>
            ))}
            {options.length === 0 ? (
              <Text style={{ color: theme.colors.foregroundMuted, padding: 10 }}>暂无可选项</Text>
            ) : null}
          </ScrollView>
        </View>
      ) : null}
    </View>
  );
}
