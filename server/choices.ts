/** choices RPC 的纯映射：providers.snapshot 的 entries → 选择器模型。
 * 不可用/无模型的 provider 不进选择器（验收期发现 Muse 空模型项导致"随机选到不可用模型"）。 */

export interface ProviderChoice {
  id: string;
  label: string;
  models: { id: string; label: string; modes: { id: string; label: string }[]; thinking: { id: string; label: string }[] }[];
}

interface ProviderEntryLike {
  provider?: string;
  label?: string;
  enabled?: boolean;
  status?: string;
  models?: unknown[];
  modes?: { id?: string; label?: string }[] | null;
}

export function mapProviderChoices(providerEntries: unknown[]): ProviderChoice[] {
  return providerEntries.flatMap((entry) => {
    const provider = entry as ProviderEntryLike;
    if (!provider.provider) return [];
    const models = (provider.models ?? []).flatMap((model) => {
      const record = model as {
        model?: string;
        id?: string;
        label?: string;
        modes?: { id?: string; label?: string }[] | null;
        thinkingOptions?: { id?: string; label?: string }[] | null;
      };
      const id = record.model ?? record.id;
      if (!id) return [];
      const modes = (record.modes ?? provider.modes ?? []).flatMap((mode) =>
        mode.id ? [{ id: mode.id, label: mode.label ?? mode.id }] : [],
      );
      const thinking = (record.thinkingOptions ?? []).flatMap((option) =>
        option.id ? [{ id: option.id, label: option.label ?? option.id }] : [],
      );
      return [{ id, label: record.label ?? id, modes, thinking }];
    });
    // 不可用/无模型的 provider（如未配置的插件 provider）不进选择器
    if (!models.length) return [];
    return [
      {
        id: provider.provider,
        label: provider.label ?? provider.provider,
        models,
      },
    ];
  });
}
