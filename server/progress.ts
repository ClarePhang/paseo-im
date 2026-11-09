import type { ChannelAdapter, AdapterContext } from "./channels/types.ts";
import type { SendResult } from "../shared/model.ts";

const PLACEHOLDER = "⏳ 正在处理…";

/** Per-conversation progress lifecycle driven by the bridge. The backend
 * follows the channel capability: edit in place, native stream, typing
 * indicator, or nothing. */
export interface ProgressSink {
  readonly messageId: string | null;
  start(): Promise<void>;
  refresh(elapsedSeconds: number, activity?: string): Promise<void>;
  /** In-place completion for edit/stream channels; null means the bridge must
   * deliver every chunk itself. */
  finish(answer: string): Promise<SendResult | null>;
  abandon(): Promise<void>;
}

export function createProgressSink(
  adapter: ChannelAdapter,
  chatId: string,
  threadId: string | undefined,
  log: AdapterContext["log"],
): ProgressSink {
  const mode = adapter.capabilities.progress;
  let messageId: string | null = null;
  let streamId: string | null = null;

  async function editInPlace(text: string): Promise<void> {
    if (!messageId) return;
    await adapter.editMessage!(messageId, chatId, text).catch((error) => {
      log("warn", `进度消息更新失败：${(error as Error).message}`);
    });
  }

  return {
    get messageId() {
      return messageId;
    },
    async start() {
      if (mode === "none") return;
      if (mode === "typing") {
        await adapter.sendTyping?.(chatId).catch(() => {});
        return;
      }
      if (mode === "edit") {
        const sent = await adapter.sendText({ chatId, threadId, text: PLACEHOLDER });
        messageId = sent.messageId ?? null;
        return;
      }
      if (adapter.streamStart) {
        const stream = await adapter.streamStart(chatId, threadId);
        streamId = stream.streamId;
      }
    },
    async refresh(elapsedSeconds, activity) {
      if (mode === "typing") {
        await adapter.sendTyping?.(chatId).catch(() => {});
        return;
      }
      const line = activity ? `⏳ ${elapsedSeconds}s · ${activity}` : `⏳ ${elapsedSeconds}s`;
      if (mode === "edit") return editInPlace(line);
      if (streamId && adapter.streamAppend) {
        await adapter.streamAppend(streamId, line).catch(() => {});
      }
    },
    async finish(answer): Promise<SendResult | null> {
      if (mode === "edit" && messageId) {
        await editInPlace(answer);
        return { messageId };
      }
      if (mode === "stream" && streamId && adapter.streamFinish) {
        const result = await adapter.streamFinish(streamId, answer);
        return { messageId: result.messageId };
      }
      return null;
    },
    async abandon() {
      if (mode === "edit" && messageId) {
        await editInPlace("任务未完成，请在 Paseo 中查看详情。");
      }
    },
  };
}
