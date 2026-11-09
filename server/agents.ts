import type { PaseoAgentHandle, PaseoAgentRunResult } from "@getpaseo/client";
import { z } from "zod";
import { UserFacingError } from "../shared/errors.ts";
import { sleep } from "./files.ts";

export type Agent = PaseoAgentHandle;
export type WaitResult = PaseoAgentRunResult;

/** A stored `closed` snapshot is not necessarily unavailable: reading the
 * timeline lets Paseo resume the saved session without sending a prompt. */
export async function refreshLinkedAgent(agent: Agent) {
  let snapshot = await agent.refresh();
  if (snapshot && !snapshot.agent.archivedAt && snapshot.agent.status === "closed") {
    const timeline = await agent.timeline.refetch({ direction: "tail", limit: 1 });
    if (timeline.error) {
      throw new UserFacingError("关联的 Paseo 聊天无法恢复，请先在 Paseo 中检查其提供方与工作区。");
    }
    snapshot = await agent.refresh();
  }
  return snapshot;
}

export function abortable<T>(task: Promise<T>, signal: AbortSignal, label = "接收器已停止"): Promise<T> {
  signal.throwIfAborted();
  const { promise, resolve, reject } = Promise.withResolvers<T>();
  const onAbort = () => reject(new Error(label));
  signal.addEventListener("abort", onAbort, { once: true });
  task.then(resolve, reject).finally(() => signal.removeEventListener("abort", onAbort));
  return promise;
}

export interface QuestionField {
  header: string;
  text: string;
  options: string[];
}

export interface ParsedQuestion {
  requestId: string;
  fields: QuestionField[];
}

const questionInputSchema = z.object({
  questions: z.array(
    z.object({
      header: z.string(),
      question: z.string(),
      options: z.array(z.object({ label: z.string() })).optional(),
    }),
  ),
});

/** Native clarification questions arrive as permission requests of kind
 * "question"; their metadata shape is validated once here. */
export function questionFrom(request: {
  id: string;
  kind?: string;
  name?: string;
  input?: unknown;
}): ParsedQuestion | undefined {
  if (request.kind !== "question") return undefined;
  const parsed = questionInputSchema.safeParse(request.input);
  if (!parsed.success) return undefined;
  const fields = parsed.data.questions.map((value) => ({
    header: value.header,
    text: value.question,
    options: (value.options ?? []).map((option) => option.label),
  }));
  if (fields.length === 0 || new Set(fields.map((field) => field.header)).size !== fields.length) {
    return undefined;
  }
  return { requestId: request.id, fields };
}

export function questionText(question: ParsedQuestion): string {
  const body = question.fields
    .map((field, index) => {
      const prefix = question.fields.length > 1 ? `${index + 1}. ` : "";
      const options = field.options.length
        ? `\n${field.options.map((option, i) => `  ${i + 1}) ${option}`).join("\n")}`
        : "";
      return `${prefix}${field.text}${options}`;
    })
    .join("\n\n");
  const instruction =
    question.fields.length === 1
      ? "回复本条消息并给出选项序号或你的答案。"
      : "回复本条消息，每行一个答案，格式：1: 答案，2: 答案。";
  return `需要澄清：\n\n${body}\n\n${instruction}`;
}

export function questionAnswers(
  question: ParsedQuestion,
  text: string,
): Record<string, string> | undefined {
  const values =
    question.fields.length === 1
      ? [text.trim()]
      : question.fields.map((_, index) => {
          const matches = text
            .split("\n")
            .map((line) => new RegExp(`^\\s*${index + 1}\\s*[:.)]\\s*(.+)$`).exec(line))
            .filter(Boolean);
          return matches.length === 1 ? matches[0]![1]!.trim() : "";
        });
  if (values.some((value) => !value)) return undefined;
  const entries = question.fields.map((field, index) => {
    const value = values[index]!;
    const number = field.options.length && /^\d+$/.test(value) ? Number(value) : NaN;
    if (Number.isFinite(number) && (number < 1 || number > field.options.length)) {
      return undefined;
    }
    return [field.header, Number.isFinite(number) ? field.options[number - 1]! : value] as const;
  });
  const collected: Record<string, string> = {};
  for (const [index, entry] of entries.entries()) {
    if (!entry) return undefined;
    collected[question.fields[index]!.header] = entry[1];
  }
  return collected;
}

interface PendingQuestionRequest {
  id: string;
  kind?: string;
  name?: string;
  input?: unknown;
}

function pendingQuestions(result: WaitResult): PendingQuestionRequest[] {
  const final = result.final;
  if (!final || typeof final !== "object" || Array.isArray(final)) return [];
  if (!("pendingPermissions" in final)) return [];
  const permissions = final.pendingPermissions;
  if (!Array.isArray(permissions)) return [];
  return permissions.flatMap((request): PendingQuestionRequest[] => {
    if (request && typeof request === "object" && "id" in request && typeof (request as { id?: unknown }).id === "string") {
      return [request as PendingQuestionRequest];
    }
    return [];
  });
}

/** Wait for the current turn, relaying async question forms through the
 * callback the moment they appear. Blocking permissions end the wait. */
export async function waitForTask(
  agent: Agent,
  signal: AbortSignal,
  onAsyncQuestion?: (question: ParsedQuestion) => Promise<void>,
  priorQuestions: ReadonlySet<string> = new Set(),
): Promise<WaitResult> {
  while (!signal.aborted) {
    const result = await abortable(agent.waitForFinish(60_000), signal);
    if (result.status === "timeout") continue;
    if (result.status !== "permission") return result;
    const permissions = pendingQuestions(result);
    const blocking = permissions.some(
      (request) => request.kind !== "question" || request.name !== "request_user_input_async",
    );
    if (blocking || !permissions.length) return result;
    if (result.final && typeof result.final === "object" && "status" in result.final && result.final.status === "error") {
      return result;
    }
    const question = permissions
      .filter((request) => !priorQuestions.has(request.id))
      .map(questionFrom)
      .find((value) => value !== undefined);
    if (question && onAsyncQuestion) await onAsyncQuestion(question);
    await sleep(1000, signal);
  }
  throw new Error("接收器已重载");
}

/** A permission RPC acknowledgement must land before an idle agent receives
 * its follow-up, otherwise the form is answered for nothing. */
export async function waitForAnswerTurn(
  agent: Agent,
  after: { epoch: string; seq: number },
  signal: AbortSignal,
): Promise<void> {
  while (!signal.aborted) {
    const timeline = await agent.timeline.refetch({ limit: 30 });
    if (timeline.epoch !== after.epoch) {
      throw new Error("问题回复的时间线已变化，请在 Paseo 中查看该聊天");
    }
    if (timeline.entries.some((entry) => entry.seqEnd > after.seq && entry.item.type === "user_message")) {
      return;
    }
    await sleep(250, signal);
  }
  throw new Error("接收器已重载");
}
