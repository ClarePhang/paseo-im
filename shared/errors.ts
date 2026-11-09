/** Errors whose message may be shown verbatim to users in UI and on IM. */
export class UserFacingError extends Error {}

export function userMessage(error: unknown): string {
  if (error instanceof UserFacingError) return error.message;
  return "任务需要在 Paseo 中处理，请打开 Paseo 查看详情。";
}
