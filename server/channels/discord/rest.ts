/** Default REST transport for the Discord adapter: JSON calls with bot auth,
 * a connection-wide pause on 429 (retry_after) and provider error mapping.
 * Injectable so tests run without network. */
import { sleep } from "../../files.ts";
import { DISCORD_API_BASE, DiscordRestError, describeError } from "./protocol.ts";

export type RestCall = (method: string, path: string, body?: unknown) => Promise<unknown>;

export interface RestClientOptions {
  token: string;
  fetchImpl?: typeof fetch;
  signal?: AbortSignal;
  baseUrl?: string;
  maxRateLimitRetries?: number;
}

export function createRestClient(options: RestClientOptions): RestCall {
  const fetchImpl = options.fetchImpl ?? fetch;
  const baseUrl = options.baseUrl ?? DISCORD_API_BASE;
  const maxRetries = options.maxRateLimitRetries ?? 2;
  let pausedUntil = 0;

  return async function rest(method, path, body) {
    for (let attempt = 0; ; attempt += 1) {
      const waitMs = pausedUntil - Date.now();
      if (waitMs > 0) await sleep(Math.min(waitMs, 60_000), options.signal);

      let response: Response;
      try {
        response = await fetchImpl(`${baseUrl}${path}`, {
          method,
          headers: {
            Authorization: `Bot ${options.token}`,
            // Multipart bodies (FormData) must keep their generated boundary.
            ...(body instanceof FormData ? {} : { "Content-Type": "application/json" }),
          },
          body: body instanceof FormData ? body : body === undefined ? undefined : JSON.stringify(body),
          signal: options.signal,
        });
      } catch (error) {
        if (options.signal?.aborted) throw new Error("Discord 请求已取消（适配器已停止）。");
        throw new Error(`Discord 网络请求失败（${method} ${path}）：${describeError(error)}`);
      }

      if (response.status === 429) {
        // Undici types: json() is unknown; narrow the one-off shape at the boundary.
        const payload = (await response.json().catch(() => undefined)) as { retry_after?: unknown } | undefined;
        const retryAfter = Number(payload?.retry_after);
        const backoffMs =
          Number.isFinite(retryAfter) && retryAfter > 0 ? Math.ceil(retryAfter * 1000) : 1_000;
        pausedUntil = Date.now() + backoffMs;
        if (attempt >= maxRetries) {
          throw new DiscordRestError(429, undefined, "请求过于频繁（429），已达重试上限。");
        }
        continue;
      }

      if (!response.ok) {
        const payload = (await response.json().catch(() => undefined)) as { message?: unknown; code?: unknown } | undefined;
        const providerMessage =
          typeof payload?.message === "string" ? payload.message : `HTTP ${response.status}`;
        const providerCode = Number.isSafeInteger(payload?.code) ? Number(payload?.code) : undefined;
        throw new DiscordRestError(response.status, providerCode, providerMessage);
      }
      if (response.status === 204) return undefined;
      return await response.json();
    }
  };
}
