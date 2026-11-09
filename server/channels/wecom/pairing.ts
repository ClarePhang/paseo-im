/** WeCom smart-bot QR pairing session.
 * Protocol facts ported from xmanrui/dsh-im (MIT) (qr-auth.mjs): the flow hits
 * GET https://work.weixin.qq.com/ai/qc/generate → { data: { scode, auth_url } },
 * then polls GET https://work.weixin.qq.com/ai/qc/query_result?scode=… →
 * { data: { status, bot_info: { botid, secret } } }. The QR lives five minutes;
 * poll every three seconds. Field names match shared/channels.ts: botId, secret. */
import QRCode from "qrcode";
import { UserFacingError } from "../../../shared/errors.ts";
import type { PairingSession, PairingStatus } from "../types.ts";

const GENERATE_URL = "https://work.weixin.qq.com/ai/qc/generate";
const POLL_URL = "https://work.weixin.qq.com/ai/qc/query_result";
const QR_TTL_MS = 5 * 60_000;
const REQUEST_TIMEOUT_MS = 10_000;

export interface WecomPairingDeps {
  fetch?: typeof globalThis.fetch;
  now?: () => number;
}

interface PairingAttempt {
  scode: string;
  verificationUrl: string;
  qrDataUrl?: string;
  expiresAt: number;
  failure?: string;
}

function cleanString(value: unknown): string | null {
  return typeof value === "string" && value.trim() ? value.trim() : null;
}

/** Only accept the official verification host: the QR must not point elsewhere. */
function safeVerificationUrl(value: unknown): string | null {
  const raw = cleanString(value);
  if (!raw) return null;
  try {
    const url = new URL(raw);
    return url.protocol === "https:" && url.hostname === "work.weixin.qq.com" && (!url.port || url.port === "443")
      ? url.href
      : null;
  } catch {
    return null;
  }
}

async function requestJson(fetchImpl: typeof globalThis.fetch, url: string, signal: AbortSignal): Promise<any> {
  const response = await fetchImpl(url, {
    method: "GET",
    redirect: "error",
    signal: AbortSignal.any([signal, AbortSignal.timeout(REQUEST_TIMEOUT_MS)]),
    headers: { accept: "application/json" },
  });
  if (!response.ok) {
    throw new UserFacingError(`企业微信配对服务返回 HTTP ${response.status}，请稍后重试。`);
  }
  return response.json();
}

export function createPairingSession(
  _attemptId: string,
  _dataDir: string,
  deps: WecomPairingDeps = {},
): PairingSession {
  const fetchImpl = deps.fetch ?? globalThis.fetch;
  const now = deps.now ?? Date.now;
  const controller = new AbortController();
  let attempt: PairingAttempt | null = null;
  let started = false;

  return {
    async start(): Promise<void> {
      started = true;
      try {
        const url = new URL(GENERATE_URL);
        url.searchParams.set("source", "paseo-im");
        url.searchParams.set("plat", process.platform === "win32" ? "2" : process.platform === "linux" ? "3" : "1");
        const body = await requestJson(fetchImpl, url.href, controller.signal);
        const scode = cleanString(body?.data?.scode);
        const verificationUrl = safeVerificationUrl(body?.data?.auth_url);
        if (!scode || !verificationUrl) {
          throw new UserFacingError("企业微信配对服务返回的数据无效，请稍后重试。");
        }
        let qrDataUrl: string | undefined;
        try {
          qrDataUrl = await QRCode.toDataURL(verificationUrl);
        } catch {
          qrDataUrl = undefined;
        }
        attempt = { scode, verificationUrl, qrDataUrl, expiresAt: now() + QR_TTL_MS };
      } catch (error) {
        if (controller.signal.aborted) throw error;
        attempt = {
          scode: "",
          verificationUrl: "",
          expiresAt: now(),
          failure: error instanceof UserFacingError ? error.message : `企业微信配对二维码生成失败：${(error as Error).message}`,
        };
      }
    },

    async poll(): Promise<PairingStatus> {
      if (!started) throw new UserFacingError("配对流程尚未启动。");
      if (attempt?.failure) return { status: "failed", detail: attempt.failure };
      if (!attempt) return { status: "pending", detail: "正在获取配对二维码…" };
      if (now() >= attempt.expiresAt) return { status: "expired" };

      const url = new URL(POLL_URL);
      url.searchParams.set("scode", attempt.scode);
      const body = await requestJson(fetchImpl, url.href, controller.signal);
      const state = cleanString(body?.data?.status)?.toLowerCase();
      if (state === "success") {
        const botId = cleanString(body?.data?.bot_info?.botid);
        const secret = cleanString(body?.data?.bot_info?.secret);
        if (!botId || !secret) {
          return { status: "failed", detail: "企业微信配对结果缺少 Bot ID 或 Secret，请改用手动填写。" };
        }
        return { status: "ok", fields: { botId, secret } };
      }
      if (state === "expired" || state === "timeout") return { status: "expired" };
      if (state === "fail" || state === "failed" || state === "error") {
        return { status: "failed", detail: "企业微信扫码配对失败，请重新发起。" };
      }
      return { status: "pending", qrDataUrl: attempt.qrDataUrl, detail: "请使用企业微信扫描二维码完成配对。" };
    },

    async stop(): Promise<void> {
      controller.abort(new Error("配对已取消"));
      attempt = null;
    },
  };
}
