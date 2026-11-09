/** Shared local HTTP callback listener for every wecom-app connection: URL
 * verification (GET) and encrypted callback dispatch (POST).
 * Protocol facts ported from xmanrui/dsh-im (MIT). */
import { createServer, type IncomingMessage, type Server, type ServerResponse } from "node:http";

import {
  buildEncryptedReply,
  extractEnvelope,
  parseWecomPlainMessage,
  safeEqualHex,
  type CallbackFormat,
  type WecomAppInboundMessage,
  type WecomCryptoLike,
} from "./protocol.ts";

const DEFAULT_MAX_BODY_BYTES = 1024 * 1024;
/** WeCom expects the passive reply within ~5s; past that the adapter's reply
 * window has already closed, so drop the socket answer. */
const AUTO_EMPTY_MS = 5_500;

export interface WecomAppRoute {
  botId: string;
  callbackSecret: string;
  cryptoFor: () => WecomCryptoLike;
  /** Called once per accepted callback message. The handler MUST eventually
   * invoke reply (encrypted passive reply) or reply(null) (empty 200). */
  onMessage: (message: WecomAppInboundMessage, reply: (payload: { msgtype: "text"; text: { content: string } } | null) => void) => void;
}

export function routePath({ botId, callbackSecret }: { botId: string; callbackSecret: string }): string {
  return `/wecom-app-callback/${botId}/${callbackSecret}`;
}

function writeBody(res: ServerResponse, status: number, body: string, headers: Record<string, string> = {}): void {
  if (res.writableEnded || res.destroyed) return;
  res.writeHead(status, {
    "content-length": String(Buffer.byteLength(body)),
    "cache-control": "no-store",
    ...headers,
  });
  res.end(body);
}

function writeText(res: ServerResponse, status: number, text: string, headers: Record<string, string> = {}): void {
  writeBody(res, status, text, { "content-type": "text/plain; charset=utf-8", ...headers });
}

function writeEmpty(res: ServerResponse): void {
  if (res.writableEnded || res.destroyed) return;
  res.statusCode = 200;
  res.end();
}

/** ES2023 lib has no Promise.withResolvers; this is its local equivalent. */
function deferred<T>(): { promise: Promise<T>; resolve: (value: T) => void; reject: (reason: unknown) => void } {
  let resolve!: (value: T) => void;
  let reject!: (reason: unknown) => void;
  const promise = new Promise<T>((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}

/** One HTTP listener shared by every wecom-app connection. Each connection
 * registers a route keyed by its full callback path; the per-connection random
 * path segment doubles as the first gate before the WeCom signature check. */
export class WecomAppCallbackServer {
  readonly #host: string;
  readonly #port: number;
  readonly #log: (level: "info" | "warn" | "error", message: string) => void;
  readonly #maxBodyBytes: number;
  #server: Server | null = null;
  readonly #routes = new Map<string, WecomAppRoute>();

  constructor({ host = "127.0.0.1", port = 30987, log = () => {}, maxBodyBytes = DEFAULT_MAX_BODY_BYTES }: {
    host?: string;
    port?: number;
    log?: (level: "info" | "warn" | "error", message: string) => void;
    maxBodyBytes?: number;
  } = {}) {
    if (!Number.isInteger(port) || port < 0 || port > 65_535) {
      throw new TypeError("WecomAppCallbackServer requires an integer port between 0 and 65535");
    }
    this.#host = host;
    this.#port = port;
    this.#log = log;
    this.#maxBodyBytes = maxBodyBytes;
  }

  get listening(): boolean {
    return this.#server !== null && this.#server.listening;
  }

  /** The OS-assigned port after listen(0); falls back to the configured port. */
  get actualPort(): number {
    const bound = this.#server?.address();
    return typeof bound === "object" && bound !== null && "port" in bound ? bound.port : this.#port;
  }

  registerRoute(route: WecomAppRoute): void {
    if (!route.botId || !route.callbackSecret || typeof route.cryptoFor !== "function" || typeof route.onMessage !== "function") {
      throw new TypeError("A wecom-app route requires botId, callbackSecret, cryptoFor, and onMessage");
    }
    this.#routes.set(routePath(route), route);
  }

  unregisterRoute(botId: string): void {
    for (const [path, route] of this.#routes.entries()) {
      if (route.botId === botId) this.#routes.delete(path);
    }
  }

  async start(): Promise<void> {
    if (this.#server) return;
    const server = createServer((request, response) => {
      this.#handleRequest(request, response).catch((error) => {
        this.#log("warn", `wecom-app 回调请求处理失败：${error instanceof Error ? error.message : String(error)}`);
        writeText(response, 500, "internal error");
      });
    });
    server.on("clientError", (_error, socket) => {
      if (socket.writable) socket.end("HTTP/1.1 400 Bad Request\r\n\r\n");
    });
    const listening = deferred<void>();
    server.once("error", (error: Error) => listening.reject(error));
    server.once("listening", () => listening.resolve());
    server.listen(this.#port, this.#host);
    try {
      await listening.promise;
    } catch (error) {
      server.close();
      throw error;
    }
    this.#server = server;
  }

  async stop(): Promise<void> {
    const server = this.#server;
    this.#server = null;
    this.#routes.clear();
    if (!server) return;
    const closed = deferred<void>();
    server.close(() => closed.resolve());
    server.closeAllConnections?.();
    await closed.promise;
  }

  async #handleRequest(request: IncomingMessage, response: ServerResponse): Promise<void> {
    if (request.method !== "GET" && request.method !== "POST") {
      writeText(response, 405, "method not allowed", { allow: "GET, POST" });
      return;
    }
    const url = new URL(request.url ?? "/", `http://${request.headers.host ?? "localhost"}`);
    const route = this.#routes.get(url.pathname) ?? null;
    if (!route) {
      writeText(response, 404, "not found");
      return;
    }
    if (request.method === "GET") {
      this.#verifyUrl(route, url.searchParams, response);
      return;
    }
    await this.#handlePost(route, url.searchParams, request, response);
  }

  #verifyUrl(route: WecomAppRoute, query: URLSearchParams, response: ServerResponse): void {
    const timestamp = query.get("timestamp") ?? "";
    const nonce = query.get("nonce") ?? "";
    const echostr = query.get("echostr") ?? "";
    const signature = query.get("msg_signature") ?? query.get("msgsignature") ?? query.get("signature") ?? "";
    if (!timestamp || !nonce || !echostr || !signature) {
      writeText(response, 400, "missing query params");
      return;
    }
    let crypto: WecomCryptoLike;
    try {
      crypto = route.cryptoFor();
    } catch {
      writeText(response, 503, "route not ready");
      return;
    }
    try {
      if (!safeEqualHex(crypto.computeSignature(timestamp, nonce, echostr), signature)) {
        this.#log("warn", "wecom-app URL 验证签名不匹配");
        writeText(response, 401, "unauthorized");
        return;
      }
      writeText(response, 200, crypto.decrypt(echostr));
    } catch {
      writeText(response, 400, "verification failed");
    }
  }

  async #handlePost(route: WecomAppRoute, query: URLSearchParams, request: IncomingMessage, response: ServerResponse): Promise<void> {
    const body = await this.#readBody(request, response);
    if (body === null) return;
    const envelope = extractEnvelope(body, query);
    if (!envelope) {
      writeText(response, 400, "missing encrypted payload");
      return;
    }
    let crypto: WecomCryptoLike;
    try {
      crypto = route.cryptoFor();
    } catch {
      writeText(response, 503, "route not ready");
      return;
    }
    let plaintext: string;
    try {
      if (!safeEqualHex(crypto.computeSignature(envelope.timestamp, envelope.nonce, envelope.encrypt), envelope.signature)) {
        this.#log("warn", "wecom-app 回调签名不匹配");
        writeText(response, 401, "unauthorized");
        return;
      }
      plaintext = crypto.decrypt(envelope.encrypt);
    } catch {
      writeText(response, 401, "unauthorized");
      return;
    }
    const message = parseWecomPlainMessage(plaintext);
    if (!message) {
      writeText(response, 400, "invalid message");
      return;
    }
    this.#dispatch(route, message, envelope, response);
  }

  #readBody(request: IncomingMessage, response: ServerResponse): Promise<string | null> {
    const done = deferred<string | null>();
    const chunks: Buffer[] = [];
    let length = 0;
    let settled = false;
    const finish = (value: string | null) => {
      if (!settled) {
        settled = true;
        done.resolve(value);
      }
    };
    request.on("data", (chunk: Buffer) => {
      length += chunk.length;
      if (length > this.#maxBodyBytes) {
        writeText(response, 413, "payload too large");
        request.destroy();
        finish(null);
        return;
      }
      chunks.push(chunk);
    });
    request.on("end", () => {
      const raw = Buffer.concat(chunks, length).toString("utf8");
      if (!raw.trim()) {
        writeText(response, 400, "empty payload");
        finish(null);
        return;
      }
      finish(raw);
    });
    request.on("error", () => finish(null));
    return done.promise;
  }

  #dispatch(route: WecomAppRoute, message: WecomAppInboundMessage, envelope: { format: CallbackFormat; timestamp: string; nonce: string }, response: ServerResponse): void {
    let answered = false;
    const answer = (payload: { msgtype: "text"; text: { content: string } } | null) => {
      if (answered) return;
      answered = true;
      clearTimeout(guard);
      if (!payload) {
        writeEmpty(response);
        return;
      }
      try {
        const body = buildEncryptedReply({
          format: envelope.format,
          crypto: route.cryptoFor(),
          plaintext: payload,
          timestamp: envelope.timestamp,
          nonce: envelope.nonce,
        });
        writeBody(response, 200, body, {
          "content-type": envelope.format === "json"
            ? "application/json; charset=utf-8"
            : "application/xml; charset=utf-8",
        });
      } catch {
        writeEmpty(response);
      }
    };
    const guard = setTimeout(() => {
      if (!answered) {
        this.#log("warn", "wecom-app 被动回复超时，已回空响应");
        answer(null);
      }
    }, AUTO_EMPTY_MS);
    if (typeof guard.unref === "function") guard.unref();
    try {
      route.onMessage(message, answer);
    } catch (error) {
      this.#log("warn", `wecom-app 回调消息处理失败：${error instanceof Error ? error.message : String(error)}`);
      answer(null);
    }
  }
}

interface SharedServer {
  server: WecomAppCallbackServer;
  refs: number;
}

/** Port → shared listener. Multiple wecom-app connections on the same port
 * share one HTTP server; the last release closes it. */
const sharedServers = new Map<number, SharedServer>();

/** acquire/release 走同一条串行队列：冷端口并发 acquire 会同时 miss 共享表并
 * 各自 listen，第二个必然 EADDRINUSE；串行后后者命中表直接复用。 */
let serverOpChain: Promise<unknown> = Promise.resolve();
function enqueueServerOp<T>(operation: () => Promise<T>): Promise<T> {
  const run = serverOpChain.then(operation, operation);
  serverOpChain = run.catch(() => undefined);
  return run;
}

export function acquireCallbackServer({ host, port, log }: {
  host?: string;
  port: number;
  log: (level: "info" | "warn" | "error", message: string) => void;
}): Promise<WecomAppCallbackServer> {
  return enqueueServerOp(async () => {
    const existing = sharedServers.get(port);
    if (existing) {
      existing.refs += 1;
      return existing.server;
    }
    const server = new WecomAppCallbackServer({ host, port, log });
    await server.start();
    sharedServers.set(port, { server, refs: 1 });
    return server;
  });
}

export function releaseCallbackServer(port: number): Promise<void> {
  return enqueueServerOp(async () => {
    const existing = sharedServers.get(port);
    if (!existing) return;
    existing.refs -= 1;
    if (existing.refs > 0) return;
    sharedServers.delete(port);
    await existing.server.stop();
  });
}
