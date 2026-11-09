/** WhatsApp QR pairing flow: emits QR data URLs, resolves ok with the
 * normalized account Jid once the phone links, and leaves credentials in
 * <dataDir>/whatsapp-auth for the regular adapter to pick up.
 * Protocol facts ported from xmanrui/dsh-im (MIT). */

import { join } from "node:path";
import { mkdir } from "node:fs/promises";
import type { AuthenticationState, ConnectionState } from "@whiskeysockets/baileys";
import { WriteChain } from "../../files.ts";
import type { PairingSession, PairingStatus } from "../types.ts";
import { AUTH_DIR_NAME, disconnectStatus, hardenAuthDirectory, makeDefaultDeps, socketConfigPreset, type WhatsappDeps, type WhatsappSocket } from "./adapter.ts";
import { baileys, type Baileys } from "./baileys.ts";

/** WhatsApp linked accounts are numeric jids on the PN or LID server. */
const ACCOUNT_JID_PATTERN = /^\d{5,32}@(s\.whatsapp\.net|lid)$/;

export class WhatsappPairingSession implements PairingSession {
  private status: PairingStatus = { status: "pending" };
  private auth?: { state: AuthenticationState; saveCreds: () => Promise<void> };
  private socket?: WhatsappSocket;
  private readonly saveChain = new WriteChain();
  private started = false;
  private stopped = false;
  private generation = 0;
  private qrShown = false;
  readonly authDir: string;
  private deps!: WhatsappDeps;
  #b!: Baileys;

  constructor(_connectionId: string, dataDir: string, deps?: WhatsappDeps) {
    if (deps) this.deps = deps;
    this.authDir = join(dataDir, AUTH_DIR_NAME);
  }

  async start(): Promise<void> {
    if (this.started) return;
    this.started = true;
    if (this.stopped) return;
    await this.spawnSocket();
  }

  async poll(): Promise<PairingStatus> {
    return this.status;
  }

  async stop(): Promise<void> {
    if (this.stopped) return;
    this.stopped = true;
    this.generation += 1;
    await this.closeSocket();
  }

  private async closeSocket(): Promise<void> {
    const socket = this.socket;
    this.socket = undefined;
    if (!socket) return;
    try {
      await socket.end(undefined);
    } catch {
      // Socket already gone; pairing state is unaffected.
    }
  }

  private async spawnSocket(): Promise<void> {
    this.deps ??= await makeDefaultDeps();
    this.#b = await baileys();
    await mkdir(this.authDir, { recursive: true, mode: 0o700 });
    await hardenAuthDirectory(this.authDir);
    const { state, saveCreds } = await this.deps.loadAuthState(this.authDir);
    this.auth = { state, saveCreds };
    const generation = ++this.generation;
    const socket = this.deps.createSocket(socketConfigPreset(this.#b, state, await this.resolveVersion()));
    this.socket = socket;

    socket.ev.on("creds.update", () => {
      if (generation !== this.generation || this.stopped) return;
      this.saveChain
        .run(async () => {
          await saveCreds();
          await hardenAuthDirectory(this.authDir);
        })
        .catch(() => {});
      this.onCredsRegistered(generation, state);
    });
    socket.ev.on("connection.update", (raw) => void this.onConnectionUpdate(generation, raw as Partial<ConnectionState>));
  }

  private async resolveVersion(): Promise<readonly [number, number, number] | undefined> {
    try {
      return await this.deps.fetchVersion();
    } catch {
      return undefined;
    }
  }

  private onCredsRegistered(generation: number, state: AuthenticationState): void {
    if (generation !== this.generation || this.stopped) return;
    if (!this.qrShown || state.creds.registered !== true) return;
    if (this.status.status === "pending") this.status = { status: "scanned" };
  }

  private async onConnectionUpdate(generation: number, update: Partial<ConnectionState>): Promise<void> {
    if (generation !== this.generation || this.stopped) return;
    if (typeof update.qr === "string" && update.qr) {
      this.qrShown = true;
      try {
        this.status = { status: "pending", qrDataUrl: await this.deps.makeQr(update.qr) };
      } catch (error) {
        this.status = { status: "failed", detail: `生成二维码失败: ${(error as Error).message}` };
      }
      return;
    }
    if (update.connection === "open") {
      await this.completeIfLinked(generation);
      return;
    }
    if (update.connection === "close") {
      const status = disconnectStatus(update.lastDisconnect?.error);
      if (status === this.#b.DisconnectReason.restartRequired) {
        // Baileys requests a socket restart mid-registration; retry once.
        try {
          await this.closeSocket();
          await this.spawnSocket();
        } catch (error) {
          this.status = { status: "failed", detail: `WhatsApp 配对连接失败: ${(error as Error).message}` };
        }
        return;
      }
      await this.closeSocket();
      this.status = {
        status: "failed",
        detail:
          status === this.#b.DisconnectReason.loggedOut
            ? "WhatsApp 拒绝了本次配对，请稍后重新发起并再次扫码。"
            : `WhatsApp 连接中断（status=${status ?? "未知"}），请重新发起配对。`,
      };
    }
  }

  private async completeIfLinked(generation: number): Promise<void> {
    const identity = this.socket?.user ?? this.auth?.state.creds.me;
    const accountJid = this.#b.jidNormalizedUser(identity?.id);
    if (!identity?.id || !ACCOUNT_JID_PATTERN.test(accountJid)) {
      // Open but identity not registered yet; creds.update completes the flow.
      return;
    }
    if (generation !== this.generation || this.stopped) return;
    try {
      await this.saveChain.run(async () => {
        await this.auth?.saveCreds();
        await hardenAuthDirectory(this.authDir);
      });
    } catch (error) {
      this.status = { status: "failed", detail: `保存 WhatsApp 凭据失败: ${(error as Error).message}` };
      await this.closeSocket();
      return;
    }
    this.status = { status: "ok", fields: { accountJid }, detail: "已关联 WhatsApp 设备。" };
    await this.closeSocket();
  }
}
