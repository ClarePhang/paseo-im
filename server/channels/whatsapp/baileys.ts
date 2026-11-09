/** Runtime loader for the Baileys module. Static imports would let the daemon
 * bundle inline pino/thread-stream/protobufjs, whose bare `__dirname` usage
 * breaks inside the daemon's CJS wrapper. */
import { loadPackage } from "../runtime-loader.ts";

export type Baileys = typeof import("@whiskeysockets/baileys");

let cached: Baileys | null = null;

export async function baileys(): Promise<Baileys> {
  if (!cached) {
    cached = await loadPackage<Baileys>("@whiskeysockets/baileys");
  }
  return cached;
}

/** Non-null once {@link baileys} has resolved (adapter start awaits it before
 * wiring socket events). Pure helpers use this with a string fallback. */
export function baileysSync(): Baileys | null {
  return cached;
}
