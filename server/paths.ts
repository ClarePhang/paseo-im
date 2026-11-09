import { join } from "node:path";
import { homedir } from "node:os";

export const paseoHome: string = process.env.PASEO_HOME || join(homedir(), ".paseo");

export const dataRoot: string = join(paseoHome, "plugin-data", "paseo-im");

const UUID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

export function connectionRoot(id: string): string {
  if (!UUID_PATTERN.test(id)) throw new Error(`Invalid connection id: ${id}`);
  return join(dataRoot, "connections", id);
}

export function secretsRoot(): string {
  return join(dataRoot, "secrets");
}

export function secretPath(id: string): string {
  if (!UUID_PATTERN.test(id)) throw new Error(`Invalid connection id: ${id}`);
  return join(secretsRoot(), `${id}.json`);
}

export function pairingRoot(): string {
  return join(dataRoot, "pairing");
}

export function pairingPath(attemptId: string): string {
  if (!/^[a-z0-9-]{8,64}$/.test(attemptId)) throw new Error(`Invalid pairing attempt id: ${attemptId}`);
  return join(pairingRoot(), `${attemptId}.json`);
}

export function mediaInboxRoot(): string {
  return join(dataRoot, "media-inbox");
}

export function registryPath(): string {
  return join(dataRoot, "connections.json");
}
