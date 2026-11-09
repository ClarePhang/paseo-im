import { chmod, mkdir, readFile, rename, writeFile } from "node:fs/promises";
import { randomUUID } from "node:crypto";
import { dirname } from "node:path";

/** Read a JSON document; missing files resolve to undefined. Other failures get
 * a user-facing message because every caller stores state the UI reads. */
export async function readJson<T>(
  path: string,
  parse: (value: unknown) => T,
): Promise<T | undefined> {
  let raw: string;
  try {
    raw = await readFile(path, "utf8");
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return undefined;
    throw new Error(`无法读取 ${path}: ${(error as Error).message}`);
  }
  try {
    return parse(JSON.parse(raw));
  } catch (error) {
    throw new Error(`无法解析 ${path}: ${(error as Error).message}`);
  }
}

/** Atomic write: 0700 directory, 0600 temp file with "wx", then rename. */
export async function atomicWrite(path: string, value: unknown): Promise<void> {
  const dir = dirname(path);
  await mkdir(dir, { recursive: true, mode: 0o700 });
  await chmod(dir, 0o700).catch(() => {});
  const tmp = `${path}.${randomUUID()}.tmp`;
  await writeFile(tmp, JSON.stringify(value, null, 2), { encoding: "utf8", mode: 0o600, flag: "wx" });
  await rename(tmp, path);
}

/** Serialize mutations on one resource: each callback runs only after the
 * previous one settles, so concurrent RPCs and bridge writes cannot interleave. */
export class WriteChain {
  private tail: Promise<unknown> = Promise.resolve();

  run<T>(operation: () => Promise<T>): Promise<T> {
    const next = this.tail.then(operation, operation);
    this.tail = next.catch(() => {});
    return next;
  }

  /** Resolves once every enqueued operation has settled (success or failure).
   * Stop producing writes before draining; afterwards rm/shutdown cannot race
   * a pending persist (same quiescence contract as the weixin pairing stop). */
  drain(): Promise<void> {
    return this.tail.then(() => {});
  }
}

export async function sleep(ms: number, signal?: AbortSignal): Promise<void> {
  const { promise, resolve, reject } = Promise.withResolvers<void>();
  const timer = setTimeout(resolve, ms);
  const onAbort = () => {
    clearTimeout(timer);
    reject(signal?.reason instanceof Error ? signal.reason : new Error("Aborted"));
  };
  signal?.addEventListener("abort", onAbort, { once: true });
  try {
    await promise;
  } finally {
    clearTimeout(timer);
    signal?.removeEventListener("abort", onAbort);
  }
}
