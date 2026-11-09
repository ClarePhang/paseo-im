/** Promise-tail queue keyed by conversation and agent. Requests with the same
 * key run strictly in order; different keys run concurrently. */
export class AgentQueue {
  private readonly tails = new Map<string, Promise<unknown>>();
  private readonly queued = new Map<string, number>();

  run<T>(
    key: string,
    operation: (signal: AbortSignal) => Promise<T>,
    signal?: AbortSignal,
  ): Promise<T> {
    if (signal?.aborted) {
      return Promise.reject(signal.reason instanceof Error ? signal.reason : new Error("已中止"));
    }
    const previous = this.tails.get(key) ?? Promise.resolve();
    const gate = Promise.withResolvers<void>();
    this.tails.set(key, gate.promise);
    this.queued.set(key, (this.queued.get(key) ?? 0) + 1);
    const child = new AbortController();
    const onAbort = () => child.abort(signal!.reason);
    signal?.addEventListener("abort", onAbort, { once: true });
    const release = () => {
      this.queued.set(key, Math.max(0, (this.queued.get(key) ?? 1) - 1));
      gate.resolve();
    };
    const { promise: result, resolve, reject } = Promise.withResolvers<T>();
    const abortReason = () => (signal!.reason instanceof Error ? signal!.reason : new Error("已中止"));
    if (signal) {
      signal.addEventListener(
        "abort",
        () => {
          child.abort(signal.reason);
          reject(abortReason());
        },
        { once: true },
      );
    }
    const tail = previous.then(() => {
      if (child.signal.aborted) throw child.signal.reason instanceof Error ? child.signal.reason : new Error("已中止");
      return operation(child.signal);
    });
    tail.then(resolve, reject);
    tail.catch(() => {}).then(() => {
      signal?.removeEventListener("abort", onAbort);
      release();
    });
    return result;
  }

  size(): number {
    let total = 0;
    for (const value of this.queued.values()) total += value;
    return total;
  }

  async idle(): Promise<void> {
    const pending: Promise<unknown>[] = [];
    for (const tail of this.tails.values()) pending.push(tail.catch(() => {}));
    await Promise.all(pending);
  }
}
