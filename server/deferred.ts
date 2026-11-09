/** Timeout recovery registry: jobs whose first wait exceeded the budget keep
 * polling; their final text is delivered late, once. */
export class DeferredRegistry {
  private readonly tracked = new Map<string, true>();
  private readonly handlers: { sweep: (jobId: string) => Promise<boolean>; intervalMs?: number };
  private timer: NodeJS.Timeout | undefined;
  private running = false;

  constructor(handlers: { sweep: (jobId: string) => Promise<boolean>; intervalMs?: number }) {
    this.handlers = handlers;
  }

  track(jobId: string): void {
    this.tracked.set(jobId, true);
    this.ensureTimer();
  }

  resolve(jobId: string): void {
    this.tracked.delete(jobId);
  }

  count(): number {
    return this.tracked.size;
  }

  private ensureTimer(): void {
    if (this.timer || this.running) return;
    const intervalMs = this.handlers.intervalMs ?? 60_000;
    this.timer = setInterval(() => {
      void this.sweepOnce();
    }, intervalMs);
    this.timer.unref?.();
  }

  private async sweepOnce(): Promise<void> {
    if (this.running) return;
    this.running = true;
    try {
      for (const jobId of [...this.tracked.keys()]) {
        try {
          const done = await this.handlers.sweep(jobId);
          if (done) this.tracked.delete(jobId);
        } catch {
          // Sweep failures retry on the next tick; the job stays tracked.
        }
      }
    } finally {
      this.running = false;
      if (!this.tracked.size) {
        clearInterval(this.timer);
        this.timer = undefined;
      }
    }
  }

  stop(): void {
    clearInterval(this.timer);
    this.timer = undefined;
    this.tracked.clear();
  }
}
