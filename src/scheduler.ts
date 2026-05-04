export interface ScanScheduler {
  runNow(): Promise<boolean>;
  stop(): void;
}

export interface SchedulerTimers {
  setInterval(callback: () => void, intervalMs: number): unknown;
  clearInterval(handle: unknown): void;
}

export interface ScanLogger {
  error(details: unknown, message?: string): void;
}

export function startScanScheduler(
  scan: () => Promise<void>,
  intervalMs: number,
  timers: SchedulerTimers = globalThis,
  logger?: ScanLogger
): ScanScheduler {
  if (!Number.isFinite(intervalMs) || intervalMs <= 0) {
    throw new Error("Scan interval must be greater than zero");
  }

  let running = false;
  const runNow = async (): Promise<boolean> => {
    if (running) return false;
    running = true;
    try {
      await scan();
      return true;
    } finally {
      running = false;
    }
  };

  const handle = timers.setInterval(() => {
    void runNow().catch((error: unknown) => {
      logger?.error({ error }, "Scheduled scan failed");
    });
  }, intervalMs);
  if (handle && typeof handle === "object" && "unref" in handle && typeof handle.unref === "function") {
    handle.unref();
  }

  return {
    runNow,
    stop: () => timers.clearInterval(handle)
  };
}
