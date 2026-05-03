import type { ScanScheduler } from "./scheduler.js";

export interface ScanLogger {
  error(details: unknown, message?: string): void;
}

export function startScanOnStart(scan: () => Promise<void>, scheduler?: Pick<ScanScheduler, "runNow">, logger?: ScanLogger): void {
  const run = scheduler
    ? scheduler.runNow().then(() => undefined)
    : scan();
  void run.catch((error: unknown) => {
    logger?.error({ error }, "Startup scan failed");
  });
}
