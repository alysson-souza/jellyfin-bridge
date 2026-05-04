import test from "node:test";
import assert from "node:assert/strict";
import { startScanScheduler, type SchedulerTimers } from "../src/scheduler.js";

test("runs scheduled scans and can be stopped", async () => {
  const timers = new FakeTimers();
  let runs = 0;
  const scheduler = startScanScheduler(async () => {
    runs += 1;
  }, 1000, timers);

  assert.equal(timers.intervalMs, 1000);
  await timers.tick();
  assert.equal(runs, 1);

  scheduler.stop();
  assert.equal(timers.cleared, true);
});

test("skips overlapping scheduled scans", async () => {
  const timers = new FakeTimers();
  let release: (() => void) | undefined;
  let runs = 0;
  startScanScheduler(async () => {
    runs += 1;
    await new Promise<void>((resolve) => {
      release = resolve;
    });
  }, 1000, timers);

  const first = timers.tick();
  await Promise.resolve();
  const second = timers.tick();
  await second;

  assert.equal(runs, 1);
  release?.();
  await first;
});

test("logs scheduled scan failures without surfacing an unhandled rejection", async () => {
  const timers = new FakeTimers();
  const logger = new FakeLogger();
  const error = new Error("scan failed");
  startScanScheduler(async () => {
    throw error;
  }, 1000, timers, logger);

  await timers.tick();
  await Promise.resolve();

  assert.equal(logger.message, "Scheduled scan failed");
  assert.equal((logger.details as { error: Error }).error, error);
});

class FakeTimers implements SchedulerTimers {
  intervalMs = 0;
  cleared = false;
  private callback: (() => void) | undefined;

  setInterval(callback: () => void, intervalMs: number): unknown {
    this.callback = callback;
    this.intervalMs = intervalMs;
    return "interval";
  }

  clearInterval(_handle: unknown): void {
    this.cleared = true;
  }

  async tick(): Promise<void> {
    this.callback?.();
    await Promise.resolve();
  }
}

class FakeLogger {
  details: unknown;
  message: string | undefined;

  error(details: unknown, message?: string): void {
    this.details = details;
    this.message = message;
  }
}
