import test from "node:test";
import assert from "node:assert/strict";
import { startScanOnStart, type ScanLogger } from "../src/startupScan.js";

test("starts scan-on-start in the background", async () => {
  let runs = 0;
  let release: (() => void) | undefined;

  startScanOnStart(async () => {
    runs += 1;
    await new Promise<void>((resolve) => {
      release = resolve;
    });
  });

  assert.equal(runs, 1);
  release?.();
});

test("uses scheduler runNow for scan-on-start when periodic scans are configured", async () => {
  let directRuns = 0;
  let schedulerRuns = 0;

  startScanOnStart(async () => {
    directRuns += 1;
  }, {
    async runNow() {
      schedulerRuns += 1;
      return true;
    }
  });

  await Promise.resolve();

  assert.equal(directRuns, 0);
  assert.equal(schedulerRuns, 1);
});

test("logs background scan failures without throwing synchronously", async () => {
  const logger = new FakeLogger();
  const error = new Error("scan failed");

  startScanOnStart(async () => {
    throw error;
  }, undefined, logger);

  await Promise.resolve();
  await Promise.resolve();

  assert.equal(logger.message, "Startup scan failed");
  assert.equal((logger.details as { error: Error }).error, error);
});

class FakeLogger implements ScanLogger {
  details: unknown;
  message: string | undefined;

  error(details: unknown, message?: string): void {
    this.details = details;
    this.message = message;
  }
}
