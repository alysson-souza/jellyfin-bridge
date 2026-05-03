import test from "node:test";
import assert from "node:assert/strict";
import { installShutdownHandlers, type SignalProcess } from "../src/lifecycle.js";

test("shutdown handlers close scheduler, app, and store before exiting", async () => {
  const process = new FakeProcess();
  const calls: string[] = [];
  installShutdownHandlers(
    process,
    { close: async () => calls.push("app") },
    { close: () => calls.push("store") },
    { stop: () => calls.push("scheduler") }
  );

  process.emit("SIGTERM");
  await Promise.resolve();

  assert.deepEqual(calls, ["scheduler", "app", "store"]);
  assert.equal(process.exitCode, 0);
});

test("shutdown handlers only run once when multiple signals arrive", async () => {
  const process = new FakeProcess();
  let release: (() => void) | undefined;
  let closes = 0;
  installShutdownHandlers(
    process,
    {
      close: async () => {
        closes += 1;
        await new Promise<void>((resolve) => {
          release = resolve;
        });
      }
    },
    { close: () => undefined }
  );

  process.emit("SIGINT");
  process.emit("SIGTERM");
  await Promise.resolve();
  release?.();
  await new Promise((resolve) => setImmediate(resolve));

  assert.equal(closes, 1);
  assert.equal(process.exitCode, 0);
});

class FakeProcess implements SignalProcess {
  exitCode: number | undefined;
  private readonly listeners: Record<string, Array<() => void>> = {};

  on(event: "SIGINT" | "SIGTERM", listener: () => void): unknown {
    this.listeners[event] ??= [];
    this.listeners[event].push(listener);
    return this;
  }

  exit(code?: number): never {
    this.exitCode = code;
    return undefined as never;
  }

  emit(event: "SIGINT" | "SIGTERM"): void {
    for (const listener of this.listeners[event] ?? []) {
      listener();
    }
  }
}
