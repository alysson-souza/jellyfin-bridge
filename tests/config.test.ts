import test from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, rename, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { tmpdir } from "node:os";
import {
  expandEnv,
  parseConfig,
  RuntimeConfig,
  startRuntimeConfigReload,
  type BridgeConfig,
  type ConfigWatcher,
  isRuntimeConfigWatchEvent,
  type RuntimeConfigWatchOptions
} from "../src/config.js";

test("expands environment variables in config values", () => {
  const raw = "token: ${TOKEN}\nfallback: ${MISSING:-default}\n";

  assert.equal(expandEnv(raw, { TOKEN: "secret" }), "token: secret\nfallback: default\n");
});

test("rejects missing environment variables", () => {
  assert.throws(() => expandEnv("token: ${TOKEN}", {}), /Missing environment variable TOKEN/);
});

test("validates duplicate upstream library mappings", () => {
  const raw = `
server:
  bind: 0.0.0.0
  port: 8096
  name: Bridge
auth:
  users:
    - name: alice
      passwordHash: hash
upstreams:
  - id: main
    name: Main
    url: https://example.com
    token: token
libraries:
  - id: movies
    name: Movies
    collectionType: movies
    sources:
      - server: main
        libraryId: abc
  - id: other
    name: Other
    collectionType: movies
    sources:
      - server: main
        libraryId: abc
`;

  assert.throws(() => parseConfig(raw, {}), /main:abc is mapped more than once/);
});

test("validates unknown upstream references", () => {
  const raw = `
server:
  bind: 0.0.0.0
  port: 8096
  name: Bridge
auth:
  users:
    - name: alice
      passwordHash: hash
upstreams:
  - id: main
    name: Main
    url: https://example.com
    token: token
libraries:
  - id: movies
    name: Movies
    collectionType: movies
    sources:
      - server: remote
        libraryId: abc
`;

  assert.throws(() => parseConfig(raw, {}), /unknown upstream remote/);
});

test("parses optional scan tuning", () => {
  const raw = `
server:
  bind: 0.0.0.0
  port: 8096
  name: Bridge
auth:
  users:
    - name: alice
      passwordHash: hash
upstreams:
  - id: main
    name: Main
    url: https://example.com
    token: token
scan:
  pageSize: 750
  concurrency: 3
  incrementalSafetySeconds: 30
  onStart: true
  intervalMinutes: 15
  fullScanIntervalMinutes: 1440
`;

  const config = parseConfig(raw, {});

  assert.deepEqual(config.scan, {
    pageSize: 750,
    concurrency: 3,
    incrementalSafetySeconds: 30,
    onStart: true,
    intervalMinutes: 15,
    fullScanIntervalMinutes: 1440
  });
});

test("parses optional startup validation tuning", () => {
  const raw = `
server:
  bind: 0.0.0.0
  port: 8096
  name: Bridge
auth:
  users:
    - name: alice
      passwordHash: hash
upstreams:
  - id: main
    name: Main
    url: https://example.com
    token: token
startup:
  validateUpstreams: false
`;

  const config = parseConfig(raw, {});

  assert.deepEqual(config.startup, { validateUpstreams: false });
});

test("runtime config applies valid file changes and keeps last good config after invalid changes", async () => {
  const dir = await mkdtemp(join(tmpdir(), "jellyfin-bridge-config-"));
  const configPath = join(dir, "config.yaml");
  await writeFile(configPath, rawConfig("Bridge", "${TOKEN}"), "utf8");
  const runtime = await RuntimeConfig.load(configPath, { TOKEN: "initial-token" });

  assert.equal(runtime.current().server.name, "Bridge");
  assert.equal(runtime.current().upstreams[0].token, "initial-token");

  await writeFile(configPath, rawConfig("Updated Bridge", "new-token"), "utf8");
  const applied = await runtime.reload();

  assert.equal(applied, true);
  assert.equal(runtime.current().server.name, "Updated Bridge");
  assert.equal(runtime.current().upstreams[0].token, "new-token");

  await writeFile(configPath, "not: [valid", "utf8");
  const invalidApplied = await runtime.reload();

  assert.equal(invalidApplied, false);
  assert.equal(runtime.current().server.name, "Updated Bridge");
  assert.equal(runtime.current().upstreams[0].token, "new-token");
});

test("runtime config reload does not notify subscribers when the parsed config is unchanged", async () => {
  const dir = await mkdtemp(join(tmpdir(), "jellyfin-bridge-config-unchanged-"));
  const configPath = join(dir, "config.yaml");
  await writeFile(configPath, rawConfig("Bridge", "token"), "utf8");
  const runtime = await RuntimeConfig.load(configPath, {});
  let notifications = 0;
  runtime.subscribe(() => {
    notifications += 1;
  });

  const applied = await runtime.reload();

  assert.equal(applied, true);
  assert.equal(notifications, 0);
});

test("runtime config watcher reacts to atomic file replacement", async () => {
  const dir = await mkdtemp(join(tmpdir(), "jellyfin-bridge-config-watch-"));
  const configPath = join(dir, "config.yaml");
  await writeFile(configPath, rawConfig("Bridge", "token"), "utf8");
  const runtime = await RuntimeConfig.load(configPath, {});
  const observed = new Promise<string>((resolve) => {
    runtime.subscribe((config) => resolve(config.server.name));
  });
  const watcher = runtime.watch({ debounceMs: 1 });
  await new Promise((resolve) => setTimeout(resolve, 10));

  await writeFile(join(dir, "config.yaml.tmp"), rawConfig("Watched Bridge", "token"), "utf8");
  await rename(join(dir, "config.yaml.tmp"), configPath);

  let timeout: NodeJS.Timeout | undefined;
  const timedObserved = Promise.race([
    observed,
    new Promise<never>((_, reject) => {
      timeout = setTimeout(() => reject(new Error("timed out waiting for config reload")), 1000);
    })
  ]);
  try {
    assert.equal(await timedObserved, "Watched Bridge");
  } finally {
    if (timeout) clearTimeout(timeout);
    watcher.stop();
  }
});

test("runtime config watcher ignores sibling atomic-save rename events", () => {
  assert.equal(isRuntimeConfigWatchEvent("config.yaml", "config.yaml.tmp"), false);
  assert.equal(isRuntimeConfigWatchEvent("config.yaml", "config.yaml"), true);
  assert.equal(isRuntimeConfigWatchEvent("config.yaml", undefined), true);
  assert.equal(isRuntimeConfigWatchEvent("config.yaml", null), true);
});

test("runtime config reload hookup always starts watching and stops cleanly", () => {
  const events: string[] = [];
  let subscribedListener: ((config: BridgeConfig) => void) | undefined;
  let observedName: string | undefined;
  let unsubscribed = false;
  let stopped = false;
  const runtime = {
    subscribe(listener: (config: BridgeConfig) => void): () => void {
      events.push("subscribe");
      subscribedListener = listener;
      return () => {
        events.push("unsubscribe");
        unsubscribed = true;
      };
    },
    watch(options?: RuntimeConfigWatchOptions): ConfigWatcher {
      assert.deepEqual(options, { debounceMs: 1 });
      events.push("watch");
      return {
        stop() {
          events.push("stop");
          stopped = true;
        }
      };
    }
  };

  const watcher = startRuntimeConfigReload(runtime, (config) => {
    observedName = config.server.name;
  }, { debounceMs: 1 });

  assert.deepEqual(events, ["subscribe", "watch"]);
  subscribedListener?.(parseConfig(rawConfig("Reloaded Bridge", "token"), {}));
  assert.equal(observedName, "Reloaded Bridge");

  watcher.stop();

  assert.equal(stopped, true);
  assert.equal(unsubscribed, true);
  assert.deepEqual(events, ["subscribe", "watch", "stop", "unsubscribe"]);
});

test("runtime config reload hookup unsubscribes if watching fails to start", () => {
  let unsubscribed = false;
  const runtime = {
    subscribe(): () => void {
      return () => {
        unsubscribed = true;
      };
    },
    watch(): ConfigWatcher {
      throw new Error("watch failed");
    }
  };

  assert.throws(() => startRuntimeConfigReload(runtime, () => undefined), /watch failed/);
  assert.equal(unsubscribed, true);
});

test("runtime config validates reloads even when startup validation is disabled", async () => {
  const dir = await mkdtemp(join(tmpdir(), "jellyfin-bridge-config-reload-"));
  const configPath = join(dir, "config.yaml");
  await writeFile(configPath, rawConfig("Bridge", "${TOKEN}", false), "utf8");
  let validateInitialCalled = false;
  let validateReloadCalled = false;
  const runtime = await RuntimeConfig.load(configPath, { TOKEN: "initial-token" }, {
    validateInitial: async (config) => {
      validateInitialCalled = true;
      if (config.startup?.validateUpstreams ?? true) {
        throw new Error("startup validation should be skipped by config");
      }
    },
    validate: async (config) => {
      validateReloadCalled = true;
      if (config.server.name === "Broken Bridge") {
        throw new Error("reload validation should run");
      }
    }
  });

  assert.equal(validateInitialCalled, true);
  assert.equal(validateReloadCalled, false);
  assert.equal(runtime.current().server.name, "Bridge");

  await writeFile(configPath, rawConfig("Broken Bridge", "new-token", false), "utf8");
  const applied = await runtime.reload();

  assert.equal(applied, false);
  assert.equal(validateReloadCalled, true);
  assert.equal(runtime.current().server.name, "Bridge");
  assert.equal(runtime.current().upstreams[0].token, "initial-token");
});

function rawConfig(name: string, token: string, startupValidateUpstreams?: boolean): string {
  const startupConfig = startupValidateUpstreams === undefined ? "" : `startup:\n  validateUpstreams: ${startupValidateUpstreams}\n`;
  return `
server:
  bind: 127.0.0.1
  port: 8096
  name: ${name}
auth:
  users:
    - name: alice
      passwordHash: hash
upstreams:
  - id: main
    name: Main
    url: https://example.com
    token: ${token}
${startupConfig}
`;
}
