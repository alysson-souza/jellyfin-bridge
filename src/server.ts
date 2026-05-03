import { parseArgs } from "node:util";
import { buildApp } from "./app.js";
import { RuntimeConfig, startRuntimeConfigReload } from "./config.js";
import { Indexer } from "./indexer.js";
import { installShutdownHandlers } from "./lifecycle.js";
import { startScanScheduler, type ScanScheduler } from "./scheduler.js";
import { startScanOnStart } from "./startupScan.js";
import { Store } from "./store.js";
import { UpstreamClient } from "./upstream.js";
import { validateUpstreams } from "./validation.js";

const args = parseArgs({
  options: {
    config: { type: "string", short: "c", default: "config.yaml" },
    database: { type: "string", short: "d", default: "jellyfin-bridge.db" },
    "skip-startup-validation": { type: "boolean" },
    "scan-on-start": { type: "boolean" },
    "scan-interval-minutes": { type: "string" },
    "full-scan-interval-minutes": { type: "string" }
  }
});

const configPath = args.values.config ?? "config.yaml";
const runtimeConfig = await RuntimeConfig.load(configPath, process.env, {
  validateInitial: async (config) => {
    const skipStartupValidation = args.values["skip-startup-validation"] ?? false;
    if (!skipStartupValidation && (config.startup?.validateUpstreams ?? true)) {
      await validateUpstreams(config, new UpstreamClient(config.upstreams));
    }
  },
  validate: async (config) => {
    await validateUpstreams(config, new UpstreamClient(config.upstreams));
  },
  logger: console
});
let config = runtimeConfig.current();
const store = new Store(args.values.database ?? "jellyfin-bridge.db");
let upstream = new UpstreamClient(config.upstreams);

let refreshScheduler: ScanScheduler | undefined;
let fullScanScheduler: ScanScheduler | undefined;
let scanIntervalMinutes = 0;
let fullScanIntervalMinutes = 0;
reconcileScanSchedulers(config);

const app = buildApp({ config: runtimeConfig, store, upstreamFactory: (upstreams) => new UpstreamClient(upstreams) });
const configWatcher = startRuntimeConfigReload(runtimeConfig, (nextConfig) => {
  const previousConfig = config;
  config = nextConfig;
  upstream = new UpstreamClient(nextConfig.upstreams);
  if (previousConfig.server.bind !== nextConfig.server.bind || previousConfig.server.port !== nextConfig.server.port) {
    app.log.warn({
      configuredBind: nextConfig.server.bind,
      configuredPort: nextConfig.server.port,
      activeBind: previousConfig.server.bind,
      activePort: previousConfig.server.port
    }, "Configuration listener address changed; restart required to apply bind or port");
  }
  reconcileScanSchedulers(nextConfig);
});
installShutdownHandlers(process, app, store, {
  stop() {
    configWatcher.stop();
    refreshScheduler?.stop();
    fullScanScheduler?.stop();
  }
});

await app.listen({ host: config.server.bind, port: config.server.port });

const scanOnStart = args.values["scan-on-start"] ?? config.scan?.onStart ?? false;
if (scanOnStart) {
  startScanOnStart(() => new Indexer(runtimeConfig.current(), store, upstream).refreshAllLibraries(), refreshScheduler, app.log);
}

function numberArg(value: string | undefined, fallback: number): number {
  return value === undefined ? fallback : Number(value);
}

function reconcileScanSchedulers(nextConfig: typeof config): void {
  const nextScanIntervalMinutes = numberArg(args.values["scan-interval-minutes"], nextConfig.scan?.intervalMinutes ?? 0);
  const nextFullScanIntervalMinutes = numberArg(args.values["full-scan-interval-minutes"], nextConfig.scan?.fullScanIntervalMinutes ?? 0);
  if (nextScanIntervalMinutes !== scanIntervalMinutes) {
    refreshScheduler?.stop();
    refreshScheduler = nextScanIntervalMinutes > 0
      ? startScanScheduler(() => new Indexer(runtimeConfig.current(), store, upstream).refreshAllLibraries(), nextScanIntervalMinutes * 60_000)
      : undefined;
    scanIntervalMinutes = nextScanIntervalMinutes;
  }
  if (nextFullScanIntervalMinutes !== fullScanIntervalMinutes) {
    fullScanScheduler?.stop();
    fullScanScheduler = nextFullScanIntervalMinutes > 0
      ? startScanScheduler(() => new Indexer(runtimeConfig.current(), store, upstream).scanAllLibraries(), nextFullScanIntervalMinutes * 60_000)
      : undefined;
    fullScanIntervalMinutes = nextFullScanIntervalMinutes;
  }
}
