import { parseArgs } from "node:util";
import { buildApp } from "./app.js";
import { loadConfig } from "./config.js";
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

const config = await loadConfig(args.values.config ?? "config.yaml");
const store = new Store(args.values.database ?? "jellyfin-bridge.db");
const upstream = new UpstreamClient(config.upstreams);
const indexer = new Indexer(config, store, upstream);

const validateOnStart = !args.values["skip-startup-validation"] && (config.startup?.validateUpstreams ?? true);
if (validateOnStart) {
  await validateUpstreams(config, upstream);
}

const scanIntervalMinutes = numberArg(args.values["scan-interval-minutes"], config.scan?.intervalMinutes ?? 0);
const fullScanIntervalMinutes = numberArg(args.values["full-scan-interval-minutes"], config.scan?.fullScanIntervalMinutes ?? 0);
let refreshScheduler: ScanScheduler | undefined;
let fullScanScheduler: ScanScheduler | undefined;
if (scanIntervalMinutes > 0) {
  refreshScheduler = startScanScheduler(() => indexer.refreshAllLibraries(), scanIntervalMinutes * 60_000);
}
if (fullScanIntervalMinutes > 0) {
  fullScanScheduler = startScanScheduler(() => indexer.scanAllLibraries(), fullScanIntervalMinutes * 60_000);
}

const app = buildApp({ config, store, upstream });
installShutdownHandlers(process, app, store, {
  stop() {
    refreshScheduler?.stop();
    fullScanScheduler?.stop();
  }
});

await app.listen({ host: config.server.bind, port: config.server.port });

const scanOnStart = args.values["scan-on-start"] ?? config.scan?.onStart ?? false;
if (scanOnStart) {
  startScanOnStart(() => indexer.refreshAllLibraries(), refreshScheduler, app.log);
}

function numberArg(value: string | undefined, fallback: number): number {
  return value === undefined ? fallback : Number(value);
}
