import { statSync, watch, watchFile, unwatchFile, type Stats } from "node:fs";
import { readFile } from "node:fs/promises";
import { basename, dirname } from "node:path";
import YAML from "yaml";
import { z } from "zod";

const idSchema = z.string().regex(/^[A-Za-z0-9_-]+$/);

const configSchema = z.object({
  server: z.object({
    bind: z.string().default("0.0.0.0"),
    port: z.number().int().min(1).max(65535).default(8096),
    publicUrl: z.string().url().optional(),
    name: z.string().min(1).default("Jellyfin Bridge")
  }),
  auth: z.object({
    users: z.array(
      z.object({
        name: z.string().min(1),
        passwordHash: z.string().min(1)
      })
    ).min(1)
  }),
  upstreams: z.array(
    z.object({
      id: idSchema,
      name: z.string().min(1),
      url: z.string().url(),
      token: z.string().min(1)
    })
  ).min(1),
  startup: z.object({
    validateUpstreams: z.boolean().optional()
  }).optional(),
  scan: z.object({
    pageSize: z.number().int().min(1).max(5000).optional(),
    concurrency: z.number().int().min(1).max(32).optional(),
    incrementalSafetySeconds: z.number().int().min(0).max(3600).optional(),
    onStart: z.boolean().optional(),
    intervalMinutes: z.number().int().min(0).optional(),
    fullScanIntervalMinutes: z.number().int().min(0).optional()
  }).optional(),
  libraries: z.array(
    z.object({
      id: idSchema,
      name: z.string().min(1),
      collectionType: z.string().min(1),
      sources: z.array(
        z.object({
          server: idSchema,
          libraryId: z.string().min(1)
        })
      ).min(1)
    })
  ).default([])
});

export type BridgeConfig = z.infer<typeof configSchema>;
export type BridgeUser = BridgeConfig["auth"]["users"][number];
export type UpstreamConfig = BridgeConfig["upstreams"][number];
export type LibraryConfig = BridgeConfig["libraries"][number];

export interface RuntimeConfigSource {
  current(): BridgeConfig;
  subscribe(listener: (config: BridgeConfig) => void): () => void;
}

export interface ConfigWatcher {
  stop(): void;
}

export interface RuntimeConfigOptions {
  validateInitial?: (config: BridgeConfig) => Promise<void>;
  validate?: (config: BridgeConfig) => Promise<void>;
  logger?: {
    info?(details: unknown, message?: string): void;
    warn?(details: unknown, message?: string): void;
    error?(details: unknown, message?: string): void;
  };
}

export interface RuntimeConfigWatchOptions {
  debounceMs?: number;
}

export interface RuntimeConfigReloadSource {
  subscribe(listener: (config: BridgeConfig) => void): () => void;
  watch(options?: RuntimeConfigWatchOptions): ConfigWatcher;
}

export function startRuntimeConfigReload(
  runtimeConfig: RuntimeConfigReloadSource,
  listener: (config: BridgeConfig) => void,
  options?: RuntimeConfigWatchOptions
): ConfigWatcher {
  const unsubscribe = runtimeConfig.subscribe(listener);
  let watcher: ConfigWatcher;
  try {
    watcher = runtimeConfig.watch(options);
  } catch (error) {
    unsubscribe();
    throw error;
  }
  return {
    stop() {
      watcher.stop();
      unsubscribe();
    }
  };
}

export function isRuntimeConfigWatchEvent(watchedFile: string, filename: string | Buffer | null | undefined): boolean {
  const changedFile = filename?.toString();
  return !changedFile || changedFile === watchedFile;
}

export async function loadConfig(path: string): Promise<BridgeConfig> {
  const raw = await readFile(path, "utf8");
  return parseConfig(raw, process.env);
}

export class RuntimeConfig implements RuntimeConfigSource {
  private listeners = new Set<(config: BridgeConfig) => void>();
  private reloadChain: Promise<boolean> = Promise.resolve(true);

  private constructor(
    private readonly path: string,
    private readonly env: NodeJS.ProcessEnv,
    private config: BridgeConfig,
    private readonly options: RuntimeConfigOptions = {}
  ) {}

  static async load(path: string, env: NodeJS.ProcessEnv = process.env, options: RuntimeConfigOptions = {}): Promise<RuntimeConfig> {
    const config = await loadConfigFrom(path, env);
    if (options.validateInitial) {
      await options.validateInitial(config);
    } else if (options.validate) {
      await options.validate(config);
    }
    return new RuntimeConfig(path, env, config, options);
  }

  current(): BridgeConfig {
    return this.config;
  }

  subscribe(listener: (config: BridgeConfig) => void): () => void {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  }

  async reload(): Promise<boolean> {
    this.reloadChain = this.reloadChain.then(() => this.reloadOnce(), () => this.reloadOnce());
    return this.reloadChain;
  }

  watch(options: RuntimeConfigWatchOptions = {}): ConfigWatcher {
    const debounceMs = options.debounceMs ?? 250;
    const watchedPath = this.path;
    const watchedDirectory = dirname(this.path);
    const watchedFile = basename(this.path);
    const initialStat = statSync(watchedPath);
    let polledMtimeMs = initialStat.mtimeMs;
    let polledSize = initialStat.size;
    let timer: NodeJS.Timeout | undefined;
    let polling = false;
    let stopped = false;
    const scheduleReload = (): void => {
      if (stopped) return;
      if (timer) clearTimeout(timer);
      timer = setTimeout(() => {
        timer = undefined;
        void this.reload();
      }, debounceMs);
      timer.unref?.();
    };
    const startPolling = (): void => {
      if (polling || stopped) return;
      polling = true;
      watchFile(watchedPath, { interval: Math.max(debounceMs, 50), persistent: false }, pollFile);
    };
    const pollFile = (current: Stats): void => {
      if (current.mtimeMs !== polledMtimeMs || current.size !== polledSize) {
        polledMtimeMs = current.mtimeMs;
        polledSize = current.size;
        scheduleReload();
      }
    };
    startPolling();
    let watcher: ReturnType<typeof watch> | undefined;
    try {
      watcher = watch(watchedDirectory, (_eventType, filename) => {
        if (isRuntimeConfigWatchEvent(watchedFile, filename)) {
          scheduleReload();
        }
      });
      watcher.on("error", (error) => {
        this.options.logger?.error?.({ error }, "Config watcher failed; polling remains active");
        watcher?.close();
      });
    } catch (error) {
      this.options.logger?.error?.({ error }, "Config watcher failed to start; polling remains active");
    }

    return {
      stop() {
        stopped = true;
        if (timer) clearTimeout(timer);
        watcher?.close();
        if (polling) unwatchFile(watchedPath, pollFile);
      }
    };
  }

  private async reloadOnce(): Promise<boolean> {
    try {
      const next = await loadConfigFrom(this.path, this.env);
      if (this.options.validate) {
        await this.options.validate(next);
      }
      if (sameConfig(this.config, next)) {
        return true;
      }
      this.config = next;
      for (const listener of this.listeners) {
        listener(next);
      }
      this.options.logger?.info?.({ path: this.path }, "Configuration reloaded");
      return true;
    } catch (error) {
      this.options.logger?.error?.({ error, path: this.path }, "Configuration reload failed; keeping previous configuration");
      return false;
    }
  }
}

function sameConfig(left: BridgeConfig, right: BridgeConfig): boolean {
  return JSON.stringify(left) === JSON.stringify(right);
}

async function loadConfigFrom(path: string, env: NodeJS.ProcessEnv): Promise<BridgeConfig> {
  const raw = await readFile(path, "utf8");
  return parseConfig(raw, env);
}

export function parseConfig(raw: string, env: NodeJS.ProcessEnv): BridgeConfig {
  const expanded = expandEnv(raw, env);
  const parsed = configSchema.parse(YAML.parse(expanded));
  validateConfig(parsed);
  return parsed;
}

export function expandEnv(raw: string, env: NodeJS.ProcessEnv): string {
  return raw.replace(/\$\{([A-Za-z_][A-Za-z0-9_]*)(?::-(.*?))?\}/g, (_, name: string, fallback: string | undefined) => {
    const value = env[name] ?? fallback;
    if (value === undefined) {
      throw new Error(`Missing environment variable ${name}`);
    }
    return value;
  });
}

function validateConfig(config: BridgeConfig): void {
  const upstreamIds = new Set<string>();
  for (const upstream of config.upstreams) {
    if (upstreamIds.has(upstream.id)) {
      throw new Error(`Duplicate upstream id ${upstream.id}`);
    }
    upstreamIds.add(upstream.id);
  }

  const userNames = new Set<string>();
  for (const user of config.auth.users) {
    const normalized = user.name.toLowerCase();
    if (userNames.has(normalized)) {
      throw new Error(`Duplicate auth user ${user.name}`);
    }
    userNames.add(normalized);
  }

  const libraryIds = new Set<string>();
  const mappedSources = new Set<string>();
  for (const library of config.libraries) {
    if (libraryIds.has(library.id)) {
      throw new Error(`Duplicate bridge library id ${library.id}`);
    }
    libraryIds.add(library.id);

    for (const source of library.sources) {
      if (!upstreamIds.has(source.server)) {
        throw new Error(`Library ${library.id} references unknown upstream ${source.server}`);
      }
      const sourceKey = `${source.server}:${source.libraryId}`;
      if (mappedSources.has(sourceKey)) {
        throw new Error(`Upstream library ${sourceKey} is mapped more than once`);
      }
      mappedSources.add(sourceKey);
    }
  }
}
