import { readFile } from "node:fs/promises";
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

export async function loadConfig(path: string): Promise<BridgeConfig> {
  const raw = await readFile(path, "utf8");
  return parseConfig(raw, process.env);
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
