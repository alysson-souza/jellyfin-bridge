import type { BridgeConfig } from "./config.js";

export interface ValidationClient {
  json<T>(serverId: string, path: string, init?: { query?: Record<string, string | number | boolean | undefined> }): Promise<T>;
}

interface JellyfinUserDto {
  Id: string;
}

interface JellyfinUserViewsResponse {
  Items: Array<{ Id: string; Name?: string }>;
}

export async function validateUpstreams(config: BridgeConfig, client: ValidationClient): Promise<void> {
  const mappedByServer = new Map<string, Set<string>>();
  for (const library of config.libraries) {
    for (const source of library.sources) {
      const mapped = mappedByServer.get(source.server) ?? new Set<string>();
      mapped.add(source.libraryId);
      mappedByServer.set(source.server, mapped);
    }
  }

  for (const upstream of config.upstreams) {
    await client.json<unknown>(upstream.id, "/System/Info/Public");
    const users = await client.json<JellyfinUserDto[]>(upstream.id, "/Users");
    const user = users[0];
    if (!user) {
      throw new Error(`Upstream ${upstream.id} returned no users for startup validation`);
    }

    const response = await client.json<JellyfinUserViewsResponse>(upstream.id, "/UserViews", {
      query: { UserId: user.Id }
    });
    const discovered = new Set(response.Items.map((item) => item.Id));
    for (const libraryId of mappedByServer.get(upstream.id) ?? []) {
      if (!discovered.has(libraryId)) {
        throw new Error(`Mapped library ${upstream.id}:${libraryId} was not found during startup validation`);
      }
    }
  }
}
