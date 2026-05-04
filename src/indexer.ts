import type { BridgeConfig } from "./config.js";
import { logicalItemKey, type SourceItem } from "./merge.js";
import type { IndexedItemRecord, Store } from "./store.js";

export interface JellyfinItemsResponse {
  Items: SourceItem[];
  TotalRecordCount: number;
  StartIndex: number;
}

export interface JellyfinUserViewsResponse {
  Items: Array<{
    Id: string;
    Name?: string;
    CollectionType?: string;
  }>;
  TotalRecordCount: number;
  StartIndex: number;
}

export interface JellyfinUserDto {
  Id: string;
  Name?: string;
}

export interface JsonClient {
  json<T>(serverId: string, path: string, init?: { query?: Record<string, string | number | boolean | undefined> }): Promise<T>;
}

export interface IndexerOptions {
  pageSize?: number;
  concurrency?: number;
  incrementalSafetyMs?: number;
  now?: () => Date;
}

const INDEX_ITEM_FIELDS = [
  "BasicSyncInfo",
  "CanDelete",
  "ChildCount",
  "CollectionType",
  "CommunityRating",
  "Container",
  "DateCreated",
  "Etag",
  "Genres",
  "MediaSources",
  "MediaStreams",
  "Overview",
  "ParentId",
  "Path",
  "PrimaryImageAspectRatio",
  "ProviderIds",
  "RecursiveItemCount",
  "SortName",
  "Studios",
  "Taglines",
  "Tags",
  "UserData"
].join(",");

export class Indexer {
  private readonly pageSize: number;
  private readonly concurrency: number;
  private readonly incrementalSafetyMs: number;
  private readonly now: () => Date;

  constructor(
    private readonly config: BridgeConfig,
    private readonly store: Store,
    private readonly client: JsonClient,
    options: IndexerOptions = {}
  ) {
    this.pageSize = options.pageSize ?? config.scan?.pageSize ?? 500;
    this.concurrency = options.concurrency ?? config.scan?.concurrency ?? 4;
    this.incrementalSafetyMs = options.incrementalSafetyMs
      ?? (config.scan?.incrementalSafetySeconds === undefined ? 60_000 : config.scan.incrementalSafetySeconds * 1000);
    this.now = options.now ?? (() => new Date());
  }

  async scanConfiguredLibraries(): Promise<void> {
    await runConcurrently(configuredSources(this.config), this.concurrency, (source) => this.scanSource(source.serverId, source.libraryId));
  }

  async refreshConfiguredLibraries(): Promise<void> {
    await runConcurrently(configuredSources(this.config), this.concurrency, (source) => this.refreshSource(source.serverId, source.libraryId));
  }

  async scanAllLibraries(): Promise<void> {
    this.store.markScanStarted("all");
    try {
      await this.scanUpstreamLibraries();
      await this.scanConfiguredLibraries();

      const mapped = new Set(this.config.libraries.flatMap((library) => library.sources.map((source) => `${source.server}:${source.libraryId}`)));
      const unmapped = this.store.listUpstreamLibraries()
        .filter((library) => !mapped.has(`${library.serverId}:${library.libraryId}`))
        .map((library) => ({ serverId: library.serverId, libraryId: library.libraryId }));
      await runConcurrently(unmapped, this.concurrency, (source) => this.scanSource(source.serverId, source.libraryId));
      this.store.markScanSucceeded("all");
    } catch (error) {
      this.store.markScanFailed("all", error instanceof Error ? error.message : String(error));
      throw error;
    }
  }

  async refreshAllLibraries(): Promise<void> {
    this.store.markScanStarted("refresh");
    try {
      await this.scanUpstreamLibraries();
      await this.refreshConfiguredLibraries();

      const mapped = new Set(this.config.libraries.flatMap((library) => library.sources.map((source) => `${source.server}:${source.libraryId}`)));
      const unmapped = this.store.listUpstreamLibraries()
        .filter((library) => !mapped.has(`${library.serverId}:${library.libraryId}`))
        .map((library) => ({ serverId: library.serverId, libraryId: library.libraryId }));
      await runConcurrently(unmapped, this.concurrency, (source) => this.refreshSource(source.serverId, source.libraryId));
      this.store.markScanSucceeded("refresh");
    } catch (error) {
      this.store.markScanFailed("refresh", error instanceof Error ? error.message : String(error));
      throw error;
    }
  }

  async scanUpstreamLibraries(): Promise<void> {
    for (const upstream of this.config.upstreams) {
      const users = await this.client.json<JellyfinUserDto[]>(upstream.id, "/Users");
      const user = users[0];
      if (!user) {
        throw new Error(`Upstream ${upstream.id} returned no users for library scan`);
      }
      const response = await this.client.json<JellyfinUserViewsResponse>(upstream.id, "/UserViews", {
        query: { UserId: user.Id }
      });
      for (const item of response.Items) {
        this.store.upsertUpstreamLibrary({
          serverId: upstream.id,
          libraryId: item.Id,
          name: item.Name ?? item.Id,
          collectionType: item.CollectionType ?? null
        });
      }
    }
  }

  private async scanSource(serverId: string, libraryId: string): Promise<void> {
    const scope = sourceScope(serverId, libraryId);
    const scanStartedAt = this.now().toISOString();
    this.store.markScanStarted(scope);
    const items: IndexedItemRecord[] = [];
    try {
      for await (const page of this.fetchSourcePages(serverId, libraryId)) {
        for (const item of page) {
          items.push(this.indexedItem(serverId, libraryId, item));
        }
      }
      this.store.replaceIndexedItems(serverId, libraryId, items);
      this.store.markScanCursor(scope, scanStartedAt);
      this.store.markScanSucceeded(scope);
    } catch (error) {
      this.store.markScanFailed(scope, error instanceof Error ? error.message : String(error));
      throw error;
    }
  }

  private async refreshSource(serverId: string, libraryId: string): Promise<void> {
    const cursor = this.store.getScanCursor(sourceScope(serverId, libraryId));
    if (!cursor) {
      await this.scanSource(serverId, libraryId);
      return;
    }

    const scope = sourceScope(serverId, libraryId);
    const scanStartedAt = this.now().toISOString();
    this.store.markScanStarted(scope);
    try {
      for await (const page of this.fetchSourcePages(serverId, libraryId, this.cursorWithSafety(cursor.cursorAt))) {
        this.store.upsertIndexedItems(page.map((item) => this.indexedItem(serverId, libraryId, item)));
      }
      this.store.markScanCursor(scope, scanStartedAt);
      this.store.markScanSucceeded(scope);
    } catch (error) {
      this.store.markScanFailed(scope, error instanceof Error ? error.message : String(error));
      throw error;
    }
  }

  private async *fetchSourcePages(serverId: string, libraryId: string, minDateLastSaved?: string): AsyncGenerator<SourceItem[]> {
    let startIndex = 0;
    let totalRecordCount = Number.POSITIVE_INFINITY;
    while (startIndex < totalRecordCount) {
      const response = await this.client.json<JellyfinItemsResponse>(serverId, "/Items", {
        query: {
          ParentId: libraryId,
          Recursive: true,
          StartIndex: startIndex,
          Limit: this.pageSize,
          Fields: INDEX_ITEM_FIELDS,
          MinDateLastSaved: minDateLastSaved
        }
      });
      totalRecordCount = response.TotalRecordCount;
      yield response.Items;
      if (response.Items.length === 0) break;
      startIndex += response.Items.length;
    }
  }

  private indexedItem(serverId: string, libraryId: string, item: SourceItem): IndexedItemRecord {
    return {
      serverId,
      itemId: item.Id,
      libraryId,
      itemType: item.Type ?? "Unknown",
      logicalKey: logicalItemKey(item, serverId),
      json: item as unknown as Record<string, unknown>
    };
  }

  private cursorWithSafety(cursorAt: string): string {
    const time = Date.parse(cursorAt);
    if (!Number.isFinite(time)) return cursorAt;
    return new Date(Math.max(0, time - this.incrementalSafetyMs)).toISOString();
  }
}

interface SourceRef {
  serverId: string;
  libraryId: string;
}

function configuredSources(config: BridgeConfig): SourceRef[] {
  return config.libraries.flatMap((library) => library.sources.map((source) => ({
    serverId: source.server,
    libraryId: source.libraryId
  })));
}

function sourceScope(serverId: string, libraryId: string): string {
  return `source:${serverId}:${libraryId}`;
}

async function runConcurrently<T>(items: T[], concurrency: number, worker: (item: T) => Promise<void>): Promise<void> {
  const limit = Number.isFinite(concurrency) && concurrency > 0 ? Math.floor(concurrency) : 1;
  let next = 0;
  const workers = Array.from({ length: Math.min(limit, items.length) }, async () => {
    while (next < items.length) {
      const index = next;
      next += 1;
      await worker(items[index]);
    }
  });
  await Promise.all(workers);
}
