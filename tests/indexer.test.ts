import test from "node:test";
import assert from "node:assert/strict";
import type { BridgeConfig } from "../src/config.js";
import { Indexer, type JellyfinItemsResponse } from "../src/indexer.js";
import { Store } from "../src/store.js";

test("indexes configured upstream library items with logical keys", async () => {
  const store = new Store(":memory:");
  const client = new FakeClient({
    "main:/Items?ParentId=library-a&Recursive=true&StartIndex=0&Limit=500": {
      Items: [
        { Id: "movie-a", Type: "Movie", Name: "Alien", ProviderIds: { Imdb: "tt0078748" } },
        { Id: "movie-b", Type: "Movie", Name: "The Thing", ProviderIds: {} }
      ],
      TotalRecordCount: 2,
      StartIndex: 0
    }
  });
  const indexer = new Indexer(config, store, client);

  await indexer.scanConfiguredLibraries();

  assert.equal(store.listIndexedItems().length, 2);
  assert.equal(store.listIndexedItems()[0].logicalKey, "movie:imdb:tt0078748");
  assert.equal(client.requests.length, 1);
  assert.match(client.requests[0], /Fields=.*MediaSources/);
  store.close();
});

test("paginates configured upstream library scans", async () => {
  const store = new Store(":memory:");
  const client = new FakeClient({
    "main:/Items?ParentId=library-a&Recursive=true&StartIndex=0&Limit=500": {
      Items: [{ Id: "movie-a", Type: "Movie", ProviderIds: { Tmdb: "1" } }],
      TotalRecordCount: 2,
      StartIndex: 0
    },
    "main:/Items?ParentId=library-a&Recursive=true&StartIndex=1&Limit=500": {
      Items: [{ Id: "movie-b", Type: "Movie", ProviderIds: { Tmdb: "2" } }],
      TotalRecordCount: 2,
      StartIndex: 1
    }
  });
  const indexer = new Indexer(config, store, client);

  await indexer.scanConfiguredLibraries();

  assert.deepEqual(store.listIndexedItems().map((item) => item.itemId), ["movie-a", "movie-b"]);
  assert.equal(client.requests.length, 2);
  store.close();
});

test("removes stale indexed source rows after rescan", async () => {
  const store = new Store(":memory:");
  store.upsertIndexedItem({
    serverId: "main",
    itemId: "stale",
    libraryId: "library-a",
    itemType: "Movie",
    logicalKey: "source:main:stale",
    json: { Id: "stale", Type: "Movie" }
  });
  const client = new FakeClient({
    "main:/Items?ParentId=library-a&Recursive=true&StartIndex=0&Limit=500": {
      Items: [{ Id: "fresh", Type: "Movie", ProviderIds: { Tmdb: "1" } }],
      TotalRecordCount: 1,
      StartIndex: 0
    }
  });
  const indexer = new Indexer(config, store, client);

  await indexer.scanConfiguredLibraries();

  assert.deepEqual(store.listIndexedItems().map((item) => item.itemId), ["fresh"]);
  store.close();
});

test("keeps existing indexed rows when a paginated scan fails before completion", async () => {
  const store = new Store(":memory:");
  store.upsertIndexedItem({
    serverId: "main",
    itemId: "old",
    libraryId: "library-a",
    itemType: "Movie",
    logicalKey: "source:main:old",
    json: { Id: "old", Type: "Movie" }
  });
  const client = new FakeClient({
    "main:/Items?ParentId=library-a&Recursive=true&StartIndex=0&Limit=500": {
      Items: [{ Id: "new", Type: "Movie", ProviderIds: { Tmdb: "1" } }],
      TotalRecordCount: 2,
      StartIndex: 0
    }
  });
  const indexer = new Indexer(config, store, client);

  await assert.rejects(() => indexer.scanConfiguredLibraries(), /Unexpected request/);

  assert.deepEqual(store.listIndexedItems().map((item) => item.itemId), ["old"]);
  store.close();
});

test("refreshes configured libraries incrementally from the saved source cursor", async () => {
  const store = new Store(":memory:");
  store.upsertIndexedItem({
    serverId: "main",
    itemId: "old",
    libraryId: "library-a",
    itemType: "Movie",
    logicalKey: "source:main:old",
    json: { Id: "old", Type: "Movie" }
  });
  store.markScanCursor("source:main:library-a", "2026-05-01T00:00:00.000Z");
  const client = new FakeClient({
    "main:/Items?ParentId=library-a&Recursive=true&StartIndex=0&Limit=500&MinDateLastSaved=2026-05-01T00%3A00%3A00.000Z": {
      Items: [{ Id: "new", Type: "Movie", ProviderIds: { Tmdb: "1" } }],
      TotalRecordCount: 1,
      StartIndex: 0
    }
  });
  const indexer = new Indexer(config, store, client, {
    incrementalSafetyMs: 0,
    now: () => new Date("2026-05-02T00:00:00.000Z")
  });

  await indexer.refreshConfiguredLibraries();

  assert.deepEqual(store.listIndexedItems().map((item) => item.itemId), ["new", "old"]);
  assert.match(client.requests[0], /MinDateLastSaved=2026-05-01T00%3A00%3A00\.000Z/);
  assert.equal(store.getScanCursor("source:main:library-a")?.cursorAt, "2026-05-02T00:00:00.000Z");
  store.close();
});

test("keeps the previous source cursor and rows when an incremental refresh fails", async () => {
  const store = new Store(":memory:");
  store.upsertIndexedItem({
    serverId: "main",
    itemId: "old",
    libraryId: "library-a",
    itemType: "Movie",
    logicalKey: "source:main:old",
    json: { Id: "old", Type: "Movie" }
  });
  store.markScanCursor("source:main:library-a", "2026-05-01T00:00:00.000Z");
  const indexer = new Indexer(config, store, new FakeClient({}), {
    incrementalSafetyMs: 0,
    now: () => new Date("2026-05-02T00:00:00.000Z")
  });

  await assert.rejects(() => indexer.refreshConfiguredLibraries(), /Unexpected request/);

  assert.deepEqual(store.listIndexedItems().map((item) => item.itemId), ["old"]);
  assert.equal(store.getScanCursor("source:main:library-a")?.cursorAt, "2026-05-01T00:00:00.000Z");
  assert.equal(store.getScanState("source:main:library-a")?.status, "failed");
  store.close();
});

test("records upstream user views so unmapped libraries can be exposed", async () => {
  const store = new Store(":memory:");
  const client = new FakeClient({
    "main:/Users?": [
      { Id: "user-a", Name: "alice" }
    ],
    "main:/UserViews?UserId=user-a": {
      Items: [
        { Id: "library-a", Name: "Movies", Type: "CollectionFolder", CollectionType: "movies" },
        { Id: "library-c", Name: "TV", Type: "CollectionFolder", CollectionType: "tvshows" }
      ],
      TotalRecordCount: 2,
      StartIndex: 0
    },
    "main:/Items?ParentId=library-a&Recursive=true": { Items: [], TotalRecordCount: 0, StartIndex: 0 }
  });
  const indexer = new Indexer(config, store, client);

  await indexer.scanUpstreamLibraries();

  assert.deepEqual(store.listUpstreamLibraries().map((library) => library.libraryId), ["library-a", "library-c"]);
  store.close();
});

test("scans configured and unmapped upstream libraries together", async () => {
  const store = new Store(":memory:");
  const client = new FakeClient({
    "main:/Users?": [{ Id: "user-a" }],
    "main:/UserViews?UserId=user-a": {
      Items: [
        { Id: "library-a", Name: "Movies", CollectionType: "movies" },
        { Id: "library-c", Name: "TV", CollectionType: "tvshows" }
      ],
      TotalRecordCount: 2,
      StartIndex: 0
    },
    "main:/Items?ParentId=library-a&Recursive=true&StartIndex=0&Limit=500": {
      Items: [{ Id: "movie-a", Type: "Movie", ProviderIds: { Tmdb: "1" } }],
      TotalRecordCount: 1,
      StartIndex: 0
    },
    "main:/Items?ParentId=library-c&Recursive=true&StartIndex=0&Limit=500": {
      Items: [{ Id: "series-a", Type: "Series", ProviderIds: { Tvdb: "2" } }],
      TotalRecordCount: 1,
      StartIndex: 0
    }
  });
  const indexer = new Indexer(config, store, client);

  await indexer.scanAllLibraries();

  assert.deepEqual(store.listIndexedItems().map((item) => item.itemId), ["movie-a", "series-a"]);
  assert.equal(store.getScanState("all")?.status, "success");
  store.close();
});

test("records failed all-library scan state", async () => {
  const store = new Store(":memory:");
  const client = new FakeClient({
    "main:/Users?": [{ Id: "user-a" }],
    "main:/UserViews?UserId=user-a": {
      Items: [{ Id: "library-a", Name: "Movies", CollectionType: "movies" }],
      TotalRecordCount: 1,
      StartIndex: 0
    }
  });
  const indexer = new Indexer(config, store, client);

  await assert.rejects(() => indexer.scanAllLibraries(), /Unexpected request/);

  const state = store.getScanState("all");
  assert.equal(state?.status, "failed");
  assert.match(state?.message ?? "", /Unexpected request/);
  store.close();
});

const config: BridgeConfig = {
  server: { bind: "127.0.0.1", port: 8096, publicUrl: "http://bridge.test", name: "Bridge" },
  auth: { users: [{ name: "alice", passwordHash: "hash" }] },
  upstreams: [{ id: "main", name: "Main", url: "https://jellyfin.example.com", token: "token" }],
  libraries: [{ id: "movies", name: "Movies", collectionType: "movies", sources: [{ server: "main", libraryId: "library-a" }] }]
};

class FakeClient {
  readonly requests: string[] = [];

  constructor(private readonly responses: Record<string, any>) {}

  async json<T>(serverId: string, path: string, init: { query?: Record<string, string | number | boolean | undefined> } = {}): Promise<T> {
    const query = new URLSearchParams();
    for (const [key, value] of Object.entries(init.query ?? {})) {
      if (value !== undefined) query.set(key, String(value));
    }
    const request = `${serverId}:${path}?${query.toString()}`;
    this.requests.push(request);
    const fallbackQuery = new URLSearchParams(query);
    fallbackQuery.delete("Fields");
    const response = this.responses[request] ?? this.responses[`${serverId}:${path}?${fallbackQuery.toString()}`];
    if (!response) throw new Error(`Unexpected request ${request}`);
    return response as T;
  }
}
