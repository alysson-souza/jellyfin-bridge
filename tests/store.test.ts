import test from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import Database from "better-sqlite3";
import { bridgeItemId } from "../src/ids.js";
import { Store } from "../src/store.js";

test("uses in-memory SQLite temp storage for scratch containers", () => {
  const store = new Store(":memory:");
  try {
    assert.equal(store.db.pragma("temp_store", { simple: true }), 2);
  } finally {
    store.close();
  }
});

test("migrates indexed items from the legacy schema without bridge item ids", () => {
  const dir = mkdtempSync(join(tmpdir(), "jellyfin-bridge-store-"));
  const path = join(dir, "store.db");
  const legacy = new Database(path);
  legacy.exec(`
    CREATE TABLE indexed_items (
      server_id TEXT NOT NULL,
      item_id TEXT NOT NULL,
      library_id TEXT NOT NULL,
      item_type TEXT NOT NULL,
      logical_key TEXT NOT NULL,
      provider_key TEXT,
      json TEXT NOT NULL,
      updated_at TEXT NOT NULL,
      PRIMARY KEY (server_id, item_id)
    );

    INSERT INTO indexed_items (server_id, item_id, library_id, item_type, logical_key, provider_key, json, updated_at)
    VALUES ('main', 'movie-a', 'library-a', 'Movie', 'movie:tmdb:1', 'movie:tmdb:1', '{"Id":"movie-a","Type":"Movie"}', '2026-05-01T00:00:00.000Z');
  `);
  legacy.close();

  let store: Store | undefined;
  try {
    store = new Store(path);

    const items = store.findIndexedItemsByBridgeId(bridgeItemId("movie:tmdb:1"));

    assert.equal(items.length, 1);
    assert.equal(items[0].itemId, "movie-a");
  } finally {
    store?.close();
    rmSync(dir, { recursive: true, force: true });
  }
});
