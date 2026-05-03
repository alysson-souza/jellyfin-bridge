import Database from "better-sqlite3";
import { bridgeItemId as makeBridgeItemId, newToken } from "./ids.js";

export interface SessionRecord {
  id: string;
  userId: string;
  userName: string;
  accessToken: string;
  deviceId: string | null;
  deviceName: string | null;
  createdAt: string;
  lastSeenAt: string;
}

export interface UserDataPatch {
  played?: boolean;
  isFavorite?: boolean;
  playbackPositionTicks?: number;
  playCount?: number;
}

export interface IndexedItemRecord {
  serverId: string;
  itemId: string;
  libraryId: string;
  itemType: string;
  logicalKey: string;
  json: Record<string, unknown>;
}

export interface MediaSourceMapping {
  bridgeMediaSourceId: string;
  serverId: string;
  upstreamItemId: string;
  upstreamMediaSourceId: string;
}

export interface PlaybackSessionMapping {
  bridgePlaySessionId: string;
  serverId: string;
  upstreamPlaySessionId: string;
  upstreamItemId: string;
  bridgeItemId: string;
}

export interface UpstreamLibraryRecord {
  serverId: string;
  libraryId: string;
  name: string;
  collectionType: string | null;
}

export interface ScanStateRecord {
  scope: string;
  status: "running" | "success" | "failed";
  startedAt: string;
  finishedAt: string | null;
  message: string | null;
}

export interface ScanCursorRecord {
  scope: string;
  cursorAt: string;
  updatedAt: string;
}

export class Store {
  readonly db: Database.Database;

  constructor(path = "jellyfin-bridge.db") {
    this.db = new Database(path);
    this.db.exec("PRAGMA foreign_keys = ON");
    this.migrate();
  }

  close(): void {
    this.db.close();
  }

  createSession(userId: string, userName: string, deviceId?: string, deviceName?: string): SessionRecord {
    const now = new Date().toISOString();
    const session: SessionRecord = {
      id: newToken(),
      userId,
      userName,
      accessToken: newToken(),
      deviceId: deviceId ?? null,
      deviceName: deviceName ?? null,
      createdAt: now,
      lastSeenAt: now
    };
    this.db.prepare(`
      INSERT INTO sessions (id, user_id, user_name, access_token, device_id, device_name, created_at, last_seen_at)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    `).run(session.id, session.userId, session.userName, session.accessToken, session.deviceId, session.deviceName, session.createdAt, session.lastSeenAt);
    return session;
  }

  findSession(accessToken: string): SessionRecord | undefined {
    const row = this.db.prepare("SELECT * FROM sessions WHERE access_token = ?").get(accessToken) as Row | undefined;
    if (!row) return undefined;
    const now = new Date().toISOString();
    this.db.prepare("UPDATE sessions SET last_seen_at = ? WHERE id = ?").run(now, row.id);
    return sessionFromRow({ ...row, last_seen_at: now });
  }

  deleteSession(accessToken: string): void {
    this.db.prepare("DELETE FROM sessions WHERE access_token = ?").run(accessToken);
  }

  upsertUserData(userId: string, itemId: string, patch: UserDataPatch): void {
    const now = new Date().toISOString();
    this.db.prepare(`
      INSERT INTO user_item_data (
        user_id, item_id, played, is_favorite, playback_position_ticks, play_count, updated_at
      )
      VALUES (?, ?, ?, ?, ?, ?, ?)
      ON CONFLICT(user_id, item_id) DO UPDATE SET
        played = COALESCE(excluded.played, user_item_data.played),
        is_favorite = COALESCE(excluded.is_favorite, user_item_data.is_favorite),
        playback_position_ticks = COALESCE(excluded.playback_position_ticks, user_item_data.playback_position_ticks),
        play_count = COALESCE(excluded.play_count, user_item_data.play_count),
        updated_at = excluded.updated_at
    `).run(
      userId,
      itemId,
      patch.played === undefined ? null : Number(patch.played),
      patch.isFavorite === undefined ? null : Number(patch.isFavorite),
      patch.playbackPositionTicks ?? null,
      patch.playCount ?? null,
      now
    );
  }

  getUserData(userId: string, itemId: string): Record<string, unknown> {
    const row = this.db.prepare("SELECT * FROM user_item_data WHERE user_id = ? AND item_id = ?").get(userId, itemId) as Row | undefined;
    return {
      PlaybackPositionTicks: Number(row?.playback_position_ticks ?? 0),
      PlayCount: Number(row?.play_count ?? 0),
      IsFavorite: Boolean(row?.is_favorite ?? 0),
      Played: Boolean(row?.played ?? 0),
      Key: itemId
    };
  }

  upsertIndexedItem(item: IndexedItemRecord): void {
    const bridgeItemId = makeBridgeItemId(item.logicalKey);
    this.db.prepare(`
      INSERT INTO indexed_items (server_id, item_id, library_id, item_type, logical_key, bridge_item_id, provider_key, json, updated_at)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
      ON CONFLICT(server_id, item_id) DO UPDATE SET
        library_id = excluded.library_id,
        item_type = excluded.item_type,
        logical_key = excluded.logical_key,
        bridge_item_id = excluded.bridge_item_id,
        provider_key = excluded.provider_key,
        json = excluded.json,
        updated_at = excluded.updated_at
    `).run(
      item.serverId,
      item.itemId,
      item.libraryId,
      item.itemType,
      item.logicalKey,
      bridgeItemId,
      item.logicalKey,
      JSON.stringify(item.json),
      new Date().toISOString()
    );
  }

  replaceIndexedItems(serverId: string, libraryId: string, items: IndexedItemRecord[]): void {
    const insert = this.db.prepare(`
      INSERT INTO indexed_items (server_id, item_id, library_id, item_type, logical_key, bridge_item_id, provider_key, json, updated_at)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    `);
    const now = new Date().toISOString();
    this.db.exec("BEGIN");
    try {
      this.db.prepare("DELETE FROM indexed_items WHERE server_id = ? AND library_id = ?").run(serverId, libraryId);
      for (const item of items) {
        insert.run(
          item.serverId,
          item.itemId,
          item.libraryId,
          item.itemType,
          item.logicalKey,
          makeBridgeItemId(item.logicalKey),
          item.logicalKey,
          JSON.stringify(item.json),
          now
        );
      }
      this.db.exec("COMMIT");
    } catch (error) {
      this.db.exec("ROLLBACK");
      throw error;
    }
  }

  removeIndexedItemsExcept(serverId: string, libraryId: string, itemIds: string[]): void {
    if (itemIds.length === 0) {
      this.db.prepare("DELETE FROM indexed_items WHERE server_id = ? AND library_id = ?").run(serverId, libraryId);
      return;
    }
    const placeholders = itemIds.map(() => "?").join(", ");
    this.db.prepare(`DELETE FROM indexed_items WHERE server_id = ? AND library_id = ? AND item_id NOT IN (${placeholders})`).run(serverId, libraryId, ...itemIds);
  }

  listIndexedItems(): IndexedItemRecord[] {
    const rows = this.db.prepare("SELECT * FROM indexed_items ORDER BY server_id, library_id, item_id").all() as Row[];
    return rows.map((row) => ({
      serverId: String(row.server_id),
      itemId: String(row.item_id),
      libraryId: String(row.library_id),
      itemType: String(row.item_type),
      logicalKey: String(row.logical_key),
      json: JSON.parse(String(row.json)) as Record<string, unknown>
    }));
  }

  listIndexedItemsForSources(sources: Array<{ serverId: string; libraryId: string }>): IndexedItemRecord[] {
    if (sources.length === 0) return [];
    const conditions = sources.map(() => "(server_id = ? AND library_id = ?)").join(" OR ");
    const values = sources.flatMap((source) => [source.serverId, source.libraryId]);
    const rows = this.db.prepare(`SELECT * FROM indexed_items WHERE ${conditions} ORDER BY server_id, library_id, item_id`).all(...values) as Row[];
    return rows.map((row) => ({
      serverId: String(row.server_id),
      itemId: String(row.item_id),
      libraryId: String(row.library_id),
      itemType: String(row.item_type),
      logicalKey: String(row.logical_key),
      json: JSON.parse(String(row.json)) as Record<string, unknown>
    }));
  }

  findIndexedItemsByBridgeId(bridgeItemId: string): IndexedItemRecord[] {
    const rows = this.db.prepare("SELECT * FROM indexed_items WHERE bridge_item_id = ? ORDER BY server_id, library_id, item_id").all(bridgeItemId) as Row[];
    return rows
      .map(indexedItemFromRow)
      .map(({ bridgeItemId: _bridgeItemId, ...item }) => item);
  }

  upsertMediaSourceMapping(mapping: MediaSourceMapping): void {
    this.db.prepare(`
      INSERT INTO media_source_mappings (bridge_media_source_id, server_id, upstream_item_id, upstream_media_source_id)
      VALUES (?, ?, ?, ?)
      ON CONFLICT(bridge_media_source_id) DO UPDATE SET
        server_id = excluded.server_id,
        upstream_item_id = excluded.upstream_item_id,
        upstream_media_source_id = excluded.upstream_media_source_id
    `).run(mapping.bridgeMediaSourceId, mapping.serverId, mapping.upstreamItemId, mapping.upstreamMediaSourceId);
  }

  findMediaSourceMapping(bridgeMediaSourceId: string): MediaSourceMapping | undefined {
    const row = this.db.prepare("SELECT * FROM media_source_mappings WHERE bridge_media_source_id = ?").get(bridgeMediaSourceId) as Row | undefined;
    if (!row) return undefined;
    return {
      bridgeMediaSourceId: String(row.bridge_media_source_id),
      serverId: String(row.server_id),
      upstreamItemId: String(row.upstream_item_id),
      upstreamMediaSourceId: String(row.upstream_media_source_id)
    };
  }

  createPlaybackSessionMapping(mapping: Omit<PlaybackSessionMapping, "bridgePlaySessionId">): PlaybackSessionMapping {
    const bridgePlaySessionId = newToken();
    this.db.prepare(`
      INSERT INTO playback_sessions (bridge_play_session_id, server_id, upstream_play_session_id, upstream_item_id, bridge_item_id, created_at)
      VALUES (?, ?, ?, ?, ?, ?)
    `).run(
      bridgePlaySessionId,
      mapping.serverId,
      mapping.upstreamPlaySessionId,
      mapping.upstreamItemId,
      mapping.bridgeItemId,
      new Date().toISOString()
    );
    return { bridgePlaySessionId, ...mapping };
  }

  findPlaybackSessionMapping(bridgePlaySessionId: string): PlaybackSessionMapping | undefined {
    const row = this.db.prepare("SELECT * FROM playback_sessions WHERE bridge_play_session_id = ?").get(bridgePlaySessionId) as Row | undefined;
    if (!row) return undefined;
    return {
      bridgePlaySessionId: String(row.bridge_play_session_id),
      serverId: String(row.server_id),
      upstreamPlaySessionId: String(row.upstream_play_session_id),
      upstreamItemId: String(row.upstream_item_id),
      bridgeItemId: String(row.bridge_item_id)
    };
  }

  upsertUpstreamLibrary(library: UpstreamLibraryRecord): void {
    this.db.prepare(`
      INSERT INTO upstream_libraries (server_id, library_id, name, collection_type, last_scanned_at)
      VALUES (?, ?, ?, ?, ?)
      ON CONFLICT(server_id, library_id) DO UPDATE SET
        name = excluded.name,
        collection_type = excluded.collection_type,
        last_scanned_at = excluded.last_scanned_at
    `).run(library.serverId, library.libraryId, library.name, library.collectionType, new Date().toISOString());
  }

  listUpstreamLibraries(): UpstreamLibraryRecord[] {
    const rows = this.db.prepare("SELECT * FROM upstream_libraries ORDER BY server_id, library_id").all() as Row[];
    return rows.map((row) => ({
      serverId: String(row.server_id),
      libraryId: String(row.library_id),
      name: String(row.name),
      collectionType: row.collection_type === null ? null : String(row.collection_type)
    }));
  }

  markScanStarted(scope: string): void {
    const now = new Date().toISOString();
    this.db.prepare(`
      INSERT INTO scan_state (scope, status, started_at, finished_at, message)
      VALUES (?, 'running', ?, NULL, NULL)
      ON CONFLICT(scope) DO UPDATE SET
        status = excluded.status,
        started_at = excluded.started_at,
        finished_at = excluded.finished_at,
        message = excluded.message
    `).run(scope, now);
  }

  markScanSucceeded(scope: string): void {
    this.db.prepare("UPDATE scan_state SET status = 'success', finished_at = ?, message = NULL WHERE scope = ?").run(new Date().toISOString(), scope);
  }

  markScanFailed(scope: string, message: string): void {
    this.db.prepare("UPDATE scan_state SET status = 'failed', finished_at = ?, message = ? WHERE scope = ?").run(new Date().toISOString(), message, scope);
  }

  getScanState(scope: string): ScanStateRecord | undefined {
    const row = this.db.prepare("SELECT * FROM scan_state WHERE scope = ?").get(scope) as Row | undefined;
    if (!row) return undefined;
    return {
      scope: String(row.scope),
      status: String(row.status) as ScanStateRecord["status"],
      startedAt: String(row.started_at),
      finishedAt: row.finished_at === null ? null : String(row.finished_at),
      message: row.message === null ? null : String(row.message)
    };
  }

  markScanCursor(scope: string, cursorAt: string): void {
    this.db.prepare(`
      INSERT INTO scan_cursors (scope, cursor_at, updated_at)
      VALUES (?, ?, ?)
      ON CONFLICT(scope) DO UPDATE SET
        cursor_at = excluded.cursor_at,
        updated_at = excluded.updated_at
    `).run(scope, cursorAt, new Date().toISOString());
  }

  getScanCursor(scope: string): ScanCursorRecord | undefined {
    const row = this.db.prepare("SELECT * FROM scan_cursors WHERE scope = ?").get(scope) as Row | undefined;
    if (!row) return undefined;
    return {
      scope: String(row.scope),
      cursorAt: String(row.cursor_at),
      updatedAt: String(row.updated_at)
    };
  }

  private migrate(): void {
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS sessions (
        id TEXT PRIMARY KEY,
        user_id TEXT NOT NULL,
        user_name TEXT NOT NULL,
        access_token TEXT NOT NULL UNIQUE,
        device_id TEXT,
        device_name TEXT,
        created_at TEXT NOT NULL,
        last_seen_at TEXT NOT NULL
      );

      CREATE TABLE IF NOT EXISTS upstream_libraries (
        server_id TEXT NOT NULL,
        library_id TEXT NOT NULL,
        name TEXT NOT NULL,
        collection_type TEXT,
        last_scanned_at TEXT,
        PRIMARY KEY (server_id, library_id)
      );

      CREATE TABLE IF NOT EXISTS indexed_items (
        server_id TEXT NOT NULL,
        item_id TEXT NOT NULL,
        library_id TEXT NOT NULL,
        item_type TEXT NOT NULL,
        logical_key TEXT NOT NULL,
        bridge_item_id TEXT,
        provider_key TEXT,
        json TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        PRIMARY KEY (server_id, item_id)
      );

      CREATE INDEX IF NOT EXISTS idx_indexed_items_logical_key ON indexed_items(logical_key);
      CREATE INDEX IF NOT EXISTS idx_indexed_items_library ON indexed_items(server_id, library_id);

      CREATE TABLE IF NOT EXISTS media_source_mappings (
        bridge_media_source_id TEXT PRIMARY KEY,
        server_id TEXT NOT NULL,
        upstream_item_id TEXT NOT NULL,
        upstream_media_source_id TEXT NOT NULL
      );

      CREATE TABLE IF NOT EXISTS playback_sessions (
        bridge_play_session_id TEXT PRIMARY KEY,
        server_id TEXT NOT NULL,
        upstream_play_session_id TEXT,
        upstream_item_id TEXT NOT NULL,
        bridge_item_id TEXT NOT NULL,
        created_at TEXT NOT NULL
      );

      CREATE TABLE IF NOT EXISTS user_item_data (
        user_id TEXT NOT NULL,
        item_id TEXT NOT NULL,
        played INTEGER,
        is_favorite INTEGER,
        playback_position_ticks INTEGER,
        play_count INTEGER,
        updated_at TEXT NOT NULL,
        PRIMARY KEY (user_id, item_id)
      );

      CREATE TABLE IF NOT EXISTS scan_state (
        scope TEXT PRIMARY KEY,
        status TEXT NOT NULL,
        started_at TEXT NOT NULL,
        finished_at TEXT,
        message TEXT
      );

      CREATE TABLE IF NOT EXISTS scan_cursors (
        scope TEXT PRIMARY KEY,
        cursor_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
      );
    `);
    this.ensureColumn("indexed_items", "bridge_item_id", "TEXT");
    this.backfillBridgeItemIds();
    this.db.exec("CREATE INDEX IF NOT EXISTS idx_indexed_items_bridge_item_id ON indexed_items(bridge_item_id)");
  }

  private ensureColumn(table: string, column: string, definition: string): void {
    const rows = this.db.prepare(`PRAGMA table_info(${table})`).all() as Array<{ name: string }>;
    if (rows.some((row) => row.name === column)) return;
    this.db.exec(`ALTER TABLE ${table} ADD COLUMN ${column} ${definition}`);
  }

  private backfillBridgeItemIds(): void {
    const rows = this.db.prepare("SELECT server_id, item_id, logical_key FROM indexed_items WHERE bridge_item_id IS NULL OR bridge_item_id = ''").all() as Row[];
    if (rows.length === 0) return;
    const update = this.db.prepare("UPDATE indexed_items SET bridge_item_id = ? WHERE server_id = ? AND item_id = ?");
    this.db.exec("BEGIN");
    try {
      for (const row of rows) {
        update.run(makeBridgeItemId(String(row.logical_key)), String(row.server_id), String(row.item_id));
      }
      this.db.exec("COMMIT");
    } catch (error) {
      this.db.exec("ROLLBACK");
      throw error;
    }
  }
}

type Row = Record<string, string | number | null>;

function sessionFromRow(row: Row): SessionRecord {
  return {
    id: String(row.id),
    userId: String(row.user_id),
    userName: String(row.user_name),
    accessToken: String(row.access_token),
    deviceId: row.device_id === null ? null : String(row.device_id),
    deviceName: row.device_name === null ? null : String(row.device_name),
    createdAt: String(row.created_at),
    lastSeenAt: String(row.last_seen_at)
  };
}

function indexedItemFromRow(row: Row): IndexedItemRecord & { bridgeItemId: string } {
  const logicalKey = String(row.logical_key);
  return {
    serverId: String(row.server_id),
    itemId: String(row.item_id),
    libraryId: String(row.library_id),
    itemType: String(row.item_type),
    logicalKey,
    bridgeItemId: makeBridgeItemId(logicalKey),
    json: JSON.parse(String(row.json)) as Record<string, unknown>
  };
}
