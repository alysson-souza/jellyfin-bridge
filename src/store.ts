import Database from "better-sqlite3";
import { randomUUID } from "node:crypto";
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
  lastPlayedDate?: string | null;
}

export interface UserDataRecord {
  userId: string;
  itemId: string;
  playbackPositionTicks: number;
  playCount: number;
  isFavorite: boolean;
  played: boolean;
  lastPlayedDate: string | null;
  updatedAt: string;
}

export interface IndexedItemRecord {
  serverId: string;
  itemId: string;
  libraryId: string;
  itemType: string;
  logicalKey: string;
  json: Record<string, unknown>;
}

export interface IndexedItemParentRef {
  serverId: string;
  libraryId: string;
  itemId: string;
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

export interface InfuseSyncCheckpointRecord {
  id: string;
  deviceId: string;
  userId: string;
  fromTimestamp: string;
  syncTimestamp: string | null;
  createdAt: string;
  updatedAt: string;
}

export class Store {
  readonly db: Database.Database;

  constructor(path = "jellyfin-bridge.db") {
    this.db = new Database(path);
    this.db.exec(`
      PRAGMA foreign_keys = ON;
      PRAGMA temp_store = MEMORY;
    `);
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
        user_id, item_id, played, is_favorite, playback_position_ticks, play_count, last_played_date, updated_at
      )
      VALUES (?, ?, ?, ?, ?, ?, ?, ?)
      ON CONFLICT(user_id, item_id) DO UPDATE SET
        played = COALESCE(excluded.played, user_item_data.played),
        is_favorite = COALESCE(excluded.is_favorite, user_item_data.is_favorite),
        playback_position_ticks = COALESCE(excluded.playback_position_ticks, user_item_data.playback_position_ticks),
        play_count = COALESCE(excluded.play_count, user_item_data.play_count),
        last_played_date = COALESCE(excluded.last_played_date, user_item_data.last_played_date),
        updated_at = excluded.updated_at
    `).run(
      userId,
      itemId,
      patch.played === undefined ? null : Number(patch.played),
      patch.isFavorite === undefined ? null : Number(patch.isFavorite),
      patch.playbackPositionTicks ?? null,
      patch.playCount ?? null,
      patch.lastPlayedDate ?? null,
      now
    );
    if (patch.lastPlayedDate === null) {
      this.db.prepare("UPDATE user_item_data SET last_played_date = NULL, updated_at = ? WHERE user_id = ? AND item_id = ?").run(now, userId, itemId);
    }
  }

  getUserData(userId: string, itemId: string): Record<string, unknown> {
    const row = this.db.prepare("SELECT * FROM user_item_data WHERE user_id = ? AND item_id = ?").get(userId, itemId) as Row | undefined;
    return userDataDtoFromRow(row, itemId);
  }

  listUserData(userId: string, itemIds: string[]): Map<string, Record<string, unknown>> {
    const ids = uniqueStrings(itemIds);
    const data = new Map<string, Record<string, unknown>>();
    for (const chunk of chunks(ids)) {
      const placeholders = chunk.map(() => "?").join(", ");
      const rows = this.db.prepare(`
        SELECT * FROM user_item_data
        WHERE user_id = ? AND item_id IN (${placeholders})
      `).all(userId, ...chunk) as Row[];
      for (const row of rows) {
        const itemId = String(row.item_id);
        data.set(itemId, userDataDtoFromRow(row, itemId));
      }
    }
    return data;
  }

  listUserDataUpdatedBetween(userId: string, fromTimestamp: string, toTimestamp: string): UserDataRecord[] {
    const rows = this.db.prepare(`
      SELECT * FROM user_item_data
      WHERE user_id = ? AND updated_at BETWEEN ? AND ?
      ORDER BY item_id
    `).all(userId, fromTimestamp, toTimestamp) as Row[];
    return rows.map(userDataFromRow);
  }

  upsertIndexedItem(item: IndexedItemRecord): void {
    this.upsertIndexedItems([item]);
  }

  upsertIndexedItems(items: IndexedItemRecord[]): void {
    if (items.length === 0) return;
    const upsert = this.db.prepare(`
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
    `);
    const now = new Date().toISOString();
    this.db.exec("BEGIN");
    try {
      for (const item of items) {
        const bridgeItemId = makeBridgeItemId(item.logicalKey);
        upsert.run(
          item.serverId,
          item.itemId,
          item.libraryId,
          item.itemType,
          item.logicalKey,
          bridgeItemId,
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

  listIndexedItems(itemTypes: string[] = [], searchTerm?: string): IndexedItemRecord[] {
    const normalizedTypes = normalizeItemTypes(itemTypes);
    const search = indexedItemSearch(searchTerm);
    const rows = normalizedTypes.length === 0
      ? this.db.prepare(`
        SELECT * FROM indexed_items
        WHERE 1 = 1${search.clause}
        ORDER BY server_id, library_id, item_id
      `).all(...search.values) as Row[]
      : this.db.prepare(`
        SELECT * FROM indexed_items
        WHERE lower(item_type) IN (${placeholders(normalizedTypes)})${search.clause}
        ORDER BY server_id, library_id, item_id
      `).all(...normalizedTypes, ...search.values) as Row[];
    return rows.map(indexedItemRecordFromRow);
  }

  listIndexedItemsUpdatedBetween(fromTimestamp: string, toTimestamp: string, itemTypes: string[] = []): IndexedItemRecord[] {
    const normalizedTypes = normalizeItemTypes(itemTypes);
    const typeClause = normalizedTypes.length === 0 ? "" : ` AND lower(item_type) IN (${placeholders(normalizedTypes)})`;
    const rows = this.db.prepare(`
      SELECT * FROM indexed_items
      WHERE updated_at BETWEEN ? AND ?${typeClause}
      ORDER BY updated_at, server_id, library_id, item_id
    `).all(fromTimestamp, toTimestamp, ...normalizedTypes) as Row[];
    return rows.map(indexedItemRecordFromRow);
  }

  listIndexedItemsForSources(sources: Array<{ serverId: string; libraryId: string }>, itemTypes: string[] = [], searchTerm?: string): IndexedItemRecord[] {
    if (sources.length === 0) return [];
    const conditions = sources.map(() => "(server_id = ? AND library_id = ?)").join(" OR ");
    const values = sources.flatMap((source) => [source.serverId, source.libraryId]);
    const normalizedTypes = normalizeItemTypes(itemTypes);
    const typeClause = normalizedTypes.length === 0 ? "" : ` AND lower(item_type) IN (${placeholders(normalizedTypes)})`;
    const search = indexedItemSearch(searchTerm);
    const rows = this.db.prepare(`
      SELECT * FROM indexed_items
      WHERE (${conditions})${typeClause}${search.clause}
      ORDER BY server_id, library_id, item_id
    `).all(...values, ...normalizedTypes, ...search.values) as Row[];
    return rows.map(indexedItemRecordFromRow);
  }

  listIndexedChildItems(parentSources: IndexedItemParentRef[], itemTypes: string[] = [], searchTerm?: string): IndexedItemRecord[] {
    if (parentSources.length === 0) return [];
    const normalizedTypes = normalizeItemTypes(itemTypes);
    const typeClause = normalizedTypes.length === 0 ? "" : ` AND lower(item_type) IN (${placeholders(normalizedTypes)})`;
    const search = indexedItemSearch(searchTerm);
    const subqueries: string[] = [];
    const values: string[] = [];
    for (const source of parentSources) {
      for (const field of ["ParentId", "SeriesId", "SeasonId"]) {
        subqueries.push(`
          SELECT * FROM indexed_items
          WHERE server_id = ? AND library_id = ?${typeClause}${search.clause} AND json_extract(json, '$.${field}') = ?
        `);
        values.push(source.serverId, source.libraryId, ...normalizedTypes, ...search.values, source.itemId);
      }
    }
    const rows = this.db.prepare(`
      ${subqueries.join(" UNION ALL ")}
    `).all(...values) as Row[];
    const uniqueRows = new Map<string, Row>();
    for (const row of rows) {
      uniqueRows.set(`${String(row.server_id)}:${String(row.item_id)}`, row);
    }
    return Array.from(uniqueRows.values()).map(indexedItemRecordFromRow);
  }

  findIndexedItemsByBridgeId(bridgeItemId: string): IndexedItemRecord[] {
    const rows = this.db.prepare("SELECT * FROM indexed_items WHERE bridge_item_id = ? ORDER BY server_id, library_id, item_id").all(bridgeItemId) as Row[];
    return rows.map(indexedItemRecordFromRow);
  }

  findIndexedItemsByBridgeIds(bridgeItemIds: string[]): Map<string, IndexedItemRecord[]> {
    const ids = uniqueStrings(bridgeItemIds);
    const byBridgeId = new Map<string, IndexedItemRecord[]>();
    for (const chunk of chunks(ids)) {
      const rows = this.db.prepare(`
        SELECT * FROM indexed_items
        WHERE bridge_item_id IN (${placeholders(chunk)})
        ORDER BY server_id, library_id, item_id
      `).all(...chunk) as Row[];
      for (const row of rows) {
        const key = String(row.bridge_item_id ?? makeBridgeItemId(String(row.logical_key)));
        const items = byBridgeId.get(key) ?? [];
        items.push(indexedItemRecordFromRow(row));
        byBridgeId.set(key, items);
      }
    }
    return byBridgeId;
  }

  findIndexedItemsBySourceId(itemId: string): IndexedItemRecord[] {
    const rows = this.db.prepare("SELECT * FROM indexed_items WHERE item_id = ? ORDER BY server_id, library_id, item_id").all(itemId) as Row[];
    return rows.map(indexedItemRecordFromRow);
  }

  findIndexedItemsBySourceIds(itemIds: string[]): Map<string, IndexedItemRecord[]> {
    const ids = uniqueStrings(itemIds);
    const bySourceId = new Map<string, IndexedItemRecord[]>();
    for (const chunk of chunks(ids)) {
      const rows = this.db.prepare(`
        SELECT * FROM indexed_items
        WHERE item_id IN (${placeholders(chunk)})
        ORDER BY server_id, library_id, item_id
      `).all(...chunk) as Row[];
      for (const row of rows) {
        const key = String(row.item_id);
        const items = bySourceId.get(key) ?? [];
        items.push(indexedItemRecordFromRow(row));
        bySourceId.set(key, items);
      }
    }
    return bySourceId;
  }

  removeIndexedItem(serverId: string, itemId: string): void {
    this.db.exec("BEGIN");
    try {
      this.db.prepare("DELETE FROM indexed_items WHERE server_id = ? AND item_id = ?").run(serverId, itemId);
      this.db.prepare("DELETE FROM media_source_mappings WHERE server_id = ? AND upstream_item_id = ?").run(serverId, itemId);
      this.db.prepare("DELETE FROM playback_sessions WHERE server_id = ? AND upstream_item_id = ?").run(serverId, itemId);
      this.db.exec("COMMIT");
    } catch (error) {
      this.db.exec("ROLLBACK");
      throw error;
    }
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

  createInfuseSyncCheckpoint(deviceId: string, userId: string): InfuseSyncCheckpointRecord {
    const now = new Date().toISOString();
    const previous = this.db.prepare(`
      SELECT sync_timestamp FROM infuse_sync_checkpoints
      WHERE device_id = ? AND user_id = ? AND sync_timestamp IS NOT NULL
      ORDER BY sync_timestamp DESC
      LIMIT 1
    `).get(deviceId, userId) as Row | undefined;
    const checkpoint: InfuseSyncCheckpointRecord = {
      id: randomUUID(),
      deviceId,
      userId,
      fromTimestamp: previous?.sync_timestamp === null || previous?.sync_timestamp === undefined ? now : String(previous.sync_timestamp),
      syncTimestamp: null,
      createdAt: now,
      updatedAt: now
    };

    this.db.exec("BEGIN");
    try {
      this.db.prepare("DELETE FROM infuse_sync_checkpoints WHERE device_id = ? AND user_id = ?").run(deviceId, userId);
      this.db.prepare(`
        INSERT INTO infuse_sync_checkpoints (id, device_id, user_id, from_timestamp, sync_timestamp, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
      `).run(
        checkpoint.id,
        checkpoint.deviceId,
        checkpoint.userId,
        checkpoint.fromTimestamp,
        checkpoint.syncTimestamp,
        checkpoint.createdAt,
        checkpoint.updatedAt
      );
      this.db.exec("COMMIT");
    } catch (error) {
      this.db.exec("ROLLBACK");
      throw error;
    }

    return checkpoint;
  }

  getInfuseSyncCheckpoint(id: string): InfuseSyncCheckpointRecord | undefined {
    const row = this.db.prepare("SELECT * FROM infuse_sync_checkpoints WHERE id = ?").get(id) as Row | undefined;
    return row ? infuseSyncCheckpointFromRow(row) : undefined;
  }

  startInfuseSyncCheckpoint(id: string): InfuseSyncCheckpointRecord | undefined {
    const now = new Date().toISOString();
    const result = this.db.prepare(`
      UPDATE infuse_sync_checkpoints
      SET sync_timestamp = ?, updated_at = ?
      WHERE id = ?
    `).run(now, now, id);
    return result.changes === 0 ? undefined : this.getInfuseSyncCheckpoint(id);
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
      CREATE INDEX IF NOT EXISTS idx_indexed_items_library_type ON indexed_items(server_id, library_id, item_type);
      CREATE INDEX IF NOT EXISTS idx_indexed_items_lower_type ON indexed_items(lower(item_type));
      CREATE INDEX IF NOT EXISTS idx_indexed_items_library_lower_type ON indexed_items(server_id, library_id, lower(item_type));
      CREATE INDEX IF NOT EXISTS idx_indexed_items_lower_type_updated_at ON indexed_items(lower(item_type), updated_at);
      CREATE INDEX IF NOT EXISTS idx_indexed_items_parent_id ON indexed_items(server_id, library_id, item_type, json_extract(json, '$.ParentId'));
      CREATE INDEX IF NOT EXISTS idx_indexed_items_series_id ON indexed_items(server_id, library_id, item_type, json_extract(json, '$.SeriesId'));
      CREATE INDEX IF NOT EXISTS idx_indexed_items_season_id ON indexed_items(server_id, library_id, item_type, json_extract(json, '$.SeasonId'));
      CREATE INDEX IF NOT EXISTS idx_indexed_items_parent_id_lower_type ON indexed_items(server_id, library_id, lower(item_type), json_extract(json, '$.ParentId'));
      CREATE INDEX IF NOT EXISTS idx_indexed_items_series_id_lower_type ON indexed_items(server_id, library_id, lower(item_type), json_extract(json, '$.SeriesId'));
      CREATE INDEX IF NOT EXISTS idx_indexed_items_season_id_lower_type ON indexed_items(server_id, library_id, lower(item_type), json_extract(json, '$.SeasonId'));
      CREATE INDEX IF NOT EXISTS idx_indexed_items_item_id ON indexed_items(item_id);

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
	        last_played_date TEXT,
	        updated_at TEXT NOT NULL,
	        PRIMARY KEY (user_id, item_id)
	      );

      CREATE INDEX IF NOT EXISTS idx_user_item_data_user_updated_at ON user_item_data(user_id, updated_at);

      CREATE TABLE IF NOT EXISTS infuse_sync_checkpoints (
        id TEXT PRIMARY KEY,
        device_id TEXT NOT NULL,
        user_id TEXT NOT NULL,
        from_timestamp TEXT NOT NULL,
        sync_timestamp TEXT,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
      );

      CREATE INDEX IF NOT EXISTS idx_infuse_sync_checkpoints_device_user ON infuse_sync_checkpoints(device_id, user_id);

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
    this.ensureColumn("user_item_data", "last_played_date", "TEXT");
    this.backfillBridgeItemIds();
    this.db.exec("CREATE INDEX IF NOT EXISTS idx_indexed_items_bridge_item_id ON indexed_items(bridge_item_id)");
    this.db.exec("CREATE INDEX IF NOT EXISTS idx_indexed_items_updated_at ON indexed_items(updated_at)");
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

function indexedItemRecordFromRow(row: Row): IndexedItemRecord {
  const { bridgeItemId: _bridgeItemId, ...item } = indexedItemFromRow(row);
  return item;
}

function userDataDtoFromRow(row: Row | undefined, itemId: string): Record<string, unknown> {
  return {
    PlaybackPositionTicks: Number(row?.playback_position_ticks ?? 0),
    PlayCount: Number(row?.play_count ?? 0),
    IsFavorite: Boolean(row?.is_favorite ?? 0),
    Played: Boolean(row?.played ?? 0),
    LastPlayedDate: row?.last_played_date === null || row?.last_played_date === undefined ? null : String(row.last_played_date),
    Key: itemId
  };
}

function userDataFromRow(row: Row): UserDataRecord {
  return {
    userId: String(row.user_id),
    itemId: String(row.item_id),
    playbackPositionTicks: Number(row.playback_position_ticks ?? 0),
    playCount: Number(row.play_count ?? 0),
    isFavorite: Boolean(row.is_favorite ?? 0),
    played: Boolean(row.played ?? 0),
    lastPlayedDate: row.last_played_date === null ? null : String(row.last_played_date),
    updatedAt: String(row.updated_at)
  };
}

function normalizeItemTypes(itemTypes: string[]): string[] {
  return uniqueStrings(itemTypes.map((itemType) => itemType.trim().toLowerCase()).filter(Boolean));
}

function indexedItemSearch(searchTerm: string | undefined): { clause: string; values: string[] } {
  const normalized = searchTerm?.trim().toLowerCase();
  if (!normalized) return { clause: "", values: [] };
  return {
    clause: `
      AND (
        instr(lower(COALESCE(json_extract(json, '$.Name'), '')), ?) > 0
        OR instr(lower(COALESCE(json_extract(json, '$.OriginalTitle'), '')), ?) > 0
        OR instr(lower(COALESCE(json_extract(json, '$.SortName'), '')), ?) > 0
      )
    `,
    values: [normalized, normalized, normalized]
  };
}

function uniqueStrings(values: string[]): string[] {
  return Array.from(new Set(values.filter(Boolean)));
}

function placeholders(values: unknown[]): string {
  return values.map(() => "?").join(", ");
}

function chunks<T>(values: T[], size = 900): T[][] {
  const output: T[][] = [];
  for (let index = 0; index < values.length; index += size) {
    output.push(values.slice(index, index + size));
  }
  return output;
}

function infuseSyncCheckpointFromRow(row: Row): InfuseSyncCheckpointRecord {
  return {
    id: String(row.id),
    deviceId: String(row.device_id),
    userId: String(row.user_id),
    fromTimestamp: String(row.from_timestamp),
    syncTimestamp: row.sync_timestamp === null ? null : String(row.sync_timestamp),
    createdAt: String(row.created_at),
    updatedAt: String(row.updated_at)
  };
}
