import type { BridgeConfig } from "./config.js";
import { bridgeItemId, bridgeServerId } from "./ids.js";
import { listBridgeItems } from "./library.js";
import type { Store } from "./store.js";

export interface MetadataQuery {
  parentId?: string;
  searchTerm?: string;
  startIndex?: number;
  limit?: number;
  personTypes?: string;
}

export function listGenres(config: BridgeConfig, store: Store, userId: string, query: MetadataQuery = {}): Record<string, unknown>[] {
  return paginate(nameDtos(config, collectGenres(itemsForQuery(config, store, userId, query)), "Genre"), query);
}

export function listArtists(config: BridgeConfig, store: Store, userId: string, query: MetadataQuery = {}): Record<string, unknown>[] {
  return paginate(nameDtos(config, collectArtists(itemsForQuery(config, store, userId, query)), "MusicArtist"), query);
}

export function listAlbumArtists(config: BridgeConfig, store: Store, userId: string, query: MetadataQuery = {}): Record<string, unknown>[] {
  return paginate(nameDtos(config, collectAlbumArtists(itemsForQuery(config, store, userId, query)), "MusicArtist"), query);
}

export function listPersons(config: BridgeConfig, store: Store, userId: string, query: MetadataQuery = {}): Record<string, unknown>[] {
  const allowedTypes = parseList(query.personTypes).map((type) => type.toLowerCase());
  const names = new Map<string, { name: string; type?: string; role?: string }>();
  for (const item of itemsForQuery(config, store, userId, query)) {
    for (const person of asRecords(item.People)) {
      const name = stringValue(person.Name);
      if (!name) continue;
      const type = stringValue(person.Type);
      if (allowedTypes.length > 0 && (!type || !allowedTypes.includes(type.toLowerCase()))) continue;
      const key = name.toLowerCase();
      if (!names.has(key)) names.set(key, { name, type, role: stringValue(person.Role) });
    }
  }
  const dtos = Array.from(names.values())
    .sort((left, right) => left.name.localeCompare(right.name))
    .map((person) => ({
      ...nameDto(config, person.name, "Person"),
      PersonType: person.type,
      Role: person.role
    }));
  return paginate(dtos, query);
}

export function listStudios(config: BridgeConfig, store: Store, userId: string, query: MetadataQuery = {}): Record<string, unknown>[] {
  return paginate(nameDtos(config, collectStudios(itemsForQuery(config, store, userId, query)), "Studio"), query);
}

export function listYears(config: BridgeConfig, store: Store, userId: string, query: MetadataQuery = {}): Record<string, unknown>[] {
  return paginate(nameDtos(config, collectYears(itemsForQuery(config, store, userId, query)), "Year"), query);
}

export function findMetadataItem(config: BridgeConfig, name: string, type: MetadataItemType): Record<string, unknown> {
  return nameDto(config, name, type);
}

export function metadataTotal<T>(items: T[]): number {
  return items.length;
}

function itemsForQuery(config: BridgeConfig, store: Store, userId: string, query: MetadataQuery): Record<string, unknown>[] {
  const search = query.searchTerm?.toLowerCase();
  return listBridgeItems(config, store, userId, { parentId: query.parentId })
    .filter((item) => !search || String(item.Name ?? "").toLowerCase().includes(search) || itemContainsMetadata(item, search));
}

function itemContainsMetadata(item: Record<string, unknown>, search: string): boolean {
  return [
    ...collectGenres([item]),
    ...collectArtists([item]),
    ...collectAlbumArtists([item]),
    ...collectStudios([item]),
    ...collectYears([item]),
    ...asRecords(item.People).map((person) => stringValue(person.Name)).filter(Boolean)
  ].some((name) => name.toLowerCase().includes(search));
}

function collectGenres(items: Record<string, unknown>[]): string[] {
  return uniqueSorted(items.flatMap((item) => [...asStrings(item.Genres), ...asRecords(item.GenreItems).map((genre) => stringValue(genre.Name)).filter(Boolean)]));
}

function collectArtists(items: Record<string, unknown>[]): string[] {
  return uniqueSorted(items.flatMap((item) => [...asStrings(item.Artists), ...asRecords(item.ArtistItems).map((artist) => stringValue(artist.Name)).filter(Boolean)]));
}

function collectAlbumArtists(items: Record<string, unknown>[]): string[] {
  return uniqueSorted(items.flatMap((item) => [...asStrings(item.AlbumArtists), stringValue(item.AlbumArtist)].filter(Boolean)));
}

function collectStudios(items: Record<string, unknown>[]): string[] {
  return uniqueSorted(items.flatMap((item) => [...asStrings(item.Studios), ...asRecords(item.Studios).map((studio) => stringValue(studio.Name)).filter(Boolean)]));
}

function collectYears(items: Record<string, unknown>[]): string[] {
  return uniqueSorted(items.map((item) => typeof item.ProductionYear === "number" ? String(item.ProductionYear) : "").filter(Boolean));
}

type MetadataItemType = "Genre" | "MusicArtist" | "Person" | "Studio" | "Year";

function nameDtos(config: BridgeConfig, names: string[], type: MetadataItemType): Record<string, unknown>[] {
  return names.map((name) => nameDto(config, name, type));
}

function nameDto(config: BridgeConfig, name: string, type: MetadataItemType): Record<string, unknown> {
  const key = `${type.toLowerCase()}:${name.toLowerCase()}`;
  const id = bridgeItemId(key);
  return {
    Name: name,
    ServerId: bridgeServerId(config.server.name),
    Id: id,
    Etag: id,
    DateCreated: new Date(0).toISOString(),
    SortName: name,
    ExternalUrls: [],
    ProviderIds: {},
    IsFolder: type !== "Person",
    ParentId: null,
    Type: type,
    UserData: { PlaybackPositionTicks: 0, PlayCount: 0, IsFavorite: false, Played: false, Key: id, ItemId: id },
    ChildCount: 0,
    DisplayPreferencesId: id,
    Tags: [],
    MediaType: "Unknown"
  };
}

function paginate<T>(items: T[], query: MetadataQuery): T[] {
  const start = query.startIndex ?? 0;
  return items.slice(start, query.limit === undefined ? undefined : start + query.limit);
}

function uniqueSorted(values: string[]): string[] {
  const byLowerName = new Map<string, string>();
  for (const value of values) {
    const trimmed = value.trim();
    if (trimmed && !byLowerName.has(trimmed.toLowerCase())) byLowerName.set(trimmed.toLowerCase(), trimmed);
  }
  return Array.from(byLowerName.values()).sort((left, right) => left.localeCompare(right));
}

function asStrings(value: unknown): string[] {
  return Array.isArray(value) ? value.filter((item): item is string => typeof item === "string") : [];
}

function asRecords(value: unknown): Record<string, unknown>[] {
  return Array.isArray(value) ? value.filter((item): item is Record<string, unknown> => Boolean(item) && typeof item === "object") : [];
}

function parseList(value: string | undefined): string[] {
  return value?.split(",").map((item) => item.trim()).filter(Boolean) ?? [];
}

function stringValue(value: unknown): string {
  return typeof value === "string" ? value : "";
}
