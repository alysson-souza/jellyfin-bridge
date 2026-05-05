import type { BridgeConfig } from "./config.js";
import { bridgeItemId, bridgeLibraryId, bridgeServerId, passThroughLibraryId } from "./ids.js";
import { ITEM_ID_FIELDS, rewriteDto } from "./rewriter.js";
import type { IndexedItemRecord, Store } from "./store.js";

export interface BrowseQuery {
  includeItemTypes?: string;
  mediaTypes?: string;
  parentId?: string;
  genres?: string;
  tags?: string;
  studios?: string;
  artists?: string;
  person?: string;
  years?: string;
  officialRatings?: string;
  filters?: string;
  isFavorite?: boolean;
  isPlayed?: boolean;
  sortBy?: string;
  sortOrder?: string;
  startIndex?: number;
  limit?: number;
}

export interface BridgeItemsResult {
  items: Record<string, unknown>[];
  total: number;
}

export function listBridgeItems(config: BridgeConfig, store: Store, userId: string, query: BrowseQuery = {}): Record<string, unknown>[] {
  return queryBridgeItems(config, store, userId, query).items;
}

export function queryBridgeItems(config: BridgeConfig, store: Store, userId: string, query: BrowseQuery = {}): BridgeItemsResult {
  const includeTypes = parseList(query.includeItemTypes).map((type) => type.toLowerCase());
  const movieOnlyPassThrough = includeTypes.includes("movie") && !includeTypes.includes("video") && isMovieCompatiblePassThroughParent(store, query.parentId);
  const effectiveIncludeTypes = movieOnlyPassThrough
    ? [...new Set([...includeTypes, "video"])]
    : includeTypes;
  const mediaTypes = parseList(query.mediaTypes).map((type) => type.toLowerCase());
  const genres = parseFilterList(query.genres).map((genre) => genre.toLowerCase());
  const tags = parseFilterList(query.tags).map((tag) => tag.toLowerCase());
  const studios = parseFilterList(query.studios).map((studio) => studio.toLowerCase());
  const artists = parseFilterList(query.artists).map((artist) => artist.toLowerCase());
  const years = parseNumberList(query.years);
  const officialRatings = parseFilterList(query.officialRatings).map((rating) => rating.toLowerCase());
  const filters = new Set(parseList(query.filters).map((filter) => filter.toLowerCase()));
  const person = query.person?.toLowerCase();
  const groups = groupByLogicalKey(indexedItemsForParent(config, store, query.parentId))
    .map((sources) => toBridgeItem(config, store, userId, sources))
    .map((item) => movieOnlyPassThrough ? coerceVideoAsMovie(item) : item)
    .filter((item) => effectiveIncludeTypes.length === 0 || effectiveIncludeTypes.includes(String(item.Type ?? "").toLowerCase()))
    .filter((item) => mediaTypes.length === 0 || mediaTypes.includes(String(item.MediaType ?? "").toLowerCase()))
    .filter((item) => query.isFavorite === undefined || Boolean((item.UserData as Record<string, unknown> | undefined)?.IsFavorite) === query.isFavorite)
    .filter((item) => query.isPlayed === undefined || Boolean((item.UserData as Record<string, unknown> | undefined)?.Played) === query.isPlayed)
    .filter((item) => !filters.has("isfavorite") || Boolean((item.UserData as Record<string, unknown> | undefined)?.IsFavorite))
    .filter((item) => !filters.has("isplayed") || Boolean((item.UserData as Record<string, unknown> | undefined)?.Played))
    .filter((item) => !filters.has("isunplayed") || !Boolean((item.UserData as Record<string, unknown> | undefined)?.Played))
    .filter((item) => !filters.has("isresumable") || Number((item.UserData as Record<string, unknown> | undefined)?.PlaybackPositionTicks ?? 0) > 0)
    .filter((item) => !filters.has("isfolder") || Boolean(item.IsFolder))
    .filter((item) => !filters.has("isnotfolder") || !Boolean(item.IsFolder))
    .filter((item) => genres.length === 0 || intersects(asStrings(item.Genres), genres))
    .filter((item) => tags.length === 0 || intersects(asStrings(item.Tags), tags))
    .filter((item) => studios.length === 0 || intersects(studioNames(item.Studios), studios))
    .filter((item) => artists.length === 0 || intersects(asStrings(item.Artists), artists) || intersects(asStrings(item.AlbumArtists), artists) || matchesString(item.AlbumArtist, artists))
    .filter((item) => years.length === 0 || years.includes(Number(item.ProductionYear ?? 0)))
    .filter((item) => officialRatings.length === 0 || matchesString(item.OfficialRating, officialRatings))
    .filter((item) => !person || asRecords(item.People).some((entry) => String(entry.Name ?? "").toLowerCase() === person))
    .sort(compareItems(query));

  const start = query.startIndex ?? 0;
  const end = query.limit === undefined ? undefined : start + query.limit;
  return {
    items: groups.slice(start, end),
    total: groups.length
  };
}

function isMovieCompatiblePassThroughParent(store: Store, parentId: string | undefined): boolean {
  if (!parentId) return false;
  const library = store.listUpstreamLibraries().find((candidate) => passThroughLibraryId(candidate.serverId, candidate.libraryId) === parentId);
  const collectionType = library?.collectionType?.toLowerCase();
  return collectionType === "homevideos" || collectionType === "homevideo" || collectionType === null || collectionType === undefined;
}

function coerceVideoAsMovie(item: Record<string, unknown>): Record<string, unknown> {
  return String(item.Type ?? "").toLowerCase() === "video" ? { ...item, Type: "Movie" } : item;
}

function indexedItemsForParent(config: BridgeConfig, store: Store, parentId: string | undefined): IndexedItemRecord[] {
  if (!parentId) return store.listIndexedItems();
  const configuredLibrary = config.libraries.find((library) => bridgeLibraryId(library.id) === parentId);
  if (configuredLibrary) {
    return store.listIndexedItemsForSources(configuredLibrary.sources.map((source) => ({
      serverId: source.server,
      libraryId: source.libraryId
    })));
  }
  const passThroughLibrary = store.listUpstreamLibraries().find((library) => passThroughLibraryId(library.serverId, library.libraryId) === parentId);
  if (passThroughLibrary) {
    return store.listIndexedItemsForSources([{ serverId: passThroughLibrary.serverId, libraryId: passThroughLibrary.libraryId }]);
  }

  const items = store.listIndexedItems();
  const parentSources = items.filter((item) => bridgeItemId(item.logicalKey) === parentId);
  if (parentSources.length === 0) return [];
  const upstreamParents = new Set(parentSources.map((source) => `${source.serverId}:${source.itemId}`));
  return items.filter((item) => {
    const parentCandidates = [item.json.ParentId, item.json.SeriesId, item.json.SeasonId]
      .filter((value): value is string => typeof value === "string");
    return parentCandidates.some((candidate) => upstreamParents.has(`${item.serverId}:${candidate}`));
  });
}

export function getBridgeItem(config: BridgeConfig, store: Store, userId: string, itemId: string): Record<string, unknown> | undefined {
  const sources = store.findIndexedItemsByBridgeId(itemId);
  if (sources.length === 0) return undefined;
  return toBridgeItem(config, store, userId, sources);
}

export function bridgeItemSources(config: BridgeConfig, store: Store, itemId: string): IndexedItemRecord[] {
  return sortSourcesByPriority(config, store.findIndexedItemsByBridgeId(itemId));
}

export function bridgeItemIdMapForSourceItem(
  store: Store,
  serverId: string,
  sources: IndexedItemRecord[],
  bridgeId: string,
  item: Record<string, unknown>
): Map<string, string> {
  const itemIdMap = new Map(sources.map((source) => [source.itemId, bridgeId]));
  for (const field of ITEM_ID_FIELDS) {
    const value = item[field];
    if (field !== "Id" && typeof value === "string") {
      const related = store.findIndexedItemsBySourceId(value).find((candidate) => candidate.serverId === serverId);
      if (related) itemIdMap.set(value, bridgeItemId(related.logicalKey));
    }
  }
  return itemIdMap;
}

export function itemCounts(store: Store): Record<string, number> {
  const groups = groupByLogicalKey(store.listIndexedItems());
  const counts = {
    MovieCount: 0,
    SeriesCount: 0,
    EpisodeCount: 0,
    ArtistCount: 0,
    ProgramCount: 0,
    TrailerCount: 0,
    SongCount: 0,
    AlbumCount: 0,
    MusicVideoCount: 0,
    BoxSetCount: 0,
    BookCount: 0,
    ItemCount: groups.length
  };
  for (const sources of groups) {
    const type = String(sources[0]?.json.Type ?? "").toLowerCase();
    if (type === "movie") counts.MovieCount += 1;
    if (type === "series") counts.SeriesCount += 1;
    if (type === "episode") counts.EpisodeCount += 1;
    if (type === "audio" || type === "track") counts.SongCount += 1;
    if (type === "musicalbum") counts.AlbumCount += 1;
    if (type === "musicvideo") counts.MusicVideoCount += 1;
    if (type === "boxset") counts.BoxSetCount += 1;
    if (type === "book") counts.BookCount += 1;
  }
  return counts;
}

function toBridgeItem(config: BridgeConfig, store: Store, userId: string, sources: IndexedItemRecord[]): Record<string, unknown> {
  const selected = defaultSource(config, sources);
  const id = bridgeItemId(selected.logicalKey);
  const itemIdMap = bridgeItemIdMapForSourceItem(store, selected.serverId, sources, id, selected.json);
  const rewritten = rewriteDto(
    {
      ...selected.json,
      Id: selected.itemId,
      ServerId: selected.serverId
    },
    { serverId: selected.serverId, bridgeServerId: bridgeServerId(config.server.name), itemIdMap }
  ) as Record<string, unknown>;

  rewritten.Id = id;
  if (typeof selected.json.SeriesId === "string") {
    rewritten.SeriesIdSource = selected.json.SeriesId;
  }
  if (typeof selected.json.SeasonId === "string") {
    rewritten.SeasonIdSource = selected.json.SeasonId;
  }
  rewritten.UserData = {
    ...store.getUserData(userId, id),
    ItemId: id
  };
  return rewritten;
}

function groupByLogicalKey(items: IndexedItemRecord[]): IndexedItemRecord[][] {
  const groups = new Map<string, IndexedItemRecord[]>();
  for (const item of items) {
    const group = groups.get(item.logicalKey) ?? [];
    group.push(item);
    groups.set(item.logicalKey, group);
  }
  return Array.from(groups.values());
}

function defaultSource(config: BridgeConfig, sources: IndexedItemRecord[]): IndexedItemRecord {
  return sortSourcesByPriority(config, sources)[0];
}

function sortSourcesByPriority(config: BridgeConfig, sources: IndexedItemRecord[]): IndexedItemRecord[] {
  const priorities = sourcePriorities(config);
  return [...sources].sort((left, right) => {
    const leftPriority = priorities.get(`${left.serverId}:${left.libraryId}`) ?? Number.MAX_SAFE_INTEGER;
    const rightPriority = priorities.get(`${right.serverId}:${right.libraryId}`) ?? Number.MAX_SAFE_INTEGER;
    return leftPriority - rightPriority;
  });
}

function sourcePriorities(config: BridgeConfig): Map<string, number> {
  const priorities = new Map<string, number>();
  let priority = 0;
  for (const library of config.libraries) {
    for (const source of library.sources) {
      priorities.set(`${source.server}:${source.libraryId}`, priority);
      priority += 1;
    }
  }
  return priorities;
}

function compareItems(query: BrowseQuery): (left: Record<string, unknown>, right: Record<string, unknown>) => number {
  const sortBy = parseList(query.sortBy)[0]?.toLowerCase() ?? "sortname";
  const descending = parseList(query.sortOrder)[0]?.toLowerCase() === "descending";
  return (left, right) => {
    const result = compareValue(sortValue(left, sortBy), sortValue(right, sortBy));
    return descending ? -result : result;
  };
}

function sortValue(item: Record<string, unknown>, sortBy: string): unknown {
  if (sortBy === "datecreated") return item.DateCreated;
  if (sortBy === "premieredate") return item.PremiereDate;
  if (sortBy === "productionyear") return item.ProductionYear;
  if (sortBy === "communityrating") return item.CommunityRating;
  if (sortBy === "runtime") return item.RunTimeTicks;
  return item.SortName ?? item.Name ?? "";
}

function compareValue(left: unknown, right: unknown): number {
  if (typeof left === "number" || typeof right === "number") return Number(left ?? 0) - Number(right ?? 0);
  return String(left ?? "").localeCompare(String(right ?? ""));
}

function parseList(value: string | undefined): string[] {
  return value?.split(",").map((item) => item.trim()).filter(Boolean) ?? [];
}

function parseFilterList(value: string | undefined): string[] {
  return value?.split(/[|,]/).map((item) => item.trim()).filter(Boolean) ?? [];
}

function parseNumberList(value: string | undefined): number[] {
  return parseList(value).map(Number).filter((item) => Number.isInteger(item));
}

function asStrings(value: unknown): string[] {
  return Array.isArray(value) ? value.filter((item): item is string => typeof item === "string") : [];
}

function asRecords(value: unknown): Record<string, unknown>[] {
  return Array.isArray(value) ? value.filter((item): item is Record<string, unknown> => Boolean(item) && typeof item === "object") : [];
}

function intersects(values: string[], filters: string[]): boolean {
  const normalized = new Set(values.map((value) => value.toLowerCase()));
  return filters.some((filter) => normalized.has(filter));
}

function matchesString(value: unknown, filters: string[]): boolean {
  return typeof value === "string" && filters.includes(value.toLowerCase());
}

function studioNames(value: unknown): string[] {
  return [...asStrings(value), ...asRecords(value).map((item) => item.Name).filter((name): name is string => typeof name === "string")];
}
