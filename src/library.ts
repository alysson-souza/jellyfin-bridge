import type { BridgeConfig } from "./config.js";
import { bridgeItemId, bridgeLibraryId, bridgeMediaSourceId, bridgeServerId, passThroughLibraryId } from "./ids.js";
import { ITEM_ID_FIELDS, rewriteDto } from "./rewriter.js";
import type { IndexedItemParentRef, IndexedItemRecord, Store } from "./store.js";

export interface BrowseQuery {
  includeItemTypes?: string;
  mediaTypes?: string;
  parentId?: string;
  recursive?: boolean;
  genres?: string;
  genreIds?: string;
  tags?: string;
  studios?: string;
  studioIds?: string;
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

interface BridgeItemHydrationContext {
  userDataByItemId: Map<string, Record<string, unknown>>;
  indexedItemsBySourceId: Map<string, IndexedItemRecord[]>;
}

export function listBridgeItems(config: BridgeConfig, store: Store, userId: string, query: BrowseQuery = {}): Record<string, unknown>[] {
  return queryBridgeItems(config, store, userId, query).items;
}

export function queryBridgeItems(config: BridgeConfig, store: Store, userId: string, query: BrowseQuery = {}): BridgeItemsResult {
  const includeTypes = parseList(query.includeItemTypes);
  return queryBridgeItemGroups(config, store, userId, indexedItemGroupsForParent(config, store, query.parentId, query.recursive, includeTypes), query);
}

export function queryBridgeItemsFromIndexedItems(
  config: BridgeConfig,
  store: Store,
  userId: string,
  indexedItems: IndexedItemRecord[],
  query: BrowseQuery = {},
  inScope: (item: IndexedItemRecord) => boolean = () => true
): BridgeItemsResult {
  return queryBridgeItemGroups(config, store, userId, completeSourceGroups(store, indexedItems, inScope), query);
}

export function countBridgeItemsFromIndexedItems(
  config: BridgeConfig,
  store: Store,
  indexedItems: IndexedItemRecord[],
  includeItemTypes: string
): number {
  const includeTypes = parseList(includeItemTypes).map((type) => type.toLowerCase());
  return completeSourceGroups(store, indexedItems)
    .filter((sources) => {
      const selected = defaultSource(config, sources);
      const type = String(selected.json.Type ?? selected.itemType ?? "").toLowerCase();
      return includeTypes.length === 0 || includeTypes.includes(type);
    }).length;
}

function queryBridgeItemGroups(
  config: BridgeConfig,
  store: Store,
  userId: string,
  sourceGroups: IndexedItemRecord[][],
  query: BrowseQuery
): BridgeItemsResult {
  const includeTypes = parseList(query.includeItemTypes).map((type) => type.toLowerCase());
  const mediaTypes = parseList(query.mediaTypes).map((type) => type.toLowerCase());
  const genres = parseFilterList(query.genres).map((genre) => genre.toLowerCase());
  const genreIds = parseFilterList(query.genreIds).map(normalizedMetadataId);
  const tags = parseFilterList(query.tags).map((tag) => tag.toLowerCase());
  const studios = parseFilterList(query.studios).map((studio) => studio.toLowerCase());
  const studioIds = parseFilterList(query.studioIds).map(normalizedMetadataId);
  const artists = parseFilterList(query.artists).map((artist) => artist.toLowerCase());
  const years = parseNumberList(query.years);
  const officialRatings = parseFilterList(query.officialRatings).map((rating) => rating.toLowerCase());
  const filters = new Set(parseList(query.filters).map((filter) => filter.toLowerCase()));
  const person = query.person?.toLowerCase();
  const context = createHydrationContext(config, store, userId, sourceGroups);
  const groups = sourceGroups
    .map((sources) => toBridgeItem(config, store, userId, sources, context))
    .filter((item) => includeTypes.length === 0 || includeTypes.includes(String(item.Type ?? "").toLowerCase()))
    .filter((item) => mediaTypes.length === 0 || mediaTypes.includes(String(item.MediaType ?? "").toLowerCase()))
    .filter((item) => query.isFavorite === undefined || Boolean((item.UserData as Record<string, unknown> | undefined)?.IsFavorite) === query.isFavorite)
    .filter((item) => query.isPlayed === undefined || Boolean((item.UserData as Record<string, unknown> | undefined)?.Played) === query.isPlayed)
    .filter((item) => !filters.has("isfavorite") || Boolean((item.UserData as Record<string, unknown> | undefined)?.IsFavorite))
    .filter((item) => !filters.has("isplayed") || Boolean((item.UserData as Record<string, unknown> | undefined)?.Played))
    .filter((item) => !filters.has("isunplayed") || !Boolean((item.UserData as Record<string, unknown> | undefined)?.Played))
    .filter((item) => !filters.has("isresumable") || Number((item.UserData as Record<string, unknown> | undefined)?.PlaybackPositionTicks ?? 0) > 0)
    .filter((item) => !filters.has("isfolder") || Boolean(item.IsFolder))
    .filter((item) => !filters.has("isnotfolder") || !Boolean(item.IsFolder))
    .filter((item) => genres.length === 0 || intersects(genreNames(item), genres))
    .filter((item) => genreIds.length === 0 || intersects(metadataIds(genreNames(item), "genre"), genreIds))
    .filter((item) => tags.length === 0 || intersects(asStrings(item.Tags), tags))
    .filter((item) => studios.length === 0 || intersects(studioNames(item.Studios), studios))
    .filter((item) => studioIds.length === 0 || intersects(metadataIds(studioNames(item.Studios), "studio"), studioIds))
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

function completeSourceGroups(store: Store, items: IndexedItemRecord[], inScope: (item: IndexedItemRecord) => boolean = () => true): IndexedItemRecord[][] {
  const fallbacks = new Map<string, IndexedItemRecord[]>();
  for (const item of items) {
    const bridgeId = bridgeItemId(item.logicalKey);
    if (fallbacks.has(bridgeId)) continue;
    fallbacks.set(bridgeId, [item]);
  }
  const completeSources = store.findIndexedItemsByBridgeIds(Array.from(fallbacks.keys()));
  return Array.from(fallbacks.entries()).map(([bridgeId, fallback]) => {
    const sources = (completeSources.get(bridgeId) ?? []).filter(inScope);
    return sources && sources.length > 0 ? sources : fallback;
  });
}

function indexedItemGroupsForParent(config: BridgeConfig, store: Store, parentId: string | undefined, recursive = false, itemTypes: string[] = []): IndexedItemRecord[][] {
  if (!parentId) return groupsFromCandidateItems(store, store.listIndexedItems(itemTypes), itemTypes);
  const configuredLibrary = config.libraries.find((library) => bridgeLibraryId(library.id) === parentId);
  if (configuredLibrary) {
    const sources = configuredLibrary.sources.map((source) => ({
      serverId: source.server,
      libraryId: source.libraryId
    }));
    const items = store.listIndexedItemsForSources(sources, recursive ? itemTypes : []);
    return groupsFromCandidateItems(store, recursive ? items : directLibraryChildren(items, sources), recursive ? itemTypes : [], sourceScope(sources));
  }
  const passThroughLibrary = store.listUpstreamLibraries().find((library) => passThroughLibraryId(library.serverId, library.libraryId) === parentId);
  if (passThroughLibrary) {
    const sources = [{ serverId: passThroughLibrary.serverId, libraryId: passThroughLibrary.libraryId }];
    const items = store.listIndexedItemsForSources(sources, recursive ? itemTypes : []);
    return groupsFromCandidateItems(store, recursive ? items : directLibraryChildren(items, sources), recursive ? itemTypes : [], sourceScope(sources));
  }

  const parentSources = store.findIndexedItemsByBridgeId(parentId);
  if (parentSources.length === 0) return [];
  return groupsFromCandidateItems(store, store.listIndexedChildItems(parentSources, itemTypes), itemTypes, childScope(parentSources));
}

function groupsFromCandidateItems(
  store: Store,
  items: IndexedItemRecord[],
  itemTypes: string[] = [],
  inScope?: (item: IndexedItemRecord) => boolean
): IndexedItemRecord[][] {
  return itemTypes.length === 0 ? groupByLogicalKey(items) : completeSourceGroups(store, items, inScope);
}

function sourceScope(sources: Array<{ serverId: string; libraryId: string }>): (item: IndexedItemRecord) => boolean {
  const sourceKeys = new Set(sources.map((source) => `${source.serverId}:${source.libraryId}`));
  return (item) => sourceKeys.has(`${item.serverId}:${item.libraryId}`);
}

function childScope(parentSources: IndexedItemParentRef[]): (item: IndexedItemRecord) => boolean {
  const parentKeys = new Set(parentSources.map((source) => `${source.serverId}:${source.libraryId}:${source.itemId}`));
  return (item) => {
    const parentCandidates = [item.json.ParentId, item.json.SeriesId, item.json.SeasonId]
      .filter((value): value is string => typeof value === "string");
    return parentCandidates.some((candidate) => parentKeys.has(`${item.serverId}:${item.libraryId}:${candidate}`));
  };
}

function directLibraryChildren(items: IndexedItemRecord[], sources: Array<{ serverId: string; libraryId: string }>): IndexedItemRecord[] {
  const direct = items.filter((item) =>
    typeof item.json.ParentId === "string"
    && sources.some((source) => item.serverId === source.serverId && item.json.ParentId === source.libraryId)
  );
  return direct.length > 0 || items.some((item) => "ParentId" in item.json) ? direct : items;
}

export function getBridgeItem(config: BridgeConfig, store: Store, userId: string, itemId: string): Record<string, unknown> | undefined {
  const sources = store.findIndexedItemsByBridgeId(itemId);
  if (sources.length === 0) return undefined;
  return toBridgeItem(config, store, userId, sources);
}

export function bridgeItemSources(config: BridgeConfig, store: Store, itemId: string): IndexedItemRecord[] {
  return sortSourcesByPriority(config, store.findIndexedItemsByBridgeId(itemId));
}

export function listBridgeItemsForSourceParents(
  config: BridgeConfig,
  store: Store,
  userId: string,
  parentSources: IndexedItemParentRef[],
  itemTypes: string[] = []
): Record<string, unknown>[] {
  const includeTypes = itemTypes.map((type) => type.trim().toLowerCase()).filter(Boolean);
  const sourceGroups = groupsFromCandidateItems(store, store.listIndexedChildItems(parentSources, itemTypes), itemTypes, childScope(parentSources));
  const context = createHydrationContext(config, store, userId, sourceGroups);
  return sourceGroups
    .map((sources) => toBridgeItem(config, store, userId, sources, context))
    .filter((item) => includeTypes.length === 0 || includeTypes.includes(String(item.Type ?? "").toLowerCase()))
    .sort(compareItems({}));
}

export function bridgeItemIdMapForSourceItem(
  config: BridgeConfig,
  store: Store,
  serverId: string,
  sources: IndexedItemRecord[],
  bridgeId: string,
  item: Record<string, unknown>,
  context?: BridgeItemHydrationContext
): Map<string, string> {
  const itemIdMap = new Map(sources.map((source) => [source.itemId, bridgeId]));
  for (const source of sources) {
    itemIdMap.set(source.libraryId, bridgeLibraryIdForSource(config, source.serverId, source.libraryId));
  }
  for (const field of ITEM_ID_FIELDS) {
    const value = item[field];
    if (field !== "Id" && typeof value === "string") {
      const relatedSources = context
        ? context.indexedItemsBySourceId.get(value) ?? []
        : store.findIndexedItemsBySourceId(value);
      const related = relatedSources.find((candidate) => candidate.serverId === serverId);
      if (related) itemIdMap.set(value, bridgeItemId(related.logicalKey));
    }
  }
  return itemIdMap;
}

function bridgeLibraryIdForSource(config: BridgeConfig, serverId: string, libraryId: string): string {
  const configuredLibrary = config.libraries.find((library) =>
    library.sources.some((source) => source.server === serverId && source.libraryId === libraryId)
  );
  return configuredLibrary ? bridgeLibraryId(configuredLibrary.id) : passThroughLibraryId(serverId, libraryId);
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

function toBridgeItem(config: BridgeConfig, store: Store, userId: string, sources: IndexedItemRecord[], context?: BridgeItemHydrationContext): Record<string, unknown> {
  const selected = defaultSource(config, sources);
  const id = bridgeItemId(selected.logicalKey);
  const itemIdMap = bridgeItemIdMapForSourceItem(config, store, selected.serverId, sources, id, selected.json, context);
  const rewritten = rewriteDto(
    {
      ...selected.json,
      Id: selected.itemId,
      ServerId: selected.serverId
    },
    {
      serverId: selected.serverId,
      bridgeServerId: bridgeServerId(config.server.name),
      itemIdMap,
      mediaSourceIdMap: bridgeMediaSourceIdMap(selected.serverId, id, selected.json.MediaSources),
      rewriteUnknownItemIds: false
    }
  ) as Record<string, unknown>;

  rewritten.Id = id;
  if (typeof selected.json.SeriesId === "string") {
    rewritten.SeriesIdSource = selected.json.SeriesId;
  }
  if (typeof selected.json.SeasonId === "string") {
    rewritten.SeasonIdSource = selected.json.SeasonId;
  }
  const userData = context
    ? context.userDataByItemId.get(id) ?? defaultUserData(id)
    : store.getUserData(userId, id);
  rewritten.UserData = {
    ...userData,
    ItemId: id
  };
  return rewritten;
}

function createHydrationContext(config: BridgeConfig, store: Store, userId: string, sourceGroups: IndexedItemRecord[][]): BridgeItemHydrationContext {
  const bridgeIds = new Set<string>();
  const relatedSourceIds = new Set<string>();
  for (const sources of sourceGroups) {
    const selected = defaultSource(config, sources);
    bridgeIds.add(bridgeItemId(selected.logicalKey));
    for (const field of ITEM_ID_FIELDS) {
      const value = selected.json[field];
      if (field !== "Id" && typeof value === "string") {
        relatedSourceIds.add(value);
      }
    }
  }
  return {
    userDataByItemId: store.listUserData(userId, Array.from(bridgeIds)),
    indexedItemsBySourceId: store.findIndexedItemsBySourceIds(Array.from(relatedSourceIds))
  };
}

function defaultUserData(itemId: string): Record<string, unknown> {
  return {
    PlaybackPositionTicks: 0,
    PlayCount: 0,
    IsFavorite: false,
    Played: false,
    LastPlayedDate: null,
    Key: itemId
  };
}

function bridgeMediaSourceIdMap(serverId: string, itemId: string, mediaSources: unknown): Map<string, string> {
  const ids = new Map<string, string>();
  if (!Array.isArray(mediaSources)) return ids;
  for (const mediaSource of mediaSources) {
    if (!mediaSource || typeof mediaSource !== "object") continue;
    const id = (mediaSource as Record<string, unknown>).Id;
    if (typeof id === "string") {
      ids.set(id, bridgeMediaSourceId(serverId, itemId, id));
    }
  }
  return ids;
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
  const requestedSortBy = parseList(query.sortBy)[0]?.toLowerCase();
  const sortBy = requestedSortBy ?? "sortname";
  const descending = parseList(query.sortOrder)[0]?.toLowerCase() === "descending";
  return (left, right) => {
    const result = requestedSortBy
      ? compareValue(sortValue(left, sortBy), sortValue(right, sortBy))
      : compareDefaultValue(left, right);
    return descending ? -result : result;
  };
}

function compareDefaultValue(left: Record<string, unknown>, right: Record<string, unknown>): number {
  const episodeOrder = compareEpisodeOrder(left, right);
  if (episodeOrder !== undefined && episodeOrder !== 0) return episodeOrder;
  return compareValue(sortValue(left, "sortname"), sortValue(right, "sortname"));
}

function compareEpisodeOrder(left: Record<string, unknown>, right: Record<string, unknown>): number | undefined {
  if (String(left.Type ?? "").toLowerCase() !== "episode" || String(right.Type ?? "").toLowerCase() !== "episode") {
    return undefined;
  }
  const season = compareOptionalNumber(left.ParentIndexNumber, right.ParentIndexNumber);
  if (season !== 0) return season;
  return compareOptionalNumber(left.IndexNumber, right.IndexNumber);
}

function compareOptionalNumber(left: unknown, right: unknown): number {
  const leftNumber = numericSortValue(left);
  const rightNumber = numericSortValue(right);
  if (leftNumber === undefined && rightNumber === undefined) return 0;
  if (leftNumber === undefined) return 1;
  if (rightNumber === undefined) return -1;
  return leftNumber - rightNumber;
}

function numericSortValue(value: unknown): number | undefined {
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (typeof value === "string" && value.trim() !== "") {
    const parsed = Number(value);
    if (Number.isFinite(parsed)) return parsed;
  }
  return undefined;
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

function normalizedMetadataId(value: string): string {
  return value.replaceAll("-", "").toLowerCase();
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

function genreNames(item: Record<string, unknown>): string[] {
  return [
    ...asStrings(item.Genres),
    ...asRecords(item.GenreItems).map((genre) => genre.Name).filter((name): name is string => typeof name === "string")
  ];
}

function studioNames(value: unknown): string[] {
  return [...asStrings(value), ...asRecords(value).map((item) => item.Name).filter((name): name is string => typeof name === "string")];
}

function metadataIds(names: string[], type: "genre" | "studio"): string[] {
  return names.map((name) => bridgeItemId(`${type}:${name.toLowerCase()}`).toLowerCase());
}
