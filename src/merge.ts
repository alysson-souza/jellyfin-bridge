export interface SourceItem {
  Id: string;
  Type?: string;
  Name?: string;
  ProviderIds?: Record<string, string | undefined>;
  SeriesId?: string;
  SeasonId?: string;
  AlbumId?: string;
  Album?: string;
  AlbumArtist?: string;
  ProductionYear?: number;
  ParentIndexNumber?: number;
  IndexNumber?: number;
}

export interface SourceCandidate<T extends SourceItem = SourceItem> {
  serverId: string;
  priority: number;
  item: T;
}

export interface LogicalGroup<T extends SourceItem = SourceItem> {
  logicalKey: string;
  defaultSource: SourceCandidate<T>;
  sources: SourceCandidate<T>[];
}

export function mergeSources<T extends SourceItem>(sources: SourceCandidate<T>[]): LogicalGroup<T>[] {
  const groups = new Map<string, SourceCandidate<T>[]>();
  for (const source of sources) {
    const key = logicalItemKey(source.item, source.serverId);
    const group = groups.get(key) ?? [];
    group.push(source);
    groups.set(key, group);
  }

  return Array.from(groups.entries()).map(([logicalKey, groupSources]) => {
    const ordered = [...groupSources].sort((left, right) => left.priority - right.priority);
    return {
      logicalKey,
      defaultSource: ordered[0],
      sources: ordered
    };
  });
}

export function logicalItemKey(item: SourceItem, serverId = "unknown"): string {
  const type = (item.Type ?? "").toLowerCase();
  if (type === "movie") {
    return providerKey(item, "movie", ["Imdb", "Tmdb"]) ?? passThroughKey(serverId, item);
  }
  if (type === "series") {
    return providerKey(item, "series", ["Tvdb", "Tmdb", "Imdb"]) ?? passThroughKey(serverId, item);
  }
  if (type === "season") {
    const provider = providerKey(item, "season", ["Tvdb", "Tmdb", "Imdb"]);
    if (provider) return provider;
    if (item.SeriesId && item.IndexNumber !== undefined) {
      return `season:series:${normalizeToken(item.SeriesId)}:season:${item.IndexNumber}`;
    }
    return passThroughKey(serverId, item);
  }
  if (type === "episode") {
    const provider = providerKey(item, "episode", ["Tvdb", "Tmdb", "Imdb"]);
    if (provider) return provider;
    if (item.SeriesId && item.ParentIndexNumber !== undefined && item.IndexNumber !== undefined) {
      return `episode:series:${normalizeToken(item.SeriesId)}:season:${item.ParentIndexNumber}:episode:${item.IndexNumber}`;
    }
    return passThroughKey(serverId, item);
  }
  if (type === "musicalbum") {
    const provider = providerKey(item, "album", ["MusicBrainzAlbum"]);
    if (provider) return provider;
    if (item.AlbumArtist && item.Name && item.ProductionYear) {
      return `album:strict:${normalizeText(item.AlbumArtist)}:${normalizeText(item.Name)}:${item.ProductionYear}`;
    }
    return passThroughKey(serverId, item);
  }
  if (type === "audio" || type === "track") {
    const provider = providerKey(item, "track", ["MusicBrainzTrack", "MusicBrainzReleaseTrackId"]);
    if (provider) return provider;
    if (item.AlbumArtist && item.Album && item.ProductionYear && item.ParentIndexNumber !== undefined && item.IndexNumber !== undefined && item.Name) {
      return `track:strict:${normalizeText(item.AlbumArtist)}:${normalizeText(item.Album)}:${item.ProductionYear}:disc:${item.ParentIndexNumber}:track:${item.IndexNumber}:${normalizeText(item.Name)}`;
    }
    return passThroughKey(serverId, item);
  }
  return passThroughKey(serverId, item);
}

function providerKey(item: SourceItem, prefix: string, providerNames: string[]): string | undefined {
  const providerIds = item.ProviderIds ?? {};
  const normalizedProviders = new Map(Object.entries(providerIds).map(([key, value]) => [key.toLowerCase(), value]));
  for (const providerName of providerNames) {
    const value = normalizedProviders.get(providerName.toLowerCase());
    if (value) {
      return `${prefix}:${providerName.toLowerCase()}:${normalizeToken(value)}`;
    }
  }
  return undefined;
}

function passThroughKey(serverId: string, item: SourceItem): string {
  return `source:${normalizeToken(serverId)}:${normalizeToken(item.Id)}`;
}

function normalizeToken(value: string): string {
  return value.trim().toLowerCase();
}

function normalizeText(value: string): string {
  return value.trim().toLowerCase().replace(/\s+/g, " ");
}
