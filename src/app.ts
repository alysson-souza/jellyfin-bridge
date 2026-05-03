import Fastify, { type FastifyInstance, type FastifyReply, type FastifyRequest } from "fastify";
import { authenticatePassword, parseAuthorization, requireSession, type AuthContext, userDto, userId } from "./auth.js";
import type { BridgeConfig, BridgeUser, RuntimeConfigSource } from "./config.js";
import { badGatewayError, notFound, unsupported } from "./errors.js";
import { bridgeItemId, bridgeLibraryId, bridgeMediaSourceId, bridgeServerId, passThroughLibraryId } from "./ids.js";
import { rewriteHlsPlaylist } from "./hls.js";
import { Indexer } from "./indexer.js";
import { libraryDto, passThroughLibraryDto, publicSystemInfo, queryResult, sessionInfo, systemInfo, userDataDto } from "./jellyfin.js";
import { bridgeItemSources, getBridgeItem, itemCounts, listBridgeItems, queryBridgeItems } from "./library.js";
import type { BrowseQuery } from "./library.js";
import { logicalItemKey, type SourceItem } from "./merge.js";
import { findMetadataItem, listAlbumArtists, listArtists, listGenres, listPersons, listStudios, listYears } from "./metadata.js";
import { rewriteDto } from "./rewriter.js";
import type { IndexedItemRecord, Store } from "./store.js";
import { UpstreamClient } from "./upstream.js";

interface AppUpstreamClient {
  json<T>(serverId: string, path: string, init?: unknown): Promise<T>;
  raw?(serverId: string, path: string, init?: unknown): Promise<{ statusCode: number; headers: Record<string, string | string[] | undefined>; body: unknown }>;
}

export interface AppDependencies {
  config: BridgeConfig | RuntimeConfigSource;
  store: Store;
  upstream?: AppUpstreamClient;
  upstreamFactory?: (upstreams: BridgeConfig["upstreams"]) => AppUpstreamClient;
  verifyPassword?: (user: BridgeUser, password: string) => Promise<boolean>;
}

interface LiveSource {
  serverId: string;
  libraryId?: string;
  bridgeLibraryId?: string;
  priority: number;
}

interface LiveCandidate {
  source: LiveSource;
  item: Record<string, unknown>;
}

interface RawProxyCandidate {
  serverId: string;
  path: string;
}

const LIVE_BROWSE_FIELDS = [
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

export function buildApp(dependencies: AppDependencies): FastifyInstance {
  const configSource = toRuntimeConfigSource(dependencies.config);
  let config = configSource.current();
  const { store } = dependencies;
  const verifyPassword = dependencies.verifyPassword ?? authenticatePassword;
  const createUpstream = dependencies.upstreamFactory ?? ((upstreams: BridgeConfig["upstreams"]) => new UpstreamClient(upstreams));
  let upstream = dependencies.upstream ?? createUpstream(config.upstreams);
  const liveRouteAggregation = dependencies.upstream !== undefined || dependencies.upstreamFactory !== undefined;
  const app = Fastify({
    logger: process.env.JELLYFIN_BRIDGE_LOG_REQUESTS === "1",
    rewriteUrl: (request) => stripEmbyBasePath(request.url ?? "/")
  });
  allowEmptyJsonDeleteBodies(app);
  let serverId = bridgeServerId(config.server.name);
  let configVersion = 0;
  const liveUserCache = new Map<string, { expiresAt: number; promise: Promise<string | undefined> }>();
  const liveUserCacheTtlMs = 5 * 60_000;
  const unsubscribeConfig = configSource.subscribe((nextConfig) => {
    config = nextConfig;
    configVersion += 1;
    serverId = bridgeServerId(nextConfig.server.name);
    if (!dependencies.upstream) {
      upstream = createUpstream(nextConfig.upstreams);
    }
    liveUserCache.clear();
  });

  app.addHook("onClose", async () => {
    unsubscribeConfig();
  });

  app.setErrorHandler((error: unknown, _request, reply) => {
    const normalized = error instanceof Error ? error : new Error("Unknown error");
    const statusCode = hasStatusCode(normalized) ? normalized.statusCode : 500;
    reply.code(statusCode).send({
      type: "https://jellyfin.org/docs/general/server/api/",
      title: statusCode === 401 ? "Unauthorized" : statusCode === 403 ? "Forbidden" : statusCode === 502 ? "Bad Gateway" : "Error",
      status: statusCode,
      detail: normalized.message
    });
  });

  app.get("/System/Info/Public", async () => publicSystemInfo(config));
  app.get("/System/Ping", async () => config.server.name);
  app.post("/System/Ping", async () => config.server.name);

  app.get("/System/Info", async (request) => {
    requireSession(request, config, store);
    return systemInfo(config);
  });

  app.get("/System/Endpoint", async (request) => {
    requireSession(request, config, store);
    return { IsLocal: false, IsInNetwork: false };
  });

  app.post("/System/Shutdown", async (_request, reply) => unsupported(reply, "Server shutdown is not supported by Jellyfin Bridge"));
  app.post("/System/Restart", async (_request, reply) => unsupported(reply, "Server restart is not supported by Jellyfin Bridge"));

  app.post("/Bridge/Scan", async (request, reply) => {
    requireSession(request, config, store);
    await new Indexer(config, store, upstream).scanAllLibraries();
    reply.code(204).send();
  });

  app.get("/Users/Public", async () => config.auth.users.map((user) => userDto(user.name, serverId, config.server.name)));

  app.get("/Users/Me", async (request) => {
    const auth = requireSession(request, config, store);
    return userDto(auth.user.name, serverId, config.server.name);
  });

  app.get("/Users/:id", async (request, reply) => {
    requireSession(request, config, store);
    const { id } = request.params as { id: string };
    const user = config.auth.users.find((candidate) => userId(candidate.name) === id);
    if (!user) return notFound(reply, "User not found");
    return userDto(user.name, serverId, config.server.name);
  });

  app.post("/Users/AuthenticateByName", async (request, reply) => {
    const snapshotConfig = config;
    const snapshotServerId = serverId;
    const body = request.body as { Username?: string; username?: string; Pw?: string; pw?: string; Password?: string; password?: string } | undefined;
    const username = body?.Username ?? body?.username;
    const password = body?.Pw ?? body?.pw ?? body?.Password ?? body?.password ?? "";
    const user = snapshotConfig.auth.users.find((candidate) => candidate.name.toLowerCase() === username?.toLowerCase());
    if (!user || !(await verifyPassword(user, password))) {
      reply.code(401);
      return {
        type: "https://jellyfin.org/docs/general/server/api/",
        title: "Unauthorized",
        status: 401,
        detail: "Invalid username or password"
      };
    }

    const authInfo = parseAuthorization(request);
    const dto = userDto(user.name, snapshotServerId, snapshotConfig.server.name);
    const session = store.createSession(String(dto.Id), user.name, authInfo.deviceId, authInfo.device);
    return {
      User: dto,
      SessionInfo: sessionInfo(session, snapshotConfig.server.name),
      AccessToken: session.accessToken,
      ServerId: snapshotServerId
    };
  });

  app.post("/Sessions/Logout", async (request, reply) => {
    const token = parseAuthorization(request).token;
    if (!token) {
      reply.code(401).send({ title: "Unauthorized", status: 401, detail: "Missing access token" });
      return;
    }
    store.deleteSession(token);
    reply.code(204).send();
  });

  app.get("/UserViews", async (request) => {
    const auth = requireSession(request, config, store);
    requireSelf(auth, userIdFromQuery(request.query));
    const snapshotConfig = config;
    const snapshotServerId = serverId;
    const client = upstream;
    await refreshLiveViewsForRequest(snapshotConfig, client, request.query);
    return queryResult(viewDtos(snapshotConfig, snapshotServerId));
  });

  app.get("/Users/:userId/Views", async (request) => {
    const auth = requireSession(request, config, store);
    const params = request.params as { userId: string };
    requireSelf(auth, params.userId);
    const snapshotConfig = config;
    const snapshotServerId = serverId;
    const client = upstream;
    await refreshLiveViewsForRequest(snapshotConfig, client, request.query);
    return queryResult(viewDtos(snapshotConfig, snapshotServerId));
  });

  app.get("/UserViews/GroupingOptions", async (request) => {
    const auth = requireSession(request, config, store);
    requireSelf(auth, userIdFromQuery(request.query));
    return groupingOptions();
  });

  app.get("/Users/:userId/GroupingOptions", async (request) => {
    const auth = requireSession(request, config, store);
    const params = request.params as { userId: string };
    requireSelf(auth, params.userId);
    return groupingOptions();
  });

  app.get("/Library/VirtualFolders", async (request) => {
    requireSession(request, config, store);
    return virtualFolders();
  });

  app.get("/DisplayPreferences/:displayPreferencesId", async (request) => {
    requireSession(request, config, store);
    const { displayPreferencesId } = request.params as { displayPreferencesId: string };
    const query = request.query as { client?: string; Client?: string };
    return displayPreferencesDto(displayPreferencesId, query.client ?? query.Client ?? "");
  });

  app.get("/Items/Root", async (request) => {
    requireSession(request, config, store);
    return {
      Name: "Root",
      ServerId: serverId,
      Id: "00000000000000000000000000000000",
      Type: "AggregateFolder",
      IsFolder: true
    };
  });

  app.get("/Items", async (request) => {
    const auth = requireSession(request, config, store);
    const query = browseQuery(request.query);
    return browseItems(auth.session.userId, query);
  });
  app.get("/Users/:userId/Items", async (request) => {
    const auth = requireSession(request, config, store);
    const params = request.params as { userId: string };
    const userIdValue = requireSelf(auth, params.userId);
    const query = browseQuery(request.query);
    return browseItems(userIdValue, query);
  });
  app.get("/Items/Latest", async (request) => {
    const auth = requireSession(request, config, store);
    const snapshotConfig = config;
    return await liveLatestItems(auth.user.name, request.query, request).catch(() => latestItems(snapshotConfig, auth.session.userId, request.query));
  });
  app.get("/Users/:userId/Items/Latest", async (request) => {
    const auth = requireSession(request, config, store);
    const params = request.params as { userId: string };
    const userIdValue = requireSelf(auth, params.userId);
    const snapshotConfig = config;
    return await liveLatestItems(auth.user.name, request.query, request).catch(() => latestItems(snapshotConfig, userIdValue, request.query));
  });
  app.get("/UserItems/Resume", async (request) => {
    const auth = requireSession(request, config, store);
    const snapshotConfig = config;
    return await liveQueryResult(auth.user.name, "/UserItems/Resume", request.query, request).catch(() => resumeItems(snapshotConfig, auth.session.userId, request.query));
  });
  app.get("/Users/:userId/Items/Resume", async (request) => {
    const auth = requireSession(request, config, store);
    const params = request.params as { userId: string };
    const userIdValue = requireSelf(auth, params.userId);
    const snapshotConfig = config;
    return await liveQueryResult(auth.user.name, "/UserItems/Resume", request.query, request).catch(() => resumeItems(snapshotConfig, userIdValue, request.query));
  });
  app.get("/Items/Suggestions", async (request) => {
    const auth = requireSession(request, config, store);
    return suggestedItems(auth.session.userId, request.query);
  });
  app.get("/Users/:userId/Suggestions", async (request) => {
    const auth = requireSession(request, config, store);
    const params = request.params as { userId: string };
    const userIdValue = requireSelf(auth, params.userId);
    return suggestedItems(userIdValue, request.query);
  });
  app.get("/Items/Counts", async (request) => {
    requireSession(request, config, store);
    return itemCounts(store);
  });

  app.get("/Items/Filters", async (request) => {
    const auth = requireSession(request, config, store);
    const items = listBridgeItems(config, store, auth.session.userId, browseQuery(request.query));
    return {
      Years: uniqueNumbers(items.map((item) => item.ProductionYear)),
      Genres: uniqueStrings(items.flatMap((item) => stringList(item.Genres))),
      Tags: uniqueStrings(items.flatMap((item) => stringList(item.Tags))),
      OfficialRatings: uniqueStrings(items.map((item) => typeof item.OfficialRating === "string" ? item.OfficialRating : ""))
    };
  });

  app.get("/Items/Filters2", async (request) => {
    const auth = requireSession(request, config, store);
    const items = listBridgeItems(config, store, auth.session.userId, browseQuery(request.query));
    return {
      Genres: uniqueStrings(items.flatMap((item) => stringList(item.Genres))).map((name) => ({
        Name: name,
        Id: bridgeItemId(`genre:${name.toLowerCase()}`)
      })),
      Tags: uniqueStrings(items.flatMap((item) => stringList(item.Tags))).map((name) => ({
        Name: name,
        Id: bridgeItemId(`tag:${name.toLowerCase()}`)
      }))
    };
  });

  app.get("/Search/Hints", async (request) => {
    const auth = requireSession(request, config, store);
    const query = request.query as { SearchTerm?: string; searchTerm?: string; Limit?: string; StartIndex?: string };
    const searchTerm = (query.SearchTerm ?? query.searchTerm ?? "").toLowerCase();
    const allItems = listBridgeItems(config, store, auth.session.userId)
      .filter((item) => String(item.Name ?? "").toLowerCase().includes(searchTerm))
      .map((item) => ({
        ItemId: item.Id,
        Id: item.Id,
        Name: item.Name,
        MatchedTerm: item.Name,
        IndexNumber: item.IndexNumber,
        ParentIndexNumber: item.ParentIndexNumber,
        ProductionYear: item.ProductionYear,
        Type: item.Type,
        MediaType: item.MediaType,
        RunTimeTicks: item.RunTimeTicks,
        PrimaryImageTag: (item.ImageTags as Record<string, unknown> | undefined)?.Primary,
        PrimaryImageItemId: item.Id,
        IsFolder: item.IsFolder,
        AlbumArtist: item.AlbumArtist,
        Artists: item.Artists
      }));
    const startIndex = query.StartIndex === undefined ? 0 : Number(query.StartIndex);
    const limit = query.Limit === undefined ? undefined : Number(query.Limit);
    return {
      SearchHints: allItems.slice(startIndex, limit === undefined ? undefined : startIndex + limit),
      TotalRecordCount: allItems.length
    };
  });

  app.get("/Genres", async (request) => {
    const auth = requireSession(request, config, store);
    const query = metadataQuery(request.query);
    const items = listGenres(config, store, auth.session.userId, query);
    return queryResult(items, query.startIndex ?? 0);
  });

  app.get("/Genres/:name", async (request) => {
    requireSession(request, config, store);
    return findMetadataItem(config, decodeURIComponent((request.params as { name: string }).name), "Genre");
  });

  app.get("/Artists", async (request) => {
    const auth = requireSession(request, config, store);
    const query = metadataQuery(request.query);
    const items = listArtists(config, store, auth.session.userId, query);
    return queryResult(items, query.startIndex ?? 0);
  });

  app.get("/Artists/AlbumArtists", async (request) => {
    const auth = requireSession(request, config, store);
    const query = metadataQuery(request.query);
    const items = listAlbumArtists(config, store, auth.session.userId, query);
    return queryResult(items, query.startIndex ?? 0);
  });

  app.get("/Artists/:name", async (request) => {
    requireSession(request, config, store);
    return findMetadataItem(config, decodeURIComponent((request.params as { name: string }).name), "MusicArtist");
  });

  app.get("/Persons", async (request) => {
    const auth = requireSession(request, config, store);
    const query = metadataQuery(request.query);
    const items = listPersons(config, store, auth.session.userId, query);
    return queryResult(items, query.startIndex ?? 0);
  });

  app.get("/Persons/:name", async (request) => {
    requireSession(request, config, store);
    return findMetadataItem(config, decodeURIComponent((request.params as { name: string }).name), "Person");
  });

  app.get("/Studios", async (request) => {
    const auth = requireSession(request, config, store);
    const query = metadataQuery(request.query);
    const items = listStudios(config, store, auth.session.userId, query);
    return queryResult(items, query.startIndex ?? 0);
  });

  app.get("/Studios/:name", async (request) => {
    requireSession(request, config, store);
    return findMetadataItem(config, decodeURIComponent((request.params as { name: string }).name), "Studio");
  });

  app.get("/Years", async (request) => {
    const auth = requireSession(request, config, store);
    const query = metadataQuery(request.query);
    const items = listYears(config, store, auth.session.userId, query);
    return queryResult(items, query.startIndex ?? 0);
  });

  app.get("/Shows/:seriesId/Seasons", async (request, reply) => {
    const auth = requireSession(request, config, store);
    const snapshotConfig = config;
    const client = upstream;
    const { seriesId } = request.params as { seriesId: string };
    const seriesSources = bridgeItemSources(snapshotConfig, store, seriesId);
    if (seriesSources.length === 0) {
      const live = await liveQueryResult(auth.user.name, `/Shows/${seriesId}/Seasons`, request.query, request).catch(() => undefined);
      if (live) return live;
    }
    if (seriesSources.length === 0) return notFound(reply, "Series not found");
    const upstreamSeriesIds = new Set(seriesSources.map((source) => source.itemId));
    let seasons = listBridgeItems(snapshotConfig, store, auth.session.userId)
      .filter((item) => String(item.Type).toLowerCase() === "season" && upstreamSeriesIds.has(String(item.SeriesIdSource ?? item.SeriesId ?? "")));
    if (seasons.length === 0) {
      await refreshLiveSeasons(client, seriesSources, request.query).catch((error: unknown) => {
        const detail = error instanceof Error ? error.message : String(error);
        throw badGatewayError(`Upstream seasons failed: ${detail}`);
      });
      seasons = listBridgeItems(snapshotConfig, store, auth.session.userId)
        .filter((item) => String(item.Type).toLowerCase() === "season" && upstreamSeriesIds.has(String(item.SeriesIdSource ?? item.SeriesId ?? "")));
    }
    return queryResult(seasons);
  });

  app.get("/Shows/:seriesId/Episodes", async (request, reply) => {
    const auth = requireSession(request, config, store);
    const snapshotConfig = config;
    const client = upstream;
    const { seriesId } = request.params as { seriesId: string };
    const query = request.query as { Season?: string; season?: string; SeasonId?: string; seasonId?: string };
    const seriesSources = bridgeItemSources(snapshotConfig, store, seriesId);
    const requestedSeasonId = query.SeasonId ?? query.seasonId;
    const seasonSources = requestedSeasonId ? bridgeItemSources(snapshotConfig, store, requestedSeasonId) : [];
    if (seriesSources.length === 0 && seasonSources.length === 0) {
      const live = await liveQueryResult(auth.user.name, `/Shows/${seriesId}/Episodes`, request.query, request).catch(() => undefined);
      if (live) return live;
    }
    if (seriesSources.length === 0 && seasonSources.length === 0) return notFound(reply, "Series not found");
    const upstreamSeriesIds = new Set(seriesSources.map((source) => source.itemId));
    const upstreamSeasonIds = new Set(seasonSources.map((source) => source.itemId));
    const seasonNumber = query.Season ?? query.season;
    let episodes = listBridgeItems(snapshotConfig, store, auth.session.userId)
      .filter((item) => String(item.Type).toLowerCase() === "episode")
      .filter((item) => upstreamSeriesIds.size === 0 || upstreamSeriesIds.has(String(item.SeriesIdSource ?? item.SeriesId ?? "")))
      .filter((item) => upstreamSeasonIds.size === 0 || upstreamSeasonIds.has(String(item.SeasonIdSource ?? item.SeasonId ?? "")))
      .filter((item) => seasonNumber === undefined || String(item.ParentIndexNumber) === seasonNumber);
    if (episodes.length === 0) {
      await refreshLiveEpisodes(client, seriesSources, seasonSources, request.query).catch((error: unknown) => {
        const detail = error instanceof Error ? error.message : String(error);
        throw badGatewayError(`Upstream episodes failed: ${detail}`);
      });
      episodes = listBridgeItems(snapshotConfig, store, auth.session.userId)
        .filter((item) => String(item.Type).toLowerCase() === "episode")
        .filter((item) => upstreamSeriesIds.size === 0 || upstreamSeriesIds.has(String(item.SeriesIdSource ?? item.SeriesId ?? "")))
        .filter((item) => upstreamSeasonIds.size === 0 || upstreamSeasonIds.has(String(item.SeasonIdSource ?? item.SeasonId ?? "")))
        .filter((item) => seasonNumber === undefined || String(item.ParentIndexNumber) === seasonNumber);
    }
    return queryResult(episodes);
  });

  app.get("/Shows/NextUp", async (request) => {
    const auth = requireSession(request, config, store);
    const snapshotConfig = config;
    const live = await liveQueryResult(auth.user.name, "/Shows/NextUp", request.query, request).catch(() => undefined);
    if (live) return live;
    const episodes = listBridgeItems(snapshotConfig, store, auth.session.userId)
      .filter((item) => String(item.Type).toLowerCase() === "episode")
      .filter((item) => !Boolean((item.UserData as Record<string, unknown> | undefined)?.Played));
    return queryResult(episodes);
  });

  app.get("/Shows/Upcoming", async (request) => {
    requireSession(request, config, store);
    return queryResult([]);
  });

  app.get("/Items/:itemId/Images/:imageType", async (request, reply) => proxyItemImage(request, reply));
  app.get("/Items/:itemId/Images/:imageType/:imageIndex", async (request, reply) => proxyItemImage(request, reply));

  app.get("/Items/:itemId/LocalTrailers", async (request) => {
    requireSession(request, config, store);
    return queryResult([]);
  });
  app.get("/Users/:userId/Items/:itemId/LocalTrailers", async (request) => {
    const auth = requireSession(request, config, store);
    const params = request.params as { userId: string };
    requireSelf(auth, params.userId);
    return queryResult([]);
  });
  app.get("/Items/:itemId/SpecialFeatures", async (request) => {
    requireSession(request, config, store);
    return queryResult([]);
  });
  app.get("/Users/:userId/Items/:itemId/SpecialFeatures", async (request) => {
    const auth = requireSession(request, config, store);
    const params = request.params as { userId: string };
    requireSelf(auth, params.userId);
    return queryResult([]);
  });

  app.get("/Items/:itemId", async (request, reply) => {
    const auth = requireSession(request, config, store);
    const { itemId } = request.params as { itemId: string };
    return itemDetail(request, reply, auth.user.name, auth.session.userId, itemId);
  });
  app.get("/Users/:userId/Items/:itemId", async (request, reply) => {
    const auth = requireSession(request, config, store);
    const { userId: routeUserId, itemId } = request.params as { userId: string; itemId: string };
    return itemDetail(request, reply, auth.user.name, requireSelf(auth, routeUserId), itemId);
  });

  app.delete("/Items/:itemId", async (request, reply) => {
    requireSession(request, config, store);
    const client = upstream;
    if (!client.raw) {
      unsupported(reply, "The configured upstream client does not support item deletion");
      return;
    }

    const { itemId } = request.params as { itemId: string };
    const source = deleteSourceForItem(itemId);
    if (!source) {
      notFound(reply, "Item not found");
      return;
    }

    try {
      await client.raw(source.serverId, `/Items/${source.itemId}`, { method: "DELETE" });
    } catch (error) {
      const detail = error instanceof Error ? error.message : String(error);
      throw badGatewayError(`Upstream item deletion failed: ${detail}`);
    }

    store.removeIndexedItem(source.serverId, source.itemId);
    reply.code(204).send();
  });

  async function itemDetail(request: FastifyRequest, reply: FastifyReply, userName: string, userIdValue: string, itemId: string): Promise<Record<string, unknown> | void> {
    const snapshotConfig = config;
    const client = upstream;
    const bridgeServerIdValue = serverId;
    const sources = bridgeItemSources(snapshotConfig, store, itemId);
    const item = viewDtos(snapshotConfig, bridgeServerIdValue).find((view) => view.Id === itemId)
      ?? (sources.length === 0 ? await getLiveItem(userName, itemId, request.query, request).catch(() => undefined) : undefined)
      ?? await getHydratedBridgeItem(itemId, userIdValue, request.query, snapshotConfig, client, bridgeServerIdValue);
    if (!item) return notFound(reply, "Item not found");
    return item;
  }

  app.get("/UserItems/:itemId/UserData", async (request) => {
    const auth = requireSession(request, config, store);
    const { itemId } = request.params as { itemId: string };
    return userDataDto(store, requireSelf(auth, userIdFromQuery(request.query)), itemId);
  });

  app.get("/Users/:userId/Items/:itemId/UserData", async (request) => {
    const auth = requireSession(request, config, store);
    const { userId: routeUserId, itemId } = request.params as { userId: string; itemId: string };
    return userDataDto(store, requireSelf(auth, routeUserId), itemId);
  });

  app.post("/UserItems/:itemId/UserData", async (request) => {
    const auth = requireSession(request, config, store);
    const { itemId } = request.params as { itemId: string };
    saveUserData(store, auth.session.userId, itemId, request.body);
    return userDataDto(store, auth.session.userId, itemId);
  });

  app.post("/Users/:userId/Items/:itemId/UserData", async (request) => {
    const auth = requireSession(request, config, store);
    const { userId: routeUserId, itemId } = request.params as { userId: string; itemId: string };
    const userIdValue = requireSelf(auth, routeUserId);
    saveUserData(store, userIdValue, itemId, request.body);
    return userDataDto(store, userIdValue, itemId);
  });

  app.post("/UserFavoriteItems/:itemId", async (request) => {
    const auth = requireSession(request, config, store);
    const { itemId } = request.params as { itemId: string };
    return setFavorite(auth.session.userId, itemId, true);
  });

  app.delete("/UserFavoriteItems/:itemId", async (request) => {
    const auth = requireSession(request, config, store);
    const { itemId } = request.params as { itemId: string };
    return setFavorite(auth.session.userId, itemId, false);
  });

  app.post("/Users/:userId/FavoriteItems/:itemId", async (request) => {
    const auth = requireSession(request, config, store);
    const { userId: routeUserId, itemId } = request.params as { userId: string; itemId: string };
    return setFavorite(requireSelf(auth, routeUserId), itemId, true);
  });

  app.delete("/Users/:userId/FavoriteItems/:itemId", async (request) => {
    const auth = requireSession(request, config, store);
    const { userId: routeUserId, itemId } = request.params as { userId: string; itemId: string };
    return setFavorite(requireSelf(auth, routeUserId), itemId, false);
  });

  app.post("/UserPlayedItems/:itemId", async (request) => {
    const auth = requireSession(request, config, store);
    const { itemId } = request.params as { itemId: string };
    return setPlayed(auth.session.userId, itemId, true);
  });

  app.delete("/UserPlayedItems/:itemId", async (request) => {
    const auth = requireSession(request, config, store);
    const { itemId } = request.params as { itemId: string };
    return setPlayed(auth.session.userId, itemId, false);
  });

  app.post("/Users/:userId/PlayedItems/:itemId", async (request) => {
    const auth = requireSession(request, config, store);
    const { userId: routeUserId, itemId } = request.params as { userId: string; itemId: string };
    return setPlayed(requireSelf(auth, routeUserId), itemId, true);
  });

  app.delete("/Users/:userId/PlayedItems/:itemId", async (request) => {
    const auth = requireSession(request, config, store);
    const { userId: routeUserId, itemId } = request.params as { userId: string; itemId: string };
    return setPlayed(requireSelf(auth, routeUserId), itemId, false);
  });

  app.post("/Sessions/Playing", async (request, reply) => {
    const auth = requireSession(request, config, store);
    await forwardPlaybackReport("/Sessions/Playing", request.body, auth.user.name);
    reply.code(204).send();
  });
  app.post("/Sessions/Playing/Progress", async (request, reply) => {
    const auth = requireSession(request, config, store);
    const body = request.body as { ItemId?: string; PositionTicks?: number } | undefined;
    if (!liveRouteAggregation && body?.ItemId) {
      store.upsertUserData(auth.session.userId, body.ItemId, { playbackPositionTicks: body.PositionTicks ?? 0 });
    }
    await forwardPlaybackReport("/Sessions/Playing/Progress", request.body, auth.user.name);
    reply.code(204).send();
  });
  app.post("/Sessions/Playing/Stopped", async (request, reply) => {
    const auth = requireSession(request, config, store);
    const body = request.body as { ItemId?: string; PositionTicks?: number } | undefined;
    if (!liveRouteAggregation && body?.ItemId) {
      store.upsertUserData(auth.session.userId, body.ItemId, { playbackPositionTicks: body.PositionTicks ?? 0 });
    }
    await forwardPlaybackReport("/Sessions/Playing/Stopped", request.body, auth.user.name);
    reply.code(204).send();
  });
  app.post("/Sessions/Playing/Ping", async (request, reply) => {
    const auth = requireSession(request, config, store);
    await forwardPlaybackReport("/Sessions/Playing/Ping", request.body, auth.user.name);
    reply.code(204).send();
  });

  app.get("/Items/:itemId/PlaybackInfo", async (request, reply) => {
    const auth = requireSession(request, config, store);
    return getPlaybackInfo(request.params as { itemId: string }, request.query, undefined, auth.session.userId, auth.user.name, request);
  });
  app.post("/Items/:itemId/PlaybackInfo", async (request, reply) => {
    const auth = requireSession(request, config, store);
    return getPlaybackInfo(request.params as { itemId: string }, request.query, request.body, auth.session.userId, auth.user.name, request);
  });

  app.get("/MediaSegments/:itemId", async (request) => {
    requireSession(request, config, store);
    return queryResult([]);
  });
  app.get("/Videos/:itemId/stream", async (request, reply) => proxyProgressiveStream("Videos", request, reply));
  app.get("/Audio/:itemId/stream", async (request, reply) => proxyProgressiveStream("Audio", request, reply));
  app.get("/Videos/:itemId/stream.:container", async (request, reply) => proxyProgressiveStream("Videos", request, reply));
  app.get("/Audio/:itemId/stream.:container", async (request, reply) => proxyProgressiveStream("Audio", request, reply));
  app.get("/Videos/:itemId/:playlist.m3u8", async (request, reply) => proxyHlsPlaylist("Videos", request, reply));
  app.get("/Audio/:itemId/:playlist.m3u8", async (request, reply) => proxyHlsPlaylist("Audio", request, reply));
  app.get("/Videos/:itemId/hls/:mediaSourceId/:playlistId/:segmentId.:container", async (request, reply) => proxyHlsSegment("Videos", request, reply));
  app.get("/Audio/:itemId/hls/:mediaSourceId/:playlistId/:segmentId.:container", async (request, reply) => proxyHlsSegment("Audio", request, reply));
  app.get("/Videos/:itemId/:mediaSourceId/Subtitles/:index/Stream.:format", async (request, reply) => proxySubtitleStream(request, reply));
  app.get("/Videos/:itemId/:mediaSourceId/Subtitles/:index/:startPositionTicks/Stream.:format", async (request, reply) => proxySubtitleStream(request, reply));

  registerUnsupportedRoutes(app);

  app.all("/*", async (_request, reply) => notFound(reply));
  return app;

  async function browseItems(userIdValue: string, query: BrowseQuery): Promise<Record<string, unknown>> {
    const snapshotConfig = config;
    const client = upstream;
    const unpagedQuery = { ...query, startIndex: undefined, limit: undefined };
    const cachedTotal = queryBridgeItems(snapshotConfig, store, userIdValue, unpagedQuery).total;
    if (cachedTotal === 0) {
      await refreshLiveBrowse(snapshotConfig, client, query).catch((error: unknown) => {
        const detail = error instanceof Error ? error.message : String(error);
        throw badGatewayError(`Upstream browse failed: ${detail}`);
      });
    }
    const result = queryBridgeItems(snapshotConfig, store, userIdValue, query);
    return queryResult(result.items, query.startIndex ?? 0, result.total);
  }

  async function refreshLiveBrowse(snapshotConfig: BridgeConfig, client: AppUpstreamClient, query: BrowseQuery): Promise<void> {
    let attempted = false;
    let sawResponse = false;
    const refresh = async (serverIdValue: string, libraryIdValue: string) => {
      attempted = true;
      try {
        await refreshLiveSource(client, serverIdValue, libraryIdValue, query);
        sawResponse = true;
      } catch (error) {
        if (!isIgnorableLiveSourceError(error)) throw error;
      }
    };

    const libraries = mappedLibrariesForBrowse(snapshotConfig, query.parentId);
    for (const library of libraries) {
      for (const source of library.sources) {
        await refresh(source.server, source.libraryId);
      }
    }
    if (libraries.length === 0 && query.parentId) {
      const passThroughLibrary = store.listUpstreamLibraries().find((library) => passThroughLibraryId(library.serverId, library.libraryId) === query.parentId);
      if (passThroughLibrary) {
        await refresh(passThroughLibrary.serverId, passThroughLibrary.libraryId);
      }
    }
    if (!sawResponse && attempted) {
      throw new Error("No upstream response for /Items");
    }
  }

  function mappedLibrariesForBrowse(snapshotConfig: BridgeConfig, parentId: string | undefined): BridgeConfig["libraries"] {
    if (!parentId) return snapshotConfig.libraries;
    const library = snapshotConfig.libraries.find((candidate) => bridgeLibraryId(candidate.id) === parentId);
    return library ? [library] : [];
  }

  async function refreshLiveSource(client: AppUpstreamClient, serverIdValue: string, libraryIdValue: string, query: BrowseQuery): Promise<void> {
    let startIndex = 0;
    let totalRecordCount = Number.POSITIVE_INFINITY;
    while (startIndex < totalRecordCount) {
      const response = await client.json<{ Items?: SourceItem[]; TotalRecordCount?: number }>(serverIdValue, "/Items", {
        query: {
          ParentId: libraryIdValue,
          Recursive: true,
          StartIndex: startIndex,
          Limit: 100,
          Fields: LIVE_BROWSE_FIELDS,
          IncludeItemTypes: query.includeItemTypes,
          MediaTypes: query.mediaTypes
        }
      });
      const items = Array.isArray(response.Items) ? response.Items : [];
      totalRecordCount = Number(response.TotalRecordCount ?? items.length);
      upsertLiveItems(serverIdValue, libraryIdValue, items);
      if (items.length === 0) break;
      startIndex += items.length;
    }
  }

  async function liveLatestItems(userName: string, rawQuery: unknown, request?: FastifyRequest): Promise<Record<string, unknown>[]> {
    if (!liveRouteAggregation) throw new Error("Live route aggregation is disabled");
    const client = upstream;
    const bridgeServerIdValue = serverId;
    const cacheVersion = configVersion;
    const sources = liveLibrarySources(rawQuery);
    logUpstreamFanout(request, "/Items/Latest", "/Items/Latest", sources);
    const candidates: LiveCandidate[] = [];
    let sawResponse = false;
    await Promise.all(sources.map(async (source) => {
      try {
        const query = await liveQueryForSource(client, cacheVersion, userName, rawQuery, source);
        logUpstreamJson(request, "/Items/Latest", source, "/Items/Latest");
        const response = await client.json<unknown>(source.serverId, "/Items/Latest", { query });
        sawResponse = true;
        for (const item of liveItemsFromResponse(response)) {
          candidates.push({ source, item });
        }
      } catch (error) {
        if (!isIgnorableLiveSourceError(error)) throw error;
      }
    }));
    if (!sawResponse) throw new Error("No upstream response for /Items/Latest");

    const limit = limitFrom(rawQuery) ?? 20;
    return mergeLiveCandidates(candidates)
      .sort(compareLiveDateCreatedDescending)
      .slice(0, limit)
      .map(({ source, item }) => rewriteLiveDto(item, source, bridgeServerIdValue));
  }

  async function liveQueryResult(userName: string, path: string, rawQuery: unknown, request?: FastifyRequest): Promise<Record<string, unknown>> {
    if (!liveRouteAggregation) throw new Error("Live route aggregation is disabled");
    const client = upstream;
    const bridgeServerIdValue = serverId;
    const cacheVersion = configVersion;
    const sources = path === "/UserItems/Resume" || path === "/Shows/NextUp" ? liveQuerySources(rawQuery) : liveUpstreamSources();
    logUpstreamFanout(request, path, path, sources);
    const candidates: LiveCandidate[] = [];
    let sawResponse = false;

    await Promise.all(sources.map(async (source) => {
      const query = aggregateLivePageQuery(rawQuery, await liveQueryForSource(client, cacheVersion, userName, rawQuery, source));
      try {
        logUpstreamJson(request, path, source, path);
        const response = await client.json<unknown>(source.serverId, path, { query });
        sawResponse = true;
        for (const item of liveItemsFromResponse(response)) {
          candidates.push({ source, item });
        }
      } catch (error) {
        if (!isIgnorableLiveSourceError(error)) throw error;
      }
    }));

    if (!sawResponse) throw new Error(`No upstream response for ${path}`);
    const startIndex = startIndexFrom(rawQuery) ?? 0;
    const limit = limitFrom(rawQuery);
    const merged = mergeLiveCandidates(candidates);
    const paged = limit === undefined ? merged.slice(startIndex) : merged.slice(startIndex, startIndex + limit);
    return queryResult(paged.map(({ source, item }) => rewriteLiveDto(item, source, bridgeServerIdValue)), startIndex, merged.length);
  }

  async function getLiveItem(userName: string, itemIdValue: string, rawQuery: unknown, request?: FastifyRequest): Promise<Record<string, unknown> | undefined> {
    if (!liveRouteAggregation) return undefined;
    const client = upstream;
    const bridgeServerIdValue = serverId;
    const cacheVersion = configVersion;
    const sources = liveUpstreamSources();
    logUpstreamFanout(request, `/Items/${itemIdValue}`, `/Items/${itemIdValue}`, sources);
    for (const source of sources) {
      const query = await liveQueryForSource(client, cacheVersion, userName, rawQuery, source);
      try {
        logUpstreamJson(request, `/Items/${itemIdValue}`, source, `/Items/${itemIdValue}`);
        const item = await client.json<Record<string, unknown>>(source.serverId, `/Items/${itemIdValue}`, { query });
        return rewriteLiveDto(item, source, bridgeServerIdValue);
      } catch (error) {
        if (!isIgnorableLiveSourceError(error)) throw error;
      }
    }
    return undefined;
  }

  function liveLibrarySources(rawQuery: unknown): LiveSource[] {
    const parentId = parentIdFrom(rawQuery);
    if (parentId) {
      const library = config.libraries.find((candidate) => bridgeLibraryId(candidate.id) === parentId);
      if (library) {
        return library.sources.map((source, index) => ({
          serverId: source.server,
          libraryId: source.libraryId,
          bridgeLibraryId: bridgeLibraryId(library.id),
          priority: liveSourcePriority(source.server, index)
        }));
      }

      const passThroughLibrary = store.listUpstreamLibraries()
        .find((candidate) => passThroughLibraryId(candidate.serverId, candidate.libraryId) === parentId);
      if (passThroughLibrary) {
        return [{
          serverId: passThroughLibrary.serverId,
          libraryId: passThroughLibrary.libraryId,
          bridgeLibraryId: parentId,
          priority: liveSourcePriority(passThroughLibrary.serverId, 0)
        }];
      }
    }

    const sources: LiveSource[] = [];
    for (const library of config.libraries) {
      for (let index = 0; index < library.sources.length; index += 1) {
        const source = library.sources[index];
        sources.push({
          serverId: source.server,
          libraryId: source.libraryId,
          bridgeLibraryId: bridgeLibraryId(library.id),
          priority: liveSourcePriority(source.server, index)
        });
      }
    }
    return sources.length > 0 ? sources : liveUpstreamSources();
  }

  function liveQuerySources(rawQuery: unknown): LiveSource[] {
    const parentId = parentIdFrom(rawQuery);
    return parentId ? liveLibrarySources(rawQuery) : liveUpstreamSources();
  }

  function liveUpstreamSources(): LiveSource[] {
    return config.upstreams.map((upstreamConfig, index) => ({
      serverId: upstreamConfig.id,
      priority: liveSourcePriority(upstreamConfig.id, index)
    }));
  }

  function liveSourcePriority(serverIdValue: string, sourceIndex: number): number {
    const upstreamIndex = config.upstreams.findIndex((upstreamConfig) => upstreamConfig.id === serverIdValue);
    return (upstreamIndex === -1 ? config.upstreams.length : upstreamIndex) * 1000 + sourceIndex;
  }

  function logUpstreamFanout(request: FastifyRequest | undefined, bridgeRoute: string, upstreamPath: string, sources: LiveSource[]): void {
    request?.log.info({
      bridgeRoute,
      upstreamPath,
      upstreamBindings: sources.map((source) => ({
        serverId: source.serverId,
        libraryId: source.libraryId,
        bridgeLibraryId: source.bridgeLibraryId
      }))
    }, "upstream fan-out");
  }

  function logUpstreamJson(request: FastifyRequest | undefined, bridgeRoute: string, source: LiveSource, upstreamPath: string): void {
    request?.log.info({
      bridgeRoute,
      upstreamBinding: {
        serverId: source.serverId,
        libraryId: source.libraryId,
        bridgeLibraryId: source.bridgeLibraryId,
        path: upstreamPath
      }
    }, "upstream json");
  }

  async function liveQueryForSource(
    client: AppUpstreamClient,
    cacheVersion: number,
    userName: string,
    rawQuery: unknown,
    source: LiveSource
  ): Promise<Record<string, string | number | boolean | undefined>> {
    const query = stripBridgeUserQuery(rawQuery);
    if (source.libraryId && parentIdFrom(rawQuery)) {
      query.ParentId = source.libraryId;
      delete query.parentId;
    }
    const upstreamUserId = await liveUserId(client, cacheVersion, source.serverId, userName);
    if (upstreamUserId) {
      query.UserId = upstreamUserId;
    }
    return query;
  }

  function aggregateLivePageQuery(rawQuery: unknown, query: Record<string, string | number | boolean | undefined>): Record<string, string | number | boolean | undefined> {
    const startIndex = startIndexFrom(rawQuery) ?? 0;
    const limit = limitFrom(rawQuery);
    delete query.startIndex;
    query.StartIndex = 0;
    if (limit !== undefined) {
      delete query.limit;
      query.Limit = startIndex + limit;
    }
    return query;
  }

  async function liveUserId(client: AppUpstreamClient, cacheVersion: number, serverIdValue: string, userName: string): Promise<string | undefined> {
    const key = `${cacheVersion}:${serverIdValue}:${userName.toLowerCase()}`;
    const cached = liveUserCache.get(key);
    const now = Date.now();
    if (cached && cached.expiresAt > now) {
      return cached.promise;
    }

    const promise = fetchLiveUserId(client, serverIdValue, userName).catch(() => undefined);
    liveUserCache.set(key, { expiresAt: now + liveUserCacheTtlMs, promise });
    const resolved = await promise;
    if (resolved === undefined) {
      liveUserCache.delete(key);
    }
    return resolved;
  }

  async function fetchLiveUserId(client: AppUpstreamClient, serverIdValue: string, userName: string): Promise<string | undefined> {
    try {
      const users = await client.json<Array<{ Id: string; Name?: string }>>(serverIdValue, "/Users", {});
      return users.find((user) => user.Name?.toLowerCase() === userName.toLowerCase())?.Id ?? users[0]?.Id;
    } catch {
      return undefined;
    }
  }

  function liveItemsFromResponse(response: unknown): Record<string, unknown>[] {
    if (Array.isArray(response)) return response.filter(isRecord);
    if (isRecord(response) && Array.isArray(response.Items)) return response.Items.filter(isRecord);
    return [];
  }

  function mergeLiveCandidates(candidates: LiveCandidate[]): LiveCandidate[] {
    const byLogicalKey = new Map<string, LiveCandidate>();
    for (const candidate of candidates) {
      const key = logicalItemKey(candidate.item as unknown as SourceItem, candidate.source.serverId);
      const existing = byLogicalKey.get(key);
      if (!existing || candidate.source.priority < existing.source.priority) {
        byLogicalKey.set(key, candidate);
      }
    }
    return Array.from(byLogicalKey.values()).sort((left, right) => left.source.priority - right.source.priority);
  }

  function rewriteLiveDto(item: Record<string, unknown>, source: LiveSource, bridgeServerIdValue: string = serverId): Record<string, unknown> {
    const rewritten = rewriteLiveValue(item, source, bridgeServerIdValue);
    return isRecord(rewritten) ? rewritten : item;
  }

  function rewriteLiveValue(value: unknown, source: LiveSource, bridgeServerIdValue: string): unknown {
    if (Array.isArray(value)) return value.map((item) => rewriteLiveValue(item, source, bridgeServerIdValue));
    if (!isRecord(value)) return value;

    const object: Record<string, unknown> = { ...value };
    if ("ServerId" in object) object.ServerId = bridgeServerIdValue;
    for (const field of ["ParentId", "TopParentId"]) {
      if (source.libraryId && source.bridgeLibraryId && object[field] === source.libraryId) {
        object[field] = source.bridgeLibraryId;
      }
    }
    if (isRecord(object.UserData) && typeof object.Id === "string") {
      object.UserData = { ...object.UserData, Key: object.Id, ItemId: object.Id };
    }
    for (const [key, child] of Object.entries(object)) {
      if (key === "UserData") continue;
      if (child && typeof child === "object") {
        object[key] = rewriteLiveValue(child, source, bridgeServerIdValue);
      }
    }
    return object;
  }

  function compareLiveDateCreatedDescending(left: LiveCandidate, right: LiveCandidate): number {
    const result = String(right.item.DateCreated ?? "").localeCompare(String(left.item.DateCreated ?? ""));
    return result === 0 ? left.source.priority - right.source.priority : result;
  }

  async function refreshLiveSeasons(client: AppUpstreamClient, seriesSources: ReturnType<typeof bridgeItemSources>, query: unknown): Promise<void> {
    let sawResponse = false;
    for (const source of seriesSources) {
      try {
        const response = await client.json<{ Items?: SourceItem[] }>(source.serverId, `/Shows/${source.itemId}/Seasons`, {
          query: stripBridgeUserQuery(query)
        });
        sawResponse = true;
        upsertLiveItems(source.serverId, source.libraryId, response.Items ?? []);
      } catch (error) {
        if (!isIgnorableLiveSourceError(error)) throw error;
      }
    }
    if (!sawResponse && seriesSources.length > 0) throw new Error("No upstream response for seasons");
  }

  async function refreshLiveEpisodes(
    client: AppUpstreamClient,
    seriesSources: ReturnType<typeof bridgeItemSources>,
    seasonSources: ReturnType<typeof bridgeItemSources>,
    query: unknown
  ): Promise<void> {
    let attempted = false;
    let sawResponse = false;
    if (seasonSources.length > 0) {
      for (const seasonSource of seasonSources) {
        const upstreamSeriesId = typeof seasonSource.json.SeriesId === "string" ? seasonSource.json.SeriesId : undefined;
        if (!upstreamSeriesId) continue;
        attempted = true;
        try {
          const response = await client.json<{ Items?: SourceItem[] }>(seasonSource.serverId, `/Shows/${upstreamSeriesId}/Episodes`, {
            query: { ...stripBridgeUserQuery(query), SeasonId: seasonSource.itemId }
          });
          sawResponse = true;
          upsertLiveItems(seasonSource.serverId, seasonSource.libraryId, response.Items ?? []);
        } catch (error) {
          if (!isIgnorableLiveSourceError(error)) throw error;
        }
      }
      if (!sawResponse && attempted) throw new Error("No upstream response for episodes");
      return;
    }

    for (const source of seriesSources) {
      attempted = true;
      try {
        const response = await client.json<{ Items?: SourceItem[] }>(source.serverId, `/Shows/${source.itemId}/Episodes`, {
          query: stripBridgeUserQuery(query)
        });
        sawResponse = true;
        upsertLiveItems(source.serverId, source.libraryId, response.Items ?? []);
      } catch (error) {
        if (!isIgnorableLiveSourceError(error)) throw error;
      }
    }
    if (!sawResponse && attempted) throw new Error("No upstream response for episodes");
  }

  function upsertLiveItems(serverIdValue: string, libraryIdValue: string, items: SourceItem[]): void {
    for (const item of items) {
      store.upsertIndexedItem({
        serverId: serverIdValue,
        itemId: item.Id,
        libraryId: libraryIdValue,
        itemType: item.Type ?? "Unknown",
        logicalKey: logicalItemKey(item, serverIdValue),
        json: item as unknown as Record<string, unknown>
      });
    }
  }

  async function refreshLiveViewsForRequest(snapshotConfig: BridgeConfig, client: AppUpstreamClient, query: unknown): Promise<void> {
    if (!shouldRefreshLiveViews(query)) return;
    const upstreamConfigs = snapshotConfig.upstreams;
    for (const upstreamConfig of upstreamConfigs) {
      try {
        const users = await client.json<Array<{ Id: string }>>(upstreamConfig.id, "/Users", {});
        const user = users[0];
        if (!user) continue;
        const response = await client.json<{ Items?: Array<{ Id: string; Name?: string; CollectionType?: string }> }>(upstreamConfig.id, "/UserViews", {
          query: { UserId: user.Id }
        });
        for (const item of response.Items ?? []) {
          store.upsertUpstreamLibrary({
            serverId: upstreamConfig.id,
            libraryId: item.Id,
            name: item.Name ?? item.Id,
            collectionType: item.CollectionType ?? null
          });
        }
      } catch (error) {
        if (!isIgnorableLiveSourceError(error)) throw error;
      }
    }
  }

  function shouldRefreshLiveViews(query: unknown): boolean {
    const record = query && typeof query === "object" ? query as Record<string, unknown> : {};
    return "includeExternalContent" in record || "IncludeExternalContent" in record || "userId" in record || "UserId" in record;
  }

  async function getHydratedBridgeItem(
    itemIdValue: string,
    userIdValue: string,
    query: unknown,
    snapshotConfig: BridgeConfig = config,
    client: AppUpstreamClient = upstream,
    bridgeServerIdValue: string = serverId
  ): Promise<Record<string, unknown> | undefined> {
    const sources = bridgeItemSources(snapshotConfig, store, itemIdValue);
    const source = sources[0];
    if (!source) return undefined;

    const fallback = getBridgeItem(snapshotConfig, store, userIdValue, itemIdValue);
    if (!shouldHydrateItem(query)) return fallback;
    const upstreamItem = await client.json<Record<string, unknown>>(source.serverId, `/Items/${source.itemId}`, {
      query: stripBridgeUserQuery(query)
    }).catch(() => undefined);
    if (!upstreamItem) return fallback;

    const rewritten = rewriteDto(
      {
        ...upstreamItem,
        Id: source.itemId,
        ServerId: source.serverId
      },
      {
        serverId: source.serverId,
        bridgeServerId: bridgeServerIdValue,
        itemIdMap: new Map(sources.map((candidate) => [candidate.itemId, itemIdValue]))
      }
    ) as Record<string, unknown>;
    rewritten.Id = itemIdValue;
    if (typeof source.json.SeriesId === "string") {
      rewritten.SeriesIdSource = source.json.SeriesId;
    }
    if (typeof source.json.SeasonId === "string") {
      rewritten.SeasonIdSource = source.json.SeasonId;
    }
    rewritten.UserData = fallback?.UserData ?? {
      ...store.getUserData(userIdValue, itemIdValue),
      ItemId: itemIdValue
    };
    recordMediaSourceMappings(itemIdValue, source.serverId, source.itemId, upstreamItem.MediaSources, rewritten.MediaSources);
    return rewritten;
  }

  async function getPlaybackInfo(params: { itemId: string }, query: unknown, body: unknown, userIdValue: string, userName: string, request?: FastifyRequest): Promise<Record<string, unknown>> {
    const snapshotConfig = config;
    const client = upstream;
    const bridgeServerIdValue = serverId;
    const sources = bridgeItemSources(snapshotConfig, store, params.itemId);
    if (sources.length === 0) {
      const live = await getLivePlaybackInfo(userName, params.itemId, query, body, request).catch(() => undefined);
      if (live) return live;
    }
    const requestedMediaSourceId = mediaSourceIdFrom(query) ?? mediaSourceIdFrom(body);
    const mediaSourceMapping = requestedMediaSourceId ? store.findMediaSourceMapping(requestedMediaSourceId) : undefined;
    const source = mediaSourceMapping
      ? sources.find((candidate) => candidate.serverId === mediaSourceMapping.serverId && candidate.itemId === mediaSourceMapping.upstreamItemId)
      : sources[0];
    if (!source) {
      throw Object.assign(new Error("Item not found"), { statusCode: 404 });
    }
    const upstreamQuery = mediaSourceMapping ? rewriteMediaSourceId(query, mediaSourceMapping.upstreamMediaSourceId) : query;
    const upstreamBody = mediaSourceMapping ? rewriteMediaSourceId(body, mediaSourceMapping.upstreamMediaSourceId) : body;
    request?.log.info({
      bridgeRoute: `/Items/${params.itemId}/PlaybackInfo`,
      upstreamBinding: {
        serverId: source.serverId,
        itemId: source.itemId,
        path: `/Items/${source.itemId}/PlaybackInfo`
      }
    }, "upstream binding");
    const response = await client.json<Record<string, unknown>>(source.serverId, `/Items/${source.itemId}/PlaybackInfo`, {
      method: upstreamBody === undefined ? "GET" : "POST",
      query: upstreamQuery,
      body: upstreamBody
    }).catch((error: unknown) => {
      const detail = error instanceof Error ? error.message : String(error);
      throw badGatewayError(`Upstream PlaybackInfo failed: ${detail}`);
    });
    const rewritten = rewriteDto(response, {
      serverId: source.serverId,
      bridgeServerId: bridgeServerIdValue,
      itemIdMap: new Map(sources.map((candidate) => [candidate.itemId, params.itemId]))
    }) as Record<string, unknown>;

    const originalSources = Array.isArray(response.MediaSources) ? response.MediaSources : [];
    const rewrittenSources = Array.isArray(rewritten.MediaSources) ? rewritten.MediaSources : [];
    recordMediaSourceMappings(params.itemId, source.serverId, source.itemId, originalSources, rewrittenSources);
    if (typeof response.PlaySessionId === "string") {
      const mapping = store.createPlaybackSessionMapping({
        serverId: source.serverId,
        upstreamPlaySessionId: response.PlaySessionId,
        upstreamItemId: source.itemId,
        bridgeItemId: params.itemId
      });
      rewritten.PlaySessionId = mapping.bridgePlaySessionId;
    }
    const bridgeItem = getBridgeItem(snapshotConfig, store, userIdValue, params.itemId);
    if (bridgeItem) {
      rewritten.UserData = bridgeItem.UserData;
    }
    return rewritten;
  }

  async function getLivePlaybackInfo(userName: string, itemIdValue: string, query: unknown, body: unknown, request?: FastifyRequest): Promise<Record<string, unknown> | undefined> {
    if (!liveRouteAggregation) return undefined;
    const client = upstream;
    const bridgeServerIdValue = serverId;
    const cacheVersion = configVersion;
    const sources = liveUpstreamSources();
    logUpstreamFanout(request, `/Items/${itemIdValue}/PlaybackInfo`, `/Items/${itemIdValue}/PlaybackInfo`, sources);
    for (const source of sources) {
      const upstreamQuery = await liveQueryForSource(client, cacheVersion, userName, query, source);
      const upstreamBody = await liveBodyForSource(client, cacheVersion, userName, body, source);
      try {
        logUpstreamJson(request, `/Items/${itemIdValue}/PlaybackInfo`, source, `/Items/${itemIdValue}/PlaybackInfo`);
        const response = await client.json<Record<string, unknown>>(source.serverId, `/Items/${itemIdValue}/PlaybackInfo`, {
          method: upstreamBody === undefined ? "GET" : "POST",
          query: upstreamQuery,
          body: upstreamBody
        });
        return rewriteLiveDto(response, source, bridgeServerIdValue);
      } catch (error) {
        if (!isIgnorableLiveSourceError(error)) throw error;
      }
    }
    return undefined;
  }

  async function liveBodyForSource(client: AppUpstreamClient, cacheVersion: number, userName: string, body: unknown, source: LiveSource): Promise<unknown> {
    if (!isRecord(body)) return body;
    const rewritten = { ...body };
    const upstreamUserId = await liveUserId(client, cacheVersion, source.serverId, userName);
    if (upstreamUserId) {
      rewritten.UserId = upstreamUserId;
    }
    return rewritten;
  }

  function mediaSourceIdFrom(value: unknown): string | undefined {
    const record = value && typeof value === "object" ? value as Record<string, unknown> : {};
    return typeof record.MediaSourceId === "string"
      ? record.MediaSourceId
      : typeof record.mediaSourceId === "string" ? record.mediaSourceId : undefined;
  }

  function rewriteMediaSourceId(value: unknown, mediaSourceId: string): unknown {
    if (!value || typeof value !== "object") return value;
    return {
      ...(value as Record<string, unknown>),
      MediaSourceId: mediaSourceId
    };
  }

  function recordMediaSourceMappings(
    bridgeItemIdValue: string,
    serverIdValue: string,
    upstreamItemId: string,
    originalSources: unknown,
    rewrittenSources: unknown
  ): void {
    const originals = Array.isArray(originalSources) ? originalSources : [];
    const rewritten = Array.isArray(rewrittenSources) ? rewrittenSources : [];
    for (let index = 0; index < originals.length; index += 1) {
      const original = originals[index] as Record<string, unknown>;
      const rewrittenSource = rewritten[index] as Record<string, unknown> | undefined;
      if (rewrittenSource && typeof rewrittenSource.ItemId !== "string") {
        rewrittenSource.ItemId = bridgeItemIdValue;
      }
      if (typeof original.Id === "string" && typeof rewrittenSource?.Id === "string") {
        store.upsertMediaSourceMapping({
          bridgeMediaSourceId: rewrittenSource.Id,
          serverId: serverIdValue,
          upstreamItemId,
          upstreamMediaSourceId: original.Id
        });
        store.upsertMediaSourceMapping({
          bridgeMediaSourceId: bridgeMediaSourceId(serverIdValue, bridgeItemIdValue, original.Id),
          serverId: serverIdValue,
          upstreamItemId,
          upstreamMediaSourceId: original.Id
        });
      }
    }
  }

  function resolveMediaSourceMapping(mediaSourceId: string, bridgeItemIdValue?: string) {
    const existing = store.findMediaSourceMapping(mediaSourceId);
    if (existing || !bridgeItemIdValue) return existing;

    for (const source of bridgeItemSources(config, store, bridgeItemIdValue)) {
      const mediaSources = Array.isArray(source.json.MediaSources) ? source.json.MediaSources : [];
      for (const mediaSource of mediaSources) {
        const original = mediaSource as Record<string, unknown>;
        if (typeof original.Id !== "string") continue;
        const rewrittenId = bridgeMediaSourceId(source.serverId, bridgeItemIdValue, original.Id);
        if (rewrittenId !== mediaSourceId) continue;
        const mapping = {
          bridgeMediaSourceId: rewrittenId,
          serverId: source.serverId,
          upstreamItemId: source.itemId,
          upstreamMediaSourceId: original.Id
        };
        store.upsertMediaSourceMapping(mapping);
        return mapping;
      }
    }
    return undefined;
  }

  function viewDtos(snapshotConfig: BridgeConfig = config, snapshotServerId: string = serverId): Record<string, unknown>[] {
    const mapped = new Set(snapshotConfig.libraries.flatMap((library) => library.sources.map((source) => `${source.server}:${source.libraryId}`)));
    const upstreamNames = new Map(snapshotConfig.upstreams.map((upstreamConfig) => [upstreamConfig.id, upstreamConfig.name]));
    const merged = snapshotConfig.libraries.map((library) => libraryDto(library, snapshotServerId));
    const passThrough = store.listUpstreamLibraries()
      .filter((library) => !mapped.has(`${library.serverId}:${library.libraryId}`))
      .map((library) => passThroughLibraryDto(library, upstreamNames.get(library.serverId) ?? library.serverId, snapshotServerId));
    return [...merged, ...passThrough];
  }

  function groupingOptions(): Record<string, unknown>[] {
    return viewDtos()
      .map((view) => ({ Name: view.Name, Id: view.Id }))
      .filter((view): view is { Name: string; Id: string } => typeof view.Name === "string" && typeof view.Id === "string")
      .sort((a, b) => a.Name.localeCompare(b.Name));
  }

  function virtualFolders(): Record<string, unknown>[] {
    return viewDtos()
      .map((view) => ({
        Name: view.Name,
        Locations: [],
        CollectionType: view.CollectionType,
        LibraryOptions: { Enabled: true },
        ItemId: view.Id,
        PrimaryImageItemId: null,
        RefreshProgress: null,
        RefreshStatus: "Idle"
      }))
      .filter((folder) => typeof folder.Name === "string" && typeof folder.ItemId === "string");
  }

  function displayPreferencesDto(id: string, client: string): Record<string, unknown> {
    return {
      Id: id,
      ViewType: null,
      SortBy: null,
      IndexBy: null,
      RememberIndexing: false,
      PrimaryImageHeight: 250,
      PrimaryImageWidth: 250,
      CustomPrefs: {},
      ScrollDirection: "Horizontal",
      ShowBackdrop: true,
      RememberSorting: false,
      SortOrder: "Ascending",
      ShowSidebar: false,
      Client: client
    };
  }

  function latestItems(snapshotConfig: BridgeConfig, userIdValue: string, rawQuery: unknown): Record<string, unknown>[] {
    const query = browseQuery(rawQuery);
    const limit = ((rawQuery ?? {}) as { Limit?: string; limit?: string }).Limit ?? ((rawQuery ?? {}) as { Limit?: string; limit?: string }).limit;
    return listBridgeItems(snapshotConfig, store, userIdValue, {
      ...query,
      sortBy: "DateCreated",
      sortOrder: "Descending",
      limit: limit === undefined ? 20 : Number(limit)
    });
  }

  function resumeItems(snapshotConfig: BridgeConfig, userIdValue: string, rawQuery: unknown): Record<string, unknown> {
    const query = browseQuery(rawQuery);
    const unpaged = listBridgeItems(snapshotConfig, store, userIdValue, { ...query, startIndex: undefined, limit: undefined })
      .filter((item) => Number((item.UserData as Record<string, unknown> | undefined)?.PlaybackPositionTicks ?? 0) > 0);
    const start = query.startIndex ?? 0;
    const items = unpaged.slice(start, query.limit === undefined ? undefined : start + query.limit);
    const total = unpaged.length;
    return queryResult(items, query.startIndex ?? 0, total);
  }

  function suggestedItems(userIdValue: string, rawQuery: unknown): Record<string, unknown> {
    const raw = (rawQuery ?? {}) as Record<string, string | undefined>;
    const query = browseQuery({
      ...raw,
      IncludeItemTypes: raw.Type ?? raw.type ?? raw.IncludeItemTypes ?? raw.includeItemTypes
    });
    const result = queryBridgeItems(config, store, userIdValue, query);
    return queryResult(result.items, query.startIndex ?? 0, result.total);
  }

  function setFavorite(userIdValue: string, itemId: string, isFavorite: boolean): Record<string, unknown> {
    store.upsertUserData(userIdValue, itemId, { isFavorite });
    return userDataDto(store, userIdValue, itemId);
  }

  function setPlayed(userIdValue: string, itemId: string, played: boolean): Record<string, unknown> {
    store.upsertUserData(userIdValue, itemId, { played, playbackPositionTicks: 0 });
    return userDataDto(store, userIdValue, itemId);
  }

  function deleteSourceForItem(itemIdValue: string): IndexedItemRecord | undefined {
    const bridgeSources = bridgeItemSources(config, store, itemIdValue);
    if (bridgeSources.length > 0) return bridgeSources[0];
    return sortIndexedSourcesByPriority(store.findIndexedItemsBySourceId(itemIdValue))[0];
  }

  function sortIndexedSourcesByPriority(sources: IndexedItemRecord[]): IndexedItemRecord[] {
    return [...sources].sort((left, right) => {
      const priorityDelta = indexedSourcePriority(left) - indexedSourcePriority(right);
      if (priorityDelta !== 0) return priorityDelta;
      const sourceDelta = left.serverId.localeCompare(right.serverId) || left.libraryId.localeCompare(right.libraryId);
      return sourceDelta || left.itemId.localeCompare(right.itemId);
    });
  }

  function indexedSourcePriority(source: IndexedItemRecord): number {
    let priority = 0;
    for (const library of config.libraries) {
      for (const configuredSource of library.sources) {
        if (configuredSource.server === source.serverId && configuredSource.libraryId === source.libraryId) {
          return priority;
        }
        priority += 1;
      }
    }
    return Number.MAX_SAFE_INTEGER;
  }

  async function proxyProgressiveStream(kind: "Videos" | "Audio", request: FastifyRequest, reply: FastifyReply): Promise<void> {
    requireSession(request, config, store);
    const client = upstream;
    if (!client.raw) {
      unsupported(reply, "The configured upstream client does not support raw proxying");
      return;
    }
    const params = request.params as { itemId: string; container?: string };
    const query = request.query as Record<string, string | undefined>;
    const mediaSourceId = query.MediaSourceId ?? query.mediaSourceId;
    if (!mediaSourceId) {
      notFound(reply, "MediaSourceId is required");
      return;
    }
    const mapping = resolveMediaSourceMapping(mediaSourceId, params.itemId);
    if (!mapping && liveRouteAggregation) {
      const streamPath = params.container === undefined
        ? `/${kind}/${params.itemId}/stream`
        : `/${kind}/${params.itemId}/stream.${params.container}`;
      const response = await proxyRawFirst(client, request, `${kind.toLowerCase()} progressive stream`, liveUpstreamSources(), streamPath, {
        method: request.method,
        query,
        headers: copyProxyRequestHeaders(request.headers)
      });
      await sendProxyBody(reply, response);
      return;
    }
    if (!mapping) {
      notFound(reply, "Media source mapping not found");
      return;
    }

    const upstreamQuery = { ...query, MediaSourceId: mapping.upstreamMediaSourceId };
    const streamPath = params.container === undefined
      ? `/${kind}/${mapping.upstreamItemId}/stream`
      : `/${kind}/${mapping.upstreamItemId}/stream.${params.container}`;
    const response = await proxyRaw(client, request, mapping.serverId, streamPath, {
      method: request.method,
      query: upstreamQuery,
      headers: copyProxyRequestHeaders(request.headers)
    });

    await sendProxyBody(reply, response);
  }

  async function proxyHlsPlaylist(kind: "Videos" | "Audio", request: FastifyRequest, reply: FastifyReply): Promise<void> {
    requireSession(request, config, store);
    const client = upstream;
    if (!client.raw) {
      unsupported(reply, "The configured upstream client does not support raw proxying");
      return;
    }
    const params = request.params as { itemId: string; playlist: string };
    const query = request.query as Record<string, string | undefined>;
    const mediaSourceId = query.MediaSourceId ?? query.mediaSourceId;
    if (!mediaSourceId) {
      notFound(reply, "MediaSourceId is required");
      return;
    }
    const mapping = resolveMediaSourceMapping(mediaSourceId, params.itemId);
    if (!mapping && liveRouteAggregation) {
      const response = await proxyRawFirst(client, request, `${kind.toLowerCase()} hls playlist`, liveUpstreamSources(), `/${kind}/${params.itemId}/${params.playlist}.m3u8`, {
        method: request.method,
        query,
        headers: copyProxyRequestHeaders(request.headers)
      });
      const body = await responseBodyText(response.body);
      const rewritten = rewriteHlsPlaylist(body, {
        bridgeBasePath: `/${kind}/${params.itemId}/hls/${mediaSourceId}`,
        upstreamBasePath: `/${kind}/${params.itemId}/hls`
      });

      reply.code(response.statusCode);
      copyProxyResponseHeaders(reply, response.headers);
      reply.header("content-length", Buffer.byteLength(rewritten).toString());
      reply.send(rewritten);
      return;
    }
    if (!mapping) {
      notFound(reply, "Media source mapping not found");
      return;
    }

    const response = await proxyRaw(client, request, mapping.serverId, `/${kind}/${mapping.upstreamItemId}/${params.playlist}.m3u8`, {
      method: request.method,
      query: { ...query, MediaSourceId: mapping.upstreamMediaSourceId },
      headers: copyProxyRequestHeaders(request.headers)
    });
    const body = await responseBodyText(response.body);
    const rewritten = rewriteHlsPlaylist(body, {
      bridgeBasePath: `/${kind}/${params.itemId}/hls/${mediaSourceId}`,
      upstreamBasePath: `/${kind}/${mapping.upstreamItemId}/hls`
    });

    reply.code(response.statusCode);
    copyProxyResponseHeaders(reply, response.headers);
    reply.header("content-length", Buffer.byteLength(rewritten).toString());
    reply.send(rewritten);
  }

  async function proxyItemImage(request: FastifyRequest, reply: FastifyReply): Promise<void> {
    const client = upstream;
    if (!client.raw) {
      unsupported(reply, "The configured upstream client does not support raw proxying");
      return;
    }
    const params = request.params as { itemId: string; imageType: string; imageIndex?: string };
    const directImagePath = itemImagePath(params.itemId, params.imageType, params.imageIndex);
    if (!directImagePath) {
      notFound(reply, "Image not found");
      return;
    }
    const sources = bridgeItemSources(config, store, params.itemId);
    if (sources.length > 0) {
      const candidates = sources
        .map((source) => {
          const path = itemImagePath(source.itemId, params.imageType, params.imageIndex);
          return path ? { serverId: source.serverId, path } : undefined;
        })
        .filter((candidate): candidate is RawProxyCandidate => candidate !== undefined);
      const response = await tryProxyRawCandidates(client, request, "indexed item image", candidates, {
        method: request.method,
        query: request.query,
        headers: copyProxyRequestHeaders(request.headers)
      });
      if (response) {
        await sendBufferedProxyBody(reply, response);
        return;
      }
      notFound(reply, "Image not found");
      return;
    }
    if (liveRouteAggregation) {
      const directResponse = await tryProxyRawCandidates(client, request, "live item image", liveUpstreamSources().map((source) => ({
        serverId: source.serverId,
        path: directImagePath
      })), {
        method: request.method,
        query: request.query,
        headers: copyProxyRequestHeaders(request.headers)
      });
      if (directResponse) {
        await sendBufferedProxyBody(reply, directResponse);
        return;
      }
    }
    notFound(reply, "Item not found");
  }

  async function proxyHlsSegment(kind: "Videos" | "Audio", request: FastifyRequest, reply: FastifyReply): Promise<void> {
    requireSession(request, config, store);
    const client = upstream;
    if (!client.raw) {
      unsupported(reply, "The configured upstream client does not support raw proxying");
      return;
    }
    const params = request.params as { itemId: string; mediaSourceId: string; playlistId: string; segmentId: string; container: string };
    const mapping = resolveMediaSourceMapping(params.mediaSourceId, params.itemId);
    if (!mapping && liveRouteAggregation) {
      const response = await proxyRawFirst(client, request, `${kind.toLowerCase()} hls segment`, liveUpstreamSources(), `/${kind}/${params.itemId}/hls/${params.playlistId}/${params.segmentId}.${params.container}`, {
        method: request.method,
        query: request.query,
        headers: copyProxyRequestHeaders(request.headers)
      });
      await sendProxyBody(reply, response);
      return;
    }
    if (!mapping) {
      notFound(reply, "Media source mapping not found");
      return;
    }
    const response = await proxyRaw(client, request, mapping.serverId, `/${kind}/${mapping.upstreamItemId}/hls/${params.playlistId}/${params.segmentId}.${params.container}`, {
      method: request.method,
      query: request.query,
      headers: copyProxyRequestHeaders(request.headers)
    });
    await sendProxyBody(reply, response);
  }

  async function proxySubtitleStream(request: FastifyRequest, reply: FastifyReply): Promise<void> {
    requireSession(request, config, store);
    const client = upstream;
    if (!client.raw) {
      unsupported(reply, "The configured upstream client does not support raw proxying");
      return;
    }
    const params = request.params as { itemId: string; mediaSourceId: string; index: string; startPositionTicks?: string; format: string };
    const mapping = resolveMediaSourceMapping(params.mediaSourceId, params.itemId);
    if (!mapping && liveRouteAggregation) {
      const subtitlePath = params.startPositionTicks === undefined
        ? `/Videos/${params.itemId}/${params.mediaSourceId}/Subtitles/${params.index}/Stream.${params.format}`
        : `/Videos/${params.itemId}/${params.mediaSourceId}/Subtitles/${params.index}/${params.startPositionTicks}/Stream.${params.format}`;
      const response = await proxyRawFirst(client, request, "subtitle stream", liveUpstreamSources(), subtitlePath, {
        method: request.method,
        query: request.query,
        headers: copyProxyRequestHeaders(request.headers)
      });
      await sendProxyBody(reply, response);
      return;
    }
    if (!mapping) {
      notFound(reply, "Media source mapping not found");
      return;
    }
    const subtitlePath = params.startPositionTicks === undefined
      ? `/Videos/${mapping.upstreamItemId}/${mapping.upstreamMediaSourceId}/Subtitles/${params.index}/Stream.${params.format}`
      : `/Videos/${mapping.upstreamItemId}/${mapping.upstreamMediaSourceId}/Subtitles/${params.index}/${params.startPositionTicks}/Stream.${params.format}`;
    const response = await proxyRaw(client, request, mapping.serverId, subtitlePath, {
      method: request.method,
      query: request.query,
      headers: copyProxyRequestHeaders(request.headers)
    });
    await sendProxyBody(reply, response);
  }

  async function proxyRaw(client: AppUpstreamClient, request: FastifyRequest, serverIdValue: string, path: string, init: unknown): Promise<{ statusCode: number; headers: Record<string, string | string[] | undefined>; body: unknown }> {
    try {
      request.log.info({ proxyPurpose: "mapped proxy", upstreamBinding: { serverId: serverIdValue, path } }, "upstream proxy");
      return await client.raw!(serverIdValue, path, init);
    } catch (error) {
      const detail = error instanceof Error ? error.message : String(error);
      throw badGatewayError(`Upstream proxy failed: ${detail}`);
    }
  }

  async function proxyRawFirst(client: AppUpstreamClient, request: FastifyRequest, purpose: string, sources: LiveSource[], path: string, init: unknown): Promise<{ statusCode: number; headers: Record<string, string | string[] | undefined>; body: unknown }> {
    const response = await tryProxyRawCandidates(client, request, purpose, sources.map((source) => ({
      serverId: source.serverId,
      path
    })), init);
    if (response) return response;
    throw badGatewayError(`Upstream proxy failed: no upstream served ${path}`);
  }

  async function tryProxyRawCandidates(
    client: AppUpstreamClient,
    request: FastifyRequest,
    purpose: string,
    candidates: RawProxyCandidate[],
    init: unknown
  ): Promise<{ statusCode: number; headers: Record<string, string | string[] | undefined>; body: unknown } | undefined> {
    for (const candidate of candidates) {
      try {
        request.log.info({
          proxyPurpose: purpose,
          upstreamBinding: {
            serverId: candidate.serverId,
            path: candidate.path
          }
        }, "upstream proxy");
        return await client.raw!(candidate.serverId, candidate.path, init);
      } catch (error) {
        if (!isIgnorableLiveSourceError(error)) {
          const detail = error instanceof Error ? error.message : String(error);
          throw badGatewayError(`Upstream proxy failed: ${detail}`);
        }
      }
    }
    return undefined;
  }

  async function sendBufferedProxyBody(
    reply: FastifyReply,
    response: { statusCode: number; headers: Record<string, string | string[] | undefined>; body: unknown }
  ): Promise<void> {
    const body = await responseBodyBuffer(response.body);
    reply.code(response.statusCode);
    copyProxyResponseHeaders(reply, response.headers);
    reply.header("content-length", body.length.toString());
    reply.send(body);
  }

  async function sendProxyBody(reply: FastifyReply, response: { statusCode: number; headers: Record<string, string | string[] | undefined>; body: unknown }): Promise<void> {
    if (isAsyncBody(response.body)) {
      reply.hijack();
      reply.raw.statusCode = response.statusCode;
      copyProxyResponseHeaders({ header: (name, value) => reply.raw.setHeader(name, value) }, response.headers);
      await forwardProxyBody(reply, response.body);
      return;
    }

    reply.code(response.statusCode);
    copyProxyResponseHeaders(reply, response.headers);
    reply.send(response.body);
  }

  function isProxyStreamClose(error: unknown): boolean {
    if (!error || typeof error !== "object" || !("code" in error)) return false;
    const code = (error as { code?: unknown }).code;
    return code === "ERR_STREAM_PREMATURE_CLOSE" || code === "ECONNRESET" || code === "UND_ERR_SOCKET";
  }

  async function forwardProxyBody(reply: FastifyReply, body: AsyncIterable<unknown>): Promise<void> {
    try {
      for await (const chunk of body) {
        if (reply.raw.destroyed || reply.raw.writableEnded) return;
        if (!reply.raw.write(chunk as string | Uint8Array)) {
          await waitForProxyDrain(reply);
        }
      }
      if (!reply.raw.destroyed && !reply.raw.writableEnded) {
        reply.raw.end();
      }
    } catch (error) {
      if (isProxyStreamClose(error)) {
        if (!reply.raw.destroyed && !reply.raw.writableEnded) {
          reply.raw.end();
        }
        return;
      }
      if (!reply.raw.destroyed) {
        reply.raw.destroy(error instanceof Error ? error : new Error(String(error)));
      }
      throw error;
    }
  }

  function waitForProxyDrain(reply: FastifyReply): Promise<void> {
    return new Promise((resolve, reject) => {
      const onDrain = () => {
        cleanup();
        resolve();
      };
      const onError = (error: Error) => {
        cleanup();
        reject(error);
      };
      const cleanup = () => {
        reply.raw.off("drain", onDrain);
        reply.raw.off("error", onError);
      };
      reply.raw.once("drain", onDrain);
      reply.raw.once("error", onError);
    });
  }

  async function forwardPlaybackReport(path: string, body: unknown, userName: string): Promise<void> {
    const client = upstream;
    const report = body && typeof body === "object" ? { ...(body as Record<string, unknown>) } : {};
    const mediaSourceId = typeof report.MediaSourceId === "string" ? report.MediaSourceId : undefined;
    if (!mediaSourceId) {
      if (liveRouteAggregation) await forwardLivePlaybackReport(path, report, userName);
      return;
    }
    const mapping = store.findMediaSourceMapping(mediaSourceId);
    if (!mapping) {
      if (liveRouteAggregation) await forwardLivePlaybackReport(path, report, userName);
      return;
    }
    report.ItemId = mapping.upstreamItemId;
    report.MediaSourceId = mapping.upstreamMediaSourceId;
    const playSessionId = typeof report.PlaySessionId === "string" ? report.PlaySessionId : undefined;
    const playbackSession = playSessionId ? store.findPlaybackSessionMapping(playSessionId) : undefined;
    if (playbackSession) {
      report.PlaySessionId = playbackSession.upstreamPlaySessionId;
    }
    try {
      await client.json(mapping.serverId, path, {
        method: "POST",
        body: report
      });
    } catch {
      // Legacy mapped sessions can outlive their upstream transcode; reports remain best-effort.
    }
  }

  async function forwardLivePlaybackReport(path: string, report: Record<string, unknown>, userName: string): Promise<void> {
    const client = upstream;
    const cacheVersion = configVersion;
    await Promise.all(liveUpstreamSources().map(async (source) => {
      const body = await liveBodyForSource(client, cacheVersion, userName, report, source);
      try {
        await client.json(source.serverId, path, {
          method: "POST",
          body
        });
      } catch {
        // The owning upstream accepts the report; non-owning upstreams can reject it.
      }
    }));
  }
}

function allowEmptyJsonDeleteBodies(app: FastifyInstance): void {
  const defaultJsonParser = app.getDefaultJsonParser("error", "error");
  app.removeContentTypeParser("application/json");
  app.addContentTypeParser<string>("application/json", { parseAs: "string" }, (request, body, done) => {
    if (request.method === "DELETE" && body.length === 0) {
      done(null, undefined);
      return;
    }

    defaultJsonParser(request, body, done);
  });
}

function toRuntimeConfigSource(config: BridgeConfig | RuntimeConfigSource): RuntimeConfigSource {
  if (isRuntimeConfigSource(config)) {
    return config;
  }
  const staticConfig = config;
  return {
    current: () => staticConfig,
    subscribe: () => () => {}
  };
}

function isRuntimeConfigSource(config: BridgeConfig | RuntimeConfigSource): config is RuntimeConfigSource {
  return typeof (config as RuntimeConfigSource).current === "function";
}

function stripEmbyBasePath(url: string): string {
  const withoutBasePath = url === "/emby" ? "/" : url.startsWith("/emby/") ? url.slice("/emby".length) : url;
  if (withoutBasePath === "/Items//" || withoutBasePath.startsWith("/Items//?")) {
    return `/Items/Root${withoutBasePath.slice("/Items//".length)}`;
  }
  return withoutBasePath;
}

function requireSelf(auth: AuthContext, requestedUserId: string | undefined): string {
  if (requestedUserId === undefined || requestedUserId.length === 0) return auth.session.userId;
  if (requestedUserId.toLowerCase() !== auth.session.userId.toLowerCase()) {
    throw Object.assign(new Error("Forbidden"), { statusCode: 403 });
  }
  return auth.session.userId;
}

function userIdFromQuery(query: unknown): string | undefined {
  const value = (query ?? {}) as Record<string, unknown>;
  const raw = value.UserId ?? value.userId;
  return typeof raw === "string" ? raw : undefined;
}

function registerUnsupportedRoutes(app: FastifyInstance): void {
  for (const prefix of [
    "Plugins",
    "Packages",
    "LiveTv",
    "SyncPlay",
    "QuickConnect",
    "Library/Refresh",
    "Repositories",
    "ScheduledTasks",
    "Backup",
    "Subtitle"
  ]) {
    app.all(`/${prefix}`, async (_request, reply) => unsupported(reply, `${prefix} is not supported by Jellyfin Bridge`));
    app.all(`/${prefix}/*`, async (_request, reply) => unsupported(reply, `${prefix} is not supported by Jellyfin Bridge`));
  }

  app.post("/Items/:itemId/Refresh", async (_request, reply) => unsupported(reply, "Metadata refresh is not supported by Jellyfin Bridge"));
  app.post("/Items/:itemId", async (_request, reply) => unsupported(reply, "Metadata editing is not supported by Jellyfin Bridge"));
}

function saveUserData(store: Store, userIdValue: string, itemId: string, body: unknown): void {
  const data = (body ?? {}) as Record<string, unknown>;
  store.upsertUserData(userIdValue, itemId, {
    played: typeof data.Played === "boolean" ? data.Played : undefined,
    isFavorite: typeof data.IsFavorite === "boolean" ? data.IsFavorite : undefined,
    playbackPositionTicks: typeof data.PlaybackPositionTicks === "number" ? data.PlaybackPositionTicks : undefined,
    playCount: typeof data.PlayCount === "number" ? data.PlayCount : undefined
  });
}

function metadataQuery(query: unknown): { parentId?: string; searchTerm?: string; startIndex?: number; limit?: number; personTypes?: string } {
  const value = (query ?? {}) as Record<string, string | undefined>;
  return {
    parentId: value.ParentId ?? value.parentId,
    searchTerm: value.SearchTerm ?? value.searchTerm,
    startIndex: numberQuery(value.StartIndex ?? value.startIndex),
    limit: numberQuery(value.Limit ?? value.limit),
    personTypes: value.PersonTypes ?? value.personTypes
  };
}

function browseQuery(query: unknown): BrowseQuery {
  const value = (query ?? {}) as Record<string, string | undefined>;
  return {
    includeItemTypes: value.IncludeItemTypes ?? value.includeItemTypes,
    mediaTypes: value.MediaTypes ?? value.mediaTypes,
    parentId: value.ParentId ?? value.parentId,
    genres: value.Genres ?? value.genres,
    tags: value.Tags ?? value.tags,
    studios: value.Studios ?? value.studios,
    artists: value.Artists ?? value.artists,
    person: value.Person ?? value.person,
    years: value.Years ?? value.years,
    officialRatings: value.OfficialRatings ?? value.officialRatings,
    filters: value.Filters ?? value.filters,
    isFavorite: booleanQuery(value.IsFavorite ?? value.isFavorite),
    isPlayed: booleanQuery(value.IsPlayed ?? value.isPlayed),
    sortBy: value.SortBy ?? value.sortBy,
    sortOrder: value.SortOrder ?? value.sortOrder,
    startIndex: numberQuery(value.StartIndex ?? value.startIndex),
    limit: numberQuery(value.Limit ?? value.limit)
  };
}

function stripBridgeUserQuery(query: unknown): Record<string, string | number | boolean | undefined> {
  const value = (query ?? {}) as Record<string, unknown>;
  const sanitized: Record<string, string | number | boolean | undefined> = {};
  for (const [key, rawValue] of Object.entries(value)) {
    if (key.toLowerCase() === "userid") continue;
    if (typeof rawValue === "string" || typeof rawValue === "number" || typeof rawValue === "boolean" || rawValue === undefined) {
      sanitized[key] = rawValue;
    }
  }
  return sanitized;
}

function shouldHydrateItem(query: unknown): boolean {
  return Object.keys((query ?? {}) as Record<string, unknown>).length > 0;
}

function numberQuery(value: string | undefined): number | undefined {
  return value === undefined ? undefined : Number(value);
}

function booleanQuery(value: string | undefined): boolean | undefined {
  if (value === undefined) return undefined;
  if (value.toLowerCase() === "true") return true;
  if (value.toLowerCase() === "false") return false;
  return undefined;
}

const IMAGE_TYPES = new Set([
  "art",
  "backdrop",
  "banner",
  "box",
  "boxrear",
  "chapter",
  "disc",
  "logo",
  "menu",
  "primary",
  "profile",
  "screenshot",
  "thumb"
]);

function itemImagePath(itemId: string, imageType: string, imageIndex?: string): string | undefined {
  const safeItemId = safeIdPathSegment(itemId);
  const safeImageType = safeImageTypePathSegment(imageType);
  const safeImageIndex = imageIndex === undefined ? undefined : safeImageIndexPathSegment(imageIndex);
  if (!safeItemId || !safeImageType || (imageIndex !== undefined && !safeImageIndex)) return undefined;
  return safeImageIndex === undefined
    ? `/Items/${safeItemId}/Images/${safeImageType}`
    : `/Items/${safeItemId}/Images/${safeImageType}/${safeImageIndex}`;
}

function safeIdPathSegment(value: string): string | undefined {
  return /^[A-Za-z0-9_-]+$/.test(value) ? encodeURIComponent(value) : undefined;
}

function safeImageTypePathSegment(value: string): string | undefined {
  return IMAGE_TYPES.has(value.toLowerCase()) ? value : undefined;
}

function safeImageIndexPathSegment(value: string): string | undefined {
  if (!/^(0|[1-9][0-9]*)$/.test(value)) return undefined;
  return Number.isSafeInteger(Number(value)) ? value : undefined;
}

function parentIdFrom(query: unknown): string | undefined {
  const value = (query ?? {}) as Record<string, string | undefined>;
  return value.ParentId ?? value.parentId;
}

function startIndexFrom(query: unknown): number | undefined {
  const value = (query ?? {}) as Record<string, string | undefined>;
  return numberQuery(value.StartIndex ?? value.startIndex);
}

function limitFrom(query: unknown): number | undefined {
  const value = (query ?? {}) as Record<string, string | undefined>;
  return numberQuery(value.Limit ?? value.limit);
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return Boolean(value) && typeof value === "object" && !Array.isArray(value);
}

function isMissingUpstreamError(error: unknown): boolean {
  if (!(error instanceof Error)) return false;
  return / returned HTTP 404 /.test(error.message) || /Unexpected upstream (raw )?request/.test(error.message);
}

function isIgnorableLiveSourceError(error: unknown): boolean {
  if (isMissingUpstreamError(error)) return true;
  if (!(error instanceof Error)) return false;
  return /^Upstream .+ request failed for /.test(error.message)
    || /^Upstream .+ returned HTTP (408|429|5\d\d) /.test(error.message);
}

function uniqueStrings(values: string[]): string[] {
  const byLower = new Map<string, string>();
  for (const value of values) {
    const trimmed = value.trim();
    if (trimmed && !byLower.has(trimmed.toLowerCase())) byLower.set(trimmed.toLowerCase(), trimmed);
  }
  return Array.from(byLower.values()).sort((left, right) => left.localeCompare(right));
}

function uniqueNumbers(values: unknown[]): number[] {
  return Array.from(new Set(values.filter((value): value is number => typeof value === "number" && value > 0))).sort((left, right) => left - right);
}

function stringList(value: unknown): string[] {
  return Array.isArray(value) ? value.filter((item): item is string => typeof item === "string") : [];
}

function hasStatusCode(error: Error): error is Error & { statusCode: number } {
  return "statusCode" in error && typeof error.statusCode === "number";
}

function copyProxyRequestHeaders(headers: Record<string, unknown>): Record<string, string> {
  const copied: Record<string, string> = {};
  for (const name of ["range", "user-agent", "accept", "accept-encoding"]) {
    const value = headers[name];
    if (typeof value === "string") copied[name] = value;
  }
  return copied;
}

function copyProxyResponseHeaders(reply: { header(name: string, value: string): unknown }, headers: Record<string, string | string[] | undefined>): void {
  for (const name of ["content-type", "content-length", "content-range", "accept-ranges", "etag", "last-modified", "cache-control"]) {
    const value = headers[name];
    if (typeof value === "string") {
      reply.header(name, value);
    }
  }
}

function isAsyncBody(body: unknown): body is AsyncIterable<unknown> {
  return !Buffer.isBuffer(body)
    && !(body instanceof Uint8Array)
    && Boolean(body)
    && Symbol.asyncIterator in Object(body);
}

async function responseBodyText(body: unknown): Promise<string> {
  if (Buffer.isBuffer(body)) return body.toString("utf8");
  if (typeof body === "string") return body;
  if (body && typeof (body as { text?: unknown }).text === "function") {
    return (body as { text(): Promise<string> }).text();
  }
  return String(body ?? "");
}

async function responseBodyBuffer(body: unknown): Promise<Buffer> {
  if (Buffer.isBuffer(body)) return body;
  if (body instanceof Uint8Array) return Buffer.from(body);
  if (typeof body === "string") return Buffer.from(body);
  if (body && typeof (body as { arrayBuffer?: unknown }).arrayBuffer === "function") {
    return Buffer.from(await (body as { arrayBuffer(): Promise<ArrayBuffer> }).arrayBuffer());
  }
  if (body && Symbol.asyncIterator in Object(body)) {
    const chunks: Buffer[] = [];
    for await (const chunk of body as AsyncIterable<unknown>) {
      chunks.push(Buffer.isBuffer(chunk) ? chunk : chunk instanceof Uint8Array ? Buffer.from(chunk) : Buffer.from(String(chunk)));
    }
    return Buffer.concat(chunks);
  }
  return Buffer.from(String(body ?? ""));
}
