import Fastify, { type FastifyInstance, type FastifyReply, type FastifyRequest } from "fastify";
import { authenticatePassword, parseAuthorization, requireSession, type AuthContext, userDto, userId } from "./auth.js";
import type { BridgeConfig, BridgeUser, RuntimeConfigSource } from "./config.js";
import { badGatewayError, notFound, unsupported } from "./errors.js";
import { bridgeItemId, bridgeLibraryId, bridgeMediaSourceId, bridgeServerId, passThroughLibraryId } from "./ids.js";
import { rewriteHlsPlaylist } from "./hls.js";
import { Indexer } from "./indexer.js";
import { libraryDto, passThroughLibraryDto, publicSystemInfo, queryResult, sessionInfo, systemInfo, userDataDto } from "./jellyfin.js";
import { bridgeItemIdMapForSourceItem, bridgeItemSources, countBridgeItemsFromIndexedItems, getBridgeItem, itemCounts, listBridgeItems, listBridgeItemsForSourceParents, queryBridgeItems, queryBridgeItemsFromIndexedItems } from "./library.js";
import type { BrowseQuery } from "./library.js";
import { logicalItemKey, type SourceItem } from "./merge.js";
import { findMetadataItem, listAlbumArtists, listArtists, listGenres, listPersons, listStudios, listYears } from "./metadata.js";
import { ITEM_ID_FIELDS, rewriteDto } from "./rewriter.js";
import type { IndexedItemRecord, InfuseSyncCheckpointRecord, MediaSourceMapping, PlaybackSessionMapping, Store, UserDataPatch, UserDataRecord } from "./store.js";
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
  collectionType?: string | null;
  priority: number;
}

interface LiveCandidate {
  source: LiveSource;
  item: Record<string, unknown>;
}

type LiveQueryParams = Record<string, string | number | boolean | undefined>;

interface RawProxyCandidate {
  serverId: string;
  path: string;
}

interface WatchedWriteTarget {
  bridgeItemId: string;
  sources: IndexedItemRecord[];
}

type WatchedUserDataPayload = Record<string, boolean | number | string>;

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
    await refreshLiveViewsForRequest(snapshotConfig, client, auth.user.name);
    return queryResult(viewDtos(snapshotConfig, snapshotServerId));
  });

  app.get("/Users/:userId/Views", async (request) => {
    const auth = requireSession(request, config, store);
    const params = request.params as { userId: string };
    requireSelf(auth, params.userId);
    const snapshotConfig = config;
    const snapshotServerId = serverId;
    const client = upstream;
    await refreshLiveViewsForRequest(snapshotConfig, client, auth.user.name);
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

  app.get("/Plugins", async (request) => {
    requireSession(request, config, store);
    return [infuseSyncPluginInfo()];
  });

  app.post("/InfuseSync/Checkpoint", async (request) => {
    const auth = requireSession(request, config, store);
    const deviceIdValue = requiredInfuseSyncQueryValue(request.query, "DeviceID");
    const userIdValue = requireSelf(auth, requiredInfuseSyncQueryValue(request.query, "UserID"));
    const checkpoint = store.createInfuseSyncCheckpoint(deviceIdValue, userIdValue);
    return { Id: checkpoint.id };
  });

  app.post("/InfuseSync/Checkpoint/:checkpointId/StartSync", async (request, reply) => {
    const auth = requireSession(request, config, store);
    const { checkpointId } = request.params as { checkpointId: string };
    const checkpoint = store.getInfuseSyncCheckpoint(checkpointId);
    if (!checkpoint) return notFound(reply, "InfuseSync checkpoint not found");
    requireSelf(auth, checkpoint.userId);
    const synced = store.startInfuseSyncCheckpoint(checkpointId);
    if (!synced) return notFound(reply, "InfuseSync checkpoint not found");
    return infuseSyncStats(requireCompletedInfuseSyncCheckpoint(synced));
  });

  app.get("/InfuseSync/Checkpoint/:checkpointId/UpdatedItems", async (request, reply) => {
    const auth = requireSession(request, config, store);
    const checkpoint = infuseSyncCheckpointFromRequest(request, reply);
    if (!checkpoint) return;
    requireSelf(auth, checkpoint.userId);
    const syncWindow = requireCompletedInfuseSyncCheckpoint(checkpoint);
    const query = infuseSyncBrowseQuery(request.query);
    const result = queryBridgeItemsFromIndexedItems(
      config,
      store,
      auth.session.userId,
      store.listIndexedItemsUpdatedBetween(syncWindow.fromTimestamp, syncWindow.syncTimestamp),
      query
    );
    return queryResult(result.items, query.startIndex ?? 0, result.total);
  });

  app.get("/InfuseSync/Checkpoint/:checkpointId/RemovedItems", async (request, reply) => {
    const auth = requireSession(request, config, store);
    const checkpoint = infuseSyncCheckpointFromRequest(request, reply);
    if (!checkpoint) return;
    requireSelf(auth, checkpoint.userId);
    requireCompletedInfuseSyncCheckpoint(checkpoint);
    return queryResult([], infuseSyncStartIndex(request.query), 0);
  });

  app.get("/InfuseSync/Checkpoint/:checkpointId/UserData", async (request, reply) => {
    const auth = requireSession(request, config, store);
    const checkpoint = infuseSyncCheckpointFromRequest(request, reply);
    if (!checkpoint) return;
    requireSelf(auth, checkpoint.userId);
    const syncWindow = requireCompletedInfuseSyncCheckpoint(checkpoint);
    const query = infuseSyncBrowseQuery(request.query);
    const rows = filterInfuseSyncUserDataRows(
      store.listUserDataUpdatedBetween(checkpoint.userId, syncWindow.fromTimestamp, syncWindow.syncTimestamp),
      parseInfuseSyncItemTypes(request.query)
    );
    const start = query.startIndex ?? 0;
    const end = query.limit === undefined ? undefined : start + query.limit;
    return queryResult(rows.slice(start, end).map(infuseSyncUserDataDto), start, rows.length);
  });

  app.get("/InfuseSync/UserFolders/:userId", async (request) => {
    const auth = requireSession(request, config, store);
    const { userId: requestedUserId } = request.params as { userId: string };
    requireSelf(auth, requestedUserId);
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
    return browseItems(auth.user.name, auth.session.userId, query);
  });
  app.get("/Users/:userId/Items", async (request) => {
    const auth = requireSession(request, config, store);
    const params = request.params as { userId: string };
    const userIdValue = requireSelf(auth, params.userId);
    const query = browseQuery(request.query);
    return browseItems(auth.user.name, userIdValue, query);
  });
  app.get("/Items/Latest", async (request) => {
    const auth = requireSession(request, config, store);
    const snapshotConfig = config;
    return await liveLatestItems(auth.user.name, auth.session.userId, request.query, request).catch(() => latestItems(snapshotConfig, auth.session.userId, request.query));
  });
  app.get("/Users/:userId/Items/Latest", async (request) => {
    const auth = requireSession(request, config, store);
    const params = request.params as { userId: string };
    const userIdValue = requireSelf(auth, params.userId);
    const snapshotConfig = config;
    return await liveLatestItems(auth.user.name, userIdValue, request.query, request).catch(() => latestItems(snapshotConfig, userIdValue, request.query));
  });
  app.get("/UserItems/Resume", async (request) => {
    const auth = requireSession(request, config, store);
    const snapshotConfig = config;
    return await liveQueryResult(auth.user.name, auth.session.userId, "/UserItems/Resume", request.query, request).catch(() => resumeItems(snapshotConfig, auth.session.userId, request.query));
  });
  app.get("/Users/:userId/Items/Resume", async (request) => {
    const auth = requireSession(request, config, store);
    const params = request.params as { userId: string };
    const userIdValue = requireSelf(auth, params.userId);
    const snapshotConfig = config;
    return await liveQueryResult(auth.user.name, userIdValue, "/UserItems/Resume", request.query, request).catch(() => resumeItems(snapshotConfig, userIdValue, request.query));
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
      const live = await liveQueryResult(auth.user.name, auth.session.userId, `/Shows/${seriesId}/Seasons`, request.query, request).catch(() => undefined);
      if (live) return live;
    }
    if (seriesSources.length === 0) return notFound(reply, "Series not found");
    let seasons = listBridgeItemsForSourceParents(snapshotConfig, store, auth.session.userId, seriesSources, ["Season"]);
    if (seasons.length === 0) {
      await refreshLiveSeasons(client, seriesSources, request.query).catch((error: unknown) => {
        const detail = error instanceof Error ? error.message : String(error);
        throw badGatewayError(`Upstream seasons failed: ${detail}`);
      });
      seasons = listBridgeItemsForSourceParents(snapshotConfig, store, auth.session.userId, seriesSources, ["Season"]);
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
      const live = await liveQueryResult(auth.user.name, auth.session.userId, `/Shows/${seriesId}/Episodes`, request.query, request).catch(() => undefined);
      if (live) return live;
    }
    if (seriesSources.length === 0 && seasonSources.length === 0) return notFound(reply, "Series not found");
    const seasonNumber = query.Season ?? query.season;
    const episodeParentSources = seasonSources.length > 0 ? seasonSources : seriesSources;
    let episodes = listBridgeItemsForSourceParents(snapshotConfig, store, auth.session.userId, episodeParentSources, ["Episode"])
      .filter((item) => seasonNumber === undefined || String(item.ParentIndexNumber) === seasonNumber);
    if (episodes.length === 0) {
      await refreshLiveEpisodes(client, seriesSources, seasonSources, request.query).catch((error: unknown) => {
        const detail = error instanceof Error ? error.message : String(error);
        throw badGatewayError(`Upstream episodes failed: ${detail}`);
      });
      episodes = listBridgeItemsForSourceParents(snapshotConfig, store, auth.session.userId, episodeParentSources, ["Episode"])
        .filter((item) => seasonNumber === undefined || String(item.ParentIndexNumber) === seasonNumber);
    }
    return pagedQueryResult(episodes, browseQuery(request.query));
  });

  app.get("/Shows/NextUp", async (request) => {
    const auth = requireSession(request, config, store);
    const snapshotConfig = config;
    const live = await liveQueryResult(auth.user.name, auth.session.userId, "/Shows/NextUp", request.query, request).catch(() => undefined);
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

  app.get("/Items/:itemId/Images", async (request, reply) => getItemImageInfos(request, reply));
  app.get("/Items/:itemId/Images/:imageType", async (request, reply) => proxyItemImage(request, reply));
  app.get("/Items/:itemId/Images/:imageType/:imageIndex", async (request, reply) => proxyItemImage(request, reply));
  app.get("/Items/:itemId/Images/:imageType/:imageIndex/:tag/:format/:maxWidth/:maxHeight/:percentPlayed/:unplayedCount", async (request, reply) => proxyItemImage(request, reply));

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
    const auth = requireSession(request, config, store);
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
    const permission = await deletePermissionForSourceItem(client, auth.user.name, source);
    if (permission === "missing") {
      notFound(reply, "Item not found");
      return;
    }
    if (permission !== "allowed") {
      reply.code(401).send({
        type: "https://jellyfin.org/docs/general/server/api/",
        title: "Unauthorized",
        status: 401,
        detail: "User is not allowed to delete this item"
      });
      return;
    }

    try {
      await client.raw(source.serverId, `/Items/${source.itemId}`, { method: "DELETE" });
    } catch (error) {
      if (isMissingUpstreamError(error)) {
        notFound(reply, "Item not found");
        return;
      }
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

  app.post("/UserItems/:itemId/UserData", async (request, reply) => {
    const auth = requireSession(request, config, store);
    const { itemId } = request.params as { itemId: string };
    const userIdValue = requireSelf(auth, userIdFromQuery(request.query));
    const watchedPayload = watchedUserDataPayload(request.body);
    if (watchedPayload) {
      const target = resolveWatchedWriteTarget(itemId);
      if (!target) return notFound(reply, "Item not found");
      await forwardWatchedUserData(target.sources, auth.user.name, watchedPayload);
      saveUserData(store, userIdValue, target.bridgeItemId, request.body);
      return userDataDto(store, userIdValue, target.bridgeItemId);
    }
    if (!canWriteUserData(userIdValue, itemId)) return notFound(reply, "Item not found");
    saveUserData(store, userIdValue, itemId, request.body);
    return userDataDto(store, userIdValue, itemId);
  });

  app.post("/Users/:userId/Items/:itemId/UserData", async (request, reply) => {
    const auth = requireSession(request, config, store);
    const { userId: routeUserId, itemId } = request.params as { userId: string; itemId: string };
    const userIdValue = requireSelf(auth, routeUserId);
    const watchedPayload = watchedUserDataPayload(request.body);
    if (watchedPayload) {
      const target = resolveWatchedWriteTarget(itemId);
      if (!target) return notFound(reply, "Item not found");
      await forwardWatchedUserData(target.sources, auth.user.name, watchedPayload);
      saveUserData(store, userIdValue, target.bridgeItemId, request.body);
      return userDataDto(store, userIdValue, target.bridgeItemId);
    }
    if (!canWriteUserData(userIdValue, itemId)) return notFound(reply, "Item not found");
    saveUserData(store, userIdValue, itemId, request.body);
    return userDataDto(store, userIdValue, itemId);
  });

  app.post("/UserFavoriteItems/:itemId", async (request, reply) => {
    const auth = requireSession(request, config, store);
    const { itemId } = request.params as { itemId: string };
    if (!canWriteUserData(auth.session.userId, itemId)) return notFound(reply, "Item not found");
    return setFavorite(auth.session.userId, itemId, true);
  });

  app.delete("/UserFavoriteItems/:itemId", async (request, reply) => {
    const auth = requireSession(request, config, store);
    const { itemId } = request.params as { itemId: string };
    if (!canWriteUserData(auth.session.userId, itemId)) return notFound(reply, "Item not found");
    return setFavorite(auth.session.userId, itemId, false);
  });

  app.post("/Users/:userId/FavoriteItems/:itemId", async (request, reply) => {
    const auth = requireSession(request, config, store);
    const { userId: routeUserId, itemId } = request.params as { userId: string; itemId: string };
    const userIdValue = requireSelf(auth, routeUserId);
    if (!canWriteUserData(userIdValue, itemId)) return notFound(reply, "Item not found");
    return setFavorite(userIdValue, itemId, true);
  });

  app.delete("/Users/:userId/FavoriteItems/:itemId", async (request, reply) => {
    const auth = requireSession(request, config, store);
    const { userId: routeUserId, itemId } = request.params as { userId: string; itemId: string };
    const userIdValue = requireSelf(auth, routeUserId);
    if (!canWriteUserData(userIdValue, itemId)) return notFound(reply, "Item not found");
    return setFavorite(userIdValue, itemId, false);
  });

  app.post("/UserPlayedItems/:itemId", async (request, reply) => {
    const auth = requireSession(request, config, store);
    const { itemId } = request.params as { itemId: string };
    const userIdValue = requireSelf(auth, userIdFromQuery(request.query));
    const target = resolveWatchedWriteTarget(itemId);
    if (!target) return notFound(reply, "Item not found");
    const datePlayed = datePlayedFromQuery(request.query);
    await forwardPlayedState(target.sources, auth.user.name, true, datePlayed);
    return setPlayed(userIdValue, target.bridgeItemId, true, datePlayed);
  });

  app.delete("/UserPlayedItems/:itemId", async (request, reply) => {
    const auth = requireSession(request, config, store);
    const { itemId } = request.params as { itemId: string };
    const userIdValue = requireSelf(auth, userIdFromQuery(request.query));
    const target = resolveWatchedWriteTarget(itemId);
    if (!target) return notFound(reply, "Item not found");
    await forwardPlayedState(target.sources, auth.user.name, false);
    return setPlayed(userIdValue, target.bridgeItemId, false);
  });

  app.post("/Users/:userId/PlayedItems/:itemId", async (request, reply) => {
    const auth = requireSession(request, config, store);
    const { userId: routeUserId, itemId } = request.params as { userId: string; itemId: string };
    const userIdValue = requireSelf(auth, routeUserId);
    const target = resolveWatchedWriteTarget(itemId);
    if (!target) return notFound(reply, "Item not found");
    const datePlayed = datePlayedFromQuery(request.query);
    await forwardPlayedState(target.sources, auth.user.name, true, datePlayed);
    return setPlayed(userIdValue, target.bridgeItemId, true, datePlayed);
  });

  app.delete("/Users/:userId/PlayedItems/:itemId", async (request, reply) => {
    const auth = requireSession(request, config, store);
    const { userId: routeUserId, itemId } = request.params as { userId: string; itemId: string };
    const userIdValue = requireSelf(auth, routeUserId);
    const target = resolveWatchedWriteTarget(itemId);
    if (!target) return notFound(reply, "Item not found");
    await forwardPlayedState(target.sources, auth.user.name, false);
    return setPlayed(userIdValue, target.bridgeItemId, false);
  });

  app.post("/Sessions/Playing", async (request, reply) => {
    const auth = requireSession(request, config, store);
    applyPlaybackStartUserData(auth.session.userId, request.body);
    await forwardPlaybackReport("/Sessions/Playing", request.body, auth.user.name);
    reply.code(204).send();
  });
  app.post("/Sessions/Playing/Progress", async (request, reply) => {
    const auth = requireSession(request, config, store);
    applyPlaybackReportUserData(auth.session.userId, request.body, false);
    await forwardPlaybackReport("/Sessions/Playing/Progress", request.body, auth.user.name);
    reply.code(204).send();
  });
  app.post("/Sessions/Playing/Stopped", async (request, reply) => {
    const auth = requireSession(request, config, store);
    applyPlaybackReportUserData(auth.session.userId, request.body, true);
    await forwardPlaybackReport("/Sessions/Playing/Stopped", request.body, auth.user.name);
    reply.code(204).send();
  });
  app.post("/Sessions/Playing/Ping", async (request, reply) => {
    const auth = requireSession(request, config, store);
    await forwardPlaybackReport("/Sessions/Playing/Ping", request.body, auth.user.name, request.query);
    reply.code(204).send();
  });

  app.get("/Items/:itemId/PlaybackInfo", async (request, reply) => {
    const auth = requireSession(request, config, store);
    return getPlaybackInfo(request.params as { itemId: string }, request.query, undefined, auth.session.userId, auth.user.name, auth.session.accessToken, request);
  });
  app.post("/Items/:itemId/PlaybackInfo", async (request, reply) => {
    const auth = requireSession(request, config, store);
    return getPlaybackInfo(request.params as { itemId: string }, request.query, request.body, auth.session.userId, auth.user.name, auth.session.accessToken, request);
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
  app.get("/Videos/:itemId/hls1/:playlistId/:segmentId.:container", async (request, reply) => proxyHlsSegment("Videos", "hls1", request, reply));
  app.get("/Audio/:itemId/hls1/:playlistId/:segmentId.:container", async (request, reply) => proxyHlsSegment("Audio", "hls1", request, reply));
  app.get("/Videos/:itemId/hls/:playlistId/stream.m3u8", async (request, reply) => proxyLegacyHlsPlaylist(request, reply));
  app.get("/Videos/:itemId/hls/:playlistId/:segmentId.:container", async (request, reply) => proxyHlsSegment("Videos", "hls", request, reply));
  app.get("/Audio/:itemId/hls/:segmentId/stream.:container", async (request, reply) => proxyLegacyAudioHlsSegment(request, reply));
  app.get("/Videos/:itemId/hls/:mediaSourceId/:playlistId/:segmentId.:container", async (request, reply) => proxyHlsSegment("Videos", "hls1", request, reply));
  app.get("/Audio/:itemId/hls/:mediaSourceId/:playlistId/:segmentId.:container", async (request, reply) => proxyHlsSegment("Audio", "hls1", request, reply));
  app.get("/Videos/:itemId/Trickplay/:width/tiles.m3u8", async (request, reply) => proxyTrickplayPlaylist(request, reply));
  app.get("/Videos/:itemId/Trickplay/:width/:index.jpg", async (request, reply) => proxyTrickplayTile(request, reply));
  app.get("/Videos/:itemId/:mediaSourceId/Subtitles/:index/subtitles.m3u8", async (request, reply) => proxySubtitlePlaylist(request, reply));
  app.get("/Videos/:itemId/:mediaSourceId/Subtitles/:index/stream.:format", async (request, reply) => proxySubtitleStream(request, reply, "stream"));
  app.get("/Videos/:itemId/:mediaSourceId/Subtitles/:index/:startPositionTicks/stream.:format", async (request, reply) => proxySubtitleStream(request, reply, "stream"));
  app.get("/Videos/:itemId/:mediaSourceId/Subtitles/:index/Stream.:format", async (request, reply) => proxySubtitleStream(request, reply));
  app.get("/Videos/:itemId/:mediaSourceId/Subtitles/:index/:startPositionTicks/Stream.:format", async (request, reply) => proxySubtitleStream(request, reply));
  app.get("/Videos/:itemId/:mediaSourceId/Attachments/:index", async (request, reply) => proxyAttachment(request, reply));

  registerUnsupportedRoutes(app);

  app.all("/*", async (_request, reply) => notFound(reply));
  return app;

  async function browseItems(userName: string, userIdValue: string, query: BrowseQuery): Promise<Record<string, unknown>> {
    const snapshotConfig = config;
    const snapshotServerId = serverId;
    const client = upstream;
    if (!query.parentId && query.recursive !== true) {
      if (liveRouteAggregation) {
        await refreshLiveViewsForRequest(snapshotConfig, client, userName);
      }
      const views = viewDtos(snapshotConfig, snapshotServerId);
      const start = query.startIndex ?? 0;
      return queryResult(views, start, views.length);
    }
    let result = queryBridgeItems(snapshotConfig, store, userIdValue, query);
    if (result.total === 0) {
      await refreshLiveBrowse(snapshotConfig, client, query).catch((error: unknown) => {
        const detail = error instanceof Error ? error.message : String(error);
        throw badGatewayError(`Upstream browse failed: ${detail}`);
      });
      result = queryBridgeItems(snapshotConfig, store, userIdValue, query);
    }
    return queryResult(result.items, query.startIndex ?? 0, result.total);
  }

  async function refreshLiveBrowse(snapshotConfig: BridgeConfig, client: AppUpstreamClient, query: BrowseQuery): Promise<void> {
    let attempted = false;
    let sawResponse = false;
    const refresh = async (serverIdValue: string, libraryIdValue: string, collectionType?: string | null) => {
      attempted = true;
      try {
        await refreshLiveSource(client, serverIdValue, libraryIdValue, query, collectionType);
        sawResponse = true;
      } catch (error) {
        if (!isIgnorableLiveSourceError(error)) throw error;
      }
    };

    const libraries = mappedLibrariesForBrowse(snapshotConfig, query.parentId);
    for (const library of libraries) {
      for (const source of library.sources) {
        await refresh(source.server, source.libraryId, library.collectionType);
      }
    }
    if (libraries.length === 0 && query.parentId) {
      const passThroughLibrary = store.listUpstreamLibraries().find((library) => passThroughLibraryId(library.serverId, library.libraryId) === query.parentId);
      if (passThroughLibrary) {
        await refresh(passThroughLibrary.serverId, passThroughLibrary.libraryId, passThroughLibrary.collectionType);
      } else {
        for (const source of bridgeItemSources(snapshotConfig, store, query.parentId)) {
          attempted = true;
          try {
            await refreshLiveSource(client, source.serverId, source.libraryId, query, collectionTypeForSource(snapshotConfig, source.serverId, source.libraryId), source.itemId);
            sawResponse = true;
          } catch (error) {
            if (!isIgnorableLiveSourceError(error)) throw error;
          }
        }
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

  async function refreshLiveSource(
    client: AppUpstreamClient,
    serverIdValue: string,
    libraryIdValue: string,
    query: BrowseQuery,
    collectionType?: string | null,
    parentIdValue = libraryIdValue
  ): Promise<void> {
    let startIndex = 0;
    let totalRecordCount = Number.POSITIVE_INFINITY;
    while (startIndex < totalRecordCount) {
      const response = await client.json<{ Items?: SourceItem[]; TotalRecordCount?: number }>(serverIdValue, "/Items", {
        query: {
          ParentId: parentIdValue,
          StartIndex: startIndex,
          Limit: 100,
          Fields: LIVE_BROWSE_FIELDS,
          IncludeItemTypes: query.includeItemTypes,
          MediaTypes: query.mediaTypes,
          Recursive: query.recursive
        }
      });
      const items = Array.isArray(response.Items) ? response.Items : [];
      totalRecordCount = Number(response.TotalRecordCount ?? items.length);
      upsertLiveItems(serverIdValue, libraryIdValue, items);
      if (items.length === 0) break;
      startIndex += items.length;
    }
  }

  function collectionTypeForSource(snapshotConfig: BridgeConfig, serverIdValue: string, libraryIdValue: string): string | null | undefined {
    const configuredLibrary = snapshotConfig.libraries.find((library) =>
      library.sources.some((source) => source.server === serverIdValue && source.libraryId === libraryIdValue)
    );
    if (configuredLibrary) return configuredLibrary.collectionType;
    return store.listUpstreamLibraries().find((library) => library.serverId === serverIdValue && library.libraryId === libraryIdValue)?.collectionType;
  }

  async function liveLatestItems(userName: string, userIdValue: string, rawQuery: unknown, request?: FastifyRequest): Promise<Record<string, unknown>[]> {
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
        const query = latestLiveQueryForSource(rawQuery, source, await liveQueryForSource(client, cacheVersion, userName, rawQuery, source));
        logUpstreamJson(request, "/Items/Latest", source, "/Items/Latest");
        const response = await client.json<unknown>(source.serverId, "/Items/Latest", { query });
        sawResponse = true;
        for (const item of liveItemsFromResponse(response)) {
          await ensureRelatedLiveItems(client, cacheVersion, userName, source, item);
          upsertLiveItem(source, item);
          candidates.push({ source, item });
        }
      } catch (error) {
        if (!isIgnorableLiveSourceError(error)) throw error;
      }
    }));
    if (!sawResponse) throw new Error("No upstream response for /Items/Latest");

    const limit = limitFrom(rawQuery) ?? 20;
    const latest = mergeLiveCandidates(candidates)
      .sort(compareLiveDateCreatedDescending)
      .map((candidate) => latestLiveDto(candidate, userIdValue, bridgeServerIdValue));
    return groupLatestDtos(latest, userIdValue).slice(0, limit);
  }

  async function liveQueryResult(userName: string, userIdValue: string, path: string, rawQuery: unknown, request?: FastifyRequest): Promise<Record<string, unknown>> {
    if (!liveRouteAggregation) throw new Error("Live route aggregation is disabled");
    const client = upstream;
    const bridgeServerIdValue = serverId;
    const cacheVersion = configVersion;
    const sources = path === "/UserItems/Resume" || path === "/Shows/NextUp" ? liveQuerySources(path, rawQuery) : liveUpstreamSources();
    logUpstreamFanout(request, path, path, sources);
    const candidates: LiveCandidate[] = [];
    let sawResponse = false;
    let upstreamTotalRecordCount = 0;

    await Promise.all(sources.map(async (source) => {
      const query = aggregateLivePageQuery(rawQuery, await liveQueryForSource(client, cacheVersion, userName, rawQuery, source));
      try {
        logUpstreamJson(request, path, source, path);
        const response = await client.json<unknown>(source.serverId, path, { query });
        sawResponse = true;
        const total = liveTotalRecordCountFromResponse(response);
        const items = liveItemsFromResponse(response);
        upstreamTotalRecordCount += total;
        for (const item of items) {
          if (source.libraryId) {
            await ensureRelatedLiveItems(client, cacheVersion, userName, source, item);
            upsertLiveItem(source, item);
          }
          candidates.push({ source, item });
        }
      } catch (error) {
        if (!isIgnorableLiveSourceError(error)) throw error;
      }
    }));

    if (!sawResponse) throw new Error(`No upstream response for ${path}`);
    const startIndex = startIndexFrom(rawQuery) ?? 0;
    const limit = limitFrom(rawQuery);
    const merged = orderLiveCandidates(path, mergeLiveCandidates(candidates));
    const paged = limit === undefined ? merged.slice(startIndex) : merged.slice(startIndex, startIndex + limit);
    const total = Math.max(merged.length, upstreamTotalRecordCount);
    return queryResult(paged.map((candidate) => scopedLiveDto(candidate, userIdValue, bridgeServerIdValue)), startIndex, total);
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
          collectionType: library.collectionType,
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
          collectionType: passThroughLibrary.collectionType,
          priority: liveSourcePriority(passThroughLibrary.serverId, 0)
        }];
      }
    }

    const sources = configuredLiveLibrarySources();
    const mapped = new Set(config.libraries.flatMap((library) => library.sources.map((source) => `${source.server}:${source.libraryId}`)));
    for (const library of store.listUpstreamLibraries()) {
      if (mapped.has(`${library.serverId}:${library.libraryId}`)) continue;
      sources.push({
        serverId: library.serverId,
        libraryId: library.libraryId,
        bridgeLibraryId: passThroughLibraryId(library.serverId, library.libraryId),
        collectionType: library.collectionType,
        priority: liveSourcePriority(library.serverId, sources.length)
      });
    }
    return sources.length > 0 ? sources : liveUpstreamSources();
  }

  function configuredLiveLibrarySources(): LiveSource[] {
    const sources: LiveSource[] = [];
    for (const library of config.libraries) {
      for (let index = 0; index < library.sources.length; index += 1) {
        const source = library.sources[index];
        sources.push({
          serverId: source.server,
          libraryId: source.libraryId,
          bridgeLibraryId: bridgeLibraryId(library.id),
          collectionType: library.collectionType,
          priority: liveSourcePriority(source.server, index)
        });
      }
    }
    return sources;
  }

  function liveQuerySources(path: string, rawQuery: unknown): LiveSource[] {
    const parentId = parentIdFrom(rawQuery);
    if (parentId) return liveLibrarySources(rawQuery);
    const sources = liveLibrarySources(rawQuery);
    if (path !== "/Shows/NextUp") return sources;
    const tvSources = sources.filter((source) => source.collectionType === "tvshows" || source.collectionType === undefined);
    return tvSources.length > 0 ? tvSources : sources;
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
    if (parentIdFrom(rawQuery)) {
      rewriteParentIdForSource(query, rawQuery, source);
    } else if (source.libraryId) {
      delete query.parentId;
      query.ParentId = source.libraryId;
    }
    rewriteSeriesIdForSource(query, rawQuery, source);
    const upstreamUserId = await liveUserId(client, cacheVersion, source.serverId, userName);
    if (upstreamUserId) {
      query.UserId = upstreamUserId;
    }
    return query;
  }

  function rewriteParentIdForSource(query: LiveQueryParams, rawQuery: unknown, source: LiveSource): void {
    const parentId = parentIdFrom(rawQuery);
    if (!parentId) return;
    const upstreamParentId = upstreamParentIdForSource(parentId, source);
    if (!upstreamParentId) return;
    delete query.parentId;
    query.ParentId = upstreamParentId;
  }

  function upstreamParentIdForSource(parentId: string, source: LiveSource): string | undefined {
    if (source.libraryId && parentId === source.bridgeLibraryId) return source.libraryId;
    const passThroughLibraryIdValue = source.libraryId ? passThroughLibraryId(source.serverId, source.libraryId) : undefined;
    if (source.libraryId && parentId === passThroughLibraryIdValue) return source.libraryId;
    return bridgeItemSources(config, store, parentId).find((candidate) => candidate.serverId === source.serverId)?.itemId;
  }

  function rewriteSeriesIdForSource(query: LiveQueryParams, rawQuery: unknown, source: LiveSource): void {
    const seriesId = seriesIdFrom(rawQuery);
    if (!seriesId) return;
    const mapped = bridgeItemSources(config, store, seriesId).find((candidate) => candidate.serverId === source.serverId);
    if (!mapped) return;
    delete query.seriesId;
    query.SeriesId = mapped.itemId;
  }

  function latestLiveQueryForSource(
    rawQuery: unknown,
    source: LiveSource,
    query: Record<string, string | number | boolean | undefined>
  ): Record<string, string | number | boolean | undefined> {
    if (source.libraryId) {
      delete query.parentId;
      query.ParentId = source.libraryId;
    }
    const includeItemTypes = latestIncludeItemTypesForCollection(source.collectionType);
    if (includeItemTypes && !includeItemTypesFrom(rawQuery)) {
      delete query.includeItemTypes;
      query.IncludeItemTypes = includeItemTypes;
    }
    return query;
  }

  function latestLiveDto(candidate: LiveCandidate, userIdValue: string, bridgeServerIdValue: string): Record<string, unknown> {
    return canonicalLiveDto(candidate, userIdValue, bridgeServerIdValue);
  }

  function groupLatestDtos(items: Record<string, unknown>[], userIdValue: string, snapshotConfig: BridgeConfig = config): Record<string, unknown>[] {
    const output: Record<string, unknown>[] = [];
    const episodeGroups = new Map<string, { container: Record<string, unknown>; children: Record<string, unknown>[]; index: number }>();
    for (const item of items.filter((item) => !isLatestSpecial(item, userIdValue, snapshotConfig))) {
      const seriesId = String(item.Type ?? "").toLowerCase() === "episode" && typeof item.SeriesId === "string" ? item.SeriesId : undefined;
      const series = seriesId ? getBridgeItem(snapshotConfig, store, userIdValue, seriesId) : undefined;
      if (!seriesId || !series) {
        output.push(item);
        continue;
      }

      const existing = episodeGroups.get(seriesId);
      if (existing) {
        existing.children.push(item);
        continue;
      }
      episodeGroups.set(seriesId, { container: series, children: [item], index: output.length });
      output.push(item);
    }

    for (const group of episodeGroups.values()) {
      if (group.children.length > 1) {
        output[group.index] = { ...group.container, ChildCount: group.children.length };
      }
    }
    return output;
  }

  function isLatestSpecial(item: Record<string, unknown>, userIdValue: string, snapshotConfig: BridgeConfig): boolean {
    const type = String(item.Type ?? "").toLowerCase();
    if (type === "season") return isZeroIndex(item.IndexNumber);
    if (type !== "episode") return false;
    if (isZeroIndex(item.ParentIndexNumber)) return true;
    if (item.ParentIndexNumber !== undefined && item.ParentIndexNumber !== null) return false;
    if (typeof item.SeasonId !== "string") return false;
    const season = getBridgeItem(snapshotConfig, store, userIdValue, item.SeasonId);
    return season ? isZeroIndex(season.IndexNumber) : false;
  }

  function isZeroIndex(value: unknown): boolean {
    return value === 0 || value === "0";
  }

  function scopedLiveDto(candidate: LiveCandidate, userIdValue: string, bridgeServerIdValue: string): Record<string, unknown> {
    return candidate.source.libraryId
      ? canonicalLiveDto(candidate, userIdValue, bridgeServerIdValue)
      : rewriteLiveDto(candidate.item, candidate.source, bridgeServerIdValue);
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

  function liveTotalRecordCountFromResponse(response: unknown): number {
    if (Array.isArray(response)) return response.length;
    if (isRecord(response) && typeof response.TotalRecordCount === "number") return response.TotalRecordCount;
    return liveItemsFromResponse(response).length;
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

  function orderLiveCandidates(path: string, candidates: LiveCandidate[]): LiveCandidate[] {
    return path === "/UserItems/Resume" ? [...candidates].sort(compareLiveResumeDescending) : candidates;
  }

  function rewriteLiveDto(item: Record<string, unknown>, source: LiveSource, bridgeServerIdValue: string = serverId): Record<string, unknown> {
    const rewritten = rewriteLiveValue(item, source, bridgeServerIdValue);
    return isRecord(rewritten) ? rewritten : item;
  }

  function canonicalLiveDto(candidate: LiveCandidate, userIdValue: string, bridgeServerIdValue: string): Record<string, unknown> {
    if (candidate.source.libraryId) {
      const id = bridgeItemId(logicalItemKey(candidate.item as unknown as SourceItem, candidate.source.serverId));
      const item = getBridgeItem(config, store, userIdValue, id);
      if (item) return item;
    }
    return rewriteLiveDto(candidate.item, candidate.source, bridgeServerIdValue);
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

  function compareLiveResumeDescending(left: LiveCandidate, right: LiveCandidate): number {
    const result = resumeSortDate(right).localeCompare(resumeSortDate(left));
    return result === 0 ? left.source.priority - right.source.priority : result;
  }

  function resumeSortDate(candidate: LiveCandidate): string {
    const userData = candidate.item.UserData;
    if (isRecord(userData) && typeof userData.LastPlayedDate === "string") return userData.LastPlayedDate;
    return typeof candidate.item.DatePlayed === "string" ? candidate.item.DatePlayed : "";
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
      upsertLiveItem({ serverId: serverIdValue, libraryId: libraryIdValue, priority: 0 }, item as unknown as Record<string, unknown>);
    }
  }

  function upsertLiveItem(source: LiveSource, item: Record<string, unknown>): void {
    if (!source.libraryId || typeof item.Id !== "string") return;
    store.upsertIndexedItem({
      serverId: source.serverId,
      itemId: item.Id,
      libraryId: source.libraryId,
      itemType: typeof item.Type === "string" ? item.Type : "Unknown",
      logicalKey: logicalItemKey(item as unknown as SourceItem, source.serverId),
      json: item
    });
  }

  async function ensureRelatedLiveItems(client: AppUpstreamClient, cacheVersion: number, userName: string, source: LiveSource, item: Record<string, unknown>): Promise<void> {
    if (!source.libraryId) return;
    for (const itemId of relatedLiveItemIds(source, item)) {
      if (store.findIndexedItemsBySourceId(itemId).some((candidate) => candidate.serverId === source.serverId)) continue;
      try {
        const related = await client.json<Record<string, unknown>>(source.serverId, `/Items/${itemId}`, {
          query: await liveQueryForSource(client, cacheVersion, userName, {}, source)
        });
        upsertLiveItem(source, related);
      } catch (error) {
        if (!isIgnorableLiveSourceError(error)) throw error;
      }
    }
  }

  function relatedLiveItemIds(source: LiveSource, item: Record<string, unknown>): string[] {
    const skipped = new Set([item.Id, source.libraryId, source.bridgeLibraryId].filter((value): value is string => typeof value === "string"));
    const ids: string[] = [];
    for (const field of ITEM_ID_FIELDS) {
      if (field === "Id") continue;
      const value = item[field];
      if (typeof value !== "string" || skipped.has(value) || ids.includes(value)) continue;
      ids.push(value);
    }
    return ids;
  }

  async function refreshLiveViewsForRequest(snapshotConfig: BridgeConfig, client: AppUpstreamClient, userName: string): Promise<void> {
    const upstreamConfigs = snapshotConfig.upstreams;
    for (const upstreamConfig of upstreamConfigs) {
      try {
        const upstreamUserId = await liveUserId(client, configVersion, upstreamConfig.id, userName);
        if (!upstreamUserId) continue;
        const response = await client.json<{ Items?: Array<{ Id: string; Name?: string; CollectionType?: string }> }>(upstreamConfig.id, "/UserViews", {
          query: { UserId: upstreamUserId }
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
        itemIdMap: bridgeItemIdMapForSourceItem(snapshotConfig, store, source.serverId, sources, itemIdValue, upstreamItem),
        mediaSourceIdMap: bridgeMediaSourceIdMapForSources(source.serverId, itemIdValue, source.json.MediaSources, upstreamItem.MediaSources),
        rewriteUnknownItemIds: false
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

  async function getPlaybackInfo(
    params: { itemId: string },
    query: unknown,
    body: unknown,
    userIdValue: string,
    userName: string,
    accessToken: string,
    request?: FastifyRequest
  ): Promise<Record<string, unknown>> {
    const snapshotConfig = config;
    const client = upstream;
    const bridgeServerIdValue = serverId;
    const sources = bridgeItemSources(snapshotConfig, store, params.itemId);
    const requestedMediaSourceId = mediaSourceIdFrom(query) ?? mediaSourceIdFrom(body);
    const mediaSourceMapping = requestedMediaSourceId ? resolveMediaSourceMapping(requestedMediaSourceId, params.itemId) : undefined;
    if (sources.length === 0) {
      const live = await getLivePlaybackInfo(userName, params.itemId, query, body, accessToken, mediaSourceMapping, request).catch(() => undefined);
      if (live) return live;
    }
    const source = mediaSourceMapping
      ? sources.find((candidate) => candidate.serverId === mediaSourceMapping.serverId && candidate.itemId === mediaSourceMapping.upstreamItemId)
      : sources[0];
    if (!source) {
      throw Object.assign(new Error("Item not found"), { statusCode: 404 });
    }
    const sourceContext = { serverId: source.serverId, priority: 0 };
    const baseQuery = await liveQueryForSource(client, configVersion, userName, query, sourceContext);
    const baseBody = await liveBodyForSource(client, configVersion, userName, body, sourceContext);
    const upstreamQuery = mediaSourceMapping ? rewriteMediaSourceId(baseQuery, mediaSourceMapping.upstreamMediaSourceId) : baseQuery;
    const upstreamBody = mediaSourceMapping ? rewriteMediaSourceId(baseBody, mediaSourceMapping.upstreamMediaSourceId) : baseBody;
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
      itemIdMap: bridgeItemIdMapForSourceItem(snapshotConfig, store, source.serverId, sources, params.itemId, response),
      itemId: params.itemId,
      rewriteUnknownItemIds: false
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
    rewritePlaybackUrls(rewritten, params.itemId, response.MediaSources, rewritten.MediaSources, response.PlaySessionId, rewritten.PlaySessionId, accessToken);
    const bridgeItem = getBridgeItem(snapshotConfig, store, userIdValue, params.itemId);
    if (bridgeItem) {
      rewritten.UserData = bridgeItem.UserData;
    }
    return rewritten;
  }

  async function getLivePlaybackInfo(
    userName: string,
    itemIdValue: string,
    query: unknown,
    body: unknown,
    accessToken: string,
    mediaSourceMapping?: MediaSourceMapping,
    request?: FastifyRequest
  ): Promise<Record<string, unknown> | undefined> {
    if (!liveRouteAggregation) return undefined;
    const client = upstream;
    const bridgeServerIdValue = serverId;
    const cacheVersion = configVersion;
    const sources = mediaSourceMapping
      ? liveUpstreamSources().filter((source) => source.serverId === mediaSourceMapping.serverId)
      : liveUpstreamSources();
    const upstreamItemId = mediaSourceMapping?.upstreamItemId ?? itemIdValue;
    logUpstreamFanout(request, `/Items/${itemIdValue}/PlaybackInfo`, `/Items/${upstreamItemId}/PlaybackInfo`, sources);
    for (const source of sources) {
      const baseQuery = await liveQueryForSource(client, cacheVersion, userName, query, source);
      const baseBody = await liveBodyForSource(client, cacheVersion, userName, body, source);
      const upstreamQuery = mediaSourceMapping ? rewriteMediaSourceId(baseQuery, mediaSourceMapping.upstreamMediaSourceId) : baseQuery;
      const upstreamBody = mediaSourceMapping ? rewriteMediaSourceId(baseBody, mediaSourceMapping.upstreamMediaSourceId) : baseBody;
      const upstreamPath = `/Items/${upstreamItemId}/PlaybackInfo`;
      try {
        logUpstreamJson(request, `/Items/${itemIdValue}/PlaybackInfo`, source, upstreamPath);
        const response = await client.json<Record<string, unknown>>(source.serverId, upstreamPath, {
          method: upstreamBody === undefined ? "GET" : "POST",
          query: upstreamQuery,
          body: upstreamBody
        });
        const rewritten = rewriteDto(response, {
          serverId: source.serverId,
          bridgeServerId: bridgeServerIdValue,
          itemIdMap: new Map([[itemIdValue, itemIdValue]]),
          itemId: itemIdValue,
          rewriteUnknownItemIds: false
        }) as Record<string, unknown>;
        const originalSources = Array.isArray(response.MediaSources) ? response.MediaSources : [];
        const rewrittenSources = Array.isArray(rewritten.MediaSources) ? rewritten.MediaSources : [];
        recordMediaSourceMappings(itemIdValue, source.serverId, upstreamItemId, originalSources, rewrittenSources);
        if (typeof response.PlaySessionId === "string") {
          const mapping = store.createPlaybackSessionMapping({
            serverId: source.serverId,
            upstreamPlaySessionId: response.PlaySessionId,
            upstreamItemId,
            bridgeItemId: itemIdValue
          });
          rewritten.PlaySessionId = mapping.bridgePlaySessionId;
        }
        rewritePlaybackUrls(rewritten, itemIdValue, response.MediaSources, rewritten.MediaSources, response.PlaySessionId, rewritten.PlaySessionId, accessToken);
        return rewritten;
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
    const rewritten = { ...(value as Record<string, unknown>) };
    delete rewritten.mediaSourceId;
    rewritten.MediaSourceId = mediaSourceId;
    return rewritten;
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

  function rewritePlaybackUrls(
    value: unknown,
    bridgeItemIdValue: string,
    originalSources: unknown,
    rewrittenSources: unknown,
    upstreamPlaySessionId: unknown,
    bridgePlaySessionId: unknown,
    accessToken: string
  ): void {
    const mediaSourceIds = mediaSourceIdMap(originalSources, rewrittenSources);
    const playSessionIds = typeof upstreamPlaySessionId === "string" && typeof bridgePlaySessionId === "string"
      ? new Map([[upstreamPlaySessionId, bridgePlaySessionId]])
      : new Map<string, string>();
    rewritePlaybackUrlsInPlace(value, bridgeItemIdValue, mediaSourceIds, playSessionIds, accessToken);
  }

  function mediaSourceIdMap(originalSources: unknown, rewrittenSources: unknown): Map<string, string> {
    const map = new Map<string, string>();
    const originals = Array.isArray(originalSources) ? originalSources : [];
    const rewritten = Array.isArray(rewrittenSources) ? rewrittenSources : [];
    for (let index = 0; index < originals.length; index += 1) {
      const original = originals[index] as Record<string, unknown>;
      const rewrittenSource = rewritten[index] as Record<string, unknown> | undefined;
      if (typeof original.Id === "string" && typeof rewrittenSource?.Id === "string") {
        map.set(original.Id, rewrittenSource.Id);
      }
    }
    return map;
  }

  function bridgeMediaSourceIdMapForSources(serverIdValue: string, bridgeItemIdValue: string, ...sourceLists: unknown[]): Map<string, string> {
    const map = new Map<string, string>();
    for (const sourceList of sourceLists) {
      if (!Array.isArray(sourceList)) continue;
      for (const source of sourceList) {
        if (!source || typeof source !== "object") continue;
        const id = (source as Record<string, unknown>).Id;
        if (typeof id === "string") {
          map.set(id, bridgeMediaSourceId(serverIdValue, bridgeItemIdValue, id));
        }
      }
    }
    return map;
  }

  function rewritePlaybackUrlsInPlace(
    value: unknown,
    bridgeItemIdValue: string,
    mediaSourceIds: Map<string, string>,
    playSessionIds: Map<string, string>,
    accessToken: string
  ): void {
    if (Array.isArray(value)) {
      for (const item of value) rewritePlaybackUrlsInPlace(item, bridgeItemIdValue, mediaSourceIds, playSessionIds, accessToken);
      return;
    }
    if (!isRecord(value)) return;
    for (const [key, child] of Object.entries(value)) {
      if ((key === "TranscodingUrl" || key === "DeliveryUrl") && typeof child === "string") {
        value[key] = rewritePlaybackUrl(child, bridgeItemIdValue, mediaSourceIds, playSessionIds, accessToken);
      } else if (child && typeof child === "object") {
        rewritePlaybackUrlsInPlace(child, bridgeItemIdValue, mediaSourceIds, playSessionIds, accessToken);
      }
    }
  }

  function rewritePlaybackUrl(
    value: string,
    bridgeItemIdValue: string,
    mediaSourceIds: Map<string, string>,
    playSessionIds: Map<string, string>,
    accessToken: string
  ): string {
    let url: URL;
    try {
      url = new URL(value, "http://bridge.local");
    } catch {
      return value;
    }
    const mediaSourceRouteMatch = /^\/videos\/([^/?#]+)\/([^/?#]+)\/(subtitles|attachments)\/(.+)$/i.exec(url.pathname);
    if (mediaSourceRouteMatch) {
      const bridgeMediaSourceId = mediaSourceIds.get(mediaSourceRouteMatch[2]) ?? mediaSourceRouteMatch[2];
      const route = mediaSourceRouteMatch[3].toLowerCase() === "attachments" ? "Attachments" : "Subtitles";
      url.pathname = `/Videos/${bridgeItemIdValue}/${bridgeMediaSourceId}/${route}/${mediaSourceRouteMatch[4]}`;
      rewritePlaybackUrlQuery(url, mediaSourceIds, playSessionIds, accessToken);
      return `${url.pathname}${url.search}${url.hash}`;
    }
    const match = /^\/(videos|audio)\/([^/?#]+)\/(.+)$/i.exec(url.pathname);
    if (!match) return value;
    const kind = match[1].toLowerCase() === "audio" ? "Audio" : "Videos";
    url.pathname = `/${kind}/${bridgeItemIdValue}/${match[3]}`;
    rewritePlaybackUrlQuery(url, mediaSourceIds, playSessionIds, accessToken);
    return `${url.pathname}${url.search}${url.hash}`;
  }

  function rewritePlaybackUrlQuery(
    url: URL,
    mediaSourceIds: Map<string, string>,
    playSessionIds: Map<string, string>,
    accessToken: string
  ): void {
    rewriteQueryId(url.searchParams, "MediaSourceId", mediaSourceIds);
    rewriteQueryId(url.searchParams, "mediaSourceId", mediaSourceIds);
    rewriteQueryId(url.searchParams, "PlaySessionId", playSessionIds);
    url.searchParams.delete("api_key");
    url.searchParams.set("ApiKey", accessToken);
  }

  function rewriteQueryId(params: URLSearchParams, name: string, ids: Map<string, string>): void {
    const value = params.get(name);
    const rewritten = value ? ids.get(value) : undefined;
    if (rewritten) params.set(name, rewritten);
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

  function rewritePlaybackProxyQuery(query: Record<string, string | undefined>, mapping?: MediaSourceMapping): Record<string, string | undefined> {
    const rewritten = { ...query };
    const mediaSourceId = rewritten.MediaSourceId ?? rewritten.mediaSourceId;
    const itemId = rewritten.ItemId ?? rewritten.itemId;
    delete rewritten.ApiKey;
    delete rewritten.api_key;
    delete rewritten.AccessToken;
    delete rewritten.accessToken;
    delete rewritten.mediaSourceId;
    delete rewritten.itemId;
    if (mapping) {
      rewritten.MediaSourceId = mapping.upstreamMediaSourceId;
      if (itemId) rewritten.ItemId = mapping.upstreamItemId;
    } else if (mediaSourceId) {
      rewritten.MediaSourceId = mediaSourceId;
      if (itemId) rewritten.ItemId = itemId;
    }
    const playbackSession = rewritten.PlaySessionId ? store.findPlaybackSessionMapping(rewritten.PlaySessionId) : undefined;
    if (playbackSession) {
      rewritten.PlaySessionId = playbackSession.upstreamPlaySessionId;
    }
    return rewritten;
  }

  function hlsQueryRewrites(query: Record<string, string | undefined>, mapping?: MediaSourceMapping): Array<{ names: string[]; ids: Map<string, string> }> {
    const rewrites: Array<{ names: string[]; ids: Map<string, string> }> = [];
    if (mapping) {
      rewrites.push({
        names: ["MediaSourceId", "mediaSourceId"],
        ids: new Map([[mapping.upstreamMediaSourceId, mapping.bridgeMediaSourceId]])
      });
    }
    const playbackSession = query.PlaySessionId ? store.findPlaybackSessionMapping(query.PlaySessionId) : undefined;
    if (playbackSession) {
      rewrites.push({
        names: ["PlaySessionId"],
        ids: new Map([[playbackSession.upstreamPlaySessionId, playbackSession.bridgePlaySessionId]])
      });
    }
    return rewrites;
  }

  function hlsPathSegmentRewrites(mapping?: MediaSourceMapping): Map<string, string> | undefined {
    return mapping ? new Map([[mapping.upstreamMediaSourceId, mapping.bridgeMediaSourceId]]) : undefined;
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

  function infuseSyncCheckpointFromRequest(request: FastifyRequest, reply: FastifyReply): InfuseSyncCheckpointRecord | undefined {
    const { checkpointId } = request.params as { checkpointId: string };
    const checkpoint = store.getInfuseSyncCheckpoint(checkpointId);
    if (!checkpoint) {
      notFound(reply, "InfuseSync checkpoint not found");
      return undefined;
    }
    return checkpoint;
  }

  function infuseSyncStats(checkpoint: InfuseSyncCheckpointRecord & { syncTimestamp: string }): Record<string, number> {
    const changedItems = store.listIndexedItemsUpdatedBetween(checkpoint.fromTimestamp, checkpoint.syncTimestamp);
    const videoItemTypes = "Movie,Episode,Video,MusicVideo";
    const updatedCount = (includeItemTypes: string): number =>
      countBridgeItemsFromIndexedItems(config, store, changedItems, includeItemTypes);
    return {
      UpdatedFolders: updatedCount("Folder"),
      RemovedFolders: 0,
      UpdatedBoxSets: updatedCount("BoxSet"),
      RemovedBoxSets: 0,
      UpdatedPlaylists: updatedCount("Playlist"),
      RemovedPlaylists: 0,
      UpdatedTvShows: updatedCount("Series"),
      RemovedTvShows: 0,
      UpdatedSeasons: updatedCount("Season"),
      RemovedSeasons: 0,
      UpdatedVideos: updatedCount(videoItemTypes),
      RemovedVideos: 0,
      UpdatedCollectionFolders: updatedCount("CollectionFolder"),
      UpdatedUserData: filterInfuseSyncUserDataRows(
        store.listUserDataUpdatedBetween(checkpoint.userId, checkpoint.fromTimestamp, checkpoint.syncTimestamp),
        includeItemTypeList(videoItemTypes)
      ).length
    };
  }

  function filterInfuseSyncUserDataRows(rows: UserDataRecord[], itemTypes: string[]): UserDataRecord[] {
    const includeTypes = new Set(itemTypes.map((type) => type.trim().toLowerCase()).filter(Boolean));
    if (includeTypes.size === 0) return rows;
    const sourcesByBridgeId = store.findIndexedItemsByBridgeIds(rows.map((row) => row.itemId));
    return rows.filter((row) => {
      const selected = sortIndexedSourcesByPriority(sourcesByBridgeId.get(row.itemId) ?? [])[0];
      const type = String(selected?.json.Type ?? selected?.itemType ?? "").toLowerCase();
      return includeTypes.has(type);
    });
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
    const query = latestFallbackQuery(snapshotConfig, rawQuery);
    const limit = ((rawQuery ?? {}) as { Limit?: string; limit?: string }).Limit ?? ((rawQuery ?? {}) as { Limit?: string; limit?: string }).limit;
    const items = listBridgeItems(snapshotConfig, store, userIdValue, {
      ...query,
      sortBy: "DateCreated",
      sortOrder: "Descending",
      limit: undefined
    });
    return groupLatestDtos(items, userIdValue, snapshotConfig).slice(0, limit === undefined ? 20 : Number(limit));
  }

  function latestFallbackQuery(snapshotConfig: BridgeConfig, rawQuery: unknown): BrowseQuery {
    const query = browseQuery(rawQuery);
    if (query.includeItemTypes) return query;
    const parentId = query.parentId;
    const configuredLibrary = parentId
      ? snapshotConfig.libraries.find((library) => bridgeLibraryId(library.id) === parentId)
      : undefined;
    const passThroughLibrary = parentId
      ? store.listUpstreamLibraries().find((library) => passThroughLibraryId(library.serverId, library.libraryId) === parentId)
      : undefined;
    const includeItemTypes = latestIncludeItemTypesForCollection(configuredLibrary?.collectionType ?? passThroughLibrary?.collectionType);
    if (includeItemTypes) return { ...query, includeItemTypes };
    return parentId ? query : { ...query, filters: appendFilter(query.filters, "IsNotFolder") };
  }

  function appendFilter(filters: string | undefined, filter: string): string {
    const values = filters?.split(",").map((value) => value.trim()).filter(Boolean) ?? [];
    return values.some((value) => value.toLowerCase() === filter.toLowerCase())
      ? values.join(",")
      : [...values, filter].join(",");
  }

  function pagedQueryResult(items: Record<string, unknown>[], query: Pick<BrowseQuery, "startIndex" | "limit">): Record<string, unknown> {
    const start = query.startIndex ?? 0;
    return queryResult(items.slice(start, query.limit === undefined ? undefined : start + query.limit), start, items.length);
  }

  function canWriteUserData(userIdValue: string, itemIdValue: string): boolean {
    return Boolean(getBridgeItem(config, store, userIdValue, itemIdValue))
      || viewDtos().some((view) => view.Id === itemIdValue);
  }

  function resumeItems(snapshotConfig: BridgeConfig, userIdValue: string, rawQuery: unknown): Record<string, unknown> {
    const query = browseQuery(rawQuery);
    const unpaged = listBridgeItems(snapshotConfig, store, userIdValue, {
      ...query,
      recursive: true,
      startIndex: undefined,
      limit: undefined
    })
      .filter((item) => Number((item.UserData as Record<string, unknown> | undefined)?.PlaybackPositionTicks ?? 0) > 0)
      .sort(compareResumeItemsDescending);
    const start = query.startIndex ?? 0;
    const items = unpaged.slice(start, query.limit === undefined ? undefined : start + query.limit);
    const total = unpaged.length;
    return queryResult(items, query.startIndex ?? 0, total);
  }

  function compareResumeItemsDescending(left: Record<string, unknown>, right: Record<string, unknown>): number {
    return resumeItemDate(right).localeCompare(resumeItemDate(left))
      || String(left.SortName ?? left.Name ?? "").localeCompare(String(right.SortName ?? right.Name ?? ""));
  }

  function resumeItemDate(item: Record<string, unknown>): string {
    const userData = item.UserData;
    if (isRecord(userData) && typeof userData.LastPlayedDate === "string") return userData.LastPlayedDate;
    return typeof item.DatePlayed === "string" ? item.DatePlayed : "";
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

  function resolveWatchedWriteTarget(itemIdValue: string): WatchedWriteTarget | undefined {
    const bridgeSources = bridgeItemSources(config, store, itemIdValue);
    if (bridgeSources.length > 0) {
      return { bridgeItemId: itemIdValue, sources: bridgeSources };
    }

    const directSources = store.findIndexedItemsBySourceId(itemIdValue);
    const logicalKeys = Array.from(new Set(directSources.map((source) => source.logicalKey)));
    if (logicalKeys.length !== 1) return undefined;
    const resolvedBridgeItemId = bridgeItemId(logicalKeys[0]);
    const sources = bridgeItemSources(config, store, resolvedBridgeItemId);
    return sources.length > 0 ? { bridgeItemId: resolvedBridgeItemId, sources } : undefined;
  }

  async function forwardPlayedState(sources: IndexedItemRecord[], userName: string, played: boolean, datePlayed?: string): Promise<void> {
    const userIds = new Map<string, string>();
    for (const source of sources) {
      const upstreamUserId = await exactUpstreamUserIdForWrite(source.serverId, userName, userIds);
      try {
        await upstream.json<Record<string, unknown>>(source.serverId, `/UserPlayedItems/${source.itemId}`, {
          method: played ? "POST" : "DELETE",
          query: played ? { userId: upstreamUserId, datePlayed } : { userId: upstreamUserId }
        });
      } catch (error) {
        const detail = error instanceof Error ? error.message : String(error);
        throw badGatewayError(`Upstream watched state write failed: ${detail}`);
      }
    }
  }

  async function forwardWatchedUserData(sources: IndexedItemRecord[], userName: string, payload: WatchedUserDataPayload): Promise<void> {
    const userIds = new Map<string, string>();
    for (const source of sources) {
      const upstreamUserId = await exactUpstreamUserIdForWrite(source.serverId, userName, userIds);
      try {
        await upstream.json<Record<string, unknown>>(source.serverId, `/UserItems/${source.itemId}/UserData`, {
          method: "POST",
          query: { userId: upstreamUserId },
          body: payload
        });
      } catch (error) {
        const detail = error instanceof Error ? error.message : String(error);
        throw badGatewayError(`Upstream watched user data write failed: ${detail}`);
      }
    }
  }

  async function exactUpstreamUserIdForWrite(serverIdValue: string, userName: string, cache: Map<string, string>): Promise<string> {
    const cached = cache.get(serverIdValue);
    if (cached) return cached;
    let users: unknown;
    try {
      users = await upstream.json<unknown>(serverIdValue, "/Users", {});
    } catch (error) {
      const detail = error instanceof Error ? error.message : String(error);
      throw badGatewayError(`Upstream user lookup failed: ${detail}`);
    }
    if (!Array.isArray(users)) {
      throw badGatewayError(`Upstream user lookup failed: /Users on ${serverIdValue} did not return a user list`);
    }
    const upstreamUserId = users.find((user) => {
      if (!isRecord(user)) return false;
      return typeof user.Id === "string" && typeof user.Name === "string" && user.Name.toLowerCase() === userName.toLowerCase();
    })?.Id;
    if (!upstreamUserId) {
      throw badGatewayError(`Upstream user ${userName} was not found on ${serverIdValue}`);
    }
    cache.set(serverIdValue, upstreamUserId);
    return upstreamUserId;
  }

  function setPlayed(userIdValue: string, itemId: string, played: boolean, datePlayed?: string): Record<string, unknown> {
    if (!played) {
      store.upsertUserData(userIdValue, itemId, { played: false, playbackPositionTicks: 0, playCount: 0, lastPlayedDate: null });
      return userDataDto(store, userIdValue, itemId);
    }
    const existing = store.getUserData(userIdValue, itemId);
    const existingPlayCount = Number(existing.PlayCount ?? 0);
    const existingLastPlayedDate = typeof existing.LastPlayedDate === "string" ? existing.LastPlayedDate : undefined;
    store.upsertUserData(userIdValue, itemId, {
      played: true,
      playbackPositionTicks: 0,
      playCount: Math.max(existingPlayCount + (datePlayed ? 1 : 0), 1),
      lastPlayedDate: datePlayed ?? existingLastPlayedDate ?? new Date().toISOString()
    });
    return userDataDto(store, userIdValue, itemId);
  }

  function applyPlaybackStartUserData(userIdValue: string, body: unknown): void {
    if (!isRecord(body) || typeof body.ItemId !== "string") return;
    const item = playbackReportItem(userIdValue, body.ItemId);
    if (!item) return;
    const existing = store.getUserData(userIdValue, item.bridgeItemId);
    store.upsertUserData(userIdValue, item.bridgeItemId, {
      played: supportsPlayedStatus(item.dto) && !supportsPositionTicksResume(item.dto),
      playCount: Number(existing.PlayCount ?? 0) + 1,
      lastPlayedDate: new Date().toISOString()
    });
  }

  function applyPlaybackReportUserData(userIdValue: string, body: unknown, markCompleteWhenMissingPosition: boolean): void {
    if (!isRecord(body) || typeof body.ItemId !== "string") return;
    const item = playbackReportItem(userIdValue, body.ItemId);
    if (!item) return;
    const positionTicks = typeof body.PositionTicks === "number" ? body.PositionTicks : undefined;
    if (positionTicks === undefined && !markCompleteWhenMissingPosition) return;
    const patch = playbackUserDataPatch(item.dto, positionTicks);
    if (positionTicks === undefined) {
      const existing = store.getUserData(userIdValue, item.bridgeItemId);
      patch.playCount = Number(existing.PlayCount ?? 0) + 1;
    }
    store.upsertUserData(userIdValue, item.bridgeItemId, patch);
  }

  function playbackReportItem(userIdValue: string, itemIdValue: string): { bridgeItemId: string; dto: Record<string, unknown> } | undefined {
    const bridgeItem = getBridgeItem(config, store, userIdValue, itemIdValue);
    if (bridgeItem) return { bridgeItemId: itemIdValue, dto: bridgeItem };

    const source = sortIndexedSourcesByPriority(store.findIndexedItemsBySourceId(itemIdValue))[0];
    if (!source) return undefined;
    const id = bridgeItemId(source.logicalKey);
    return { bridgeItemId: id, dto: getBridgeItem(config, store, userIdValue, id) ?? source.json };
  }

  function playbackUserDataPatch(item: Record<string, unknown>, reportedPositionTicks: number | undefined): UserDataPatch {
    const runtimeTicks = typeof item.RunTimeTicks === "number" ? item.RunTimeTicks : 0;
    let positionTicks = reportedPositionTicks ?? runtimeTicks;
    let played: boolean | undefined;

    if (positionTicks > 0 && runtimeTicks > 0 && !isBookType(item)) {
      const percentIn = (positionTicks / runtimeTicks) * 100;
      if (percentIn < 5) {
        positionTicks = 0;
      } else if (percentIn > 90 || positionTicks >= runtimeTicks - 10_000_000) {
        positionTicks = 0;
        played = true;
      } else if (runtimeTicks < 3_000_000_000) {
        positionTicks = 0;
        played = true;
      }
    } else if (!runtimeTicks) {
      positionTicks = 0;
      played = true;
    }

    if (!supportsPlayedStatus(item)) {
      positionTicks = 0;
      played = false;
    }
    if (!supportsPositionTicksResume(item)) {
      positionTicks = 0;
    }

    return { played, playbackPositionTicks: positionTicks };
  }

  function isBookType(item: Record<string, unknown>): boolean {
    const type = String(item.Type ?? "").toLowerCase();
    return type === "book" || type === "audiobook";
  }

  function supportsPlayedStatus(item: Record<string, unknown>): boolean {
    const type = String(item.Type ?? "").toLowerCase();
    return ["audio", "audiobook", "book", "episode", "folder", "movie", "musicvideo", "video"].includes(type);
  }

  function supportsPositionTicksResume(item: Record<string, unknown>): boolean {
    const type = String(item.Type ?? "").toLowerCase();
    return ["audiobook", "book", "episode", "movie", "musicvideo", "video"].includes(type);
  }

  function deleteSourceForItem(itemIdValue: string): IndexedItemRecord | undefined {
    const bridgeSources = bridgeItemSources(config, store, itemIdValue);
    if (bridgeSources.length > 0) return bridgeSources[0];
    return sortIndexedSourcesByPriority(store.findIndexedItemsBySourceId(itemIdValue))[0];
  }

  async function deletePermissionForSourceItem(client: AppUpstreamClient, userName: string, source: IndexedItemRecord): Promise<"allowed" | "denied" | "missing"> {
    const upstreamUserId = await liveUserId(client, configVersion, source.serverId, userName);
    if (!upstreamUserId) return "denied";
    try {
      const item = await client.json<Record<string, unknown>>(source.serverId, `/Items/${source.itemId}`, {
        query: { UserId: upstreamUserId, Fields: "CanDelete" }
      });
      return item.CanDelete === true ? "allowed" : "denied";
    } catch (error) {
      if (isMissingUpstreamError(error)) return "missing";
      const detail = error instanceof Error ? error.message : String(error);
      throw badGatewayError(`Upstream delete permission check failed: ${detail}`);
    }
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
        query: rewritePlaybackProxyQuery(query),
        headers: copyProxyRequestHeaders(request.headers)
      });
      await sendProxyBody(reply, response);
      return;
    }
    if (!mapping) {
      notFound(reply, "Media source mapping not found");
      return;
    }

    const upstreamQuery = rewritePlaybackProxyQuery(query, mapping);
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
    const auth = requireSession(request, config, store);
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
        query: rewritePlaybackProxyQuery(query),
        headers: copyProxyRequestHeaders(request.headers)
      });
      const body = await responseBodyText(response.body);
      const rewritten = rewriteHlsPlaylist(body, {
        bridgeBasePath: `/${kind}/${params.itemId}`,
        upstreamBasePath: `/${kind}/${params.itemId}`,
        authToken: auth.session.accessToken,
        queryRewrites: hlsQueryRewrites(query)
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

    const upstreamQuery = rewritePlaybackProxyQuery(query, mapping);
    const response = await proxyRaw(client, request, mapping.serverId, `/${kind}/${mapping.upstreamItemId}/${params.playlist}.m3u8`, {
      method: request.method,
      query: upstreamQuery,
      headers: copyProxyRequestHeaders(request.headers)
    });
    const body = await responseBodyText(response.body);
    const rewritten = rewriteHlsPlaylist(body, {
      bridgeBasePath: `/${kind}/${params.itemId}`,
      upstreamBasePath: `/${kind}/${mapping.upstreamItemId}`,
      authToken: auth.session.accessToken,
      pathSegmentRewrites: hlsPathSegmentRewrites(mapping),
      queryRewrites: hlsQueryRewrites(query, mapping)
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
    const params = request.params as {
      itemId: string;
      imageType: string;
      imageIndex?: string;
      tag?: string;
      format?: string;
      maxWidth?: string;
      maxHeight?: string;
      percentPlayed?: string;
      unplayedCount?: string;
    };
    const directImagePath = itemImagePath(params);
    if (!directImagePath) {
      notFound(reply, "Image not found");
      return;
    }
    const sources = bridgeItemSources(config, store, params.itemId);
    if (sources.length > 0) {
      const candidates = sources
        .map((source) => {
          const path = itemImagePath({ ...params, itemId: source.itemId });
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

  async function getItemImageInfos(request: FastifyRequest, reply: FastifyReply): Promise<Record<string, unknown>[] | void> {
    requireSession(request, config, store);
    const client = upstream;
    const params = request.params as { itemId: string };
    const sources = bridgeItemSources(config, store, params.itemId);
    for (const source of sources) {
      try {
        return await client.json<Record<string, unknown>[]>(source.serverId, `/Items/${source.itemId}/Images`, {
          query: request.query
        });
      } catch (error) {
        if (!isIgnorableLiveSourceError(error)) {
          const detail = error instanceof Error ? error.message : String(error);
          throw badGatewayError(`Upstream image metadata failed: ${detail}`);
        }
      }
    }
    if (liveRouteAggregation) {
      for (const source of liveUpstreamSources()) {
        try {
          return await client.json<Record<string, unknown>[]>(source.serverId, `/Items/${params.itemId}/Images`, {
            query: request.query
          });
        } catch (error) {
          if (!isIgnorableLiveSourceError(error)) {
            const detail = error instanceof Error ? error.message : String(error);
            throw badGatewayError(`Upstream image metadata failed: ${detail}`);
          }
        }
      }
    }
    notFound(reply, "Item not found");
  }

  async function proxyHlsSegment(kind: "Videos" | "Audio", upstreamRoute: "hls" | "hls1", request: FastifyRequest, reply: FastifyReply): Promise<void> {
    requireSession(request, config, store);
    const client = upstream;
    if (!client.raw) {
      unsupported(reply, "The configured upstream client does not support raw proxying");
      return;
    }
    const params = request.params as { itemId: string; mediaSourceId?: string; playlistId: string; segmentId: string; container: string };
    const query = request.query as Record<string, string | undefined>;
    const mediaSourceId = params.mediaSourceId ?? query.MediaSourceId ?? query.mediaSourceId;
    if (!mediaSourceId) {
      notFound(reply, "MediaSourceId is required");
      return;
    }
    const upstreamPathFor = (itemId: string) => `/${kind}/${itemId}/${upstreamRoute}/${params.playlistId}/${params.segmentId}.${params.container}`;
    const mapping = resolveMediaSourceMapping(mediaSourceId, params.itemId);
    if (!mapping && liveRouteAggregation) {
      const response = await proxyRawFirst(client, request, `${kind.toLowerCase()} hls segment`, liveUpstreamSources(), upstreamPathFor(params.itemId), {
        method: request.method,
        query: rewritePlaybackProxyQuery(query),
        headers: copyProxyRequestHeaders(request.headers)
      });
      await sendProxyBody(reply, response);
      return;
    }
    if (!mapping) {
      notFound(reply, "Media source mapping not found");
      return;
    }
    const response = await proxyRaw(client, request, mapping.serverId, upstreamPathFor(mapping.upstreamItemId), {
      method: request.method,
      query: rewritePlaybackProxyQuery(query, mapping),
      headers: copyProxyRequestHeaders(request.headers)
    });
    await sendProxyBody(reply, response);
  }

  async function proxyLegacyHlsPlaylist(request: FastifyRequest, reply: FastifyReply): Promise<void> {
    const auth = requireSession(request, config, store);
    const client = upstream;
    if (!client.raw) {
      unsupported(reply, "The configured upstream client does not support raw proxying");
      return;
    }
    const params = request.params as { itemId: string; playlistId: string };
    const query = request.query as Record<string, string | undefined>;
    const mediaSourceId = query.MediaSourceId ?? query.mediaSourceId;
    const mapping = mediaSourceId ? resolveMediaSourceMapping(mediaSourceId, params.itemId) : undefined;
    const source = mapping ?? bridgeItemSources(config, store, params.itemId)[0];
    if (!source && liveRouteAggregation) {
      const response = await proxyRawFirst(client, request, "legacy hls playlist", liveUpstreamSources(), `/Videos/${params.itemId}/hls/${params.playlistId}/stream.m3u8`, {
        method: request.method,
        query: rewritePlaybackProxyQuery(query),
        headers: copyProxyRequestHeaders(request.headers)
      });
      const body = await responseBodyText(response.body);
      const rewritten = rewriteHlsPlaylist(body, {
        bridgeBasePath: `/Videos/${params.itemId}/hls/${params.playlistId}`,
        upstreamBasePath: `/Videos/${params.itemId}/hls/${params.playlistId}`,
        authToken: auth.session.accessToken,
        queryRewrites: hlsQueryRewrites(query)
      });
      reply.code(response.statusCode);
      copyProxyResponseHeaders(reply, response.headers);
      reply.header("content-length", Buffer.byteLength(rewritten).toString());
      reply.send(rewritten);
      return;
    }
    if (!source) {
      notFound(reply, "Item not found");
      return;
    }

    const serverIdValue = "upstreamMediaSourceId" in source ? source.serverId : source.serverId;
    const upstreamItemId = "upstreamMediaSourceId" in source ? source.upstreamItemId : source.itemId;
    const response = await proxyRaw(client, request, serverIdValue, `/Videos/${upstreamItemId}/hls/${params.playlistId}/stream.m3u8`, {
      method: request.method,
      query: rewritePlaybackProxyQuery(query, mapping),
      headers: copyProxyRequestHeaders(request.headers)
    });
    const body = await responseBodyText(response.body);
    const rewritten = rewriteHlsPlaylist(body, {
      bridgeBasePath: `/Videos/${params.itemId}/hls/${params.playlistId}`,
      upstreamBasePath: `/Videos/${upstreamItemId}/hls/${params.playlistId}`,
      authToken: auth.session.accessToken,
      queryRewrites: hlsQueryRewrites(query, mapping)
    });
    reply.code(response.statusCode);
    copyProxyResponseHeaders(reply, response.headers);
    reply.header("content-length", Buffer.byteLength(rewritten).toString());
    reply.send(rewritten);
  }

  async function proxyLegacyAudioHlsSegment(request: FastifyRequest, reply: FastifyReply): Promise<void> {
    requireSession(request, config, store);
    const client = upstream;
    if (!client.raw) {
      unsupported(reply, "The configured upstream client does not support raw proxying");
      return;
    }
    const params = request.params as { itemId: string; segmentId: string; container: string };
    const query = request.query as Record<string, string | undefined>;
    const mediaSourceId = query.MediaSourceId ?? query.mediaSourceId;
    if (!mediaSourceId) {
      notFound(reply, "MediaSourceId is required");
      return;
    }
    const mapping = resolveMediaSourceMapping(mediaSourceId, params.itemId);
    if (!mapping && liveRouteAggregation) {
      const response = await proxyRawFirst(client, request, "audio hls segment", liveUpstreamSources(), `/Audio/${params.itemId}/hls/${params.segmentId}/stream.${params.container}`, {
        method: request.method,
        query: rewritePlaybackProxyQuery(query),
        headers: copyProxyRequestHeaders(request.headers)
      });
      await sendProxyBody(reply, response);
      return;
    }
    if (!mapping) {
      notFound(reply, "Media source mapping not found");
      return;
    }
    const response = await proxyRaw(client, request, mapping.serverId, `/Audio/${mapping.upstreamItemId}/hls/${params.segmentId}/stream.${params.container}`, {
      method: request.method,
      query: rewritePlaybackProxyQuery(query, mapping),
      headers: copyProxyRequestHeaders(request.headers)
    });
    await sendProxyBody(reply, response);
  }

  async function proxyTrickplayPlaylist(request: FastifyRequest, reply: FastifyReply): Promise<void> {
    const auth = requireSession(request, config, store);
    const client = upstream;
    if (!client.raw) {
      unsupported(reply, "The configured upstream client does not support raw proxying");
      return;
    }
    const params = request.params as { itemId: string; width: string };
    const query = request.query as Record<string, string | undefined>;
    const mediaSourceId = query.MediaSourceId ?? query.mediaSourceId;
    const mapping = mediaSourceId ? resolveMediaSourceMapping(mediaSourceId, params.itemId) : undefined;
    if (!mapping && liveRouteAggregation) {
      const response = await proxyRawFirst(client, request, "trickplay playlist", liveUpstreamSources(), `/Videos/${params.itemId}/Trickplay/${params.width}/tiles.m3u8`, {
        method: request.method,
        query: rewritePlaybackProxyQuery(query),
        headers: copyProxyRequestHeaders(request.headers)
      });
      const body = await responseBodyText(response.body);
      const rewritten = rewriteHlsPlaylist(body, {
        bridgeBasePath: `/Videos/${params.itemId}/Trickplay/${params.width}`,
        upstreamBasePath: `/Videos/${params.itemId}/Trickplay/${params.width}`,
        authToken: auth.session.accessToken,
        queryRewrites: hlsQueryRewrites(query)
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
    const response = await proxyRaw(client, request, mapping.serverId, `/Videos/${mapping.upstreamItemId}/Trickplay/${params.width}/tiles.m3u8`, {
      method: request.method,
      query: rewritePlaybackProxyQuery(query, mapping),
      headers: copyProxyRequestHeaders(request.headers)
    });
    const body = await responseBodyText(response.body);
    const rewritten = rewriteHlsPlaylist(body, {
      bridgeBasePath: `/Videos/${params.itemId}/Trickplay/${params.width}`,
      upstreamBasePath: `/Videos/${mapping.upstreamItemId}/Trickplay/${params.width}`,
      authToken: auth.session.accessToken,
      queryRewrites: hlsQueryRewrites(query, mapping)
    });
    reply.code(response.statusCode);
    copyProxyResponseHeaders(reply, response.headers);
    reply.header("content-length", Buffer.byteLength(rewritten).toString());
    reply.send(rewritten);
  }

  async function proxyTrickplayTile(request: FastifyRequest, reply: FastifyReply): Promise<void> {
    requireSession(request, config, store);
    const client = upstream;
    if (!client.raw) {
      unsupported(reply, "The configured upstream client does not support raw proxying");
      return;
    }
    const params = request.params as { itemId: string; width: string; index: string };
    const query = request.query as Record<string, string | undefined>;
    const mediaSourceId = query.MediaSourceId ?? query.mediaSourceId;
    const mapping = mediaSourceId ? resolveMediaSourceMapping(mediaSourceId, params.itemId) : undefined;
    if (!mapping && liveRouteAggregation) {
      const response = await proxyRawFirst(client, request, "trickplay tile", liveUpstreamSources(), `/Videos/${params.itemId}/Trickplay/${params.width}/${params.index}.jpg`, {
        method: request.method,
        query: rewritePlaybackProxyQuery(query),
        headers: copyProxyRequestHeaders(request.headers)
      });
      await sendProxyBody(reply, response);
      return;
    }
    if (!mapping) {
      notFound(reply, "Media source mapping not found");
      return;
    }
    const response = await proxyRaw(client, request, mapping.serverId, `/Videos/${mapping.upstreamItemId}/Trickplay/${params.width}/${params.index}.jpg`, {
      method: request.method,
      query: rewritePlaybackProxyQuery(query, mapping),
      headers: copyProxyRequestHeaders(request.headers)
    });
    await sendProxyBody(reply, response);
  }

  async function proxySubtitlePlaylist(request: FastifyRequest, reply: FastifyReply): Promise<void> {
    const auth = requireSession(request, config, store);
    const client = upstream;
    if (!client.raw) {
      unsupported(reply, "The configured upstream client does not support raw proxying");
      return;
    }
    const params = request.params as { itemId: string; mediaSourceId: string; index: string };
    const mapping = resolveMediaSourceMapping(params.mediaSourceId, params.itemId);
    if (!mapping && liveRouteAggregation) {
      const subtitlePath = `/Videos/${params.itemId}/${params.mediaSourceId}/Subtitles/${params.index}/subtitles.m3u8`;
	      const response = await proxyRawFirst(client, request, "subtitle playlist", liveUpstreamSources(), subtitlePath, {
	        method: request.method,
	        query: rewritePlaybackProxyQuery(request.query as Record<string, string | undefined>),
	        headers: copyProxyRequestHeaders(request.headers)
	      });
      await sendRewrittenSubtitlePlaylist(reply, response, params.itemId, params.mediaSourceId, params.index, params.itemId, params.mediaSourceId, auth.session.accessToken);
      return;
    }
    if (!mapping) {
      notFound(reply, "Media source mapping not found");
      return;
    }
    const subtitlePath = `/Videos/${mapping.upstreamItemId}/${mapping.upstreamMediaSourceId}/Subtitles/${params.index}/subtitles.m3u8`;
	    const response = await proxyRaw(client, request, mapping.serverId, subtitlePath, {
	      method: request.method,
	      query: rewritePlaybackProxyQuery(request.query as Record<string, string | undefined>, mapping),
	      headers: copyProxyRequestHeaders(request.headers)
	    });
    await sendRewrittenSubtitlePlaylist(reply, response, params.itemId, params.mediaSourceId, params.index, mapping.upstreamItemId, mapping.upstreamMediaSourceId, auth.session.accessToken);
  }

  async function sendRewrittenSubtitlePlaylist(
    reply: FastifyReply,
    response: { statusCode: number; headers: Record<string, string | string[] | undefined>; body: unknown },
    bridgeItemIdValue: string,
    bridgeMediaSourceId: string,
    index: string,
    upstreamItemId: string,
    upstreamMediaSourceId: string,
    accessToken: string
  ): Promise<void> {
    const body = await responseBodyText(response.body);
    const rewritten = rewriteHlsPlaylist(body, {
      bridgeBasePath: `/Videos/${bridgeItemIdValue}/${bridgeMediaSourceId}/Subtitles/${index}`,
      upstreamBasePath: `/Videos/${upstreamItemId}/${upstreamMediaSourceId}/Subtitles/${index}`,
      authToken: accessToken
    });
    reply.code(response.statusCode);
    copyProxyResponseHeaders(reply, response.headers);
    reply.header("content-length", Buffer.byteLength(rewritten).toString());
    reply.send(rewritten);
  }

  async function proxySubtitleStream(request: FastifyRequest, reply: FastifyReply, routeStreamSegment = "Stream"): Promise<void> {
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
        ? `/Videos/${params.itemId}/${params.mediaSourceId}/Subtitles/${params.index}/${routeStreamSegment}.${params.format}`
        : `/Videos/${params.itemId}/${params.mediaSourceId}/Subtitles/${params.index}/${params.startPositionTicks}/${routeStreamSegment}.${params.format}`;
	      const response = await proxyRawFirst(client, request, "subtitle stream", liveUpstreamSources(), subtitlePath, {
	        method: request.method,
	        query: rewritePlaybackProxyQuery(request.query as Record<string, string | undefined>),
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
      ? `/Videos/${mapping.upstreamItemId}/${mapping.upstreamMediaSourceId}/Subtitles/${params.index}/${routeStreamSegment}.${params.format}`
      : `/Videos/${mapping.upstreamItemId}/${mapping.upstreamMediaSourceId}/Subtitles/${params.index}/${params.startPositionTicks}/${routeStreamSegment}.${params.format}`;
	    const response = await proxyRaw(client, request, mapping.serverId, subtitlePath, {
	      method: request.method,
	      query: rewritePlaybackProxyQuery(request.query as Record<string, string | undefined>, mapping),
	      headers: copyProxyRequestHeaders(request.headers)
	    });
    await sendProxyBody(reply, response);
  }

  async function proxyAttachment(request: FastifyRequest, reply: FastifyReply): Promise<void> {
    requireSession(request, config, store);
    const client = upstream;
    if (!client.raw) {
      unsupported(reply, "The configured upstream client does not support raw proxying");
      return;
    }
    const params = request.params as { itemId: string; mediaSourceId: string; index: string };
    const mapping = resolveMediaSourceMapping(params.mediaSourceId, params.itemId);
    if (!mapping && liveRouteAggregation) {
      const response = await proxyRawFirst(client, request, "video attachment", liveUpstreamSources(), `/Videos/${params.itemId}/${params.mediaSourceId}/Attachments/${params.index}`, {
        method: request.method,
        query: rewritePlaybackProxyQuery(request.query as Record<string, string | undefined>),
        headers: copyProxyRequestHeaders(request.headers)
      });
      await sendProxyBody(reply, response);
      return;
    }
    if (!mapping) {
      notFound(reply, "Media source mapping not found");
      return;
    }
    const response = await proxyRaw(client, request, mapping.serverId, `/Videos/${mapping.upstreamItemId}/${mapping.upstreamMediaSourceId}/Attachments/${params.index}`, {
      method: request.method,
      query: rewritePlaybackProxyQuery(request.query as Record<string, string | undefined>, mapping),
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

  async function forwardPlaybackReport(path: string, body: unknown, userName: string, query?: unknown): Promise<void> {
    const client = upstream;
    const report = body && typeof body === "object" ? { ...(body as Record<string, unknown>) } : {};
    const reportQuery = playbackReportQuery(query);
    const mediaSourceId = typeof report.MediaSourceId === "string" ? report.MediaSourceId : undefined;
    const playSessionId = typeof report.PlaySessionId === "string"
      ? report.PlaySessionId
      : reportQuery.PlaySessionId ?? reportQuery.playSessionId;
    const playbackSession = playSessionId ? store.findPlaybackSessionMapping(playSessionId) : undefined;
    if (!mediaSourceId) {
      if (playbackSession) {
        rewritePlaybackReportSession(report, reportQuery, playbackSession);
        try {
          await client.json(playbackSession.serverId, path, {
            method: "POST",
            query: reportQuery,
            body: report
          });
        } catch {
          // Legacy mapped sessions can outlive their upstream transcode; reports remain best-effort.
        }
        return;
      }
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
    if (playbackSession) {
      rewritePlaybackReportSession(report, reportQuery, playbackSession);
    }
    try {
      await client.json(mapping.serverId, path, {
        method: "POST",
        query: reportQuery,
        body: report
      });
    } catch {
      // Legacy mapped sessions can outlive their upstream transcode; reports remain best-effort.
    }
  }

  function playbackReportQuery(value: unknown): Record<string, string | undefined> {
    if (!value || typeof value !== "object") return {};
    const query: Record<string, string | undefined> = {};
    for (const [key, raw] of Object.entries(value as Record<string, unknown>)) {
      if (typeof raw === "string" || raw === undefined) query[key] = raw;
    }
    return query;
  }

  function rewritePlaybackReportSession(
    report: Record<string, unknown>,
    query: Record<string, string | undefined>,
    playbackSession: PlaybackSessionMapping
  ): void {
    if (typeof report.ItemId === "string") {
      report.ItemId = playbackSession.upstreamItemId;
    }
    if (typeof report.PlaySessionId === "string") {
      report.PlaySessionId = playbackSession.upstreamPlaySessionId;
    }
    if (query.PlaySessionId) {
      query.PlaySessionId = playbackSession.upstreamPlaySessionId;
    }
    if (query.playSessionId) {
      query.playSessionId = playbackSession.upstreamPlaySessionId;
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

function infuseSyncPluginInfo(): Record<string, unknown> {
  return {
    Name: "InfuseSync",
    Description: "Plugin for fast synchronization with Infuse.",
    Id: "022a3003-993f-45f1-8565-87d12af2e12a",
    Version: "1.5.2.0",
    CanUninstall: false,
    HasImage: false,
    Status: "Active"
  };
}

function requiredInfuseSyncQueryValue(query: unknown, name: string): string {
  const value = caseInsensitiveQueryValue(query, name);
  if (!value) throw badRequestError(`${name} is required`);
  return value;
}

function caseInsensitiveQueryValue(query: unknown, name: string): string | undefined {
  const expected = name.toLowerCase();
  for (const [key, value] of Object.entries((query ?? {}) as Record<string, unknown>)) {
    if (key.toLowerCase() !== expected) continue;
    const raw = Array.isArray(value) ? value[0] : value;
    return typeof raw === "string" && raw.length > 0 ? raw : undefined;
  }
  return undefined;
}

function infuseSyncBrowseQuery(query: unknown): BrowseQuery {
  return {
    includeItemTypes: includeItemTypesFrom(query),
    startIndex: startIndexFrom(query),
    limit: limitFrom(query)
  };
}

function infuseSyncStartIndex(query: unknown): number {
  return startIndexFrom(query) ?? 0;
}

function parseInfuseSyncItemTypes(query: unknown): string[] {
  return includeItemTypeList(includeItemTypesFrom(query));
}

function requireCompletedInfuseSyncCheckpoint(checkpoint: InfuseSyncCheckpointRecord): InfuseSyncCheckpointRecord & { syncTimestamp: string } {
  if (!checkpoint.syncTimestamp) throw badRequestError("InfuseSync checkpoint has not started");
  return checkpoint as InfuseSyncCheckpointRecord & { syncTimestamp: string };
}

function infuseSyncUserDataDto(record: UserDataRecord): Record<string, unknown> {
  return {
    PlaybackPositionTicks: record.playbackPositionTicks,
    PlayCount: record.playCount,
    IsFavorite: record.isFavorite,
    LastPlayedDate: record.lastPlayedDate,
    Played: record.played,
    Key: record.itemId,
    ItemId: record.itemId
  };
}

function badRequestError(message: string): Error & { statusCode: number } {
  return Object.assign(new Error(message), { statusCode: 400 });
}

function registerUnsupportedRoutes(app: FastifyInstance): void {
  app.all("/Plugins/*", async (_request, reply) => unsupported(reply, "Plugins is not supported by Jellyfin Bridge"));

  for (const prefix of [
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
    playCount: typeof data.PlayCount === "number" ? data.PlayCount : undefined,
    lastPlayedDate: typeof data.LastPlayedDate === "string" ? data.LastPlayedDate : undefined
  });
}

function watchedUserDataPayload(body: unknown): WatchedUserDataPayload | undefined {
  if (!isRecord(body)) return undefined;
  const triggersWatchedWrite = typeof body.Played === "boolean"
    || typeof body.PlayCount === "number"
    || typeof body.LastPlayedDate === "string";
  if (!triggersWatchedWrite) return undefined;

  const payload: WatchedUserDataPayload = {};
  if (typeof body.Played === "boolean") payload.Played = body.Played;
  if (typeof body.PlayCount === "number") payload.PlayCount = body.PlayCount;
  if (typeof body.LastPlayedDate === "string") payload.LastPlayedDate = body.LastPlayedDate;
  if (typeof body.PlaybackPositionTicks === "number") payload.PlaybackPositionTicks = body.PlaybackPositionTicks;
  return payload;
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
    recursive: booleanQuery(value.Recursive ?? value.recursive),
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

function itemImagePath(params: {
  itemId: string;
  imageType: string;
  imageIndex?: string;
  tag?: string;
  format?: string;
  maxWidth?: string;
  maxHeight?: string;
  percentPlayed?: string;
  unplayedCount?: string;
}): string | undefined {
  const safeItemId = safeIdPathSegment(params.itemId);
  const safeImageType = safeImageTypePathSegment(params.imageType);
  const safeImageIndex = params.imageIndex === undefined ? undefined : safeImageIndexPathSegment(params.imageIndex);
  const hasLegacyPath = params.tag !== undefined
    || params.format !== undefined
    || params.maxWidth !== undefined
    || params.maxHeight !== undefined
    || params.percentPlayed !== undefined
    || params.unplayedCount !== undefined;
  if (hasLegacyPath) {
    const safeTag = params.tag === undefined ? undefined : safePathSegment(params.tag);
    const safeFormat = params.format === undefined ? undefined : safePathSegment(params.format);
    const safeMaxWidth = params.maxWidth === undefined ? undefined : safeImageIndexPathSegment(params.maxWidth);
    const safeMaxHeight = params.maxHeight === undefined ? undefined : safeImageIndexPathSegment(params.maxHeight);
    const safePercentPlayed = params.percentPlayed === undefined ? undefined : safeNumberPathSegment(params.percentPlayed);
    const safeUnplayedCount = params.unplayedCount === undefined ? undefined : safeImageIndexPathSegment(params.unplayedCount);
    if (!safeItemId || !safeImageType || !safeImageIndex || !safeTag || !safeFormat || !safeMaxWidth || !safeMaxHeight || !safePercentPlayed || !safeUnplayedCount) return undefined;
    return `/Items/${safeItemId}/Images/${safeImageType}/${safeImageIndex}/${safeTag}/${safeFormat}/${safeMaxWidth}/${safeMaxHeight}/${safePercentPlayed}/${safeUnplayedCount}`;
  }
  if (!safeItemId || !safeImageType || (params.imageIndex !== undefined && !safeImageIndex)) return undefined;
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

function safeNumberPathSegment(value: string): string | undefined {
  return /^(0|[1-9][0-9]*)(\.[0-9]+)?$/.test(value) ? value : undefined;
}

function safePathSegment(value: string): string | undefined {
  return value.length > 0 && !/[\\/]/.test(value) && value !== "." && value !== ".." ? encodeURIComponent(value) : undefined;
}

function parentIdFrom(query: unknown): string | undefined {
  const value = (query ?? {}) as Record<string, string | undefined>;
  return value.ParentId ?? value.parentId;
}

function includeItemTypesFrom(query: unknown): string | undefined {
  const value = (query ?? {}) as Record<string, string | undefined>;
  const includeItemTypes = value.IncludeItemTypes ?? value.includeItemTypes;
  return includeItemTypes && includeItemTypes.length > 0 ? includeItemTypes : undefined;
}

function seriesIdFrom(query: unknown): string | undefined {
  const value = (query ?? {}) as Record<string, string | undefined>;
  return value.SeriesId ?? value.seriesId;
}

function includeItemTypeList(value: string | undefined): string[] {
  return value?.split(",").map((type) => type.trim().toLowerCase()).filter(Boolean) ?? [];
}

function latestIncludeItemTypesForCollection(collectionType: string | null | undefined): string | undefined {
  if (collectionType === "movies") return "Movie";
  if (collectionType === "tvshows") return "Episode";
  return undefined;
}

function startIndexFrom(query: unknown): number | undefined {
  const value = (query ?? {}) as Record<string, string | undefined>;
  return numberQuery(value.StartIndex ?? value.startIndex);
}

function limitFrom(query: unknown): number | undefined {
  const value = (query ?? {}) as Record<string, string | undefined>;
  return numberQuery(value.Limit ?? value.limit);
}

function datePlayedFromQuery(query: unknown): string | undefined {
  const value = (query ?? {}) as Record<string, string | undefined>;
  return value.DatePlayed ?? value.datePlayed;
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
