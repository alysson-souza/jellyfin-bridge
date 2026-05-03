import { bridgeLibraryId, bridgeServerId, passThroughLibraryId } from "./ids.js";
import type { BridgeConfig, LibraryConfig } from "./config.js";
import type { UpstreamLibraryRecord } from "./store.js";
import type { SessionRecord, Store } from "./store.js";

export const BRIDGE_VERSION = "10.10.0";

export function publicSystemInfo(config: BridgeConfig): Record<string, unknown> {
  const id = bridgeServerId(config.server.name);
  return {
    LocalAddress: config.server.publicUrl ?? `http://${config.server.bind}:${config.server.port}`,
    ServerName: config.server.name,
    Version: BRIDGE_VERSION,
    ProductName: "Jellyfin Bridge",
    OperatingSystem: "",
    Id: id,
    StartupWizardCompleted: true
  };
}

export function systemInfo(config: BridgeConfig): Record<string, unknown> {
  return {
    ...publicSystemInfo(config),
    OperatingSystemDisplayName: "",
    PackageName: "jellyfin-bridge",
    HasPendingRestart: false,
    IsShuttingDown: false,
    SupportsLibraryMonitor: false,
    WebSocketPortNumber: config.server.port,
    CompletedInstallations: [],
    CanSelfRestart: false,
    CanLaunchWebBrowser: false,
    ProgramDataPath: "",
    WebPath: "",
    ItemsByNamePath: "",
    CachePath: "",
    LogPath: "",
    InternalMetadataPath: "",
    TranscodingTempPath: "",
    CastReceiverApplications: [],
    HasUpdateAvailable: false,
    EncoderLocation: "System",
    SystemArchitecture: process.arch
  };
}

export function queryResult<T>(items: T[], startIndex = 0, totalRecordCount = items.length): Record<string, unknown> {
  return {
    Items: items,
    TotalRecordCount: totalRecordCount,
    StartIndex: startIndex
  };
}

export function libraryDto(library: LibraryConfig, serverId: string): Record<string, unknown> {
  const id = bridgeLibraryId(library.id);
  return {
    Name: library.name,
    ServerId: serverId,
    Id: id,
    Etag: id,
    DateCreated: new Date(0).toISOString(),
    CanDelete: false,
    CanDownload: false,
    SortName: library.name,
    ExternalUrls: [],
    Path: "",
    EnableMediaSourceDisplay: false,
    ChannelId: null,
    Taglines: [],
    Genres: [],
    PlayAccess: "Full",
    RemoteTrailers: [],
    ProviderIds: {},
    IsFolder: true,
    ParentId: null,
    Type: "CollectionFolder",
    People: [],
    Studios: [],
    GenreItems: [],
    LocalTrailerCount: 0,
    UserData: { PlaybackPositionTicks: 0, PlayCount: 0, IsFavorite: false, Played: false, Key: id, ItemId: id },
    ChildCount: 0,
    SpecialFeatureCount: 0,
    DisplayPreferencesId: id,
    Tags: [],
    PrimaryImageAspectRatio: 0.6666666666666666,
    CollectionType: library.collectionType,
    LocationType: "FileSystem",
    MediaType: "Unknown"
  };
}

export function passThroughLibraryDto(library: UpstreamLibraryRecord, upstreamName: string, serverId: string): Record<string, unknown> {
  const id = passThroughLibraryId(library.serverId, library.libraryId);
  return {
    Name: `${upstreamName} - ${library.name}`,
    ServerId: serverId,
    Id: id,
    Etag: id,
    DateCreated: new Date(0).toISOString(),
    CanDelete: false,
    CanDownload: false,
    SortName: `${upstreamName} - ${library.name}`,
    ExternalUrls: [],
    Path: "",
    EnableMediaSourceDisplay: false,
    ChannelId: null,
    Taglines: [],
    Genres: [],
    PlayAccess: "Full",
    RemoteTrailers: [],
    ProviderIds: {},
    IsFolder: true,
    ParentId: null,
    Type: "CollectionFolder",
    People: [],
    Studios: [],
    GenreItems: [],
    LocalTrailerCount: 0,
    UserData: { PlaybackPositionTicks: 0, PlayCount: 0, IsFavorite: false, Played: false, Key: id, ItemId: id },
    ChildCount: 0,
    SpecialFeatureCount: 0,
    DisplayPreferencesId: id,
    Tags: [],
    PrimaryImageAspectRatio: 0.6666666666666666,
    CollectionType: library.collectionType,
    LocationType: "FileSystem",
    MediaType: "Unknown"
  };
}

export function sessionInfo(session: SessionRecord, serverName: string): Record<string, unknown> {
  return {
    PlayState: {
      CanSeek: false,
      IsPaused: false,
      IsMuted: false,
      RepeatMode: "RepeatNone"
    },
    AdditionalUsers: [],
    Capabilities: {
      PlayableMediaTypes: [],
      SupportedCommands: [],
      SupportsMediaControl: false,
      SupportsContentUploading: false,
      SupportsPersistentIdentifier: true,
      SupportsSync: false
    },
    RemoteEndPoint: "",
    PlayableMediaTypes: [],
    Id: session.id,
    UserId: session.userId,
    UserName: session.userName,
    Client: "Jellyfin Bridge",
    LastActivityDate: session.lastSeenAt,
    LastPlaybackCheckIn: session.lastSeenAt,
    DeviceName: session.deviceName ?? "Unknown Device",
    DeviceId: session.deviceId ?? session.id,
    ApplicationVersion: BRIDGE_VERSION,
    IsActive: true,
    SupportsMediaControl: false,
    SupportsRemoteControl: false,
    NowPlayingQueue: [],
    NowPlayingQueueFullItems: [],
    HasCustomDeviceName: false,
    ServerName: serverName
  };
}

export function userDataDto(store: Store, userId: string, itemId: string): Record<string, unknown> {
  return {
    ...store.getUserData(userId, itemId),
    ItemId: itemId
  };
}
