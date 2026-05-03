import type { FastifyRequest } from "fastify";
import { verify } from "@node-rs/argon2";
import type { BridgeConfig, BridgeUser } from "./config.js";
import { uuidV5 } from "./ids.js";
import type { SessionRecord, Store } from "./store.js";

const USER_NAMESPACE = "a08aa442-4164-5dd7-8f3d-96b8f8d6b3ad";

export interface AuthHeaderInfo {
  client?: string;
  device?: string;
  deviceId?: string;
  version?: string;
  token?: string;
}

export interface AuthContext {
  session: SessionRecord;
  user: BridgeUser;
}

export function parseAuthorization(request: FastifyRequest): AuthHeaderInfo {
  const info: AuthHeaderInfo = {};
  const authorization = request.headers.authorization;
  if (authorization?.startsWith("MediaBrowser ") || authorization?.startsWith("Emby ")) {
    Object.assign(info, parseKeyValueHeader(authorization.slice(authorization.indexOf(" ") + 1)));
  }

  const legacy = request.headers["x-emby-authorization"];
  if (typeof legacy === "string") {
    Object.assign(info, parseKeyValueHeader(legacy));
  }

  const tokenHeader = request.headers["x-mediabrowser-token"] ?? request.headers["x-emby-token"];
  if (typeof tokenHeader === "string") {
    info.token = tokenHeader;
  }

  const query = request.query as Record<string, string | undefined>;
  info.token ??= query.api_key ?? query.ApiKey ?? query.AccessToken;
  return info;
}

export async function authenticatePassword(user: BridgeUser, password: string): Promise<boolean> {
  if (user.passwordHash.startsWith("$argon2")) {
    return verify(user.passwordHash, password);
  }
  return false;
}

export function requireSession(request: FastifyRequest, config: BridgeConfig, store: Store): AuthContext {
  const token = parseAuthorization(request).token;
  if (!token) {
    throw Object.assign(new Error("Missing access token"), { statusCode: 401 });
  }
  const session = store.findSession(token);
  if (!session) {
    throw Object.assign(new Error("Invalid access token"), { statusCode: 401 });
  }
  const user = config.auth.users.find((candidate) => userId(candidate.name) === session.userId);
  if (!user) {
    throw Object.assign(new Error("Session user no longer exists"), { statusCode: 401 });
  }
  return { session, user };
}

export function userId(name: string): string {
  return uuidV5(`user:${name.toLowerCase()}`, USER_NAMESPACE).replaceAll("-", "");
}

export function userDto(name: string, serverId: string, serverName: string): Record<string, unknown> {
  return {
    Name: name,
    ServerId: serverId,
    ServerName: serverName,
    Id: userId(name),
    HasPassword: true,
    HasConfiguredPassword: true,
    HasConfiguredEasyPassword: false,
    EnableAutoLogin: false,
    Configuration: {
      AudioLanguagePreference: "",
      PlayDefaultAudioTrack: true,
      SubtitleLanguagePreference: "",
      DisplayMissingEpisodes: false,
      GroupedFolders: [],
      SubtitleMode: "Default",
      DisplayCollectionsView: false,
      EnableLocalPassword: false,
      OrderedViews: [],
      LatestItemsExcludes: [],
      MyMediaExcludes: [],
      HidePlayedInLatest: true,
      RememberAudioSelections: true,
      RememberSubtitleSelections: true,
      EnableNextEpisodeAutoPlay: true
    },
    Policy: {
      IsAdministrator: false,
      IsHidden: false,
      IsDisabled: false,
      EnableUserPreferenceAccess: true,
      EnableRemoteControlOfOtherUsers: false,
      EnableSharedDeviceControl: false,
      EnableRemoteAccess: true,
      EnableLiveTvManagement: false,
      EnableLiveTvAccess: false,
      EnableMediaPlayback: true,
      EnableAudioPlaybackTranscoding: true,
      EnableVideoPlaybackTranscoding: true,
      EnablePlaybackRemuxing: true,
      EnableContentDeletion: false,
      EnableContentDeletionFromFolders: [],
      EnableContentDownloading: false,
      EnableSyncTranscoding: false,
      EnableMediaConversion: false,
      EnabledDevices: [],
      EnableAllDevices: true,
      EnabledChannels: [],
      EnableAllChannels: true,
      EnabledFolders: [],
      EnableAllFolders: true,
      InvalidLoginAttemptCount: 0,
      LoginAttemptsBeforeLockout: -1,
      MaxActiveSessions: 0,
      EnablePublicSharing: false,
      BlockedTags: [],
      BlockUnratedItems: [],
      EnableSubtitleDownloading: false,
      EnableSubtitleManagement: false,
      SyncPlayAccess: "None"
    }
  };
}

function parseKeyValueHeader(value: string): AuthHeaderInfo {
  const result: AuthHeaderInfo = {};
  for (const part of value.split(",")) {
    const [rawKey, ...rawValue] = part.split("=");
    const key = rawKey?.trim().toLowerCase();
    const parsedValue = rawValue.join("=").trim().replace(/^"|"$/g, "");
    if (!key) continue;
    if (key === "client") result.client = parsedValue;
    if (key === "device") result.device = parsedValue;
    if (key === "deviceid") result.deviceId = parsedValue;
    if (key === "version") result.version = parsedValue;
    if (key === "token") result.token = parsedValue;
  }
  return result;
}
