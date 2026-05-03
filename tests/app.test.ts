import test from "node:test";
import assert from "node:assert/strict";
import { Readable } from "node:stream";
import { hash } from "@node-rs/argon2";
import { buildApp } from "../src/app.js";
import type { BridgeConfig, RuntimeConfigSource } from "../src/config.js";
import { Store } from "../src/store.js";
import { bridgeItemId, bridgeLibraryId, bridgeMediaSourceId, bridgeServerId } from "../src/ids.js";
import { passThroughLibraryId } from "../src/ids.js";

test("supports Jellyfin login, authenticated system routes, user views, user data, and logout", async () => {
  const passwordHash = await hash("secret");
  const config: BridgeConfig = {
    server: { bind: "127.0.0.1", port: 8096, publicUrl: "http://bridge.test", name: "Bridge" },
    auth: { users: [{ name: "alice", passwordHash }] },
    upstreams: [{ id: "main", name: "Main", url: "https://jellyfin.example.com", token: "token" }],
    libraries: [{ id: "movies", name: "Movies", collectionType: "movies", sources: [{ server: "main", libraryId: "abc" }] }]
  };
  const store = new Store(":memory:");
  const app = buildApp({ config, store });

  const publicInfo = await app.inject({ method: "GET", url: "/System/Info/Public" });
  assert.equal(publicInfo.statusCode, 200);
  assert.equal(publicInfo.json().ServerName, "Bridge");

  const login = await app.inject({
    method: "POST",
    url: "/Users/AuthenticateByName",
    headers: {
      Authorization: 'MediaBrowser Client="Swiftfin", Device="iPhone", DeviceId="device-1", Version="1.0"'
    },
    payload: { Username: "alice", Pw: "secret" }
  });
  assert.equal(login.statusCode, 200);
  const auth = login.json();
  assert.equal(auth.User.Name, "alice");
  assert.equal(auth.SessionInfo.DeviceId, "device-1");
  assert.match(auth.AccessToken, /^[0-9a-f]{64}$/);

  const passwordLogin = await app.inject({
    method: "POST",
    url: "/Users/AuthenticateByName",
    payload: { Username: "alice", Password: "secret" }
  });
  assert.equal(passwordLogin.statusCode, 200);
  assert.equal(passwordLogin.json().User.Name, "alice");

  const embyPrefixedLogin = await app.inject({
    method: "POST",
    url: "/emby/Users/AuthenticateByName",
    payload: { Username: "alice", Pw: "secret" }
  });
  assert.equal(embyPrefixedLogin.statusCode, 200);
  assert.equal(embyPrefixedLogin.json().User.Name, "alice");

  const token = auth.AccessToken;
  const systemInfo = await app.inject({ method: "GET", url: "/System/Info", headers: { "X-MediaBrowser-Token": token } });
  assert.equal(systemInfo.statusCode, 200);
  assert.equal(systemInfo.json().ProductName, "Jellyfin Bridge");

  const views = await app.inject({ method: "GET", url: `/Users/${auth.User.Id}/Views`, headers: { "X-MediaBrowser-Token": token } });
  assert.equal(views.statusCode, 200);
  assert.equal(views.json().Items[0].Name, "Movies");
  assert.equal(views.json().Items[0].Type, "CollectionFolder");

  const libraryItem = await app.inject({
    method: "GET",
    url: `/Items/${views.json().Items[0].Id}?userId=${auth.User.Id}`,
    headers: { "X-MediaBrowser-Token": token }
  });
  assert.equal(libraryItem.statusCode, 200);
  assert.equal(libraryItem.json().Name, "Movies");
  assert.equal(libraryItem.json().Type, "CollectionFolder");

  const groupingOptions = await app.inject({
    method: "GET",
    url: `/UserViews/GroupingOptions?userId=${auth.User.Id}`,
    headers: { "X-MediaBrowser-Token": token }
  });
  assert.equal(groupingOptions.statusCode, 200);
  assert.deepEqual(groupingOptions.json(), [{ Name: "Movies", Id: views.json().Items[0].Id }]);

  const legacyGroupingOptions = await app.inject({
    method: "GET",
    url: `/Users/${auth.User.Id}/GroupingOptions`,
    headers: { "X-MediaBrowser-Token": token }
  });
  assert.equal(legacyGroupingOptions.statusCode, 200);
  assert.deepEqual(legacyGroupingOptions.json(), groupingOptions.json());

  const virtualFolders = await app.inject({
    method: "GET",
    url: "/Library/VirtualFolders",
    headers: { "X-MediaBrowser-Token": token }
  });
  assert.equal(virtualFolders.statusCode, 200);
  assert.deepEqual(virtualFolders.json().map((folder: any) => ({
    Name: folder.Name,
    CollectionType: folder.CollectionType,
    ItemId: folder.ItemId,
    Locations: folder.Locations,
    RefreshStatus: folder.RefreshStatus
  })), [{
    Name: "Movies",
    CollectionType: "movies",
    ItemId: views.json().Items[0].Id,
    Locations: [],
    RefreshStatus: "Idle"
  }]);
  assert.equal(virtualFolders.json()[0].LibraryOptions.Enabled, true);

  const displayPreferences = await app.inject({
    method: "GET",
    url: `/DisplayPreferences/usersettings?userId=${auth.User.Id}&client=emby`,
    headers: { "X-MediaBrowser-Token": token }
  });
  assert.equal(displayPreferences.statusCode, 200);
  assert.equal(displayPreferences.json().Id, "usersettings");
  assert.equal(displayPreferences.json().Client, "emby");
  assert.equal(displayPreferences.json().ShowBackdrop, true);
  assert.deepEqual(displayPreferences.json().CustomPrefs, {});

  const itemId = "0123456789abcdef0123456789abcdef";
  const rootItem = await app.inject({
    method: "GET",
    url: `/Items//?userId=${auth.User.Id}`,
    headers: { "X-MediaBrowser-Token": token }
  });
  assert.equal(rootItem.statusCode, 200);
  assert.equal(rootItem.json().Type, "AggregateFolder");

  const updateUserData = await app.inject({
    method: "POST",
    url: `/Users/${auth.User.Id}/Items/${itemId}/UserData`,
    headers: { "X-MediaBrowser-Token": token },
    payload: { IsFavorite: true, Played: true, PlaybackPositionTicks: 1234, PlayCount: 2 }
  });
  assert.equal(updateUserData.statusCode, 200);
  assert.equal(updateUserData.json().IsFavorite, true);
  assert.equal(updateUserData.json().Played, true);

  const userData = await app.inject({
    method: "GET",
    url: `/UserItems/${itemId}/UserData?UserId=${auth.User.Id}`,
    headers: { "X-MediaBrowser-Token": token }
  });
  assert.equal(userData.statusCode, 200);
  assert.equal(userData.json().PlaybackPositionTicks, 1234);

  const logout = await app.inject({ method: "POST", url: "/Sessions/Logout", headers: { "X-MediaBrowser-Token": token } });
  assert.equal(logout.statusCode, 204);

  const afterLogout = await app.inject({ method: "GET", url: "/System/Info", headers: { "X-MediaBrowser-Token": token } });
  assert.equal(afterLogout.statusCode, 401);

  await app.close();
  store.close();
});

test("uses updated runtime config after startup", async () => {
  const oldPasswordHash = await hash("old-secret");
  const newPasswordHash = await hash("new-secret");
  const runtimeConfig = new MutableRuntimeConfig({
    server: { bind: "127.0.0.1", port: 8096, publicUrl: "http://old.test", name: "Old Bridge" },
    auth: { users: [{ name: "alice", passwordHash: oldPasswordHash }] },
    upstreams: [{ id: "main", name: "Main", url: "https://jellyfin.example.com", token: "token" }],
    libraries: [{ id: "movies", name: "Movies", collectionType: "movies", sources: [{ server: "main", libraryId: "abc" }] }]
  });
  const store = new Store(":memory:");
  const app = buildApp({ config: runtimeConfig, store });

  runtimeConfig.update({
    server: { bind: "127.0.0.1", port: 8096, publicUrl: "http://new.test", name: "New Bridge" },
    auth: { users: [{ name: "bob", passwordHash: newPasswordHash }] },
    upstreams: [{ id: "main", name: "Main", url: "https://jellyfin.example.com", token: "token" }],
    libraries: [{ id: "series", name: "Series", collectionType: "tvshows", sources: [{ server: "main", libraryId: "def" }] }]
  });

  const publicInfo = await app.inject({ method: "GET", url: "/System/Info/Public" });
  assert.equal(publicInfo.json().ServerName, "New Bridge");
  assert.equal(publicInfo.json().LocalAddress, "http://new.test");

  const oldLogin = await app.inject({
    method: "POST",
    url: "/Users/AuthenticateByName",
    payload: { Username: "alice", Pw: "old-secret" }
  });
  assert.equal(oldLogin.statusCode, 401);

  const newLogin = await app.inject({
    method: "POST",
    url: "/Users/AuthenticateByName",
    payload: { Username: "bob", Pw: "new-secret" }
  });
  assert.equal(newLogin.statusCode, 200);
  const token = newLogin.json().AccessToken;

  const views = await app.inject({ method: "GET", url: "/UserViews", headers: { "X-MediaBrowser-Token": token } });
  assert.deepEqual(views.json().Items.map((item: Record<string, unknown>) => item.Name), ["Series"]);

  await app.close();
  store.close();
});

test("keeps login response on the pre-reload config across password verification", async () => {
  const runtimeConfig = new MutableRuntimeConfig({
    server: { bind: "127.0.0.1", port: 8096, publicUrl: "http://old.test", name: "Old Bridge" },
    auth: { users: [{ name: "alice", passwordHash: "old-hash" }] },
    upstreams: [{ id: "main", name: "Main", url: "https://jellyfin.example.com", token: "token" }],
    libraries: []
  });
  const store = new Store(":memory:");
  let releaseVerification!: () => void;
  let verificationStarted!: () => void;
  const releaseVerificationPromise = new Promise<void>((resolve) => {
    releaseVerification = resolve;
  });
  const verificationStartedPromise = new Promise<void>((resolve) => {
    verificationStarted = resolve;
  });
  const app = buildApp({
    config: runtimeConfig,
    store,
    verifyPassword: async () => {
      verificationStarted();
      await releaseVerificationPromise;
      return true;
    }
  });

  const loginPromise = app.inject({
    method: "POST",
    url: "/Users/AuthenticateByName",
    payload: { Username: "alice", Pw: "secret" }
  });
  await verificationStartedPromise;

  runtimeConfig.update({
    server: { bind: "127.0.0.1", port: 8096, publicUrl: "http://new.test", name: "New Bridge" },
    auth: { users: [{ name: "bob", passwordHash: "new-hash" }] },
    upstreams: [{ id: "main", name: "Main", url: "https://jellyfin.example.com", token: "token" }],
    libraries: []
  });
  releaseVerification();

  const login = await loginPromise;

  assert.equal(login.statusCode, 200, login.body);
  assert.equal(login.json().User.Name, "alice");
  assert.equal(login.json().User.ServerId, bridgeServerId("Old Bridge"));
  assert.equal(login.json().SessionInfo.ServerName, "Old Bridge");
  assert.equal(login.json().ServerId, bridgeServerId("Old Bridge"));

  await app.close();
  store.close();
});

test("keeps in-flight live latest requests on the pre-reload upstream client", async () => {
  const passwordHash = await hash("secret");
  const runtimeConfig = new MutableRuntimeConfig({
    server: { bind: "127.0.0.1", port: 8096, publicUrl: "http://bridge.test", name: "Bridge" },
    auth: { users: [{ name: "alice", passwordHash }] },
    upstreams: [{ id: "main", name: "Main", url: "https://main.example.com", token: "old-token" }],
    libraries: [{ id: "movies", name: "Movies", collectionType: "movies", sources: [{ server: "main", libraryId: "main-movies" }] }]
  });
  const store = new Store(":memory:");
  let releaseUsers!: () => void;
  let usersStarted!: () => void;
  const releaseUsersPromise = new Promise<void>((resolve) => {
    releaseUsers = resolve;
  });
  const usersStartedPromise = new Promise<void>((resolve) => {
    usersStarted = resolve;
  });
  const clients: ReloadingLiveUpstream[] = [];
  const app = buildApp({
    config: runtimeConfig,
    store,
    upstreamFactory: (upstreams) => {
      const upstreamIds = new Set(upstreams.map((upstream) => upstream.id));
      const client = new ReloadingLiveUpstream(upstreamIds, upstreamIds.has("main")
        ? async () => {
          usersStarted();
          await releaseUsersPromise;
        }
        : undefined);
      clients.push(client);
      return client;
    }
  });

  const login = await app.inject({ method: "POST", url: "/Users/AuthenticateByName", payload: { Username: "alice", Pw: "secret" } });
  const latestPromise = app.inject({
    method: "GET",
    url: `/Items/Latest?ParentId=${bridgeLibraryId("movies")}&Limit=1`,
    headers: { "X-MediaBrowser-Token": login.json().AccessToken }
  });
  await usersStartedPromise;

  runtimeConfig.update({
    server: { bind: "127.0.0.1", port: 8096, publicUrl: "http://bridge.test", name: "Bridge" },
    auth: { users: [{ name: "alice", passwordHash }] },
    upstreams: [{ id: "remote", name: "Remote", url: "https://remote.example.com", token: "new-token" }],
    libraries: [{ id: "movies", name: "Movies", collectionType: "movies", sources: [{ server: "remote", libraryId: "remote-movies" }] }]
  });
  releaseUsers();

  const latest = await latestPromise;

  assert.equal(latest.statusCode, 200, latest.body);
  assert.deepEqual(latest.json().map((item: Record<string, unknown>) => item.Name), ["Old Main Latest"]);
  assert.deepEqual(clients[0].requests.map((request) => `${request.serverId}:${request.path}`), ["main:/Users", "main:/Items/Latest"]);
  assert.equal(clients[1].requests.length, 0);

  await app.close();
  store.close();
});

test("keeps live latest fallback responses on the pre-reload config after a reload", async () => {
  const passwordHash = await hash("secret");
  const runtimeConfig = new MutableRuntimeConfig({
    server: { bind: "127.0.0.1", port: 8096, publicUrl: "http://bridge.test", name: "Bridge" },
    auth: { users: [{ name: "alice", passwordHash }] },
    upstreams: [{ id: "main", name: "Main", url: "https://main.example.com", token: "old-token" }],
    libraries: [{ id: "movies", name: "Movies", collectionType: "movies", sources: [{ server: "main", libraryId: "main-movies" }] }]
  });
  const store = new Store(":memory:");
  store.upsertIndexedItem({
    serverId: "main",
    itemId: "main-cached",
    libraryId: "main-movies",
    itemType: "Movie",
    logicalKey: "movie:main-cached",
    json: {
      Id: "main-cached",
      Type: "Movie",
      Name: "Cached Old Main",
      DateCreated: "2026-05-03T00:00:00.000Z",
      ProviderIds: {}
    }
  });
  let releaseUsers!: () => void;
  let usersStarted!: () => void;
  const releaseUsersPromise = new Promise<void>((resolve) => {
    releaseUsers = resolve;
  });
  const usersStartedPromise = new Promise<void>((resolve) => {
    usersStarted = resolve;
  });
  const clients: ReloadingLiveUpstream[] = [];
  const app = buildApp({
    config: runtimeConfig,
    store,
    upstreamFactory: (upstreams) => {
      const upstreamIds = new Set(upstreams.map((upstream) => upstream.id));
      const client = new ReloadingLiveUpstream(upstreamIds, upstreamIds.has("main")
        ? async () => {
          usersStarted();
          await releaseUsersPromise;
        }
        : undefined, undefined, undefined, new Error("Upstream main request failed for /Items/Latest: offline"));
      clients.push(client);
      return client;
    }
  });

  const login = await app.inject({ method: "POST", url: "/Users/AuthenticateByName", payload: { Username: "alice", Pw: "secret" } });
  const latestPromise = app.inject({
    method: "GET",
    url: "/Items/Latest?Limit=1",
    headers: { "X-MediaBrowser-Token": login.json().AccessToken }
  });
  await usersStartedPromise;

  runtimeConfig.update({
    server: { bind: "127.0.0.1", port: 8096, publicUrl: "http://bridge.test", name: "Reloaded Bridge" },
    auth: { users: [{ name: "alice", passwordHash }] },
    upstreams: [{ id: "remote", name: "Remote", url: "https://remote.example.com", token: "new-token" }],
    libraries: [{ id: "movies", name: "Movies", collectionType: "movies", sources: [{ server: "remote", libraryId: "remote-movies" }] }]
  });
  releaseUsers();

  const latest = await latestPromise;

  assert.equal(latest.statusCode, 200, latest.body);
  assert.deepEqual(latest.json().map((item: Record<string, unknown>) => item.Name), ["Cached Old Main"]);
  assert.equal(latest.json()[0].ServerId, bridgeServerId("Bridge"));
  assert.deepEqual(clients[0].requests.map((request) => `${request.serverId}:${request.path}`), ["main:/Users", "main:/Items/Latest"]);
  assert.equal(clients[1].requests.length, 0);

  await app.close();
  store.close();
});

test("keeps live next-up fallback responses on the pre-reload config after a reload", async () => {
  const passwordHash = await hash("secret");
  const runtimeConfig = new MutableRuntimeConfig({
    server: { bind: "127.0.0.1", port: 8096, publicUrl: "http://bridge.test", name: "Bridge" },
    auth: { users: [{ name: "alice", passwordHash }] },
    upstreams: [{ id: "main", name: "Main", url: "https://main.example.com", token: "old-token" }],
    libraries: [{ id: "shows", name: "Shows", collectionType: "tvshows", sources: [{ server: "main", libraryId: "main-shows" }] }]
  });
  const store = new Store(":memory:");
  store.upsertIndexedItem({
    serverId: "main",
    itemId: "main-episode",
    libraryId: "main-shows",
    itemType: "Episode",
    logicalKey: "episode:main-episode",
    json: {
      Id: "main-episode",
      Type: "Episode",
      Name: "Cached Old NextUp",
      SeriesId: "main-series",
      SeasonId: "main-season",
      ProviderIds: {}
    }
  });
  let releaseUsers!: () => void;
  let usersStarted!: () => void;
  const releaseUsersPromise = new Promise<void>((resolve) => {
    releaseUsers = resolve;
  });
  const usersStartedPromise = new Promise<void>((resolve) => {
    usersStarted = resolve;
  });
  const clients: ReloadingLiveUpstream[] = [];
  const app = buildApp({
    config: runtimeConfig,
    store,
    upstreamFactory: (upstreams) => {
      const upstreamIds = new Set(upstreams.map((upstream) => upstream.id));
      const client = new ReloadingLiveUpstream(upstreamIds, upstreamIds.has("main")
        ? async () => {
          usersStarted();
          await releaseUsersPromise;
        }
        : undefined, undefined, undefined, undefined, new Error("Upstream main request failed for /Shows/NextUp: offline"));
      clients.push(client);
      return client;
    }
  });

  const login = await app.inject({ method: "POST", url: "/Users/AuthenticateByName", payload: { Username: "alice", Pw: "secret" } });
  const nextUpPromise = app.inject({
    method: "GET",
    url: "/Shows/NextUp",
    headers: { "X-MediaBrowser-Token": login.json().AccessToken }
  });
  await usersStartedPromise;

  runtimeConfig.update({
    server: { bind: "127.0.0.1", port: 8096, publicUrl: "http://bridge.test", name: "Reloaded Bridge" },
    auth: { users: [{ name: "alice", passwordHash }] },
    upstreams: [{ id: "remote", name: "Remote", url: "https://remote.example.com", token: "new-token" }],
    libraries: [{ id: "shows", name: "Shows", collectionType: "tvshows", sources: [{ server: "remote", libraryId: "remote-shows" }] }]
  });
  releaseUsers();

  const nextUp = await nextUpPromise;

  assert.equal(nextUp.statusCode, 200, nextUp.body);
  assert.deepEqual(nextUp.json().Items.map((item: Record<string, unknown>) => item.Name), ["Cached Old NextUp"]);
  assert.equal(nextUp.json().Items[0].ServerId, bridgeServerId("Bridge"));
  assert.deepEqual(clients[0].requests.map((request) => `${request.serverId}:${request.path}`), ["main:/Users", "main:/Shows/NextUp"]);
  assert.equal(clients[1].requests.length, 0);

  await app.close();
  store.close();
});

test("keeps in-flight live browse responses on the pre-reload config and upstream client", async () => {
  const passwordHash = await hash("secret");
  const runtimeConfig = new MutableRuntimeConfig({
    server: { bind: "127.0.0.1", port: 8096, publicUrl: "http://bridge.test", name: "Bridge" },
    auth: { users: [{ name: "alice", passwordHash }] },
    upstreams: [{ id: "main", name: "Main", url: "https://main.example.com", token: "old-token" }],
    libraries: [{ id: "movies", name: "Movies", collectionType: "movies", sources: [{ server: "main", libraryId: "main-movies" }] }]
  });
  const store = new Store(":memory:");
  let releaseItems!: () => void;
  let itemsStarted!: () => void;
  const releaseItemsPromise = new Promise<void>((resolve) => {
    releaseItems = resolve;
  });
  const itemsStartedPromise = new Promise<void>((resolve) => {
    itemsStarted = resolve;
  });
  const clients: ReloadingLiveUpstream[] = [];
  const app = buildApp({
    config: runtimeConfig,
    store,
    upstreamFactory: (upstreams) => {
      const upstreamIds = new Set(upstreams.map((upstream) => upstream.id));
      const client = new ReloadingLiveUpstream(upstreamIds, undefined, upstreamIds.has("main")
        ? async () => {
          itemsStarted();
          await releaseItemsPromise;
        }
        : undefined);
      clients.push(client);
      return client;
    }
  });

  const login = await app.inject({ method: "POST", url: "/Users/AuthenticateByName", payload: { Username: "alice", Pw: "secret" } });
  const browsePromise = app.inject({
    method: "GET",
    url: `/Items?ParentId=${bridgeLibraryId("movies")}&Limit=10`,
    headers: { "X-MediaBrowser-Token": login.json().AccessToken }
  });
  await itemsStartedPromise;

  runtimeConfig.update({
    server: { bind: "127.0.0.1", port: 8096, publicUrl: "http://bridge.test", name: "Bridge" },
    auth: { users: [{ name: "alice", passwordHash }] },
    upstreams: [{ id: "remote", name: "Remote", url: "https://remote.example.com", token: "new-token" }],
    libraries: [{ id: "movies", name: "Movies", collectionType: "movies", sources: [{ server: "remote", libraryId: "remote-movies" }] }]
  });
  releaseItems();

  const browse = await browsePromise;

  assert.equal(browse.statusCode, 200, browse.body);
  assert.deepEqual(browse.json().Items.map((item: Record<string, unknown>) => item.Name), ["Old Main Browse"]);
  assert.deepEqual(clients[0].requests.map((request) => `${request.serverId}:${request.path}`), ["main:/Items"]);
  assert.equal(clients[1].requests.length, 0);

  await app.close();
  store.close();
});

test("keeps in-flight live season refresh responses on the pre-reload config and upstream client", async () => {
  const passwordHash = await hash("secret");
  const runtimeConfig = new MutableRuntimeConfig({
    server: { bind: "127.0.0.1", port: 8096, publicUrl: "http://bridge.test", name: "Bridge" },
    auth: { users: [{ name: "alice", passwordHash }] },
    upstreams: [{ id: "main", name: "Main", url: "https://main.example.com", token: "old-token" }],
    libraries: [{ id: "shows", name: "Shows", collectionType: "tvshows", sources: [{ server: "main", libraryId: "main-shows" }] }]
  });
  const store = new Store(":memory:");
  const seriesBridgeId = bridgeItemId("series:main-series");
  store.upsertIndexedItem({
    serverId: "main",
    itemId: "main-series",
    libraryId: "main-shows",
    itemType: "Series",
    logicalKey: "series:main-series",
    json: { Id: "main-series", Type: "Series", Name: "Old Main Series", ProviderIds: {} }
  });
  let releaseSeasons!: () => void;
  let seasonsStarted!: () => void;
  const releaseSeasonsPromise = new Promise<void>((resolve) => {
    releaseSeasons = resolve;
  });
  const seasonsStartedPromise = new Promise<void>((resolve) => {
    seasonsStarted = resolve;
  });
  const clients: ReloadingLiveUpstream[] = [];
  const app = buildApp({
    config: runtimeConfig,
    store,
    upstreamFactory: (upstreams) => {
      const upstreamIds = new Set(upstreams.map((upstream) => upstream.id));
      const client = new ReloadingLiveUpstream(upstreamIds, undefined, undefined, upstreamIds.has("main")
        ? async () => {
          seasonsStarted();
          await releaseSeasonsPromise;
        }
        : undefined);
      clients.push(client);
      return client;
    }
  });

  const login = await app.inject({ method: "POST", url: "/Users/AuthenticateByName", payload: { Username: "alice", Pw: "secret" } });
  const seasonsPromise = app.inject({
    method: "GET",
    url: `/Shows/${seriesBridgeId}/Seasons`,
    headers: { "X-MediaBrowser-Token": login.json().AccessToken }
  });
  await seasonsStartedPromise;

  runtimeConfig.update({
    server: { bind: "127.0.0.1", port: 8096, publicUrl: "http://bridge.test", name: "Reloaded Bridge" },
    auth: { users: [{ name: "alice", passwordHash }] },
    upstreams: [{ id: "remote", name: "Remote", url: "https://remote.example.com", token: "new-token" }],
    libraries: [{ id: "shows", name: "Shows", collectionType: "tvshows", sources: [{ server: "remote", libraryId: "remote-shows" }] }]
  });
  releaseSeasons();

  const seasons = await seasonsPromise;

  assert.equal(seasons.statusCode, 200, seasons.body);
  assert.deepEqual(seasons.json().Items.map((item: Record<string, unknown>) => item.Name), ["Old Main Season"]);
  assert.equal(seasons.json().Items[0].ServerId, bridgeServerId("Bridge"));
  assert.deepEqual(clients[0].requests.map((request) => `${request.serverId}:${request.path}`), ["main:/Shows/main-series/Seasons"]);
  assert.equal(clients[1].requests.length, 0);

  await app.close();
  store.close();
});

test("returns explicit unsupported response instead of upstream passthrough", async () => {
  const passwordHash = await hash("secret");
  const config: BridgeConfig = {
    server: { bind: "127.0.0.1", port: 8096, publicUrl: "http://bridge.test", name: "Bridge" },
    auth: { users: [{ name: "alice", passwordHash }] },
    upstreams: [{ id: "main", name: "Main", url: "https://jellyfin.example.com", token: "token" }],
    libraries: []
  };
  const store = new Store(":memory:");
  const app = buildApp({ config, store });

  const response = await app.inject({ method: "POST", url: "/System/Shutdown" });

  assert.equal(response.statusCode, 501);
  assert.equal(response.json().title, "Not Implemented");

  for (const request of [
    { method: "GET", url: "/Plugins" },
    { method: "GET", url: "/Packages" },
    { method: "GET", url: "/LiveTv/Channels" },
    { method: "POST", url: "/QuickConnect/Initiate" },
    { method: "POST", url: "/SyncPlay/NewGroup" }
  ] as const) {
    const unsupportedResponse = await app.inject(request);
    assert.equal(unsupportedResponse.statusCode, 501, `${request.method} ${request.url}`);
    assert.equal(unsupportedResponse.json().title, "Not Implemented");
  }

  await app.close();
  store.close();
});

class MutableRuntimeConfig implements RuntimeConfigSource {
  private listeners = new Set<(config: BridgeConfig) => void>();

  constructor(private config: BridgeConfig) {}

  current(): BridgeConfig {
    return this.config;
  }

  subscribe(listener: (config: BridgeConfig) => void): () => void {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  }

  update(config: BridgeConfig): void {
    this.config = config;
    for (const listener of this.listeners) {
      listener(config);
    }
  }
}

class ReloadingLiveUpstream {
  readonly requests: Array<{ serverId: string; path: string; init: unknown }> = [];

  constructor(
    private readonly upstreamIds: Set<string>,
    private readonly beforeUsersResponse?: () => Promise<void>,
    private readonly beforeItemsResponse?: () => Promise<void>,
    private readonly beforeSeasonsResponse?: () => Promise<void>,
    private readonly latestError?: Error,
    private readonly nextUpError?: Error
  ) {}

  async json<T>(serverId: string, path: string, init: unknown): Promise<T> {
    if (!this.upstreamIds.has(serverId)) {
      throw new Error(`Unknown upstream ${serverId}`);
    }
    this.requests.push({ serverId, path, init });
    if (path === "/Users") {
      await this.beforeUsersResponse?.();
      return [{ Id: `${serverId}-user`, Name: "alice" }] as unknown as T;
    }
    if (path === "/Items/Latest") {
      if (this.latestError) throw this.latestError;
      return [
        {
          Id: `${serverId}-latest`,
          Type: "Movie",
          Name: "Old Main Latest",
          DateCreated: "2026-05-03T00:00:00.000Z",
          ProviderIds: {}
        }
      ] as unknown as T;
    }
    if (path === "/Items") {
      await this.beforeItemsResponse?.();
      return {
        Items: [
          {
            Id: `${serverId}-browse`,
            Type: "Movie",
            Name: "Old Main Browse",
            ProviderIds: {}
          }
        ],
        TotalRecordCount: 1,
        StartIndex: 0
      } as unknown as T;
    }
    if (path.endsWith("/Seasons")) {
      await this.beforeSeasonsResponse?.();
      return {
        Items: [
          {
            Id: `${serverId}-season-1`,
            Type: "Season",
            Name: "Old Main Season",
            SeriesId: "main-series",
            IndexNumber: 1,
            ProviderIds: {}
          }
        ],
        TotalRecordCount: 1,
        StartIndex: 0
      } as unknown as T;
    }
    if (path === "/Shows/NextUp") {
      if (this.nextUpError) throw this.nextUpError;
      return { Items: [], TotalRecordCount: 0, StartIndex: 0 } as unknown as T;
    }
    throw new Error(`Unexpected upstream request ${serverId}:${path}`);
  }
}

test("deletes indexed items through the resolved upstream source", async () => {
  const passwordHash = await hash("secret");
  const config: BridgeConfig = {
    server: { bind: "127.0.0.1", port: 8096, publicUrl: "http://bridge.test", name: "Bridge" },
    auth: { users: [{ name: "alice", passwordHash }] },
    upstreams: [
      { id: "main", name: "Main", url: "https://main.example.com", token: "token" },
      { id: "remote", name: "Remote", url: "https://remote.example.com", token: "token" }
    ],
    libraries: [{ id: "movies", name: "Movies", collectionType: "movies", sources: [{ server: "main", libraryId: "library-a" }, { server: "remote", libraryId: "library-b" }] }]
  };
  const store = new Store(":memory:");
  store.upsertIndexedItem({
    serverId: "main",
    itemId: "main-alien",
    libraryId: "library-a",
    itemType: "Movie",
    logicalKey: "movie:imdb:tt0078748",
    json: { Id: "main-alien", Type: "Movie", Name: "Alien", ProviderIds: { Imdb: "tt0078748" }, MediaSources: [{ Id: "source-main", ItemId: "main-alien" }] }
  });
  store.upsertIndexedItem({
    serverId: "remote",
    itemId: "remote-alien",
    libraryId: "library-b",
    itemType: "Movie",
    logicalKey: "movie:imdb:tt0078748",
    json: { Id: "remote-alien", Type: "Movie", Name: "Alien", ProviderIds: { Imdb: "tt0078748" }, MediaSources: [{ Id: "source-remote", ItemId: "remote-alien" }] }
  });
  store.upsertIndexedItem({
    serverId: "remote",
    itemId: "remote-thing",
    libraryId: "library-b",
    itemType: "Movie",
    logicalKey: "movie:tmdb:1091",
    json: { Id: "remote-thing", Type: "Movie", Name: "The Thing", ProviderIds: { Tmdb: "1091" } }
  });
  store.upsertIndexedItem({
    serverId: "remote",
    itemId: "remote-empty-json-delete",
    libraryId: "library-b",
    itemType: "Movie",
    logicalKey: "movie:tmdb:2222",
    json: { Id: "remote-empty-json-delete", Type: "Movie", Name: "Empty JSON Delete", ProviderIds: { Tmdb: "2222" } }
  });
  store.upsertMediaSourceMapping({
    bridgeMediaSourceId: "remote-thing-source",
    serverId: "remote",
    upstreamItemId: "remote-thing",
    upstreamMediaSourceId: "source-thing"
  });
  const playbackSession = store.createPlaybackSessionMapping({
    serverId: "remote",
    upstreamPlaySessionId: "upstream-play-session",
    upstreamItemId: "remote-thing",
    bridgeItemId: bridgeItemId("movie:tmdb:1091")
  });

  const upstream = new FakeUpstream({});
  upstream.rawResponses["remote:/Items/remote-thing"] = { statusCode: 204, headers: {}, body: "" };
  upstream.rawResponses["remote:/Items/remote-empty-json-delete"] = { statusCode: 204, headers: {}, body: "" };
  upstream.rawResponses["main:/Items/main-alien"] = { statusCode: 204, headers: {}, body: "" };
  const app = buildApp({ config, store, upstream });
  const login = await app.inject({ method: "POST", url: "/Users/AuthenticateByName", payload: { Username: "alice", Pw: "secret" } });
  const token = login.json().AccessToken;

  const upstreamIdDelete = await app.inject({
    method: "DELETE",
    url: "/Items/remote-thing",
    headers: { "X-MediaBrowser-Token": token }
  });
  assert.equal(upstreamIdDelete.statusCode, 204);
  assert.equal(upstreamIdDelete.body, "");
  assert.deepEqual(upstream.rawRequests[0], {
    serverId: "remote",
    path: "/Items/remote-thing",
    init: { method: "DELETE" },
    headers: {}
  });
  assert.equal(store.listIndexedItems().some((item) => item.serverId === "remote" && item.itemId === "remote-thing"), false);
  assert.equal(store.findMediaSourceMapping("remote-thing-source"), undefined);
  assert.equal(store.findPlaybackSessionMapping(playbackSession.bridgePlaySessionId), undefined);

  const bridgeIdDelete = await app.inject({
    method: "DELETE",
    url: `/Items/${bridgeItemId("movie:imdb:tt0078748")}`,
    headers: { "X-MediaBrowser-Token": token }
  });
  assert.equal(bridgeIdDelete.statusCode, 204);
  assert.equal(upstream.rawRequests[1].serverId, "main");
  assert.equal(upstream.rawRequests[1].path, "/Items/main-alien");
  assert.equal(upstream.rawRequests[1].init.method, "DELETE");
  assert.equal(store.listIndexedItems().some((item) => item.serverId === "main" && item.itemId === "main-alien"), false);
  assert.equal(store.listIndexedItems().some((item) => item.serverId === "remote" && item.itemId === "remote-alien"), true);

  const emptyJsonBodyDelete = await app.inject({
    method: "DELETE",
    url: "/Items/remote-empty-json-delete",
    headers: { "X-MediaBrowser-Token": token, "Content-Type": "application/json" }
  });
  assert.equal(emptyJsonBodyDelete.statusCode, 204, emptyJsonBodyDelete.body);
  assert.equal(upstream.rawRequests[2].serverId, "remote");
  assert.equal(upstream.rawRequests[2].path, "/Items/remote-empty-json-delete");
  assert.equal(upstream.rawRequests[2].init.method, "DELETE");

  const unknownDelete = await app.inject({
    method: "DELETE",
    url: "/Items/not-indexed",
    headers: { "X-MediaBrowser-Token": token }
  });
  assert.equal(unknownDelete.statusCode, 404);
  assert.equal(upstream.rawRequests.length, 3);

  await app.close();
  store.close();
});

test("rejects cross-user state access for non-admin bridge users", async () => {
  const passwordHash = await hash("secret");
  const config: BridgeConfig = {
    server: { bind: "127.0.0.1", port: 8096, publicUrl: "http://bridge.test", name: "Bridge" },
    auth: { users: [{ name: "alice", passwordHash }, { name: "bob", passwordHash }] },
    upstreams: [{ id: "main", name: "Main", url: "https://jellyfin.example.com", token: "token" }],
    libraries: []
  };
  const store = new Store(":memory:");
  const app = buildApp({ config, store });

  const aliceLogin = await app.inject({ method: "POST", url: "/Users/AuthenticateByName", payload: { Username: "alice", Pw: "secret" } });
  const bobLogin = await app.inject({ method: "POST", url: "/Users/AuthenticateByName", payload: { Username: "bob", Pw: "secret" } });
  const aliceToken = aliceLogin.json().AccessToken;
  const bobId = bobLogin.json().User.Id;
  const itemId = "0123456789abcdef0123456789abcdef";

  for (const request of [
    { method: "GET", url: `/Users/${bobId}/Items` },
    { method: "GET", url: `/UserItems/${itemId}/UserData?UserId=${bobId}` },
    { method: "GET", url: `/UserItems/${itemId}/UserData?userId=${bobId}` },
    { method: "POST", url: `/Users/${bobId}/Items/${itemId}/UserData`, payload: { IsFavorite: true } },
    { method: "POST", url: `/Users/${bobId}/FavoriteItems/${itemId}` },
    { method: "DELETE", url: `/Users/${bobId}/PlayedItems/${itemId}` }
  ] as const) {
    const response = await app.inject({
      ...request,
      headers: { "X-MediaBrowser-Token": aliceToken }
    });
    assert.equal(response.statusCode, 403, `${request.method} ${request.url}`);
  }

  assert.equal(store.getUserData(bobId, itemId).IsFavorite, false);

  await app.close();
  store.close();
});

test("serves indexed items as merged bridge items", async () => {
  const passwordHash = await hash("secret");
  const config: BridgeConfig = {
    server: { bind: "127.0.0.1", port: 8096, publicUrl: "http://bridge.test", name: "Bridge" },
    auth: { users: [{ name: "alice", passwordHash }] },
    upstreams: [
      { id: "main", name: "Main", url: "https://main.example.com", token: "token" },
      { id: "remote", name: "Remote", url: "https://remote.example.com", token: "token" }
    ],
    libraries: [{ id: "movies", name: "Movies", collectionType: "movies", sources: [{ server: "main", libraryId: "library-a" }, { server: "remote", libraryId: "library-b" }] }]
  };
  const store = new Store(":memory:");
  store.upsertIndexedItem({
    serverId: "main",
    itemId: "main-alien",
    libraryId: "library-a",
    itemType: "Movie",
    logicalKey: "movie:imdb:tt0078748",
    json: { Id: "main-alien", Type: "Movie", Name: "Alien", ServerId: "main", Genres: ["Horror"], DateCreated: "2024-01-01T00:00:00.000Z", ProviderIds: { Imdb: "tt0078748" } }
  });
  store.upsertIndexedItem({
    serverId: "remote",
    itemId: "remote-alien",
    libraryId: "library-b",
    itemType: "Movie",
    logicalKey: "movie:imdb:tt0078748",
    json: { Id: "remote-alien", Type: "Movie", Name: "Alien", ServerId: "remote", Genres: ["Horror"], DateCreated: "2024-01-01T00:00:00.000Z", ProviderIds: { Imdb: "tt0078748" } }
  });
  store.upsertIndexedItem({
    serverId: "remote",
    itemId: "remote-thing",
    libraryId: "library-b",
    itemType: "Movie",
    logicalKey: "movie:tmdb:1091",
    json: { Id: "remote-thing", Type: "Movie", Name: "The Thing", ServerId: "remote", Genres: ["Horror"], DateCreated: "2024-03-01T00:00:00.000Z", ProviderIds: { Tmdb: "1091" } }
  });
  store.upsertIndexedItem({
    serverId: "remote",
    itemId: "remote-arrival",
    libraryId: "library-b",
    itemType: "Movie",
    logicalKey: "movie:tmdb:329865",
    json: { Id: "remote-arrival", Type: "Movie", Name: "Arrival", ServerId: "remote", Genres: ["Science Fiction"], DateCreated: "2024-02-01T00:00:00.000Z", ProviderIds: { Tmdb: "329865" } }
  });
  const app = buildApp({ config, store });

  const login = await app.inject({
    method: "POST",
    url: "/Users/AuthenticateByName",
    payload: { Username: "alice", Pw: "secret" }
  });
  const auth = login.json();
  const token = auth.AccessToken;
  const alienBridgeId = bridgeItemId("movie:imdb:tt0078748");

  const updateUserData = await app.inject({
    method: "POST",
    url: `/Users/${auth.User.Id}/Items/${alienBridgeId}/UserData`,
    headers: { "X-MediaBrowser-Token": token },
    payload: { IsFavorite: true }
  });
  assert.equal(updateUserData.statusCode, 200);

  const items = await app.inject({ method: "GET", url: "/Items?IncludeItemTypes=Movie", headers: { "X-MediaBrowser-Token": token } });
  assert.equal(items.statusCode, 200);
  assert.equal(items.json().TotalRecordCount, 3);
  assert.deepEqual(items.json().Items.map((item: any) => item.Name), ["Alien", "Arrival", "The Thing"]);
  assert.equal(items.json().Items[0].Id, alienBridgeId);
  assert.equal(items.json().Items[0].UserData.IsFavorite, true);

  const lowerCasePagedItems = await app.inject({
    method: "GET",
    url: "/Items?includeItemTypes=Movie&startIndex=1&limit=1",
    headers: { "X-MediaBrowser-Token": token }
  });
  assert.equal(lowerCasePagedItems.statusCode, 200);
  assert.equal(lowerCasePagedItems.json().TotalRecordCount, 3);
  assert.equal(lowerCasePagedItems.json().StartIndex, 1);
  assert.deepEqual(lowerCasePagedItems.json().Items.map((item: any) => item.Name), ["Arrival"]);

  const legacyUnfavorite = await app.inject({
    method: "DELETE",
    url: `/Users/${auth.User.Id}/FavoriteItems/${alienBridgeId}`,
    headers: { "X-MediaBrowser-Token": token }
  });
  assert.equal(legacyUnfavorite.statusCode, 200);
  assert.equal(legacyUnfavorite.json().IsFavorite, false);

  const legacyFavorite = await app.inject({
    method: "POST",
    url: `/Users/${auth.User.Id}/FavoriteItems/${alienBridgeId}`,
    headers: { "X-MediaBrowser-Token": token }
  });
  assert.equal(legacyFavorite.statusCode, 200);
  assert.equal(legacyFavorite.json().IsFavorite, true);

  const filteredByItemFilter = await app.inject({
    method: "GET",
    url: "/Items?IncludeItemTypes=Movie&Filters=IsFavorite",
    headers: { "X-MediaBrowser-Token": token }
  });
  assert.equal(filteredByItemFilter.statusCode, 200);
  assert.deepEqual(filteredByItemFilter.json().Items.map((item: any) => item.Name), ["Alien"]);

  const filtered = await app.inject({
    method: "GET",
    url: "/Items?IncludeItemTypes=Movie&Genres=Horror&IsFavorite=true&SortBy=SortName&SortOrder=Descending",
    headers: { "X-MediaBrowser-Token": token }
  });
  assert.equal(filtered.statusCode, 200);
  assert.equal(filtered.json().TotalRecordCount, 1);
  assert.deepEqual(filtered.json().Items.map((item: any) => item.Name), ["Alien"]);

  const item = await app.inject({ method: "GET", url: `/Items/${alienBridgeId}`, headers: { "X-MediaBrowser-Token": token } });
  assert.equal(item.statusCode, 200);
  assert.equal(item.json().Name, "Alien");

  const detailUpstream = new FakeUpstream({
    "main:/Items/main-alien": {
      Id: "main-alien",
      Type: "Movie",
      Name: "Alien",
      Overview: "A salvage crew finds something hostile.",
      ServerId: "main",
      MediaSources: [{ Id: "source-1", ItemId: "main-alien", Path: "/media/alien.mkv" }],
      MediaStreams: [{ Type: "Video", Codec: "h264" }],
      UserData: { ItemId: "main-alien", Played: true }
    }
  });
  const detailApp = buildApp({ config, store, upstream: detailUpstream });
  const hydratedItem = await detailApp.inject({
    method: "GET",
    url: `/Items/${alienBridgeId}?userId=${auth.User.Id}&fields=Overview,MediaSources,MediaStreams`,
    headers: { "X-MediaBrowser-Token": token }
  });
  assert.equal(hydratedItem.statusCode, 200);
  assert.equal(hydratedItem.json().Overview, "A salvage crew finds something hostile.");
  assert.equal(hydratedItem.json().Id, alienBridgeId);
  assert.equal(hydratedItem.json().MediaSources[0].ItemId, alienBridgeId);
  assert.equal(hydratedItem.json().UserData.IsFavorite, true);
  assert.equal(store.findMediaSourceMapping(hydratedItem.json().MediaSources[0].Id)?.upstreamMediaSourceId, "source-1");
  assert.deepEqual(detailUpstream.requests[0], {
    serverId: "main",
    path: "/Items/main-alien",
    init: { query: { fields: "Overview,MediaSources,MediaStreams" } }
  });
  const legacyHydratedItem = await detailApp.inject({
    method: "GET",
    url: `/Users/${auth.User.Id}/Items/${alienBridgeId}?fields=Overview,MediaSources,MediaStreams`,
    headers: { "X-MediaBrowser-Token": token }
  });
  assert.equal(legacyHydratedItem.statusCode, 200);
  assert.equal(legacyHydratedItem.json().Overview, "A salvage crew finds something hostile.");
  assert.equal(legacyHydratedItem.json().Id, alienBridgeId);
  assert.equal(legacyHydratedItem.json().MediaSources[0].ItemId, alienBridgeId);
  assert.equal(legacyHydratedItem.json().UserData.IsFavorite, true);
  assert.deepEqual(detailUpstream.requests[1], {
    serverId: "main",
    path: "/Items/main-alien",
    init: { query: { fields: "Overview,MediaSources,MediaStreams" } }
  });
  const localTrailers = await detailApp.inject({
    method: "GET",
    url: `/Items/${alienBridgeId}/LocalTrailers?userId=${auth.User.Id}&fields=DateCreated,Etag,MediaSources`,
    headers: { "X-MediaBrowser-Token": token }
  });
  assert.equal(localTrailers.statusCode, 200);
  assert.deepEqual(localTrailers.json(), { Items: [], TotalRecordCount: 0, StartIndex: 0 });
  const specialFeatures = await detailApp.inject({
    method: "GET",
    url: `/Users/${auth.User.Id}/Items/${alienBridgeId}/SpecialFeatures`,
    headers: { "X-MediaBrowser-Token": token }
  });
  assert.equal(specialFeatures.statusCode, 200);
  assert.deepEqual(specialFeatures.json(), { Items: [], TotalRecordCount: 0, StartIndex: 0 });
  await detailApp.close();

  const upstream = new FakeUpstream({});
  upstream.rawResponses["main:/Items/main-alien/Images/Primary"] = {
    statusCode: 200,
    headers: { "content-type": "image/jpeg", "content-length": "3" },
    body: Buffer.from("jpg")
  };
  upstream.rawResponses["main:/Items/main-alien/Images/Logo"] = {
    statusCode: 200,
    headers: { "content-type": "image/png", "content-length": "3" },
    body: Readable.from(Buffer.from("png"))
  };
  const imageApp = buildApp({ config, store, upstream });
  const image = await imageApp.inject({
    method: "GET",
    url: `/Items/${alienBridgeId}/Images/Primary`,
    headers: { "X-MediaBrowser-Token": token }
  });
  assert.equal(image.statusCode, 200);
  assert.equal(image.headers["content-type"], "image/jpeg");
  assert.equal(image.body, "jpg");
  const unauthenticatedImage = await imageApp.inject({
    method: "GET",
    url: `/Items/${alienBridgeId}/Images/Primary?maxWidth=400&tag=abc&quality=90`
  });
  assert.equal(unauthenticatedImage.statusCode, 200);
  assert.equal(unauthenticatedImage.headers["content-type"], "image/jpeg");
  assert.equal(unauthenticatedImage.body, "jpg");
  const rawRequestCount = upstream.rawRequests.length;
  const traversalImage = await imageApp.inject({
    method: "GET",
    url: `/Items/${alienBridgeId}/Images/Primary/%2e%2e%2f%2e%2e%2f%2e%2e%2f%2e%2e%2fSystem%2fInfo`
  });
  assert.equal(traversalImage.statusCode, 404);
  assert.equal(upstream.rawRequests.length, rawRequestCount);
  const streamImage = await imageApp.inject({
    method: "GET",
    url: `/Items/${alienBridgeId}/Images/Logo`,
    headers: { "X-MediaBrowser-Token": token }
  });
  assert.equal(streamImage.statusCode, 200);
  assert.equal(streamImage.headers["content-type"], "image/png");
  assert.equal(streamImage.body, "png");
  await imageApp.close();

  const fallbackUpstream = new FakeUpstream({});
  fallbackUpstream.rawResponses["remote:/Items/remote-alien/Images/Primary"] = {
    statusCode: 200,
    headers: { "content-type": "image/jpeg", "content-length": "6" },
    body: Buffer.from("remote")
  };
  const fallbackImageApp = buildApp({ config, store, upstream: fallbackUpstream });
  const fallbackImage = await fallbackImageApp.inject({
    method: "GET",
    url: `/Items/${alienBridgeId}/Images/Primary`,
    headers: { "X-MediaBrowser-Token": token }
  });
  assert.equal(fallbackImage.statusCode, 200);
  assert.equal(fallbackImage.headers["content-type"], "image/jpeg");
  assert.equal(fallbackImage.body, "remote");
  assert.deepEqual(fallbackUpstream.rawRequests.map((request) => `${request.serverId}:${request.path}`), [
    "main:/Items/main-alien/Images/Primary",
    "remote:/Items/remote-alien/Images/Primary"
  ]);
  await fallbackImageApp.close();

  const failingImageApp = buildApp({ config, store, upstream: new FakeUpstream({}) });
  const failingImage = await failingImageApp.inject({
    method: "GET",
    url: `/Items/${alienBridgeId}/Images/Primary`,
    headers: { "X-MediaBrowser-Token": token }
  });
  assert.equal(failingImage.statusCode, 404);
  assert.equal(failingImage.json().title, "Not Found");
  await failingImageApp.close();

  const counts = await app.inject({ method: "GET", url: "/Items/Counts", headers: { "X-MediaBrowser-Token": token } });
  assert.equal(counts.statusCode, 200);
  assert.equal(counts.json().MovieCount, 3);
  assert.equal(counts.json().ItemCount, 3);

  const search = await app.inject({ method: "GET", url: "/Search/Hints?SearchTerm=thing", headers: { "X-MediaBrowser-Token": token } });
  assert.equal(search.statusCode, 200);
  assert.equal(search.json().TotalRecordCount, 1);
  assert.equal(search.json().SearchHints[0].Name, "The Thing");

  const progress = await app.inject({
    method: "POST",
    url: "/Sessions/Playing/Progress",
    headers: { "X-MediaBrowser-Token": token },
    payload: { ItemId: alienBridgeId, PositionTicks: 9000 }
  });
  assert.equal(progress.statusCode, 204);

  const resumable = await app.inject({
    method: "GET",
    url: "/Items?IncludeItemTypes=Movie&Filters=IsResumable",
    headers: { "X-MediaBrowser-Token": token }
  });
  assert.equal(resumable.statusCode, 200);
  assert.deepEqual(resumable.json().Items.map((item: any) => item.Name), ["Alien"]);

  const resume = await app.inject({ method: "GET", url: "/UserItems/Resume", headers: { "X-MediaBrowser-Token": token } });
  assert.equal(resume.statusCode, 200);
  assert.deepEqual(resume.json().Items.map((item: any) => item.Name), ["Alien"]);

  const legacyResume = await app.inject({
    method: "GET",
    url: `/Users/${auth.User.Id}/Items/Resume?ParentId=${bridgeLibraryId("movies")}&IncludeItemTypes=Movie`,
    headers: { "X-MediaBrowser-Token": token }
  });
  assert.equal(legacyResume.statusCode, 200);
  assert.deepEqual(legacyResume.json().Items.map((item: any) => item.Name), ["Alien"]);

  const legacyPlayed = await app.inject({
    method: "POST",
    url: `/Users/${auth.User.Id}/PlayedItems/${alienBridgeId}`,
    headers: { "X-MediaBrowser-Token": token }
  });
  assert.equal(legacyPlayed.statusCode, 200);
  assert.equal(legacyPlayed.json().Played, true);

  const legacyUnplayed = await app.inject({
    method: "DELETE",
    url: `/Users/${auth.User.Id}/PlayedItems/${alienBridgeId}`,
    headers: { "X-MediaBrowser-Token": token }
  });
  assert.equal(legacyUnplayed.statusCode, 200);
  assert.equal(legacyUnplayed.json().Played, false);

  const latest = await app.inject({ method: "GET", url: "/Items/Latest?Limit=1", headers: { "X-MediaBrowser-Token": token } });
  assert.equal(latest.statusCode, 200);
  assert.equal(latest.json().length, 1);
  assert.equal(latest.json()[0].Name, "The Thing");

  const legacyLatest = await app.inject({
    method: "GET",
    url: `/Users/${auth.User.Id}/Items/Latest?ParentId=${bridgeLibraryId("movies")}&IncludeItemTypes=Movie&Limit=2`,
    headers: { "X-MediaBrowser-Token": token }
  });
  assert.equal(legacyLatest.statusCode, 200);
  assert.deepEqual(legacyLatest.json().map((item: any) => item.Name), ["The Thing", "Arrival"]);

  const suggestions = await app.inject({
    method: "GET",
    url: "/Items/Suggestions?Type=Movie&Limit=2&EnableTotalRecordCount=true",
    headers: { "X-MediaBrowser-Token": token }
  });
  assert.equal(suggestions.statusCode, 200);
  assert.equal(suggestions.json().TotalRecordCount, 3);
  assert.deepEqual(suggestions.json().Items.map((item: any) => item.Name), ["Alien", "Arrival"]);

  const legacySuggestions = await app.inject({
    method: "GET",
    url: `/Users/${auth.User.Id}/Suggestions?Type=Movie&StartIndex=1&Limit=1`,
    headers: { "X-MediaBrowser-Token": token }
  });
  assert.equal(legacySuggestions.statusCode, 200);
  assert.equal(legacySuggestions.json().StartIndex, 1);
  assert.deepEqual(legacySuggestions.json().Items.map((item: any) => item.Name), ["Arrival"]);

  await app.close();
  store.close();
});

test("serves mapped library browse by live fan-out without a prior scan", async () => {
  const passwordHash = await hash("secret");
  const config: BridgeConfig = {
    server: { bind: "127.0.0.1", port: 8096, publicUrl: "http://bridge.test", name: "Bridge" },
    auth: { users: [{ name: "alice", passwordHash }] },
    upstreams: [
      { id: "main", name: "Main", url: "https://main.example.com", token: "token" },
      { id: "remote", name: "Remote", url: "https://remote.example.com", token: "token" }
    ],
    libraries: [{
      id: "feature-films",
      name: "Feature Films",
      collectionType: "movies",
      sources: [{ server: "main", libraryId: "library-a" }, { server: "remote", libraryId: "library-b" }]
    }]
  };
  const upstream = new FakeUpstream({
    "main:/Items": {
      Items: [
        { Id: "main-shared", Type: "Movie", Name: "Shared Title", SortName: "Shared Title", ProviderIds: { Tmdb: "100" }, MediaSources: [{ Id: "source-main", ItemId: "main-shared" }] }
      ],
      TotalRecordCount: 1,
      StartIndex: 0
    },
    "remote:/Items": {
      Items: [
        { Id: "remote-shared", Type: "Movie", Name: "Shared Title", SortName: "Shared Title", ProviderIds: { Tmdb: "100" }, MediaSources: [{ Id: "source-remote", ItemId: "remote-shared" }] },
        { Id: "remote-unique", Type: "Movie", Name: "Unique Title", SortName: "Unique Title", ProviderIds: { Tmdb: "200" }, MediaSources: [{ Id: "source-unique", ItemId: "remote-unique" }] }
      ],
      TotalRecordCount: 2,
      StartIndex: 0
    }
  });
  const store = new Store(":memory:");
  const app = buildApp({ config, store, upstream });
  const login = await app.inject({ method: "POST", url: "/Users/AuthenticateByName", payload: { Username: "alice", Pw: "secret" } });
  const token = login.json().AccessToken;
  const parentId = bridgeLibraryId("feature-films");

  const response = await app.inject({
    method: "GET",
    url: `/Items?ParentId=${parentId}&IncludeItemTypes=Movie&SortBy=SortName&StartIndex=1&Limit=1`,
    headers: { "X-MediaBrowser-Token": token }
  });

  assert.equal(response.statusCode, 200);
  assert.equal(response.json().TotalRecordCount, 2);
  assert.equal(response.json().StartIndex, 1);
  assert.deepEqual(response.json().Items.map((item: any) => item.Name), ["Unique Title"]);
  assert.equal(store.findIndexedItemsByBridgeId(bridgeItemId("movie:tmdb:100")).length, 2);
  assert.equal(store.findIndexedItemsByBridgeId(bridgeItemId("movie:tmdb:200")).length, 1);
  assert.deepEqual(upstream.requests.map((request) => `${request.serverId}:${request.path}`), ["main:/Items", "remote:/Items"]);
  assert.equal((upstream.requests[0].init as any).query.ParentId, "library-a");
  assert.equal((upstream.requests[1].init as any).query.ParentId, "library-b");

  await app.close();
  store.close();
});

test("serves mapped library browse from online sources when another source is unavailable", async () => {
  const passwordHash = await hash("secret");
  const config: BridgeConfig = {
    server: { bind: "127.0.0.1", port: 8096, publicUrl: "http://bridge.test", name: "Bridge" },
    auth: { users: [{ name: "alice", passwordHash }] },
    upstreams: [
      { id: "primary", name: "Primary", url: "https://primary.example.com", token: "token" },
      { id: "secondary", name: "Secondary", url: "https://secondary.example.com", token: "token" }
    ],
    libraries: [{
      id: "movies",
      name: "Movies",
      collectionType: "movies",
      sources: [{ server: "primary", libraryId: "primary-movies" }, { server: "secondary", libraryId: "secondary-movies" }]
    }]
  };
  const upstream = new FakeUpstream({
    "primary:/Items": new Error("Upstream primary request failed for /Items: getaddrinfo ENOTFOUND jellyfin-primary.example.com"),
    "secondary:/Items": {
      Items: [{ Id: "movie-a", Type: "Movie", Name: "Online Movie", ProviderIds: { Tmdb: "100" } }],
      TotalRecordCount: 1,
      StartIndex: 0
    }
  });
  const store = new Store(":memory:");
  const app = buildApp({ config, store, upstream });
  const login = await app.inject({ method: "POST", url: "/Users/AuthenticateByName", payload: { Username: "alice", Pw: "secret" } });

  const response = await app.inject({
    method: "GET",
    url: `/Items?ParentId=${bridgeLibraryId("movies")}&IncludeItemTypes=Movie`,
    headers: { "X-MediaBrowser-Token": login.json().AccessToken }
  });

  assert.equal(response.statusCode, 200);
  assert.deepEqual(response.json().Items.map((item: any) => item.Name), ["Online Movie"]);

  await app.close();
  store.close();
});

test("serves TV seasons and episodes live after a scan-free series browse", async () => {
  const passwordHash = await hash("secret");
  const config: BridgeConfig = {
    server: { bind: "127.0.0.1", port: 8096, publicUrl: "http://bridge.test", name: "Bridge" },
    auth: { users: [{ name: "alice", passwordHash }] },
    upstreams: [{ id: "main", name: "Main", url: "https://main.example.com", token: "token" }],
    libraries: [{ id: "shows", name: "Series", collectionType: "tvshows", sources: [{ server: "main", libraryId: "shows-lib" }] }]
  };
  const upstream = new FakeUpstream({
    "main:/Items": {
      Items: [{ Id: "series-a", Type: "Series", Name: "Example Series", ProviderIds: { Tvdb: "100" } }],
      TotalRecordCount: 1,
      StartIndex: 0
    },
    "main:/Shows/series-a/Seasons": {
      Items: [{ Id: "season-a", Type: "Season", Name: "Season 1", SeriesId: "series-a", IndexNumber: 1 }],
      TotalRecordCount: 1,
      StartIndex: 0
    },
    "main:/Shows/series-a/Episodes": {
      Items: [{ Id: "episode-a", Type: "Episode", Name: "Pilot", SeriesId: "series-a", SeasonId: "season-a", ParentIndexNumber: 1, IndexNumber: 1 }],
      TotalRecordCount: 1,
      StartIndex: 0
    }
  });
  const store = new Store(":memory:");
  const app = buildApp({ config, store, upstream });
  const login = await app.inject({ method: "POST", url: "/Users/AuthenticateByName", payload: { Username: "alice", Pw: "secret" } });
  const token = login.json().AccessToken;
  const parentId = bridgeLibraryId("shows");

  const series = await app.inject({
    method: "GET",
    url: `/Items?ParentId=${parentId}&IncludeItemTypes=Series`,
    headers: { "X-MediaBrowser-Token": token }
  });
  assert.equal(series.statusCode, 200);
  const seriesId = series.json().Items[0].Id;

  const seasons = await app.inject({
    method: "GET",
    url: `/Shows/${seriesId}/Seasons?Fields=ProviderIds`,
    headers: { "X-MediaBrowser-Token": token }
  });
  assert.equal(seasons.statusCode, 200);
  assert.deepEqual(seasons.json().Items.map((item: any) => item.Name), ["Season 1"]);
  const seasonId = seasons.json().Items[0].Id;

  const episodes = await app.inject({
    method: "GET",
    url: `/Shows/${seriesId}/Episodes?SeasonId=${seasonId}`,
    headers: { "X-MediaBrowser-Token": token }
  });
  assert.equal(episodes.statusCode, 200);
  assert.deepEqual(episodes.json().Items.map((item: any) => item.Name), ["Pilot"]);
  assert.deepEqual(upstream.requests.map((request) => `${request.serverId}:${request.path}`), [
    "main:/Items",
    "main:/Shows/series-a/Seasons",
    "main:/Shows/series-a/Episodes"
  ]);

  await app.close();
  store.close();
});

test("serves pass-through library browse live without indexed items", async () => {
  const passwordHash = await hash("secret");
  const config: BridgeConfig = {
    server: { bind: "127.0.0.1", port: 8096, publicUrl: "http://bridge.test", name: "Bridge" },
    auth: { users: [{ name: "alice", passwordHash }] },
    upstreams: [{ id: "main", name: "Main", url: "https://main.example.com", token: "token" }],
    libraries: []
  };
  const upstream = new FakeUpstream({
    "main:/Items": {
      Items: [{ Id: "video-a", Type: "Movie", Name: "Standalone Video", ProviderIds: { Tmdb: "300" } }],
      TotalRecordCount: 1,
      StartIndex: 0
    }
  });
  const store = new Store(":memory:");
  store.upsertUpstreamLibrary({ serverId: "main", libraryId: "library-a", name: "Archive", collectionType: "movies" });
  const app = buildApp({ config, store, upstream });
  const login = await app.inject({ method: "POST", url: "/Users/AuthenticateByName", payload: { Username: "alice", Pw: "secret" } });
  const token = login.json().AccessToken;
  const parentId = passThroughLibraryId("main", "library-a");

  const response = await app.inject({
    method: "GET",
    url: `/Items?ParentId=${parentId}&IncludeItemTypes=Movie`,
    headers: { "X-MediaBrowser-Token": token }
  });

  assert.equal(response.statusCode, 200);
  assert.equal(response.json().TotalRecordCount, 1);
  assert.equal(response.json().Items[0].Name, "Standalone Video");
  assert.equal((upstream.requests[0].init as any).query.ParentId, "library-a");

  await app.close();
  store.close();
});

test("discovers unmapped upstream views live without a scan", async () => {
  const passwordHash = await hash("secret");
  const config: BridgeConfig = {
    server: { bind: "127.0.0.1", port: 8096, publicUrl: "http://bridge.test", name: "Bridge" },
    auth: { users: [{ name: "alice", passwordHash }] },
    upstreams: [{ id: "main", name: "Main", url: "https://main.example.com", token: "token" }],
    libraries: []
  };
  const upstream = new FakeUpstream({
    "main:/Users": [{ Id: "upstream-user", Name: "Service" }],
    "main:/UserViews": {
      Items: [{ Id: "library-a", Name: "Archive", CollectionType: "movies" }],
      TotalRecordCount: 1,
      StartIndex: 0
    }
  });
  const store = new Store(":memory:");
  const app = buildApp({ config, store, upstream });
  const login = await app.inject({ method: "POST", url: "/Users/AuthenticateByName", payload: { Username: "alice", Pw: "secret" } });

  const views = await app.inject({
    method: "GET",
    url: `/UserViews?userId=${login.json().User.Id}&includeExternalContent=false`,
    headers: { "X-MediaBrowser-Token": login.json().AccessToken }
  });

  assert.equal(views.statusCode, 200);
  assert.deepEqual(views.json().Items.map((item: any) => ({
    Name: item.Name,
    CollectionType: item.CollectionType
  })), [{ Name: "Main - Archive", CollectionType: "movies" }]);
  assert.deepEqual(store.listUpstreamLibraries().map((library) => library.libraryId), ["library-a"]);

  await app.close();
  store.close();
});

test("discovers views from online upstreams when another upstream is unavailable", async () => {
  const passwordHash = await hash("secret");
  const config: BridgeConfig = {
    server: { bind: "127.0.0.1", port: 8096, publicUrl: "http://bridge.test", name: "Bridge" },
    auth: { users: [{ name: "alice", passwordHash }] },
    upstreams: [
      { id: "primary", name: "Primary", url: "https://primary.example.com", token: "token" },
      { id: "secondary", name: "Secondary", url: "https://secondary.example.com", token: "token" }
    ],
    libraries: []
  };
  const upstream = new FakeUpstream({
    "primary:/Users": new Error("Upstream primary request failed for /Users: getaddrinfo ENOTFOUND jellyfin-primary.example.com"),
    "secondary:/Users": [{ Id: "upstream-user", Name: "alice" }],
    "secondary:/UserViews": {
      Items: [{ Id: "anime-lib", Name: "Anime", CollectionType: "tvshows" }],
      TotalRecordCount: 1,
      StartIndex: 0
    }
  });
  const store = new Store(":memory:");
  const app = buildApp({ config, store, upstream });
  const login = await app.inject({ method: "POST", url: "/Users/AuthenticateByName", payload: { Username: "alice", Pw: "secret" } });

  const views = await app.inject({
    method: "GET",
    url: `/UserViews?userId=${login.json().User.Id}&includeExternalContent=false`,
    headers: { "X-MediaBrowser-Token": login.json().AccessToken }
  });

  assert.equal(views.statusCode, 200);
  assert.deepEqual(views.json().Items.map((item: any) => item.Name), ["Secondary - Anime"]);

  await app.close();
  store.close();
});

test("exposes unmapped upstream libraries as prefixed pass-through views", async () => {
  const passwordHash = await hash("secret");
  const config: BridgeConfig = {
    server: { bind: "127.0.0.1", port: 8096, publicUrl: "http://bridge.test", name: "Bridge" },
    auth: { users: [{ name: "alice", passwordHash }] },
    upstreams: [{ id: "main", name: "Main", url: "https://main.example.com", token: "token" }],
    libraries: [{ id: "movies", name: "Movies", collectionType: "movies", sources: [{ server: "main", libraryId: "library-a" }] }]
  };
  const store = new Store(":memory:");
  store.upsertUpstreamLibrary({ serverId: "main", libraryId: "library-a", name: "Movies", collectionType: "movies" });
  store.upsertUpstreamLibrary({ serverId: "main", libraryId: "library-c", name: "TV", collectionType: "tvshows" });
  store.upsertUpstreamLibrary({ serverId: "main", libraryId: "library-d", name: "Home Videos", collectionType: "homevideos" });
  const app = buildApp({ config, store });
  const login = await app.inject({ method: "POST", url: "/Users/AuthenticateByName", payload: { Username: "alice", Pw: "secret" } });

  const views = await app.inject({
    method: "GET",
    url: "/UserViews",
    headers: { "X-MediaBrowser-Token": login.json().AccessToken }
  });

  assert.equal(views.statusCode, 200);
  assert.deepEqual(views.json().Items.map((item: any) => item.Name), ["Movies", "Main - TV", "Main - Home Videos"]);
  assert.equal(views.json().Items[1].CollectionType, "tvshows");

  store.upsertIndexedItem({
    serverId: "main",
    itemId: "tv-series",
    libraryId: "library-c",
    itemType: "Series",
    logicalKey: "source:main:tv-series",
    json: { Id: "tv-series", Type: "Series", Name: "Pass Through Show" }
  });
  store.upsertIndexedItem({
    serverId: "main",
    itemId: "mapped-movie",
    libraryId: "library-a",
    itemType: "Movie",
    logicalKey: "movie:tmdb:1",
    json: { Id: "mapped-movie", Type: "Movie", Name: "Mapped Movie" }
  });
  store.upsertIndexedItem({
    serverId: "main",
    itemId: "home-video",
    libraryId: "library-d",
    itemType: "Video",
    logicalKey: "source:main:home-video",
    json: { Id: "home-video", Type: "Video", MediaType: "Video", Name: "Pass Through Video" }
  });
  const items = await app.inject({
    method: "GET",
    url: `/Items?ParentId=${passThroughLibraryId("main", "library-c")}`,
    headers: { "X-MediaBrowser-Token": login.json().AccessToken }
  });
  assert.equal(items.statusCode, 200);
  assert.deepEqual(items.json().Items.map((item: any) => item.Name), ["Pass Through Show"]);

  const mappedItems = await app.inject({
    method: "GET",
    url: `/Items?ParentId=${bridgeLibraryId("movies")}`,
    headers: { "X-MediaBrowser-Token": login.json().AccessToken }
  });
  assert.equal(mappedItems.statusCode, 200);
  assert.deepEqual(mappedItems.json().Items.map((item: any) => item.Name), ["Mapped Movie"]);

  const movieFilteredVideos = await app.inject({
    method: "GET",
    url: `/Items?ParentId=${passThroughLibraryId("main", "library-d")}&IncludeItemTypes=Movie`,
    headers: { "X-MediaBrowser-Token": login.json().AccessToken }
  });
  assert.equal(movieFilteredVideos.statusCode, 200);
  assert.equal(movieFilteredVideos.json().TotalRecordCount, 1);
  assert.deepEqual(movieFilteredVideos.json().Items.map((item: any) => ({ Name: item.Name, Type: item.Type })), [
    { Name: "Pass Through Video", Type: "Movie" }
  ]);

  await app.close();
  store.close();
});

test("runs an authenticated manual scan through the bridge", async () => {
  const passwordHash = await hash("secret");
  const config: BridgeConfig = {
    server: { bind: "127.0.0.1", port: 8096, publicUrl: "http://bridge.test", name: "Bridge" },
    auth: { users: [{ name: "alice", passwordHash }] },
    upstreams: [{ id: "main", name: "Main", url: "https://main.example.com", token: "token" }],
    libraries: [{ id: "movies", name: "Movies", collectionType: "movies", sources: [{ server: "main", libraryId: "library-a" }] }]
  };
  const store = new Store(":memory:");
  const upstream = new FakeUpstream({
    "main:/Users": [{ Id: "user-a" }],
    "main:/UserViews": { Items: [{ Id: "library-a", Name: "Movies", CollectionType: "movies" }], TotalRecordCount: 1, StartIndex: 0 },
    "main:/Items": { Items: [{ Id: "movie-a", Type: "Movie", Name: "Scanned Movie", ProviderIds: { Tmdb: "1" } }], TotalRecordCount: 1, StartIndex: 0 }
  });
  const app = buildApp({ config, store, upstream });
  const login = await app.inject({ method: "POST", url: "/Users/AuthenticateByName", payload: { Username: "alice", Pw: "secret" } });

  const scan = await app.inject({ method: "POST", url: "/Bridge/Scan", headers: { "X-MediaBrowser-Token": login.json().AccessToken } });

  assert.equal(scan.statusCode, 204);
  assert.equal(store.listIndexedItems()[0].itemId, "movie-a");

  await app.close();
  store.close();
});

test("serves indexed genre, artist, album artist, and person browse metadata", async () => {
  const passwordHash = await hash("secret");
  const config: BridgeConfig = {
    server: { bind: "127.0.0.1", port: 8096, publicUrl: "http://bridge.test", name: "Bridge" },
    auth: { users: [{ name: "alice", passwordHash }] },
    upstreams: [{ id: "main", name: "Main", url: "https://main.example.com", token: "token" }],
    libraries: [
      { id: "music", name: "Music", collectionType: "music", sources: [{ server: "main", libraryId: "music-lib" }] },
      { id: "movies", name: "Movies", collectionType: "movies", sources: [{ server: "main", libraryId: "movie-lib" }] }
    ]
  };
  const store = new Store(":memory:");
  store.upsertIndexedItem({
    serverId: "main",
    itemId: "song-a",
    libraryId: "music-lib",
    itemType: "Audio",
    logicalKey: "track:mb:recording-a",
    json: {
      Id: "song-a",
      Type: "Audio",
      MediaType: "Audio",
      Name: "Song A",
      Genres: ["Rock"],
      Artists: ["Example Band"],
      AlbumArtist: "Example Band",
      People: [{ Name: "Singer One", Type: "Artist" }]
    }
  });
  store.upsertIndexedItem({
    serverId: "main",
    itemId: "movie-a",
    libraryId: "movie-lib",
    itemType: "Movie",
    logicalKey: "movie:tmdb:99",
    json: {
      Id: "movie-a",
      Type: "Movie",
      MediaType: "Video",
      Name: "Movie A",
      Genres: ["Drama"],
      Tags: ["Festival"],
      Studios: [{ Name: "Example Studio" }],
      ProductionYear: 2024,
      OfficialRating: "PG-13",
      People: [{ Name: "Director One", Type: "Director" }]
    }
  });
  store.upsertIndexedItem({
    serverId: "main",
    itemId: "movie-b",
    libraryId: "movie-lib",
    itemType: "Movie",
    logicalKey: "movie:tmdb:100",
    json: {
      Id: "movie-b",
      Type: "Movie",
      MediaType: "Video",
      Name: "Movie B",
      Genres: ["Comedy"],
      Tags: ["Archive"],
      Studios: ["Other Studio"],
      ProductionYear: 2023,
      OfficialRating: "R",
      People: [{ Name: "Director Two", Type: "Director" }]
    }
  });
  const app = buildApp({ config, store });
  const login = await app.inject({ method: "POST", url: "/Users/AuthenticateByName", payload: { Username: "alice", Pw: "secret" } });
  const token = login.json().AccessToken;

  const genres = await app.inject({
    method: "GET",
    url: `/Genres?ParentId=${bridgeLibraryId("music")}`,
    headers: { "X-MediaBrowser-Token": token }
  });
  assert.equal(genres.statusCode, 200);
  assert.deepEqual(genres.json().Items.map((item: any) => [item.Name, item.Type]), [["Rock", "Genre"]]);

  const artists = await app.inject({
    method: "GET",
    url: "/Artists?SearchTerm=band",
    headers: { "X-MediaBrowser-Token": token }
  });
  assert.equal(artists.statusCode, 200);
  assert.equal(artists.json().Items[0].Name, "Example Band");

  const audioItems = await app.inject({
    method: "GET",
    url: "/Items?MediaTypes=Audio",
    headers: { "X-MediaBrowser-Token": token }
  });
  assert.equal(audioItems.statusCode, 200);
  assert.deepEqual(audioItems.json().Items.map((item: any) => item.Name), ["Song A"]);

  const albumArtists = await app.inject({
    method: "GET",
    url: "/Artists/AlbumArtists",
    headers: { "X-MediaBrowser-Token": token }
  });
  assert.equal(albumArtists.statusCode, 200);
  assert.deepEqual(albumArtists.json().Items.map((item: any) => item.Name), ["Example Band"]);

  const persons = await app.inject({
    method: "GET",
    url: "/Persons?PersonTypes=Director&SearchTerm=One",
    headers: { "X-MediaBrowser-Token": token }
  });
  assert.equal(persons.statusCode, 200);
  assert.deepEqual(persons.json().Items.map((item: any) => item.Name), ["Director One"]);

  const person = await app.inject({
    method: "GET",
    url: "/Persons/Director%20One",
    headers: { "X-MediaBrowser-Token": token }
  });
  assert.equal(person.statusCode, 200);
  assert.equal(person.json().Type, "Person");

  const studios = await app.inject({
    method: "GET",
    url: "/Studios?SearchTerm=example",
    headers: { "X-MediaBrowser-Token": token }
  });
  assert.equal(studios.statusCode, 200);
  assert.deepEqual(studios.json().Items.map((item: any) => [item.Name, item.Type]), [["Example Studio", "Studio"]]);

  const years = await app.inject({
    method: "GET",
    url: `/Years?ParentId=${bridgeLibraryId("movies")}&SearchTerm=2024`,
    headers: { "X-MediaBrowser-Token": token }
  });
  assert.equal(years.statusCode, 200);
  assert.deepEqual(years.json().Items.map((item: any) => [item.Name, item.Type]), [["2024", "Year"]]);

  const legacyFilters = await app.inject({
    method: "GET",
    url: `/Items/Filters?ParentId=${bridgeLibraryId("movies")}&IncludeItemTypes=Movie`,
    headers: { "X-MediaBrowser-Token": token }
  });
  assert.equal(legacyFilters.statusCode, 200);
  assert.deepEqual(legacyFilters.json().Years, [2023, 2024]);
  assert.deepEqual(legacyFilters.json().Genres, ["Comedy", "Drama"]);
  assert.deepEqual(legacyFilters.json().Tags, ["Archive", "Festival"]);
  assert.deepEqual(legacyFilters.json().OfficialRatings, ["PG-13", "R"]);

  const filters = await app.inject({
    method: "GET",
    url: `/Items/Filters2?ParentId=${bridgeLibraryId("movies")}&IncludeItemTypes=Movie`,
    headers: { "X-MediaBrowser-Token": token }
  });
  assert.equal(filters.statusCode, 200);
  assert.deepEqual(filters.json().Genres.map((item: any) => item.Name), ["Comedy", "Drama"]);
  assert.match(filters.json().Genres[0].Id, /^[0-9a-f]{32}$/);

  const filteredItems = await app.inject({
    method: "GET",
    url: `/Items?ParentId=${bridgeLibraryId("movies")}&IncludeItemTypes=Movie&Years=2024&OfficialRatings=PG-13&Studios=Example%20Studio&Tags=Festival`,
    headers: { "X-MediaBrowser-Token": token }
  });
  assert.equal(filteredItems.statusCode, 200);
  assert.deepEqual(filteredItems.json().Items.map((item: any) => item.Name), ["Movie A"]);

  await app.close();
  store.close();
});

test("serves TV seasons and episodes from indexed bridge items", async () => {
  const passwordHash = await hash("secret");
  const config: BridgeConfig = {
    server: { bind: "127.0.0.1", port: 8096, publicUrl: "http://bridge.test", name: "Bridge" },
    auth: { users: [{ name: "alice", passwordHash }] },
    upstreams: [{ id: "main", name: "Main", url: "https://main.example.com", token: "token" }],
    libraries: [{ id: "shows", name: "Shows", collectionType: "tvshows", sources: [{ server: "main", libraryId: "shows-lib" }] }]
  };
  const store = new Store(":memory:");
  store.upsertIndexedItem({
    serverId: "main",
    itemId: "series-a",
    libraryId: "shows-lib",
    itemType: "Series",
    logicalKey: "series:tvdb:100",
    json: { Id: "series-a", Type: "Series", Name: "Example Show", ProviderIds: { Tvdb: "100" } }
  });
  store.upsertIndexedItem({
    serverId: "main",
    itemId: "season-a",
    libraryId: "shows-lib",
    itemType: "Season",
    logicalKey: "season:series:series-a:season:1",
    json: { Id: "season-a", Type: "Season", Name: "Season 1", SeriesId: "series-a", IndexNumber: 1 }
  });
  store.upsertIndexedItem({
    serverId: "main",
    itemId: "episode-a",
    libraryId: "shows-lib",
    itemType: "Episode",
    logicalKey: "episode:series:series-a:season:1:episode:1",
    json: { Id: "episode-a", Type: "Episode", Name: "Pilot", SeriesId: "series-a", SeasonId: "season-a", ParentIndexNumber: 1, IndexNumber: 1 }
  });
  const app = buildApp({ config, store });
  const login = await app.inject({ method: "POST", url: "/Users/AuthenticateByName", payload: { Username: "alice", Pw: "secret" } });
  const token = login.json().AccessToken;
  const seriesId = bridgeItemId("series:tvdb:100");

  const seasons = await app.inject({ method: "GET", url: `/Shows/${seriesId}/Seasons`, headers: { "X-MediaBrowser-Token": token } });
  assert.equal(seasons.statusCode, 200);
  assert.equal(seasons.json().Items[0].Name, "Season 1");

  const episodes = await app.inject({ method: "GET", url: `/Shows/${seriesId}/Episodes?Season=1`, headers: { "X-MediaBrowser-Token": token } });
  assert.equal(episodes.statusCode, 200);
  assert.equal(episodes.json().Items[0].Name, "Pilot");

  const seasonId = seasons.json().Items[0].Id;
  const episodesBySeasonId = await app.inject({
    method: "GET",
    url: `/Shows/unknown-series/Episodes?seasonId=${seasonId}&StartIndex=0&Limit=50`,
    headers: { "X-MediaBrowser-Token": token }
  });
  assert.equal(episodesBySeasonId.statusCode, 200);
  assert.equal(episodesBySeasonId.json().Items[0].Name, "Pilot");

  const childItems = await app.inject({
    method: "GET",
    url: `/Items?ParentId=${seasonId}&StartIndex=0&Limit=50`,
    headers: { "X-MediaBrowser-Token": token }
  });
  assert.equal(childItems.statusCode, 200);
  assert.equal(childItems.json().Items[0].Name, "Pilot");

  const nextUp = await app.inject({ method: "GET", url: "/Shows/NextUp", headers: { "X-MediaBrowser-Token": token } });
  assert.equal(nextUp.statusCode, 200);
  assert.equal(nextUp.json().Items[0].Name, "Pilot");

  const upcoming = await app.inject({ method: "GET", url: "/Shows/Upcoming", headers: { "X-MediaBrowser-Token": token } });
  assert.equal(upcoming.statusCode, 200);
  assert.deepEqual(upcoming.json().Items, []);

  await app.close();
  store.close();
});

test("resolves PlaybackInfo through the priority upstream source and stores media source mappings", async () => {
  const passwordHash = await hash("secret");
  const config: BridgeConfig = {
    server: { bind: "127.0.0.1", port: 8096, publicUrl: "http://bridge.test", name: "Bridge" },
    auth: { users: [{ name: "alice", passwordHash }] },
    upstreams: [
      { id: "main", name: "Main", url: "https://main.example.com", token: "token" },
      { id: "remote", name: "Remote", url: "https://remote.example.com", token: "token" }
    ],
    libraries: [{ id: "movies", name: "Movies", collectionType: "movies", sources: [{ server: "main", libraryId: "library-a" }, { server: "remote", libraryId: "library-b" }] }]
  };
  const store = new Store(":memory:");
  store.upsertIndexedItem({
    serverId: "main",
    itemId: "main-alien",
    libraryId: "library-a",
    itemType: "Movie",
    logicalKey: "movie:imdb:tt0078748",
    json: { Id: "main-alien", Type: "Movie", Name: "Alien", ProviderIds: { Imdb: "tt0078748" } }
  });
  store.upsertIndexedItem({
    serverId: "remote",
    itemId: "remote-alien",
    libraryId: "library-b",
    itemType: "Movie",
    logicalKey: "movie:imdb:tt0078748",
    json: { Id: "remote-alien", Type: "Movie", Name: "Alien", ProviderIds: { Imdb: "tt0078748" } }
  });
  const upstream = new FakeUpstream({
    "main:/Items/main-alien/PlaybackInfo": {
      MediaSources: [{ Id: "source-main", Path: "/media/alien.mkv" }],
      PlaySessionId: "upstream-play-session"
    }
  });
  const app = buildApp({ config, store, upstream });
  const login = await app.inject({ method: "POST", url: "/Users/AuthenticateByName", payload: { Username: "alice", Pw: "secret" } });
  const token = login.json().AccessToken;
  const itemId = bridgeItemId("movie:imdb:tt0078748");

  const playback = await app.inject({
    method: "POST",
    url: `/Items/${itemId}/PlaybackInfo`,
    headers: { "X-MediaBrowser-Token": token },
    payload: { UserId: login.json().User.Id }
  });

  assert.equal(playback.statusCode, 200);
  assert.equal(upstream.requests[0].serverId, "main");
  assert.equal(upstream.requests[0].path, "/Items/main-alien/PlaybackInfo");
  assert.equal(playback.json().MediaSources[0].ItemId, itemId);
  assert.match(playback.json().MediaSources[0].Id, /^[0-9a-f]{32}$/);
  assert.match(playback.json().PlaySessionId, /^[0-9a-f]{64}$/);
  assert.notEqual(playback.json().PlaySessionId, "upstream-play-session");
  assert.equal(store.findMediaSourceMapping(playback.json().MediaSources[0].Id)?.upstreamMediaSourceId, "source-main");

  const selectedPlayback = await app.inject({
    method: "POST",
    url: `/Items/${itemId}/PlaybackInfo?MediaSourceId=${playback.json().MediaSources[0].Id}`,
    headers: { "X-MediaBrowser-Token": token },
    payload: { MediaSourceId: playback.json().MediaSources[0].Id }
  });
  assert.equal(selectedPlayback.statusCode, 200);
  assert.equal((upstream.requests[1].init as any).query.MediaSourceId, "source-main");
  assert.equal((upstream.requests[1].init as any).body.MediaSourceId, "source-main");

  upstream.rawResponses["main:/Videos/main-alien/stream.mkv"] = {
    statusCode: 206,
    headers: {
      "content-type": "video/x-matroska",
      "content-range": "bytes 0-3/10",
      "accept-ranges": "bytes",
      "content-length": "4"
    },
    body: Readable.from(Buffer.from("data"))
  };
  const stream = await app.inject({
    method: "GET",
    url: `/Videos/${itemId}/stream.mkv?MediaSourceId=${playback.json().MediaSources[0].Id}`,
    headers: { "X-MediaBrowser-Token": token, Range: "bytes=0-3" }
  });
  assert.equal(stream.statusCode, 206);
  assert.equal(stream.headers["content-type"], "video/x-matroska");
  assert.equal(stream.headers["content-range"], "bytes 0-3/10");
  assert.equal(stream.body, "data");
  assert.equal(upstream.rawRequests[0].headers.range, "bytes=0-3");

  upstream.rawResponses["main:/Videos/main-alien/stream"] = {
    statusCode: 200,
    headers: { "content-type": "video/x-matroska", "content-length": "4" },
    body: Readable.from(Buffer.from("data"))
  };
  const extensionlessStream = await app.inject({
    method: "GET",
    url: `/Videos/${itemId}/stream?MediaSourceId=${playback.json().MediaSources[0].Id}&Static=true`,
    headers: { "X-MediaBrowser-Token": token }
  });
  assert.equal(extensionlessStream.statusCode, 200);
  assert.equal(extensionlessStream.headers["content-type"], "video/x-matroska");
  assert.equal(extensionlessStream.body, "data");

  const detailMediaSourceId = bridgeMediaSourceId("main", itemId, "source-main");
  upstream.rawResponses["main:/Videos/main-alien/stream"] = {
    statusCode: 206,
    headers: { "content-type": "video/x-matroska", "content-range": "bytes 0-3/10", "content-length": "4" },
    body: Readable.from(Buffer.from("data"))
  };
  const extensionlessStreamFromIndexedMediaSource = await app.inject({
    method: "GET",
    url: `/Videos/${itemId}/stream?MediaSourceId=${detailMediaSourceId}&Static=true`,
    headers: { "X-MediaBrowser-Token": token, Range: "bytes=0-3" }
  });
  assert.equal(extensionlessStreamFromIndexedMediaSource.statusCode, 206);
  assert.equal(extensionlessStreamFromIndexedMediaSource.body, "data");

  const mediaSegments = await app.inject({
    method: "GET",
    url: `/MediaSegments/${itemId}`,
    headers: { "X-MediaBrowser-Token": token }
  });
  assert.equal(mediaSegments.statusCode, 200);
  assert.deepEqual(mediaSegments.json(), { Items: [], TotalRecordCount: 0, StartIndex: 0 });

  upstream.rawResponses["main:/Videos/main-alien/master.m3u8"] = {
    statusCode: 200,
    headers: { "content-type": "application/vnd.apple.mpegurl" },
    body: Buffer.from("#EXTM3U\nmain.m3u8\n")
  };
  const hls = await app.inject({
    method: "GET",
    url: `/Videos/${itemId}/master.m3u8?MediaSourceId=${playback.json().MediaSources[0].Id}`,
    headers: { "X-MediaBrowser-Token": token }
  });
  assert.equal(hls.statusCode, 200);
  assert.equal(hls.body, `#EXTM3U\n/Videos/${itemId}/hls/${playback.json().MediaSources[0].Id}/main.m3u8\n`);

  upstream.rawResponses[`main:/Videos/main-alien/hls/playlist/segment0.ts`] = {
    statusCode: 200,
    headers: { "content-type": "video/mp2t", "content-length": "3" },
    body: Buffer.from("seg")
  };
  const segment = await app.inject({
    method: "GET",
    url: `/Videos/${itemId}/hls/${playback.json().MediaSources[0].Id}/playlist/segment0.ts`,
    headers: { "X-MediaBrowser-Token": token }
  });
  assert.equal(segment.statusCode, 200);
  assert.equal(segment.headers["content-type"], "video/mp2t");
  assert.equal(segment.body, "seg");

  upstream.rawResponses["main:/Videos/main-alien/source-main/Subtitles/2/Stream.vtt"] = {
    statusCode: 200,
    headers: { "content-type": "text/vtt", "content-length": "6" },
    body: Buffer.from("WEBVTT")
  };
  const subtitles = await app.inject({
    method: "GET",
    url: `/Videos/${itemId}/${playback.json().MediaSources[0].Id}/Subtitles/2/Stream.vtt`,
    headers: { "X-MediaBrowser-Token": token }
  });
  assert.equal(subtitles.statusCode, 200);
  assert.equal(subtitles.headers["content-type"], "text/vtt");
  assert.equal(subtitles.body, "WEBVTT");

  upstream.rawResponses["main:/Videos/main-alien/source-main/Subtitles/2/0/Stream.srt"] = {
    statusCode: 200,
    headers: { "content-type": "text/plain", "content-length": "3" },
    body: Readable.from(Buffer.from("srt"))
  };
  const subtitlesWithTicks = await app.inject({
    method: "GET",
    url: `/Videos/${itemId}/${playback.json().MediaSources[0].Id}/Subtitles/2/0/Stream.srt`,
    headers: { "X-MediaBrowser-Token": token }
  });
  assert.equal(subtitlesWithTicks.statusCode, 200);
  assert.equal(subtitlesWithTicks.body, "srt");

  const progress = await app.inject({
    method: "POST",
    url: "/Sessions/Playing/Progress",
    headers: { "X-MediaBrowser-Token": token },
    payload: {
      ItemId: itemId,
      MediaSourceId: playback.json().MediaSources[0].Id,
      PositionTicks: 5000,
      PlaySessionId: playback.json().PlaySessionId
    }
  });
  assert.equal(progress.statusCode, 204);
  assert.equal(upstream.requests.at(-1)?.path, "/Sessions/Playing/Progress");
  assert.equal((upstream.requests.at(-1)?.init as any).body.ItemId, "main-alien");
  assert.equal((upstream.requests.at(-1)?.init as any).body.MediaSourceId, "source-main");
  assert.equal((upstream.requests.at(-1)?.init as any).body.PlaySessionId, "upstream-play-session");

  await app.close();
  store.close();
});

test("returns bad gateway when upstream PlaybackInfo fails", async () => {
  const passwordHash = await hash("secret");
  const config: BridgeConfig = {
    server: { bind: "127.0.0.1", port: 8096, publicUrl: "http://bridge.test", name: "Bridge" },
    auth: { users: [{ name: "alice", passwordHash }] },
    upstreams: [{ id: "main", name: "Main", url: "https://main.example.com", token: "token" }],
    libraries: [{ id: "movies", name: "Movies", collectionType: "movies", sources: [{ server: "main", libraryId: "library-a" }] }]
  };
  const store = new Store(":memory:");
  store.upsertIndexedItem({
    serverId: "main",
    itemId: "main-alien",
    libraryId: "library-a",
    itemType: "Movie",
    logicalKey: "movie:imdb:tt0078748",
    json: { Id: "main-alien", Type: "Movie", Name: "Alien", ProviderIds: { Imdb: "tt0078748" } }
  });
  const app = buildApp({ config, store, upstream: new FakeUpstream({}) });
  const login = await app.inject({ method: "POST", url: "/Users/AuthenticateByName", payload: { Username: "alice", Pw: "secret" } });

  const playback = await app.inject({
    method: "POST",
    url: `/Items/${bridgeItemId("movie:imdb:tt0078748")}/PlaybackInfo`,
    headers: { "X-MediaBrowser-Token": login.json().AccessToken },
    payload: { UserId: login.json().User.Id }
  });

  assert.equal(playback.statusCode, 502);
  assert.equal(playback.json().title, "Bad Gateway");

  await app.close();
  store.close();
});

test("keeps the server alive when an upstream closes a proxied stream", async () => {
  const passwordHash = await hash("secret");
  const config: BridgeConfig = {
    server: { bind: "127.0.0.1", port: 8096, publicUrl: "http://bridge.test", name: "Bridge" },
    auth: { users: [{ name: "alice", passwordHash }] },
    upstreams: [{ id: "main", name: "Main", url: "https://main.example.com", token: "token" }],
    libraries: [{ id: "movies", name: "Movies", collectionType: "movies", sources: [{ server: "main", libraryId: "library-a" }] }]
  };
  const store = new Store(":memory:");
  store.upsertMediaSourceMapping({
    bridgeMediaSourceId: "bridge-source",
    serverId: "main",
    upstreamItemId: "main-alien",
    upstreamMediaSourceId: "source-main"
  });
  const upstream = new FakeUpstream({});
  upstream.rawResponses["main:/Videos/main-alien/stream"] = {
    statusCode: 200,
    headers: { "content-type": "video/x-matroska" },
    body: upstreamSocketClosedStream()
  };
  const app = buildApp({ config, store, upstream });
  const login = await app.inject({ method: "POST", url: "/Users/AuthenticateByName", payload: { Username: "alice", Pw: "secret" } });

  const stream = await app.inject({
    method: "GET",
    url: "/Videos/bridge-alien/stream?MediaSourceId=bridge-source&Static=true",
    headers: { "X-MediaBrowser-Token": login.json().AccessToken }
  });

  assert.equal(stream.statusCode, 200);

  await app.close();
  store.close();
});

test("aggregates and proxies home, artwork, detail, and playback routes without indexed state", async () => {
  const passwordHash = await hash("secret");
  const config: BridgeConfig = {
    server: { bind: "127.0.0.1", port: 8096, publicUrl: "http://bridge.test", name: "Bridge" },
    auth: { users: [{ name: "alice", passwordHash }] },
    upstreams: [
      { id: "primary", name: "Primary", url: "https://primary.example.com", token: "token" },
      { id: "secondary", name: "Secondary", url: "https://secondary.example.com", token: "token" }
    ],
    libraries: [
      { id: "movies", name: "Movies", collectionType: "movies", sources: [{ server: "primary", libraryId: "primary-movies" }, { server: "secondary", libraryId: "secondary-movies" }] },
      { id: "tv", name: "TV", collectionType: "tvshows", sources: [{ server: "primary", libraryId: "primary-tv" }, { server: "secondary", libraryId: "secondary-tv" }] }
    ]
  };
  const upstream = new FakeUpstream({
    "primary:/Users": [{ Id: "primary-user", Name: "alice" }],
    "secondary:/Users": [{ Id: "secondary-user", Name: "alice" }],
    "primary:/Items/Latest": [
      { Id: "movie-a", Type: "Movie", Name: "Primary Movie", ProviderIds: { Tmdb: "100" }, DateCreated: "2026-01-03T00:00:00.000Z", ImageTags: { Primary: "primary-tag" }, ServerId: "primary" }
    ],
    "secondary:/Items/Latest": [
      { Id: "movie-a-copy", Type: "Movie", Name: "Primary Movie Copy", ProviderIds: { Tmdb: "100" }, DateCreated: "2026-01-02T00:00:00.000Z", ImageTags: { Primary: "copy-tag" }, ServerId: "secondary" },
      { Id: "movie-b", Type: "Movie", Name: "Secondary Movie", ProviderIds: { Tmdb: "200" }, DateCreated: "2026-01-04T00:00:00.000Z", ImageTags: { Primary: "equal-tag" }, ServerId: "secondary" }
    ],
    "primary:/UserItems/Resume": {
      Items: [{ Id: "resume-a", Type: "Movie", Name: "Resume Primary", ProviderIds: { Tmdb: "300" }, UserData: { PlaybackPositionTicks: 100 }, ServerId: "primary" }],
      TotalRecordCount: 1,
      StartIndex: 0
    },
    "secondary:/UserItems/Resume": {
      Items: [{ Id: "resume-b", Type: "Movie", Name: "Resume Secondary", ProviderIds: { Tmdb: "400" }, UserData: { PlaybackPositionTicks: 200 }, ServerId: "secondary" }],
      TotalRecordCount: 1,
      StartIndex: 0
    },
    "primary:/Shows/NextUp": {
      Items: [{ Id: "episode-a", Type: "Episode", Name: "Next Primary", SeriesId: "series-a", ProviderIds: { Tvdb: "500" }, ServerId: "primary" }],
      TotalRecordCount: 1,
      StartIndex: 0
    },
    "secondary:/Shows/NextUp": {
      Items: [{ Id: "episode-b", Type: "Episode", Name: "Next Secondary", SeriesId: "series-b", ProviderIds: { Tvdb: "600" }, ServerId: "secondary" }],
      TotalRecordCount: 1,
      StartIndex: 0
    },
    "primary:/Items/movie-a": {
      Id: "movie-a",
      Type: "Movie",
      Name: "Primary Movie Detail",
      ProviderIds: { Tmdb: "100" },
      MediaSources: [{ Id: "media-a", ItemId: "movie-a" }],
      ServerId: "primary"
    },
    "primary:/Shows/series-a/Seasons": {
      Items: [{ Id: "season-a", Type: "Season", Name: "Season 1", SeriesId: "series-a" }],
      TotalRecordCount: 1,
      StartIndex: 0
    },
    "primary:/Items/movie-a/PlaybackInfo": {
      MediaSources: [{ Id: "media-a", ItemId: "movie-a", Path: "/media/movie-a.mkv" }],
      PlaySessionId: "upstream-play-session"
    },
    "primary:/Sessions/Playing/Progress": {}
  });
  upstream.rawResponses["primary:/Items/movie-a/Images/Primary"] = {
    statusCode: 200,
    headers: { "content-type": "image/jpeg", "content-length": "3" },
    body: Buffer.from("jpg")
  };
  const store = new Store(":memory:");
  const app = buildApp({ config, store, upstream });
  const login = await app.inject({ method: "POST", url: "/Users/AuthenticateByName", payload: { Username: "alice", Pw: "secret" } });
  const token = login.json().AccessToken;

  const latest = await app.inject({
    method: "GET",
    url: `/Items/Latest?userId=${login.json().User.Id}&ParentId=${bridgeLibraryId("movies")}&IncludeItemTypes=Movie&Limit=2`,
    headers: { "X-MediaBrowser-Token": token }
  });
  assert.equal(latest.statusCode, 200);
  assert.deepEqual(latest.json().map((item: any) => [item.Id, item.Name]), [
    ["movie-b", "Secondary Movie"],
    ["movie-a", "Primary Movie"]
  ]);
  assert.equal((upstream.requests.find((request) => request.serverId === "primary" && request.path === "/Items/Latest")?.init as any).query.ParentId, "primary-movies");
  assert.equal((upstream.requests.find((request) => request.serverId === "secondary" && request.path === "/Items/Latest")?.init as any).query.ParentId, "secondary-movies");

  const resume = await app.inject({
    method: "GET",
    url: `/UserItems/Resume?userId=${login.json().User.Id}&limit=40&mediaTypes=Video&recursive=true`,
    headers: { "X-MediaBrowser-Token": token }
  });
  assert.equal(resume.statusCode, 200);
  assert.deepEqual(resume.json().Items.map((item: any) => item.Id), ["resume-a", "resume-b"]);
  assert.equal(resume.json().TotalRecordCount, 2);
  assert.equal((upstream.requests.find((request) => request.serverId === "primary" && request.path === "/UserItems/Resume")?.init as any).query.UserId, "primary-user");

  const nextUp = await app.inject({
    method: "GET",
    url: `/Shows/NextUp?userId=${login.json().User.Id}&startIndex=0&limit=20`,
    headers: { "X-MediaBrowser-Token": token }
  });
  assert.equal(nextUp.statusCode, 200);
  assert.deepEqual(nextUp.json().Items.map((item: any) => item.Id), ["episode-a", "episode-b"]);

  const detail = await app.inject({
    method: "GET",
    url: `/Items/movie-a?userId=${login.json().User.Id}&fields=MediaSources`,
    headers: { "X-MediaBrowser-Token": token }
  });
  assert.equal(detail.statusCode, 200);
  assert.equal(detail.json().Id, "movie-a");
  assert.equal(detail.json().MediaSources[0].Id, "media-a");

  const legacyDetail = await app.inject({
    method: "GET",
    url: `/Users/${login.json().User.Id}/Items/movie-a?fields=MediaSources`,
    headers: { "X-MediaBrowser-Token": token }
  });
  assert.equal(legacyDetail.statusCode, 200);
  assert.equal(legacyDetail.json().Id, "movie-a");
  assert.equal(legacyDetail.json().MediaSources[0].Id, "media-a");

  const seasons = await app.inject({
    method: "GET",
    url: `/Shows/series-a/Seasons?userId=${login.json().User.Id}&limit=200`,
    headers: { "X-MediaBrowser-Token": token }
  });
  assert.equal(seasons.statusCode, 200);
  assert.deepEqual(seasons.json().Items.map((item: any) => item.Id), ["season-a"]);

  const image = await app.inject({ method: "GET", url: "/Items/movie-a/Images/Primary?tag=primary-tag" });
  assert.equal(image.statusCode, 200);
  assert.equal(image.headers["content-type"], "image/jpeg");
  assert.equal(image.body, "jpg");

  const playback = await app.inject({
    method: "POST",
    url: "/Items/movie-a/PlaybackInfo",
    headers: { "X-MediaBrowser-Token": token },
    payload: { UserId: login.json().User.Id, MediaSourceId: "media-a" }
  });
  assert.equal(playback.statusCode, 200);
  assert.equal(playback.json().MediaSources[0].Id, "media-a");
  assert.equal(playback.json().PlaySessionId, "upstream-play-session");
  assert.equal(store.findMediaSourceMapping("media-a"), undefined);

  const progress = await app.inject({
    method: "POST",
    url: "/Sessions/Playing/Progress",
    headers: { "X-MediaBrowser-Token": token },
    payload: { ItemId: "movie-a", MediaSourceId: "media-a", PositionTicks: 5000, PlaySessionId: "upstream-play-session" }
  });
  assert.equal(progress.statusCode, 204);
  assert.equal(upstream.requests.at(-1)?.path, "/Sessions/Playing/Progress");
  assert.equal((upstream.requests.at(-1)?.init as any).body.ItemId, "movie-a");

  await app.close();
  store.close();
});

test("serves latest media from online sources when another source is unavailable", async () => {
  const passwordHash = await hash("secret");
  const config: BridgeConfig = {
    server: { bind: "127.0.0.1", port: 8096, publicUrl: "http://bridge.test", name: "Bridge" },
    auth: { users: [{ name: "alice", passwordHash }] },
    upstreams: [
      { id: "primary", name: "Primary", url: "https://primary.example.com", token: "token" },
      { id: "secondary", name: "Secondary", url: "https://secondary.example.com", token: "token" }
    ],
    libraries: [{
      id: "movies",
      name: "Movies",
      collectionType: "movies",
      sources: [{ server: "primary", libraryId: "primary-movies" }, { server: "secondary", libraryId: "secondary-movies" }]
    }]
  };
  const upstream = new FakeUpstream({
    "primary:/Items/Latest": new Error("Upstream primary request failed for /Items/Latest: getaddrinfo ENOTFOUND jellyfin-primary.example.com"),
    "secondary:/Users": [{ Id: "upstream-user", Name: "alice" }],
    "secondary:/Items/Latest": [
      { Id: "movie-a", Type: "Movie", Name: "Online Latest", ProviderIds: { Tmdb: "100" }, DateCreated: "2026-01-01T00:00:00.000Z" }
    ]
  });
  const store = new Store(":memory:");
  const app = buildApp({ config, store, upstream });
  const login = await app.inject({ method: "POST", url: "/Users/AuthenticateByName", payload: { Username: "alice", Pw: "secret" } });

  const latest = await app.inject({
    method: "GET",
    url: `/Items/Latest?userId=${login.json().User.Id}&ParentId=${bridgeLibraryId("movies")}&IncludeItemTypes=Movie&Limit=20`,
    headers: { "X-MediaBrowser-Token": login.json().AccessToken }
  });

  assert.equal(latest.statusCode, 200);
  assert.deepEqual(latest.json().map((item: any) => item.Name), ["Online Latest"]);
  assert.equal((upstream.requests.find((request) => request.serverId === "secondary" && request.path === "/Items/Latest")?.init as any).query.ParentId, "secondary-movies");

  await app.close();
  store.close();
});

test("pages live next-up after merging upstream results", async () => {
  const passwordHash = await hash("secret");
  const config: BridgeConfig = {
    server: { bind: "127.0.0.1", port: 8096, publicUrl: "http://bridge.test", name: "Bridge" },
    auth: { users: [{ name: "alice", passwordHash }] },
    upstreams: [
      { id: "primary", name: "Primary", url: "https://primary.example.com", token: "token" },
      { id: "secondary", name: "Secondary", url: "https://secondary.example.com", token: "token" }
    ],
    libraries: []
  };
  const allItems = {
    primary: Array.from({ length: 30 }, (_, index) => ({
      Id: `primary-${index}`,
      Type: "Episode",
      Name: `Primary ${index}`,
      ProviderIds: { Tvdb: `p-${index}` }
    })),
    secondary: Array.from({ length: 30 }, (_, index) => ({
      Id: `secondary-${index}`,
      Type: "Episode",
      Name: `Secondary ${index}`,
      ProviderIds: { Tvdb: `s-${index}` }
    }))
  };
  const upstream = {
    requests: [] as Array<{ serverId: string; path: string; init: any }>,
    async json<T>(serverId: string, path: string, init: any): Promise<T> {
      this.requests.push({ serverId, path, init });
      if (path === "/Users") return [{ Id: `${serverId}-user`, Name: "alice" }] as T;
      if (path !== "/Shows/NextUp") throw new Error(`Unexpected upstream request ${serverId}:${path}`);
      const query = init.query as Record<string, string | number | undefined>;
      const start = Number(query.StartIndex ?? query.startIndex ?? 0);
      const limit = Number(query.Limit ?? query.limit ?? allItems[serverId as keyof typeof allItems].length);
      const items = allItems[serverId as keyof typeof allItems].slice(start, start + limit);
      return { Items: items, TotalRecordCount: allItems[serverId as keyof typeof allItems].length, StartIndex: start } as T;
    }
  };
  const store = new Store(":memory:");
  const app = buildApp({ config, store, upstream });
  const login = await app.inject({ method: "POST", url: "/Users/AuthenticateByName", payload: { Username: "alice", Pw: "secret" } });

  const nextUp = await app.inject({
    method: "GET",
    url: `/Shows/NextUp?userId=${login.json().User.Id}&startIndex=50&limit=6`,
    headers: { "X-MediaBrowser-Token": login.json().AccessToken }
  });

  assert.equal(nextUp.statusCode, 200);
  assert.equal(nextUp.json().Items.length, 6);
  assert.equal(nextUp.json().TotalRecordCount, 60);
  assert.deepEqual(nextUp.json().Items.map((item: any) => item.Name), [
    "Secondary 20",
    "Secondary 21",
    "Secondary 22",
    "Secondary 23",
    "Secondary 24",
    "Secondary 25"
  ]);
  assert.deepEqual(upstream.requests
    .filter((request) => request.path === "/Shows/NextUp")
    .map((request) => request.init.query), [
    { StartIndex: 0, Limit: 56, UserId: "primary-user" },
    { StartIndex: 0, Limit: 56, UserId: "secondary-user" }
  ]);

  await app.close();
  store.close();
});

test("routes pass-through latest media to the selected upstream library", async () => {
  const passwordHash = await hash("secret");
  const config: BridgeConfig = {
    server: { bind: "127.0.0.1", port: 8096, publicUrl: "http://bridge.test", name: "Bridge" },
    auth: { users: [{ name: "alice", passwordHash }] },
    upstreams: [{ id: "primary", name: "Primary Tone", url: "https://primary.example.com", token: "token" }],
    libraries: []
  };
  const store = new Store(":memory:");
  store.upsertUpstreamLibrary({ serverId: "primary", libraryId: "instagram-lib", name: "Instagram", collectionType: "homevideos" });
  store.upsertUpstreamLibrary({ serverId: "primary", libraryId: "youtube-lib", name: "YouTube", collectionType: "homevideos" });
  const upstream = new FakeUpstream({
    "primary:/Users": [{ Id: "primary-user", Name: "alice" }],
    "primary:/Items/Latest": [
      { Id: "instagram-video", Type: "Video", Name: "Instagram Clip", DateCreated: "2026-01-01T00:00:00.000Z" }
    ]
  });
  const app = buildApp({ config, store, upstream });
  const login = await app.inject({ method: "POST", url: "/Users/AuthenticateByName", payload: { Username: "alice", Pw: "secret" } });
  const token = login.json().AccessToken;

  const latest = await app.inject({
    method: "GET",
    url: `/Users/${login.json().User.Id}/Items/Latest?ParentId=${passThroughLibraryId("primary", "instagram-lib")}&Limit=20`,
    headers: { "X-MediaBrowser-Token": token }
  });

  assert.equal(latest.statusCode, 200);
  assert.deepEqual(latest.json().map((item: any) => item.Name), ["Instagram Clip"]);
  assert.deepEqual(upstream.requests
    .filter((request) => request.path === "/Items/Latest")
    .map((request) => (request.init as any).query.ParentId), ["instagram-lib"]);

  await app.close();
  store.close();
});

class FakeUpstream {
  readonly requests: Array<{ serverId: string; path: string; init: unknown }> = [];
  readonly rawRequests: Array<{ serverId: string; path: string; init: any; headers: Record<string, string> }> = [];
  readonly rawResponses: Record<string, { statusCode: number; headers: Record<string, string>; body: unknown }> = {};

  constructor(private readonly responses: Record<string, unknown>) {}

  async json<T>(serverId: string, path: string, init: unknown): Promise<T> {
    this.requests.push({ serverId, path, init });
    const response = this.responses[`${serverId}:${path}`];
    if (!response) throw new Error(`Unexpected upstream request ${serverId}:${path}`);
    if (response instanceof Error) throw response;
    return response as T;
  }

  async raw(serverId: string, path: string, init: any): Promise<{ statusCode: number; headers: Record<string, string>; body: unknown }> {
    this.rawRequests.push({ serverId, path, init, headers: init.headers ?? {} });
    const response = this.rawResponses[`${serverId}:${path}`];
    if (!response) throw new Error(`Unexpected upstream raw request ${serverId}:${path}`);
    return response;
  }
}

function upstreamSocketClosedStream(): Readable {
  let sent = false;
  return new Readable({
    read() {
      if (sent) return;
      sent = true;
      this.push(Buffer.from("data"));
      const error = new Error("other side closed") as Error & { code?: string };
      error.code = "UND_ERR_SOCKET";
      this.destroy(error);
    }
  });
}
