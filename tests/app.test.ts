import test from "node:test";
import assert from "node:assert/strict";
import { Readable } from "node:stream";
import { hash } from "@node-rs/argon2";
import { buildApp } from "../src/app.js";
import { userId as bridgeUserId } from "../src/auth.js";
import type { BridgeConfig, RuntimeConfigSource } from "../src/config.js";
import { Store } from "../src/store.js";
import { bridgeItemId, bridgeLibraryId, bridgeMediaSourceId, bridgeServerId } from "../src/ids.js";
import { passThroughLibraryId } from "../src/ids.js";
import { UpstreamClient } from "../src/upstream.js";

test("supports Jellyfin login, authenticated system routes, user views, user data, and logout", async () => {
  const passwordHash = await hash("secret");
  const config: BridgeConfig = {
    server: { bind: "127.0.0.1", port: 8096, publicUrl: "http://bridge.test", name: "Bridge" },
    auth: { users: [{ name: "alice", passwordHash }] },
    upstreams: [{ id: "main", name: "Main", url: "https://jellyfin.example.com", token: "token" }],
    libraries: [{ id: "movies", name: "Movies", collectionType: "movies", sources: [{ server: "main", libraryId: "abc" }] }]
  };
  const store = new Store(":memory:");
  const upstream = new FakeUpstream({
    "main:/Users": [{ Id: "main-user", Name: "alice" }],
    "main:/UserItems/movie-a/UserData": { Played: true }
  });
  const app = buildApp({ config, store, upstream });

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

  const itemId = bridgeItemId("movie:tmdb:1");
  store.upsertIndexedItem({
    serverId: "main",
    itemId: "movie-a",
    libraryId: "abc",
    itemType: "Movie",
    logicalKey: "movie:tmdb:1",
    json: { Id: "movie-a", Type: "Movie", Name: "Movie A", ProviderIds: { Tmdb: "1" } }
  });
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

test("discovers pass-through views through user-scoped Jellyfin views route", async () => {
  const passwordHash = await hash("secret");
  const config: BridgeConfig = {
    server: { bind: "127.0.0.1", port: 8096, publicUrl: "http://bridge.test", name: "Bridge" },
    auth: { users: [{ name: "alice", passwordHash }] },
    upstreams: [{ id: "main", name: "Main", url: "https://main.example.com", token: "token" }],
    libraries: []
  };
  const upstream = new FakeUpstream({
    "main:/Users": [{ Id: "upstream-user", Name: "alice" }],
    "main:/UserViews": {
      Items: [{ Id: "shows-lib", Name: "TV", CollectionType: "tvshows" }],
      TotalRecordCount: 1,
      StartIndex: 0
    }
  });
  const store = new Store(":memory:");
  const app = buildApp({ config, store, upstream });
  const login = await app.inject({ method: "POST", url: "/Users/AuthenticateByName", payload: { Username: "alice", Pw: "secret" } });

  const views = await app.inject({
    method: "GET",
    url: `/Users/${login.json().User.Id}/Views`,
    headers: { "X-MediaBrowser-Token": login.json().AccessToken }
  });

  assert.equal(views.statusCode, 200);
  assert.deepEqual(views.json().Items.map((item: Record<string, unknown>) => item.Name), ["Main - TV"]);
  assert.deepEqual(upstream.requests.map((request) => `${request.serverId}:${request.path}`), ["main:/Users", "main:/UserViews"]);

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
    { method: "GET", url: "/Plugins/022a3003-993f-45f1-8565-87d12af2e12a" },
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

test("exposes InfuseSync plugin discovery", async () => {
  const passwordHash = await hash("secret");
  const config: BridgeConfig = {
    server: { bind: "127.0.0.1", port: 8096, publicUrl: "http://bridge.test", name: "Bridge" },
    auth: { users: [{ name: "alice", passwordHash }] },
    upstreams: [{ id: "main", name: "Main", url: "https://jellyfin.example.com", token: "token" }],
    libraries: []
  };
  const store = new Store(":memory:");
  const app = buildApp({ config, store });

  const unauthenticated = await app.inject({ method: "GET", url: "/Plugins" });
  assert.equal(unauthenticated.statusCode, 401);

  const login = await app.inject({
    method: "POST",
    url: "/Users/AuthenticateByName",
    payload: { Username: "alice", Pw: "secret" }
  });
  const plugins = await app.inject({
    method: "GET",
    url: "/Plugins",
    headers: { "X-MediaBrowser-Token": login.json().AccessToken }
  });

  assert.equal(plugins.statusCode, 200);
  assert.deepEqual(plugins.json(), [{
    Name: "InfuseSync",
    Description: "Plugin for fast synchronization with Infuse.",
    Id: "022a3003-993f-45f1-8565-87d12af2e12a",
    Version: "1.5.2.0",
    CanUninstall: false,
    HasImage: false,
    Status: "Active"
  }]);

  await app.close();
  store.close();
});

test("serves InfuseSync checkpoints from bridge-owned index and user data", async () => {
  const passwordHash = await hash("secret");
  const config: BridgeConfig = {
    server: { bind: "127.0.0.1", port: 8096, publicUrl: "http://bridge.test", name: "Bridge" },
    auth: { users: [{ name: "alice", passwordHash }, { name: "bob", passwordHash }] },
    upstreams: [
      { id: "main", name: "Main", url: "https://main.example.com", token: "token" },
      { id: "remote", name: "Remote", url: "https://remote.example.com", token: "token" }
    ],
    libraries: [{
      id: "movies",
      name: "Movies",
      collectionType: "movies",
      sources: [{ server: "main", libraryId: "library-a" }, { server: "remote", libraryId: "library-b" }]
    }]
  };
  const store = new Store(":memory:");
  const app = buildApp({ config, store });

  const login = await app.inject({
    method: "POST",
    url: "/Users/AuthenticateByName",
    payload: { Username: "alice", Pw: "secret" }
  });
  const auth = login.json();
  const token = auth.AccessToken;
  const userId = auth.User.Id;
  const otherUserId = bridgeUserId("bob");

  const missingToken = await app.inject({ method: "GET", url: `/InfuseSync/UserFolders/${userId}` });
  assert.equal(missingToken.statusCode, 401);

  const crossUserCheckpoint = await app.inject({
    method: "POST",
    url: `/InfuseSync/Checkpoint?deviceId=device-1&uSeRiD=${otherUserId}`,
    headers: { "X-MediaBrowser-Token": token }
  });
  assert.equal(crossUserCheckpoint.statusCode, 403);

  const checkpoint = await app.inject({
    method: "POST",
    url: `/InfuseSync/Checkpoint?deviceId=device-1&uSeRiD=${userId}`,
    headers: { "X-MediaBrowser-Token": token }
  });
  assert.equal(checkpoint.statusCode, 200);
  assert.match(checkpoint.json().Id, /^[0-9a-f-]{36}$/);
  const checkpointId = checkpoint.json().Id;

  const bridgeMovieId = bridgeItemId("movie:tmdb:100");
  store.upsertIndexedItems([
    {
      serverId: "main",
      itemId: "main-movie",
      libraryId: "library-a",
      itemType: "Movie",
      logicalKey: "movie:tmdb:100",
      json: { Id: "main-movie", Type: "Movie", Name: "Merged Movie", ParentId: "library-a", DateCreated: "2026-01-01T00:00:00.000Z", ProviderIds: { Tmdb: "100" } }
    },
    {
      serverId: "remote",
      itemId: "remote-movie",
      libraryId: "library-b",
      itemType: "Movie",
      logicalKey: "movie:tmdb:100",
      json: { Id: "remote-movie", Type: "Movie", Name: "Merged Movie", ParentId: "library-b", DateCreated: "2026-01-01T00:00:00.000Z", ProviderIds: { Tmdb: "100" } }
    },
    {
      serverId: "remote",
      itemId: "remote-series",
      libraryId: "library-b",
      itemType: "Series",
      logicalKey: "series:tvdb:200",
      json: { Id: "remote-series", Type: "Series", Name: "Changed Series", ParentId: "library-b", DateCreated: "2026-01-02T00:00:00.000Z", ProviderIds: { Tvdb: "200" } }
    }
  ]);
  store.upsertUserData(userId, bridgeMovieId, {
    isFavorite: true,
    played: true,
    playCount: 2,
    playbackPositionTicks: 123_000_000,
    lastPlayedDate: "2026-05-07T10:00:00.000Z"
  });
  store.upsertUserData(userId, bridgeLibraryId("movies"), { isFavorite: true });
  store.upsertUserData(otherUserId, bridgeMovieId, { isFavorite: false, played: false });

  const bobLogin = await app.inject({
    method: "POST",
    url: "/Users/AuthenticateByName",
    payload: { Username: "bob", Pw: "secret" }
  });
  const crossUserStart = await app.inject({
    method: "POST",
    url: `/InfuseSync/Checkpoint/${checkpointId}/StartSync`,
    headers: { "X-MediaBrowser-Token": bobLogin.json().AccessToken }
  });
  assert.equal(crossUserStart.statusCode, 403);
  const originalGetUserData = store.getUserData.bind(store);
  let singleUserDataReads = 0;
  store.getUserData = (userIdValue, itemIdValue) => {
    singleUserDataReads += 1;
    return originalGetUserData(userIdValue, itemIdValue);
  };

  const startSync = await app.inject({
    method: "POST",
    url: `/InfuseSync/Checkpoint/${checkpointId}/StartSync`,
    headers: { "X-MediaBrowser-Token": token }
  });
  assert.equal(startSync.statusCode, 200);
  const stats = startSync.json();
  for (const field of [
    "UpdatedFolders",
    "RemovedFolders",
    "UpdatedBoxSets",
    "RemovedBoxSets",
    "UpdatedPlaylists",
    "RemovedPlaylists",
    "UpdatedTvShows",
    "RemovedTvShows",
    "UpdatedSeasons",
    "RemovedSeasons",
    "UpdatedVideos",
    "RemovedVideos",
    "UpdatedCollectionFolders",
    "UpdatedUserData"
  ]) {
    assert.equal(typeof stats[field], "number", field);
  }
  assert.equal(stats.UpdatedVideos, 1);
  assert.equal(stats.UpdatedTvShows, 1);
  assert.equal(stats.UpdatedUserData, 1);
  assert.equal(stats.RemovedVideos, 0);
  assert.equal(singleUserDataReads, 0);

  const folders = await app.inject({
    method: "GET",
    url: `/InfuseSync/UserFolders/${userId}`,
    headers: { "X-MediaBrowser-Token": token }
  });
  assert.equal(folders.statusCode, 200);
  assert.deepEqual(folders.json().map((folder: Record<string, unknown>) => folder.ItemId), [bridgeLibraryId("movies")]);

  const updatedMovies = await app.inject({
    method: "GET",
    url: `/InfuseSync/Checkpoint/${checkpointId}/UpdatedItems?IncludeItemTypes=Movie&StartIndex=0&Limit=1`,
    headers: { "X-MediaBrowser-Token": token }
  });
  assert.equal(updatedMovies.statusCode, 200);
  assert.equal(updatedMovies.json().TotalRecordCount, 1);
  assert.equal(updatedMovies.json().StartIndex, 0);
  assert.equal(updatedMovies.json().Items.length, 1);
  assert.equal(updatedMovies.json().Items[0].Id, bridgeMovieId);
  assert.equal(updatedMovies.json().Items[0].ParentId, bridgeLibraryId("movies"));
  assert.equal(updatedMovies.json().Items[0].ProviderIds.Tmdb, "100");

  const secondPage = await app.inject({
    method: "GET",
    url: `/InfuseSync/Checkpoint/${checkpointId}/UpdatedItems?IncludeItemTypes=Movie&StartIndex=1&Limit=1`,
    headers: { "X-MediaBrowser-Token": token }
  });
  assert.equal(secondPage.statusCode, 200);
  assert.equal(secondPage.json().TotalRecordCount, 1);
  assert.equal(secondPage.json().StartIndex, 1);
  assert.deepEqual(secondPage.json().Items, []);

  const updatedUserData = await app.inject({
    method: "GET",
    url: `/InfuseSync/Checkpoint/${checkpointId}/UserData?IncludeItemTypes=Movie`,
    headers: { "X-MediaBrowser-Token": token }
  });
  assert.equal(updatedUserData.statusCode, 200);
  assert.equal(updatedUserData.json().TotalRecordCount, 1);
  assert.equal(updatedUserData.json().Items[0].ItemId, bridgeMovieId);
  assert.equal(updatedUserData.json().Items[0].Key, bridgeMovieId);
  assert.equal(updatedUserData.json().Items[0].IsFavorite, true);
  assert.equal(updatedUserData.json().Items[0].Played, true);

  const crossUserUpdatedItems = await app.inject({
    method: "GET",
    url: `/InfuseSync/Checkpoint/${checkpointId}/UpdatedItems`,
    headers: { "X-MediaBrowser-Token": bobLogin.json().AccessToken }
  });
  assert.equal(crossUserUpdatedItems.statusCode, 403);

  const removedItems = await app.inject({
    method: "GET",
    url: `/InfuseSync/Checkpoint/${checkpointId}/RemovedItems?StartIndex=2&Limit=10`,
    headers: { "X-MediaBrowser-Token": token }
  });
  assert.equal(removedItems.statusCode, 200);
  assert.deepEqual(removedItems.json(), { Items: [], TotalRecordCount: 0, StartIndex: 2 });

  await app.close();
  store.close();
});

test("InfuseSync updated item type filtering uses the selected merged item type", async () => {
  const passwordHash = await hash("secret");
  const config: BridgeConfig = {
    server: { bind: "127.0.0.1", port: 8096, publicUrl: "http://bridge.test", name: "Bridge" },
    auth: { users: [{ name: "alice", passwordHash }] },
    upstreams: [
      { id: "main", name: "Main", url: "https://main.example.com", token: "token" },
      { id: "remote", name: "Remote", url: "https://remote.example.com", token: "token" }
    ],
    libraries: [{ id: "media", name: "Media", collectionType: "mixed", sources: [{ server: "main", libraryId: "library-a" }, { server: "remote", libraryId: "library-b" }] }]
  };
  const store = new Store(":memory:");
  store.upsertIndexedItem({
    serverId: "main",
    itemId: "main-movie",
    libraryId: "library-a",
    itemType: "Movie",
    logicalKey: "shared:item:2",
    json: { Id: "main-movie", Type: "Movie", Name: "Priority Movie", ParentId: "library-a" }
  });
  store.db.prepare("UPDATE indexed_items SET updated_at = ? WHERE server_id = ? AND item_id = ?").run("2026-01-01T00:00:00.000Z", "main", "main-movie");
  const app = buildApp({ config, store });
  const login = await app.inject({ method: "POST", url: "/Users/AuthenticateByName", payload: { Username: "alice", Pw: "secret" } });
  const token = login.json().AccessToken;
  const checkpoint = await app.inject({
    method: "POST",
    url: `/InfuseSync/Checkpoint?deviceId=device-2&userId=${login.json().User.Id}`,
    headers: { "X-MediaBrowser-Token": token }
  });
  store.upsertIndexedItem({
    serverId: "remote",
    itemId: "remote-video",
    libraryId: "library-b",
    itemType: "Video",
    logicalKey: "shared:item:2",
    json: { Id: "remote-video", Type: "Video", Name: "Lower Priority Video", ParentId: "library-b" }
  });

  const startSync = await app.inject({
    method: "POST",
    url: `/InfuseSync/Checkpoint/${checkpoint.json().Id}/StartSync`,
    headers: { "X-MediaBrowser-Token": token }
  });
  assert.equal(startSync.statusCode, 200);
  assert.equal(startSync.json().UpdatedVideos, 1);

  const updatedMovies = await app.inject({
    method: "GET",
    url: `/InfuseSync/Checkpoint/${checkpoint.json().Id}/UpdatedItems?IncludeItemTypes=Movie`,
    headers: { "X-MediaBrowser-Token": token }
  });

  assert.equal(updatedMovies.statusCode, 200);
  assert.equal(updatedMovies.json().TotalRecordCount, 1);
  assert.equal(updatedMovies.json().Items[0].Id, bridgeItemId("shared:item:2"));
  assert.equal(updatedMovies.json().Items[0].Name, "Priority Movie");

  await app.close();
  store.close();
});

test("InfuseSync user data item type filtering uses the selected merged item type", async () => {
  const passwordHash = await hash("secret");
  const config: BridgeConfig = {
    server: { bind: "127.0.0.1", port: 8096, publicUrl: "http://bridge.test", name: "Bridge" },
    auth: { users: [{ name: "alice", passwordHash }] },
    upstreams: [
      { id: "main", name: "Main", url: "https://main.example.com", token: "token" },
      { id: "remote", name: "Remote", url: "https://remote.example.com", token: "token" }
    ],
    libraries: [{ id: "media", name: "Media", collectionType: "mixed", sources: [{ server: "main", libraryId: "library-a" }, { server: "remote", libraryId: "library-b" }] }]
  };
  const store = new Store(":memory:");
  const bridgeId = bridgeItemId("shared:item:3");
  store.upsertIndexedItem({
    serverId: "main",
    itemId: "main-series",
    libraryId: "library-a",
    itemType: "Series",
    logicalKey: "shared:item:3",
    json: { Id: "main-series", Type: "Series", Name: "Priority Series", ParentId: "library-a" }
  });
  store.upsertIndexedItem({
    serverId: "remote",
    itemId: "remote-movie",
    libraryId: "library-b",
    itemType: "Movie",
    logicalKey: "shared:item:3",
    json: { Id: "remote-movie", Type: "Movie", Name: "Lower Priority Movie", ParentId: "library-b" }
  });
  const app = buildApp({ config, store });
  const login = await app.inject({ method: "POST", url: "/Users/AuthenticateByName", payload: { Username: "alice", Pw: "secret" } });
  const token = login.json().AccessToken;
  const checkpoint = await app.inject({
    method: "POST",
    url: `/InfuseSync/Checkpoint?deviceId=device-3&userId=${login.json().User.Id}`,
    headers: { "X-MediaBrowser-Token": token }
  });
  store.upsertUserData(login.json().User.Id, bridgeId, { isFavorite: true });
  const startSync = await app.inject({
    method: "POST",
    url: `/InfuseSync/Checkpoint/${checkpoint.json().Id}/StartSync`,
    headers: { "X-MediaBrowser-Token": token }
  });
  assert.equal(startSync.statusCode, 200);

  const movieUserData = await app.inject({
    method: "GET",
    url: `/InfuseSync/Checkpoint/${checkpoint.json().Id}/UserData?IncludeItemTypes=Movie`,
    headers: { "X-MediaBrowser-Token": token }
  });
  assert.equal(movieUserData.statusCode, 200);
  assert.deepEqual(movieUserData.json(), { Items: [], TotalRecordCount: 0, StartIndex: 0 });

  const seriesUserData = await app.inject({
    method: "GET",
    url: `/InfuseSync/Checkpoint/${checkpoint.json().Id}/UserData?IncludeItemTypes=Series`,
    headers: { "X-MediaBrowser-Token": token }
  });
  assert.equal(seriesUserData.statusCode, 200);
  assert.equal(seriesUserData.json().TotalRecordCount, 1);
  assert.equal(seriesUserData.json().Items[0].ItemId, bridgeId);

  await app.close();
  store.close();
});

test("keeps cached parent browse scoped to the requested library source", async () => {
  const passwordHash = await hash("secret");
  const config: BridgeConfig = {
    server: { bind: "127.0.0.1", port: 8096, publicUrl: "http://bridge.test", name: "Bridge" },
    auth: { users: [{ name: "alice", passwordHash }] },
    upstreams: [
      { id: "main", name: "Main", url: "https://main.example.com", token: "token" },
      { id: "remote", name: "Remote", url: "https://remote.example.com", token: "token" }
    ],
    libraries: [
      { id: "main-movies", name: "Main Movies", collectionType: "movies", sources: [{ server: "main", libraryId: "library-a" }] },
      { id: "remote-movies", name: "Remote Movies", collectionType: "movies", sources: [{ server: "remote", libraryId: "library-b" }] }
    ]
  };
  const store = new Store(":memory:");
  store.upsertIndexedItems([
    {
      serverId: "main",
      itemId: "main-movie",
      libraryId: "library-a",
      itemType: "Movie",
      logicalKey: "movie:tmdb:100",
      json: { Id: "main-movie", Type: "Movie", Name: "Main Movie", ParentId: "library-a", DateCreated: "2026-01-01T00:00:00.000Z", ProviderIds: { Tmdb: "100" } }
    },
    {
      serverId: "remote",
      itemId: "remote-movie",
      libraryId: "library-b",
      itemType: "Movie",
      logicalKey: "movie:tmdb:100",
      json: { Id: "remote-movie", Type: "Movie", Name: "Remote Movie", ParentId: "library-b", DateCreated: "2026-01-01T00:00:00.000Z", ProviderIds: { Tmdb: "100" } }
    }
  ]);
  const app = buildApp({ config, store });

  const login = await app.inject({
    method: "POST",
    url: "/Users/AuthenticateByName",
    payload: { Username: "alice", Pw: "secret" }
  });
  const remoteItems = await app.inject({
    method: "GET",
    url: `/Items?ParentId=${bridgeLibraryId("remote-movies")}`,
    headers: { "X-MediaBrowser-Token": login.json().AccessToken }
  });

  assert.equal(remoteItems.statusCode, 200);
  assert.equal(remoteItems.json().Items.length, 1);
  assert.equal(remoteItems.json().Items[0].Name, "Remote Movie");
  assert.equal(remoteItems.json().Items[0].ParentId, bridgeLibraryId("remote-movies"));

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
  store.upsertIndexedItem({
    serverId: "remote",
    itemId: "remote-denied",
    libraryId: "library-b",
    itemType: "Movie",
    logicalKey: "movie:tmdb:3333",
    json: { Id: "remote-denied", Type: "Movie", Name: "Denied Delete", ProviderIds: { Tmdb: "3333" } }
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

  const upstream = new FakeUpstream({
    "main:/Users": [{ Id: "main-user", Name: "alice" }],
    "remote:/Users": [{ Id: "remote-user", Name: "alice" }],
    "main:/Items/main-alien": { CanDelete: true },
    "remote:/Items/remote-thing": { CanDelete: true },
    "remote:/Items/remote-empty-json-delete": { CanDelete: true },
    "remote:/Items/remote-denied": { CanDelete: false },
    "main:/Items/main-alien/Images": [{ ImageType: "Primary", ImageIndex: 0, Path: "primary.jpg" }]
  });
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

  const deniedDelete = await app.inject({
    method: "DELETE",
    url: "/Items/remote-denied",
    headers: { "X-MediaBrowser-Token": token }
  });
  assert.equal(deniedDelete.statusCode, 401);
  assert.equal(upstream.rawRequests.length, 3);
  assert.equal(store.listIndexedItems().some((item) => item.serverId === "remote" && item.itemId === "remote-denied"), true);

  await app.close();
  store.close();
});

test("forwards watched playstate writes to every logical upstream source", async () => {
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
    "main:/Users": [{ Id: "wrong-main-user", Name: "bob" }, { Id: "main-user", Name: "alice" }],
    "remote:/Users": [{ Id: "remote-user", Name: "ALICE" }],
    "main:/UserPlayedItems/main-alien": { Played: true },
    "remote:/UserPlayedItems/remote-alien": { Played: true }
  });
  const app = buildApp({ config, store, upstream });
  const login = await app.inject({ method: "POST", url: "/Users/AuthenticateByName", payload: { Username: "alice", Pw: "secret" } });
  const userId = login.json().User.Id;
  const token = login.json().AccessToken;
  const alienBridgeId = bridgeItemId("movie:imdb:tt0078748");
  const datePlayed = "2024-05-01T00:00:00.000Z";

  const played = await app.inject({
    method: "POST",
    url: `/UserPlayedItems/${alienBridgeId}?DatePlayed=${encodeURIComponent(datePlayed)}`,
    headers: { "X-MediaBrowser-Token": token }
  });
  assert.equal(played.statusCode, 200);
  assert.equal(played.json().Played, true);
  assert.equal(played.json().PlayCount, 1);
  assert.equal(played.json().LastPlayedDate, datePlayed);
  assert.equal(store.getUserData(userId, alienBridgeId).Played, true);

  const playedWrites = upstream.requests.filter((request) => request.path.startsWith("/UserPlayedItems/"));
  assert.deepEqual(playedWrites.map((request) => {
    const init = request.init as any;
    return `${request.serverId}:${request.path}:${init.method}:${init.query.userId}:${init.query.datePlayed ?? ""}`;
  }).sort(), [
    `main:/UserPlayedItems/main-alien:POST:main-user:${datePlayed}`,
    `remote:/UserPlayedItems/remote-alien:POST:remote-user:${datePlayed}`
  ]);
  assert.deepEqual(upstream.requests.filter((request) => request.path === "/Users").map((request) => {
    const init = request.init as any;
    return { serverId: request.serverId, method: init.method, query: init.query, body: init.body };
  }), [
    { serverId: "main", method: undefined, query: undefined, body: undefined },
    { serverId: "remote", method: undefined, query: undefined, body: undefined }
  ]);

  const unplayed = await app.inject({
    method: "DELETE",
    url: `/Users/${userId}/PlayedItems/main-alien`,
    headers: { "X-MediaBrowser-Token": token }
  });
  assert.equal(unplayed.statusCode, 200);
  assert.equal(unplayed.json().Played, false);
  assert.equal(unplayed.json().PlayCount, 0);
  assert.equal(unplayed.json().LastPlayedDate, null);
  assert.equal(store.getUserData(userId, alienBridgeId).Played, false);
  assert.equal(store.listUserData(userId, ["main-alien"]).has("main-alien"), false);

  const allWrites = upstream.requests.filter((request) => request.path.startsWith("/UserPlayedItems/"));
  assert.deepEqual(allWrites.slice(2).map((request) => {
    const init = request.init as any;
    return `${request.serverId}:${request.path}:${init.method}:${init.query.userId}:${init.query.datePlayed ?? ""}`;
  }).sort(), [
    "main:/UserPlayedItems/main-alien:DELETE:main-user:",
    "remote:/UserPlayedItems/remote-alien:DELETE:remote-user:"
  ]);

  await app.close();
  store.close();
});

test("forwards generic watched user data writes and keeps non-watched writes local", async () => {
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
    "main:/Users": [{ Id: "main-user", Name: "alice" }],
    "remote:/Users": [{ Id: "remote-user", Name: "alice" }],
    "main:/UserItems/main-alien/UserData": { Played: true },
    "remote:/UserItems/remote-alien/UserData": { Played: true }
  });
  const app = buildApp({ config, store, upstream });
  const login = await app.inject({ method: "POST", url: "/Users/AuthenticateByName", payload: { Username: "alice", Pw: "secret" } });
  const userId = login.json().User.Id;
  const token = login.json().AccessToken;
  const alienBridgeId = bridgeItemId("movie:imdb:tt0078748");
  const lastPlayedDate = "2024-05-02T00:00:00.000Z";

  const watchedUserData = await app.inject({
    method: "POST",
    url: `/Users/${userId}/Items/${alienBridgeId}/UserData`,
    headers: { "X-MediaBrowser-Token": token },
    payload: { Played: true, LastPlayedDate: lastPlayedDate, PlaybackPositionTicks: 1234, IsFavorite: true }
  });
  assert.equal(watchedUserData.statusCode, 200);
  assert.equal(watchedUserData.json().Played, true);
  assert.equal(watchedUserData.json().LastPlayedDate, lastPlayedDate);
  assert.equal(watchedUserData.json().PlaybackPositionTicks, 1234);
  assert.equal(watchedUserData.json().IsFavorite, true);
  assert.deepEqual(upstream.requests.filter((request) => request.path.includes("/UserData")).map((request) => {
    const init = request.init as any;
    return {
      serverId: request.serverId,
      path: request.path,
      method: init.method,
      query: init.query,
      body: init.body
    };
  }).sort((left, right) => left.serverId.localeCompare(right.serverId)), [
    {
      serverId: "main",
      path: "/UserItems/main-alien/UserData",
      method: "POST",
      query: { userId: "main-user" },
      body: { Played: true, PlaybackPositionTicks: 1234, LastPlayedDate: lastPlayedDate }
    },
    {
      serverId: "remote",
      path: "/UserItems/remote-alien/UserData",
      method: "POST",
      query: { userId: "remote-user" },
      body: { Played: true, PlaybackPositionTicks: 1234, LastPlayedDate: lastPlayedDate }
    }
  ]);

  const upstreamRequestCount = upstream.requests.length;
  const positionOnly = await app.inject({
    method: "POST",
    url: `/UserItems/${alienBridgeId}/UserData`,
    headers: { "X-MediaBrowser-Token": token },
    payload: { PlaybackPositionTicks: 4567 }
  });
  assert.equal(positionOnly.statusCode, 200);
  assert.equal(positionOnly.json().PlaybackPositionTicks, 4567);
  assert.equal(upstream.requests.length, upstreamRequestCount);

  const favoriteOnly = await app.inject({
    method: "POST",
    url: `/UserItems/${alienBridgeId}/UserData`,
    headers: { "X-MediaBrowser-Token": token },
    payload: { IsFavorite: false }
  });
  assert.equal(favoriteOnly.statusCode, 200);
  assert.equal(favoriteOnly.json().IsFavorite, false);
  assert.equal(upstream.requests.length, upstreamRequestCount);

  await app.close();
  store.close();
});

test("does not mutate local watched state when upstream propagation cannot complete", async () => {
  const passwordHash = await hash("secret");
  const config: BridgeConfig = {
    server: { bind: "127.0.0.1", port: 8096, publicUrl: "http://bridge.test", name: "Bridge" },
    auth: { users: [{ name: "alice", passwordHash }] },
    upstreams: [{ id: "main", name: "Main", url: "https://main.example.com", token: "token" }],
    libraries: [{ id: "movies", name: "Movies", collectionType: "movies", sources: [{ server: "main", libraryId: "library-a" }] }]
  };

  const missingStore = new Store(":memory:");
  const missingApp = buildApp({ config, store: missingStore, upstream: new FakeUpstream({}) });
  const missingLogin = await missingApp.inject({ method: "POST", url: "/Users/AuthenticateByName", payload: { Username: "alice", Pw: "secret" } });
  const missingToken = missingLogin.json().AccessToken;
  const missingItem = await missingApp.inject({
    method: "POST",
    url: "/UserPlayedItems/not-indexed",
    headers: { "X-MediaBrowser-Token": missingToken }
  });
  assert.equal(missingItem.statusCode, 404);
  const syntheticView = await missingApp.inject({
    method: "POST",
    url: `/UserPlayedItems/${bridgeLibraryId("movies")}`,
    headers: { "X-MediaBrowser-Token": missingToken }
  });
  assert.equal(syntheticView.statusCode, 404);
  await missingApp.close();
  missingStore.close();

  const missingUserStore = new Store(":memory:");
  missingUserStore.upsertIndexedItem({
    serverId: "main",
    itemId: "main-alien",
    libraryId: "library-a",
    itemType: "Movie",
    logicalKey: "movie:imdb:tt0078748",
    json: { Id: "main-alien", Type: "Movie", Name: "Alien", ProviderIds: { Imdb: "tt0078748" } }
  });
  const missingUserUpstream = new FakeUpstream({
    "main:/Users": [{ Id: "wrong-user", Name: "bob" }]
  });
  const missingUserApp = buildApp({ config, store: missingUserStore, upstream: missingUserUpstream });
  const missingUserLogin = await missingUserApp.inject({ method: "POST", url: "/Users/AuthenticateByName", payload: { Username: "alice", Pw: "secret" } });
  const missingUserId = missingUserLogin.json().User.Id;
  const missingUserPlayed = await missingUserApp.inject({
    method: "POST",
    url: `/UserPlayedItems/${bridgeItemId("movie:imdb:tt0078748")}`,
    headers: { "X-MediaBrowser-Token": missingUserLogin.json().AccessToken }
  });
  assert.equal(missingUserPlayed.statusCode, 502);
  assert.equal(missingUserStore.getUserData(missingUserId, bridgeItemId("movie:imdb:tt0078748")).Played, false);
  assert.equal(missingUserUpstream.requests.filter((request) => request.path.startsWith("/UserPlayedItems/")).length, 0);
  await missingUserApp.close();
  missingUserStore.close();

  const failureStore = new Store(":memory:");
  failureStore.upsertIndexedItem({
    serverId: "main",
    itemId: "main-alien",
    libraryId: "library-a",
    itemType: "Movie",
    logicalKey: "movie:imdb:tt0078748",
    json: { Id: "main-alien", Type: "Movie", Name: "Alien", ProviderIds: { Imdb: "tt0078748" } }
  });
  const failureUpstream = new FakeUpstream({
    "main:/Users": [{ Id: "main-user", Name: "alice" }],
    "main:/UserPlayedItems/main-alien": new Error("upstream write failed")
  });
  const failureApp = buildApp({ config, store: failureStore, upstream: failureUpstream });
  const failureLogin = await failureApp.inject({ method: "POST", url: "/Users/AuthenticateByName", payload: { Username: "alice", Pw: "secret" } });
  const failureUserId = failureLogin.json().User.Id;
  const failurePlayed = await failureApp.inject({
    method: "POST",
    url: `/UserPlayedItems/${bridgeItemId("movie:imdb:tt0078748")}`,
    headers: { "X-MediaBrowser-Token": failureLogin.json().AccessToken }
  });
  assert.equal(failurePlayed.statusCode, 502);
  assert.equal(failureStore.getUserData(failureUserId, bridgeItemId("movie:imdb:tt0078748")).Played, false);
  await failureApp.close();
  failureStore.close();

  const userDataFailureStore = new Store(":memory:");
  userDataFailureStore.upsertIndexedItem({
    serverId: "main",
    itemId: "main-alien",
    libraryId: "library-a",
    itemType: "Movie",
    logicalKey: "movie:imdb:tt0078748",
    json: { Id: "main-alien", Type: "Movie", Name: "Alien", ProviderIds: { Imdb: "tt0078748" } }
  });
  userDataFailureStore.upsertIndexedItem({
    serverId: "remote",
    itemId: "remote-alien",
    libraryId: "library-b",
    itemType: "Movie",
    logicalKey: "movie:imdb:tt0078748",
    json: { Id: "remote-alien", Type: "Movie", Name: "Alien", ProviderIds: { Imdb: "tt0078748" } }
  });
  const userDataFailureConfig: BridgeConfig = {
    ...config,
    upstreams: [
      { id: "main", name: "Main", url: "https://main.example.com", token: "token" },
      { id: "remote", name: "Remote", url: "https://remote.example.com", token: "token" }
    ],
    libraries: [{ id: "movies", name: "Movies", collectionType: "movies", sources: [{ server: "main", libraryId: "library-a" }, { server: "remote", libraryId: "library-b" }] }]
  };
  const userDataFailureUpstream = new FakeUpstream({
    "main:/Users": [{ Id: "main-user", Name: "alice" }],
    "remote:/Users": [{ Id: "remote-user", Name: "alice" }],
    "main:/UserItems/main-alien/UserData": { Played: true },
    "remote:/UserItems/remote-alien/UserData": new Error("remote user data failed")
  });
  const userDataFailureApp = buildApp({ config: userDataFailureConfig, store: userDataFailureStore, upstream: userDataFailureUpstream });
  const userDataFailureLogin = await userDataFailureApp.inject({ method: "POST", url: "/Users/AuthenticateByName", payload: { Username: "alice", Pw: "secret" } });
  const userDataFailureUserId = userDataFailureLogin.json().User.Id;
  const userDataFailure = await userDataFailureApp.inject({
    method: "POST",
    url: `/Users/${userDataFailureUserId}/Items/${bridgeItemId("movie:imdb:tt0078748")}/UserData`,
    headers: { "X-MediaBrowser-Token": userDataFailureLogin.json().AccessToken },
    payload: { Played: true, PlaybackPositionTicks: 1111, IsFavorite: true }
  });
  assert.equal(userDataFailure.statusCode, 502);
  assert.equal(userDataFailureStore.getUserData(userDataFailureUserId, bridgeItemId("movie:imdb:tt0078748")).Played, false);
  assert.equal(userDataFailureStore.getUserData(userDataFailureUserId, bridgeItemId("movie:imdb:tt0078748")).IsFavorite, false);
  assert.equal(userDataFailureStore.listUserData(userDataFailureUserId, ["main-alien"]).has("main-alien"), false);
  assert.deepEqual(userDataFailureUpstream.requests
    .filter((request) => request.path.includes("/UserData"))
    .map((request) => {
      const init = request.init as any;
      return { serverId: request.serverId, path: request.path, body: init.body };
    }), [
    { serverId: "main", path: "/UserItems/main-alien/UserData", body: { Played: true, PlaybackPositionTicks: 1111 } },
    { serverId: "remote", path: "/UserItems/remote-alien/UserData", body: { Played: true, PlaybackPositionTicks: 1111 } }
  ]);
  await userDataFailureApp.close();
  userDataFailureStore.close();

  const partialStore = new Store(":memory:");
  partialStore.upsertIndexedItem({
    serverId: "main",
    itemId: "main-alien",
    libraryId: "library-a",
    itemType: "Movie",
    logicalKey: "movie:imdb:tt0078748",
    json: { Id: "main-alien", Type: "Movie", Name: "Alien", ProviderIds: { Imdb: "tt0078748" } }
  });
  partialStore.upsertIndexedItem({
    serverId: "remote",
    itemId: "remote-alien",
    libraryId: "library-b",
    itemType: "Movie",
    logicalKey: "movie:imdb:tt0078748",
    json: { Id: "remote-alien", Type: "Movie", Name: "Alien", ProviderIds: { Imdb: "tt0078748" } }
  });
  const partialConfig: BridgeConfig = {
    ...config,
    upstreams: [
      { id: "main", name: "Main", url: "https://main.example.com", token: "token" },
      { id: "remote", name: "Remote", url: "https://remote.example.com", token: "token" }
    ],
    libraries: [{ id: "movies", name: "Movies", collectionType: "movies", sources: [{ server: "main", libraryId: "library-a" }, { server: "remote", libraryId: "library-b" }] }]
  };
  const partialUpstream = new FakeUpstream({
    "main:/Users": [{ Id: "main-user", Name: "alice" }],
    "remote:/Users": [{ Id: "remote-user", Name: "alice" }],
    "main:/UserPlayedItems/main-alien": { Played: true },
    "remote:/UserPlayedItems/remote-alien": new Error("remote write failed")
  });
  const partialApp = buildApp({ config: partialConfig, store: partialStore, upstream: partialUpstream });
  const partialLogin = await partialApp.inject({ method: "POST", url: "/Users/AuthenticateByName", payload: { Username: "alice", Pw: "secret" } });
  const partialUserId = partialLogin.json().User.Id;
  const partialPlayed = await partialApp.inject({
    method: "POST",
    url: `/UserPlayedItems/${bridgeItemId("movie:imdb:tt0078748")}`,
    headers: { "X-MediaBrowser-Token": partialLogin.json().AccessToken }
  });
  assert.equal(partialPlayed.statusCode, 502);
  assert.equal(partialStore.getUserData(partialUserId, bridgeItemId("movie:imdb:tt0078748")).Played, false);
  assert.deepEqual(partialUpstream.requests
    .filter((request) => request.path.startsWith("/UserPlayedItems/"))
    .map((request) => `${request.serverId}:${request.path}`), [
    "main:/UserPlayedItems/main-alien",
    "remote:/UserPlayedItems/remote-alien"
  ]);
  await partialApp.close();
  partialStore.close();

  const upstream404Store = new Store(":memory:");
  upstream404Store.upsertIndexedItem({
    serverId: "main",
    itemId: "main-alien",
    libraryId: "library-a",
    itemType: "Movie",
    logicalKey: "movie:imdb:tt0078748",
    json: { Id: "main-alien", Type: "Movie", Name: "Alien", ProviderIds: { Imdb: "tt0078748" } }
  });
  const upstream404Client = new UpstreamClient(config.upstreams, {
    request: async (url) => {
      if (url.pathname === "/Users") {
        return {
          statusCode: 200,
          headers: {},
          body: { json: async () => [{ Id: "main-user", Name: "alice" }] }
        };
      }
      return {
        statusCode: 404,
        headers: {},
        body: { json: async () => ({ error: "not found" }) }
      };
    },
    retries: 0
  });
  const upstream404App = buildApp({ config, store: upstream404Store, upstream: upstream404Client });
  const upstream404Login = await upstream404App.inject({ method: "POST", url: "/Users/AuthenticateByName", payload: { Username: "alice", Pw: "secret" } });
  const upstream404Played = await upstream404App.inject({
    method: "POST",
    url: `/UserPlayedItems/${bridgeItemId("movie:imdb:tt0078748")}`,
    headers: { "X-MediaBrowser-Token": upstream404Login.json().AccessToken }
  });
  assert.equal(upstream404Played.statusCode, 502);
  assert.equal(upstream404Store.getUserData(upstream404Login.json().User.Id, bridgeItemId("movie:imdb:tt0078748")).Played, false);
  await upstream404App.close();
  upstream404Store.close();

  const userLookupFailureStore = new Store(":memory:");
  userLookupFailureStore.upsertIndexedItem({
    serverId: "main",
    itemId: "main-alien",
    libraryId: "library-a",
    itemType: "Movie",
    logicalKey: "movie:imdb:tt0078748",
    json: { Id: "main-alien", Type: "Movie", Name: "Alien", ProviderIds: { Imdb: "tt0078748" } }
  });
  const userLookupFailureClient = new UpstreamClient(config.upstreams, {
    request: async () => ({
      statusCode: 404,
      headers: {},
      body: { json: async () => ({ error: "users not found" }) }
    }),
    retries: 0
  });
  const userLookupFailureApp = buildApp({ config, store: userLookupFailureStore, upstream: userLookupFailureClient });
  const userLookupFailureLogin = await userLookupFailureApp.inject({ method: "POST", url: "/Users/AuthenticateByName", payload: { Username: "alice", Pw: "secret" } });
  const userLookupFailurePlayed = await userLookupFailureApp.inject({
    method: "POST",
    url: `/UserPlayedItems/${bridgeItemId("movie:imdb:tt0078748")}`,
    headers: { "X-MediaBrowser-Token": userLookupFailureLogin.json().AccessToken }
  });
  assert.equal(userLookupFailurePlayed.statusCode, 502);
  assert.equal(userLookupFailureStore.getUserData(userLookupFailureLogin.json().User.Id, bridgeItemId("movie:imdb:tt0078748")).Played, false);
  await userLookupFailureApp.close();
  userLookupFailureStore.close();

  const nonJsonStore = new Store(":memory:");
  nonJsonStore.upsertIndexedItem({
    serverId: "main",
    itemId: "main-alien",
    libraryId: "library-a",
    itemType: "Movie",
    logicalKey: "movie:imdb:tt0078748",
    json: { Id: "main-alien", Type: "Movie", Name: "Alien", ProviderIds: { Imdb: "tt0078748" } }
  });
  const nonJsonUpstream = new UpstreamClient(config.upstreams, {
    request: async (url) => {
      if (url.pathname === "/Users") {
        return {
          statusCode: 200,
          headers: {},
          body: { json: async () => [{ Id: "main-user", Name: "alice" }] }
        };
      }
      return { statusCode: 200, headers: {}, body: "not json" };
    },
    retries: 0
  });
  const nonJsonApp = buildApp({ config, store: nonJsonStore, upstream: nonJsonUpstream });
  const nonJsonLogin = await nonJsonApp.inject({ method: "POST", url: "/Users/AuthenticateByName", payload: { Username: "alice", Pw: "secret" } });
  const nonJsonPlayed = await nonJsonApp.inject({
    method: "POST",
    url: `/UserPlayedItems/${bridgeItemId("movie:imdb:tt0078748")}`,
    headers: { "X-MediaBrowser-Token": nonJsonLogin.json().AccessToken }
  });
  assert.equal(nonJsonPlayed.statusCode, 502);
  assert.equal(nonJsonStore.getUserData(nonJsonLogin.json().User.Id, bridgeItemId("movie:imdb:tt0078748")).Played, false);
  await nonJsonApp.close();
  nonJsonStore.close();
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
    { method: "POST", url: `/UserItems/${itemId}/UserData?UserId=${bobId}`, payload: { Played: true } },
    { method: "POST", url: `/Users/${bobId}/Items/${itemId}/UserData`, payload: { IsFavorite: true } },
    { method: "POST", url: `/Users/${bobId}/FavoriteItems/${itemId}` },
    { method: "POST", url: `/UserPlayedItems/${itemId}?UserId=${bobId}` },
    { method: "DELETE", url: `/UserPlayedItems/${itemId}?userId=${bobId}` },
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

test("rejects user-data writes for missing items", async () => {
  const passwordHash = await hash("secret");
  const config: BridgeConfig = {
    server: { bind: "127.0.0.1", port: 8096, publicUrl: "http://bridge.test", name: "Bridge" },
    auth: { users: [{ name: "alice", passwordHash }] },
    upstreams: [],
    libraries: []
  };
  const store = new Store(":memory:");
  const app = buildApp({ config, store });
  const login = await app.inject({ method: "POST", url: "/Users/AuthenticateByName", payload: { Username: "alice", Pw: "secret" } });
  const token = login.json().AccessToken;
  const missingItemId = "0123456789abcdef0123456789abcdef";

  for (const request of [
    { method: "POST", url: `/Users/${login.json().User.Id}/Items/${missingItemId}/UserData`, payload: { Played: true } },
    { method: "POST", url: `/Users/${login.json().User.Id}/FavoriteItems/${missingItemId}` },
    { method: "POST", url: `/Users/${login.json().User.Id}/PlayedItems/${missingItemId}` }
  ] as const) {
    const response = await app.inject({
      ...request,
      headers: { "X-MediaBrowser-Token": token }
    });
    assert.equal(response.statusCode, 404, `${request.method} ${request.url}`);
  }
  assert.equal(store.getUserData(login.json().User.Id, missingItemId).Played, false);

  await app.close();
  store.close();
});

test("root item browse returns all views without applying item paging", async () => {
  const passwordHash = await hash("secret");
  const config: BridgeConfig = {
    server: { bind: "127.0.0.1", port: 8096, publicUrl: "http://bridge.test", name: "Bridge" },
    auth: { users: [{ name: "alice", passwordHash }] },
    upstreams: [],
    libraries: [
      { id: "movies", name: "Movies", collectionType: "movies", sources: [] },
      { id: "shows", name: "Shows", collectionType: "tvshows", sources: [] }
    ]
  };
  const store = new Store(":memory:");
  const app = buildApp({ config, store });
  const login = await app.inject({ method: "POST", url: "/Users/AuthenticateByName", payload: { Username: "alice", Pw: "secret" } });

  const rootItems = await app.inject({
    method: "GET",
    url: "/Items?StartIndex=1&Limit=1",
    headers: { "X-MediaBrowser-Token": login.json().AccessToken }
  });

  assert.equal(rootItems.statusCode, 200);
  assert.equal(rootItems.json().StartIndex, 1);
  assert.equal(rootItems.json().TotalRecordCount, 2);
  assert.deepEqual(rootItems.json().Items.map((item: any) => item.Name), ["Movies", "Shows"]);

  await app.close();
  store.close();
});

test("root item browse discovers pass-through upstream views live", async () => {
  const passwordHash = await hash("secret");
  const config: BridgeConfig = {
    server: { bind: "127.0.0.1", port: 8096, publicUrl: "http://bridge.test", name: "Bridge" },
    auth: { users: [{ name: "alice", passwordHash }] },
    upstreams: [{ id: "main", name: "Main", url: "https://main.example.com", token: "token" }],
    libraries: []
  };
  const upstream = new FakeUpstream({
    "main:/Users": [{ Id: "main-user", Name: "alice" }],
    "main:/UserViews": { Items: [{ Id: "youtube-lib", Name: "YouTube", CollectionType: "homevideos" }] }
  });
  const store = new Store(":memory:");
  const app = buildApp({ config, store, upstream });
  const login = await app.inject({ method: "POST", url: "/Users/AuthenticateByName", payload: { Username: "alice", Pw: "secret" } });

  const rootItems = await app.inject({
    method: "GET",
    url: "/Items",
    headers: { "X-MediaBrowser-Token": login.json().AccessToken }
  });

  assert.equal(rootItems.statusCode, 200);
  assert.deepEqual(rootItems.json().Items.map((item: any) => item.Name), ["Main - YouTube"]);

  await app.close();
  store.close();
});

test("paged cached item browse reads the indexed catalog once on cache hits", async () => {
  const passwordHash = await hash("secret");
  const config: BridgeConfig = {
    server: { bind: "127.0.0.1", port: 8096, publicUrl: "http://bridge.test", name: "Bridge" },
    auth: { users: [{ name: "alice", passwordHash }] },
    upstreams: [{ id: "main", name: "Main", url: "https://main.example.com", token: "token" }],
    libraries: [{ id: "movies", name: "Movies", collectionType: "movies", sources: [{ server: "main", libraryId: "library-a" }] }]
  };
  const store = new Store(":memory:");
  for (let index = 0; index < 5; index += 1) {
    store.upsertIndexedItem({
      serverId: "main",
      itemId: `movie-${index}`,
      libraryId: "library-a",
      itemType: "Movie",
      logicalKey: `movie:tmdb:${index}`,
      json: { Id: `movie-${index}`, Type: "Movie", Name: `Movie ${index}`, SortName: `Movie ${index}`, ProviderIds: { Tmdb: String(index) } }
    });
  }
  const originalListIndexedItems = store.listIndexedItems.bind(store);
  let catalogReads = 0;
  store.listIndexedItems = () => {
    catalogReads += 1;
    return originalListIndexedItems();
  };
  const app = buildApp({ config, store });
  const login = await app.inject({ method: "POST", url: "/Users/AuthenticateByName", payload: { Username: "alice", Pw: "secret" } });

  const response = await app.inject({
    method: "GET",
    url: "/Items?Recursive=true&IncludeItemTypes=Movie&StartIndex=1&Limit=2",
    headers: { "X-MediaBrowser-Token": login.json().AccessToken }
  });

  assert.equal(response.statusCode, 200);
  assert.equal(response.json().TotalRecordCount, 5);
  assert.deepEqual(response.json().Items.map((item: any) => item.Name), ["Movie 1", "Movie 2"]);
  assert.equal(catalogReads, 1);

  await app.close();
  store.close();
});

test("recursive item browse pushes include item types into the indexed catalog read", async () => {
  const passwordHash = await hash("secret");
  const config: BridgeConfig = {
    server: { bind: "127.0.0.1", port: 8096, publicUrl: "http://bridge.test", name: "Bridge" },
    auth: { users: [{ name: "alice", passwordHash }] },
    upstreams: [{ id: "main", name: "Main", url: "https://main.example.com", token: "token" }],
    libraries: [{ id: "media", name: "Media", collectionType: "mixed", sources: [{ server: "main", libraryId: "library-a" }] }]
  };
  const store = new Store(":memory:");
  store.upsertIndexedItem({
    serverId: "main",
    itemId: "movie-a",
    libraryId: "library-a",
    itemType: "Movie",
    logicalKey: "movie:tmdb:100",
    json: { Id: "movie-a", Type: "Movie", Name: "Movie A", ProviderIds: { Tmdb: "100" } }
  });
  store.upsertIndexedItem({
    serverId: "main",
    itemId: "series-a",
    libraryId: "library-a",
    itemType: "Series",
    logicalKey: "series:tvdb:200",
    json: { Id: "series-a", Type: "Series", Name: "Series A", ProviderIds: { Tvdb: "200" } }
  });
  type InstrumentedStore = Store & { listIndexedItems: (itemTypes?: string[]) => ReturnType<Store["listIndexedItems"]> };
  const instrumented = store as InstrumentedStore;
  const originalListIndexedItems = instrumented.listIndexedItems.bind(store);
  let requestedItemTypes: string[] = [];
  instrumented.listIndexedItems = (itemTypes = []) => {
    requestedItemTypes = itemTypes;
    return originalListIndexedItems(itemTypes);
  };
  const app = buildApp({ config, store });
  const login = await app.inject({ method: "POST", url: "/Users/AuthenticateByName", payload: { Username: "alice", Pw: "secret" } });

  const response = await app.inject({
    method: "GET",
    url: "/Items?Recursive=true&includeItemTypes=movie",
    headers: { "X-MediaBrowser-Token": login.json().AccessToken }
  });

  assert.equal(response.statusCode, 200);
  assert.deepEqual(requestedItemTypes.map((type) => type.toLowerCase()), ["movie"]);
  assert.deepEqual(response.json().Items.map((item: any) => item.Name), ["Movie A"]);

  await app.close();
  store.close();
});

test("cached bridge item browse batches user data and related source id hydration", async () => {
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
    json: { Id: "series-a", Type: "Series", Name: "Series A", ProviderIds: { Tvdb: "100" } }
  });
  store.upsertIndexedItem({
    serverId: "main",
    itemId: "season-a",
    libraryId: "shows-lib",
    itemType: "Season",
    logicalKey: "season:series:series-a:season:1",
    json: { Id: "season-a", Type: "Season", Name: "Season 1", SeriesId: "series-a", IndexNumber: 1 }
  });
  for (let index = 1; index <= 3; index += 1) {
    store.upsertIndexedItem({
      serverId: "main",
      itemId: `episode-${index}`,
      libraryId: "shows-lib",
      itemType: "Episode",
      logicalKey: `episode:series:series-a:season:1:episode:${index}`,
      json: {
        Id: `episode-${index}`,
        Type: "Episode",
        Name: `Episode ${index}`,
        ParentId: "season-a",
        SeriesId: "series-a",
        SeasonId: "season-a",
        ParentIndexNumber: 1,
        IndexNumber: index
      }
    });
  }
  const originalGetUserData = store.getUserData.bind(store);
  const originalFindSourceId = store.findIndexedItemsBySourceId.bind(store);
  let singleUserDataReads = 0;
  let singleSourceIdReads = 0;
  store.getUserData = (userIdValue, itemIdValue) => {
    singleUserDataReads += 1;
    return originalGetUserData(userIdValue, itemIdValue);
  };
  store.findIndexedItemsBySourceId = (itemIdValue) => {
    singleSourceIdReads += 1;
    return originalFindSourceId(itemIdValue);
  };
  const app = buildApp({ config, store });
  const login = await app.inject({ method: "POST", url: "/Users/AuthenticateByName", payload: { Username: "alice", Pw: "secret" } });

  const response = await app.inject({
    method: "GET",
    url: "/Items?Recursive=true&IncludeItemTypes=Episode&StartIndex=0&Limit=2",
    headers: { "X-MediaBrowser-Token": login.json().AccessToken }
  });

  assert.equal(response.statusCode, 200);
  assert.equal(response.json().TotalRecordCount, 3);
  assert.deepEqual(response.json().Items.map((item: any) => item.Name), ["Episode 1", "Episode 2"]);
  assert.equal(singleUserDataReads, 0);
  assert.equal(singleSourceIdReads, 0);

  await app.close();
  store.close();
});

test("cached item type filtering preserves priority source selection for merged items", async () => {
  const passwordHash = await hash("secret");
  const config: BridgeConfig = {
    server: { bind: "127.0.0.1", port: 8096, publicUrl: "http://bridge.test", name: "Bridge" },
    auth: { users: [{ name: "alice", passwordHash }] },
    upstreams: [
      { id: "main", name: "Main", url: "https://main.example.com", token: "token" },
      { id: "remote", name: "Remote", url: "https://remote.example.com", token: "token" }
    ],
    libraries: [{ id: "media", name: "Media", collectionType: "mixed", sources: [{ server: "main", libraryId: "library-a" }, { server: "remote", libraryId: "library-b" }] }]
  };
  const store = new Store(":memory:");
  store.upsertIndexedItem({
    serverId: "main",
    itemId: "main-series",
    libraryId: "library-a",
    itemType: "Series",
    logicalKey: "shared:item:1",
    json: { Id: "main-series", Type: "Series", Name: "Priority Series", ParentId: "library-a" }
  });
  store.upsertIndexedItem({
    serverId: "remote",
    itemId: "remote-movie",
    libraryId: "library-b",
    itemType: "Movie",
    logicalKey: "shared:item:1",
    json: { Id: "remote-movie", Type: "Movie", Name: "Lower Priority Movie", ParentId: "library-b" }
  });
  const upstream = new FakeUpstream({
    "main:/Items": { Items: [], TotalRecordCount: 0 },
    "remote:/Items": { Items: [], TotalRecordCount: 0 }
  });
  const app = buildApp({ config, store, upstream });
  const login = await app.inject({ method: "POST", url: "/Users/AuthenticateByName", payload: { Username: "alice", Pw: "secret" } });

  const response = await app.inject({
    method: "GET",
    url: "/Items?Recursive=true&IncludeItemTypes=Movie",
    headers: { "X-MediaBrowser-Token": login.json().AccessToken }
  });

  assert.equal(response.statusCode, 200);
  assert.equal(response.json().TotalRecordCount, 0);
  assert.deepEqual(response.json().Items, []);

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
    json: { Id: "main-alien", Type: "Movie", Name: "Alien", ServerId: "main", Genres: ["Horror"], RunTimeTicks: 4_000_000_000, DateCreated: "2024-01-01T00:00:00.000Z", ProviderIds: { Imdb: "tt0078748" } }
  });
  store.upsertIndexedItem({
    serverId: "remote",
    itemId: "remote-alien",
    libraryId: "library-b",
    itemType: "Movie",
    logicalKey: "movie:imdb:tt0078748",
    json: { Id: "remote-alien", Type: "Movie", Name: "Alien", ServerId: "remote", Genres: ["Horror"], RunTimeTicks: 4_000_000_000, DateCreated: "2024-01-01T00:00:00.000Z", ProviderIds: { Imdb: "tt0078748" } }
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
  const playstateUpstream = new FakeUpstream({
    "main:/Users": [{ Id: "main-user", Name: "alice" }],
    "remote:/Users": [{ Id: "remote-user", Name: "alice" }],
    "main:/UserPlayedItems/main-alien": { Played: true },
    "remote:/UserPlayedItems/remote-alien": { Played: true }
  });
  const app = buildApp({ config, store, upstream: playstateUpstream });

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

  const rootItems = await app.inject({ method: "GET", url: "/Items", headers: { "X-MediaBrowser-Token": token } });
  assert.equal(rootItems.statusCode, 200);
  assert.deepEqual(rootItems.json().Items.map((item: any) => item.Name), ["Movies"]);

  const items = await app.inject({ method: "GET", url: "/Items?Recursive=true&IncludeItemTypes=Movie", headers: { "X-MediaBrowser-Token": token } });
  assert.equal(items.statusCode, 200);
  assert.equal(items.json().TotalRecordCount, 3);
  assert.deepEqual(items.json().Items.map((item: any) => item.Name), ["Alien", "Arrival", "The Thing"]);
  assert.equal(items.json().Items[0].Id, alienBridgeId);
  assert.equal(items.json().Items[0].UserData.IsFavorite, true);

  const lowerCasePagedItems = await app.inject({
    method: "GET",
    url: "/Items?recursive=true&includeItemTypes=Movie&startIndex=1&limit=1",
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
    url: "/Items?Recursive=true&IncludeItemTypes=Movie&Filters=IsFavorite",
    headers: { "X-MediaBrowser-Token": token }
  });
  assert.equal(filteredByItemFilter.statusCode, 200);
  assert.deepEqual(filteredByItemFilter.json().Items.map((item: any) => item.Name), ["Alien"]);

  const filtered = await app.inject({
    method: "GET",
    url: "/Items?Recursive=true&IncludeItemTypes=Movie&Genres=Horror&IsFavorite=true&SortBy=SortName&SortOrder=Descending",
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

  const upstream = new FakeUpstream({
    "main:/Items/main-alien/Images": [{ ImageType: "Primary", ImageIndex: 0, Path: "primary.jpg" }]
  });
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
  upstream.rawResponses["main:/Items/main-alien/Images/Primary/0/primary-tag/jpg/320/180/0/0"] = {
    statusCode: 200,
    headers: { "content-type": "image/jpeg", "content-length": "3" },
    body: Buffer.from("old")
  };
  const legacyImage = await imageApp.inject({
    method: "GET",
    url: `/Items/${alienBridgeId}/Images/Primary/0/primary-tag/jpg/320/180/0/0`
  });
  assert.equal(legacyImage.statusCode, 200);
  assert.equal(legacyImage.headers["content-type"], "image/jpeg");
  assert.equal(legacyImage.body, "old");
  const imageInfos = await imageApp.inject({
    method: "GET",
    url: `/Items/${alienBridgeId}/Images`,
    headers: { "X-MediaBrowser-Token": token }
  });
  assert.equal(imageInfos.statusCode, 200);
  assert.deepEqual(imageInfos.json(), [{ ImageType: "Primary", ImageIndex: 0, Path: "primary.jpg" }]);
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
    payload: { ItemId: alienBridgeId, PositionTicks: 600_000_000 }
  });
  assert.equal(progress.statusCode, 204);

  const resumable = await app.inject({
    method: "GET",
    url: "/Items?Recursive=true&IncludeItemTypes=Movie&Filters=IsResumable",
    headers: { "X-MediaBrowser-Token": token }
  });
  assert.equal(resumable.statusCode, 200);
  assert.deepEqual(resumable.json().Items.map((item: any) => item.Name), ["Alien"]);

  const resume = await app.inject({ method: "GET", url: "/UserItems/Resume", headers: { "X-MediaBrowser-Token": token } });
  assert.equal(resume.statusCode, 200);
  assert.deepEqual(resume.json().Items.map((item: any) => item.Name), ["Alien"]);

  const stopped = await app.inject({
    method: "POST",
    url: "/Sessions/Playing/Stopped",
    headers: { "X-MediaBrowser-Token": token },
    payload: { ItemId: alienBridgeId, PositionTicks: 3_800_000_000 }
  });
  assert.equal(stopped.statusCode, 204);
  const completedUserData = store.getUserData(auth.User.Id, alienBridgeId);
  assert.equal(completedUserData.Played, true);
  assert.equal(completedUserData.PlaybackPositionTicks, 0);

  const legacyResume = await app.inject({
    method: "GET",
    url: `/Users/${auth.User.Id}/Items/Resume?ParentId=${bridgeLibraryId("movies")}&IncludeItemTypes=Movie`,
    headers: { "X-MediaBrowser-Token": token }
  });
  assert.equal(legacyResume.statusCode, 200);
  assert.deepEqual(legacyResume.json().Items.map((item: any) => item.Name), []);

  const datePlayed = "2024-05-01T00:00:00.000Z";
  const legacyPlayed = await app.inject({
    method: "POST",
    url: `/Users/${auth.User.Id}/PlayedItems/${alienBridgeId}?datePlayed=${encodeURIComponent(datePlayed)}`,
    headers: { "X-MediaBrowser-Token": token }
  });
  assert.equal(legacyPlayed.statusCode, 200);
  assert.equal(legacyPlayed.json().Played, true);
  assert.equal(legacyPlayed.json().PlayCount, 1);
  assert.equal(legacyPlayed.json().LastPlayedDate, datePlayed);

  const legacyUnplayed = await app.inject({
    method: "DELETE",
    url: `/Users/${auth.User.Id}/PlayedItems/${alienBridgeId}`,
    headers: { "X-MediaBrowser-Token": token }
  });
  assert.equal(legacyUnplayed.statusCode, 200);
  assert.equal(legacyUnplayed.json().Played, false);
  assert.equal(legacyUnplayed.json().PlayCount, 0);
  assert.equal(legacyUnplayed.json().LastPlayedDate, null);

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

test("serves generic child browse live for bridge item parents", async () => {
  const passwordHash = await hash("secret");
  const config: BridgeConfig = {
    server: { bind: "127.0.0.1", port: 8096, publicUrl: "http://bridge.test", name: "Bridge" },
    auth: { users: [{ name: "alice", passwordHash }] },
    upstreams: [{ id: "main", name: "Main", url: "https://main.example.com", token: "token" }],
    libraries: [{ id: "shows", name: "Series", collectionType: "tvshows", sources: [{ server: "main", libraryId: "shows-lib" }] }]
  };
  const upstream = new FakeUpstream({
    "main:/Items": (_serverId: string, _path: string, init: any) => {
      if (init.query.ParentId === "shows-lib") {
        return {
          Items: [{ Id: "series-a", Type: "Series", Name: "Example Series", ProviderIds: { Tvdb: "100" } }],
          TotalRecordCount: 1,
          StartIndex: init.query.StartIndex
        };
      }
      if (init.query.ParentId === "series-a") {
        return {
          Items: [{ Id: "season-a", Type: "Season", Name: "Season 1", SeriesId: "series-a", IndexNumber: 1 }],
          TotalRecordCount: 1,
          StartIndex: init.query.StartIndex
        };
      }
      return { Items: [], TotalRecordCount: 0, StartIndex: init.query.StartIndex };
    }
  });
  const store = new Store(":memory:");
  const app = buildApp({ config, store, upstream });
  const login = await app.inject({ method: "POST", url: "/Users/AuthenticateByName", payload: { Username: "alice", Pw: "secret" } });
  const token = login.json().AccessToken;

  const series = await app.inject({
    method: "GET",
    url: `/Items?ParentId=${bridgeLibraryId("shows")}&IncludeItemTypes=Series`,
    headers: { "X-MediaBrowser-Token": token }
  });
  assert.equal(series.statusCode, 200);
  const seriesId = series.json().Items[0].Id;

  const seasons = await app.inject({
    method: "GET",
    url: `/Items?ParentId=${seriesId}`,
    headers: { "X-MediaBrowser-Token": token }
  });
  assert.equal(seasons.statusCode, 200);
  assert.deepEqual(seasons.json().Items.map((item: any) => item.Name), ["Season 1"]);
  assert.deepEqual(upstream.requests.map((request) => (request.init as any).query.ParentId), ["shows-lib", "series-a"]);

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

test("serves live pass-through library folders when cached parent ids differ from the library", async () => {
  const passwordHash = await hash("secret");
  const config: BridgeConfig = {
    server: { bind: "127.0.0.1", port: 8096, publicUrl: "http://bridge.test", name: "Bridge" },
    auth: { users: [{ name: "alice", passwordHash }] },
    upstreams: [{ id: "main", name: "Main", url: "https://main.example.com", token: "token" }],
    libraries: []
  };
  const store = new Store(":memory:");
  store.upsertUpstreamLibrary({ serverId: "main", libraryId: "library-a", name: "Archive", collectionType: "homevideos" });
  store.upsertIndexedItem({
    serverId: "main",
    itemId: "folder-a",
    libraryId: "library-a",
    itemType: "Folder",
    logicalKey: "source:main:folder-a",
    json: { Id: "folder-a", Type: "Folder", IsFolder: true, Name: "Folder A", ParentId: "upstream-root" }
  });
  const upstream = new FakeUpstream({
    "main:/Items": (_serverId: string, _path: string, init: any) => ({
      Items: [{ Id: "folder-a", Type: "Folder", IsFolder: true, Name: "Folder A", ParentId: "upstream-root" }],
      TotalRecordCount: 1,
      StartIndex: init.query.StartIndex
    })
  });
  const app = buildApp({ config, store, upstream });
  const login = await app.inject({ method: "POST", url: "/Users/AuthenticateByName", payload: { Username: "alice", Pw: "secret" } });
  const parentId = passThroughLibraryId("main", "library-a");

  const response = await app.inject({
    method: "GET",
    url: `/Users/${login.json().User.Id}/Items?ParentId=${parentId}&Recursive=false&Limit=30`,
    headers: { "X-MediaBrowser-Token": login.json().AccessToken }
  });

  assert.equal(response.statusCode, 200);
  assert.equal(response.json().TotalRecordCount, 1);
  assert.deepEqual(response.json().Items.map((item: any) => [item.Name, item.Type, item.IsFolder]), [["Folder A", "Folder", true]]);
  assert.equal(response.json().Items[0].Id, bridgeItemId("source:main:folder-a"));
  assert.equal((upstream.requests[0].init as any).query.ParentId, "library-a");
  assert.equal((upstream.requests[0].init as any).query.Recursive, "false");

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
  store.upsertUpstreamLibrary({ serverId: "main", libraryId: "library-e", name: "Unknown Videos", collectionType: null });
  const upstream = new FakeUpstream({
    "main:/Items": { Items: [], TotalRecordCount: 0, StartIndex: 0 }
  });
  const app = buildApp({ config, store, upstream });
  const login = await app.inject({ method: "POST", url: "/Users/AuthenticateByName", payload: { Username: "alice", Pw: "secret" } });

  const views = await app.inject({
    method: "GET",
    url: "/UserViews",
    headers: { "X-MediaBrowser-Token": login.json().AccessToken }
  });

  assert.equal(views.statusCode, 200);
  assert.deepEqual(views.json().Items.map((item: any) => item.Name), ["Movies", "Main - TV", "Main - Home Videos", "Main - Unknown Videos"]);
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
  store.upsertIndexedItem({
    serverId: "main",
    itemId: "unknown-video",
    libraryId: "library-e",
    itemType: "Video",
    logicalKey: "source:main:unknown-video",
    json: { Id: "unknown-video", Type: "Video", MediaType: "Video", Name: "Unknown Video" }
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
  assert.equal(movieFilteredVideos.json().TotalRecordCount, 0);

  const movieFilteredUnknownVideos = await app.inject({
    method: "GET",
    url: `/Items?ParentId=${passThroughLibraryId("main", "library-e")}&IncludeItemTypes=Movie`,
    headers: { "X-MediaBrowser-Token": login.json().AccessToken }
  });
  assert.equal(movieFilteredUnknownVideos.statusCode, 200);
  assert.equal(movieFilteredUnknownVideos.json().TotalRecordCount, 0);

  await app.close();
  store.close();
});

test("does not rewrite pass-through homevideos Movie browse requests to Video", async () => {
  const passwordHash = await hash("secret");
  const config: BridgeConfig = {
    server: { bind: "127.0.0.1", port: 8096, publicUrl: "http://bridge.test", name: "Bridge" },
    auth: { users: [{ name: "alice", passwordHash }] },
    upstreams: [{ id: "main", name: "Main", url: "https://main.example.com", token: "token" }],
    libraries: []
  };
  const store = new Store(":memory:");
  store.upsertUpstreamLibrary({ serverId: "main", libraryId: "homevideos-lib", name: "Home Videos", collectionType: "homevideos" });
  const upstream = new FakeUpstream({
    "main:/Users": [{ Id: "main-user", Name: "alice" }],
    "main:/Items": (_serverId: string, _path: string, init: any) => ({
      Items: init.query.IncludeItemTypes === "Movie"
        ? [{ Id: "home-video", Type: "Video", MediaType: "Video", Name: "Home Clip" }]
        : [],
      TotalRecordCount: init.query.IncludeItemTypes === "Movie" ? 1 : 0,
      StartIndex: init.query.StartIndex
    })
  });
  const app = buildApp({ config, store, upstream });
  const login = await app.inject({ method: "POST", url: "/Users/AuthenticateByName", payload: { Username: "alice", Pw: "secret" } });

  const items = await app.inject({
    method: "GET",
    url: `/Items?ParentId=${passThroughLibraryId("main", "homevideos-lib")}&IncludeItemTypes=Movie`,
    headers: { "X-MediaBrowser-Token": login.json().AccessToken }
  });

  assert.equal(items.statusCode, 200);
  assert.deepEqual(items.json().Items, []);
  assert.deepEqual(upstream.requests
    .filter((request) => request.path === "/Items")
    .map((request) => (request.init as any).query.IncludeItemTypes), ["Movie"]);

  await app.close();
  store.close();
});

test("does not rewrite configured homevideos Movie browse requests to Video", async () => {
  const passwordHash = await hash("secret");
  const config: BridgeConfig = {
    server: { bind: "127.0.0.1", port: 8096, publicUrl: "http://bridge.test", name: "Bridge" },
    auth: { users: [{ name: "alice", passwordHash }] },
    upstreams: [{ id: "main", name: "Main", url: "https://main.example.com", token: "token" }],
    libraries: [{ id: "videos", name: "Videos", collectionType: "homevideos", sources: [{ server: "main", libraryId: "homevideos-lib" }] }]
  };
  const store = new Store(":memory:");
  const upstream = new FakeUpstream({
    "main:/Users": [{ Id: "main-user", Name: "alice" }],
    "main:/Items": (_serverId: string, _path: string, init: any) => ({
      Items: init.query.IncludeItemTypes === "Movie"
        ? [{ Id: "home-video", Type: "Video", MediaType: "Video", Name: "Home Clip" }]
        : [],
      TotalRecordCount: init.query.IncludeItemTypes === "Movie" ? 1 : 0,
      StartIndex: init.query.StartIndex
    })
  });
  const app = buildApp({ config, store, upstream });
  const login = await app.inject({ method: "POST", url: "/Users/AuthenticateByName", payload: { Username: "alice", Pw: "secret" } });

  const items = await app.inject({
    method: "GET",
    url: `/Items?ParentId=${bridgeLibraryId("videos")}&IncludeItemTypes=Movie`,
    headers: { "X-MediaBrowser-Token": login.json().AccessToken }
  });

  assert.equal(items.statusCode, 200);
  assert.deepEqual(items.json().Items, []);
  assert.deepEqual(upstream.requests
    .filter((request) => request.path === "/Items")
    .map((request) => (request.init as any).query.IncludeItemTypes), ["Movie"]);

  await app.close();
  store.close();
});

test("does not treat unknown pass-through browse collections as homevideos", async () => {
  const passwordHash = await hash("secret");
  const config: BridgeConfig = {
    server: { bind: "127.0.0.1", port: 8096, publicUrl: "http://bridge.test", name: "Bridge" },
    auth: { users: [{ name: "alice", passwordHash }] },
    upstreams: [{ id: "main", name: "Main", url: "https://main.example.com", token: "token" }],
    libraries: []
  };
  const store = new Store(":memory:");
  store.upsertUpstreamLibrary({ serverId: "main", libraryId: "unknown-lib", name: "Unknown Videos", collectionType: null });
  const upstream = new FakeUpstream({
    "main:/Users": [{ Id: "main-user", Name: "alice" }],
    "main:/Items": (_serverId: string, _path: string, init: any) => ({
      Items: init.query.IncludeItemTypes === "Video"
        ? [{ Id: "unknown-video", Type: "Video", MediaType: "Video", Name: "Unknown Clip" }]
        : [],
      TotalRecordCount: init.query.IncludeItemTypes === "Video" ? 1 : 0,
      StartIndex: init.query.StartIndex
    })
  });
  const app = buildApp({ config, store, upstream });
  const login = await app.inject({ method: "POST", url: "/Users/AuthenticateByName", payload: { Username: "alice", Pw: "secret" } });

  const items = await app.inject({
    method: "GET",
    url: `/Items?ParentId=${passThroughLibraryId("main", "unknown-lib")}&IncludeItemTypes=Movie`,
    headers: { "X-MediaBrowser-Token": login.json().AccessToken }
  });

  assert.equal(items.statusCode, 200);
  assert.deepEqual(items.json().Items, []);
  assert.deepEqual(upstream.requests
    .filter((request) => request.path === "/Items")
    .map((request) => (request.init as any).query.IncludeItemTypes), ["Movie"]);

  await app.close();
  store.close();
});

test("library root browse honors Jellyfin recursive defaults", async () => {
  const passwordHash = await hash("secret");
  const config: BridgeConfig = {
    server: { bind: "127.0.0.1", port: 8096, publicUrl: "http://bridge.test", name: "Bridge" },
    auth: { users: [{ name: "alice", passwordHash }] },
    upstreams: [{ id: "main", name: "Main", url: "https://main.example.com", token: "token" }],
    libraries: [{ id: "videos", name: "Videos", collectionType: "homevideos", sources: [{ server: "main", libraryId: "videos-lib" }] }]
  };
  const store = new Store(":memory:");
  store.upsertIndexedItem({
    serverId: "main",
    itemId: "folder-a",
    libraryId: "videos-lib",
    itemType: "Folder",
    logicalKey: "source:main:folder-a",
    json: { Id: "folder-a", Type: "Folder", IsFolder: true, Name: "Folder A", ParentId: "videos-lib" }
  });
  store.upsertIndexedItem({
    serverId: "main",
    itemId: "root-clip",
    libraryId: "videos-lib",
    itemType: "Video",
    logicalKey: "source:main:root-clip",
    json: { Id: "root-clip", Type: "Video", MediaType: "Video", Name: "Root Clip", ParentId: "videos-lib" }
  });
  store.upsertIndexedItem({
    serverId: "main",
    itemId: "nested-clip",
    libraryId: "videos-lib",
    itemType: "Video",
    logicalKey: "source:main:nested-clip",
    json: { Id: "nested-clip", Type: "Video", MediaType: "Video", Name: "Nested Clip", ParentId: "folder-a" }
  });
  const app = buildApp({ config, store });
  const login = await app.inject({ method: "POST", url: "/Users/AuthenticateByName", payload: { Username: "alice", Pw: "secret" } });

  const root = await app.inject({
    method: "GET",
    url: `/Items?ParentId=${bridgeLibraryId("videos")}`,
    headers: { "X-MediaBrowser-Token": login.json().AccessToken }
  });
  assert.equal(root.statusCode, 200);
  assert.deepEqual(root.json().Items.map((item: any) => item.Name), ["Folder A", "Root Clip"]);

  const recursive = await app.inject({
    method: "GET",
    url: `/Items?ParentId=${bridgeLibraryId("videos")}&Recursive=true`,
    headers: { "X-MediaBrowser-Token": login.json().AccessToken }
  });
  assert.equal(recursive.statusCode, 200);
  assert.deepEqual(recursive.json().Items.map((item: any) => item.Name), ["Folder A", "Nested Clip", "Root Clip"]);

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
    url: "/Items?Recursive=true&MediaTypes=Audio",
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

test("fans out genre and studio browse to upstream Jellyfin sources", async () => {
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
    "primary:/Users": [{ Id: "primary-user", Name: "alice" }],
    "secondary:/Users": [{ Id: "secondary-user", Name: "alice" }],
    "primary:/Genres": {
      Items: [{ Id: "upstream-drama", Type: "Genre", Name: "Drama" }, { Id: "upstream-horror", Type: "Genre", Name: "Horror" }],
      TotalRecordCount: 2,
      StartIndex: 0
    },
    "secondary:/Genres": {
      Items: [{ Id: "duplicate-drama", Type: "Genre", Name: "drama" }, { Id: "upstream-comedy", Type: "Genre", Name: "Comedy" }],
      TotalRecordCount: 2,
      StartIndex: 0
    },
    "primary:/Studios": {
      Items: [{ Id: "studio-a", Type: "Studio", Name: "Example Studio" }],
      TotalRecordCount: 1,
      StartIndex: 0
    },
    "secondary:/Studios": {
      Items: [{ Id: "studio-b", Type: "Studio", Name: "example studio" }, { Id: "studio-c", Type: "Studio", Name: "Other Studio" }],
      TotalRecordCount: 2,
      StartIndex: 0
    }
  });
  const store = new Store(":memory:");
  const app = buildApp({ config, store, upstream });
  const login = await app.inject({ method: "POST", url: "/Users/AuthenticateByName", payload: { Username: "alice", Pw: "secret" } });
  const parentId = bridgeLibraryId("movies");

  const genres = await app.inject({
    method: "GET",
    url: `/Genres?ParentId=${parentId}&Limit=10&SortBy=sortName&SortOrder=Ascending&UserId=${login.json().User.Id}`,
    headers: { "X-MediaBrowser-Token": login.json().AccessToken }
  });
  assert.equal(genres.statusCode, 200);
  assert.deepEqual(genres.json().Items.map((item: any) => item.Name), ["Comedy", "Drama", "Horror"]);
  assert.equal(genres.json().Items.find((item: any) => item.Name === "Drama").Id, bridgeItemId("genre:drama"));

  const studios = await app.inject({
    method: "GET",
    url: `/Studios?ParentId=${parentId}&Limit=10&SortBy=sortName&SortOrder=Ascending&UserId=${login.json().User.Id}`,
    headers: { "X-MediaBrowser-Token": login.json().AccessToken }
  });
  assert.equal(studios.statusCode, 200);
  assert.deepEqual(studios.json().Items.map((item: any) => item.Name), ["Example Studio", "Other Studio"]);
  assert.equal(studios.json().Items.find((item: any) => item.Name === "Example Studio").Id, bridgeItemId("studio:example studio"));

  assert.deepEqual(upstream.requests
    .filter((request) => request.path === "/Genres")
    .map((request) => [request.serverId, (request.init as any).query.ParentId, (request.init as any).query.UserId]), [
    ["primary", "primary-movies", "primary-user"],
    ["secondary", "secondary-movies", "secondary-user"]
  ]);
  assert.deepEqual(upstream.requests
    .filter((request) => request.path === "/Studios")
    .map((request) => [request.serverId, (request.init as any).query.ParentId, (request.init as any).query.UserId]), [
    ["primary", "primary-movies", "primary-user"],
    ["secondary", "secondary-movies", "secondary-user"]
  ]);

  await app.close();
  store.close();
});

test("falls back to cached genre and studio metadata when upstream metadata fails", async () => {
  const passwordHash = await hash("secret");
  const config: BridgeConfig = {
    server: { bind: "127.0.0.1", port: 8096, publicUrl: "http://bridge.test", name: "Bridge" },
    auth: { users: [{ name: "alice", passwordHash }] },
    upstreams: [{ id: "main", name: "Main", url: "https://main.example.com", token: "token" }],
    libraries: [{ id: "movies", name: "Movies", collectionType: "movies", sources: [{ server: "main", libraryId: "movie-lib" }] }]
  };
  const store = new Store(":memory:");
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
      Studios: [{ Name: "Example Studio" }]
    }
  });
  const upstream = new FakeUpstream({
    "main:/Genres": new Error("Upstream main request failed for /Genres: offline"),
    "main:/Studios": new Error("Upstream main request failed for /Studios: offline")
  });
  const app = buildApp({ config, store, upstream });
  const login = await app.inject({ method: "POST", url: "/Users/AuthenticateByName", payload: { Username: "alice", Pw: "secret" } });
  const token = login.json().AccessToken;

  const genres = await app.inject({
    method: "GET",
    url: `/Genres?ParentId=${bridgeLibraryId("movies")}`,
    headers: { "X-MediaBrowser-Token": token }
  });
  assert.equal(genres.statusCode, 200);
  assert.deepEqual(genres.json().Items.map((item: any) => item.Name), ["Drama"]);

  const studios = await app.inject({
    method: "GET",
    url: `/Studios?ParentId=${bridgeLibraryId("movies")}`,
    headers: { "X-MediaBrowser-Token": token }
  });
  assert.equal(studios.statusCode, 200);
  assert.deepEqual(studios.json().Items.map((item: any) => item.Name), ["Example Studio"]);

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

test("cached TV episodes route applies Jellyfin paging", async () => {
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
  for (const index of [1, 2, 3]) {
    store.upsertIndexedItem({
      serverId: "main",
      itemId: `episode-${index}`,
      libraryId: "shows-lib",
      itemType: "Episode",
      logicalKey: `episode:series:series-a:season:1:episode:${index}`,
      json: { Id: `episode-${index}`, Type: "Episode", Name: `Episode ${index}`, SeriesId: "series-a", ParentIndexNumber: 1, IndexNumber: index }
    });
  }
  const app = buildApp({ config, store });
  const login = await app.inject({ method: "POST", url: "/Users/AuthenticateByName", payload: { Username: "alice", Pw: "secret" } });

  const episodes = await app.inject({
    method: "GET",
    url: `/Shows/${bridgeItemId("series:tvdb:100")}/Episodes?StartIndex=1&Limit=1`,
    headers: { "X-MediaBrowser-Token": login.json().AccessToken }
  });

  assert.equal(episodes.statusCode, 200);
  assert.equal(episodes.json().StartIndex, 1);
  assert.equal(episodes.json().TotalRecordCount, 3);
  assert.deepEqual(episodes.json().Items.map((item: any) => item.Name), ["Episode 2"]);

  await app.close();
  store.close();
});

test("cached resume recurses under library parents and sorts by date played", async () => {
  const passwordHash = await hash("secret");
  const config: BridgeConfig = {
    server: { bind: "127.0.0.1", port: 8096, publicUrl: "http://bridge.test", name: "Bridge" },
    auth: { users: [{ name: "alice", passwordHash }] },
    upstreams: [{ id: "main", name: "Main", url: "https://main.example.com", token: "token" }],
    libraries: [{ id: "shows", name: "Shows", collectionType: "tvshows", sources: [{ server: "main", libraryId: "shows-lib" }] }]
  };
  const store = new Store(":memory:");
  const app = buildApp({ config, store });
  const login = await app.inject({ method: "POST", url: "/Users/AuthenticateByName", payload: { Username: "alice", Pw: "secret" } });
  const userIdValue = login.json().User.Id;
  for (const index of [1, 2]) {
    const logicalKey = `episode:series:series-a:season:1:episode:${index}`;
    const bridgeId = bridgeItemId(logicalKey);
    store.upsertIndexedItem({
      serverId: "main",
      itemId: `episode-${index}`,
      libraryId: "shows-lib",
      itemType: "Episode",
      logicalKey,
      json: {
        Id: `episode-${index}`,
        Type: "Episode",
        Name: `Episode ${index}`,
        ParentId: "season-a",
        SeriesId: "series-a",
        SeasonId: "season-a",
        ParentIndexNumber: 1,
        IndexNumber: index
      }
    });
    store.upsertUserData(userIdValue, bridgeId, {
      playbackPositionTicks: 100,
      lastPlayedDate: index === 1 ? "2026-01-01T00:00:00.000Z" : "2026-02-01T00:00:00.000Z"
    });
  }

  const resume = await app.inject({
    method: "GET",
    url: `/UserItems/Resume?ParentId=${bridgeLibraryId("shows")}&Limit=20`,
    headers: { "X-MediaBrowser-Token": login.json().AccessToken }
  });

  assert.equal(resume.statusCode, 200);
  assert.deepEqual(resume.json().Items.map((item: any) => item.Name), ["Episode 2", "Episode 1"]);

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
    json: { Id: "main-alien", Type: "Movie", Name: "Alien", RunTimeTicks: 4_000_000_000, ProviderIds: { Imdb: "tt0078748" } }
  });
  store.upsertIndexedItem({
    serverId: "remote",
    itemId: "remote-alien",
    libraryId: "library-b",
    itemType: "Movie",
    logicalKey: "movie:imdb:tt0078748",
    json: { Id: "remote-alien", Type: "Movie", Name: "Alien", RunTimeTicks: 4_000_000_000, ProviderIds: { Imdb: "tt0078748" } }
  });
  const upstream = new FakeUpstream({
    "main:/Users": [{ Id: "main-user", Name: "alice" }],
	    "main:/Items/main-alien/PlaybackInfo": {
	      MediaSources: [{
	        Id: "source-main",
	        Path: "/media/alien.mkv",
	        TranscodingUrl: "/videos/main-alien/master.m3u8?MediaSourceId=source-main&PlaySessionId=upstream-play-session&ApiKey=upstream-token",
        MediaStreams: [{
          Type: "Subtitle",
          Index: 2,
          DeliveryUrl: "/Videos/main-alien/source-main/Subtitles/2/0/Stream.vtt?api_key=upstream-token"
        }],
        MediaAttachments: [{
          Index: 0,
          DeliveryUrl: "/Videos/main-alien/source-main/Attachments/0?api_key=upstream-token"
	        }]
	      }],
	      Trickplay: {
	        "source-main": {
	          "320": { Width: 320, TileWidth: 320, TileHeight: 180 }
	        }
	      },
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
  const playbackRequests = () => upstream.requests.filter((request) => request.path === "/Items/main-alien/PlaybackInfo");
  assert.equal(playbackRequests()[0].serverId, "main");
  assert.equal((playbackRequests()[0].init as any).body.UserId, "main-user");
	  assert.equal(playback.json().MediaSources[0].ItemId, itemId);
	  assert.equal(playback.json().MediaSources[0].Id, bridgeMediaSourceId("main", itemId, "source-main"));
	  assert.deepEqual(Object.keys(playback.json().Trickplay), [playback.json().MediaSources[0].Id]);
	  assert.match(playback.json().PlaySessionId, /^[0-9a-f]{64}$/);
  assert.notEqual(playback.json().PlaySessionId, "upstream-play-session");
  assert.equal(store.findMediaSourceMapping(playback.json().MediaSources[0].Id)?.upstreamMediaSourceId, "source-main");
  const transcodingUrl = new URL(playback.json().MediaSources[0].TranscodingUrl, "http://bridge.test");
  assert.equal(transcodingUrl.pathname, `/Videos/${itemId}/master.m3u8`);
  assert.equal(transcodingUrl.searchParams.get("MediaSourceId"), playback.json().MediaSources[0].Id);
  assert.equal(transcodingUrl.searchParams.get("PlaySessionId"), playback.json().PlaySessionId);
  assert.notEqual(transcodingUrl.searchParams.get("ApiKey"), "upstream-token");
  const subtitleDeliveryUrl = new URL(playback.json().MediaSources[0].MediaStreams[0].DeliveryUrl, "http://bridge.test");
  assert.equal(subtitleDeliveryUrl.pathname, `/Videos/${itemId}/${playback.json().MediaSources[0].Id}/Subtitles/2/0/Stream.vtt`);
  assert.equal(subtitleDeliveryUrl.searchParams.get("ApiKey"), token);
  assert.equal(subtitleDeliveryUrl.searchParams.get("api_key"), null);
  const attachmentDeliveryUrl = new URL(playback.json().MediaSources[0].MediaAttachments[0].DeliveryUrl, "http://bridge.test");
  assert.equal(attachmentDeliveryUrl.pathname, `/Videos/${itemId}/${playback.json().MediaSources[0].Id}/Attachments/0`);
  assert.equal(attachmentDeliveryUrl.searchParams.get("ApiKey"), token);
  assert.equal(attachmentDeliveryUrl.searchParams.get("api_key"), null);

  const selectedPlayback = await app.inject({
    method: "POST",
    url: `/Items/${itemId}/PlaybackInfo?MediaSourceId=${playback.json().MediaSources[0].Id}`,
    headers: { "X-MediaBrowser-Token": token },
    payload: { MediaSourceId: playback.json().MediaSources[0].Id }
  });
  assert.equal(selectedPlayback.statusCode, 200);
  assert.equal((playbackRequests()[1].init as any).query.MediaSourceId, "source-main");
  assert.equal((playbackRequests()[1].init as any).query.UserId, "main-user");
  assert.equal((playbackRequests()[1].init as any).body.MediaSourceId, "source-main");
  assert.equal((playbackRequests()[1].init as any).body.UserId, "main-user");

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
    url: `/Videos/${itemId}/stream.mkv?MediaSourceId=${playback.json().MediaSources[0].Id}&PlaySessionId=${playback.json().PlaySessionId}`,
    headers: { "X-MediaBrowser-Token": token, Range: "bytes=0-3" }
  });
  assert.equal(stream.statusCode, 206);
  assert.equal(stream.headers["content-type"], "video/x-matroska");
  assert.equal(stream.headers["content-range"], "bytes 0-3/10");
  assert.equal(stream.body, "data");
  assert.equal(upstream.rawRequests[0].init.query.MediaSourceId, "source-main");
  assert.equal(upstream.rawRequests[0].init.query.PlaySessionId, "upstream-play-session");
  assert.equal(upstream.rawRequests[0].init.query.mediaSourceId, undefined);
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

  upstream.rawResponses["main:/Videos/main-alien/stream"] = {
    statusCode: 200,
    headers: { "content-type": "video/x-matroska", "content-length": "4" },
    body: Readable.from(Buffer.from("data"))
  };
  const lowercaseStream = await app.inject({
    method: "GET",
    url: `/videos/${itemId}/stream?MediaSourceId=${playback.json().MediaSources[0].Id}&Static=true`,
    headers: { "X-MediaBrowser-Token": token }
  });
  assert.equal(lowercaseStream.statusCode, 200);
  assert.equal(lowercaseStream.body, "data");

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
    body: Buffer.from("#EXTM3U\nmain.m3u8?MediaSourceId=source-main&PlaySessionId=upstream-play-session\n")
  };
  const hls = await app.inject({
    method: "GET",
    url: `/Videos/${itemId}/master.m3u8?MediaSourceId=${playback.json().MediaSources[0].Id}&PlaySessionId=${playback.json().PlaySessionId}`,
    headers: { "X-MediaBrowser-Token": token }
  });
	  assert.equal(hls.statusCode, 200);
	  assert.equal(hls.body, `#EXTM3U\n/Videos/${itemId}/main.m3u8?MediaSourceId=${playback.json().MediaSources[0].Id}&PlaySessionId=${playback.json().PlaySessionId}&ApiKey=${token}\n`);

	  upstream.rawResponses["main:/Videos/main-alien/hls/legacy-playlist/stream.m3u8"] = {
	    statusCode: 200,
	    headers: { "content-type": "application/vnd.apple.mpegurl" },
	    body: Buffer.from("#EXTM3U\nsegment.ts?MediaSourceId=source-main&PlaySessionId=upstream-play-session\n")
	  };
	  const legacyHls = await app.inject({
	    method: "GET",
	    url: `/Videos/${itemId}/hls/legacy-playlist/stream.m3u8?MediaSourceId=${playback.json().MediaSources[0].Id}&PlaySessionId=${playback.json().PlaySessionId}`,
	    headers: { "X-MediaBrowser-Token": token }
	  });
	  assert.equal(legacyHls.statusCode, 200);
	  assert.equal(legacyHls.body, `#EXTM3U\n/Videos/${itemId}/hls/legacy-playlist/segment.ts?MediaSourceId=${playback.json().MediaSources[0].Id}&PlaySessionId=${playback.json().PlaySessionId}&ApiKey=${token}\n`);

  upstream.rawResponses["main:/Videos/main-alien/main.m3u8"] = {
    statusCode: 200,
    headers: { "content-type": "application/vnd.apple.mpegurl" },
    body: Buffer.from("#EXTM3U\n#EXT-X-MEDIA:TYPE=SUBTITLES,GROUP-ID=\"subs\",NAME=\"English\",URI=\"source-main/Subtitles/2/subtitles.m3u8?SegmentLength=30&ApiKey=upstream-token\"\n#EXT-X-IMAGE-STREAM-INF:BANDWIDTH=86000,URI=\"Trickplay/320/tiles.m3u8?MediaSourceId=source-main&ApiKey=upstream-token\"\nhls1/main/0.ts?MediaSourceId=source-main&PlaySessionId=upstream-play-session\n")
  };
  const variant = await app.inject({
    method: "GET",
    url: `/Videos/${itemId}/main.m3u8?MediaSourceId=${playback.json().MediaSources[0].Id}&PlaySessionId=${playback.json().PlaySessionId}`,
    headers: { "X-MediaBrowser-Token": token }
  });
  assert.equal(variant.statusCode, 200);
  assert.equal(variant.body, `#EXTM3U\n#EXT-X-MEDIA:TYPE=SUBTITLES,GROUP-ID="subs",NAME="English",URI="/Videos/${itemId}/${playback.json().MediaSources[0].Id}/Subtitles/2/subtitles.m3u8?SegmentLength=30&ApiKey=${token}"\n#EXT-X-IMAGE-STREAM-INF:BANDWIDTH=86000,URI="/Videos/${itemId}/Trickplay/320/tiles.m3u8?MediaSourceId=${playback.json().MediaSources[0].Id}&ApiKey=${token}"\n/Videos/${itemId}/hls1/main/0.ts?MediaSourceId=${playback.json().MediaSources[0].Id}&PlaySessionId=${playback.json().PlaySessionId}&ApiKey=${token}\n`);

  upstream.rawResponses["main:/Videos/main-alien/hls1/main/0.ts"] = {
    statusCode: 200,
    headers: { "content-type": "video/mp2t", "content-length": "3" },
    body: Buffer.from("seg")
  };
  const segment = await app.inject({
    method: "GET",
    url: `/Videos/${itemId}/hls1/main/0.ts?MediaSourceId=${playback.json().MediaSources[0].Id}&PlaySessionId=${playback.json().PlaySessionId}`,
    headers: { "X-MediaBrowser-Token": token }
  });
  assert.equal(segment.statusCode, 200);
  assert.equal(segment.headers["content-type"], "video/mp2t");
  assert.equal(segment.body, "seg");

  upstream.rawResponses["main:/Videos/main-alien/main.m3u8"] = {
    statusCode: 200,
    headers: { "content-type": "application/vnd.apple.mpegurl" },
    body: Buffer.from("#EXTM3U\nhls/main/init.mp4?MediaSourceId=source-main&PlaySessionId=upstream-play-session\n")
  };
  const legacyVariant = await app.inject({
    method: "GET",
    url: `/Videos/${itemId}/main.m3u8?MediaSourceId=${playback.json().MediaSources[0].Id}&PlaySessionId=${playback.json().PlaySessionId}`,
    headers: { "X-MediaBrowser-Token": token }
  });
  assert.equal(legacyVariant.statusCode, 200);
  assert.equal(legacyVariant.body, `#EXTM3U\n/Videos/${itemId}/hls/main/init.mp4?MediaSourceId=${playback.json().MediaSources[0].Id}&PlaySessionId=${playback.json().PlaySessionId}&ApiKey=${token}\n`);

  upstream.rawResponses["main:/Videos/main-alien/hls/main/init.mp4"] = {
    statusCode: 200,
    headers: { "content-type": "video/mp4", "content-length": "4" },
    body: Buffer.from("init")
  };
  const legacySegment = await app.inject({
    method: "GET",
    url: `/Videos/${itemId}/hls/main/init.mp4?MediaSourceId=${playback.json().MediaSources[0].Id}&PlaySessionId=${playback.json().PlaySessionId}`,
    headers: { "X-MediaBrowser-Token": token }
  });
  assert.equal(legacySegment.statusCode, 200);
  assert.equal(legacySegment.headers["content-type"], "video/mp4");
  assert.equal(legacySegment.body, "init");
  assert.equal(upstream.rawRequests.at(-1)?.path, "/Videos/main-alien/hls/main/init.mp4");

  upstream.rawResponses["main:/Videos/main-alien/Trickplay/320/tiles.m3u8"] = {
    statusCode: 200,
    headers: { "content-type": "application/vnd.apple.mpegurl" },
    body: Buffer.from("#EXTM3U\n0.jpg?MediaSourceId=source-main&ApiKey=upstream-token\n")
  };
  const trickplayPlaylist = await app.inject({
    method: "GET",
    url: `/Videos/${itemId}/Trickplay/320/tiles.m3u8?MediaSourceId=${playback.json().MediaSources[0].Id}`,
    headers: { "X-MediaBrowser-Token": token }
  });
  assert.equal(trickplayPlaylist.statusCode, 200);
  assert.equal(trickplayPlaylist.body, `#EXTM3U\n/Videos/${itemId}/Trickplay/320/0.jpg?MediaSourceId=${playback.json().MediaSources[0].Id}&ApiKey=${token}\n`);
  assert.equal(upstream.rawRequests.at(-1)?.init.query.MediaSourceId, "source-main");

  upstream.rawResponses["main:/Videos/main-alien/Trickplay/320/0.jpg"] = {
    statusCode: 200,
    headers: { "content-type": "image/jpeg", "content-length": "4" },
    body: Buffer.from("tile")
  };
  const trickplayTile = await app.inject({
    method: "GET",
    url: `/Videos/${itemId}/Trickplay/320/0.jpg?MediaSourceId=${playback.json().MediaSources[0].Id}`,
    headers: { "X-MediaBrowser-Token": token }
  });
  assert.equal(trickplayTile.statusCode, 200);
  assert.equal(trickplayTile.body, "tile");
  assert.equal(upstream.rawRequests.at(-1)?.init.query.MediaSourceId, "source-main");

  upstream.rawResponses["main:/Videos/main-alien/source-main/Subtitles/2/subtitles.m3u8"] = {
    statusCode: 200,
    headers: { "content-type": "application/vnd.apple.mpegurl" },
    body: Buffer.from("#EXTM3U\nstream.vtt?api_key=upstream-token\n")
  };
  const subtitlePlaylist = await app.inject({
    method: "GET",
    url: `/Videos/${itemId}/${playback.json().MediaSources[0].Id}/Subtitles/2/subtitles.m3u8?SegmentLength=30`,
    headers: { "X-MediaBrowser-Token": token }
  });
  assert.equal(subtitlePlaylist.statusCode, 200);
  assert.equal(subtitlePlaylist.body, `#EXTM3U\n/Videos/${itemId}/${playback.json().MediaSources[0].Id}/Subtitles/2/stream.vtt?ApiKey=${token}\n`);

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

	  const subtitlesWithQueryIds = await app.inject({
	    method: "GET",
	    url: `/Videos/${itemId}/${playback.json().MediaSources[0].Id}/Subtitles/2/Stream.vtt?mediaSourceId=${playback.json().MediaSources[0].Id}&itemId=${itemId}`,
	    headers: { "X-MediaBrowser-Token": token }
	  });
	  assert.equal(subtitlesWithQueryIds.statusCode, 200);
	  assert.equal(upstream.rawRequests.at(-1)?.init.query.MediaSourceId, "source-main");
	  assert.equal(upstream.rawRequests.at(-1)?.init.query.ItemId, "main-alien");
	  assert.equal(upstream.rawRequests.at(-1)?.init.query.mediaSourceId, undefined);
	  assert.equal(upstream.rawRequests.at(-1)?.init.query.itemId, undefined);

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

  upstream.rawResponses["main:/Videos/main-alien/source-main/Attachments/0"] = {
    statusCode: 200,
    headers: { "content-type": "application/octet-stream", "content-length": "3" },
    body: Buffer.from("att")
  };
  const attachment = await app.inject({
    method: "GET",
    url: `/Videos/${itemId}/${playback.json().MediaSources[0].Id}/Attachments/0`,
    headers: { "X-MediaBrowser-Token": token }
  });
  assert.equal(attachment.statusCode, 200);
  assert.equal(attachment.body, "att");

  const progress = await app.inject({
    method: "POST",
    url: "/Sessions/Playing/Progress",
    headers: { "X-MediaBrowser-Token": token },
    payload: {
      ItemId: itemId,
      MediaSourceId: playback.json().MediaSources[0].Id,
      PositionTicks: 600_000_000,
      PlaySessionId: playback.json().PlaySessionId
    }
  });
  assert.equal(progress.statusCode, 204);
  assert.equal(upstream.requests.at(-1)?.path, "/Sessions/Playing/Progress");
  assert.equal((upstream.requests.at(-1)?.init as any).body.ItemId, "main-alien");
  assert.equal((upstream.requests.at(-1)?.init as any).body.MediaSourceId, "source-main");
  assert.equal((upstream.requests.at(-1)?.init as any).body.PlaySessionId, "upstream-play-session");
  assert.equal(store.getUserData(login.json().User.Id, itemId).PlaybackPositionTicks, 600_000_000);

  const sessionOnlyProgress = await app.inject({
    method: "POST",
    url: "/Sessions/Playing/Progress",
    headers: { "X-MediaBrowser-Token": token },
    payload: {
      ItemId: itemId,
      PositionTicks: 700_000_000,
      PlaySessionId: playback.json().PlaySessionId
    }
  });
  assert.equal(sessionOnlyProgress.statusCode, 204);
  assert.equal(upstream.requests.at(-1)?.path, "/Sessions/Playing/Progress");
  assert.equal((upstream.requests.at(-1)?.init as any).body.ItemId, "main-alien");
  assert.equal((upstream.requests.at(-1)?.init as any).body.PlaySessionId, "upstream-play-session");
  assert.equal(store.getUserData(login.json().User.Id, itemId).PlaybackPositionTicks, 700_000_000);

  const ping = await app.inject({
    method: "POST",
    url: `/Sessions/Playing/Ping?playSessionId=${playback.json().PlaySessionId}`,
    headers: { "X-MediaBrowser-Token": token }
  });
  assert.equal(ping.statusCode, 204);
  assert.equal(upstream.requests.at(-1)?.path, "/Sessions/Playing/Ping");
  assert.equal((upstream.requests.at(-1)?.init as any).query.playSessionId, "upstream-play-session");

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
      MediaSources: [{
        Id: "media-a",
        ItemId: "movie-a",
        Path: "/media/movie-a.mkv",
        TranscodingUrl: "/videos/movie-a/master.m3u8?MediaSourceId=media-a&PlaySessionId=upstream-play-session&ApiKey=upstream-token"
      }],
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
    [bridgeItemId("movie:tmdb:200"), "Secondary Movie"],
    [bridgeItemId("movie:tmdb:100"), "Primary Movie"]
  ]);
  assert.equal((upstream.requests.find((request) => request.serverId === "primary" && request.path === "/Items/Latest")?.init as any).query.ParentId, "primary-movies");
  assert.equal((upstream.requests.find((request) => request.serverId === "secondary" && request.path === "/Items/Latest")?.init as any).query.ParentId, "secondary-movies");

  const resume = await app.inject({
    method: "GET",
    url: `/UserItems/Resume?userId=${login.json().User.Id}&limit=40&mediaTypes=Video&recursive=true`,
    headers: { "X-MediaBrowser-Token": token }
  });
  assert.equal(resume.statusCode, 200);
  assert.deepEqual(resume.json().Items.map((item: any) => item.Id), [bridgeItemId("movie:tmdb:300"), bridgeItemId("movie:tmdb:400")]);
  assert.equal(resume.json().TotalRecordCount, 4);
  assert.equal((upstream.requests.find((request) => request.serverId === "primary" && request.path === "/UserItems/Resume")?.init as any).query.UserId, "primary-user");
  assert.equal((upstream.requests.find((request) => request.serverId === "primary" && request.path === "/UserItems/Resume")?.init as any).query.ParentId, "primary-movies");

  const nextUp = await app.inject({
    method: "GET",
    url: `/Shows/NextUp?userId=${login.json().User.Id}&startIndex=0&limit=20`,
    headers: { "X-MediaBrowser-Token": token }
  });
  assert.equal(nextUp.statusCode, 200);
  assert.deepEqual(nextUp.json().Items.map((item: any) => item.Id), [bridgeItemId("episode:tvdb:500"), bridgeItemId("episode:tvdb:600")]);
  assert.equal((upstream.requests.find((request) => request.serverId === "primary" && request.path === "/Shows/NextUp")?.init as any).query.ParentId, "primary-tv");

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
  assert.notEqual(playback.json().MediaSources[0].Id, "media-a");
  assert.notEqual(playback.json().PlaySessionId, "upstream-play-session");
  assert.equal(store.findMediaSourceMapping(playback.json().MediaSources[0].Id)?.upstreamMediaSourceId, "media-a");
  const liveTranscodingUrl = new URL(playback.json().MediaSources[0].TranscodingUrl, "http://bridge.test");
  assert.equal(liveTranscodingUrl.pathname, "/Videos/movie-a/master.m3u8");
  assert.equal(liveTranscodingUrl.searchParams.get("MediaSourceId"), playback.json().MediaSources[0].Id);
  assert.equal(liveTranscodingUrl.searchParams.get("PlaySessionId"), playback.json().PlaySessionId);
  assert.notEqual(liveTranscodingUrl.searchParams.get("ApiKey"), "upstream-token");

  const selectedLivePlayback = await app.inject({
    method: "POST",
    url: `/Items/movie-a/PlaybackInfo?MediaSourceId=${playback.json().MediaSources[0].Id}`,
    headers: { "X-MediaBrowser-Token": token },
    payload: { MediaSourceId: playback.json().MediaSources[0].Id }
  });
  assert.equal(selectedLivePlayback.statusCode, 200);
  assert.equal(upstream.requests.at(-1)?.path, "/Items/movie-a/PlaybackInfo");
  assert.equal((upstream.requests.at(-1)?.init as any).query.MediaSourceId, "media-a");
  assert.equal((upstream.requests.at(-1)?.init as any).body.MediaSourceId, "media-a");

  const progress = await app.inject({
    method: "POST",
    url: "/Sessions/Playing/Progress",
    headers: { "X-MediaBrowser-Token": token },
    payload: { ItemId: "movie-a", MediaSourceId: playback.json().MediaSources[0].Id, PositionTicks: 5000, PlaySessionId: playback.json().PlaySessionId }
  });
  assert.equal(progress.statusCode, 204);
  assert.equal(upstream.requests.at(-1)?.path, "/Sessions/Playing/Progress");
  assert.equal((upstream.requests.at(-1)?.init as any).body.ItemId, "movie-a");
  assert.equal((upstream.requests.at(-1)?.init as any).body.MediaSourceId, "media-a");
  assert.equal((upstream.requests.at(-1)?.init as any).body.PlaySessionId, "upstream-play-session");

  await app.close();
  store.close();
});

test("returns bridge item ids from live latest media so TV seasons resolve", async () => {
  const passwordHash = await hash("secret");
  const config: BridgeConfig = {
    server: { bind: "127.0.0.1", port: 8096, publicUrl: "http://bridge.test", name: "Bridge" },
    auth: { users: [{ name: "alice", passwordHash }] },
    upstreams: [{ id: "main", name: "Main", url: "https://main.example.com", token: "token" }],
    libraries: [{ id: "shows", name: "Shows", collectionType: "tvshows", sources: [{ server: "main", libraryId: "shows-lib" }] }]
  };
  const upstream = new FakeUpstream({
    "main:/Users": [{ Id: "main-user", Name: "alice" }],
    "main:/Items/Latest": [
      { Id: "series-a", Type: "Series", Name: "Latest Series", ProviderIds: { Tvdb: "100" }, DateCreated: "2026-01-01T00:00:00.000Z" }
    ],
    "main:/Shows/series-a/Seasons": {
      Items: [{ Id: "season-a", Type: "Season", Name: "Season 1", SeriesId: "series-a", IndexNumber: 1 }],
      TotalRecordCount: 1,
      StartIndex: 0
    }
  });
  const store = new Store(":memory:");
  const app = buildApp({ config, store, upstream });
  const login = await app.inject({ method: "POST", url: "/Users/AuthenticateByName", payload: { Username: "alice", Pw: "secret" } });

  const latest = await app.inject({
    method: "GET",
    url: `/Items/Latest?userId=${login.json().User.Id}&parentId=${bridgeLibraryId("shows")}&limit=20`,
    headers: { "X-MediaBrowser-Token": login.json().AccessToken }
  });
  assert.equal(latest.statusCode, 200);
  const seriesId = latest.json()[0].Id;
  assert.equal(seriesId, bridgeItemId("series:tvdb:100"));

  const seasons = await app.inject({
    method: "GET",
    url: `/Shows/${seriesId}/Seasons`,
    headers: { "X-MediaBrowser-Token": login.json().AccessToken }
  });
  assert.equal(seasons.statusCode, 200);
  assert.deepEqual(seasons.json().Items.map((item: any) => item.Name), ["Season 1"]);
  assert.deepEqual(upstream.requests.map((request) => `${request.serverId}:${request.path}`), [
    "main:/Users",
    "main:/Items/Latest",
    "main:/Shows/series-a/Seasons"
  ]);

  await app.close();
  store.close();
});

test("narrows live latest TV libraries to episodes like Jellyfin user views", async () => {
  const passwordHash = await hash("secret");
  const config: BridgeConfig = {
    server: { bind: "127.0.0.1", port: 8096, publicUrl: "http://bridge.test", name: "Bridge" },
    auth: { users: [{ name: "alice", passwordHash }] },
    upstreams: [{ id: "main", name: "Main", url: "https://main.example.com", token: "token" }],
    libraries: [{ id: "shows", name: "Shows", collectionType: "tvshows", sources: [{ server: "main", libraryId: "shows-lib" }] }]
  };
  const upstream = {
    requests: [] as Array<{ serverId: string; path: string; init: any }>,
    async json<T>(serverId: string, path: string, init: any): Promise<T> {
      this.requests.push({ serverId, path, init });
      if (path === "/Users") return [{ Id: "main-user", Name: "alice" }] as T;
      if (path === "/Items/Latest") {
        return ((init.query.IncludeItemTypes ?? init.query.includeItemTypes) === "Episode"
          ? [{
              Id: "episode-a",
              Type: "Episode",
              Name: "Latest Episode",
              ParentId: "season-a",
              SeriesId: "series-a",
              SeasonId: "season-a",
              TopParentId: "shows-lib",
              ParentIndexNumber: 1,
              IndexNumber: 1,
              DateCreated: "2026-01-02T00:00:00.000Z"
            }]
          : [{ Id: "season-a", Type: "Season", Name: "Season 1", SeriesId: "series-a", IndexNumber: 1, DateCreated: "2026-01-03T00:00:00.000Z" }]) as T;
      }
      if (path === "/Shows/series-a/Seasons") {
        return { Items: [{ Id: "season-a", Type: "Season", Name: "Season 1", SeriesId: "series-a", IndexNumber: 1 }], TotalRecordCount: 1, StartIndex: 0 } as T;
      }
      throw new Error(`Unexpected upstream request ${serverId}:${path}`);
    }
  };
  const store = new Store(":memory:");
  const seriesId = bridgeItemId("series:tvdb:100");
  const seasonId = bridgeItemId("season:series:series-a:season:1");
  store.upsertIndexedItem({
    serverId: "main",
    itemId: "series-a",
    libraryId: "shows-lib",
    itemType: "Series",
    logicalKey: "series:tvdb:100",
    json: { Id: "series-a", Type: "Series", Name: "Example Series", ProviderIds: { Tvdb: "100" } }
  });
  store.upsertIndexedItem({
    serverId: "main",
    itemId: "season-a",
    libraryId: "shows-lib",
    itemType: "Season",
    logicalKey: "season:series:series-a:season:1",
    json: { Id: "season-a", Type: "Season", Name: "Season 1", SeriesId: "series-a", IndexNumber: 1 }
  });
  const app = buildApp({ config, store, upstream });
  const login = await app.inject({ method: "POST", url: "/Users/AuthenticateByName", payload: { Username: "alice", Pw: "secret" } });

  const latest = await app.inject({
    method: "GET",
    url: `/Items/Latest?userId=${login.json().User.Id}&parentId=${bridgeLibraryId("shows")}&startIndex=0&limit=20`,
    headers: { "X-MediaBrowser-Token": login.json().AccessToken }
  });

  assert.equal(latest.statusCode, 200);
  assert.deepEqual(latest.json().map((item: any) => [item.Type, item.Name]), [["Episode", "Latest Episode"]]);
  assert.equal(latest.json()[0].ParentId, seasonId);
  assert.equal(latest.json()[0].SeriesId, seriesId);
  assert.equal(latest.json()[0].SeasonId, seasonId);
  assert.equal(latest.json()[0].TopParentId, bridgeLibraryId("shows"));
  assert.equal(upstream.requests.find((request) => request.path === "/Items/Latest")?.init.query.IncludeItemTypes, "Episode");

  const seasons = await app.inject({
    method: "GET",
    url: `/Shows/${latest.json()[0].SeriesId}/Seasons`,
    headers: { "X-MediaBrowser-Token": login.json().AccessToken }
  });
  assert.equal(seasons.statusCode, 200);
  assert.deepEqual(seasons.json().Items.map((item: any) => item.Name), ["Season 1"]);

  await app.close();
  store.close();
});

test("excludes live latest TV specials before applying the shelf limit", async () => {
  const passwordHash = await hash("secret");
  const config: BridgeConfig = {
    server: { bind: "127.0.0.1", port: 8096, publicUrl: "http://bridge.test", name: "Bridge" },
    auth: { users: [{ name: "alice", passwordHash }] },
    upstreams: [{ id: "main", name: "Main", url: "https://main.example.com", token: "token" }],
    libraries: [{ id: "shows", name: "Shows", collectionType: "tvshows", sources: [{ server: "main", libraryId: "shows-lib" }] }]
  };
  const upstream = new FakeUpstream({
    "main:/Users": [{ Id: "main-user", Name: "alice" }],
    "main:/Items/Latest": (_serverId: string, _path: string, init: any) =>
      (init.query.IncludeItemTypes === "Episode"
        ? [
            {
              Id: "special-a",
              Type: "Episode",
              Name: "Special Episode",
              ParentId: "season-specials",
              SeriesId: "series-a",
              SeasonId: "season-specials",
              ParentIndexNumber: 0,
              IndexNumber: 1,
              DateCreated: "2026-01-03T00:00:00.000Z"
            },
            {
              Id: "episode-a",
              Type: "Episode",
              Name: "Regular Episode",
              ParentId: "season-a",
              SeriesId: "series-a",
              SeasonId: "season-a",
              ParentIndexNumber: 1,
              IndexNumber: 1,
              DateCreated: "2026-01-02T00:00:00.000Z"
            }
          ]
        : []) as unknown[],
    "main:/Items/series-a": { Id: "series-a", Type: "Series", Name: "Example Series", ProviderIds: { Tvdb: "100" } },
    "main:/Items/season-specials": { Id: "season-specials", Type: "Season", Name: "Specials", SeriesId: "series-a", IndexNumber: 0 },
    "main:/Items/season-a": { Id: "season-a", Type: "Season", Name: "Season 1", SeriesId: "series-a", IndexNumber: 1 }
  });
  const store = new Store(":memory:");
  const app = buildApp({ config, store, upstream });
  const login = await app.inject({ method: "POST", url: "/Users/AuthenticateByName", payload: { Username: "alice", Pw: "secret" } });

  const latest = await app.inject({
    method: "GET",
    url: `/Items/Latest?userId=${login.json().User.Id}&ParentId=${bridgeLibraryId("shows")}&Limit=1`,
    headers: { "X-MediaBrowser-Token": login.json().AccessToken }
  });

  assert.equal(latest.statusCode, 200);
  assert.deepEqual(latest.json().map((item: any) => item.Name), ["Regular Episode"]);
  assert.equal((upstream.requests.find((request) => request.path === "/Items/Latest")?.init as any).query.IncludeItemTypes, "Episode");

  await app.close();
  store.close();
});

test("groups multiple live latest episodes by series like Jellyfin", async () => {
  const passwordHash = await hash("secret");
  const config: BridgeConfig = {
    server: { bind: "127.0.0.1", port: 8096, publicUrl: "http://bridge.test", name: "Bridge" },
    auth: { users: [{ name: "alice", passwordHash }] },
    upstreams: [
      { id: "primary", name: "Primary", url: "https://primary.example.com", token: "token" },
      { id: "secondary", name: "Secondary", url: "https://secondary.example.com", token: "token" }
    ],
    libraries: [{
      id: "shows",
      name: "Shows",
      collectionType: "tvshows",
      sources: [{ server: "primary", libraryId: "primary-shows" }, { server: "secondary", libraryId: "secondary-shows" }]
    }]
  };
  const upstream = new FakeUpstream({
    "primary:/Users": [{ Id: "primary-user", Name: "alice" }],
    "secondary:/Users": [{ Id: "secondary-user", Name: "alice" }],
    "primary:/Items/Latest": [{
      Id: "primary-episode-1",
      Type: "Episode",
      Name: "Pilot",
      SeriesId: "primary-series",
      ParentIndexNumber: 1,
      IndexNumber: 1,
      ProviderIds: { Tvdb: "episode-1" },
      DateCreated: "2026-02-02T00:00:00.000Z"
    }],
    "secondary:/Items/Latest": [{
      Id: "secondary-episode-2",
      Type: "Episode",
      Name: "Second",
      SeriesId: "secondary-series",
      ParentIndexNumber: 1,
      IndexNumber: 2,
      ProviderIds: { Tvdb: "episode-2" },
      DateCreated: "2026-02-01T00:00:00.000Z"
    }],
    "primary:/Items/primary-series": { Id: "primary-series", Type: "Series", Name: "Example Series", ProviderIds: { Tvdb: "100" } },
    "secondary:/Items/secondary-series": { Id: "secondary-series", Type: "Series", Name: "Example Series", ProviderIds: { Tvdb: "100" } }
  });
  const store = new Store(":memory:");
  const app = buildApp({ config, store, upstream });
  const login = await app.inject({ method: "POST", url: "/Users/AuthenticateByName", payload: { Username: "alice", Pw: "secret" } });

  const latest = await app.inject({
    method: "GET",
    url: `/Items/Latest?userId=${login.json().User.Id}&ParentId=${bridgeLibraryId("shows")}&Limit=20`,
    headers: { "X-MediaBrowser-Token": login.json().AccessToken }
  });

  assert.equal(latest.statusCode, 200);
  assert.deepEqual(latest.json().map((item: any) => [item.Type, item.Name, item.ChildCount]), [
    ["Series", "Example Series", 2]
  ]);

  await app.close();
  store.close();
});

test("indexes live latest related TV items before returning bridge ids", async () => {
  const passwordHash = await hash("secret");
  const config: BridgeConfig = {
    server: { bind: "127.0.0.1", port: 8096, publicUrl: "http://bridge.test", name: "Bridge" },
    auth: { users: [{ name: "alice", passwordHash }] },
    upstreams: [{ id: "main", name: "Main", url: "https://main.example.com", token: "token" }],
    libraries: [{ id: "shows", name: "Shows", collectionType: "tvshows", sources: [{ server: "main", libraryId: "shows-lib" }] }]
  };
  const upstream = new FakeUpstream({
    "main:/Users": [{ Id: "main-user", Name: "alice" }],
    "main:/Items/Latest": [
      {
        Id: "episode-a",
        Type: "Episode",
        Name: "Latest Episode",
        ParentId: "season-a",
        SeriesId: "series-a",
        SeasonId: "season-a",
        ParentIndexNumber: 1,
        IndexNumber: 1,
        DateCreated: "2026-01-02T00:00:00.000Z"
      }
    ],
    "main:/Items/series-a": { Id: "series-a", Type: "Series", Name: "Example Series", ProviderIds: { Tvdb: "100" } },
    "main:/Items/season-a": { Id: "season-a", Type: "Season", Name: "Season 1", SeriesId: "series-a", IndexNumber: 1 },
    "main:/Shows/series-a/Seasons": {
      Items: [{ Id: "season-a", Type: "Season", Name: "Season 1", SeriesId: "series-a", IndexNumber: 1 }],
      TotalRecordCount: 1,
      StartIndex: 0
    }
  });
  const store = new Store(":memory:");
  const app = buildApp({ config, store, upstream });
  const login = await app.inject({ method: "POST", url: "/Users/AuthenticateByName", payload: { Username: "alice", Pw: "secret" } });

  const latest = await app.inject({
    method: "GET",
    url: `/Items/Latest?userId=${login.json().User.Id}&ParentId=${bridgeLibraryId("shows")}&Limit=20`,
    headers: { "X-MediaBrowser-Token": login.json().AccessToken }
  });

  const seriesId = bridgeItemId("series:tvdb:100");
  const seasonId = bridgeItemId("season:series:series-a:season:1");
  assert.equal(latest.statusCode, 200);
  assert.equal(latest.json()[0].SeriesId, seriesId);
  assert.equal(latest.json()[0].SeasonId, seasonId);
  assert.equal(latest.json()[0].ParentId, seasonId);

  const seasons = await app.inject({
    method: "GET",
    url: `/Shows/${latest.json()[0].SeriesId}/Seasons`,
    headers: { "X-MediaBrowser-Token": login.json().AccessToken }
  });
  assert.equal(seasons.statusCode, 200);
  assert.deepEqual(seasons.json().Items.map((item: any) => item.Name), ["Season 1"]);

  await app.close();
  store.close();
});

test("cached show season and episode routes use targeted indexed children", async () => {
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
    json: { Id: "series-a", Type: "Series", Name: "Example Series", ProviderIds: { Tvdb: "100" } }
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
    json: { Id: "episode-a", Type: "Episode", Name: "Episode 1", SeriesId: "series-a", SeasonId: "season-a", ParentIndexNumber: 1, IndexNumber: 1 }
  });
  for (let index = 0; index < 25; index += 1) {
    store.upsertIndexedItem({
      serverId: "main",
      itemId: `unrelated-${index}`,
      libraryId: "shows-lib",
      itemType: "Episode",
      logicalKey: `source:main:unrelated-${index}`,
      json: { Id: `unrelated-${index}`, Type: "Episode", Name: `Unrelated ${index}`, SeriesId: "other-series", ParentIndexNumber: 1, IndexNumber: index + 1 }
    });
  }
  store.listIndexedItems = () => {
    throw new Error("full catalog scan");
  };
  const app = buildApp({ config, store });
  const login = await app.inject({ method: "POST", url: "/Users/AuthenticateByName", payload: { Username: "alice", Pw: "secret" } });
  const seriesId = bridgeItemId("series:tvdb:100");

  const seasons = await app.inject({
    method: "GET",
    url: `/Shows/${seriesId}/Seasons?userId=${login.json().User.Id}&limit=200`,
    headers: { "X-MediaBrowser-Token": login.json().AccessToken }
  });
  assert.equal(seasons.statusCode, 200);
  assert.deepEqual(seasons.json().Items.map((item: any) => item.Name), ["Season 1"]);

  const episodes = await app.inject({
    method: "GET",
    url: `/Shows/${seriesId}/Episodes?userId=${login.json().User.Id}&SeasonId=${seasons.json().Items[0].Id}&limit=200`,
    headers: { "X-MediaBrowser-Token": login.json().AccessToken }
  });
  assert.equal(episodes.statusCode, 200);
  assert.deepEqual(episodes.json().Items.map((item: any) => item.Name), ["Episode 1"]);

  await app.close();
  store.close();
});

test("cached show episode routes keep regular seasons in episode order", async () => {
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
    json: { Id: "series-a", Type: "Series", Name: "Example Series", ProviderIds: { Tvdb: "100" } }
  });
  store.upsertIndexedItem({
    serverId: "main",
    itemId: "season-a",
    libraryId: "shows-lib",
    itemType: "Season",
    logicalKey: "season:series:series-a:season:1",
    json: { Id: "season-a", Type: "Season", Name: "Season 1", SeriesId: "series-a", IndexNumber: 1 }
  });
  for (const episode of [
    { id: "episode-a", name: "Zeta Pilot", index: 1 },
    { id: "episode-b", name: "Alpha Follow-up", index: 2 },
    { id: "episode-c", name: "Middle Finale", index: 10 }
  ]) {
    store.upsertIndexedItem({
      serverId: "main",
      itemId: episode.id,
      libraryId: "shows-lib",
      itemType: "Episode",
      logicalKey: `episode:series:series-a:season:1:episode:${episode.index}`,
      json: {
        Id: episode.id,
        Type: "Episode",
        Name: episode.name,
        SeriesId: "series-a",
        SeasonId: "season-a",
        ParentIndexNumber: 1,
        IndexNumber: episode.index
      }
    });
  }
  const app = buildApp({ config, store });
  const login = await app.inject({ method: "POST", url: "/Users/AuthenticateByName", payload: { Username: "alice", Pw: "secret" } });
  const seriesId = bridgeItemId("series:tvdb:100");
  const seasonId = bridgeItemId("season:series:series-a:season:1");

  const episodes = await app.inject({
    method: "GET",
    url: `/Shows/${seriesId}/Episodes?userId=${login.json().User.Id}&SeasonId=${seasonId}&limit=200`,
    headers: { "X-MediaBrowser-Token": login.json().AccessToken }
  });

  assert.equal(episodes.statusCode, 200);
  assert.deepEqual(episodes.json().Items.map((item: any) => item.Name), ["Zeta Pilot", "Alpha Follow-up", "Middle Finale"]);

  const childItems = await app.inject({
    method: "GET",
    url: `/Items?ParentId=${seasonId}&StartIndex=0&Limit=50`,
    headers: { "X-MediaBrowser-Token": login.json().AccessToken }
  });

  assert.equal(childItems.statusCode, 200);
  assert.deepEqual(childItems.json().Items.map((item: any) => item.Name), ["Zeta Pilot", "Alpha Follow-up", "Middle Finale"]);

  await app.close();
  store.close();
});

test("cached show child routes preserve priority source selection after type narrowing", async () => {
  const passwordHash = await hash("secret");
  const config: BridgeConfig = {
    server: { bind: "127.0.0.1", port: 8096, publicUrl: "http://bridge.test", name: "Bridge" },
    auth: { users: [{ name: "alice", passwordHash }] },
    upstreams: [
      { id: "main", name: "Main", url: "https://main.example.com", token: "token" },
      { id: "remote", name: "Remote", url: "https://remote.example.com", token: "token" }
    ],
    libraries: [{ id: "shows", name: "Shows", collectionType: "tvshows", sources: [{ server: "main", libraryId: "shows-a" }, { server: "remote", libraryId: "shows-b" }] }]
  };
  const store = new Store(":memory:");
  store.upsertIndexedItem({
    serverId: "main",
    itemId: "main-series",
    libraryId: "shows-a",
    itemType: "Series",
    logicalKey: "series:tvdb:100",
    json: { Id: "main-series", Type: "Series", Name: "Priority Series", ProviderIds: { Tvdb: "100" } }
  });
  store.upsertIndexedItem({
    serverId: "remote",
    itemId: "remote-series",
    libraryId: "shows-b",
    itemType: "Series",
    logicalKey: "series:tvdb:100",
    json: { Id: "remote-series", Type: "Series", Name: "Remote Series", ProviderIds: { Tvdb: "100" } }
  });
  store.upsertIndexedItem({
    serverId: "main",
    itemId: "main-season",
    libraryId: "shows-a",
    itemType: "Folder",
    logicalKey: "season:mismatched:1",
    json: { Id: "main-season", Type: "Folder", Name: "Priority Folder", SeriesId: "main-series", IndexNumber: 1 }
  });
  store.upsertIndexedItem({
    serverId: "remote",
    itemId: "remote-season",
    libraryId: "shows-b",
    itemType: "Season",
    logicalKey: "season:mismatched:1",
    json: { Id: "remote-season", Type: "Season", Name: "Lower Priority Season", SeriesId: "remote-series", IndexNumber: 1 }
  });
  store.upsertIndexedItem({
    serverId: "main",
    itemId: "main-episode",
    libraryId: "shows-a",
    itemType: "Video",
    logicalKey: "episode:mismatched:1",
    json: { Id: "main-episode", Type: "Video", Name: "Priority Video", SeriesId: "main-series", ParentIndexNumber: 1, IndexNumber: 1 }
  });
  store.upsertIndexedItem({
    serverId: "remote",
    itemId: "remote-episode",
    libraryId: "shows-b",
    itemType: "Episode",
    logicalKey: "episode:mismatched:1",
    json: { Id: "remote-episode", Type: "Episode", Name: "Lower Priority Episode", SeriesId: "remote-series", ParentIndexNumber: 1, IndexNumber: 1 }
  });
  const upstream = new FakeUpstream({
    "main:/Shows/main-series/Seasons": { Items: [] },
    "remote:/Shows/remote-series/Seasons": { Items: [] },
    "main:/Shows/main-series/Episodes": { Items: [] },
    "remote:/Shows/remote-series/Episodes": { Items: [] }
  });
  const app = buildApp({ config, store, upstream });
  const login = await app.inject({ method: "POST", url: "/Users/AuthenticateByName", payload: { Username: "alice", Pw: "secret" } });
  const seriesId = bridgeItemId("series:tvdb:100");

  const seasons = await app.inject({
    method: "GET",
    url: `/Shows/${seriesId}/Seasons`,
    headers: { "X-MediaBrowser-Token": login.json().AccessToken }
  });
  assert.equal(seasons.statusCode, 200);
  assert.deepEqual(seasons.json(), { Items: [], TotalRecordCount: 0, StartIndex: 0 });

  const episodes = await app.inject({
    method: "GET",
    url: `/Shows/${seriesId}/Episodes`,
    headers: { "X-MediaBrowser-Token": login.json().AccessToken }
  });
  assert.equal(episodes.statusCode, 200);
  assert.deepEqual(episodes.json(), { Items: [], TotalRecordCount: 0, StartIndex: 0 });

  await app.close();
  store.close();
});

test("indexes parent-scoped live next-up items before returning bridge ids", async () => {
  const passwordHash = await hash("secret");
  const config: BridgeConfig = {
    server: { bind: "127.0.0.1", port: 8096, publicUrl: "http://bridge.test", name: "Bridge" },
    auth: { users: [{ name: "alice", passwordHash }] },
    upstreams: [{ id: "main", name: "Main", url: "https://main.example.com", token: "token" }],
    libraries: [{ id: "shows", name: "Shows", collectionType: "tvshows", sources: [{ server: "main", libraryId: "shows-lib" }] }]
  };
  const upstream = new FakeUpstream({
    "main:/Users": [{ Id: "main-user", Name: "alice" }],
    "main:/Shows/NextUp": (_serverId: string, _path: string, init: any) => {
      if (init.query.SeriesId && init.query.SeriesId !== "series-a") {
        return { Items: [], TotalRecordCount: 0, StartIndex: 0 };
      }
      return {
        Items: [{
          Id: "episode-a",
          Type: "Episode",
          Name: "Next Episode",
          ParentId: "season-a",
          SeriesId: "series-a",
          SeasonId: "season-a",
          ParentIndexNumber: 1,
          IndexNumber: 1,
          DateCreated: "2026-01-02T00:00:00.000Z"
        }],
        TotalRecordCount: 1,
        StartIndex: 0
      };
    },
    "main:/Items/series-a": { Id: "series-a", Type: "Series", Name: "Example Series", ProviderIds: { Tvdb: "100" } },
    "main:/Items/season-a": { Id: "season-a", Type: "Season", Name: "Season 1", SeriesId: "series-a", IndexNumber: 1 },
    "main:/Shows/series-a/Seasons": {
      Items: [{ Id: "season-a", Type: "Season", Name: "Season 1", SeriesId: "series-a", IndexNumber: 1 }],
      TotalRecordCount: 1,
      StartIndex: 0
    }
  });
  const store = new Store(":memory:");
  const app = buildApp({ config, store, upstream });
  const login = await app.inject({ method: "POST", url: "/Users/AuthenticateByName", payload: { Username: "alice", Pw: "secret" } });

  const nextUp = await app.inject({
    method: "GET",
    url: `/Shows/NextUp?userId=${login.json().User.Id}&ParentId=${bridgeLibraryId("shows")}&limit=20`,
    headers: { "X-MediaBrowser-Token": login.json().AccessToken }
  });

  const seriesId = bridgeItemId("series:tvdb:100");
  assert.equal(nextUp.statusCode, 200);
  assert.equal(nextUp.json().Items[0].Id, bridgeItemId("episode:series:series-a:season:1:episode:1"));
  assert.equal(nextUp.json().Items[0].SeriesId, seriesId);

  const filteredNextUp = await app.inject({
    method: "GET",
    url: `/Shows/NextUp?userId=${login.json().User.Id}&ParentId=${bridgeLibraryId("shows")}&SeriesId=${seriesId}&limit=20`,
    headers: { "X-MediaBrowser-Token": login.json().AccessToken }
  });
  assert.equal(filteredNextUp.statusCode, 200);
  assert.equal(filteredNextUp.json().Items[0].Name, "Next Episode");
  const filteredNextUpRequest = upstream.requests
    .filter((request) => request.path === "/Shows/NextUp")
    .at(-1)?.init as any;
  assert.equal(filteredNextUpRequest.query.SeriesId, "series-a");

  const seasons = await app.inject({
    method: "GET",
    url: `/Shows/${nextUp.json().Items[0].SeriesId}/Seasons`,
    headers: { "X-MediaBrowser-Token": login.json().AccessToken }
  });
  assert.equal(seasons.statusCode, 200);
  assert.deepEqual(seasons.json().Items.map((item: any) => item.Name), ["Season 1"]);

  await app.close();
  store.close();
});

test("live next-up preserves existing indexed episode identity when upstream omits provider fields", async () => {
  const passwordHash = await hash("secret");
  const config: BridgeConfig = {
    server: { bind: "127.0.0.1", port: 8096, publicUrl: "http://bridge.test", name: "Bridge" },
    auth: { users: [{ name: "alice", passwordHash }] },
    upstreams: [{ id: "main", name: "Main", url: "https://main.example.com", token: "token" }],
    libraries: [{ id: "shows", name: "Shows", collectionType: "tvshows", sources: [{ server: "main", libraryId: "shows-lib" }] }]
  };
  const upstream = new FakeUpstream({
    "main:/Users": [{ Id: "main-user", Name: "alice" }],
    "main:/Shows/NextUp": {
      Items: [{
        Id: "episode-a",
        Type: "Episode",
        Name: "Pilot",
        SeriesId: "series-a",
        SeasonId: "season-a",
        ParentIndexNumber: 1,
        IndexNumber: 1
      }],
      TotalRecordCount: 1,
      StartIndex: 0
    }
  });
  const store = new Store(":memory:");
  store.upsertIndexedItem({
    serverId: "main",
    itemId: "series-a",
    libraryId: "shows-lib",
    itemType: "Series",
    logicalKey: "series:tvdb:100",
    json: { Id: "series-a", Type: "Series", Name: "Example Series", ProviderIds: { Tvdb: "100" } }
  });
  store.upsertIndexedItem({
    serverId: "main",
    itemId: "season-a",
    libraryId: "shows-lib",
    itemType: "Season",
    logicalKey: "season:tvdb:200",
    json: { Id: "season-a", Type: "Season", Name: "Season 1", SeriesId: "series-a", ProviderIds: { Tvdb: "200" }, IndexNumber: 1 }
  });
  store.upsertIndexedItem({
    serverId: "main",
    itemId: "episode-a",
    libraryId: "shows-lib",
    itemType: "Episode",
    logicalKey: "episode:tvdb:300",
    json: {
      Id: "episode-a",
      Type: "Episode",
      Name: "Pilot",
      SortName: "001 - 0001 - Pilot",
      Path: "/media/show/episode-a.mkv",
      ParentId: "season-a",
      SeriesId: "series-a",
      SeasonId: "season-a",
      ParentIndexNumber: 1,
      IndexNumber: 1,
      ProviderIds: { Tvdb: "300" },
      MediaSources: [{ Id: "episode-a-source", Path: "/media/show/episode-a.mkv" }]
    }
  });
  const app = buildApp({ config, store, upstream });
  const login = await app.inject({ method: "POST", url: "/Users/AuthenticateByName", payload: { Username: "alice", Pw: "secret" } });
  const stableEpisodeId = bridgeItemId("episode:tvdb:300");

  const nextUp = await app.inject({
    method: "GET",
    url: `/Shows/NextUp?userId=${login.json().User.Id}&ParentId=${bridgeLibraryId("shows")}&SeriesId=${bridgeItemId("series:tvdb:100")}&Limit=1`,
    headers: { "X-MediaBrowser-Token": login.json().AccessToken }
  });

  assert.equal(nextUp.statusCode, 200);
  assert.equal(nextUp.json().Items[0].Id, stableEpisodeId);
  assert.equal(nextUp.json().Items[0].ProviderIds.Tvdb, "300");
  assert.equal(nextUp.json().Items[0].Path, "/media/show/episode-a.mkv");
  assert.equal(store.findIndexedItemsBySourceId("episode-a")[0]?.logicalKey, "episode:tvdb:300");

  await app.close();
  store.close();
});

test("live next-up scopes series queries to the indexed series library", async () => {
  const passwordHash = await hash("secret");
  const config: BridgeConfig = {
    server: { bind: "127.0.0.1", port: 8096, publicUrl: "http://bridge.test", name: "Bridge" },
    auth: { users: [{ name: "alice", passwordHash }] },
    upstreams: [{ id: "main", name: "Main", url: "https://main.example.com", token: "token" }],
    libraries: [
      { id: "shows", name: "Shows", collectionType: "tvshows", sources: [{ server: "main", libraryId: "shows-lib" }] },
      { id: "anime", name: "Anime", collectionType: "tvshows", sources: [{ server: "main", libraryId: "anime-lib" }] }
    ]
  };
  const upstream = new FakeUpstream({
    "main:/Users": [{ Id: "main-user", Name: "alice" }],
    "main:/Shows/NextUp": {
      Items: [{
        Id: "episode-a",
        Type: "Episode",
        Name: "Pilot",
        SeriesId: "series-a",
        SeasonId: "season-a",
        ParentIndexNumber: 1,
        IndexNumber: 1
      }],
      TotalRecordCount: 1,
      StartIndex: 0
    }
  });
  const store = new Store(":memory:");
  store.upsertIndexedItem({
    serverId: "main",
    itemId: "series-a",
    libraryId: "anime-lib",
    itemType: "Series",
    logicalKey: "series:tvdb:100",
    json: { Id: "series-a", Type: "Series", Name: "Example Series", ProviderIds: { Tvdb: "100" } }
  });
  store.upsertIndexedItem({
    serverId: "main",
    itemId: "season-a",
    libraryId: "anime-lib",
    itemType: "Season",
    logicalKey: "season:series:series-a:season:1",
    json: { Id: "season-a", Type: "Season", Name: "Season 1", SeriesId: "series-a", IndexNumber: 1 }
  });
  store.upsertIndexedItem({
    serverId: "main",
    itemId: "episode-a",
    libraryId: "anime-lib",
    itemType: "Episode",
    logicalKey: "episode:series:series-a:season:1:episode:1",
    json: {
      Id: "episode-a",
      Type: "Episode",
      Name: "Pilot",
      ParentId: "season-a",
      SeriesId: "series-a",
      SeasonId: "season-a",
      ParentIndexNumber: 1,
      IndexNumber: 1
    }
  });
  const app = buildApp({ config, store, upstream });
  const login = await app.inject({ method: "POST", url: "/Users/AuthenticateByName", payload: { Username: "alice", Pw: "secret" } });
  const seriesId = bridgeItemId("series:tvdb:100");
  const seasonId = bridgeItemId("season:series:series-a:season:1");

  const nextUp = await app.inject({
    method: "GET",
    url: `/Shows/NextUp?userId=${login.json().User.Id}&SeriesId=${seriesId}&Limit=1`,
    headers: { "X-MediaBrowser-Token": login.json().AccessToken }
  });
  const episodes = await app.inject({
    method: "GET",
    url: `/Shows/${seriesId}/Episodes?userId=${login.json().User.Id}&SeasonId=${seasonId}&Limit=50`,
    headers: { "X-MediaBrowser-Token": login.json().AccessToken }
  });

  assert.equal(nextUp.statusCode, 200);
  assert.deepEqual(upstream.requests
    .filter((request) => request.path === "/Shows/NextUp")
    .map((request) => (request.init as any).query.ParentId), ["anime-lib"]);
  assert.equal(store.findIndexedItemsBySourceId("episode-a")[0]?.libraryId, "anime-lib");
  assert.equal(episodes.statusCode, 200);
  assert.deepEqual(episodes.json().Items.map((item: any) => item.Name), ["Pilot"]);

  await app.close();
  store.close();
});

test("maps item-scoped live resume parent ids without broadening to the library", async () => {
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
    itemId: "season-a",
    libraryId: "shows-lib",
    itemType: "Season",
    logicalKey: "season:series:series-a:season:1",
    json: { Id: "season-a", Type: "Season", Name: "Season 1", SeriesId: "series-a", IndexNumber: 1 }
  });
  const seasonId = bridgeItemId("season:series:series-a:season:1");
  const upstream = new FakeUpstream({
    "main:/Users": [{ Id: "main-user", Name: "alice" }],
    "main:/UserItems/Resume": (_serverId: string, _path: string, init: any) => ({
      Items: init.query.ParentId === "season-a"
        ? [{
            Id: "episode-a",
            Type: "Episode",
            Name: "Season Resume",
            ParentId: "season-a",
            SeriesId: "series-a",
            SeasonId: "season-a",
            ParentIndexNumber: 1,
            IndexNumber: 1
          }]
        : [{
            Id: "episode-b",
            Type: "Episode",
            Name: "Wrong Season",
            ParentId: "season-b",
            SeriesId: "series-b",
            SeasonId: "season-b",
            ParentIndexNumber: 1,
            IndexNumber: 1
          }],
      TotalRecordCount: 1,
      StartIndex: 0
    }),
    "main:/Items/series-a": { Id: "series-a", Type: "Series", Name: "Series A", ProviderIds: { Tvdb: "100" } },
    "main:/Items/season-a": { Id: "season-a", Type: "Season", Name: "Season 1", SeriesId: "series-a", IndexNumber: 1 }
  });
  const app = buildApp({ config, store, upstream });
  const login = await app.inject({ method: "POST", url: "/Users/AuthenticateByName", payload: { Username: "alice", Pw: "secret" } });

  const resume = await app.inject({
    method: "GET",
    url: `/UserItems/Resume?userId=${login.json().User.Id}&ParentId=${seasonId}&limit=20`,
    headers: { "X-MediaBrowser-Token": login.json().AccessToken }
  });

  const resumeRequest = upstream.requests.find((request) => request.path === "/UserItems/Resume")?.init as any;
  assert.equal(resume.statusCode, 200);
  assert.equal(resumeRequest.query.ParentId, "season-a");
  assert.deepEqual(resume.json().Items.map((item: any) => item.Name), ["Season Resume"]);

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

test("orders live resume items by last played date across upstreams", async () => {
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
    "primary:/Users": [{ Id: "primary-user", Name: "alice" }],
    "secondary:/Users": [{ Id: "secondary-user", Name: "alice" }],
    "primary:/UserItems/Resume": {
      Items: [{ Id: "old-movie", Type: "Movie", Name: "Older Resume", ProviderIds: { Tmdb: "100" }, UserData: { PlaybackPositionTicks: 100, LastPlayedDate: "2026-01-01T00:00:00.000Z" } }],
      TotalRecordCount: 1,
      StartIndex: 0
    },
    "secondary:/UserItems/Resume": {
      Items: [{ Id: "new-movie", Type: "Movie", Name: "Newer Resume", ProviderIds: { Tmdb: "200" }, UserData: { PlaybackPositionTicks: 200, LastPlayedDate: "2026-02-01T00:00:00.000Z" } }],
      TotalRecordCount: 1,
      StartIndex: 0
    }
  });
  const store = new Store(":memory:");
  const app = buildApp({ config, store, upstream });
  const login = await app.inject({ method: "POST", url: "/Users/AuthenticateByName", payload: { Username: "alice", Pw: "secret" } });

  const resume = await app.inject({
    method: "GET",
    url: `/UserItems/Resume?userId=${login.json().User.Id}&limit=20`,
    headers: { "X-MediaBrowser-Token": login.json().AccessToken }
  });

  assert.equal(resume.statusCode, 200);
  assert.deepEqual(resume.json().Items.map((item: any) => item.Name), ["Newer Resume", "Older Resume"]);

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
    primary: Array.from({ length: 100 }, (_, index) => ({
      Id: `primary-${index}`,
      Type: "Episode",
      Name: `Primary ${index}`,
      ProviderIds: { Tvdb: `p-${index}` }
    })),
    secondary: Array.from({ length: 100 }, (_, index) => ({
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
  assert.equal(nextUp.json().TotalRecordCount, 200);
  assert.deepEqual(nextUp.json().Items.map((item: any) => item.Name), [
    "Primary 50",
    "Primary 51",
    "Primary 52",
    "Primary 53",
    "Primary 54",
    "Primary 55"
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

test("does not exhaust live resume pages to compute totals", async () => {
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
    primary: Array.from({ length: 100 }, (_, index) => ({
      Id: `primary-${index}`,
      Type: "Movie",
      Name: `Primary ${index}`,
      ProviderIds: { Tmdb: `p-${index}` },
      UserData: { PlaybackPositionTicks: 100, LastPlayedDate: `2026-01-${String(28 - (index % 28)).padStart(2, "0")}T00:00:00.000Z` }
    })),
    secondary: Array.from({ length: 100 }, (_, index) => ({
      Id: `secondary-${index}`,
      Type: "Movie",
      Name: `Secondary ${index}`,
      ProviderIds: { Tmdb: `s-${index}` },
      UserData: { PlaybackPositionTicks: 100, LastPlayedDate: `2026-01-${String(28 - (index % 28)).padStart(2, "0")}T00:00:00.000Z` }
    }))
  };
  const upstream = {
    requests: [] as Array<{ serverId: string; path: string; init: any }>,
    async json<T>(serverId: string, path: string, init: any): Promise<T> {
      this.requests.push({ serverId, path, init });
      if (path === "/Users") return [{ Id: `${serverId}-user`, Name: "alice" }] as T;
      if (path !== "/UserItems/Resume") throw new Error(`Unexpected upstream request ${serverId}:${path}`);
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

  const resume = await app.inject({
    method: "GET",
    url: `/UserItems/Resume?userId=${login.json().User.Id}&startIndex=50&limit=6`,
    headers: { "X-MediaBrowser-Token": login.json().AccessToken }
  });

  assert.equal(resume.statusCode, 200);
  assert.equal(resume.json().Items.length, 6);
  assert.equal(resume.json().TotalRecordCount, 200);
  assert.deepEqual(upstream.requests
    .filter((request) => request.path === "/UserItems/Resume")
    .map((request) => request.init.query), [
    { StartIndex: 0, Limit: 56, UserId: "primary-user" },
    { StartIndex: 0, Limit: 56, UserId: "secondary-user" }
  ]);

  await app.close();
  store.close();
});

test("deduplicates fetched live next-up rows without extra total fetches", async () => {
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
    "primary:/Users": [{ Id: "primary-user", Name: "alice" }],
    "secondary:/Users": [{ Id: "secondary-user", Name: "alice" }],
    "primary:/Shows/NextUp": {
      Items: [{ Id: "primary-episode", Type: "Episode", Name: "Episode", ProviderIds: { Tvdb: "100" } }],
      TotalRecordCount: 1,
      StartIndex: 0
    },
    "secondary:/Shows/NextUp": {
      Items: [{ Id: "secondary-episode", Type: "Episode", Name: "Episode Copy", ProviderIds: { Tvdb: "100" } }],
      TotalRecordCount: 1,
      StartIndex: 0
    }
  });
  const store = new Store(":memory:");
  const app = buildApp({ config, store, upstream });
  const login = await app.inject({ method: "POST", url: "/Users/AuthenticateByName", payload: { Username: "alice", Pw: "secret" } });

  const nextUp = await app.inject({
    method: "GET",
    url: `/Shows/NextUp?userId=${login.json().User.Id}&limit=20`,
    headers: { "X-MediaBrowser-Token": login.json().AccessToken }
  });

  assert.equal(nextUp.statusCode, 200);
  assert.equal(nextUp.json().Items.length, 1);
  assert.equal(nextUp.json().TotalRecordCount, 2);

  await app.close();
  store.close();
});

test("does not chase duplicate live next-up rows beyond the fetched page", async () => {
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
    primary: Array.from({ length: 100 }, (_, index) => ({
      Id: `primary-${index}`,
      Type: "Episode",
      Name: `Primary ${index}`,
      ProviderIds: { Tvdb: `${index}` }
    })),
    secondary: Array.from({ length: 100 }, (_, index) => ({
      Id: `secondary-${index}`,
      Type: "Episode",
      Name: `Secondary ${index}`,
      ProviderIds: { Tvdb: `${index}` }
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
  assert.equal(nextUp.json().TotalRecordCount, 200);
  assert.deepEqual(nextUp.json().Items.map((item: any) => item.Name), [
    "Primary 50",
    "Primary 51",
    "Primary 52",
    "Primary 53",
    "Primary 54",
    "Primary 55"
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

test("does not chase duplicate live next-up rows that begin after the requested page", async () => {
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
    primary: Array.from({ length: 100 }, (_, index) => ({
      Id: `primary-${index}`,
      Type: "Episode",
      Name: `Primary ${index}`,
      ProviderIds: { Tvdb: `p-${index}` }
    })),
    secondary: Array.from({ length: 100 }, (_, index) => ({
      Id: `secondary-${index}`,
      Type: "Episode",
      Name: `Secondary ${index}`,
      ProviderIds: { Tvdb: index < 50 ? `s-${index}` : `p-${index}` }
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
    url: `/Shows/NextUp?userId=${login.json().User.Id}&startIndex=0&limit=20`,
    headers: { "X-MediaBrowser-Token": login.json().AccessToken }
  });

  assert.equal(nextUp.statusCode, 200);
  assert.equal(nextUp.json().Items.length, 20);
  assert.equal(nextUp.json().TotalRecordCount, 200);
  assert.deepEqual(upstream.requests
    .filter((request) => request.path === "/Shows/NextUp")
    .map((request) => request.init.query), [
    { StartIndex: 0, Limit: 20, UserId: "primary-user" },
    { StartIndex: 0, Limit: 20, UserId: "secondary-user" }
  ]);

  await app.close();
  store.close();
});

test("cached latest fallback applies Jellyfin collection defaults", async () => {
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
    itemId: "season-a",
    libraryId: "shows-lib",
    itemType: "Season",
    logicalKey: "season:series:series-a:season:1",
    json: { Id: "season-a", Type: "Season", Name: "Season 1", SeriesId: "series-a", IndexNumber: 1, DateCreated: "2026-01-03T00:00:00.000Z" }
  });
  store.upsertIndexedItem({
    serverId: "main",
    itemId: "episode-a",
    libraryId: "shows-lib",
    itemType: "Episode",
    logicalKey: "episode:series:series-a:season:1:episode:1",
    json: { Id: "episode-a", Type: "Episode", Name: "Episode 1", SeriesId: "series-a", SeasonId: "season-a", ParentIndexNumber: 1, IndexNumber: 1, DateCreated: "2026-01-02T00:00:00.000Z" }
  });
  const upstream = new FakeUpstream({
    "main:/Users": [{ Id: "main-user", Name: "alice" }],
    "main:/Items/Latest": new Error("Upstream main request failed for /Items/Latest: offline")
  });
  const app = buildApp({ config, store, upstream });
  const login = await app.inject({ method: "POST", url: "/Users/AuthenticateByName", payload: { Username: "alice", Pw: "secret" } });

  const latest = await app.inject({
    method: "GET",
    url: `/Items/Latest?userId=${login.json().User.Id}&ParentId=${bridgeLibraryId("shows")}&Limit=20`,
    headers: { "X-MediaBrowser-Token": login.json().AccessToken }
  });

  assert.equal(latest.statusCode, 200);
  assert.deepEqual(latest.json().map((item: any) => [item.Name, item.Type]), [["Episode 1", "Episode"]]);

  await app.close();
  store.close();
});

test("cached latest fallback excludes specials before applying the shelf limit", async () => {
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
    itemId: "season-specials",
    libraryId: "shows-lib",
    itemType: "Season",
    logicalKey: "season:series:series-a:season:0",
    json: { Id: "season-specials", Type: "Season", Name: "Specials", SeriesId: "series-a", IndexNumber: 0 }
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
    itemId: "special-a",
    libraryId: "shows-lib",
    itemType: "Episode",
    logicalKey: "episode:series:series-a:season:0:episode:1",
    json: { Id: "special-a", Type: "Episode", Name: "Special Episode", SeriesId: "series-a", SeasonId: "season-specials", IndexNumber: 1, DateCreated: "2026-01-03T00:00:00.000Z" }
  });
  store.upsertIndexedItem({
    serverId: "main",
    itemId: "episode-a",
    libraryId: "shows-lib",
    itemType: "Episode",
    logicalKey: "episode:series:series-a:season:1:episode:1",
    json: { Id: "episode-a", Type: "Episode", Name: "Regular Episode", SeriesId: "series-a", SeasonId: "season-a", ParentIndexNumber: 1, IndexNumber: 1, DateCreated: "2026-01-02T00:00:00.000Z" }
  });
  const upstream = new FakeUpstream({
    "main:/Users": [{ Id: "main-user", Name: "alice" }],
    "main:/Items/Latest": new Error("Upstream main request failed for /Items/Latest: offline")
  });
  const app = buildApp({ config, store, upstream });
  const login = await app.inject({ method: "POST", url: "/Users/AuthenticateByName", payload: { Username: "alice", Pw: "secret" } });

  const latest = await app.inject({
    method: "GET",
    url: `/Items/Latest?userId=${login.json().User.Id}&ParentId=${bridgeLibraryId("shows")}&Limit=1`,
    headers: { "X-MediaBrowser-Token": login.json().AccessToken }
  });

  assert.equal(latest.statusCode, 200);
  assert.deepEqual(latest.json().map((item: any) => item.Name), ["Regular Episode"]);

  await app.close();
  store.close();
});

test("cached latest fallback groups multiple episodes by series", async () => {
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
    json: { Id: "series-a", Type: "Series", Name: "Series A", ProviderIds: { Tvdb: "100" }, DateCreated: "2026-01-01T00:00:00.000Z" }
  });
  for (const index of [1, 2]) {
    store.upsertIndexedItem({
      serverId: "main",
      itemId: `episode-${index}`,
      libraryId: "shows-lib",
      itemType: "Episode",
      logicalKey: `episode:series:series-a:season:1:episode:${index}`,
      json: { Id: `episode-${index}`, Type: "Episode", Name: `Episode ${index}`, SeriesId: "series-a", ParentIndexNumber: 1, IndexNumber: index, DateCreated: `2026-01-0${index + 1}T00:00:00.000Z` }
    });
  }
  const upstream = new FakeUpstream({
    "main:/Users": [{ Id: "main-user", Name: "alice" }],
    "main:/Items/Latest": new Error("Upstream main request failed for /Items/Latest: offline")
  });
  const app = buildApp({ config, store, upstream });
  const login = await app.inject({ method: "POST", url: "/Users/AuthenticateByName", payload: { Username: "alice", Pw: "secret" } });

  const latest = await app.inject({
    method: "GET",
    url: `/Items/Latest?userId=${login.json().User.Id}&ParentId=${bridgeLibraryId("shows")}&Limit=20`,
    headers: { "X-MediaBrowser-Token": login.json().AccessToken }
  });

  assert.equal(latest.statusCode, 200);
  assert.deepEqual(latest.json().map((item: any) => [item.Type, item.Name, item.ChildCount]), [["Series", "Series A", 2]]);

  await app.close();
  store.close();
});

test("cached root latest fallback excludes folders like Jellyfin latest media", async () => {
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
    json: { Id: "series-a", Type: "Series", Name: "Series A", IsFolder: true, DateCreated: "2026-01-03T00:00:00.000Z", ProviderIds: { Tvdb: "100" } }
  });
  store.upsertIndexedItem({
    serverId: "main",
    itemId: "episode-a",
    libraryId: "shows-lib",
    itemType: "Episode",
    logicalKey: "episode:series:series-a:season:1:episode:1",
    json: { Id: "episode-a", Type: "Episode", Name: "Episode 1", IsFolder: false, SeriesId: "series-a", ParentIndexNumber: 1, IndexNumber: 1, DateCreated: "2026-01-02T00:00:00.000Z" }
  });
  const upstream = new FakeUpstream({
    "main:/Users": [{ Id: "main-user", Name: "alice" }],
    "main:/Items/Latest": new Error("Upstream main request failed for /Items/Latest: offline")
  });
  const app = buildApp({ config, store, upstream });
  const login = await app.inject({ method: "POST", url: "/Users/AuthenticateByName", payload: { Username: "alice", Pw: "secret" } });

  const latest = await app.inject({
    method: "GET",
    url: `/Items/Latest?userId=${login.json().User.Id}&Limit=20`,
    headers: { "X-MediaBrowser-Token": login.json().AccessToken }
  });

  assert.equal(latest.statusCode, 200);
  assert.deepEqual(latest.json().map((item: any) => [item.Name, item.Type]), [["Episode 1", "Episode"]]);

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
    "primary:/Items/Latest": (_serverId: string, _path: string, init: any) =>
      (init.query.IncludeItemTypes === "Video"
        ? [{ Id: "instagram-video", Type: "Video", MediaType: "Video", Name: "Instagram Clip", DateCreated: "2026-01-01T00:00:00.000Z" }]
        : [])
  });
  const app = buildApp({ config, store, upstream });
  const login = await app.inject({ method: "POST", url: "/Users/AuthenticateByName", payload: { Username: "alice", Pw: "secret" } });
  const token = login.json().AccessToken;

  const latest = await app.inject({
    method: "GET",
    url: `/Users/${login.json().User.Id}/Items/Latest?ParentId=${passThroughLibraryId("primary", "instagram-lib")}&IncludeItemTypes=Video&Limit=20`,
    headers: { "X-MediaBrowser-Token": token }
  });

  assert.equal(latest.statusCode, 200);
  assert.deepEqual(latest.json().map((item: any) => item.Name), ["Instagram Clip"]);
  assert.deepEqual(latest.json().map((item: any) => item.Type), ["Video"]);
  assert.deepEqual(upstream.requests
    .filter((request) => request.path === "/Items/Latest")
    .map((request) => (request.init as any).query.ParentId), ["instagram-lib"]);

  await app.close();
  store.close();
});

test("does not apply homevideos movie compatibility to root latest movie queries", async () => {
  const passwordHash = await hash("secret");
  const config: BridgeConfig = {
    server: { bind: "127.0.0.1", port: 8096, publicUrl: "http://bridge.test", name: "Bridge" },
    auth: { users: [{ name: "alice", passwordHash }] },
    upstreams: [{ id: "primary", name: "Primary Tone", url: "https://primary.example.com", token: "token" }],
    libraries: [{ id: "movies", name: "Movies", collectionType: "movies", sources: [{ server: "primary", libraryId: "movie-lib" }] }]
  };
  const store = new Store(":memory:");
  store.upsertUpstreamLibrary({ serverId: "primary", libraryId: "movie-lib", name: "Movies", collectionType: "movies" });
  store.upsertUpstreamLibrary({ serverId: "primary", libraryId: "youtube-lib", name: "YouTube", collectionType: "homevideos" });
  const upstream = new FakeUpstream({
    "primary:/Users": [{ Id: "primary-user", Name: "alice" }],
    "primary:/Items/Latest": (_serverId: string, _path: string, init: any) => {
      if (init.query.ParentId === "movie-lib" && init.query.IncludeItemTypes === "Movie") {
        return [{ Id: "movie-a", Type: "Movie", Name: "Movie A", ProviderIds: { Tmdb: "100" }, DateCreated: "2026-01-01T00:00:00.000Z" }];
      }
      if (init.query.ParentId === "youtube-lib" && init.query.IncludeItemTypes === "Video") {
        return [{ Id: "youtube-a", Type: "Video", Name: "YouTube A", DateCreated: "2026-01-02T00:00:00.000Z" }];
      }
      return [];
    }
  });
  const app = buildApp({ config, store, upstream });
  const login = await app.inject({ method: "POST", url: "/Users/AuthenticateByName", payload: { Username: "alice", Pw: "secret" } });

  const latest = await app.inject({
    method: "GET",
    url: `/Items/Latest?userId=${login.json().User.Id}&IncludeItemTypes=Movie&Limit=20`,
    headers: { "X-MediaBrowser-Token": login.json().AccessToken }
  });

  assert.equal(latest.statusCode, 200);
  assert.deepEqual(latest.json().map((item: any) => [item.Name, item.Type]), [
    ["Movie A", "Movie"]
  ]);
  assert.deepEqual(upstream.requests
    .filter((request) => request.path === "/Items/Latest")
    .map((request) => `${(request.init as any).query.ParentId}:${(request.init as any).query.IncludeItemTypes}`)
    .sort(), ["movie-lib:Movie", "youtube-lib:Movie"]);

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
    if (typeof response === "function") return response(serverId, path, init) as T;
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
