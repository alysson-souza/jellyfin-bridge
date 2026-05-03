import test from "node:test";
import assert from "node:assert/strict";
import { libraryDto, publicSystemInfo, queryResult, systemInfo } from "../src/jellyfin.js";

const config = {
  server: { bind: "0.0.0.0", port: 8096, publicUrl: "https://bridge.example.com", name: "Bridge" },
  auth: { users: [{ name: "alice", passwordHash: "hash" }] },
  upstreams: [{ id: "main", name: "Main", url: "https://jellyfin.example.com", token: "token" }],
  libraries: []
};

test("public system info follows Jellyfin public info shape", () => {
  const info = publicSystemInfo(config);

  assert.equal(info.LocalAddress, "https://bridge.example.com");
  assert.equal(info.ServerName, "Bridge");
  assert.equal(info.ProductName, "Jellyfin Bridge");
  assert.equal(info.StartupWizardCompleted, true);
  assert.match(String(info.Id), /^[0-9a-f]{32}$/);
});

test("system info extends public info with server capabilities", () => {
  const info = systemInfo(config);

  assert.equal(info.ServerName, "Bridge");
  assert.equal(info.SupportsLibraryMonitor, false);
  assert.deepEqual(info.CompletedInstallations, []);
});

test("query result uses Jellyfin Items/TotalRecordCount/StartIndex envelope", () => {
  assert.deepEqual(queryResult([{ Id: "1" }], 2, 10), {
    Items: [{ Id: "1" }],
    TotalRecordCount: 10,
    StartIndex: 2
  });
});

test("library dto uses CollectionFolder user view shape", () => {
  const dto = libraryDto(
    { id: "movies", name: "Movies", collectionType: "movies", sources: [{ server: "main", libraryId: "abc" }] },
    "server-id"
  );

  assert.equal(dto.Type, "CollectionFolder");
  assert.equal(dto.CollectionType, "movies");
  assert.equal(dto.IsFolder, true);
  assert.equal(dto.ServerId, "server-id");
});
