import test from "node:test";
import assert from "node:assert/strict";
import { parseAuthorization, userDto, userId } from "../src/auth.js";

test("parses Jellyfin MediaBrowser authorization header", () => {
  const info = parseAuthorization({
    headers: {
      authorization: 'MediaBrowser Client="Swiftfin", Device="iPhone", DeviceId="abc", Version="1.0", Token="tok"'
    },
    query: {}
  } as any);

  assert.deepEqual(info, {
    client: "Swiftfin",
    device: "iPhone",
    deviceId: "abc",
    version: "1.0",
    token: "tok"
  });
});

test("accepts legacy token headers and ApiKey query token", () => {
  assert.equal(parseAuthorization({ headers: { "x-mediabrowser-token": "header-token" }, query: {} } as any).token, "header-token");
  assert.equal(parseAuthorization({ headers: {}, query: { ApiKey: "query-token" } } as any).token, "query-token");
});

test("builds local Jellyfin-shaped user dto", () => {
  const dto = userDto("alice", "server-id", "Bridge");

  assert.equal(dto.Name, "alice");
  assert.equal(dto.Id, userId("alice"));
  assert.equal(dto.ServerId, "server-id");
  assert.equal((dto.Policy as any).EnableMediaPlayback, true);
  assert.equal((dto.Policy as any).EnableLiveTvAccess, false);
});
