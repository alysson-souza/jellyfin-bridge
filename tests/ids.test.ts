import test from "node:test";
import assert from "node:assert/strict";
import { bridgeItemId, bridgeLibraryId, bridgeMediaSourceId, bridgeServerId } from "../src/ids.js";

test("bridge ids are deterministic Jellyfin-style 32 character ids", () => {
  assert.equal(bridgeServerId("Bridge"), bridgeServerId("Bridge"));
  assert.match(bridgeServerId("Bridge"), /^[0-9a-f]{32}$/);
  assert.match(bridgeLibraryId("movies"), /^[0-9a-f]{32}$/);
  assert.match(bridgeItemId("main:item-1"), /^[0-9a-f]{32}$/);
  assert.match(bridgeMediaSourceId("main", "item-1", "source-1"), /^[0-9a-f]{32}$/);
});

test("bridge ids include source identity to avoid collisions", () => {
  assert.notEqual(bridgeItemId("main:item-1"), bridgeItemId("remote:item-1"));
  assert.notEqual(
    bridgeMediaSourceId("main", "item-1", "source-1"),
    bridgeMediaSourceId("remote", "item-1", "source-1")
  );
});
