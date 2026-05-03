import test from "node:test";
import assert from "node:assert/strict";
import { bridgeItemId } from "../src/ids.js";
import { rewriteDto } from "../src/rewriter.js";

test("rewrites Jellyfin item ids, server ids, media source ids, and user data keys", () => {
  const rewritten = rewriteDto(
    {
      Id: "upstream-item",
      ServerId: "upstream-server",
      ParentId: "upstream-parent",
      SeriesId: "upstream-series",
      UserData: {
        Key: "upstream-item",
        ItemId: "upstream-item",
        Played: false
      },
      MediaSources: [
        {
          Id: "source-1",
          ItemId: "upstream-item"
        },
        {
          Id: "source-2"
        }
      ]
    },
    { serverId: "main", bridgeServerId: "bridge-server" }
  ) as Record<string, any>;

  assert.equal(rewritten.Id, bridgeItemId("main:upstream-item"));
  assert.equal(rewritten.ServerId, "bridge-server");
  assert.equal(rewritten.ParentId, bridgeItemId("main:upstream-parent"));
  assert.equal(rewritten.SeriesId, bridgeItemId("main:upstream-series"));
  assert.equal(rewritten.UserData.Key, rewritten.Id);
  assert.equal(rewritten.UserData.ItemId, rewritten.Id);
  assert.match(rewritten.MediaSources[0].Id, /^[0-9a-f]{32}$/);
  assert.equal(rewritten.MediaSources[0].ItemId, rewritten.Id);
  assert.match(rewritten.MediaSources[1].Id, /^[0-9a-f]{32}$/);
  assert.equal(rewritten.MediaSources[1].ItemId, rewritten.Id);
});

test("uses logical item map when upstream items are merged", () => {
  const logicalId = bridgeItemId("movie:tmdb:123");
  const rewritten = rewriteDto(
    {
      Id: "remote-copy",
      ParentId: "remote-library"
    },
    {
      serverId: "remote",
      bridgeServerId: "bridge-server",
      itemIdMap: new Map([
        ["remote-copy", logicalId],
        ["remote-library", "library-id"]
      ])
    }
  ) as Record<string, unknown>;

  assert.equal(rewritten.Id, logicalId);
  assert.equal(rewritten.ParentId, "library-id");
});
