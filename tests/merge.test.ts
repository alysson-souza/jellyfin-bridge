import test from "node:test";
import assert from "node:assert/strict";
import { logicalItemKey, mergeSources } from "../src/merge.js";

test("movies merge by strong provider ids with source priority order preserved", () => {
  const groups = mergeSources([
    { serverId: "main", priority: 0, item: { Id: "a", Type: "Movie", Name: "Alien", ProviderIds: { Imdb: "tt0078748" } } },
    { serverId: "remote", priority: 1, item: { Id: "b", Type: "Movie", Name: "Alien", ProviderIds: { Imdb: "tt0078748" } } },
    { serverId: "remote", priority: 1, item: { Id: "c", Type: "Movie", Name: "Aliens", ProviderIds: { Tmdb: "679" } } }
  ]);

  assert.equal(groups.length, 2);
  assert.deepEqual(groups[0].sources.map((source) => source.item.Id), ["a", "b"]);
  assert.equal(groups[0].defaultSource.item.Id, "a");
  assert.equal(groups[0].logicalKey, "movie:imdb:tt0078748");
});

test("episodes merge by provider id or series plus season and episode numbers", () => {
  assert.equal(
    logicalItemKey({
      Id: "episode",
      Type: "Episode",
      SeriesId: "series",
      ParentIndexNumber: 1,
      IndexNumber: 2,
      ProviderIds: {}
    }),
    "episode:series:series:season:1:episode:2"
  );
  assert.equal(
    logicalItemKey({ Id: "episode", Type: "Episode", ProviderIds: { Tvdb: "123" } }),
    "episode:tvdb:123"
  );
});

test("ambiguous items without strong keys do not merge", () => {
  const groups = mergeSources([
    { serverId: "main", priority: 0, item: { Id: "a", Type: "Movie", Name: "The Thing", ProductionYear: 1982, ProviderIds: {} } },
    { serverId: "remote", priority: 1, item: { Id: "b", Type: "Movie", Name: "The Thing", ProductionYear: 1982, ProviderIds: {} } }
  ]);

  assert.equal(groups.length, 2);
  assert.notEqual(groups[0].logicalKey, groups[1].logicalKey);
});

test("music tracks use MusicBrainz ids before strict album artist metadata", () => {
  assert.equal(
    logicalItemKey({ Id: "track", Type: "Audio", ProviderIds: { MusicBrainzTrack: "mb-track" } }),
    "track:musicbrainztrack:mb-track"
  );
  assert.equal(
    logicalItemKey({
      Id: "track",
      Type: "Audio",
      AlbumArtist: "Massive Attack",
      Album: "Mezzanine",
      ProductionYear: 1998,
      ParentIndexNumber: 1,
      IndexNumber: 3,
      Name: "Teardrop",
      ProviderIds: {}
    }),
    "track:strict:massive attack:mezzanine:1998:disc:1:track:3:teardrop"
  );
});
