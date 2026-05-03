import test from "node:test";
import assert from "node:assert/strict";
import { rewriteHlsPlaylist } from "../src/hls.js";

test("rewrites relative HLS playlist URLs back through the bridge", () => {
  const playlist = `#EXTM3U
#EXT-X-STREAM-INF:BANDWIDTH=1400000
main.m3u8
#EXTINF:6.0,
segment0.ts
#EXT-X-MAP:URI="init.mp4"
https://cdn.example.com/absolute.ts
`;

  assert.equal(
    rewriteHlsPlaylist(playlist, {
      bridgeBasePath: "/Videos/bridge-item/hls/bridge-source",
      upstreamBasePath: "/Videos/upstream-item/hls"
    }),
    `#EXTM3U
#EXT-X-STREAM-INF:BANDWIDTH=1400000
/Videos/bridge-item/hls/bridge-source/main.m3u8
#EXTINF:6.0,
/Videos/bridge-item/hls/bridge-source/segment0.ts
#EXT-X-MAP:URI="/Videos/bridge-item/hls/bridge-source/init.mp4"
https://cdn.example.com/absolute.ts
`
  );
});

test("rewrites absolute upstream HLS URLs that point at the same upstream path", () => {
  const playlist = "#EXTM3U\nhttps://upstream.example.com/Videos/upstream-item/hls/segment1.ts?token=abc#frag\n";

  assert.equal(
    rewriteHlsPlaylist(playlist, {
      bridgeBasePath: "/Videos/bridge-item/hls/bridge-source",
      upstreamBasePath: "/Videos/upstream-item/hls"
    }),
    "#EXTM3U\n/Videos/bridge-item/hls/bridge-source/segment1.ts?token=abc#frag\n"
  );
});

test("rewrites HLS tag URIs for subtitles, keys, and iframe playlists", () => {
  const playlist = `#EXTM3U
#EXT-X-KEY:METHOD=AES-128,URI="key.bin?token=abc"
#EXT-X-MEDIA:TYPE=SUBTITLES,GROUP-ID="subs",NAME="English",URI="subtitles/eng.m3u8"
#EXT-X-I-FRAME-STREAM-INF:BANDWIDTH=86000,URI="iframes/main.m3u8"
`;

  assert.equal(
    rewriteHlsPlaylist(playlist, {
      bridgeBasePath: "/Videos/bridge-item/hls/bridge-source",
      upstreamBasePath: "/Videos/upstream-item/hls"
    }),
    `#EXTM3U
#EXT-X-KEY:METHOD=AES-128,URI="/Videos/bridge-item/hls/bridge-source/key.bin?token=abc"
#EXT-X-MEDIA:TYPE=SUBTITLES,GROUP-ID="subs",NAME="English",URI="/Videos/bridge-item/hls/bridge-source/subtitles/eng.m3u8"
#EXT-X-I-FRAME-STREAM-INF:BANDWIDTH=86000,URI="/Videos/bridge-item/hls/bridge-source/iframes/main.m3u8"
`
  );
});
