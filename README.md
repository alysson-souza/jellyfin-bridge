# Jellyfin Bridge

Jellyfin Bridge exposes one Jellyfin-compatible endpoint in front of two or more real Jellyfin servers.

It is an aggregator and proxy. Clients connect to the bridge, and the bridge proxies Jellyfin metadata, artwork, playback info, subtitles, and stream bytes from the upstream servers. Matching upstream libraries can be merged into one bridge library.

The bridge does not scan media, edit metadata, manage plugins, or replace your upstream Jellyfin servers.

## Run With Docker Compose

Docker Compose is the normal way to run Jellyfin Bridge. You can use the
prebuilt image from GitHub Container Registry:

```fish
cp config.example.yaml config.yaml
touch .env
```

Add your upstream Jellyfin API keys to `.env`:

```text
MAIN_JELLYFIN_TOKEN=replace-me
SECONDARY_JELLYFIN_TOKEN=replace-me
```

Generate the bridge password hash:

```fish
env BRIDGE_PASSWORD=replace-with-a-strong-password node --input-type=module -e 'import { hash } from "@node-rs/argon2"; console.log(await hash(process.env.BRIDGE_PASSWORD));'
```

Put the printed hash in `config.yaml`, then edit the upstream URLs and library IDs.

Use this compose file:

```yaml
services:
  jellyfin-bridge:
    image: ghcr.io/alysson-souza/jellyfin-bridge:latest
    ports:
      - "8096:8096"
    volumes:
      - ./config.yaml:/config/config.yaml:ro
      - jellyfin-bridge-data:/data
    env_file:
      - .env
    restart: unless-stopped

volumes:
  jellyfin-bridge-data:
```

Start the bridge:

```fish
docker compose up -d
curl http://localhost:8096/System/Ping
```

## Build Locally

The repository also includes a compose file that builds the image locally:

```fish
docker compose up -d --build
curl http://localhost:8096/System/Ping
```

## Configure

Edit `config.yaml`. The comments in `config.example.yaml` explain each section, and the schema line at the top gives editor validation.

By default the bridge validates upstream reachability and mapped library IDs before it starts listening. For local testing in an environment where an upstream DNS name or VPN route is temporarily unavailable, set `startup.validateUpstreams: false`; request-time upstream failures are still returned normally.

To find Jellyfin library IDs, first get a user ID:

```fish
curl -H "X-Emby-Token: $MAIN_JELLYFIN_TOKEN" https://jellyfin-main.example.com/Users
curl -H "X-Emby-Token: $SECONDARY_JELLYFIN_TOKEN" https://jellyfin-secondary.example.com/Users
```

Then list that user's views:

```fish
curl -H "X-Emby-Token: $MAIN_JELLYFIN_TOKEN" "https://jellyfin-main.example.com/UserViews?UserId=USER_ID_HERE"
curl -H "X-Emby-Token: $SECONDARY_JELLYFIN_TOKEN" "https://jellyfin-secondary.example.com/UserViews?UserId=USER_ID_HERE"
```

Use the `Id` values as `libraryId` entries in `config.yaml`.

### Refreshing the Catalog

The bridge serves cached catalog data for speed. A full manual reconciliation is available at:

```fish
curl -X POST -H "X-Emby-Token: $BRIDGE_TOKEN" http://localhost:8096/Bridge/Scan
```

For automatic refreshes, set `scan.onStart` or `scan.intervalMinutes` in `config.yaml`.
Incremental refreshes use Jellyfin's `MinDateLastSaved` support after the first successful scan. Set
`scan.fullScanIntervalMinutes` when you also want periodic full reconciliation to remove deleted upstream items.

## Client Setup

Add the bridge as a normal Jellyfin server in your client:

- URL: `http://bridge-host:8096`
- Username: bridge username from `config.yaml`
- Password: the password used to generate the hash

## Node.js

```fish
npm ci
npm run build
env MAIN_JELLYFIN_TOKEN=replace-me SECONDARY_JELLYFIN_TOKEN=replace-me npm start -- --config config.yaml --database ./jellyfin-bridge.db
```

## Verify

```fish
npm test
npm run build
docker compose logs -f jellyfin-bridge
```
