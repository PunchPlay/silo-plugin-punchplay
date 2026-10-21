# PunchPlay sync plugin for Silo

Syncs a [Silo](https://siloserver.org) media server with [PunchPlay](https://punchplay.tv), both ways, with live scrobbling. Built on [silo-plugin-sdk](https://github.com/Silo-Server/silo-plugin-sdk) v0.28.0 as a `watch_sync_provider.v1` plugin.

## Install

**From the PunchPlay repository (recommended, gets updates).** In Silo, open **Admin > Integrations > Plugins**, choose **Add repository**, and enter:

```
https://raw.githubusercontent.com/PunchPlay/silo-plugin-punchplay/main/repository.json
```

Then find **PunchPlay Watch Provider** in the **Catalog** tab and choose **Install**. New releases show up there as updates.

**From a file.** Download the binary for your server from the [latest release](https://github.com/PunchPlay/silo-plugin-punchplay/releases/latest) (`plugin-linux-amd64` for most Linux servers and NAS units, `plugin-linux-arm64` for ARM boards and Docker on Apple Silicon, `plugin-darwin-arm64` for Silo running natively on a Mac), then use **Install from a file** on the same page.

## Setup

1. Install the plugin as above. It needs no API keys or admin settings.
2. Connect each Silo profile from its watch-provider settings. Silo shows a code; enter it at [punchplay.tv/link](https://punchplay.tv/link).

That's it. The plugin signs in through PunchPlay's own public Silo app, so nothing secret ships in the binary.

## What syncs

| | From PunchPlay into Silo | From Silo to PunchPlay |
| --- | --- | --- |
| Watched movies and episodes | Yes | Yes, every rewatch as its own play |
| Unwatching | | Yes (moved to PunchPlay's trash, restorable) |
| Resume progress | Yes | From live playback |
| Live playback (start, pause, stop) | | Yes, shown as Now Playing |
| Ratings (movies, series, episodes) | Yes | Yes |
| Favourites | Yes | Yes |
| Watchlist | Yes | Yes |
| Dropped shows | Yes | Yes |

Anime is included. PunchPlay stores anime by TVDB series ID in official order, which is also Silo's default.

## How it works

The plugin is stateless. Silo keeps the credentials, cursors and retry queue; each RPC becomes calls to the PunchPlay Platform API.

- **Sign-in:** device code flow (`/auth/device/code`, `/auth/device/token`). Access tokens last an hour and refresh with rotation (`/auth/refresh`). Credentials record the app and server that issued them.
- **Export:** consecutive events for the same endpoint go out as one bulk write of up to 100 items (`/sync/history`, `/sync/ratings`, `/sync/watchlist`), in Silo's order. Each play is keyed on Silo's watch-history ID, so a retry or a reconnect never logs the same play twice. Every write carries an `Idempotency-Key`.
- **Live playback:** `/playback/start`, `/pause` and `/stop`. A stop logs a watch only when Silo says the playback completed. Starts and pauses more than two hours old are skipped, since they no longer describe anything live.
- **Import:** the first sync reads full snapshots (`/me/sync/snapshot`) and ends at the change journal's high-water mark; later syncs read only the changes since (`/me/sync/changes`). An expired cursor starts a new snapshot. Each read takes PunchPlay's largest page, 500 rows; what doesn't fit Silo's page is carried to the next one rather than read again. Rows left out, such as a watch without a TMDB or TVDB ID, are counted by reason and reported once, as warnings on the sync.
- **Rate limits:** requests are paced per profile, just under PunchPlay's limits: about 110 reads a minute, 29 bulk writes a minute, and 119 playback events every five minutes. If PunchPlay still answers 429 and its `Retry-After` fits within the call, the plugin waits and retries in place, at most twice; otherwise Silo pauses the connection for that long. This matters for large libraries, because Silo discards a whole import on any failure and starts again from the first page.
- **Unmatched titles:** an item PunchPlay can't match yet is reported to Silo as applied. PunchPlay keeps it, retries the match, and lists it for the user in Settings.

## Known limits

- Unwatching needs a TMDB ID; an episode known only by TVDB ID is rejected.
- Silo sends no periodic progress during playback, so the resume point updates on pause and stop.
- The Watchlist has no manual order on PunchPlay yet, so none is synced.

## Development

```bash
go test -race ./...
make audit        # govulncheck: known vulnerabilities in reachable code
make build        # ./plugin for this machine
make build-all    # dist/ for every release platform
go run . manifest # print the manifest with this binary's checksum
```

To test against a local PunchPlay, set the plugin's advanced settings in Silo: **API base URL** (for example `http://localhost:3000`; plain http is accepted only for this machine) and **Client ID** of a public device app registered there.

## Releases

Run the Release workflow to bump the patch version, or set `version` in `manifest.json` and push a matching `vX.Y.Z` tag (the workflow refuses a tag that differs from the manifest). It builds `linux/amd64`, `linux/arm64` and `darwin/arm64`, publishes them with checksums, and then updates `repository.json` on `main` so Silo servers see the new version.

## Licence

Apache-2.0. "Silo" is used only to say what this plugin plugs into; this is not an official Silo plugin.
