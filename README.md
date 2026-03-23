# garmin-golf

CLI-first Python project to download golf data from Garmin Connect, store it locally as Parquet, and compute basic golf statistics over time.

## Stack

- `uv` for project and dependency management
- `garminconnect` for Garmin Connect access
- `polars` + Parquet for local analytics storage
- `typer` + `rich` for the CLI
- `pytest`, `ruff`, and `mypy` for development checks

## Quick start

```bash
uv sync --extra dev
uv run garmin-golf auth login
uv run garmin-golf sync rounds --from 2025-01-01
uv run garmin-golf sync shots
uv run garmin-golf stats summary
```

## Environment

Set Garmin credentials with environment variables:

```bash
export GARMIN_GOLF_GARMIN_EMAIL="you@example.com"
export GARMIN_GOLF_GARMIN_PASSWORD="secret"
```

You can also store them in `~/.config/garmin-golf/config.toml`:

```toml
garmin_email = "you@example.com"
garmin_password = "secret"
# optional
data_dir = "/home/you/garmin-golf-data"
```

Recommended permissions:

```bash
mkdir -p ~/.config/garmin-golf
chmod 700 ~/.config/garmin-golf
chmod 600 ~/.config/garmin-golf/config.toml
```

Optional overrides:

- `GARMIN_GOLF_DATA_DIR` to change the local dataset directory
- `GARMIN_GOLF_CONFIG_FILE` to point at a different TOML config file
- `GARMINTOKENS` to isolate Garmin token storage if desired

Precedence is: environment variables, then local `.env`, then `~/.config/garmin-golf/config.toml`.

Garmin OAuth tokens are cached by default under `~/.config/garmin-golf/tokens/` to avoid repeated logins.
They are not stored in this repository.

## Garmin golf data caveat

Garmin's dedicated golf scorecard endpoint may return empty data for some accounts even when golf rounds exist.
When that happens, `garmin-golf sync rounds` falls back to the standard Garmin activity feed for `golf`
activities and stores:

- round metadata in Parquet
- raw activity JSON
- the original downloaded activity archive (`.zip` containing the FIT file)

That fallback is reliable for round discovery, but hole- and shot-level golf stats may still be unavailable until
Garmin exposes a stable scorecard endpoint or the FIT export can be decoded more completely.

You can inspect the downloaded FIT archive for a synced round with:

```bash
uv run garmin-golf inspect fit --round-id 22068626916
```

If Garmin Connect shows richer golf detail in the browser than the API client can fetch, export it from your
logged-in browser session:

```bash
uv run garmin-golf export browser-script --out garmin-connect-export.js
```

Then:

1. Sign in to Garmin Connect in your browser.
2. Open the exact Garmin scorecard page, for example `https://connect.garmin.com/app/scorecard/<id>`.
3. Open the developer console on that Garmin Connect tab.
4. Paste the generated script from `garmin-connect-export.js`.
5. If Garmin prompts for a CSRF token, copy the `connect-csrf-token` request header value from DevTools Network and paste it.
6. Save the downloaded `garmin-golf-export.json`.
7. Import it locally:

```bash
uv run garmin-golf sync import-browser-export --path garmin-golf-export.json
```

This scorecard-page console export is the recommended fallback when the direct API sync does not expose the golf detail you need.

Garmin staff also indicate that the actual golf scorecard can live in a separate FIT file from the activity FIT.
If you can copy scorecard FIT files from the watch or a Garmin Express sync folder, you can inspect or import them:

```bash
uv run garmin-golf inspect scorecard-fit --path /path/to/scorecard.fit
uv run garmin-golf sync import-scorecards --source-dir /path/to/scorecards
```
