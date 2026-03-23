# garmin-golf

CLI-first Python project to download golf data from Garmin Connect, store it locally as Parquet, and compute basic golf statistics over time.

## Stack

- `uv` for project and dependency management
- `garminconnect` for Garmin Connect access
- `polars` + Parquet for local analytics storage
- Chrome DevTools Protocol for attached-browser Garmin Connect mirroring
- `typer` + `rich` for the CLI
- `pytest`, `ruff`, and `mypy` for development checks

## Quick start

```bash
uv sync --extra dev
uv run garmin-golf auth login
uv run garmin-golf sync rounds --from 2025-01-01
uv run garmin-golf mirror scorecards --url https://connect.garmin.com/app/scorecards/<username>
uv run garmin-golf sync shots
uv run garmin-golf stats summary
```

The local stats summary includes normalized 18-hole score averages plus hole-level metrics such as GIR, FIR,
scrambling, scoring breakdowns, par-type scoring, three-putt rate, and penalty rate when the mirrored scorecard
data is available.

You can scope stats to a date window:

```bash
uv run garmin-golf stats summary --from 2025-01-01 --to 2025-12-31
uv run garmin-golf stats summary --period last-12-months
uv run garmin-golf stats summary --period last-year
```

Supported `--period` values are `last-12-months`, `this-year`, and `last-year`.
Use either `--period` or `--from/--to`, not both.

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

If Garmin Connect shows richer golf detail in the browser than the API client can fetch, the preferred fallback is
an attached-browser mirror of your Garmin scorecards page:

```bash
uv run garmin-golf mirror scorecards --url https://connect.garmin.com/app/scorecards/<username>
```

The mirror command:

- attaches to a Chrome instance you already started with remote debugging
- uses your existing logged-in Garmin session
- fetches the full scorecard/detail/shot payload for every discovered scorecard
- writes one raw JSON export per scorecard under `data/raw/browser-mirror/`
- imports those exports into the local Parquet dataset

Start Chrome with remote debugging and log into Garmin in that browser first:

```bash
google-chrome --remote-debugging-port=9222
uv run garmin-golf mirror scorecards \
  --url https://connect.garmin.com/app/scorecards/<username> \
  --debugger-address 127.0.0.1:9222
```

That mode lets the mirror use your real logged-in browser session directly instead of trying to reproduce the Garmin
SSO flow in an isolated browser profile.

Re-running the same command is incremental by default and skips scorecards already mirrored locally. Use
`--force` to refresh everything:

```bash
uv run garmin-golf mirror scorecards --url https://connect.garmin.com/app/scorecards/<username> --force
```

If you need to recover or inspect a single scorecard manually, you can still export it from your logged-in browser
session with the console script:

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

This scorecard-page console export remains the manual fallback when the attached-browser mirror is not suitable or
when you only need one scorecard.

Garmin staff also indicate that the actual golf scorecard can live in a separate FIT file from the activity FIT.
If you can copy scorecard FIT files from the watch or a Garmin Express sync folder, you can inspect or import them:

```bash
uv run garmin-golf inspect scorecard-fit --path /path/to/scorecard.fit
uv run garmin-golf sync import-scorecards --source-dir /path/to/scorecards
```
