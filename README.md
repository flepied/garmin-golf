# garmin-golf

CLI-first Python project to download golf data from Garmin Connect, store it locally as Parquet, and compute basic golf statistics over time.

## Stack

- `uv` for project and dependency management
- `polars` + Parquet for local analytics storage
- Chrome DevTools Protocol for attached-browser Garmin Connect mirroring
- `typer` + `rich` for the CLI
- `pytest`, `ruff`, and `mypy` for development checks

## Quick start

```bash
uv sync --extra dev
google-chrome --remote-debugging-port=9222 --user-data-dir=/tmp/chrome-debug
# log into Garmin Connect in that Chrome window first
uv run garmin-golf mirror scorecards --url https://connect.garmin.com/app/scorecards/<username>
uv run garmin-golf stats rounds
uv run garmin-golf stats courses
uv run garmin-golf stats summary
```

The mirror command attaches to a Chrome instance with remote debugging enabled and reuses your
logged-in Garmin session. If you have not set that up yet, follow the Chrome steps in
[`Garmin golf data caveat`](#garmin-golf-data-caveat) before running the mirror.

The local stats summary includes normalized 18-hole score averages plus hole-level metrics such as GIR, FIR,
scrambling, scoring breakdowns, par-type scoring, three-putt rate, and penalty rate when the mirrored scorecard
data is available.

You can scope stats to a date window:

```bash
uv run garmin-golf stats rounds --from 2025-01-01 --to 2025-12-31
uv run garmin-golf stats rounds --period last-12-months
uv run garmin-golf stats courses --period last-12-months
uv run garmin-golf stats summary --from 2025-01-01 --to 2025-12-31
uv run garmin-golf stats summary --period last-12-months
uv run garmin-golf stats summary --period last-year
```

Supported `--period` values are `last-12-months`, `this-year`, and `last-year`.
Use either `--period` or `--from/--to`, not both.

To inspect a single round after discovering its id:

```bash
uv run garmin-golf stats rounds
uv run garmin-golf stats round --round-id 22068626916
```

The round view includes the round summary, a hole-by-hole table, club usage for that round, and a
second-shot breakdown for par 4s and par 5s when shot data is available.

To inspect rolling trends over your recent rounds:

```bash
uv run garmin-golf stats trends --window 5
uv run garmin-golf stats trends --window 10 --period last-12-months
```

The trends view shows each round alongside rolling averages and deltas for score to par, GIR, FIR,
scrambling, three-putt rate, and penalties over the selected trailing round window.

To inspect club usage by golf context instead of only raw inventory:

```bash
uv run garmin-golf stats clubs --by-context
uv run garmin-golf stats clubs --by-context --json
```

That view groups clubs by contexts such as par-3 tee shots, par-4 tee shots, par-4 approaches,
par-5 second shots, short game, recovery, and putting.

For agent or script consumption, most commands also support `--json`:

```bash
uv run garmin-golf stats summary --json
uv run garmin-golf stats round --round-id 22068626916 --json
uv run garmin-golf stats trends --window 5 --json
uv run garmin-golf stats clubs --json
```

JSON mode emits structured data only and skips Rich tables and prose formatting.

To review your history on a specific course and identify the hardest holes:

```bash
uv run garmin-golf stats courses
uv run garmin-golf stats course --course "Golf National ~ Aigle"
uv run garmin-golf stats course --course "Golf National ~ Aigle" --period last-12-months
```

## Environment

You can optionally store local settings in `~/.config/garmin-golf/config.toml`:

```toml
data_dir = "/home/you/garmin-golf-data"

[club_name_overrides]
"10400964" = "4 Wood"
"10400967" = "3 Rescue"
"10400977" = "56 Wedge"
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

Precedence is: environment variables, then local `.env`, then `~/.config/garmin-golf/config.toml`.

Club names are inferred from Garmin shot metadata and may not match a player's actual bag exactly.
Use `uv run garmin-golf stats clubs` to inspect observed `club_id` values, inferred names, configured
names, shot counts, and average distances, then add `club_name_overrides` entries for any ids that
need bag-specific labels. Use `uv run garmin-golf stats clubs --by-context` to inspect how each club
is used across tee shots, approaches, short game, recovery shots, and putting contexts.

Displayed distance averages in `stats summary`, `stats clubs`, and `stats second-shots` trim obvious
outliers automatically: once a comparison group has at least 5 shots, distances outside `mean +/- 2`
standard deviations are excluded from the reported averages. Usage counts and score outcomes still use
all matching shots.

## Garmin golf data caveat

The supported ingestion path is an attached-browser mirror of your Garmin scorecards page:

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
google-chrome --remote-debugging-port=9222 --user-data-dir=/tmp/chrome-debug
curl -sS http://127.0.0.1:9222/json/version
uv run garmin-golf mirror scorecards \
  --url https://connect.garmin.com/app/scorecards/<username> \
  --debugger-address 127.0.0.1:9222
```

Some Chrome setups will not expose the debugger cleanly from your default profile. If the plain
`--remote-debugging-port=9222` launch does not work, start Chrome with a dedicated temporary
profile such as `--user-data-dir=/tmp/chrome-debug`.
The `curl` command should return JSON describing the running browser and its websocket debugger URL.

That mode lets the mirror use your real logged-in browser session directly instead of trying to reproduce the Garmin
SSO flow in an isolated browser profile.

Re-running the same command is incremental by default and skips scorecards already mirrored locally. Use
`--force` to refresh everything:

```bash
uv run garmin-golf mirror scorecards --url https://connect.garmin.com/app/scorecards/<username> --force
```
