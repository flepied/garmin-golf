# garmin-golf — Agent instructions

## Package management

- **`uv` only.** Never use `pip`, `poetry`, or `pipenv`.
- Install: `uv sync --extra dev` (includes pytest, ruff, mypy).
- Python 3.13 only (`requires-python = ">=3.13,<3.14"`).

## Key commands

| Action | Command |
|---|---|
| Install dev deps | `uv sync --extra dev` |
| Run CLI | `uv run garmin-golf <command>` |
| Run CLI directly | `uv run python -m garmin_golf <command>` |
| All tests | `uv run pytest` |
| Tests with coverage | `uv run pytest --cov` |
| Lint check | `uv run ruff check .` |
| Format check | `uv run ruff format --check .` |
| Auto-format | `uv run ruff format .` |
| Typecheck | `uv run mypy .` |

Required order for full check: `ruff check . && ruff format --check . && mypy . && pytest`

## Architecture

- **src-layout:** package code under `src/garmin_golf/`, tests in `tests/`.
- **CLI entrypoint:** `garmin_golf.cli:main` (typer app with subcommands).
- **Data flow:** browser mirror → raw JSON (`data/raw/browser-mirror/`) → `normalize.py` → Parquet (`data/parquet/`) → `stats.py`.
- **`fit_parser.py`** is unused by the main flow — ignore it.
- **`browser_export.py`** just holds a JS bookmarklet string — the real ingestion is `browser_mirror.py`.

## Storage

- Parquet files under `data/parquet/` (`rounds.parquet`, `holes.parquet`, `shots.parquet`). **Gitignored — not committed.**
- Upsert semantics via `pl.DataFrame.unique()`.
- Round-local metadata lives on `rounds.parquet`: `exclude_from_stats` (bool) and `comment` (freeform text). Browser re-import preserves these columns.

## Config

- Pydantic-settings layered: env vars > `.env` > `~/.config/garmin-golf/config.toml`.
- Env overrides: `GARMIN_GOLF_DATA_DIR`, `GARMIN_GOLF_CONFIG_FILE`.
- Club name overrides via config or `uv run garmin-golf config set-club-name`.

## Quirks

- **Chrome DevTools Protocol:** the `mirror scorecards` command needs a Chrome instance started with `--remote-debugging-port=9222` and a logged-in Garmin session. Not a headless or automated browser — attaches to a real user session.
- **Ruff does lint AND format** — two separate commands (`ruff check`, `ruff format`).
- **mypy strict** is on; `fitdecode` has missing-imports override.
- **All CLI commands** support `--json` for machine-readable output. Prefer `--json` when the consumer is an AI agent.
- **Date filters:** `--period` (last-12-months, this-year, last-year) OR `--from`/`--to`, never both.
- **Round annotations:** use `uv run garmin-golf stats annotate-round --round-id <id> --exclude-from-stats --comment "match play"` to mark special rounds. `--include-in-stats` restores aggregate inclusion; `--clear-comment` removes the note.
- **Excluded rounds:** aggregate multi-round stats skip `exclude_from_stats=true` rounds by default. `stats rounds` still lists them, and `stats round --round-id <id>` still works for single-round inspection.
- **Distance outlier trimming:** groups with ≥5 shots exclude distances outside mean ± 2 stddev. Usage counts and score outcomes use all shots.
- **Round merging:** scorecard + activity rounds matched by date+time within 2 hours, preferring scorecard data.

## Tests

- Uses `typer.testing.CliRunner`, heavy monkeypatch, temp Parquet stores.
- No CI, no pre-commit hooks.

## References

- `SKILL.md` has detailed CLI usage guidance for golf stats analysis — read it for stats subcommand documentation.
- `README.md` has full Chrome setup and mirror workflow.
