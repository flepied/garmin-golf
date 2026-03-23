from __future__ import annotations

import json
import shutil
from collections.abc import Mapping
from datetime import date
from pathlib import Path

import typer
from rich.console import Console
from rich.table import Table

from .browser_export import BROWSER_EXPORT_SCRIPT
from .client import GarminGolfClient
from .config import default_config_template, get_config_file, get_settings
from .fit_parser import inspect_activity_archive, inspect_fit_file
from .normalize import normalize_holes, normalize_round, normalize_shots
from .stats import build_round_stats, build_summary_stats
from .storage import Storage
from .sync import sync_rounds, sync_shots

app = typer.Typer(help="Download golf data from Garmin Connect and compute local stats.")
auth_app = typer.Typer(help="Authentication commands.")
config_app = typer.Typer(help="Configuration commands.")
export_app = typer.Typer(help="Browser export helpers.")
inspect_app = typer.Typer(help="Inspection commands.")
sync_app = typer.Typer(help="Synchronization commands.")
stats_app = typer.Typer(help="Local statistics commands.")
app.add_typer(auth_app, name="auth")
app.add_typer(config_app, name="config")
app.add_typer(export_app, name="export")
app.add_typer(inspect_app, name="inspect")
app.add_typer(sync_app, name="sync")
app.add_typer(stats_app, name="stats")


def _console() -> Console:
    return Console()


def _client() -> GarminGolfClient:
    return GarminGolfClient(get_settings(), console=_console())


def _storage() -> Storage:
    return Storage(get_settings())


DATE_FROM_OPTION = typer.Option(None, "--from", help="Inclusive round date in YYYY-MM-DD format.")
DATE_TO_OPTION = typer.Option(None, "--to", help="Inclusive round date in YYYY-MM-DD format.")
ROUND_ID_OPTION = typer.Option(None, "--round-id", help="Round id to sync. Repeatable.")
ROUND_ID_REQUIRED_OPTION = typer.Option(..., "--round-id", help="Round id to inspect.")
FORCE_OPTION = typer.Option(False, "--force", help="Overwrite an existing config file.")
PATH_REQUIRED_OPTION = typer.Option(..., "--path", help="Path to a local FIT file.")
SOURCE_DIR_REQUIRED_OPTION = typer.Option(
    ...,
    "--source-dir",
    help="Directory containing FIT files.",
)
OUT_OPTION = typer.Option(
    Path("garmin-connect-export.js"),
    "--out",
    help="Where to write the browser export script.",
)


@auth_app.command("login")
def auth_login() -> None:
    """Authenticate against Garmin Connect and cache the session tokens."""

    client = _client()
    client.login()
    _console().print("[green]Garmin login succeeded.[/green]")


@config_app.command("init")
def config_init(force: bool = FORCE_OPTION) -> None:
    """Create a starter config file under ~/.config/garmin-golf/."""

    config_file = get_config_file()
    config_file.parent.mkdir(parents=True, exist_ok=True)
    if config_file.exists() and not force:
        raise typer.BadParameter(
            f"Config file already exists at {config_file}. Use --force to overwrite."
        )
    config_file.write_text(default_config_template(), encoding="utf-8")
    _console().print(f"[green]Wrote config file:[/green] {config_file}")
    _console().print(f"Set restrictive permissions, for example: chmod 600 {config_file}")


@config_app.command("show-path")
def config_show_path() -> None:
    """Print the config file path in use."""

    _console().print(str(get_config_file()))


@export_app.command("browser-script")
def export_browser_script(out: Path = OUT_OPTION) -> None:
    """Write a Garmin Connect browser export script for golf scorecards."""

    out.write_text(BROWSER_EXPORT_SCRIPT + "\n", encoding="utf-8")
    _console().print(f"[green]Wrote browser export script:[/green] {out}")


@sync_app.command("rounds")
def sync_rounds_command(
    date_from: str | None = DATE_FROM_OPTION,
    date_to: str | None = DATE_TO_OPTION,
) -> None:
    """Sync golf rounds and hole summaries into local Parquet files."""

    client = _client()
    client.login()
    result = sync_rounds(
        client,
        _storage(),
        date_from=_parse_optional_date(date_from, "--from"),
        date_to=_parse_optional_date(date_to, "--to"),
    )
    _console().print(
        f"[green]Synced[/green] {result.rounds_synced} rounds, "
        f"{result.holes_synced} holes, {result.raw_files_written} raw files."
    )


@sync_app.command("shots")
def sync_shots_command(round_id: list[int] | None = ROUND_ID_OPTION) -> None:
    """Sync shot-level data for locally known rounds."""

    client = _client()
    client.login()
    result = sync_shots(client, _storage(), round_ids=round_id)
    _console().print(
        f"[green]Synced[/green] {result.shots_synced} shots "
        f"from {result.raw_files_written} raw files."
    )


@stats_app.command("summary")
def stats_summary() -> None:
    """Print aggregate golf statistics for all locally synced rounds."""

    storage = _storage()
    summary = build_summary_stats(
        storage.read_table("rounds"),
        storage.read_table("holes"),
        storage.read_table("shots"),
    )
    _render_mapping("Golf Summary", summary)


@stats_app.command("round")
def stats_round(round_id: int = ROUND_ID_REQUIRED_OPTION) -> None:
    """Print local statistics for one round."""

    storage = _storage()
    summary = build_round_stats(
        storage.read_table("rounds"),
        storage.read_table("holes"),
        storage.read_table("shots"),
        round_id,
    )
    _render_mapping(f"Round {round_id}", summary)


@inspect_app.command("fit")
def inspect_fit(round_id: int = ROUND_ID_REQUIRED_OPTION) -> None:
    """Inspect the downloaded FIT archive for one locally synced round."""

    archive_path = get_settings().raw_dir / "activities" / f"{round_id}.zip"
    if not archive_path.exists():
        raise typer.BadParameter(f"No activity archive found at {archive_path}.")
    inspection = inspect_activity_archive(archive_path.read_bytes())
    _render_mapping(
        f"FIT Inspection {round_id}",
        {
            "lap_count": inspection.lap_count,
            "record_count": inspection.record_count,
            "message_types": len(inspection.message_counts),
            "unknown_message_types": len(inspection.unknown_message_counts),
            "archive_members": ", ".join(inspection.archive_members),
            "unknown_messages": ", ".join(
                f"{name}:{count}" for name, count in inspection.unknown_message_counts.items()
            ),
        },
    )


@inspect_app.command("scorecard-fit")
def inspect_scorecard_fit(path: Path = PATH_REQUIRED_OPTION) -> None:
    """Inspect a standalone scorecard FIT file copied from a watch or sync folder."""

    if not path.exists():
        raise typer.BadParameter(f"FIT file not found: {path}")
    inspection = inspect_fit_file(path)
    _render_mapping(
        f"Scorecard FIT {path.name}",
        {
            "lap_count": inspection.lap_count,
            "record_count": inspection.record_count,
            "message_types": len(inspection.message_counts),
            "unknown_message_types": len(inspection.unknown_message_counts),
            "unknown_messages": ", ".join(
                f"{name}:{count}" for name, count in inspection.unknown_message_counts.items()
            ),
        },
    )


@sync_app.command("import-scorecards")
def import_scorecards(
    source_dir: Path = SOURCE_DIR_REQUIRED_OPTION,
    force: bool = FORCE_OPTION,
) -> None:
    """Copy standalone scorecard FIT files into local raw storage and inspect them."""

    if not source_dir.exists() or not source_dir.is_dir():
        raise typer.BadParameter(f"Source directory not found: {source_dir}")

    settings = get_settings()
    target_dir = settings.raw_dir / "scorecards_fit"
    target_dir.mkdir(parents=True, exist_ok=True)
    imported = 0

    for fit_path in sorted(source_dir.glob("*.fit")):
        destination = target_dir / fit_path.name
        if destination.exists() and not force:
            continue
        shutil.copy2(fit_path, destination)
        inspection = inspect_fit_file(destination)
        (target_dir / f"{fit_path.name}.json").write_text(
            json.dumps(inspection.as_dict(), indent=2, default=str, sort_keys=True),
            encoding="utf-8",
        )
        imported += 1

    _console().print(f"[green]Imported[/green] {imported} scorecard FIT files into {target_dir}")


@sync_app.command("import-browser-export")
def import_browser_export(path: Path = PATH_REQUIRED_OPTION) -> None:
    """Import golf data exported from a Garmin Connect browser session."""

    if not path.exists():
        raise typer.BadParameter(f"Export file not found: {path}")

    payload = json.loads(path.read_text(encoding="utf-8"))
    summary = payload.get("summary")
    details = payload.get("details")
    shots = payload.get("shots")

    if (
        not isinstance(summary, dict)
        or not isinstance(details, list)
        or not isinstance(shots, list)
    ):
        raise typer.BadParameter("Unexpected browser export format.")

    scorecard_summaries = summary.get("scorecardSummaries")
    if not isinstance(scorecard_summaries, list):
        raise typer.BadParameter("Browser export is missing summary.scorecardSummaries.")

    summaries_by_id: dict[int, dict[str, object]] = {}
    for item in scorecard_summaries:
        if isinstance(item, dict) and isinstance(item.get("id"), int):
            summaries_by_id[item["id"]] = item

    details_by_id: dict[int, dict[str, object]] = {}
    for item in details:
        if not isinstance(item, dict):
            continue
        scorecard_id = _extract_scorecard_id(item)
        if scorecard_id is not None:
            details_by_id[scorecard_id] = item

    shots_by_id: dict[int, dict[str, object]] = {}
    for item in shots:
        if not isinstance(item, dict):
            continue
        scorecard_id = item.get("scorecardId")
        shot_payload = item.get("payload")
        if isinstance(scorecard_id, int) and isinstance(shot_payload, dict):
            shots_by_id[scorecard_id] = shot_payload

    round_rows: list[dict[str, object]] = []
    hole_rows: list[dict[str, object]] = []
    shot_rows: list[dict[str, object]] = []

    for scorecard_id, summary_row in summaries_by_id.items():
        detail_row = details_by_id.get(scorecard_id)
        if not isinstance(detail_row, dict):
            continue
        round_rows.append(normalize_round(summary_row, detail_row))
        hole_rows.extend(normalize_holes(scorecard_id, detail_row))
        shot_payload = shots_by_id.get(scorecard_id)
        if isinstance(shot_payload, dict):
            for hole_number in range(1, 19):
                shot_rows.extend(normalize_shots(scorecard_id, hole_number, shot_payload))

    storage = _storage()
    storage.upsert_rows("rounds", round_rows, unique_by=["round_id"])
    storage.upsert_rows("holes", hole_rows, unique_by=["round_id", "hole_number"])
    storage.upsert_rows("shots", shot_rows, unique_by=["round_id", "hole_number", "shot_number"])
    storage.write_json_snapshot(Path("browser-export") / path.name, payload)

    _console().print(
        f"[green]Imported browser export:[/green] "
        f"{len(round_rows)} rounds, {len(hole_rows)} holes, {len(shot_rows)} shots."
    )


def _render_mapping(title: str, values: Mapping[str, object]) -> None:
    table = Table(title=title)
    table.add_column("Metric")
    table.add_column("Value", justify="right")
    for key, value in values.items():
        table.add_row(key, str(value))
    _console().print(table)


def _parse_optional_date(value: str | None, option_name: str) -> date | None:
    if value is None:
        return None
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise typer.BadParameter(
            f"{option_name} must be an ISO date like 2025-06-01."
        ) from exc


def _extract_scorecard_id(detail_payload: dict[str, object]) -> int | None:
    scorecard_details = detail_payload.get("scorecardDetails")
    if isinstance(scorecard_details, list):
        for item in scorecard_details:
            if isinstance(item, dict):
                scorecard = item.get("scorecard")
                if isinstance(scorecard, dict) and isinstance(scorecard.get("id"), int):
                    return int(scorecard["id"])
    scorecard = detail_payload.get("scorecard")
    if isinstance(scorecard, dict) and isinstance(scorecard.get("id"), int):
        return int(scorecard["id"])
    payload_id = detail_payload.get("id")
    if isinstance(payload_id, int):
        return int(payload_id)
    return None


def main() -> None:
    app()
