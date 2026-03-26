from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import date, datetime, timedelta
from pathlib import Path

import polars as pl
import typer
from rich.console import Console
from rich.table import Table

from .browser_mirror import BrowserMirror, BrowserMirrorError, validate_scorecards_url
from .config import default_config_template, get_config_file, get_settings
from .stats import (
    build_club_context_stats,
    build_course_focus_stats,
    build_course_hole_stats,
    build_practice_focus_stats,
    build_round_stats,
    build_round_trends,
    build_second_shot_stats,
    build_summary_stats,
    trim_distance_outliers,
)
from .storage import Storage

app = typer.Typer(help="Download golf data from Garmin Connect and compute local stats.")
config_app = typer.Typer(help="Configuration commands.")
mirror_app = typer.Typer(help="Interactive browser mirroring commands.")
stats_app = typer.Typer(help="Local statistics commands.")
app.add_typer(config_app, name="config")
app.add_typer(mirror_app, name="mirror")
app.add_typer(stats_app, name="stats")


def _console() -> Console:
    return Console()


def _storage() -> Storage:
    return Storage(get_settings())


def _shots_with_normalized_shot_types(shots: pl.DataFrame) -> pl.DataFrame:
    if shots.is_empty():
        return shots

    if "shot_number" in shots.columns and "shot_type" in shots.columns:
        return shots.with_columns(
            pl.when(
                (pl.col("shot_number").cast(pl.Int64, strict=False) == 1)
                & (pl.col("shot_type").cast(pl.String, strict=False) != "PUTT")
            )
            .then(pl.lit("TEE"))
            .otherwise(pl.col("shot_type").cast(pl.String, strict=False))
            .alias("shot_type")
        )
    return shots


def _shots_with_configured_club_names(shots: pl.DataFrame) -> pl.DataFrame:
    shots = _shots_with_normalized_shot_types(shots)

    if "club_id" not in shots.columns:
        return shots

    overrides: dict[int, str] = {}
    for key, value in get_settings().club_name_overrides.items():
        try:
            overrides[int(key)] = value
        except (TypeError, ValueError):
            continue
    if not overrides:
        return shots

    fields = ["club_id"] + (["club"] if "club" in shots.columns else [])
    return shots.with_columns(
        pl.struct(fields)
        .map_elements(
            lambda row: overrides.get(row["club_id"]) or row.get("club"),
            return_dtype=pl.String,
        )
        .alias("club")
    )


DATE_FROM_OPTION = typer.Option(None, "--from", help="Inclusive round date in YYYY-MM-DD format.")
DATE_TO_OPTION = typer.Option(None, "--to", help="Inclusive round date in YYYY-MM-DD format.")
ROUND_ID_REQUIRED_OPTION = typer.Option(..., "--round-id", help="Round id to inspect.")
FORCE_OPTION = typer.Option(False, "--force", help="Overwrite an existing config file.")
TIMEOUT_OPTION = typer.Option(
    300,
    "--timeout",
    min=30,
    help="Interactive timeout in seconds while waiting for Garmin sign-in.",
)
OUT_DIR_OPTION = typer.Option(
    None,
    "--out-dir",
    help="Directory where mirrored browser exports are written.",
)
URL_REQUIRED_OPTION = typer.Option(..., "--url", help="Garmin Connect URL to mirror.")
DEBUGGER_ADDRESS_REQUIRED_OPTION = typer.Option(
    ...,
    "--debugger-address",
    help=(
        "Attach to an existing Chrome started with "
        "--remote-debugging-port, for example 127.0.0.1:9222."
    ),
)
PERIOD_OPTION = typer.Option(
    None,
    "--period",
    help="Shortcut period: last-12-months, this-year, or last-year.",
)
TREND_WINDOW_OPTION = typer.Option(
    5,
    "--window",
    help="Rolling round window size: 5, 10, or 20.",
)
COURSE_REQUIRED_OPTION = typer.Option(..., "--course", help="Exact course name to analyze.")
JSON_OPTION = typer.Option(False, "--json", help="Emit structured JSON to stdout.")
BY_CONTEXT_OPTION = typer.Option(
    False,
    "--by-context",
    help="Group club performance by golf context such as tee shots and approaches.",
)
ROUND_MATCH_TOLERANCE = timedelta(hours=2)


@config_app.command("init")
def config_init(force: bool = FORCE_OPTION, json_output: bool = JSON_OPTION) -> None:
    """Create a starter config file under ~/.config/garmin-golf/."""

    config_file = get_config_file()
    existed = config_file.exists()
    config_file.parent.mkdir(parents=True, exist_ok=True)
    if existed and not force:
        raise typer.BadParameter(
            f"Config file already exists at {config_file}. Use --force to overwrite."
        )
    config_file.write_text(default_config_template(), encoding="utf-8")
    if json_output:
        _emit_json(
            {
                "config_path": str(config_file),
                "overwritten": existed,
                "permissions_hint": f"chmod 600 {config_file}",
            }
        )
        return
    _console().print(f"[green]Wrote config file:[/green] {config_file}")
    _console().print(f"Set restrictive permissions, for example: chmod 600 {config_file}")


@config_app.command("show-path")
def config_show_path(json_output: bool = JSON_OPTION) -> None:
    """Print the config file path in use."""

    config_path = str(get_config_file())
    if json_output:
        _emit_json({"config_path": config_path})
        return
    _console().print(config_path)


@mirror_app.command("scorecards")
def mirror_scorecards(
    url: str = URL_REQUIRED_OPTION,
    out_dir: Path | None = OUT_DIR_OPTION,
    debugger_address: str = DEBUGGER_ADDRESS_REQUIRED_OPTION,
    force: bool = typer.Option(
        False,
        "--force",
        help="Re-fetch scorecards that were already mirrored.",
    ),
    timeout: int = TIMEOUT_OPTION,
    json_output: bool = JSON_OPTION,
) -> None:
    """Mirror Garmin scorecards with an interactive browser session."""

    try:
        validate_scorecards_url(url)
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc

    settings = get_settings()
    output_dir = out_dir if out_dir is not None else settings.raw_dir / "browser-mirror"
    storage = Storage(settings)
    mirror = BrowserMirror(
        timeout_seconds=timeout,
        debugger_address=debugger_address,
        console=_console(),
    )
    try:
        result = mirror.mirror(url, storage=storage, output_dir=output_dir, force=force)
    except (BrowserMirrorError, ValueError) as exc:
        raise typer.BadParameter(str(exc)) from exc

    payload = {
        "discovered": result.discovered,
        "exported": result.exported,
        "skipped": result.skipped,
        "rounds_imported": result.rounds_imported,
        "holes_imported": result.holes_imported,
        "shots_imported": result.shots_imported,
        "output_dir": str(output_dir),
    }
    if json_output:
        _emit_json(payload)
        return
    _console().print(
        f"[green]Mirrored scorecards:[/green] discovered={result.discovered}, "
        f"exported={result.exported}, skipped={result.skipped}, "
        f"imported={result.rounds_imported} rounds, {result.holes_imported} holes, "
        f"{result.shots_imported} shots into {output_dir}"
    )


@stats_app.command("summary")
def stats_summary(
    date_from: str | None = DATE_FROM_OPTION,
    date_to: str | None = DATE_TO_OPTION,
    period: str | None = PERIOD_OPTION,
    json_output: bool = JSON_OPTION,
) -> None:
    """Print aggregate golf statistics for all locally stored rounds."""

    storage = _storage()
    resolved_from, resolved_to = _resolve_date_window(
        date_from=date_from,
        date_to=date_to,
        period=period,
    )
    rounds = storage.read_table("rounds")
    holes = storage.read_table("holes")
    shots = _shots_with_configured_club_names(storage.read_table("shots"))
    filtered_rounds, filtered_holes, filtered_shots = _filter_stats_tables(
        rounds,
        holes,
        shots,
        date_from=resolved_from,
        date_to=resolved_to,
    )
    canonical_rounds, _ = _canonicalize_rounds(filtered_rounds)
    summary = build_summary_stats(
        canonical_rounds,
        filtered_holes,
        filtered_shots,
    )
    if json_output:
        _emit_json(summary)
        return
    _render_mapping("Golf Summary", summary)


@stats_app.command("practice-focus")
def stats_practice_focus(
    date_from: str | None = DATE_FROM_OPTION,
    date_to: str | None = DATE_TO_OPTION,
    period: str | None = PERIOD_OPTION,
    json_output: bool = JSON_OPTION,
) -> None:
    """Rank the biggest recurring score leaks to guide practice time."""

    storage = _storage()
    rounds = storage.read_table("rounds")
    if rounds.is_empty():
        if json_output:
            _emit_json(build_practice_focus_stats(pl.DataFrame(), pl.DataFrame(), pl.DataFrame()))
            return
        _console().print("No local rounds found. Run `garmin-golf mirror scorecards ...` first.")
        return

    resolved_from, resolved_to = _resolve_date_window(
        date_from=date_from,
        date_to=date_to,
        period=period,
    )
    holes = storage.read_table("holes")
    shots = _shots_with_configured_club_names(storage.read_table("shots"))
    filtered_rounds, filtered_holes, filtered_shots = _filter_stats_tables(
        rounds,
        holes,
        shots,
        date_from=resolved_from,
        date_to=resolved_to,
    )
    canonical_rounds, _ = _canonicalize_rounds(filtered_rounds)
    focus = build_practice_focus_stats(canonical_rounds, filtered_holes, filtered_shots)
    if json_output:
        _emit_json(focus)
        return
    _render_mapping("Practice Focus", focus)


@stats_app.command("trends")
def stats_trends(
    date_from: str | None = DATE_FROM_OPTION,
    date_to: str | None = DATE_TO_OPTION,
    period: str | None = PERIOD_OPTION,
    window: int = TREND_WINDOW_OPTION,
    json_output: bool = JSON_OPTION,
) -> None:
    """Show rolling trend metrics for each round."""

    if window not in {5, 10, 20}:
        raise typer.BadParameter("--window must be one of: 5, 10, 20.")

    storage = _storage()
    canonical_rounds = _load_canonical_rounds(
        date_from=date_from,
        date_to=date_to,
        period=period,
    )
    if canonical_rounds.is_empty():
        if json_output:
            _emit_json([])
            return
        _console().print("No local rounds found for the selected date window.")
        return

    round_ids = canonical_rounds["round_id"].drop_nulls().to_list()
    holes = _filter_round_table(storage.read_table("holes"), round_ids)
    shots = _filter_round_table(
        _shots_with_configured_club_names(storage.read_table("shots")),
        round_ids,
    )
    trends = build_round_trends(canonical_rounds, holes, shots, window=window)
    if trends.is_empty():
        if json_output:
            _emit_json([])
            return
        _console().print("No round trends are available for this selection yet.")
        return
    if json_output:
        _emit_json(trends)
        return
    _render_trends_table(trends, window=window)


@stats_app.command("second-shots")
def stats_second_shots(
    date_from: str | None = DATE_FROM_OPTION,
    date_to: str | None = DATE_TO_OPTION,
    period: str | None = PERIOD_OPTION,
    json_output: bool = JSON_OPTION,
) -> None:
    """Show second-shot club usage and outcomes on par 4s and par 5s."""

    storage = _storage()
    rounds = storage.read_table("rounds")
    if rounds.is_empty():
        if json_output:
            _emit_json([])
            return
        _console().print("No local rounds found. Run `garmin-golf mirror scorecards ...` first.")
        return

    resolved_from, resolved_to = _resolve_date_window(
        date_from=date_from,
        date_to=date_to,
        period=period,
    )
    holes = storage.read_table("holes")
    shots = _shots_with_configured_club_names(storage.read_table("shots"))
    _, filtered_holes, filtered_shots = _filter_stats_tables(
        rounds,
        holes,
        shots,
        date_from=resolved_from,
        date_to=resolved_to,
    )
    second_shots = build_second_shot_stats(filtered_holes, filtered_shots)
    if second_shots.is_empty():
        if json_output:
            _emit_json([])
            return
        _console().print("No par-4 or par-5 second-shot data is available for this selection.")
        return
    if json_output:
        _emit_json(second_shots)
        return
    _render_second_shots_table(second_shots)


@stats_app.command("clubs")
def stats_clubs(by_context: bool = BY_CONTEXT_OPTION, json_output: bool = JSON_OPTION) -> None:
    """List observed club ids with inferred and configured names."""

    raw_shots = _shots_with_normalized_shot_types(_storage().read_table("shots"))
    if raw_shots.is_empty() or "club_id" not in raw_shots.columns:
        if json_output:
            _emit_json([])
            return
        _console().print(
            "No club data is available yet. "
            "Run `garmin-golf mirror scorecards ...` first."
        )
        return

    resolved_shots = _shots_with_configured_club_names(raw_shots)
    if by_context:
        holes = _storage().read_table("holes")
        context_stats = build_club_context_stats(holes, resolved_shots)
        if context_stats.is_empty():
            if json_output:
                _emit_json([])
                return
            _console().print("No context-aware club data is available for this selection yet.")
            return
        if json_output:
            _emit_json(context_stats)
            return
        _render_club_context_table(context_stats)
        return

    club_inventory = _build_club_inventory_table(raw_shots, resolved_shots)
    if club_inventory.is_empty():
        if json_output:
            _emit_json([])
            return
        _console().print("No club ids were found in the local shot dataset.")
        return
    if json_output:
        _emit_json(club_inventory)
        return
    _render_club_inventory_table(club_inventory)


@stats_app.command("rounds")
def stats_rounds(
    date_from: str | None = DATE_FROM_OPTION,
    date_to: str | None = DATE_TO_OPTION,
    period: str | None = PERIOD_OPTION,
    json_output: bool = JSON_OPTION,
) -> None:
    """List local rounds and round ids for round-level stats lookup."""

    storage = _storage()
    rounds = storage.read_table("rounds")
    if rounds.is_empty():
        if json_output:
            _emit_json([])
            return
        _console().print("No local rounds found. Run `garmin-golf mirror scorecards ...` first.")
        return

    resolved_from, resolved_to = _resolve_date_window(
        date_from=date_from,
        date_to=date_to,
        period=period,
    )
    filtered_rounds, _, _ = _filter_stats_tables(
        rounds,
        pl.DataFrame(),
        pl.DataFrame(),
        date_from=resolved_from,
        date_to=resolved_to,
    )
    canonical_rounds, _ = _canonicalize_rounds(filtered_rounds)
    if canonical_rounds.is_empty():
        if json_output:
            _emit_json([])
            return
        _console().print("No rounds matched the selected date window.")
        return

    display_rounds = _prepare_rounds_for_display(canonical_rounds)
    if json_output:
        _emit_json(display_rounds)
        return
    _render_rounds_table(display_rounds)


@stats_app.command("courses")
def stats_courses(
    date_from: str | None = DATE_FROM_OPTION,
    date_to: str | None = DATE_TO_OPTION,
    period: str | None = PERIOD_OPTION,
    json_output: bool = JSON_OPTION,
) -> None:
    """List locally known courses with round counts."""

    canonical_rounds = _load_canonical_rounds(
        date_from=date_from,
        date_to=date_to,
        period=period,
    )
    if canonical_rounds.is_empty():
        if json_output:
            _emit_json([])
            return
        _console().print("No courses matched the selected date window.")
        return

    courses = _build_courses_table(canonical_rounds)
    if json_output:
        _emit_json(courses)
        return
    _render_courses_table(courses)


@stats_app.command("course")
def stats_course(
    course: str = COURSE_REQUIRED_OPTION,
    date_from: str | None = DATE_FROM_OPTION,
    date_to: str | None = DATE_TO_OPTION,
    period: str | None = PERIOD_OPTION,
    json_output: bool = JSON_OPTION,
) -> None:
    """Print course-specific stats and hole difficulty insights."""

    storage = _storage()
    canonical_rounds = _load_canonical_rounds(
        date_from=date_from,
        date_to=date_to,
        period=period,
    )
    if canonical_rounds.is_empty():
        if json_output:
            _emit_json({"summary": {}, "focus": {}, "holes": []})
            return
        _console().print("No courses matched the selected date window.")
        return

    target_rounds = canonical_rounds.filter(pl.col("display_course_name") == course)
    if target_rounds.is_empty():
        raise typer.BadParameter(
            f"Course not found: {course}. "
            "Use `garmin-golf stats courses` to inspect available names."
        )

    round_ids = target_rounds["round_id"].drop_nulls().to_list()
    holes = storage.read_table("holes")
    shots = _shots_with_configured_club_names(storage.read_table("shots"))
    target_holes = (
        holes.filter(pl.col("round_id").is_in(round_ids))
        if not holes.is_empty() and "round_id" in holes.columns
        else pl.DataFrame()
    )
    target_shots = (
        shots.filter(pl.col("round_id").is_in(round_ids))
        if not shots.is_empty() and "round_id" in shots.columns
        else pl.DataFrame()
    )

    summary = build_summary_stats(target_rounds, target_holes, target_shots)
    hole_stats = build_course_hole_stats(target_rounds, target_holes)
    focus = build_course_focus_stats(hole_stats)
    if json_output:
        _emit_json({"summary": summary, "focus": focus, "holes": hole_stats})
        return

    _render_mapping(f"Course Summary: {course}", summary)
    _render_mapping(f"Next Round Focus: {course}", focus)
    if hole_stats.is_empty():
        _console().print("No hole-level data is available for this course yet.")
        return
    _render_course_holes_table(course, hole_stats)


@stats_app.command("round")
def stats_round(round_id: int = ROUND_ID_REQUIRED_OPTION, json_output: bool = JSON_OPTION) -> None:
    """Print local analysis for one round."""

    storage = _storage()
    rounds = storage.read_table("rounds")
    canonical_rounds, round_aliases = _canonicalize_rounds(rounds)
    resolved_round_id = round_aliases.get(round_id, round_id)
    all_holes = storage.read_table("holes")
    all_shots = _shots_with_configured_club_names(storage.read_table("shots"))
    summary = build_round_stats(
        canonical_rounds,
        all_holes,
        all_shots,
        resolved_round_id,
    )
    round_info = canonical_rounds.filter(pl.col("round_id") == resolved_round_id)
    round_title = _format_round_title(resolved_round_id, round_info)

    holes = (
        all_holes.filter(pl.col("round_id") == resolved_round_id)
        if not all_holes.is_empty() and "round_id" in all_holes.columns
        else pl.DataFrame()
    )
    shots = (
        all_shots.filter(pl.col("round_id") == resolved_round_id)
        if not all_shots.is_empty() and "round_id" in all_shots.columns
        else pl.DataFrame()
    )

    hole_table = _build_round_holes_table(holes)
    club_table = _build_round_clubs_table(shots)
    second_shots = build_second_shot_stats(holes, shots)
    if json_output:
        _emit_json(
            {
                "summary": summary,
                "holes": hole_table,
                "clubs": club_table,
                "second_shots": second_shots,
            }
        )
        return

    _render_mapping(round_title, summary)
    if not hole_table.is_empty():
        _render_round_holes_table(round_title, hole_table)

    if not club_table.is_empty():
        _render_round_clubs_table(round_title, club_table)

    if not second_shots.is_empty():
        _render_second_shots_table(second_shots, title=f"{round_title}: Second Shots")


def _render_mapping(title: str, values: Mapping[str, object]) -> None:
    table = Table(title=title)
    table.add_column("Metric")
    table.add_column("Value", justify="right")
    for key, value in values.items():
        table.add_row(key, _display_value(value))
    _console().print(table)


def _render_rounds_table(rounds: pl.DataFrame) -> None:
    table = Table(title="Local Rounds")
    table.add_column("round_id", justify="right")
    table.add_column("played_on")
    table.add_column("course_name")

    for row in rounds.iter_rows(named=True):
        round_id = row.get("round_id")
        played_on = row.get("played_on")
        course_name = row.get("display_course_name")
        table.add_row(
            _display_value(round_id),
            _display_value(played_on),
            _display_value(course_name),
        )
    _console().print(table)


def _render_courses_table(courses: pl.DataFrame) -> None:
    table = Table(title="Local Courses")
    table.add_column("course_name")
    table.add_column("rounds", justify="right")
    table.add_column("first_played")
    table.add_column("last_played")

    for row in courses.iter_rows(named=True):
        table.add_row(
            _display_value(row.get("course_name")),
            _display_value(row.get("rounds_played")),
            _display_value(row.get("first_played")),
            _display_value(row.get("last_played")),
        )
    _console().print(table)


def _render_course_holes_table(course: str, hole_stats: pl.DataFrame) -> None:
    table = Table(title=f"Course Holes: {course}")
    columns = [
        "hole_number",
        "rounds_played",
        "par",
        "avg_strokes",
        "avg_to_par",
        "par_or_better_pct",
        "bogey_or_worse_pct",
        "double_or_worse_pct",
        "gir_pct",
        "fairway_hit_pct",
        "three_putt_pct",
        "penalty_rate",
    ]
    for column in columns:
        justify = "right" if column != "hole_number" else "right"
        table.add_column(column, justify=justify)

    for row in hole_stats.iter_rows(named=True):
        table.add_row(*[_display_value(row.get(column)) for column in columns])
    _console().print(table)


def _render_second_shots_table(second_shots: pl.DataFrame, *, title: str = "Second Shots") -> None:
    table = Table(title=title)
    columns = [
        "par",
        "club",
        "second_shots",
        "rounds",
        "avg_distance_m",
        "par_or_better_pct",
        "bogey_or_worse_pct",
        "double_or_worse_pct",
        "avg_to_par",
    ]
    for column in columns:
        table.add_column(column, justify="right" if column != "club" else "left")

    for row in second_shots.iter_rows(named=True):
        table.add_row(*[_display_value(row.get(column)) for column in columns])
    _console().print(table)


def _render_trends_table(trends: pl.DataFrame, *, window: int) -> None:
    table = Table(title=f"Round Trends (Last {window})")
    columns = [
        "played_on",
        "course_name",
        "round_to_par",
        "window_average_to_par",
        "delta_average_to_par",
        "window_gir_pct",
        "delta_gir_pct",
        "window_fir_pct",
        "delta_fir_pct",
        "window_penalties_per_18",
        "delta_penalties_per_18",
    ]
    for column in columns:
        justify = "left" if column in {"played_on", "course_name"} else "right"
        table.add_column(column, justify=justify)

    for row in trends.iter_rows(named=True):
        table.add_row(*[_display_value(row.get(column)) for column in columns])
    _console().print(table)


def _render_round_holes_table(title: str, holes: pl.DataFrame) -> None:
    table = Table(title=f"{title}: Holes")
    columns = [
        "hole_number",
        "par",
        "strokes",
        "to_par",
        "putts",
        "gir",
        "fairway_hit",
        "penalties",
    ]
    for column in columns:
        table.add_column(column, justify="right")

    for row in holes.iter_rows(named=True):
        table.add_row(*[_display_value(row.get(column)) for column in columns])
    _console().print(table)


def _render_round_clubs_table(title: str, clubs: pl.DataFrame) -> None:
    table = Table(title=f"{title}: Clubs")
    columns = ["club", "shots", "avg_distance_m", "shot_type_breakdown"]
    for column in columns:
        justify = "right" if column in {"shots", "avg_distance_m"} else "left"
        table.add_column(column, justify=justify)

    for row in clubs.iter_rows(named=True):
        table.add_row(*[_display_value(row.get(column)) for column in columns])
    _console().print(table)


def _render_club_inventory_table(club_inventory: pl.DataFrame) -> None:
    table = Table(title="Clubs")
    columns = [
        "club_id",
        "default_name",
        "configured_name",
        "shots",
        "avg_distance_m",
        "shot_type_breakdown",
        "tee_avg_m",
        "approach_avg_m",
        "layup_avg_m",
        "chip_avg_m",
    ]
    for column in columns:
        justify = (
            "right"
            if column in {"club_id", "shots"} or column.endswith("_avg_m")
            else "left"
        )
        table.add_column(
            column,
            justify=justify,
        )

    for row in club_inventory.iter_rows(named=True):
        table.add_row(*[_display_value(row.get(column)) for column in columns])
    _console().print(table)


def _render_club_context_table(context_stats: pl.DataFrame) -> None:
    table = Table(title="Club Context")
    columns = [
        "club",
        "context",
        "shots",
        "rounds",
        "shot_type",
        "lie",
        "avg_distance_m",
        "par_or_better_pct",
        "bogey_or_worse_pct",
        "avg_to_par",
    ]
    for column in columns:
        justify = "right" if column in {"shots", "rounds", "avg_distance_m"} else "left"
        table.add_column(column, justify=justify)

    for row in context_stats.iter_rows(named=True):
        table.add_row(*[_display_value(row.get(column)) for column in columns])
    _console().print(table)


def _build_club_inventory_table(
    raw_shots: pl.DataFrame, resolved_shots: pl.DataFrame
) -> pl.DataFrame:
    if raw_shots.is_empty() or "club_id" not in raw_shots.columns:
        return pl.DataFrame()

    default_name_column = (
        pl.col("club").cast(pl.String, strict=False).fill_null("Unknown")
        if "club" in raw_shots.columns
        else pl.lit("Unknown")
    )
    configured_name_column = (
        pl.col("club").cast(pl.String, strict=False).fill_null("Unknown")
        if "club" in resolved_shots.columns
        else pl.lit("Unknown")
    )

    trimmed_by_club = trim_distance_outliers(raw_shots, group_columns=["club_id"])
    defaults = raw_shots.group_by("club_id").agg(
        [
            default_name_column.first().alias("default_name"),
            pl.len().alias("shots"),
        ]
    )
    if "distance_meters" in raw_shots.columns:
        defaults = defaults.join(
            trimmed_by_club.group_by("club_id").agg(
                pl.col("distance_meters")
                .cast(pl.Float64, strict=False)
                .mean()
                .round(1)
                .alias("avg_distance_m")
            ),
            on="club_id",
            how="left",
        )
    else:
        defaults = defaults.with_columns(pl.lit(None, dtype=pl.Float64).alias("avg_distance_m"))
    configured = resolved_shots.group_by("club_id").agg(
        configured_name_column.first().alias("configured_name")
    )
    trimmed_by_type = (
        trim_distance_outliers(raw_shots, group_columns=["club_id", "shot_type"])
        if "shot_type" in raw_shots.columns
        else pl.DataFrame()
    )
    by_type = (
        raw_shots.group_by(["club_id", "shot_type"])
        .agg(pl.len().alias("count"))
        .sort(["club_id", "count", "shot_type"], descending=[False, True, False], nulls_last=True)
        if "shot_type" in raw_shots.columns
        else pl.DataFrame()
    )
    if not by_type.is_empty():
        if "distance_meters" in raw_shots.columns and not trimmed_by_type.is_empty():
            by_type = by_type.join(
                trimmed_by_type.group_by(["club_id", "shot_type"]).agg(
                    pl.col("distance_meters")
                    .cast(pl.Float64, strict=False)
                    .mean()
                    .round(1)
                    .alias("avg_distance_m")
                ),
                on=["club_id", "shot_type"],
                how="left",
            )
        else:
            by_type = by_type.with_columns(pl.lit(None, dtype=pl.Float64).alias("avg_distance_m"))
    if not by_type.is_empty():
        by_type_rows: list[dict[str, object]] = []
        for club_id, frame in by_type.partition_by("club_id", as_dict=True).items():
            rows = frame.to_dicts()
            breakdown = ", ".join(
                f"{(row.get('shot_type') or 'Unknown')}: {row.get('count')}" for row in rows[:4]
            )
            distance_by_type = {
                row.get("shot_type"): row.get("avg_distance_m")
                for row in rows
                if isinstance(row.get("shot_type"), str)
            }
            by_type_rows.append(
                {
                    "club_id": club_id[0] if isinstance(club_id, tuple) else club_id,
                    "shot_type_breakdown": breakdown,
                    "tee_avg_m": distance_by_type.get("TEE"),
                    "approach_avg_m": distance_by_type.get("APPROACH"),
                    "layup_avg_m": distance_by_type.get("LAYUP"),
                    "chip_avg_m": distance_by_type.get("CHIP"),
                }
            )
        by_type_summary = pl.DataFrame(by_type_rows)
    else:
        by_type_summary = pl.DataFrame()
    result = defaults.join(configured, on="club_id", how="left")
    if not by_type_summary.is_empty():
        result = result.join(by_type_summary, on="club_id", how="left")
    return (
        result.with_columns(pl.col("configured_name").fill_null(pl.col("default_name")))
        .sort(["shots", "club_id"], descending=[True, False], nulls_last=True)
    )


def _build_round_holes_table(holes: pl.DataFrame) -> pl.DataFrame:
    if holes.is_empty():
        return pl.DataFrame()

    frame = holes.with_columns(
        [
            (
                pl.col("hole_number").cast(pl.Int64, strict=False)
                if "hole_number" in holes.columns
                else pl.lit(None, dtype=pl.Int64)
            ).alias("hole_number"),
            (
                pl.col("par").cast(pl.Int64, strict=False)
                if "par" in holes.columns
                else pl.lit(None, dtype=pl.Int64)
            ).alias("par"),
            (
                pl.col("strokes").cast(pl.Int64, strict=False)
                if "strokes" in holes.columns
                else pl.lit(None, dtype=pl.Int64)
            ).alias("strokes"),
            (
                pl.col("putts").cast(pl.Int64, strict=False)
                if "putts" in holes.columns
                else pl.lit(None, dtype=pl.Int64)
            ).alias("putts"),
            (
                pl.col("penalties").cast(pl.Int64, strict=False).fill_null(0)
                if "penalties" in holes.columns
                else pl.lit(0, dtype=pl.Int64)
            ).alias("penalties"),
            (
                pl.col("gir").cast(pl.Boolean, strict=False)
                if "gir" in holes.columns
                else pl.lit(None, dtype=pl.Boolean)
            ).alias("gir"),
            (
                pl.col("fairway_hit").cast(pl.Boolean, strict=False)
                if "fairway_hit" in holes.columns
                else pl.lit(None, dtype=pl.Boolean)
            ).alias("fairway_hit"),
        ]
    )
    if "par" in holes.columns and "strokes" in holes.columns:
        frame = frame.with_columns(
            (
                pl.col("strokes").cast(pl.Int64, strict=False)
                - pl.col("par").cast(pl.Int64, strict=False)
            ).alias("to_par")
        )
    else:
        frame = frame.with_columns(pl.lit(None, dtype=pl.Int64).alias("to_par"))
    return frame.select(
        ["hole_number", "par", "strokes", "to_par", "putts", "gir", "fairway_hit", "penalties"]
    ).sort("hole_number")


def _build_round_clubs_table(shots: pl.DataFrame) -> pl.DataFrame:
    if shots.is_empty() or "club" not in shots.columns:
        return pl.DataFrame()

    normalized = shots.with_columns(
        [
            pl.col("club").cast(pl.String, strict=False).fill_null("Unknown").alias("club"),
            pl.col("shot_type")
            .cast(pl.String, strict=False)
            .fill_null("UNKNOWN")
            .alias("shot_type"),
        ]
    )
    trimmed = trim_distance_outliers(normalized, group_columns=["club"])
    clubs = normalized.group_by("club").agg(pl.len().alias("shots"))
    if "distance_meters" in normalized.columns:
        clubs = clubs.join(
            trimmed.group_by("club").agg(
                pl.col("distance_meters")
                .cast(pl.Float64, strict=False)
                .mean()
                .round(1)
                .alias("avg_distance_m")
            ),
            on="club",
            how="left",
        )
    else:
        clubs = clubs.with_columns(pl.lit(None, dtype=pl.Float64).alias("avg_distance_m"))

    by_type = (
        normalized.group_by(["club", "shot_type"])
        .agg(pl.len().alias("count"))
        .sort(["club", "count", "shot_type"], descending=[False, True, False], nulls_last=True)
    )
    rows: list[dict[str, object]] = []
    for club, frame in by_type.partition_by("club", as_dict=True).items():
        club_name = club[0] if isinstance(club, tuple) else club
        parts = [f"{row.get('shot_type')}: {row.get('count')}" for row in frame.to_dicts()[:4]]
        rows.append({"club": club_name, "shot_type_breakdown": ", ".join(parts)})
    type_summary = pl.DataFrame(rows) if rows else pl.DataFrame()
    result = (
        clubs.join(type_summary, on="club", how="left")
        if not type_summary.is_empty()
        else clubs
    )
    return result.sort(["shots", "club"], descending=[True, False], nulls_last=True)


def _format_round_title(round_id: int, round_info: pl.DataFrame) -> str:
    if round_info.is_empty():
        return f"Round {round_id}"

    row = round_info.row(0, named=True)
    course = row.get("display_course_name") or row.get("course_name") or row.get("location_name")
    played_on = row.get("played_on")
    suffix_parts = [str(value) for value in (course, played_on) if value]
    if not suffix_parts:
        return f"Round {round_id}"
    return f"Round {round_id} ({' | '.join(suffix_parts)})"


def _display_value(value: object) -> str:
    return "" if value is None else str(value)


def _filter_round_table(frame: pl.DataFrame, round_ids: list[int]) -> pl.DataFrame:
    if frame.is_empty() or "round_id" not in frame.columns or not round_ids:
        return frame.head(0)
    return frame.filter(pl.col("round_id").is_in(round_ids))


def _emit_json(payload: object) -> None:
    typer.echo(json.dumps(_json_ready(payload), ensure_ascii=False, default=str))


def _json_ready(payload: object) -> object:
    if isinstance(payload, pl.DataFrame):
        return payload.to_dicts()
    if isinstance(payload, Mapping):
        return {str(key): _json_ready(value) for key, value in payload.items()}
    if isinstance(payload, list):
        return [_json_ready(item) for item in payload]
    if isinstance(payload, tuple):
        return [_json_ready(item) for item in payload]
    if isinstance(payload, Path):
        return str(payload)
    if isinstance(payload, date | datetime):
        return payload.isoformat()
    return payload


def _build_courses_table(rounds: pl.DataFrame) -> pl.DataFrame:
    if rounds.is_empty() or "display_course_name" not in rounds.columns:
        return pl.DataFrame()
    return (
        rounds.drop_nulls(["display_course_name"])
        .group_by("display_course_name")
        .agg(
            [
                pl.col("round_id").n_unique().alias("rounds_played"),
                pl.col("played_on").drop_nulls().min().alias("first_played"),
                pl.col("played_on").drop_nulls().max().alias("last_played"),
            ]
        )
        .rename({"display_course_name": "course_name"})
        .sort(["rounds_played", "course_name"], descending=[True, False], nulls_last=True)
    )


def _prepare_rounds_for_display(rounds: pl.DataFrame) -> pl.DataFrame:
    if rounds.is_empty():
        return rounds

    frame = rounds.with_columns(
        [
            (
                pl.when(
                    pl.col("course_name").cast(pl.String, strict=False).str.strip_chars() != ""
                )
                .then(pl.col("course_name"))
                .otherwise(pl.col("location_name"))
                .cast(pl.String, strict=False)
                .alias("display_course_name")
                if "course_name" in rounds.columns and "location_name" in rounds.columns
                else (
                    pl.col("course_name").cast(pl.String, strict=False).alias("display_course_name")
                    if "course_name" in rounds.columns
                    else (
                        pl.col("location_name")
                        .cast(pl.String, strict=False)
                        .alias("display_course_name")
                        if "location_name" in rounds.columns
                        else pl.lit("").alias("display_course_name")
                    )
                )
            ),
            (
                pl.col("played_on").str.to_date(strict=False).alias("_played_on_date")
                if "played_on" in rounds.columns
                else pl.lit(None, dtype=pl.Date).alias("_played_on_date")
            ),
            (
                pl.col("round_id").cast(pl.Int64, strict=False).alias("_round_id_sort")
                if "round_id" in rounds.columns
                else pl.lit(None, dtype=pl.Int64).alias("_round_id_sort")
            ),
        ]
    )
    return frame.sort(
        by=["_played_on_date", "_round_id_sort"],
        descending=[True, True],
        nulls_last=True,
    ).select(
        [
            pl.col("round_id") if "round_id" in frame.columns else pl.lit(None).alias("round_id"),
            pl.col("played_on") if "played_on" in frame.columns else pl.lit("").alias("played_on"),
            pl.col("display_course_name"),
        ]
    )


def _load_canonical_rounds(
    *,
    date_from: str | None,
    date_to: str | None,
    period: str | None,
) -> pl.DataFrame:
    storage = _storage()
    rounds = storage.read_table("rounds")
    if rounds.is_empty():
        return rounds
    resolved_from, resolved_to = _resolve_date_window(
        date_from=date_from,
        date_to=date_to,
        period=period,
    )
    filtered_rounds, _, _ = _filter_stats_tables(
        rounds,
        pl.DataFrame(),
        pl.DataFrame(),
        date_from=resolved_from,
        date_to=resolved_to,
    )
    canonical_rounds, _ = _canonicalize_rounds(filtered_rounds)
    if canonical_rounds.is_empty():
        return canonical_rounds
    return canonical_rounds.with_columns(_display_course_expr(canonical_rounds))


def _display_course_expr(rounds: pl.DataFrame) -> pl.Expr:
    if "course_name" in rounds.columns and "location_name" in rounds.columns:
        return (
            pl.when(pl.col("course_name").cast(pl.String, strict=False).str.strip_chars() != "")
            .then(pl.col("course_name"))
            .otherwise(pl.col("location_name"))
            .cast(pl.String, strict=False)
            .alias("display_course_name")
        )
    if "course_name" in rounds.columns:
        return pl.col("course_name").cast(pl.String, strict=False).alias("display_course_name")
    if "location_name" in rounds.columns:
        return pl.col("location_name").cast(pl.String, strict=False).alias("display_course_name")
    return pl.lit("").alias("display_course_name")


def _canonicalize_rounds(rounds: pl.DataFrame) -> tuple[pl.DataFrame, dict[int, int]]:
    if rounds.is_empty() or "round_id" not in rounds.columns:
        return rounds, {}

    scorecard_rows: list[dict[str, object]] = []
    activity_rows: list[dict[str, object]] = []
    scorecards_by_date: dict[str, list[dict[str, object]]] = {}
    for row in rounds.to_dicts():
        if isinstance(row.get("scorecard_id"), int):
            scorecard_rows.append(row)
            played_on = row.get("played_on")
            if isinstance(played_on, str):
                scorecards_by_date.setdefault(played_on, []).append(row)
        elif isinstance(row.get("activity_id"), int):
            activity_rows.append(row)
        else:
            scorecard_rows.append(row)

    round_aliases: dict[int, int] = {}
    activity_by_scorecard_id: dict[int, dict[str, object]] = {}
    used_scorecard_ids: set[int] = set()

    for activity_row in activity_rows:
        match = _find_matching_scorecard(activity_row, scorecards_by_date, used_scorecard_ids)
        if match is None:
            continue
        activity_round_id = activity_row.get("round_id")
        scorecard_round_id = match.get("round_id")
        if isinstance(activity_round_id, int) and isinstance(scorecard_round_id, int):
            round_aliases[activity_round_id] = scorecard_round_id
            used_scorecard_ids.add(scorecard_round_id)
            activity_by_scorecard_id[scorecard_round_id] = activity_row

    canonical_rows: list[dict[str, object]] = []
    for scorecard_row in scorecard_rows:
        scorecard_round_id = scorecard_row.get("round_id")
        if isinstance(scorecard_round_id, int) and scorecard_round_id in activity_by_scorecard_id:
            canonical_rows.append(
                _merge_scorecard_and_activity_round(
                    scorecard_row,
                    activity_by_scorecard_id[scorecard_round_id],
                )
            )
        else:
            canonical_rows.append(scorecard_row)

    for activity_row in activity_rows:
        activity_round_id = activity_row.get("round_id")
        if not isinstance(activity_round_id, int) or activity_round_id not in round_aliases:
            canonical_rows.append(activity_row)

    if not canonical_rows:
        return rounds.head(0), round_aliases
    return pl.DataFrame(canonical_rows, schema=rounds.schema), round_aliases


def _find_matching_scorecard(
    activity_row: dict[str, object],
    scorecards_by_date: dict[str, list[dict[str, object]]],
    used_scorecard_ids: set[int],
) -> dict[str, object] | None:
    played_on = activity_row.get("played_on")
    if not isinstance(played_on, str):
        return None

    candidates = [
        candidate
        for candidate in scorecards_by_date.get(played_on, [])
        if isinstance(candidate.get("round_id"), int)
        and candidate["round_id"] not in used_scorecard_ids
    ]
    if not candidates:
        return None

    same_player = [
        candidate for candidate in candidates if _same_player_profile(candidate, activity_row)
    ]
    if same_player:
        candidates = same_player

    activity_start = _parse_round_start_time(activity_row.get("start_time"))
    timed_candidates: list[tuple[timedelta, dict[str, object]]] = []
    for candidate in candidates:
        candidate_start = _parse_round_start_time(candidate.get("start_time"))
        if activity_start is None or candidate_start is None:
            continue
        time_delta = abs(activity_start - candidate_start)
        if time_delta <= ROUND_MATCH_TOLERANCE:
            timed_candidates.append((time_delta, candidate))

    if timed_candidates:
        timed_candidates.sort(key=lambda item: item[0])
        return timed_candidates[0][1]

    return candidates[0] if len(candidates) == 1 else None


def _merge_scorecard_and_activity_round(
    scorecard_row: dict[str, object],
    activity_row: dict[str, object],
) -> dict[str, object]:
    merged = dict(activity_row)
    merged.update(scorecard_row)
    if merged.get("activity_id") is None:
        merged["activity_id"] = activity_row.get("activity_id")
    if not merged.get("location_name"):
        merged["location_name"] = activity_row.get("location_name")
    merged["data_source"] = "scorecard+activity"
    return merged


def _same_player_profile(left: dict[str, object], right: dict[str, object]) -> bool:
    left_profile_id = left.get("player_profile_id")
    right_profile_id = right.get("player_profile_id")
    if isinstance(left_profile_id, int) and isinstance(right_profile_id, int):
        return left_profile_id == right_profile_id
    return True


def _parse_round_start_time(value: object) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    normalized = value.strip()
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if parsed.tzinfo is not None:
        return parsed.astimezone().replace(tzinfo=None)
    return parsed


def _parse_optional_date(value: str | None, option_name: str) -> date | None:
    if value is None:
        return None
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise typer.BadParameter(
            f"{option_name} must be an ISO date like 2025-06-01."
        ) from exc


def _resolve_date_window(
    *,
    date_from: str | None,
    date_to: str | None,
    period: str | None,
    today: date | None = None,
) -> tuple[date | None, date | None]:
    parsed_from = _parse_optional_date(date_from, "--from")
    parsed_to = _parse_optional_date(date_to, "--to")
    if period is None:
        return parsed_from, parsed_to
    if parsed_from is not None or parsed_to is not None:
        raise typer.BadParameter("Use either --period or --from/--to, not both.")

    current_day = today or date.today()
    if period == "last-12-months":
        return _months_back_window(current_day, 12)
    if period == "this-year":
        return date(current_day.year, 1, 1), current_day
    if period == "last-year":
        return date(current_day.year - 1, 1, 1), date(current_day.year - 1, 12, 31)
    raise typer.BadParameter("Unsupported --period. Use last-12-months, this-year, or last-year.")


def _months_back_window(today: date, months: int) -> tuple[date, date]:
    year = today.year
    month = today.month - months
    while month <= 0:
        year -= 1
        month += 12
    start_day = min(today.day, _days_in_month(year, month))
    return date(year, month, start_day) + timedelta(days=1), today


def _days_in_month(year: int, month: int) -> int:
    next_month = date(year + 1, 1, 1) if month == 12 else date(year, month + 1, 1)
    return (next_month - date(year, month, 1)).days


def _filter_stats_tables(
    rounds: pl.DataFrame,
    holes: pl.DataFrame,
    shots: pl.DataFrame,
    *,
    date_from: date | None,
    date_to: date | None,
) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    if rounds.is_empty() or (date_from is None and date_to is None):
        return rounds, holes, shots

    filtered_rounds = rounds
    if "played_on" in rounds.columns:
        filtered_rounds = rounds.with_columns(
            pl.col("played_on").str.to_date(strict=False).alias("_played_on_date")
        )
        if date_from is not None:
            filtered_rounds = filtered_rounds.filter(pl.col("_played_on_date") >= pl.lit(date_from))
        if date_to is not None:
            filtered_rounds = filtered_rounds.filter(pl.col("_played_on_date") <= pl.lit(date_to))
        filtered_rounds = filtered_rounds.drop("_played_on_date")

    if filtered_rounds.is_empty() or "round_id" not in filtered_rounds.columns:
        return filtered_rounds, holes.clear(), shots.clear()

    round_ids = filtered_rounds["round_id"].drop_nulls().to_list()
    filtered_holes = (
        holes.filter(pl.col("round_id").is_in(round_ids)) if not holes.is_empty() else holes
    )
    filtered_shots = (
        shots.filter(pl.col("round_id").is_in(round_ids)) if not shots.is_empty() else shots
    )
    return filtered_rounds, filtered_holes, filtered_shots

def main() -> None:
    app()
