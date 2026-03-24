from __future__ import annotations

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
    build_course_focus_stats,
    build_course_hole_stats,
    build_round_stats,
    build_summary_stats,
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
COURSE_REQUIRED_OPTION = typer.Option(..., "--course", help="Exact course name to analyze.")
ROUND_MATCH_TOLERANCE = timedelta(hours=2)


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
    shots = storage.read_table("shots")
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
    _render_mapping("Golf Summary", summary)


@stats_app.command("rounds")
def stats_rounds(
    date_from: str | None = DATE_FROM_OPTION,
    date_to: str | None = DATE_TO_OPTION,
    period: str | None = PERIOD_OPTION,
) -> None:
    """List local rounds and round ids for round-level stats lookup."""

    storage = _storage()
    rounds = storage.read_table("rounds")
    if rounds.is_empty():
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
        _console().print("No rounds matched the selected date window.")
        return

    _render_rounds_table(canonical_rounds)


@stats_app.command("courses")
def stats_courses(
    date_from: str | None = DATE_FROM_OPTION,
    date_to: str | None = DATE_TO_OPTION,
    period: str | None = PERIOD_OPTION,
) -> None:
    """List locally known courses with round counts."""

    canonical_rounds = _load_canonical_rounds(
        date_from=date_from,
        date_to=date_to,
        period=period,
    )
    if canonical_rounds.is_empty():
        _console().print("No courses matched the selected date window.")
        return

    _render_courses_table(canonical_rounds)


@stats_app.command("course")
def stats_course(
    course: str = COURSE_REQUIRED_OPTION,
    date_from: str | None = DATE_FROM_OPTION,
    date_to: str | None = DATE_TO_OPTION,
    period: str | None = PERIOD_OPTION,
) -> None:
    """Print course-specific stats and hole difficulty insights."""

    storage = _storage()
    canonical_rounds = _load_canonical_rounds(
        date_from=date_from,
        date_to=date_to,
        period=period,
    )
    if canonical_rounds.is_empty():
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
    shots = storage.read_table("shots")
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

    _render_mapping(f"Course Summary: {course}", summary)
    _render_mapping(f"Next Round Focus: {course}", focus)
    if hole_stats.is_empty():
        _console().print("No hole-level data is available for this course yet.")
        return
    _render_course_holes_table(course, hole_stats)


@stats_app.command("round")
def stats_round(round_id: int = ROUND_ID_REQUIRED_OPTION) -> None:
    """Print local statistics for one round."""

    storage = _storage()
    rounds = storage.read_table("rounds")
    canonical_rounds, round_aliases = _canonicalize_rounds(rounds)
    resolved_round_id = round_aliases.get(round_id, round_id)
    summary = build_round_stats(
        canonical_rounds,
        storage.read_table("holes"),
        storage.read_table("shots"),
        resolved_round_id,
    )
    _render_mapping(f"Round {resolved_round_id}", summary)


def _render_mapping(title: str, values: Mapping[str, object]) -> None:
    table = Table(title=title)
    table.add_column("Metric")
    table.add_column("Value", justify="right")
    for key, value in values.items():
        table.add_row(key, str(value))
    _console().print(table)


def _render_rounds_table(rounds: pl.DataFrame) -> None:
    table = Table(title="Local Rounds")
    table.add_column("round_id", justify="right")
    table.add_column("played_on")
    table.add_column("course_name")

    display_rounds = _prepare_rounds_for_display(rounds)
    for row in display_rounds.iter_rows(named=True):
        round_id = row.get("round_id")
        played_on = row.get("played_on")
        course_name = row.get("display_course_name")
        table.add_row(
            "" if round_id is None else str(round_id),
            "" if played_on is None else str(played_on),
            "" if course_name is None else str(course_name),
        )
    _console().print(table)


def _render_courses_table(rounds: pl.DataFrame) -> None:
    table = Table(title="Local Courses")
    table.add_column("course_name")
    table.add_column("rounds", justify="right")
    table.add_column("first_played")
    table.add_column("last_played")

    for row in _build_courses_table(rounds).iter_rows(named=True):
        table.add_row(
            str(row.get("course_name") or ""),
            str(row.get("rounds_played") or ""),
            str(row.get("first_played") or ""),
            str(row.get("last_played") or ""),
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
        table.add_row(*[str(row.get(column) or "") for column in columns])
    _console().print(table)


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
