from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl

from .client import GarminGolfClient
from .fit_parser import inspect_activity_archive
from .models import SyncResult
from .normalize import (
    normalize_holes,
    normalize_round,
    normalize_round_from_activity,
    normalize_shots,
)
from .storage import Storage


def sync_rounds(
    client: GarminGolfClient,
    storage: Storage,
    *,
    date_from: date | None = None,
    date_to: date | None = None,
) -> SyncResult:
    result = SyncResult()
    summaries = client.list_scorecards()
    storage.write_json_snapshot(Path("scorecards/summary.json"), {"scorecardSummaries": summaries})
    result.raw_files_written += 1

    if not summaries:
        return _sync_rounds_from_activities(
            client,
            storage,
            date_from=date_from,
            date_to=date_to,
            result=result,
        )

    round_rows: list[dict[str, object]] = []
    hole_rows: list[dict[str, object]] = []

    for summary in summaries:
        scorecard_id = summary.get("id")
        if not isinstance(scorecard_id, int):
            continue
        detail = client.get_scorecard_detail(scorecard_id)
        storage.write_json_snapshot(Path("scorecards") / f"{scorecard_id}.json", detail)
        result.raw_files_written += 1

        round_row = normalize_round(summary, detail)
        played_on = round_row.get("played_on")
        if not _in_date_range(played_on, date_from=date_from, date_to=date_to):
            continue
        round_rows.append(round_row)
        hole_rows.extend(normalize_holes(scorecard_id, detail))

    rounds = storage.upsert_rows("rounds", round_rows, unique_by=["round_id"])
    holes = storage.upsert_rows("holes", hole_rows, unique_by=["round_id", "hole_number"])
    result.rounds_synced = rounds.height
    result.holes_synced = holes.height
    return result


def _sync_rounds_from_activities(
    client: GarminGolfClient,
    storage: Storage,
    *,
    date_from: date | None,
    date_to: date | None,
    result: SyncResult,
) -> SyncResult:
    activities = client.list_golf_activities(date_from=date_from, date_to=date_to)
    storage.write_json_snapshot(Path("activities/golf-summary.json"), {"activities": activities})
    result.raw_files_written += 1

    round_rows: list[dict[str, object]] = []
    for activity in activities:
        activity_id = activity.get("activityId")
        if not isinstance(activity_id, int):
            continue
        detail = client.get_activity_detail(activity_id)
        storage.write_json_snapshot(Path("activities") / f"{activity_id}.json", detail)
        result.raw_files_written += 1
        try:
            original_archive = client.download_activity_original(activity_id)
        except Exception:
            original_archive = None
        if original_archive is not None:
            storage.write_bytes_snapshot(
                Path("activities") / f"{activity_id}.zip",
                original_archive,
            )
            result.raw_files_written += 1
            fit_inspection = inspect_activity_archive(original_archive)
            storage.write_json_snapshot(
                Path("activities") / f"{activity_id}.fit.json",
                fit_inspection.as_dict(),
            )
            result.raw_files_written += 1
        round_rows.append(normalize_round_from_activity(activity, detail))

    rounds = storage.upsert_rows("rounds", round_rows, unique_by=["round_id"])
    result.rounds_synced = rounds.height
    return result


def sync_shots(
    client: GarminGolfClient,
    storage: Storage,
    *,
    round_ids: list[int] | None = None,
) -> SyncResult:
    result = SyncResult()
    holes = storage.read_table("holes")
    if holes.is_empty():
        return result

    target_holes = holes
    if round_ids:
        target_holes = holes.filter(pl.col("round_id").is_in(round_ids))

    shot_rows: list[dict[str, object]] = []
    for row in target_holes.select("round_id", "hole_number").iter_rows(named=True):
        round_id = row["round_id"]
        hole_number = row["hole_number"]
        if not isinstance(round_id, int) or not isinstance(hole_number, int):
            continue
        payload = client.get_hole_shots(round_id, hole_number)
        storage.write_json_snapshot(
            Path("shots") / f"{round_id}" / f"hole-{hole_number}.json",
            payload,
        )
        result.raw_files_written += 1
        shot_rows.extend(normalize_shots(round_id, hole_number, payload))

    shots = storage.upsert_rows(
        "shots",
        shot_rows,
        unique_by=["round_id", "hole_number", "shot_number"],
    )
    result.shots_synced = shots.height
    return result


def _in_date_range(
    played_on: object,
    *,
    date_from: date | None,
    date_to: date | None,
) -> bool:
    if not isinstance(played_on, str):
        return date_from is None and date_to is None
    try:
        parsed = date.fromisoformat(played_on)
    except ValueError:
        return True
    if date_from is not None and parsed < date_from:
        return False
    return not (date_to is not None and parsed > date_to)
