from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from .models import JsonDict
from .normalize import normalize_holes, normalize_round, normalize_shots
from .storage import Storage


@dataclass(slots=True)
class BrowserImportResult:
    rounds_imported: int = 0
    holes_imported: int = 0
    shots_imported: int = 0


def import_browser_export_payload(
    storage: Storage,
    payload: JsonDict,
    *,
    snapshot_relative_path: Path | None = None,
) -> BrowserImportResult:
    summary = payload.get("summary")
    details = payload.get("details")
    shots = payload.get("shots")

    if (
        not isinstance(summary, dict)
        or not isinstance(details, list)
        or not isinstance(shots, list)
    ):
        raise ValueError("Unexpected browser export format.")

    scorecard_summaries = summary.get("scorecardSummaries")
    if not isinstance(scorecard_summaries, list):
        raise ValueError("Browser export is missing summary.scorecardSummaries.")

    summaries_by_id: dict[int, JsonDict] = {}
    for item in scorecard_summaries:
        if isinstance(item, dict) and isinstance(item.get("id"), int):
            summaries_by_id[item["id"]] = item

    details_by_id: dict[int, JsonDict] = {}
    for item in details:
        if not isinstance(item, dict):
            continue
        scorecard_id = extract_scorecard_id(item)
        if scorecard_id is not None:
            details_by_id[scorecard_id] = item

    shots_by_id: dict[int, JsonDict] = {}
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

    storage.upsert_rows("rounds", round_rows, unique_by=["round_id"])
    storage.upsert_rows("holes", hole_rows, unique_by=["round_id", "hole_number"])
    storage.upsert_rows("shots", shot_rows, unique_by=["round_id", "hole_number", "shot_number"])
    if snapshot_relative_path is not None:
        storage.write_json_snapshot(snapshot_relative_path, payload)

    return BrowserImportResult(
        rounds_imported=len(round_rows),
        holes_imported=len(hole_rows),
        shots_imported=len(shot_rows),
    )


def extract_scorecard_id(detail_payload: JsonDict) -> int | None:
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
