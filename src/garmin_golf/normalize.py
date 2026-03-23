from __future__ import annotations

from typing import Any

from .client import parse_round_date
from .models import JsonDict


def normalize_round(summary: JsonDict, detail: JsonDict) -> dict[str, Any]:
    scorecard = extract_scorecard(detail)
    round_id = scorecard.get("id", summary.get("id"))
    start_time = scorecard.get("startTime", summary.get("startTime"))
    played_on = parse_round_date(str(start_time) if start_time is not None else None)

    total_score = _coalesce_int(
        scorecard.get("totalScore"),
        summary.get("totalScore"),
        summary.get("score"),
    )
    total_par = _coalesce_int(scorecard.get("totalPar"), summary.get("totalPar"))
    course_name = _coalesce_str(
        scorecard.get("courseName"),
        summary.get("courseName"),
        _nested_get(scorecard, "course", "courseName"),
    )
    tee_name = _coalesce_str(scorecard.get("teeName"), _nested_get(scorecard, "tee", "name"))

    return {
        "round_id": round_id,
        "scorecard_id": round_id,
        "played_on": played_on.isoformat() if played_on else None,
        "start_time": start_time,
        "course_name": course_name,
        "tee_name": tee_name,
        "total_score": total_score,
        "total_par": total_par,
        "strokes_gained_handicap": scorecard.get("strokesGainedHandicap"),
        "player_profile_id": scorecard.get("playerProfileId", summary.get("playerProfileId")),
    }


def normalize_round_from_activity(summary: JsonDict, detail: JsonDict) -> dict[str, Any]:
    activity_id = _coalesce_int(summary.get("activityId"), detail.get("activityId"))
    summary_dto_value = detail.get("summaryDTO")
    summary_dto: JsonDict = summary_dto_value if isinstance(summary_dto_value, dict) else {}
    metadata_dto_value = detail.get("metadataDTO")
    metadata_dto: JsonDict = metadata_dto_value if isinstance(metadata_dto_value, dict) else {}
    start_time_local = _coalesce_str(
        summary_dto.get("startTimeLocal"),
        summary.get("startTimeLocal"),
    )
    played_on = parse_round_date(
        start_time_local
    )
    activity_name = _coalesce_str(detail.get("activityName"), summary.get("activityName"))
    location_name = _coalesce_str(detail.get("locationName"), summary.get("locationName"))

    return {
        "round_id": activity_id,
        "scorecard_id": None,
        "activity_id": activity_id,
        "played_on": played_on.isoformat() if played_on else None,
        "start_time": start_time_local,
        "course_name": activity_name,
        "tee_name": None,
        "location_name": location_name,
        "total_score": None,
        "total_par": None,
        "distance_meters": _coalesce_float(summary_dto.get("distance"), summary.get("distance")),
        "duration_seconds": _coalesce_float(summary_dto.get("duration"), summary.get("duration")),
        "moving_duration_seconds": _coalesce_float(summary_dto.get("movingDuration")),
        "elapsed_duration_seconds": _coalesce_float(summary_dto.get("elapsedDuration")),
        "calories": _coalesce_float(summary_dto.get("calories")),
        "average_hr": _coalesce_float(summary_dto.get("averageHR")),
        "max_hr": _coalesce_float(summary_dto.get("maxHR")),
        "device_id": _nested_get(metadata_dto, "deviceMetaDataDTO", "deviceId"),
        "player_profile_id": detail.get("userProfileId", summary.get("userProfileId")),
        "data_source": "activities",
    }


def normalize_holes(round_id: int, detail: JsonDict) -> list[dict[str, Any]]:
    scorecard = extract_scorecard(detail)
    holes = scorecard.get("holes")
    if not isinstance(holes, list):
        return []

    rows: list[dict[str, Any]] = []
    for index, hole in enumerate(holes, start=1):
        if not isinstance(hole, dict):
            continue
        strokes = _coalesce_int(hole.get("strokes"), hole.get("score"))
        par = _coalesce_int(hole.get("par"))
        putts = _coalesce_int(hole.get("putts"))
        fairway_hit = _coalesce_bool(
            hole.get("fairwayHit"),
            _nested_get(hole, "teeShot", "fairwayHit"),
        )
        gir = _coalesce_bool(
            hole.get("greenInRegulation"),
            hole.get("gir"),
        )
        penalties = _coalesce_int(hole.get("penalties"), hole.get("penaltyStrokes"), default=0)
        rows.append(
            {
                "round_id": round_id,
                "hole_number": _coalesce_int(hole.get("number"), default=index),
                "par": par,
                "strokes": strokes,
                "putts": putts,
                "fairway_hit": fairway_hit,
                "gir": gir,
                "penalties": penalties,
            }
        )
    return rows


def normalize_shots(round_id: int, hole_number: int, payload: JsonDict) -> list[dict[str, Any]]:
    hole_shots = payload.get("holeShots")
    if isinstance(hole_shots, list) and hole_shots:
        hole_payload = next(
            (
                item
                for item in hole_shots
                if isinstance(item, dict) and _coalesce_int(item.get("holeNumber")) == hole_number
            ),
            {},
        )
    elif isinstance(hole_shots, dict):
        hole_payload = hole_shots
    else:
        hole_payload = payload

    shots = hole_payload.get("shots")
    if not isinstance(shots, list):
        return []

    rows: list[dict[str, Any]] = []
    for index, shot in enumerate(shots, start=1):
        if not isinstance(shot, dict):
            continue
        rows.append(
            {
                "round_id": round_id,
                "hole_number": hole_number,
                "shot_number": _coalesce_int(shot.get("shotNumber"), default=index),
                "club": _coalesce_str(shot.get("club"), _nested_get(shot, "club", "name")),
                "distance_meters": _coalesce_float(
                    shot.get("distance"),
                    shot.get("distanceMeters"),
                    _nested_get(shot, "distance", "meters"),
                ),
                "lie": _coalesce_str(shot.get("lie")),
                "result": _coalesce_str(shot.get("result")),
                "start_lat": _coalesce_float(
                    _nested_get(shot, "startLocation", "lat"),
                    _nested_get(shot, "startCoordinate", "latitude"),
                ),
                "start_lon": _coalesce_float(
                    _nested_get(shot, "startLocation", "lon"),
                    _nested_get(shot, "startCoordinate", "longitude"),
                ),
                "end_lat": _coalesce_float(
                    _nested_get(shot, "endLocation", "lat"),
                    _nested_get(shot, "endCoordinate", "latitude"),
                ),
                "end_lon": _coalesce_float(
                    _nested_get(shot, "endLocation", "lon"),
                    _nested_get(shot, "endCoordinate", "longitude"),
                ),
            }
        )
    return rows


def extract_scorecard(detail: JsonDict) -> JsonDict:
    scorecard_details = detail.get("scorecardDetails")
    if isinstance(scorecard_details, list):
        for item in scorecard_details:
            if isinstance(item, dict):
                scorecard = item.get("scorecard")
                if isinstance(scorecard, dict):
                    return scorecard
    scorecard = detail.get("scorecard")
    if isinstance(scorecard, dict):
        return scorecard
    return detail


def _nested_get(data: JsonDict, *keys: str) -> Any:
    current: Any = data
    for key in keys:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current


def _coalesce_str(*values: Any) -> str | None:
    for value in values:
        if isinstance(value, str) and value:
            return value
    return None


def _coalesce_int(*values: Any, default: int | None = None) -> int | None:
    for value in values:
        if value is None:
            continue
        try:
            return int(value)
        except (TypeError, ValueError):
            continue
    return default


def _coalesce_float(*values: Any) -> float | None:
    for value in values:
        if value is None:
            continue
        try:
            return float(value)
        except (TypeError, ValueError):
            continue
    return None


def _coalesce_bool(*values: Any) -> bool | None:
    for value in values:
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            lowered = value.lower()
            if lowered in {"true", "yes", "1"}:
                return True
            if lowered in {"false", "no", "0"}:
                return False
    return None
