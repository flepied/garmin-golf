from __future__ import annotations

import json
from datetime import date
from typing import Any

from .models import JsonDict


CLUB_TYPE_NAMES: dict[int, str] = {
    1: "Driver",
    2: "3 Wood",
    3: "5 Wood",
    6: "7 Wood",
    7: "Hybrid",
    13: "5 Iron",
    14: "6 Iron",
    15: "7 Iron",
    16: "8 Iron",
    17: "9 Iron",
    18: "Pitching Wedge",
    19: "Gap Wedge",
    20: "Sand Wedge",
    21: "Lob Wedge",
    23: "Putter",
}


def normalize_round(summary: JsonDict, detail: JsonDict) -> dict[str, Any]:
    scorecard = extract_scorecard(detail)
    round_id = scorecard.get("id", summary.get("id"))
    start_time = scorecard.get("startTime", summary.get("startTime"))
    played_on = parse_round_date(str(start_time) if start_time is not None else None)
    hole_pars = extract_hole_pars(summary, detail)

    total_score = _coalesce_int(
        scorecard.get("totalScore"),
        scorecard.get("strokes"),
        summary.get("totalScore"),
        summary.get("strokes"),
        summary.get("score"),
    )
    total_par = _coalesce_int(
        scorecard.get("totalPar"),
        summary.get("totalPar"),
        _nested_get(detail, "courseSnapshots", 0, "roundPar"),
        sum(hole_pars) if hole_pars else None,
    )
    course_name = _coalesce_str(
        scorecard.get("courseName"),
        summary.get("courseName"),
        _nested_get(scorecard, "course", "courseName"),
        _nested_get(detail, "courseSnapshots", 0, "name"),
    )
    tee_name = _coalesce_str(
        scorecard.get("teeName"),
        _nested_get(scorecard, "tee", "name"),
        scorecard.get("teeBox"),
    )

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
        "summary_json": _json_dumps(summary),
        "scorecard_json": _json_dumps(scorecard),
    }


def normalize_holes(round_id: int, detail: JsonDict) -> list[dict[str, Any]]:
    scorecard = extract_scorecard(detail)
    holes = scorecard.get("holes")
    if not isinstance(holes, list):
        return []
    hole_pars = extract_hole_pars({}, detail)

    rows: list[dict[str, Any]] = []
    for index, hole in enumerate(holes, start=1):
        if not isinstance(hole, dict):
            continue
        strokes = _coalesce_int(hole.get("strokes"), hole.get("score"))
        par = _coalesce_int(
            hole.get("par"),
            hole_pars[index - 1] if index - 1 < len(hole_pars) else None,
        )
        putts = _coalesce_int(hole.get("putts"))
        fairway_shot_outcome = _coalesce_str(hole.get("fairwayShotOutcome"))
        fairway_hit = _coalesce_bool(
            hole.get("fairwayHit"),
            _nested_get(hole, "teeShot", "fairwayHit"),
            _fairway_hit_from_outcome(fairway_shot_outcome, par),
        )
        gir = _coalesce_bool(
            hole.get("greenInRegulation"),
            hole.get("gir"),
            _gir_from_strokes(strokes, putts, par),
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
                "fairway_shot_outcome": fairway_shot_outcome,
                "handicap_score": _coalesce_int(hole.get("handicapScore")),
                "pin_position_lat": _coalesce_float(hole.get("pinPositionLat")),
                "pin_position_lon": _coalesce_float(hole.get("pinPositionLon")),
                "last_modified": _coalesce_str(hole.get("lastModifiedDt")),
                "hole_json": _json_dumps(hole),
            }
        )
    return rows


def normalize_shots(round_id: int, hole_number: int, payload: JsonDict) -> list[dict[str, Any]]:
    club_lookup = _club_lookup(payload)
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
                "shot_id": _coalesce_int(shot.get("id")),
                "scorecard_id": _coalesce_int(shot.get("scorecardId"), default=round_id),
                "player_profile_id": _coalesce_int(shot.get("playerProfileId")),
                "shot_order": _coalesce_int(shot.get("shotOrder"), default=index),
                "club": _coalesce_str(
                    shot.get("club"),
                    _nested_get(shot, "club", "name"),
                    club_lookup.get(_coalesce_int(shot.get("clubId"))),
                ),
                "club_id": _coalesce_int(shot.get("clubId")),
                "distance_meters": _coalesce_float(
                    shot.get("meters"),
                    shot.get("distance"),
                    shot.get("distanceMeters"),
                    _nested_get(shot, "distance", "meters"),
                ),
                "lie": _coalesce_str(shot.get("lie")),
                "result": _coalesce_str(shot.get("result")),
                "shot_type": _coalesce_str(shot.get("shotType")),
                "auto_shot_type": _coalesce_str(shot.get("autoShotType")),
                "shot_source": _coalesce_str(shot.get("shotSource")),
                "shot_time": _coalesce_str(shot.get("shotTime")),
                "shot_time_zone_offset": _coalesce_int(shot.get("shotTimeZoneOffset")),
                "start_lat": _coalesce_float(
                    _nested_get(shot, "startLoc", "lat"),
                    _nested_get(shot, "startLocation", "lat"),
                    _nested_get(shot, "startCoordinate", "latitude"),
                ),
                "start_lon": _coalesce_float(
                    _nested_get(shot, "startLoc", "lon"),
                    _nested_get(shot, "startLocation", "lon"),
                    _nested_get(shot, "startCoordinate", "longitude"),
                ),
                "start_x": _coalesce_int(_nested_get(shot, "startLoc", "x")),
                "start_y": _coalesce_int(_nested_get(shot, "startLoc", "y")),
                "end_lat": _coalesce_float(
                    _nested_get(shot, "endLoc", "lat"),
                    _nested_get(shot, "endLocation", "lat"),
                    _nested_get(shot, "endCoordinate", "latitude"),
                ),
                "end_lon": _coalesce_float(
                    _nested_get(shot, "endLoc", "lon"),
                    _nested_get(shot, "endLocation", "lon"),
                    _nested_get(shot, "endCoordinate", "longitude"),
                ),
                "end_x": _coalesce_int(_nested_get(shot, "endLoc", "x")),
                "end_y": _coalesce_int(_nested_get(shot, "endLoc", "y")),
                "shot_json": _json_dumps(shot),
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


def extract_hole_pars(summary: JsonDict, detail: JsonDict) -> list[int]:
    raw_hole_pars = _coalesce_str(
        summary.get("holePars"),
        _nested_get(detail, "courseSnapshots", 0, "holePars"),
    )
    if raw_hole_pars:
        return [int(value) for value in raw_hole_pars if value.isdigit()]

    scorecard = extract_scorecard(detail)
    holes = scorecard.get("holes")
    if not isinstance(holes, list):
        return []

    parsed: list[int] = []
    for hole in holes:
        if not isinstance(hole, dict):
            continue
        par = _coalesce_int(hole.get("par"))
        if par is not None:
            parsed.append(par)
    return parsed


def _club_lookup(payload: JsonDict) -> dict[int, str]:
    club_details = payload.get("clubDetails")
    if not isinstance(club_details, list):
        return {}

    lookup: dict[int, str] = {}
    for detail in club_details:
        if not isinstance(detail, dict):
            continue
        club_id = _coalesce_int(detail.get("id"))
        club_type_id = _coalesce_int(detail.get("clubTypeId"))
        if club_id is None or club_type_id is None:
            continue
        club_name = CLUB_TYPE_NAMES.get(club_type_id)
        if club_name is not None:
            lookup[club_id] = club_name
    return lookup


def _nested_get(data: Any, *keys: Any) -> Any:
    current: Any = data
    for key in keys:
        if isinstance(key, int):
            if not isinstance(current, list) or key >= len(current):
                return None
            current = current[key]
            continue
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
        if isinstance(value, int) and value in {0, 1}:
            return bool(value)
        if isinstance(value, str):
            lowered = value.lower()
            if lowered in {"true", "yes", "1"}:
                return True
            if lowered in {"false", "no", "0"}:
                return False
    return None


def _fairway_hit_from_outcome(outcome: str | None, par: int | None) -> bool | None:
    if par == 3:
        return None
    if not outcome:
        return None
    normalized = outcome.upper()
    if normalized == "HIT":
        return True
    if normalized in {"LEFT", "RIGHT", "SHORT", "LONG"}:
        return False
    return None


def _gir_from_strokes(strokes: int | None, putts: int | None, par: int | None) -> bool | None:
    if strokes is None or putts is None or par is None:
        return None
    return (strokes - putts) <= (par - 2)


def _json_dumps(value: Any) -> str | None:
    if value is None:
        return None
    try:
        return json.dumps(value, sort_keys=True, separators=(",", ":"))
    except TypeError:
        return json.dumps(value, sort_keys=True, default=str, separators=(",", ":"))


def parse_round_date(value: str | None) -> date | None:
    if not value:
        return None
    try:
        return date.fromisoformat(value[:10])
    except ValueError:
        return None
