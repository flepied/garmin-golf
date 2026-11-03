"""Observed GPS directions for a round; no weather or intended targets inferred."""

from __future__ import annotations

import math
from typing import Any

import polars as pl

from .analysis.engine import _coordinate_degrees, _haversine

Point = tuple[float, float]


def _point(row: dict[str, Any], lat_key: str, lon_key: str) -> Point | None:
    lat, lon = row.get(lat_key), row.get(lon_key)
    if not isinstance(lat, int | float) or not isinstance(lon, int | float):
        return None
    if not math.isfinite(lat) or not math.isfinite(lon):
        return None
    latitude = _coordinate_degrees(lat, latitude=True)
    longitude = _coordinate_degrees(lon, latitude=False)
    if latitude is None or longitude is None:
        return None
    return latitude, longitude


def _direction(start: Point | None, end: Point | None) -> tuple[float, float] | None:
    if start is None or end is None:
        return None
    distance = _haversine(*start, *end)
    # Sub-metre displacements do not provide a useful GPS bearing.
    if distance < 1:
        return None
    lat1, lat2 = math.radians(start[0]), math.radians(end[0])
    delta_lon = math.radians(end[1] - start[1])
    east = math.sin(delta_lon) * math.cos(lat2)
    north = math.cos(lat1) * math.sin(lat2) - math.sin(lat1) * math.cos(lat2) * math.cos(delta_lon)
    return math.degrees(math.atan2(east, north)) % 360, distance


def _coordinates(point: Point | None) -> dict[str, float] | None:
    return {"lat": point[0], "lon": point[1]} if point is not None else None


def build_hole_directions(holes: pl.DataFrame, shots: pl.DataFrame) -> list[dict[str, Any]]:
    """Return hole axes and individual non-putt directions from recorded round GPS.

    A hole axis prefers tee to recorded pin, falling back to the latest usable
    shot endpoint. Neither is a reconstruction of the fairway or an aim line.
    """
    hole_rows = {
        (r["round_id"], r["hole_number"]): r
        for r in holes.to_dicts()
        if r.get("round_id") is not None and r.get("hole_number") is not None
    }
    grouped: dict[tuple[int, int], list[dict[str, Any]]] = {}
    for row in shots.to_dicts():
        if row.get("round_id") is not None and row.get("hole_number") is not None:
            grouped.setdefault((row["round_id"], row["hole_number"]), []).append(row)
    results = []
    for key in sorted(hole_rows.keys() | grouped.keys()):
        rows = sorted(
            (r for r in grouped.get(key, []) if isinstance(r.get("shot_number"), int)),
            key=lambda r: r["shot_number"],
        )
        tee = next(
            (
                r
                for r in rows
                if r["shot_number"] == 1 and str(r.get("shot_type") or "").upper() != "PUTT"
            ),
            {},
        )
        start = _point(tee, "start_lat", "start_lon")
        end = _point(hole_rows.get(key, {}), "pin_position_lat", "pin_position_lon")
        direction = _direction(start, end)
        source = "tee_to_recorded_pin" if direction else "unavailable"
        endpoint_shot = None
        if direction is None:
            end = None
            for shot in reversed(rows):
                candidate = _point(shot, "end_lat", "end_lon")
                direction = _direction(start, candidate)
                if direction is not None:
                    end = candidate
                    endpoint_shot = shot["shot_number"]
                    source = (
                        "tee_shot_start_to_end"
                        if endpoint_shot == 1
                        else "tee_to_last_recorded_endpoint"
                    )
                    break
        shot_directions = []
        for shot in rows:
            if str(shot.get("shot_type") or "").upper() == "PUTT":
                continue
            shot_start = _point(shot, "start_lat", "start_lon")
            shot_end = _point(shot, "end_lat", "end_lon")
            observed = _direction(shot_start, shot_end)
            shot_directions.append(
                {
                    "shot_number": shot["shot_number"],
                    "shot_time": shot.get("shot_time"),
                    "shot_time_zone_offset": shot.get("shot_time_zone_offset"),
                    "bearing_deg": observed[0] if observed else None,
                    "straight_line_distance_m": observed[1] if observed else None,
                    "start": _coordinates(shot_start),
                    "end": _coordinates(shot_end),
                    "source": "recorded_shot_start_to_end" if observed else "unavailable",
                }
            )
        results.append(
            {
                "round_id": key[0],
                "hole_number": key[1],
                "bearing_deg": direction[0] if direction else None,
                "straight_line_distance_m": direction[1] if direction else None,
                "source": source,
                "endpoint_shot_number": endpoint_shot,
                "start": _coordinates(start),
                "end": _coordinates(end),
                "observation_rounds": 1,
                "estimated": True,
                "unavailable_reason": None
                if direction
                else "Missing valid GPS endpoints or displacement below 1 metre.",
                "shots": shot_directions,
            }
        )
    return results
