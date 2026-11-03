"""Local historical comparisons with explicit sample sizes and missing-data handling."""

from __future__ import annotations

import math
import statistics
from typing import Any, cast

import polars as pl


def window_metadata(rounds: pl.DataFrame) -> dict[str, Any]:
    dates = rounds["played_on"].drop_nulls().to_list() if "played_on" in rounds.columns else []
    return {
        "from": min(dates) if dates else None,
        "to": max(dates) if dates else None,
        "rounds": rounds.height,
        "round_ids": rounds["round_id"].to_list() if "round_id" in rounds.columns else [],
        "exploratory": rounds.height < 5,
        "excluded_rounds_included": False,
    }


def by_nine(holes: pl.DataFrame) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for name, low, high in (("front_nine", 1, 9), ("back_nine", 10, 18)):
        rows = [r for r in holes.to_dicts() if low <= (r.get("hole_number") or 0) <= high]
        scored = [r for r in rows if r.get("strokes") is not None and r.get("par") is not None]
        result[name] = {
            "holes_played": len(rows),
            "holes_scored": len(scored),
            "complete": len({r["hole_number"] for r in scored}) == 9,
            "strokes": sum(r["strokes"] for r in scored) if scored else None,
            "to_par": sum(r["strokes"] - r["par"] for r in scored) if scored else None,
            "putts": sum(r["putts"] for r in rows if r.get("putts") is not None)
            if any(r.get("putts") is not None for r in rows)
            else None,
        }
    return result


def compare_holes(holes: pl.DataFrame, baseline: pl.DataFrame) -> list[dict[str, Any]]:
    result = []
    for row in holes.sort("hole_number").to_dicts() if "hole_number" in holes.columns else []:
        history = [
            r
            for r in baseline.to_dicts()
            if r.get("hole_number") == row["hole_number"]
            and r.get("strokes") is not None
            and r.get("par") is not None
        ]
        mean = statistics.mean(r["strokes"] - r["par"] for r in history) if history else None
        value = (
            row["strokes"] - row["par"]
            if row.get("strokes") is not None and row.get("par") is not None
            else None
        )
        n = len({r["round_id"] for r in history})
        result.append(
            {
                "hole_number": row["hole_number"],
                "to_par": value,
                "baseline_avg_to_par": mean,
                "delta_to_par": value - mean if value is not None and mean is not None else None,
                "rounds_played": n,
                "exploratory": n < 5,
            }
        )
    return result


def round_value(holes: pl.DataFrame, metric: str) -> float | None:
    split = by_nine(holes)
    if metric == "front_back_gap":
        front, back = split["front_nine"], split["back_nine"]
        return (
            float(front["to_par"] - back["to_par"])
            if front["complete"] and back["complete"]
            else None
        )
    rows = holes.to_dicts()
    if not rows:
        return None
    if metric == "average_to_par":
        if any(r.get("strokes") is None or r.get("par") is None for r in rows):
            return None
        return float(sum(r["strokes"] - r["par"] for r in rows)) * 18.0 / len(rows)
    column = {
        "gir_pct": "gir",
        "fir_pct": "fairway_hit",
        "three_putts_per_18": "putts",
        "penalties_per_18": "penalties",
    }[metric]
    values = [r[column] for r in rows if r.get(column) is not None]
    if not values:
        return None
    if metric.endswith("pct"):
        return statistics.mean(float(v) for v in values) * 100
    if len(values) != len(rows):
        return None
    return sum(float(v >= 3) if column == "putts" else float(v) for v in values) * 18 / len(rows)


def rarity(value: float | None, values: list[float]) -> dict[str, Any]:
    n = len(values)
    std = statistics.stdev(values) if n > 1 else None
    return {
        "value": value,
        "n": n,
        "exploratory": n < 5,
        "mean": statistics.mean(values) if n else None,
        "stddev": std,
        "percentile": 100
        * (sum(v < value for v in values) + 0.5 * sum(v == value for v in values))
        / n
        if n and value is not None
        else None,
        "z": (value - statistics.mean(values)) / std if value is not None and std else None,
        "percentile_method": "midrank",
        "stddev_method": "sample",
        "higher_means": "larger numeric value; not necessarily better",
    }


def hole_distances(shots: pl.DataFrame) -> list[dict[str, Any]]:
    from .analysis.engine import _coordinate_degrees

    groups: dict[int, list[dict[str, Any]]] = {}
    for row in shots.to_dicts():
        if (
            row.get("shot_number") == 1
            and row.get("shot_type") != "PUTT"
            and row.get("hole_number") is not None
        ):
            groups.setdefault(row["hole_number"], []).append(row)
    result = []
    for number, rows in sorted(groups.items()):
        distances = [
            float(r["distance_meters"])
            for r in rows
            if r.get("distance_meters") is not None
            and math.isfinite(float(r["distance_meters"]))
            and float(r["distance_meters"]) > 0
        ]
        vectors = []
        for row in rows:
            coords = [row.get(k) for k in ("start_lat", "start_lon", "end_lat", "end_lon")]
            if any(v is None or not math.isfinite(float(v)) for v in coords):
                continue
            decoded = [
                _coordinate_degrees(cast(float, v), latitude=i % 2 == 0)
                for i, v in enumerate(coords)
            ]
            if any(v is None for v in decoded):
                continue
            lat1, lon1, lat2, lon2 = [float(v) for v in decoded if v is not None]
            east = math.radians(lon2 - lon1) * math.cos(math.radians((lat1 + lat2) / 2))
            north = math.radians(lat2 - lat1)
            length = math.hypot(east, north)
            if length > 0:
                vectors.append((east / length, north / length))
        east_mean = statistics.mean(v[0] for v in vectors) if vectors else 0
        north_mean = statistics.mean(v[1] for v in vectors) if vectors else 0
        concentration = math.hypot(east_mean, north_mean)
        result.append(
            {
                "hole_number": number,
                "shots": len(rows),
                "n": len(distances),
                "rounds": len({r["round_id"] for r in rows}),
                "exploratory": len(distances) < 10 or len({r["round_id"] for r in rows}) < 5,
                "mean_m": statistics.mean(distances) if distances else None,
                "stddev_m": statistics.stdev(distances) if len(distances) > 1 else None,
                "median_m": statistics.median(distances) if distances else None,
                "geometry_n": len(vectors),
                "geometry_exploratory": len(vectors) < 10 or len({r["round_id"] for r in rows}) < 5,
                "direction_concentration": concentration if vectors else None,
                "observed_tee_bearing_deg": math.degrees(math.atan2(east_mean, north_mean)) % 360
                if concentration > 1e-6
                else None,
                "geometry_source": "recorded tee-shot start/end; not hole layout or intended aim",
                "distance_trimming": "none",
            }
        )
    return result
