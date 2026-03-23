from __future__ import annotations

from typing import cast

import polars as pl


def build_summary_stats(
    rounds: pl.DataFrame,
    holes: pl.DataFrame,
    shots: pl.DataFrame | None = None,
) -> dict[str, float | int | str]:
    shots = shots if shots is not None else pl.DataFrame()
    if rounds.is_empty():
        return {
            "rounds_played": 0,
            "average_score": 0.0,
            "average_to_par": 0.0,
            "gir_pct": 0.0,
            "fairway_hit_pct": 0.0,
            "average_putts_per_hole": 0.0,
            "average_putts_per_round": 0.0,
            "average_hole_score": 0.0,
            "penalties": 0,
            "penalties_per_round": 0.0,
            "total_shots": 0,
            "average_shots_per_round": 0.0,
            "average_tee_shot_distance_m": 0.0,
            "average_putt_distance_m": 0.0,
            "shot_type_breakdown": "",
            "club_usage_top5": "",
        }

    average_score = _mean(rounds["total_score"]) if "total_score" in rounds.columns else 0.0
    average_to_par = (
        _mean(rounds["total_score"] - rounds["total_par"])
        if "total_score" in rounds.columns and "total_par" in rounds.columns
        else 0.0
    )
    gir_pct = _ratio_mean(holes["gir"]) if not holes.is_empty() and "gir" in holes.columns else 0.0
    fairway_hit_pct = (
        _ratio_mean(holes["fairway_hit"])
        if not holes.is_empty() and "fairway_hit" in holes.columns
        else 0.0
    )
    average_putts_per_hole = (
        _mean(holes["putts"]) if not holes.is_empty() and "putts" in holes.columns else 0.0
    )
    average_putts_per_round = (
        _per_round_mean(holes, "putts") if not holes.is_empty() and "putts" in holes.columns else 0.0
    )
    average_hole_score = (
        _mean(holes["strokes"]) if not holes.is_empty() and "strokes" in holes.columns else 0.0
    )
    penalties = (
        int(holes["penalties"].fill_null(0).sum())
        if not holes.is_empty() and "penalties" in holes.columns
        else 0
    )
    penalties_per_round = penalties / rounds.height if rounds.height else 0.0
    total_shots = shots.height if not shots.is_empty() else 0
    average_shots_per_round = total_shots / rounds.height if rounds.height else 0.0
    average_tee_shot_distance = _mean_for_filter(shots, "shot_type", "TEE", "distance_meters")
    average_putt_distance = _mean_for_filter(shots, "shot_type", "PUTT", "distance_meters")

    return {
        "rounds_played": rounds.height,
        "average_score": round(average_score, 2),
        "average_to_par": round(average_to_par, 2),
        "gir_pct": round(gir_pct * 100, 2),
        "fairway_hit_pct": round(fairway_hit_pct * 100, 2),
        "average_putts_per_hole": round(average_putts_per_hole, 2),
        "average_putts_per_round": round(average_putts_per_round, 2),
        "average_hole_score": round(average_hole_score, 2),
        "penalties": penalties,
        "penalties_per_round": round(penalties_per_round, 2),
        "total_shots": total_shots,
        "average_shots_per_round": round(average_shots_per_round, 2),
        "average_tee_shot_distance_m": round(average_tee_shot_distance, 2),
        "average_putt_distance_m": round(average_putt_distance, 2),
        "shot_type_breakdown": _format_count_breakdown(shots, "shot_type"),
        "club_usage_top5": _format_count_breakdown(shots, "club", limit=5),
    }


def build_round_stats(
    rounds: pl.DataFrame,
    holes: pl.DataFrame,
    shots: pl.DataFrame | None,
    round_id: int,
) -> dict[str, float | int | str]:
    target_rounds = rounds.filter(pl.col("round_id") == round_id)
    target_holes = holes.filter(pl.col("round_id") == round_id)
    target_shots = shots.filter(pl.col("round_id") == round_id) if shots is not None else pl.DataFrame()
    if target_rounds.is_empty():
        msg = f"Round {round_id} was not found in the local dataset."
        raise ValueError(msg)
    summary = build_summary_stats(target_rounds, target_holes, target_shots)
    summary["round_id"] = round_id
    return summary


def _mean(series: pl.Series) -> float:
    value = series.cast(pl.Float64, strict=False).drop_nulls().mean()
    return float(cast(int | float, value)) if value is not None else 0.0


def _ratio_mean(series: pl.Series) -> float:
    numeric = series.cast(pl.Float64, strict=False).drop_nulls()
    value = numeric.mean()
    return float(cast(int | float, value)) if value is not None else 0.0


def _per_round_mean(frame: pl.DataFrame, column: str) -> float:
    if frame.is_empty() or "round_id" not in frame.columns or column not in frame.columns:
        return 0.0
    grouped = frame.group_by("round_id").agg(pl.col(column).cast(pl.Float64, strict=False).sum())
    return _mean(grouped[column])


def _mean_for_filter(frame: pl.DataFrame, filter_column: str, filter_value: str, value_column: str) -> float:
    if frame.is_empty() or filter_column not in frame.columns or value_column not in frame.columns:
        return 0.0
    filtered = frame.filter(pl.col(filter_column) == filter_value)
    if filtered.is_empty():
        return 0.0
    return _mean(filtered[value_column])


def _format_count_breakdown(frame: pl.DataFrame, column: str, *, limit: int | None = None) -> str:
    if frame.is_empty() or column not in frame.columns:
        return ""
    counts = (
        frame
        .drop_nulls(column)
        .group_by(column)
        .len()
        .sort(["len", column], descending=[True, False])
    )
    if counts.is_empty():
        return ""
    if limit is not None:
        counts = counts.head(limit)
    labels = counts[column].to_list()
    values = counts["len"].to_list()
    parts = [f"{label}: {value}" for label, value in zip(labels, values, strict=False)]
    return ", ".join(parts)
