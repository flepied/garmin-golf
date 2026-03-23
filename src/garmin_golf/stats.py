from __future__ import annotations

from typing import cast

import polars as pl


def build_summary_stats(rounds: pl.DataFrame, holes: pl.DataFrame) -> dict[str, float | int]:
    if rounds.is_empty():
        return {
            "rounds_played": 0,
            "average_score": 0.0,
            "average_to_par": 0.0,
            "gir_pct": 0.0,
            "fairway_hit_pct": 0.0,
            "average_putts": 0.0,
            "penalties": 0,
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
    average_putts = (
        _mean(holes["putts"]) if not holes.is_empty() and "putts" in holes.columns else 0.0
    )
    penalties = (
        int(holes["penalties"].fill_null(0).sum())
        if not holes.is_empty() and "penalties" in holes.columns
        else 0
    )

    return {
        "rounds_played": rounds.height,
        "average_score": round(average_score, 2),
        "average_to_par": round(average_to_par, 2),
        "gir_pct": round(gir_pct * 100, 2),
        "fairway_hit_pct": round(fairway_hit_pct * 100, 2),
        "average_putts": round(average_putts, 2),
        "penalties": penalties,
    }


def build_round_stats(
    rounds: pl.DataFrame,
    holes: pl.DataFrame,
    round_id: int,
) -> dict[str, float | int]:
    target_rounds = rounds.filter(pl.col("round_id") == round_id)
    target_holes = holes.filter(pl.col("round_id") == round_id)
    if target_rounds.is_empty():
        msg = f"Round {round_id} was not found in the local dataset."
        raise ValueError(msg)
    summary = build_summary_stats(target_rounds, target_holes)
    summary["round_id"] = round_id
    return summary


def _mean(series: pl.Series) -> float:
    value = series.cast(pl.Float64, strict=False).drop_nulls().mean()
    return float(cast(int | float, value)) if value is not None else 0.0


def _ratio_mean(series: pl.Series) -> float:
    numeric = series.cast(pl.Float64, strict=False).drop_nulls()
    value = numeric.mean()
    return float(cast(int | float, value)) if value is not None else 0.0
