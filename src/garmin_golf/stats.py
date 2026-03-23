from __future__ import annotations

from typing import cast

import polars as pl

SUPPORTED_EQUIVALENT_HOLE_COUNTS = (9, 18)


def build_summary_stats(
    rounds: pl.DataFrame,
    holes: pl.DataFrame,
    shots: pl.DataFrame | None = None,
) -> dict[str, float | int | str]:
    shots = shots if shots is not None else pl.DataFrame()
    if rounds.is_empty():
        return {
            "rounds_played": 0,
            "rounds_18_hole": 0,
            "rounds_9_hole": 0,
            "rounds_other_hole_count": 0,
            "average_score": 0.0,
            "average_to_par": 0.0,
            "gir_pct": 0.0,
            "gir_per_18": 0.0,
            "fir_pct": 0.0,
            "fir_per_18": 0.0,
            "fairway_hit_pct": 0.0,
            "scrambling_pct": 0.0,
            "scrambles_per_18": 0.0,
            "birdie_or_better_pct": 0.0,
            "par_pct": 0.0,
            "bogey_or_worse_pct": 0.0,
            "bogeys_or_worse_per_18": 0.0,
            "double_bogey_or_worse_pct": 0.0,
            "double_bogeys_or_worse_per_18": 0.0,
            "three_putt_pct": 0.0,
            "three_putts_per_18": 0.0,
            "penalty_hole_pct": 0.0,
            "par_3_average_to_par": 0.0,
            "par_4_average_to_par": 0.0,
            "par_5_average_to_par": 0.0,
            "average_putts_per_hole": 0.0,
            "average_putts_per_round": 0.0,
            "average_putts_per_18": 0.0,
            "average_hole_score": 0.0,
            "penalties": 0,
            "penalties_per_round": 0.0,
            "penalties_per_18": 0.0,
            "total_shots": 0,
            "average_shots_per_round": 0.0,
            "average_shots_per_18": 0.0,
            "average_tee_shot_distance_m": 0.0,
            "average_putt_distance_m": 0.0,
            "shot_type_breakdown": "",
            "club_usage_top5": "",
        }

    round_counts = _round_hole_counts(holes)
    rounds_18_hole = _count_rounds_with_holes(round_counts, 18)
    rounds_9_hole = _count_rounds_with_holes(round_counts, 9)
    rounds_other_hole_count = (
        round_counts.height - rounds_18_hole - rounds_9_hole
        if not round_counts.is_empty()
        else 0
    )

    average_score = _mean_round_equivalent(rounds, round_counts, "total_score")
    average_to_par = (
        _mean_round_delta_equivalent(rounds, round_counts, "total_score", "total_par")
        if "total_score" in rounds.columns and "total_par" in rounds.columns
        else 0.0
    )
    gir_pct = _ratio_mean(holes["gir"]) if not holes.is_empty() and "gir" in holes.columns else 0.0
    gir_per_18 = _boolean_successes_per_18(holes, "gir")
    fairway_hit_pct = (
        _ratio_mean(holes["fairway_hit"])
        if not holes.is_empty() and "fairway_hit" in holes.columns
        else 0.0
    )
    fir_per_18 = _boolean_successes_per_18(holes, "fairway_hit")
    scoring_frame = _holes_with_relative_score(holes)
    scrambling_pct = _scrambling_pct(scoring_frame)
    scrambles_per_18 = _scrambles_per_18(scoring_frame)
    birdie_or_better_pct = _score_bucket_pct(scoring_frame, "birdie_or_better")
    par_pct = _score_bucket_pct(scoring_frame, "par")
    bogey_or_worse_pct = _score_bucket_pct(scoring_frame, "bogey_or_worse")
    bogeys_or_worse_per_18 = _score_bucket_per_18(scoring_frame, "bogey_or_worse")
    double_bogey_or_worse_pct = _score_bucket_pct(scoring_frame, "double_bogey_or_worse")
    double_bogeys_or_worse_per_18 = _score_bucket_per_18(
        scoring_frame, "double_bogey_or_worse"
    )
    three_putt_pct = _threshold_pct(holes, "putts", 3)
    three_putts_per_18 = _threshold_successes_per_18(holes, "putts", 3)
    penalty_hole_pct = _threshold_pct(holes, "penalties", 1)
    par_3_average_to_par = _par_type_average_to_par(scoring_frame, 3)
    par_4_average_to_par = _par_type_average_to_par(scoring_frame, 4)
    par_5_average_to_par = _par_type_average_to_par(scoring_frame, 5)
    average_putts_per_hole = (
        _mean(holes["putts"]) if not holes.is_empty() and "putts" in holes.columns else 0.0
    )
    average_putts_per_round = (
        _per_round_mean(holes, "putts")
        if not holes.is_empty() and "putts" in holes.columns
        else 0.0
    )
    average_putts_per_18 = (
        _per_round_18_equivalent(holes, "putts")
        if not holes.is_empty() and "putts" in holes.columns
        else 0.0
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
    penalties_per_18 = (
        _per_round_18_equivalent(holes, "penalties")
        if not holes.is_empty() and "penalties" in holes.columns
        else 0.0
    )
    total_shots = shots.height if not shots.is_empty() else 0
    average_shots_per_round = total_shots / rounds.height if rounds.height else 0.0
    average_shots_per_18 = (
        _shots_per_18_equivalent(shots, holes)
        if not shots.is_empty() and not holes.is_empty()
        else 0.0
    )
    average_tee_shot_distance = _mean_for_filter(shots, "shot_type", "TEE", "distance_meters")
    average_putt_distance = _mean_for_filter(shots, "shot_type", "PUTT", "distance_meters")

    return {
        "rounds_played": rounds.height,
        "rounds_18_hole": rounds_18_hole,
        "rounds_9_hole": rounds_9_hole,
        "rounds_other_hole_count": rounds_other_hole_count,
        "average_score": round(average_score, 2),
        "average_to_par": round(average_to_par, 2),
        "gir_pct": round(gir_pct * 100, 2),
        "gir_per_18": round(gir_per_18, 2),
        "fir_pct": round(fairway_hit_pct * 100, 2),
        "fir_per_18": round(fir_per_18, 2),
        "fairway_hit_pct": round(fairway_hit_pct * 100, 2),
        "scrambling_pct": round(scrambling_pct * 100, 2),
        "scrambles_per_18": round(scrambles_per_18, 2),
        "birdie_or_better_pct": round(birdie_or_better_pct * 100, 2),
        "par_pct": round(par_pct * 100, 2),
        "bogey_or_worse_pct": round(bogey_or_worse_pct * 100, 2),
        "bogeys_or_worse_per_18": round(bogeys_or_worse_per_18, 2),
        "double_bogey_or_worse_pct": round(double_bogey_or_worse_pct * 100, 2),
        "double_bogeys_or_worse_per_18": round(double_bogeys_or_worse_per_18, 2),
        "three_putt_pct": round(three_putt_pct * 100, 2),
        "three_putts_per_18": round(three_putts_per_18, 2),
        "penalty_hole_pct": round(penalty_hole_pct * 100, 2),
        "par_3_average_to_par": round(par_3_average_to_par, 2),
        "par_4_average_to_par": round(par_4_average_to_par, 2),
        "par_5_average_to_par": round(par_5_average_to_par, 2),
        "average_putts_per_hole": round(average_putts_per_hole, 2),
        "average_putts_per_round": round(average_putts_per_round, 2),
        "average_putts_per_18": round(average_putts_per_18, 2),
        "average_hole_score": round(average_hole_score, 2),
        "penalties": penalties,
        "penalties_per_round": round(penalties_per_round, 2),
        "penalties_per_18": round(penalties_per_18, 2),
        "total_shots": total_shots,
        "average_shots_per_round": round(average_shots_per_round, 2),
        "average_shots_per_18": round(average_shots_per_18, 2),
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
    target_shots = (
        shots.filter(pl.col("round_id") == round_id) if shots is not None else pl.DataFrame()
    )
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


def _boolean_successes_per_18(frame: pl.DataFrame, column: str) -> float:
    if frame.is_empty() or column not in frame.columns or "round_id" not in frame.columns:
        return 0.0
    valid = frame.drop_nulls([column]).filter(pl.col(column))
    if valid.is_empty():
        return 0.0
    grouped = valid.group_by("round_id").agg(pl.len().alias("successes"))
    hole_counts = _round_hole_counts(frame)
    return _per_round_metric_18_equivalent(grouped, hole_counts, "successes")


def _holes_with_relative_score(holes: pl.DataFrame) -> pl.DataFrame:
    if holes.is_empty() or "strokes" not in holes.columns or "par" not in holes.columns:
        return pl.DataFrame()
    frame = holes.with_columns(
        [
            pl.col("strokes").cast(pl.Float64, strict=False).alias("strokes_value"),
            pl.col("par").cast(pl.Float64, strict=False).alias("par_value"),
        ]
    ).drop_nulls(["strokes_value", "par_value"])
    if frame.is_empty():
        return frame
    return frame.with_columns((pl.col("strokes_value") - pl.col("par_value")).alias("to_par"))


def _scrambles_per_18(frame: pl.DataFrame) -> float:
    if frame.is_empty() or "gir" not in frame.columns or "to_par" not in frame.columns:
        return 0.0
    candidates = frame.drop_nulls(["gir"]).filter(~pl.col("gir"))
    if candidates.is_empty():
        return 0.0
    successes = candidates.filter(pl.col("to_par") <= 0).group_by("round_id").agg(
        pl.len().alias("successes")
    )
    hole_counts = _round_hole_counts(frame)
    return _per_round_metric_18_equivalent(successes, hole_counts, "successes")


def _round_hole_counts(holes: pl.DataFrame) -> pl.DataFrame:
    if holes.is_empty() or "round_id" not in holes.columns:
        return pl.DataFrame(schema={"round_id": pl.Int64, "hole_count": pl.UInt32})
    return holes.group_by("round_id").len().rename({"len": "hole_count"})


def _count_rounds_with_holes(round_counts: pl.DataFrame, hole_count: int) -> int:
    if round_counts.is_empty():
        return 0
    return round_counts.filter(pl.col("hole_count") == hole_count).height


def _supported_hole_counts(frame: pl.DataFrame) -> pl.DataFrame:
    if frame.is_empty() or "hole_count" not in frame.columns:
        return frame
    return frame.filter(pl.col("hole_count").is_in(SUPPORTED_EQUIVALENT_HOLE_COUNTS))


def _per_round_metric_18_equivalent(
    grouped: pl.DataFrame,
    hole_counts: pl.DataFrame,
    value_column: str,
) -> float:
    if grouped.is_empty() or hole_counts.is_empty() or value_column not in grouped.columns:
        return 0.0
    joined = _supported_hole_counts(
        hole_counts.join(grouped, on="round_id", how="left")
    ).with_columns(
        pl.col(value_column).fill_null(0).cast(pl.Float64, strict=False).alias(value_column)
    )
    if joined.is_empty():
        return 0.0
    valid = joined.with_columns(
        (pl.col(value_column) * 18.0 / pl.col("hole_count")).alias("equivalent")
    )
    return _mean(valid["equivalent"])


def _score_bucket_pct(frame: pl.DataFrame, bucket: str) -> float:
    if frame.is_empty() or "to_par" not in frame.columns:
        return 0.0
    expr = {
        "birdie_or_better": pl.col("to_par") <= -1,
        "par": pl.col("to_par") == 0,
        "bogey_or_worse": pl.col("to_par") >= 1,
        "double_bogey_or_worse": pl.col("to_par") >= 2,
    }.get(bucket)
    if expr is None:
        return 0.0
    return frame.select(expr.cast(pl.Float64).mean().alias("value")).item() or 0.0


def _score_bucket_per_18(frame: pl.DataFrame, bucket: str) -> float:
    if frame.is_empty() or "round_id" not in frame.columns or "to_par" not in frame.columns:
        return 0.0
    expr = {
        "birdie_or_better": pl.col("to_par") <= -1,
        "par": pl.col("to_par") == 0,
        "bogey_or_worse": pl.col("to_par") >= 1,
        "double_bogey_or_worse": pl.col("to_par") >= 2,
    }.get(bucket)
    if expr is None:
        return 0.0
    grouped = frame.filter(expr).group_by("round_id").agg(pl.len().alias("successes"))
    hole_counts = _round_hole_counts(frame)
    return _per_round_metric_18_equivalent(grouped, hole_counts, "successes")


def _scrambling_pct(frame: pl.DataFrame) -> float:
    if frame.is_empty() or "gir" not in frame.columns or "to_par" not in frame.columns:
        return 0.0
    candidates = frame.drop_nulls(["gir"]).filter(~pl.col("gir"))
    if candidates.is_empty():
        return 0.0
    value = candidates.select((pl.col("to_par") <= 0).cast(pl.Float64).mean().alias("value")).item()
    return float(value) if value is not None else 0.0


def _threshold_pct(frame: pl.DataFrame, column: str, threshold: int) -> float:
    if frame.is_empty() or column not in frame.columns:
        return 0.0
    valid = frame.with_columns(
        pl.col(column).cast(pl.Float64, strict=False).alias("_metric_value")
    ).drop_nulls(["_metric_value"])
    if valid.is_empty():
        return 0.0
    value = valid.select(
        (pl.col("_metric_value") >= threshold).cast(pl.Float64).mean().alias("value")
    ).item()
    return float(value) if value is not None else 0.0


def _threshold_successes_per_18(frame: pl.DataFrame, column: str, threshold: int) -> float:
    if frame.is_empty() or column not in frame.columns or "round_id" not in frame.columns:
        return 0.0
    valid = frame.with_columns(
        pl.col(column).cast(pl.Float64, strict=False).alias("_metric_value")
    ).drop_nulls(["_metric_value"])
    if valid.is_empty():
        return 0.0
    grouped = valid.filter(pl.col("_metric_value") >= threshold).group_by("round_id").agg(
        pl.len().alias("successes")
    )
    hole_counts = _round_hole_counts(frame)
    return _per_round_metric_18_equivalent(grouped, hole_counts, "successes")


def _par_type_average_to_par(frame: pl.DataFrame, par_value: int) -> float:
    if frame.is_empty() or "par_value" not in frame.columns or "to_par" not in frame.columns:
        return 0.0
    valid = frame.filter(pl.col("par_value") == float(par_value))
    return _mean(valid["to_par"]) if not valid.is_empty() else 0.0


def _per_round_mean(frame: pl.DataFrame, column: str) -> float:
    if frame.is_empty() or "round_id" not in frame.columns or column not in frame.columns:
        return 0.0
    grouped = frame.group_by("round_id").agg(pl.col(column).cast(pl.Float64, strict=False).sum())
    return _mean(grouped[column])


def _per_round_18_equivalent(frame: pl.DataFrame, column: str) -> float:
    if frame.is_empty() or "round_id" not in frame.columns or column not in frame.columns:
        return 0.0
    grouped = frame.group_by("round_id").agg(
        [
            pl.col(column).cast(pl.Float64, strict=False).sum().alias(column),
            pl.len().alias("hole_count"),
        ]
    )
    valid = _supported_hole_counts(grouped).with_columns(
        (pl.col(column) * 18.0 / pl.col("hole_count")).alias("equivalent")
    )
    return _mean(valid["equivalent"]) if not valid.is_empty() else 0.0


def _mean_round_equivalent(rounds: pl.DataFrame, round_counts: pl.DataFrame, column: str) -> float:
    if rounds.is_empty() or column not in rounds.columns or round_counts.is_empty():
        return 0.0
    joined = _supported_hole_counts(rounds.join(round_counts, on="round_id", how="inner"))
    if joined.is_empty():
        return 0.0
    valid = joined.with_columns(
        (pl.col(column).cast(pl.Float64, strict=False) * 18.0 / pl.col("hole_count")).alias(
            "equivalent"
        )
    )
    return _mean(valid["equivalent"])


def _mean_round_delta_equivalent(
    rounds: pl.DataFrame,
    round_counts: pl.DataFrame,
    value_column: str,
    baseline_column: str,
) -> float:
    if (
        rounds.is_empty()
        or value_column not in rounds.columns
        or baseline_column not in rounds.columns
        or round_counts.is_empty()
    ):
        return 0.0
    joined = _supported_hole_counts(rounds.join(round_counts, on="round_id", how="inner"))
    if joined.is_empty():
        return 0.0
    valid = joined.with_columns(
        (
            (
                pl.col(value_column).cast(pl.Float64, strict=False)
                - pl.col(baseline_column).cast(pl.Float64, strict=False)
            )
            * 18.0
            / pl.col("hole_count")
        ).alias("equivalent")
    )
    return _mean(valid["equivalent"])


def _shots_per_18_equivalent(shots: pl.DataFrame, holes: pl.DataFrame) -> float:
    if shots.is_empty() or holes.is_empty():
        return 0.0
    hole_counts = _round_hole_counts(holes)
    if hole_counts.is_empty():
        return 0.0
    shot_counts = shots.group_by("round_id").len().rename({"len": "shot_count"})
    joined = _supported_hole_counts(shot_counts.join(hole_counts, on="round_id", how="inner"))
    if joined.is_empty():
        return 0.0
    valid = joined.with_columns(
        (pl.col("shot_count").cast(pl.Float64) * 18.0 / pl.col("hole_count")).alias("equivalent")
    )
    return _mean(valid["equivalent"])


def _mean_for_filter(
    frame: pl.DataFrame,
    filter_column: str,
    filter_value: str,
    value_column: str,
) -> float:
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
