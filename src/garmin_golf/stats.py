from __future__ import annotations

from collections.abc import Sequence
from typing import cast

import polars as pl

SUPPORTED_EQUIVALENT_HOLE_COUNTS = (9, 18)
DISTANCE_OUTLIER_STDDEV_THRESHOLD = 2.0
DISTANCE_OUTLIER_MIN_GROUP_SIZE = 5
TREND_METRIC_COLUMNS: dict[str, tuple[str, str, str]] = {
    "average_to_par": ("round_to_par", "window_average_to_par", "delta_average_to_par"),
    "gir_pct": ("round_gir_pct", "window_gir_pct", "delta_gir_pct"),
    "fir_pct": ("round_fir_pct", "window_fir_pct", "delta_fir_pct"),
    "scrambling_pct": (
        "round_scrambling_pct",
        "window_scrambling_pct",
        "delta_scrambling_pct",
    ),
    "three_putts_per_18": (
        "round_three_putts_per_18",
        "window_three_putts_per_18",
        "delta_three_putts_per_18",
    ),
    "penalties_per_18": (
        "round_penalties_per_18",
        "window_penalties_per_18",
        "delta_penalties_per_18",
    ),
}


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
    target_rounds = (
        rounds.filter(pl.col("round_id") == round_id)
        if "round_id" in rounds.columns
        else pl.DataFrame()
    )
    target_holes = (
        holes.filter(pl.col("round_id") == round_id)
        if "round_id" in holes.columns
        else pl.DataFrame()
    )
    target_shots = (
        shots.filter(pl.col("round_id") == round_id)
        if shots is not None and "round_id" in shots.columns
        else pl.DataFrame()
    )
    if target_rounds.is_empty():
        msg = f"Round {round_id} was not found in the local dataset."
        raise ValueError(msg)
    summary = build_summary_stats(target_rounds, target_holes, target_shots)
    summary["round_id"] = round_id
    return summary


def build_round_trends(
    rounds: pl.DataFrame,
    holes: pl.DataFrame,
    shots: pl.DataFrame | None = None,
    *,
    window: int = 5,
) -> pl.DataFrame:
    shots = shots if shots is not None else pl.DataFrame()
    if rounds.is_empty() or "round_id" not in rounds.columns:
        return pl.DataFrame()
    if window <= 0:
        return pl.DataFrame()

    ordered_rounds = _sort_rounds_for_trends(rounds)
    round_rows = ordered_rounds.to_dicts()
    if not round_rows:
        return pl.DataFrame()

    trend_rows: list[dict[str, object]] = []
    for index, round_row in enumerate(round_rows):
        round_id = round_row.get("round_id")
        if not isinstance(round_id, int):
            continue

        current_window_rows = round_rows[max(0, index - window + 1) : index + 1]
        previous_window_rows = round_rows[
            max(0, index - (window * 2) + 1) : max(0, index - window + 1)
        ]

        current_window_round_ids = _extract_round_ids(current_window_rows)
        previous_window_round_ids = _extract_round_ids(previous_window_rows)

        current_rounds = ordered_rounds.filter(pl.col("round_id") == round_id)
        current_holes = _filter_by_round_ids(holes, [round_id])
        current_shots = _filter_by_round_ids(shots, [round_id])
        current_summary = build_summary_stats(current_rounds, current_holes, current_shots)

        window_rounds = _filter_by_round_ids(ordered_rounds, current_window_round_ids)
        window_holes = _filter_by_round_ids(holes, current_window_round_ids)
        window_shots = _filter_by_round_ids(shots, current_window_round_ids)
        window_summary = build_summary_stats(window_rounds, window_holes, window_shots)

        previous_summary: dict[str, float | int | str] | None = None
        if previous_window_round_ids:
            previous_rounds = _filter_by_round_ids(ordered_rounds, previous_window_round_ids)
            previous_holes = _filter_by_round_ids(holes, previous_window_round_ids)
            previous_shots = _filter_by_round_ids(shots, previous_window_round_ids)
            previous_summary = build_summary_stats(previous_rounds, previous_holes, previous_shots)

        trend_rows.append(
            {
                "round_id": round_id,
                "played_on": round_row.get("played_on"),
                "course_name": _trend_course_name(round_row),
                "hole_count": int(current_holes.height) if not current_holes.is_empty() else 0,
                "window": window,
                "rounds_in_window": len(current_window_round_ids),
                "rounds_in_previous_window": len(previous_window_round_ids),
                "round_to_par": _as_float(current_summary.get("average_to_par")),
                "round_gir_pct": _as_float(current_summary.get("gir_pct")),
                "round_fir_pct": _as_float(current_summary.get("fir_pct")),
                "round_scrambling_pct": _as_float(current_summary.get("scrambling_pct")),
                "round_three_putts_per_18": _as_float(current_summary.get("three_putts_per_18")),
                "round_penalties_per_18": _as_float(current_summary.get("penalties_per_18")),
                "window_average_to_par": _as_float(window_summary.get("average_to_par")),
                "window_gir_pct": _as_float(window_summary.get("gir_pct")),
                "window_fir_pct": _as_float(window_summary.get("fir_pct")),
                "window_scrambling_pct": _as_float(window_summary.get("scrambling_pct")),
                "window_three_putts_per_18": _as_float(window_summary.get("three_putts_per_18")),
                "window_penalties_per_18": _as_float(window_summary.get("penalties_per_18")),
                "delta_average_to_par": _delta_metric(
                    window_summary, previous_summary, "average_to_par"
                ),
                "delta_gir_pct": _delta_metric(window_summary, previous_summary, "gir_pct"),
                "delta_fir_pct": _delta_metric(window_summary, previous_summary, "fir_pct"),
                "delta_scrambling_pct": _delta_metric(
                    window_summary, previous_summary, "scrambling_pct"
                ),
                "delta_three_putts_per_18": _delta_metric(
                    window_summary, previous_summary, "three_putts_per_18"
                ),
                "delta_penalties_per_18": _delta_metric(
                    window_summary, previous_summary, "penalties_per_18"
                ),
            }
        )

    if not trend_rows:
        return pl.DataFrame()

    trend_frame = pl.DataFrame(trend_rows)
    return trend_frame.sort(
        by=["played_on", "round_id"],
        descending=[True, True],
        nulls_last=True,
    )


def build_course_hole_stats(rounds: pl.DataFrame, holes: pl.DataFrame) -> pl.DataFrame:
    if (
        rounds.is_empty()
        or holes.is_empty()
        or "round_id" not in rounds.columns
        or "round_id" not in holes.columns
        or "hole_number" not in holes.columns
    ):
        return pl.DataFrame()

    joined = holes.join(rounds.select("round_id"), on="round_id", how="inner")
    if joined.is_empty():
        return pl.DataFrame()

    scoring = _holes_with_relative_score(joined)
    frame = joined.join(
        scoring.select(["round_id", "hole_number", "to_par"]),
        on=["round_id", "hole_number"],
        how="left",
    )
    return (
        frame.group_by("hole_number")
        .agg(
            [
                pl.col("round_id").n_unique().alias("rounds_played"),
                pl.col("par").drop_nulls().first().alias("par"),
                pl.col("strokes").cast(pl.Float64, strict=False).mean().alias("avg_strokes"),
                pl.col("to_par").cast(pl.Float64, strict=False).mean().alias("avg_to_par"),
                _pct_expr(pl.col("to_par") <= 0).alias("par_or_better_pct"),
                _pct_expr(pl.col("to_par") >= 1).alias("bogey_or_worse_pct"),
                _pct_expr(pl.col("to_par") >= 2).alias("double_or_worse_pct"),
                _pct_expr(pl.col("gir")).alias("gir_pct"),
                _pct_expr(pl.col("fairway_hit")).alias("fairway_hit_pct"),
                _pct_expr(pl.col("putts") >= 3).alias("three_putt_pct"),
                pl.col("penalties")
                .cast(pl.Float64, strict=False)
                .fill_null(0)
                .mean()
                .alias("penalty_rate"),
                pl.col("putts").cast(pl.Float64, strict=False).mean().alias("avg_putts"),
            ]
        )
        .sort(
            by=["avg_to_par", "penalty_rate", "hole_number"],
            descending=[True, True, False],
            nulls_last=True,
        )
        .with_columns(
            [
                pl.col("avg_strokes").round(2),
                pl.col("avg_to_par").round(2),
                pl.col("par_or_better_pct").round(2),
                pl.col("bogey_or_worse_pct").round(2),
                pl.col("double_or_worse_pct").round(2),
                pl.col("gir_pct").round(2),
                pl.col("fairway_hit_pct").round(2),
                pl.col("three_putt_pct").round(2),
                pl.col("penalty_rate").round(2),
                pl.col("avg_putts").round(2),
            ]
        )
    )


def build_course_focus_stats(hole_stats: pl.DataFrame) -> dict[str, str]:
    if hole_stats.is_empty() or "hole_number" not in hole_stats.columns:
        return {
            "hardest_holes": "",
            "penalty_holes": "",
            "three_putt_holes": "",
            "gir_trouble_holes": "",
        }

    return {
        "hardest_holes": _format_hole_focus(
            hole_stats,
            "avg_to_par",
            suffix=" to par",
            highest=True,
        ),
        "penalty_holes": _format_hole_focus(
            hole_stats, "penalty_rate", suffix=" penalties", highest=True
        ),
        "three_putt_holes": _format_hole_focus(
            hole_stats, "three_putt_pct", suffix="% 3-putts", highest=True
        ),
        "gir_trouble_holes": _format_hole_focus(
            hole_stats, "gir_pct", suffix="% GIR", highest=False
        ),
    }


def build_practice_focus_stats(
    rounds: pl.DataFrame,
    holes: pl.DataFrame,
    shots: pl.DataFrame | None = None,
) -> dict[str, str | float | int]:
    shots = shots if shots is not None else pl.DataFrame()
    if rounds.is_empty() or holes.is_empty():
        return {
            "rounds_played": 0,
            "priority_1": "",
            "priority_2": "",
            "priority_3": "",
            "estimated_strokes_to_save_per_18": 0.0,
        }

    summary = build_summary_stats(rounds, holes, shots)
    gir_opportunities_per_18 = _nonnull_count_per_18(holes, "gir")
    fairway_opportunities_per_18 = _nonnull_count_per_18(holes, "fairway_hit")

    penalties_per_18 = float(summary["penalties_per_18"])
    three_putts_per_18 = float(summary["three_putts_per_18"])
    scrambles_per_18 = float(summary["scrambles_per_18"])
    gir_per_18 = float(summary["gir_per_18"])
    fir_per_18 = float(summary["fir_per_18"])

    missed_gir_per_18 = max(0.0, gir_opportunities_per_18 - gir_per_18)
    failed_scrambles_per_18 = max(0.0, missed_gir_per_18 - scrambles_per_18)
    missed_fairways_per_18 = max(0.0, fairway_opportunities_per_18 - fir_per_18)

    candidates = [
        (
            penalties_per_18,
            (
                "Penalty control",
                f"{penalties_per_18:.2f} penalties per 18; "
                "focus on tee-shot and recovery discipline.",
            ),
        ),
        (
            three_putts_per_18,
            (
                "Lag putting",
                f"{three_putts_per_18:.2f} three-putts per 18; "
                "prioritize pace control from long range.",
            ),
        ),
        (
            failed_scrambles_per_18 * 0.5,
            (
                "Scrambling",
                f"{failed_scrambles_per_18:.2f} failed saves per 18 after missed GIR; "
                "practice chips and first putts inside scoring range.",
            ),
        ),
        (
            missed_gir_per_18 * 0.15,
            (
                "Approach play",
                f"{missed_gir_per_18:.2f} missed greens per 18; "
                "tighten start lines and distance control into greens.",
            ),
        ),
        (
            missed_fairways_per_18 * 0.1,
            (
                "Driving accuracy",
                f"{missed_fairways_per_18:.2f} missed fairways per 18; "
                "favor stock tee-shot shapes and conservative targets.",
            ),
        ),
    ]

    ranked = [item for item in candidates if item[0] > 0.0]
    ranked.sort(key=lambda item: (-item[0], item[1][0]))
    priorities = [f"{label}: {detail}" for _, (label, detail) in ranked[:3]]

    while len(priorities) < 3:
        priorities.append("")

    estimated_strokes = sum(score for score, _ in ranked[:3])
    return {
        "rounds_played": rounds.height,
        "priority_1": priorities[0],
        "priority_2": priorities[1],
        "priority_3": priorities[2],
        "estimated_strokes_to_save_per_18": round(estimated_strokes, 2),
    }


def build_second_shot_stats(holes: pl.DataFrame, shots: pl.DataFrame) -> pl.DataFrame:
    if (
        holes.is_empty()
        or shots.is_empty()
        or "round_id" not in holes.columns
        or "hole_number" not in holes.columns
        or "par" not in holes.columns
        or "round_id" not in shots.columns
        or "hole_number" not in shots.columns
        or "shot_number" not in shots.columns
    ):
        return pl.DataFrame()

    hole_scoring = _holes_with_relative_score(holes)
    joined = (
        shots.filter(pl.col("shot_number").cast(pl.Int64, strict=False) == 2)
        .join(
            holes.select(["round_id", "hole_number", "par"]),
            on=["round_id", "hole_number"],
            how="inner",
        )
        .join(
            hole_scoring.select(["round_id", "hole_number", "to_par"]),
            on=["round_id", "hole_number"],
            how="left",
        )
        .filter(pl.col("par").cast(pl.Int64, strict=False).is_in([4, 5]))
        .with_columns(
            [
                pl.col("par").cast(pl.Int64, strict=False).alias("par"),
                pl.col("club")
                .cast(pl.String, strict=False)
                .fill_null("Unknown")
                .str.strip_chars()
                .alias("club"),
                pl.col("distance_meters").cast(pl.Float64, strict=False).alias("distance_meters"),
            ]
        )
    )
    if joined.is_empty():
        return pl.DataFrame()

    trimmed_joined = trim_distance_outliers(joined, group_columns=["par", "club"])
    distance_summary = (
        trimmed_joined.group_by(["par", "club"])
        .agg(pl.col("distance_meters").mean().alias("avg_distance_m"))
        if not trimmed_joined.is_empty()
        else pl.DataFrame(schema={"par": pl.Int64, "club": pl.String, "avg_distance_m": pl.Float64})
    )

    return (
        joined.group_by(["par", "club"])
        .agg(
            [
                pl.len().alias("second_shots"),
                pl.col("round_id").n_unique().alias("rounds"),
                _pct_expr(pl.col("to_par") <= 0).alias("par_or_better_pct"),
                _pct_expr(pl.col("to_par") >= 1).alias("bogey_or_worse_pct"),
                _pct_expr(pl.col("to_par") >= 2).alias("double_or_worse_pct"),
                pl.col("to_par").cast(pl.Float64, strict=False).mean().alias("avg_to_par"),
            ]
        )
        .join(distance_summary, on=["par", "club"], how="left")
        .sort(
            by=["par", "second_shots", "avg_to_par", "club"],
            descending=[False, True, True, False],
            nulls_last=True,
        )
        .with_columns(
            [
                pl.col("avg_distance_m").round(1),
                pl.col("par_or_better_pct").round(2),
                pl.col("bogey_or_worse_pct").round(2),
                pl.col("double_or_worse_pct").round(2),
                pl.col("avg_to_par").round(2),
            ]
        )
    )


def build_club_context_stats(holes: pl.DataFrame, shots: pl.DataFrame) -> pl.DataFrame:
    if (
        holes.is_empty()
        or shots.is_empty()
        or "round_id" not in holes.columns
        or "hole_number" not in holes.columns
        or "par" not in holes.columns
        or "round_id" not in shots.columns
        or "hole_number" not in shots.columns
        or "shot_number" not in shots.columns
    ):
        return pl.DataFrame()

    hole_scoring = _holes_with_relative_score(holes)
    joined = (
        shots.join(
            holes.select(["round_id", "hole_number", "par"]),
            on=["round_id", "hole_number"],
            how="inner",
        )
        .join(
            hole_scoring.select(["round_id", "hole_number", "to_par"]),
            on=["round_id", "hole_number"],
            how="left",
        )
        .with_columns(
            [
                pl.col("par").cast(pl.Int64, strict=False).alias("par"),
                pl.col("shot_number").cast(pl.Int64, strict=False).alias("shot_number"),
                pl.col("club")
                .cast(pl.String, strict=False)
                .fill_null("Unknown")
                .str.strip_chars()
                .alias("club"),
                pl.col("lie")
                .cast(pl.String, strict=False)
                .fill_null("Unknown")
                .str.strip_chars()
                .alias("lie"),
                pl.col("shot_type")
                .cast(pl.String, strict=False)
                .fill_null("UNKNOWN")
                .str.strip_chars()
                .alias("shot_type"),
                pl.col("distance_meters").cast(pl.Float64, strict=False).alias("distance_meters"),
            ]
        )
        .with_columns(_club_context_expr().alias("context"))
    )
    if joined.is_empty():
        return pl.DataFrame()

    trimmed = trim_distance_outliers(joined, group_columns=["club", "context"])
    distance_summary = (
        trimmed.group_by(["club", "context"])
        .agg(pl.col("distance_meters").mean().alias("avg_distance_m"))
        if not trimmed.is_empty()
        else pl.DataFrame(
            schema={
                "club": pl.String,
                "context": pl.String,
                "avg_distance_m": pl.Float64,
            }
        )
    )

    return (
        joined.group_by(["club", "context"])
        .agg(
            [
                pl.len().alias("shots"),
                pl.col("round_id").n_unique().alias("rounds"),
                _pct_expr(pl.col("to_par") <= 0).alias("par_or_better_pct"),
                _pct_expr(pl.col("to_par") >= 1).alias("bogey_or_worse_pct"),
                _pct_expr(pl.col("to_par") >= 2).alias("double_or_worse_pct"),
                pl.col("to_par").cast(pl.Float64, strict=False).mean().alias("avg_to_par"),
                _format_mode_expr("shot_type").alias("shot_type"),
                _format_mode_expr("lie").alias("lie"),
            ]
        )
        .join(distance_summary, on=["club", "context"], how="left")
        .sort(
            by=["shots", "club", "context"],
            descending=[True, False, False],
            nulls_last=True,
        )
        .with_columns(
            [
                pl.col("avg_distance_m").round(1),
                pl.col("par_or_better_pct").round(2),
                pl.col("bogey_or_worse_pct").round(2),
                pl.col("double_or_worse_pct").round(2),
                pl.col("avg_to_par").round(2),
            ]
        )
    )


def build_metric_trend_series(trends: pl.DataFrame, metric: str) -> pl.DataFrame:
    metric_columns = TREND_METRIC_COLUMNS.get(metric)
    if metric_columns is None:
        supported = ", ".join(sorted(TREND_METRIC_COLUMNS))
        msg = f"Unsupported trend metric: {metric}. Use one of: {supported}."
        raise ValueError(msg)
    if trends.is_empty():
        return pl.DataFrame()

    round_column, window_column, delta_column = metric_columns
    required_columns = {
        "played_on",
        "round_id",
        "course_name",
        "window",
        round_column,
        window_column,
        delta_column,
    }
    if not required_columns.issubset(set(trends.columns)):
        return pl.DataFrame()

    return trends.select(
        [
            "played_on",
            "round_id",
            "course_name",
            "window",
            pl.lit(metric).alias("metric"),
            pl.col(round_column).alias("round_value"),
            pl.col(window_column).alias("window_value"),
            pl.col(delta_column).alias("delta_value"),
        ]
    )


def _mean(series: pl.Series) -> float:
    value = series.cast(pl.Float64, strict=False).drop_nulls().mean()
    return float(cast(int | float, value)) if value is not None else 0.0


def _club_context_expr() -> pl.Expr:
    return (
        pl.when(pl.col("shot_type") == "PUTT")
        .then(pl.lit("putt"))
        .when(pl.col("shot_number") == 1)
        .then(
            pl.when(pl.col("par") == 3)
            .then(pl.lit("tee_par_3"))
            .when(pl.col("par") == 4)
            .then(pl.lit("tee_par_4"))
            .when(pl.col("par") == 5)
            .then(pl.lit("tee_par_5"))
            .otherwise(pl.lit("tee_other"))
        )
        .when((pl.col("shot_number") == 2) & (pl.col("par") == 4))
        .then(pl.lit("approach_par_4"))
        .when((pl.col("shot_number") == 2) & (pl.col("par") == 5))
        .then(pl.lit("second_par_5"))
        .when(pl.col("shot_type").is_in(["CHIP", "PITCH", "BUNKER"]))
        .then(pl.lit("short_game"))
        .when(pl.col("lie").str.to_lowercase().str.contains("green"))
        .then(pl.lit("putt"))
        .when(pl.col("shot_type").is_in(["LAYUP", "RECOVERY"]))
        .then(pl.col("shot_type").str.to_lowercase())
        .otherwise(pl.lit("other"))
    )


def _format_mode_expr(column: str) -> pl.Expr:
    return (
        pl.col(column)
        .drop_nulls()
        .mode()
        .first()
        .fill_null("Unknown")
        .alias(column)
    )


def _sort_rounds_for_trends(rounds: pl.DataFrame) -> pl.DataFrame:
    sort_columns: list[pl.Expr] = []
    if "played_on" in rounds.columns:
        sort_columns.append(pl.col("played_on").str.to_date(strict=False).alias("_played_on_date"))
    if "round_id" in rounds.columns:
        sort_columns.append(pl.col("round_id").cast(pl.Int64, strict=False).alias("_round_id_sort"))
    if not sort_columns:
        return rounds

    working = rounds.with_columns(sort_columns)
    return working.sort(
        by=["_played_on_date", "_round_id_sort"],
        descending=[False, False],
        nulls_last=True,
    ).drop(["_played_on_date", "_round_id_sort"], strict=False)


def _extract_round_ids(round_rows: Sequence[dict[str, object]]) -> list[int]:
    round_ids: list[int] = []
    for row in round_rows:
        round_id = row.get("round_id")
        if isinstance(round_id, int):
            round_ids.append(round_id)
    return round_ids


def _filter_by_round_ids(frame: pl.DataFrame, round_ids: list[int]) -> pl.DataFrame:
    if frame.is_empty() or "round_id" not in frame.columns or not round_ids:
        return frame.head(0)
    return frame.filter(pl.col("round_id").is_in(round_ids))


def _trend_course_name(round_row: dict[str, object]) -> str | None:
    for key in ("display_course_name", "course_name", "location_name"):
        value = round_row.get(key)
        if isinstance(value, str) and value.strip():
            return value
    return None


def _as_float(value: object) -> float:
    if isinstance(value, bool):
        return float(value)
    if isinstance(value, int | float):
        return float(value)
    return 0.0


def _delta_metric(
    current_summary: dict[str, float | int | str],
    previous_summary: dict[str, float | int | str] | None,
    key: str,
) -> float | None:
    if previous_summary is None:
        return None
    return round(_as_float(current_summary.get(key)) - _as_float(previous_summary.get(key)), 2)


def trim_distance_outliers(frame: pl.DataFrame, *, group_columns: list[str]) -> pl.DataFrame:
    if frame.is_empty() or "distance_meters" not in frame.columns:
        return frame

    working = frame.with_row_index("_row_idx").with_columns(
        pl.col("distance_meters").cast(pl.Float64, strict=False).alias("_distance_value")
    )
    if working.is_empty():
        return frame

    if group_columns:
        stats = working.group_by(group_columns).agg(
            [
                pl.len().alias("_distance_count"),
                pl.col("_distance_value").drop_nulls().mean().alias("_distance_mean"),
                pl.col("_distance_value").drop_nulls().std(ddof=1).alias("_distance_std"),
            ]
        )
        enriched = working.join(stats, on=group_columns, how="left")
    else:
        non_null_distances = working["_distance_value"].drop_nulls()
        enriched = working.with_columns(
            [
                pl.lit(working.height).alias("_distance_count"),
                pl.lit(non_null_distances.mean(), dtype=pl.Float64).alias("_distance_mean"),
                pl.lit(non_null_distances.std(ddof=1), dtype=pl.Float64).alias("_distance_std"),
            ]
        )

    trimmed = enriched.filter(
        pl.col("_distance_value").is_null()
        | (pl.col("_distance_count") < DISTANCE_OUTLIER_MIN_GROUP_SIZE)
        | pl.col("_distance_std").is_null()
        | (pl.col("_distance_std") == 0)
        | (
            (pl.col("_distance_value") - pl.col("_distance_mean")).abs()
            <= pl.col("_distance_std") * DISTANCE_OUTLIER_STDDEV_THRESHOLD
        )
    )
    return trimmed.select(frame.columns)


def _pct_expr(expr: pl.Expr) -> pl.Expr:
    return expr.cast(pl.Float64, strict=False).mean() * 100


def _format_hole_focus(
    hole_stats: pl.DataFrame,
    column: str,
    *,
    suffix: str,
    highest: bool,
    limit: int = 3,
) -> str:
    if column not in hole_stats.columns:
        return ""
    valid = hole_stats.drop_nulls([column]).sort(
        by=[column, "hole_number"],
        descending=[highest, False],
        nulls_last=True,
    )
    if valid.is_empty():
        return ""
    parts: list[str] = []
    for row in valid.head(limit).iter_rows(named=True):
        hole_number = row.get("hole_number")
        value = row.get(column)
        if not isinstance(hole_number, int | float) or not isinstance(value, int | float):
            continue
        parts.append(f"{int(hole_number)} ({float(value):.2f}{suffix})")
    return ", ".join(parts)


def _ratio_mean(series: pl.Series) -> float:
    numeric = series.cast(pl.Float64, strict=False).drop_nulls()
    value = numeric.mean()
    return float(cast(int | float, value)) if value is not None else 0.0


def _nonnull_count_per_18(frame: pl.DataFrame, column: str) -> float:
    if frame.is_empty() or "round_id" not in frame.columns or column not in frame.columns:
        return 0.0
    valid = frame.drop_nulls([column]).group_by("round_id").agg(pl.len().alias("count"))
    hole_counts = _round_hole_counts(frame)
    return _per_round_metric_18_equivalent(valid, hole_counts, "count")


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
    if value_column == "distance_meters":
        filtered = trim_distance_outliers(filtered, group_columns=[])
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
