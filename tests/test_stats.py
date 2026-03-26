import polars as pl

from garmin_golf.stats import (
    build_club_context_stats,
    build_course_focus_stats,
    build_course_hole_stats,
    build_practice_focus_stats,
    build_round_stats,
    build_round_trends,
    build_second_shot_stats,
    build_summary_stats,
    trim_distance_outliers,
)


def test_build_summary_stats() -> None:
    rounds = pl.DataFrame(
        [
            {"round_id": 1, "total_score": 84, "total_par": 72},
            {"round_id": 2, "total_score": 81, "total_par": 72},
        ]
    )
    holes = pl.DataFrame(
        [
            {
                "round_id": 1,
                "putts": 2,
                "gir": True,
                "fairway_hit": True,
                "penalties": 0,
            },
            {
                "round_id": 1,
                "putts": 1,
                "gir": False,
                "fairway_hit": False,
                "penalties": 1,
            },
            {
                "round_id": 2,
                "putts": 2,
                "gir": True,
                "fairway_hit": True,
                "penalties": 0,
            },
        ]
    )
    shots = pl.DataFrame(
        [
            {"round_id": 1, "shot_type": "TEE", "club": "Driver", "distance_meters": 200.0},
            {"round_id": 1, "shot_type": "PUTT", "club": "Putter", "distance_meters": 3.0},
            {"round_id": 2, "shot_type": "TEE", "club": "Driver", "distance_meters": 220.0},
            {"round_id": 2, "shot_type": "APPROACH", "club": "7 Iron", "distance_meters": 140.0},
        ]
    )

    summary = build_summary_stats(rounds, holes, shots)

    assert summary["rounds_played"] == 2
    assert summary["rounds_18_hole"] == 0
    assert summary["rounds_9_hole"] == 0
    assert summary["average_score"] == 0.0
    assert summary["average_to_par"] == 0.0
    assert summary["gir_pct"] == 66.67
    assert summary["gir_per_18"] == 0.0
    assert summary["fir_pct"] == 66.67
    assert summary["fir_per_18"] == 0.0
    assert summary["fairway_hit_pct"] == 66.67
    assert summary["scrambling_pct"] == 0.0
    assert summary["scrambles_per_18"] == 0.0
    assert summary["birdie_or_better_pct"] == 0.0
    assert summary["par_pct"] == 0.0
    assert summary["bogey_or_worse_pct"] == 0.0
    assert summary["bogeys_or_worse_per_18"] == 0.0
    assert summary["double_bogey_or_worse_pct"] == 0.0
    assert summary["double_bogeys_or_worse_per_18"] == 0.0
    assert summary["three_putt_pct"] == 0.0
    assert summary["three_putts_per_18"] == 0.0
    assert summary["penalty_hole_pct"] == 33.33
    assert summary["par_3_average_to_par"] == 0.0
    assert summary["par_4_average_to_par"] == 0.0
    assert summary["par_5_average_to_par"] == 0.0
    assert summary["average_putts_per_hole"] == 1.67
    assert summary["average_putts_per_round"] == 2.5
    assert summary["average_putts_per_18"] == 0.0
    assert summary["average_hole_score"] == 0.0
    assert summary["penalties"] == 1
    assert summary["penalties_per_round"] == 0.5
    assert summary["penalties_per_18"] == 0.0
    assert summary["total_shots"] == 4
    assert summary["average_shots_per_round"] == 2.0
    assert summary["average_shots_per_18"] == 0.0
    assert summary["average_tee_shot_distance_m"] == 210.0
    assert summary["average_putt_distance_m"] == 3.0
    assert summary["shot_type_breakdown"] == "TEE: 2, APPROACH: 1, PUTT: 1"
    assert summary["club_usage_top5"] == "Driver: 2, 7 Iron: 1, Putter: 1"


def test_build_round_stats() -> None:
    rounds = pl.DataFrame([{"round_id": 7, "total_score": 79, "total_par": 72}])
    holes = pl.DataFrame(
        [{"round_id": 7, "putts": 2, "gir": True, "fairway_hit": False, "penalties": 0}]
    )
    shots = pl.DataFrame(
        [{"round_id": 7, "shot_type": "TEE", "club": "Driver", "distance_meters": 210.0}]
    )

    summary = build_round_stats(rounds, holes, shots, 7)

    assert summary["round_id"] == 7
    assert summary["average_score"] == 0.0
    assert summary["total_shots"] == 1


def test_build_summary_stats_without_holes() -> None:
    rounds = pl.DataFrame([{"round_id": 7, "total_score": None, "total_par": None}])
    holes = pl.DataFrame()

    summary = build_summary_stats(rounds, holes, pl.DataFrame())

    assert summary["rounds_played"] == 1
    assert summary["average_score"] == 0.0
    assert summary["gir_pct"] == 0.0
    assert summary["gir_per_18"] == 0.0
    assert summary["fir_pct"] == 0.0
    assert summary["fir_per_18"] == 0.0
    assert summary["scrambling_pct"] == 0.0
    assert summary["scrambles_per_18"] == 0.0


def test_build_summary_stats_with_9_and_18_hole_rounds() -> None:
    rounds = pl.DataFrame(
        [
            {"round_id": 1, "total_score": 40, "total_par": 36},
            {"round_id": 2, "total_score": 82, "total_par": 72},
        ]
    )
    holes = pl.DataFrame(
        [
            {
                "round_id": 1,
                "hole_number": hole,
                "putts": 2,
                "par": 4,
                "gir": True,
                "fairway_hit": True,
                "penalties": 0,
                "strokes": 4,
            }
            for hole in range(1, 10)
        ]
        + [
            {
                "round_id": 2,
                "hole_number": hole,
                "putts": 2,
                "par": 4,
                "gir": False,
                "fairway_hit": False,
                "penalties": 1,
                "strokes": 5,
            }
            for hole in range(1, 19)
        ]
    )
    shots = pl.DataFrame(
        [{"round_id": 1, "shot_type": "TEE", "distance_meters": 200.0}]
        * 9
        + [{"round_id": 2, "shot_type": "TEE", "distance_meters": 210.0}] * 18
    )

    summary = build_summary_stats(rounds, holes, shots)

    assert summary["rounds_9_hole"] == 1
    assert summary["rounds_18_hole"] == 1
    assert summary["average_score"] == 81.0
    assert summary["average_to_par"] == 9.0
    assert summary["scrambling_pct"] == 0.0
    assert summary["scrambles_per_18"] == 0.0
    assert summary["birdie_or_better_pct"] == 0.0
    assert summary["par_pct"] == 33.33
    assert summary["bogey_or_worse_pct"] == 66.67
    assert summary["bogeys_or_worse_per_18"] == 9.0
    assert summary["double_bogey_or_worse_pct"] == 0.0
    assert summary["double_bogeys_or_worse_per_18"] == 0.0
    assert summary["three_putt_pct"] == 0.0
    assert summary["three_putts_per_18"] == 0.0
    assert summary["gir_per_18"] == 9.0
    assert summary["fir_per_18"] == 9.0
    assert summary["penalty_hole_pct"] == 66.67
    assert summary["par_3_average_to_par"] == 0.0
    assert summary["par_4_average_to_par"] == 0.67
    assert summary["par_5_average_to_par"] == 0.0
    assert summary["average_putts_per_round"] == 27.0
    assert summary["average_putts_per_18"] == 36.0
    assert summary["average_shots_per_round"] == 13.5
    assert summary["average_shots_per_18"] == 18.0


def test_trim_distance_outliers_drops_large_outlier_after_minimum_sample() -> None:
    shots = pl.DataFrame(
        [
            {"club_id": 1, "distance_meters": 100.0},
            {"club_id": 1, "distance_meters": 101.0},
            {"club_id": 1, "distance_meters": 102.0},
            {"club_id": 1, "distance_meters": 103.0},
            {"club_id": 1, "distance_meters": 104.0},
            {"club_id": 1, "distance_meters": 250.0},
        ]
    )

    trimmed = trim_distance_outliers(shots, group_columns=["club_id"])

    assert trimmed.height == 5
    assert 250.0 not in trimmed["distance_meters"].to_list()


def test_trim_distance_outliers_keeps_small_samples_and_zero_variance() -> None:
    small_sample = pl.DataFrame(
        [
            {"club_id": 1, "distance_meters": 100.0},
            {"club_id": 1, "distance_meters": 101.0},
            {"club_id": 1, "distance_meters": 250.0},
        ]
    )
    zero_variance = pl.DataFrame(
        [
            {"club_id": 2, "distance_meters": 120.0},
            {"club_id": 2, "distance_meters": 120.0},
            {"club_id": 2, "distance_meters": 120.0},
            {"club_id": 2, "distance_meters": 120.0},
            {"club_id": 2, "distance_meters": 120.0},
            {"club_id": 2, "distance_meters": 120.0},
        ]
    )

    trimmed_small = trim_distance_outliers(small_sample, group_columns=["club_id"])
    trimmed_zero_variance = trim_distance_outliers(zero_variance, group_columns=["club_id"])

    assert trimmed_small.height == 3
    assert trimmed_zero_variance.height == 6


def test_build_summary_stats_trims_distance_outliers_for_average_distances() -> None:
    rounds = pl.DataFrame([{"round_id": 1, "total_score": 84, "total_par": 72}])
    holes = pl.DataFrame()
    shots = pl.DataFrame(
        [
            {"round_id": 1, "shot_type": "TEE", "club": "Driver", "distance_meters": 200.0},
            {"round_id": 1, "shot_type": "TEE", "club": "Driver", "distance_meters": 201.0},
            {"round_id": 1, "shot_type": "TEE", "club": "Driver", "distance_meters": 202.0},
            {"round_id": 1, "shot_type": "TEE", "club": "Driver", "distance_meters": 203.0},
            {"round_id": 1, "shot_type": "TEE", "club": "Driver", "distance_meters": 204.0},
            {"round_id": 1, "shot_type": "TEE", "club": "Driver", "distance_meters": 350.0},
            {"round_id": 1, "shot_type": "PUTT", "club": "Putter", "distance_meters": 3.0},
        ]
    )

    summary = build_summary_stats(rounds, holes, shots)

    assert summary["total_shots"] == 7
    assert summary["average_tee_shot_distance_m"] == 202.0


def test_build_course_hole_stats() -> None:
    rounds = pl.DataFrame(
        [
            {"round_id": 1, "display_course_name": "Blue Hills"},
            {"round_id": 2, "display_course_name": "Blue Hills"},
        ]
    )
    holes = pl.DataFrame(
        [
            {
                "round_id": 1,
                "hole_number": 1,
                "par": 4,
                "strokes": 5,
                "putts": 2,
                "gir": False,
                "fairway_hit": False,
                "penalties": 1,
            },
            {
                "round_id": 2,
                "hole_number": 1,
                "par": 4,
                "strokes": 4,
                "putts": 1,
                "gir": True,
                "fairway_hit": True,
                "penalties": 0,
            },
            {
                "round_id": 1,
                "hole_number": 2,
                "par": 3,
                "strokes": 3,
                "putts": 1,
                "gir": True,
                "fairway_hit": None,
                "penalties": 0,
            },
            {
                "round_id": 2,
                "hole_number": 2,
                "par": 3,
                "strokes": 5,
                "putts": 3,
                "gir": False,
                "fairway_hit": None,
                "penalties": 0,
            },
        ]
    )

    hole_stats = build_course_hole_stats(rounds, holes)

    first_hole = hole_stats.filter(pl.col("hole_number") == 2).row(0, named=True)
    assert first_hole["rounds_played"] == 2
    assert first_hole["avg_strokes"] == 4.0
    assert first_hole["avg_to_par"] == 1.0
    assert first_hole["three_putt_pct"] == 50.0

    second_hole = hole_stats.filter(pl.col("hole_number") == 1).row(0, named=True)
    assert second_hole["par_or_better_pct"] == 50.0
    assert second_hole["penalty_rate"] == 0.5


def test_build_course_focus_stats() -> None:
    hole_stats = pl.DataFrame(
        [
            {
                "hole_number": 4,
                "avg_to_par": 1.2,
                "penalty_rate": 0.8,
                "three_putt_pct": 5.0,
                "gir_pct": 30.0,
            },
            {
                "hole_number": 7,
                "avg_to_par": 0.7,
                "penalty_rate": 0.2,
                "three_putt_pct": 20.0,
                "gir_pct": 40.0,
            },
            {
                "hole_number": 2,
                "avg_to_par": 0.1,
                "penalty_rate": 0.0,
                "three_putt_pct": 10.0,
                "gir_pct": 70.0,
            },
        ]
    )

    focus = build_course_focus_stats(hole_stats)

    assert focus["hardest_holes"].startswith("4 (1.20 to par)")
    assert focus["penalty_holes"].startswith("4 (0.80 penalties)")
    assert focus["three_putt_holes"].startswith("7 (20.00% 3-putts)")
    assert focus["gir_trouble_holes"].startswith("4 (30.00% GIR)")


def test_build_practice_focus_stats() -> None:
    rounds = pl.DataFrame(
        [
            {"round_id": 1, "total_score": 84, "total_par": 72},
            {"round_id": 2, "total_score": 88, "total_par": 72},
        ]
    )
    holes = pl.DataFrame(
        [
            {
                "round_id": round_id,
                "hole_number": hole,
                "par": 4,
                "strokes": 5 if hole <= 12 else 4,
                "putts": 3 if hole <= 3 else 2,
                "gir": hole > 10,
                "fairway_hit": hole > 8,
                "penalties": 1 if hole <= 2 else 0,
            }
            for round_id in (1, 2)
            for hole in range(1, 19)
        ]
    )
    shots = pl.DataFrame(
        [
            {"round_id": 1, "shot_type": "TEE", "club": "Driver", "distance_meters": 205.0},
            {"round_id": 2, "shot_type": "TEE", "club": "Driver", "distance_meters": 210.0},
        ]
    )

    focus = build_practice_focus_stats(rounds, holes, shots)

    assert focus["rounds_played"] == 2
    combined = " ".join(str(focus[key]) for key in ("priority_1", "priority_2", "priority_3"))
    assert "Scrambling:" in combined
    assert "Penalty control:" in combined
    assert "Lag putting:" in combined
    assert str(focus["priority_3"]) != ""
    assert float(focus["estimated_strokes_to_save_per_18"]) > 0.0


def test_build_round_trends() -> None:
    rounds = pl.DataFrame(
        [
            {
                "round_id": 1,
                "played_on": "2025-06-01",
                "course_name": "Blue Hills",
                "total_score": 90,
                "total_par": 72,
            },
            {
                "round_id": 2,
                "played_on": "2025-06-08",
                "course_name": "Blue Hills",
                "total_score": 81,
                "total_par": 72,
            },
            {
                "round_id": 3,
                "played_on": "2025-06-15",
                "course_name": "Red Oaks",
                "total_score": 72,
                "total_par": 72,
            },
        ]
    )
    holes = pl.DataFrame(
        [
            {
                "round_id": 1,
                "hole_number": hole,
                "par": 4,
                "strokes": 5,
                "putts": 2,
                "gir": False,
                "fairway_hit": False,
                "penalties": 1 if hole <= 2 else 0,
            }
            for hole in range(1, 19)
        ]
        + [
            {
                "round_id": 2,
                "hole_number": hole,
                "par": 4,
                "strokes": 4 if hole <= 9 else 5,
                "putts": 2,
                "gir": hole <= 9,
                "fairway_hit": hole <= 9,
                "penalties": 1 if hole == 1 else 0,
            }
            for hole in range(1, 19)
        ]
        + [
            {
                "round_id": 3,
                "hole_number": hole,
                "par": 4,
                "strokes": 4,
                "putts": 2,
                "gir": True,
                "fairway_hit": True,
                "penalties": 0,
            }
            for hole in range(1, 19)
        ]
    )

    trends = build_round_trends(rounds, holes, pl.DataFrame(), window=2)

    assert trends.height == 3
    latest = trends.row(0, named=True)
    assert latest["round_id"] == 3
    assert latest["course_name"] == "Red Oaks"
    assert latest["round_to_par"] == 0.0
    assert latest["window_average_to_par"] == 4.5
    assert latest["delta_average_to_par"] == -13.5
    assert latest["window_gir_pct"] == 75.0
    assert latest["delta_gir_pct"] == 75.0
    assert latest["window_penalties_per_18"] == 0.5
    assert latest["delta_penalties_per_18"] == -1.5

    earliest = trends.row(2, named=True)
    assert earliest["round_id"] == 1
    assert earliest["rounds_in_window"] == 1
    assert earliest["rounds_in_previous_window"] == 0
    assert earliest["delta_average_to_par"] is None


def test_build_club_context_stats() -> None:
    holes = pl.DataFrame(
        [
            {"round_id": 1, "hole_number": 1, "par": 4, "strokes": 4},
            {"round_id": 1, "hole_number": 2, "par": 3, "strokes": 3},
            {"round_id": 1, "hole_number": 3, "par": 5, "strokes": 5},
        ]
    )
    shots = pl.DataFrame(
        [
            {
                "round_id": 1,
                "hole_number": 1,
                "shot_number": 1,
                "club": "Driver",
                "shot_type": "TEE",
                "lie": "TEE_BOX",
                "distance_meters": 220.0,
            },
            {
                "round_id": 1,
                "hole_number": 1,
                "shot_number": 2,
                "club": "8 Iron",
                "shot_type": "APPROACH",
                "lie": "FAIRWAY",
                "distance_meters": 135.0,
            },
            {
                "round_id": 1,
                "hole_number": 2,
                "shot_number": 1,
                "club": "7 Iron",
                "shot_type": "APPROACH",
                "lie": "TEE_BOX",
                "distance_meters": 155.0,
            },
            {
                "round_id": 1,
                "hole_number": 3,
                "shot_number": 2,
                "club": "3 Wood",
                "shot_type": "LAYUP",
                "lie": "FAIRWAY",
                "distance_meters": 205.0,
            },
        ]
    )

    stats = build_club_context_stats(holes, shots)

    assert stats.height == 4
    driver = stats.filter(pl.col("club") == "Driver").row(0, named=True)
    assert driver["context"] == "tee_par_4"
    assert driver["avg_distance_m"] == 220.0

    approach = stats.filter(pl.col("club") == "8 Iron").row(0, named=True)
    assert approach["context"] == "approach_par_4"
    assert approach["lie"] == "FAIRWAY"

    par3 = stats.filter(pl.col("club") == "7 Iron").row(0, named=True)
    assert par3["context"] == "tee_par_3"

    par5 = stats.filter(pl.col("club") == "3 Wood").row(0, named=True)
    assert par5["context"] == "second_par_5"


def test_build_second_shot_stats() -> None:
    holes = pl.DataFrame(
        [
            {"round_id": 1, "hole_number": 1, "par": 4, "strokes": 4},
            {"round_id": 1, "hole_number": 2, "par": 5, "strokes": 6},
            {"round_id": 2, "hole_number": 1, "par": 4, "strokes": 5},
        ]
    )
    shots = pl.DataFrame(
        [
            {
                "round_id": 1,
                "hole_number": 1,
                "shot_number": 2,
                "club": "8 Iron",
                "distance_meters": 135.0,
            },
            {
                "round_id": 1,
                "hole_number": 2,
                "shot_number": 2,
                "club": "3 Wood",
                "distance_meters": 205.0,
            },
            {
                "round_id": 2,
                "hole_number": 1,
                "shot_number": 2,
                "club": "3 Wood",
                "distance_meters": 195.0,
            },
            {
                "round_id": 2,
                "hole_number": 1,
                "shot_number": 3,
                "club": "Wedge",
                "distance_meters": 80.0,
            },
        ]
    )

    stats = build_second_shot_stats(holes, shots)

    assert stats.height == 3
    par4_three_wood = stats.filter(
        (pl.col("par") == 4) & (pl.col("club") == "3 Wood")
    ).row(0, named=True)
    assert par4_three_wood["second_shots"] == 1
    assert par4_three_wood["avg_distance_m"] == 195.0
    assert par4_three_wood["bogey_or_worse_pct"] == 100.0
    par5_three_wood = stats.filter(
        (pl.col("par") == 5) & (pl.col("club") == "3 Wood")
    ).row(0, named=True)
    assert par5_three_wood["avg_to_par"] == 1.0


def test_build_second_shot_stats_trims_distance_outliers_only_for_distance_average() -> None:
    holes = pl.DataFrame(
        [
            {"round_id": 1, "hole_number": 1, "par": 4, "strokes": 4},
            {"round_id": 1, "hole_number": 2, "par": 4, "strokes": 5},
            {"round_id": 1, "hole_number": 3, "par": 4, "strokes": 4},
            {"round_id": 1, "hole_number": 4, "par": 4, "strokes": 5},
            {"round_id": 1, "hole_number": 5, "par": 4, "strokes": 4},
            {"round_id": 1, "hole_number": 6, "par": 4, "strokes": 6},
        ]
    )
    shots = pl.DataFrame(
        [
            {
                "round_id": 1,
                "hole_number": 1,
                "shot_number": 2,
                "club": "3 Wood",
                "distance_meters": 150.0,
            },
            {
                "round_id": 1,
                "hole_number": 2,
                "shot_number": 2,
                "club": "3 Wood",
                "distance_meters": 151.0,
            },
            {
                "round_id": 1,
                "hole_number": 3,
                "shot_number": 2,
                "club": "3 Wood",
                "distance_meters": 152.0,
            },
            {
                "round_id": 1,
                "hole_number": 4,
                "shot_number": 2,
                "club": "3 Wood",
                "distance_meters": 153.0,
            },
            {
                "round_id": 1,
                "hole_number": 5,
                "shot_number": 2,
                "club": "3 Wood",
                "distance_meters": 154.0,
            },
            {
                "round_id": 1,
                "hole_number": 6,
                "shot_number": 2,
                "club": "3 Wood",
                "distance_meters": 320.0,
            },
        ]
    )

    stats = build_second_shot_stats(holes, shots)

    par4_three_wood = stats.filter(
        (pl.col("par") == 4) & (pl.col("club") == "3 Wood")
    ).row(0, named=True)
    assert par4_three_wood["second_shots"] == 6
    assert par4_three_wood["avg_distance_m"] == 152.0
