import polars as pl

from garmin_golf.stats import (
    build_course_focus_stats,
    build_course_hole_stats,
    build_round_stats,
    build_summary_stats,
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
