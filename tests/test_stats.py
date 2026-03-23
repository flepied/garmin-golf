import polars as pl

from garmin_golf.stats import build_round_stats, build_summary_stats


def test_build_summary_stats() -> None:
    rounds = pl.DataFrame(
        [
            {"round_id": 1, "total_score": 84, "total_par": 72},
            {"round_id": 2, "total_score": 81, "total_par": 72},
        ]
    )
    holes = pl.DataFrame(
        [
            {"round_id": 1, "putts": 2, "gir": True, "fairway_hit": True, "penalties": 0},
            {"round_id": 1, "putts": 1, "gir": False, "fairway_hit": False, "penalties": 1},
            {"round_id": 2, "putts": 2, "gir": True, "fairway_hit": True, "penalties": 0},
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
    assert summary["average_score"] == 82.5
    assert summary["average_to_par"] == 10.5
    assert summary["gir_pct"] == 66.67
    assert summary["fairway_hit_pct"] == 66.67
    assert summary["average_putts_per_hole"] == 1.67
    assert summary["average_putts_per_round"] == 2.5
    assert summary["average_hole_score"] == 0.0
    assert summary["penalties"] == 1
    assert summary["penalties_per_round"] == 0.5
    assert summary["total_shots"] == 4
    assert summary["average_shots_per_round"] == 2.0
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
    assert summary["average_score"] == 79.0
    assert summary["total_shots"] == 1


def test_build_summary_stats_without_holes() -> None:
    rounds = pl.DataFrame([{"round_id": 7, "total_score": None, "total_par": None}])
    holes = pl.DataFrame()

    summary = build_summary_stats(rounds, holes, pl.DataFrame())

    assert summary["rounds_played"] == 1
    assert summary["average_score"] == 0.0
    assert summary["gir_pct"] == 0.0
