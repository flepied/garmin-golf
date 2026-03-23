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

    summary = build_summary_stats(rounds, holes)

    assert summary["rounds_played"] == 2
    assert summary["average_score"] == 82.5
    assert summary["average_to_par"] == 10.5
    assert summary["gir_pct"] == 66.67
    assert summary["fairway_hit_pct"] == 66.67
    assert summary["penalties"] == 1


def test_build_round_stats() -> None:
    rounds = pl.DataFrame([{"round_id": 7, "total_score": 79, "total_par": 72}])
    holes = pl.DataFrame(
        [{"round_id": 7, "putts": 2, "gir": True, "fairway_hit": False, "penalties": 0}]
    )

    summary = build_round_stats(rounds, holes, 7)

    assert summary["round_id"] == 7
    assert summary["average_score"] == 79.0


def test_build_summary_stats_without_holes() -> None:
    rounds = pl.DataFrame([{"round_id": 7, "total_score": None, "total_par": None}])
    holes = pl.DataFrame()

    summary = build_summary_stats(rounds, holes)

    assert summary["rounds_played"] == 1
    assert summary["average_score"] == 0.0
    assert summary["gir_pct"] == 0.0
