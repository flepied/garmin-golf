from __future__ import annotations

import polars as pl

from garmin_golf.analysis import analyze_data_quality, analyze_player


def _rounds(count: int = 5) -> pl.DataFrame:
    return pl.DataFrame(
        [
            {
                "round_id": index,
                "played_on": f"2026-01-{index:02d}",
                "total_score": 90,
                "total_par": 72,
            }
            for index in range(1, count + 1)
        ]
    )


def test_data_quality_reports_unavailable_domains_without_optional_columns() -> None:
    result = analyze_data_quality(_rounds(), pl.DataFrame(), pl.DataFrame())

    assert result.data_quality["scoring"].status == "unavailable"
    assert result.scope.rounds == 5


def test_player_analysis_detects_double_or_worse_concentration() -> None:
    rounds = _rounds()
    holes = pl.DataFrame(
        [
            {"round_id": round_id, "hole_number": hole, "par": 4, "strokes": 6, "putts": 2}
            for round_id in range(1, 6)
            for hole in range(1, 19)
        ]
    )

    result = analyze_player(rounds, holes, pl.DataFrame())

    assert result.primary_priority is not None
    assert result.primary_priority.id == "scoring_double_or_worse_concentration"
    assert result.primary_priority.confidence == "high"


def test_excluded_rounds_do_not_enter_aggregate_analysis() -> None:
    rounds = _rounds().with_columns(
        pl.when(pl.col("round_id") == 5).then(True).otherwise(False).alias("exclude_from_stats")
    )
    result = analyze_data_quality(rounds, pl.DataFrame(), pl.DataFrame())

    assert result.scope.rounds == 4
    assert result.scope.excluded_rounds == 1


def test_player_analysis_does_not_recommend_unknown_club() -> None:
    rounds = _rounds(10)
    holes = pl.DataFrame(
        [
            {
                "round_id": round_id,
                "hole_number": hole,
                "par": 4,
                "strokes": 6 if hole == 1 else 4,
                "fairway_shot_outcome": "RIGHT" if hole == 1 else "HIT",
            }
            for round_id in range(1, 11)
            for hole in range(1, 3)
        ]
    )
    shots = pl.DataFrame(
        [
            {"round_id": round_id, "hole_number": hole, "shot_number": 1, "club": "Unknown"}
            for round_id in range(1, 11)
            for hole in range(1, 3)
        ]
    )

    result = analyze_player(rounds, holes, shots)

    assert not any(candidate.category == "tee_shot" for candidate in result.insights)
