# ruff: noqa: E501
from __future__ import annotations

from collections import defaultdict
from datetime import date
from math import asin, cos, radians, sin, sqrt
from statistics import median, pstdev
from typing import Any, cast

import polars as pl

from ..stats import build_course_hole_stats, build_summary_stats
from .models import AnalysisResult, AnalysisStatus, Candidate, Confidence, DomainQuality, Scope

ANALYSIS_VERSION = "1.0"
APPROACH_BUCKETS = (
    (0.0, 50.0, "< 50 m"),
    (50.0, 75.0, "50–75 m"),
    (75.0, 100.0, "75–100 m"),
    (100.0, 125.0, "100–125 m"),
    (125.0, 150.0, "125–150 m"),
    (150.0, 175.0, "150–175 m"),
    (175.0, float("inf"), "> 175 m"),
)
PRIORITY_WEIGHTS = (0.35, 0.20, 0.25, 0.20)


def _as_bool(value: object) -> bool:
    return bool(value) if value is not None else False


def _number(value: object) -> float | None:
    return float(value) if isinstance(value, int | float) else None


def _status(available: int, total: int, *, events: int = 0, rounds: int = 0) -> DomainQuality:
    coverage = round((available / total * 100) if total else 0.0, 2)
    if available == 0:
        state: AnalysisStatus = "unavailable"
    elif coverage >= 80 and events >= 30 and rounds >= 10:
        state = "strong"
    elif coverage >= 50 and events >= 10 and rounds >= 5:
        state = "usable"
    else:
        state = "limited"
    limits = [] if state in {"usable", "strong"} else ["Coverage or sample size is limited."]
    return DomainQuality(
        status=state, available=available, total=total, coverage_pct=coverage, limitations=limits
    )


def _eligible_rounds(rounds: pl.DataFrame) -> tuple[pl.DataFrame, int]:
    if rounds.is_empty():
        return rounds, 0
    excluded = 0
    result = rounds
    if "exclude_from_stats" in result.columns:
        mask = result["exclude_from_stats"].cast(pl.Boolean, strict=False).fill_null(False)
        excluded += int(mask.sum())
        result = result.filter(~mask)
    if "comment" in result.columns:
        match_play = (
            result["comment"]
            .cast(pl.String, strict=False)
            .str.to_lowercase()
            .str.contains("match ?play", literal=False)
            .fill_null(False)
        )
        excluded += int(match_play.sum())
        result = result.filter(~match_play)
    return result, excluded


def select_scope(
    rounds: pl.DataFrame,
    holes: pl.DataFrame,
    shots: pl.DataFrame,
    *,
    date_from: date | None = None,
    date_to: date | None = None,
    last_rounds: int | None = None,
    course: str | None = None,
) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame, Scope]:
    eligible, excluded = _eligible_rounds(rounds)
    if "played_on" in eligible.columns:
        eligible = eligible.with_columns(
            pl.col("played_on").str.to_date(strict=False).alias("_date")
        )
        if date_from is not None:
            eligible = eligible.filter(pl.col("_date") >= pl.lit(date_from))
        if date_to is not None:
            eligible = eligible.filter(pl.col("_date") <= pl.lit(date_to))
    course_column = (
        "display_course_name"
        if "display_course_name" in eligible.columns
        else "course_name"
        if "course_name" in eligible.columns
        else None
    )
    if course is not None and course_column is not None:
        eligible = eligible.filter(pl.col(course_column) == course)
    if last_rounds is not None:
        eligible = eligible.sort(
            ["_date", "round_id"], descending=[True, True], nulls_last=True
        ).head(last_rounds)
    ids = eligible["round_id"].drop_nulls().to_list() if "round_id" in eligible.columns else []
    scoped_holes = (
        holes.filter(pl.col("round_id").is_in(ids))
        if ids and "round_id" in holes.columns
        else holes.head(0)
    )
    scoped_shots = (
        shots.filter(pl.col("round_id").is_in(ids))
        if ids and "round_id" in shots.columns
        else shots.head(0)
    )
    dates = eligible["_date"].drop_nulls().to_list() if "_date" in eligible.columns else []
    courses = eligible[course_column].drop_nulls().n_unique() if course_column is not None else 0
    scope = Scope(
        period={
            "from": min(dates).isoformat() if dates else None,
            "to": max(dates).isoformat() if dates else None,
        },
        rounds=eligible.height,
        holes=scoped_holes.height,
        shots=scoped_shots.height,
        courses=courses,
        excluded_rounds=excluded,
    )
    return eligible.drop("_date", strict=False), scoped_holes, scoped_shots, scope


def data_quality(
    rounds: pl.DataFrame, holes: pl.DataFrame, shots: pl.DataFrame
) -> dict[str, DomainQuality]:
    def nonnull(frame: pl.DataFrame, columns: list[str]) -> int:
        if frame.is_empty() or not set(columns).issubset(frame.columns):
            return 0
        return frame.drop_nulls(columns).height

    expected_holes = rounds.height * 18
    scored_rounds = 0
    if not holes.is_empty() and {"round_id", "strokes", "par"}.issubset(holes.columns):
        scored_rounds = (
            holes.drop_nulls(["strokes", "par"])
            .group_by("round_id")
            .len()
            .filter(pl.col("len") >= 8)
            .height
        )
    fairway_total = (
        holes.filter(pl.col("par").cast(pl.Int64, strict=False) != 3).height
        if "par" in holes.columns
        else 0
    )
    return {
        "scoring": _status(scored_rounds, rounds.height, events=holes.height, rounds=scored_rounds),
        "putting": _status(
            nonnull(holes, ["putts"]),
            holes.height,
            events=nonnull(holes, ["putts"]),
            rounds=rounds.height,
        ),
        "gir": _status(
            nonnull(holes, ["gir"]),
            holes.height,
            events=nonnull(holes, ["gir"]),
            rounds=rounds.height,
        ),
        "fairway": _status(
            nonnull(holes, ["fairway_shot_outcome"]),
            fairway_total,
            events=nonnull(holes, ["fairway_shot_outcome"]),
            rounds=rounds.height,
        ),
        "pin_coordinates": _status(
            nonnull(holes, ["pin_position_lat", "pin_position_lon"]),
            holes.height,
            events=nonnull(holes, ["pin_position_lat", "pin_position_lon"]),
            rounds=rounds.height,
        ),
        "club_labels": _status(
            nonnull(shots, ["club"]),
            shots.height,
            events=nonnull(shots, ["club"]),
            rounds=rounds.height,
        ),
        "shot_distances": _status(
            nonnull(shots, ["distance_meters"]),
            shots.height,
            events=nonnull(shots, ["distance_meters"]),
            rounds=rounds.height,
        ),
        "start_coordinates": _status(
            nonnull(shots, ["start_lat", "start_lon"]),
            shots.height,
            events=nonnull(shots, ["start_lat", "start_lon"]),
            rounds=rounds.height,
        ),
        "end_coordinates": _status(
            nonnull(shots, ["end_lat", "end_lon"]),
            shots.height,
            events=nonnull(shots, ["end_lat", "end_lon"]),
            rounds=rounds.height,
        ),
        "round_shot_coverage": _status(
            min(shots.height, expected_holes),
            expected_holes,
            events=shots.height,
            rounds=rounds.height,
        ),
    }


def _haversine(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    phi1, phi2 = radians(lat1), radians(lat2)
    d_phi, d_lambda = radians(lat2 - lat1), radians(lon2 - lon1)
    a = sin(d_phi / 2) ** 2 + cos(phi1) * cos(phi2) * sin(d_lambda / 2) ** 2
    return 6371008.8 * 2 * asin(sqrt(a))


def _approach_events(holes: pl.DataFrame, shots: pl.DataFrame) -> list[dict[str, Any]]:
    if (
        holes.is_empty()
        or shots.is_empty()
        or not {"round_id", "hole_number"}.issubset(set(holes.columns) | set(shots.columns))
    ):
        return []
    fields = [
        field
        for field in [
            "round_id",
            "hole_number",
            "par",
            "strokes",
            "gir",
            "pin_position_lat",
            "pin_position_lon",
        ]
        if field in holes.columns
    ]
    joined = shots.join(holes.select(fields), on=["round_id", "hole_number"], how="inner")
    rows: list[dict[str, Any]] = []
    for row in joined.to_dicts():
        kind = str(row.get("shot_type") or "").upper()
        if kind == "PUTT" or not (
            kind == "APPROACH" or (row.get("shot_number") == 2 and row.get("par") == 4)
        ):
            continue
        values = [
            row.get(key)
            for key in (
                "start_lat",
                "start_lon",
                "end_lat",
                "end_lon",
                "pin_position_lat",
                "pin_position_lon",
            )
        ]
        if not all(isinstance(value, int | float) for value in values):
            continue
        start_lat, start_lon, end_lat, end_lon, pin_lat, pin_lon = (
            float(cast(int | float, value)) for value in values
        )
        if not (
            -90 <= start_lat <= 90
            and -90 <= end_lat <= 90
            and -90 <= pin_lat <= 90
            and -180 <= start_lon <= 180
            and -180 <= end_lon <= 180
            and -180 <= pin_lon <= 180
        ):
            continue
        row["start_to_pin_m"] = _haversine(start_lat, start_lon, pin_lat, pin_lon)
        row["end_to_pin_m"] = _haversine(end_lat, end_lon, pin_lat, pin_lon)
        row["distance_reduction_m"] = row["start_to_pin_m"] - row["end_to_pin_m"]
        row["bucket"] = next(
            label for low, high, label in APPROACH_BUCKETS if low <= row["start_to_pin_m"] < high
        )
        rows.append(row)
    return rows


def _score_candidate(
    *,
    identifier: str,
    category: str,
    title: str,
    observation: str,
    scope: dict[str, int],
    metrics: dict[str, Any],
    baseline: dict[str, Any],
    comparison: dict[str, Any],
    impact: float,
    recurrence: float,
    confidence: float,
    actionability: float,
    limitations: list[str],
    actions: list[dict[str, Any]],
    success: list[str],
) -> Candidate:
    values = [
        max(0.0, min(1.0, value)) for value in (impact, recurrence, confidence, actionability)
    ]
    priority = sum(weight * value for weight, value in zip(PRIORITY_WEIGHTS, values, strict=True))
    label: Confidence = "high" if values[2] >= 0.75 else "medium" if values[2] >= 0.4 else "low"
    return Candidate(
        id=identifier,
        category=category,
        title=title,
        observation=observation,
        scope=scope,
        metrics=metrics,
        baseline=baseline,
        comparison=comparison,
        impact_score=round(values[0], 3),
        recurrence_score=round(values[1], 3),
        confidence_score=round(values[2], 3),
        actionability_score=round(values[3], 3),
        priority_score=round(priority, 3),
        confidence=label,
        limitations=limitations,
        candidate_actions=actions,
        success_metrics=success,
    )


def _double_detector(rounds: pl.DataFrame, holes: pl.DataFrame) -> list[Candidate]:
    if holes.is_empty() or not {"strokes", "par", "round_id"}.issubset(holes.columns):
        return []
    rows = [
        row
        for row in holes.to_dicts()
        if _number(row.get("strokes")) is not None and _number(row.get("par")) is not None
    ]
    doubles = [row for row in rows if float(row["strokes"]) - float(row["par"]) >= 2]
    cost = sum(float(row["strokes"]) - float(row["par"]) for row in doubles)
    total_cost = sum(max(0.0, float(row["strokes"]) - float(row["par"])) for row in rows)
    if (
        len(doubles) < 5
        or len({row["round_id"] for row in doubles}) < 3
        or not total_cost
        or cost / total_cost < 0.35
    ):
        return []
    per18 = len(doubles) / len(rows) * 18
    return [
        _score_candidate(
            identifier="scoring_double_or_worse_concentration",
            category="scoring",
            title="Severe holes drive a large share of score cost",
            observation="Double bogeys or worse account for a disproportionate share of recorded score above par.",
            scope={"rounds": rounds.height, "holes": len(rows), "shots": 0},
            metrics={
                "double_or_worse_holes": len(doubles),
                "doubles_per_18": round(per18, 2),
                "score_cost": round(cost, 2),
                "share_of_score_above_par_pct": round(cost / total_cost * 100, 2),
            },
            baseline={"total_score_above_par": round(total_cost, 2)},
            comparison={},
            impact=min(1.0, cost / total_cost),
            recurrence=min(1.0, per18 / 3),
            confidence=min(1.0, len(doubles) / 30),
            actionability=0.8,
            limitations=[
                "Scorecards do not identify the strategic or execution cause of a severe hole."
            ],
            actions=[
                {
                    "type": "recovery_rule",
                    "action": "For the next five rounds, use a conservative recovery decision after a costly miss rather than attempting a low-probability rescue.",
                    "eligibility": {},
                }
            ],
            success=["double_or_worse_pct", "avg_to_par"],
        )
    ]


def _tee_detector(
    rounds: pl.DataFrame, holes: pl.DataFrame, shots: pl.DataFrame
) -> list[Candidate]:
    if (
        holes.is_empty()
        or shots.is_empty()
        or not {"round_id", "hole_number", "par", "strokes"}.issubset(holes.columns)
        or not {"round_id", "hole_number", "shot_number"}.issubset(shots.columns)
    ):
        return []
    hole_rows = {(r["round_id"], r["hole_number"]): r for r in holes.to_dicts()}
    groups: dict[tuple[int, str], dict[str, list[dict[str, Any]]]] = defaultdict(
        lambda: defaultdict(list)
    )
    for shot in shots.to_dicts():
        if shot.get("shot_number") != 1:
            continue
        hole = hole_rows.get((shot.get("round_id"), shot.get("hole_number")))
        if (
            hole is None
            or hole.get("par") not in (4, 5)
            or _number(hole.get("strokes")) is None
            or _number(hole.get("par")) is None
        ):
            continue
        outcome = str(hole.get("fairway_shot_outcome") or "").upper()
        result = (
            "fairway"
            if outcome == "HIT" or (not outcome and _as_bool(hole.get("fairway_hit")))
            else "miss_right"
            if outcome == "RIGHT"
            else "miss_left"
            if outcome == "LEFT"
            else "other"
        )
        if result not in {"fairway", "miss_right", "miss_left"}:
            continue
        shot["to_par"] = float(hole["strokes"]) - float(hole["par"])
        groups[(int(hole["par"]), str(shot.get("club") or "Unknown"))][result].append(shot)
    candidates: list[Candidate] = []
    for (par, club), outcomes in groups.items():
        if club.strip().lower() == "unknown":
            continue
        fairways = outcomes["fairway"]
        if len(fairways) < 10:
            continue
        fairway_avg = sum(s["to_par"] for s in fairways) / len(fairways)
        for miss in ("miss_left", "miss_right"):
            misses = outcomes[miss]
            if len(misses) < 10:
                continue
            miss_avg = sum(s["to_par"] for s in misses) / len(misses)
            diff = miss_avg - fairway_avg
            if diff < 0.25:
                continue
            severe_diff = sum(s["to_par"] >= 2 for s in misses) / len(misses) - sum(
                s["to_par"] >= 2 for s in fairways
            ) / len(fairways)
            candidates.append(
                _score_candidate(
                    identifier=f"tee_{club.lower().replace(' ', '_')}_par_{par}_{miss}_cost",
                    category="tee_shot",
                    title=f"{miss.replace('_', ' ').title()} misses with {club} cost more",
                    observation=f"On par {par}s, {miss.replace('_', ' ')} outcomes with {club} are associated with worse hole scores than fairways.",
                    scope={
                        "rounds": len({s["round_id"] for s in misses + fairways}),
                        "holes": len(misses) + len(fairways),
                        "shots": len(misses),
                    },
                    metrics={
                        "miss_avg_to_par": round(miss_avg, 2),
                        "fairway_avg_to_par": round(fairway_avg, 2),
                        "difference_avg_to_par": round(diff, 2),
                        "double_or_worse_difference_pct": round(severe_diff * 100, 2),
                    },
                    baseline={
                        "fairway": {"shots": len(fairways), "avg_to_par": round(fairway_avg, 2)}
                    },
                    comparison={"miss": miss, "club": club, "par": par, "shots": len(misses)},
                    impact=min(1.0, diff / 1.5),
                    recurrence=min(1.0, len(misses) / 30),
                    confidence=min(1.0, min(len(misses), len(fairways)) / 30),
                    actionability=0.9,
                    limitations=[
                        "This is an association; Garmin data does not record wind, hazards, target intent, or swing cause."
                    ],
                    actions=[
                        {
                            "type": "conservative_tee",
                            "action": f"For five eligible rounds, use a conservative target with {club} on par {par}s when the {miss.replace('_', ' ')} is costly.",
                            "eligibility": {"par": par, "club": club, "fairway_result": miss},
                        }
                    ],
                    success=[
                        "double_or_worse_pct",
                        "avg_to_par",
                        "penalties_per_eligible_tee_shot",
                    ],
                )
            )
    return candidates


def _approach_detector(
    rounds: pl.DataFrame, holes: pl.DataFrame, shots: pl.DataFrame
) -> list[Candidate]:
    events = _approach_events(holes, shots)
    groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for event in events:
        groups[str(event["bucket"])].append(event)
    candidates: list[Candidate] = []
    labels = [label for _, _, label in APPROACH_BUCKETS]
    for index, label in enumerate(labels):
        current = groups[label]
        if len(current) < 10 or len({row["round_id"] for row in current}) < 5:
            continue
        adjacent = (groups[labels[index - 1]] if index else []) + (
            groups[labels[index + 1]] if index + 1 < len(labels) else []
        )
        if len(adjacent) < 10:
            continue
        current_gir = sum(_as_bool(row.get("gir")) for row in current) / len(current)
        other_gir = sum(_as_bool(row.get("gir")) for row in adjacent) / len(adjacent)
        current_proximity = median([float(row["end_to_pin_m"]) for row in current])
        other_proximity = median([float(row["end_to_pin_m"]) for row in adjacent])
        if other_gir - current_gir < 0.10 and current_proximity - other_proximity < 3:
            continue
        candidates.append(
            _score_candidate(
                identifier=f"approach_{label.replace(' ', '_').replace('–', '_').replace('>', 'over').replace('<', 'under')}_weakness",
                category="approach",
                title=f"Approaches from {label} are a recurring constraint",
                observation=f"Recorded approaches from {label} have weaker GIR or proximity than adjacent distance bands.",
                scope={
                    "rounds": len({row["round_id"] for row in current}),
                    "holes": len(current),
                    "shots": len(current),
                },
                metrics={
                    "gir_pct": round(current_gir * 100, 2),
                    "median_proximity_m": round(current_proximity, 2),
                    "proximity_stddev_m": round(
                        pstdev([float(row["end_to_pin_m"]) for row in current]), 2
                    ),
                },
                baseline={
                    "adjacent_gir_pct": round(other_gir * 100, 2),
                    "adjacent_median_proximity_m": round(other_proximity, 2),
                },
                comparison={"distance_bucket": label},
                impact=min(
                    1.0, max(other_gir - current_gir, (current_proximity - other_proximity) / 20)
                ),
                recurrence=min(1.0, len(current) / 40),
                confidence=min(1.0, len(current) / 30),
                actionability=0.85,
                limitations=[
                    "Pin coordinates describe finishing proximity but not wind, lie, or intended target."
                ],
                actions=[
                    {
                        "type": "distance_control_practice",
                        "action": f"For three weeks, practise a distance-control ladder covering {label} with the most-used clubs in this band.",
                        "eligibility": {"distance_bucket": label},
                    }
                ],
                success=["gir_pct", "median_proximity_m"],
            )
        )
    return candidates


def _profile(rounds: pl.DataFrame, holes: pl.DataFrame, shots: pl.DataFrame) -> dict[str, Any]:
    summary = build_summary_stats(rounds, holes, shots)
    double_pct = float(summary.get("double_bogey_or_worse_pct", 0))
    penalties = float(summary.get("penalties_per_18", 0))
    return {
        "scoring_shape": "catastrophe_driven" if double_pct >= 20 else "accumulation_driven",
        "volatility": "volatile" if double_pct >= 20 else "stable",
        "penalty_profile": "penalty_heavy" if penalties >= 1 else "penalty_light",
        "summary": summary,
    }


def analyze_data_quality(
    rounds: pl.DataFrame, holes: pl.DataFrame, shots: pl.DataFrame, **scope_options: Any
) -> AnalysisResult:
    scoped_rounds, scoped_holes, scoped_shots, scope = select_scope(
        rounds, holes, shots, **scope_options
    )
    return AnalysisResult(
        analysis_type="data_quality",
        scope=scope,
        data_quality=data_quality(scoped_rounds, scoped_holes, scoped_shots),
    )


def analyze_player(
    rounds: pl.DataFrame, holes: pl.DataFrame, shots: pl.DataFrame, **scope_options: Any
) -> AnalysisResult:
    scoped_rounds, scoped_holes, scoped_shots, scope = select_scope(
        rounds, holes, shots, **scope_options
    )
    quality = data_quality(scoped_rounds, scoped_holes, scoped_shots)
    candidates = (
        _double_detector(scoped_rounds, scoped_holes)
        + _tee_detector(scoped_rounds, scoped_holes, scoped_shots)
        + _approach_detector(scoped_rounds, scoped_holes, scoped_shots)
    )
    candidates.sort(key=lambda item: (-item.priority_score, item.id))
    return AnalysisResult(
        analysis_type="player",
        scope=scope,
        data_quality=quality,
        player_profile=_profile(scoped_rounds, scoped_holes, scoped_shots),
        insights=candidates[:5],
        primary_priority=candidates[0] if candidates else None,
        limitations=[
            "Recommendations are historical associations, not swing diagnoses or causal claims."
        ],
    )


def analyze_course(
    rounds: pl.DataFrame,
    holes: pl.DataFrame,
    shots: pl.DataFrame,
    *,
    course: str,
    **scope_options: Any,
) -> AnalysisResult:
    scoped_rounds, scoped_holes, scoped_shots, scope = select_scope(
        rounds, holes, shots, course=course, **scope_options
    )
    candidates = _tee_detector(scoped_rounds, scoped_holes, scoped_shots)
    candidates.sort(key=lambda item: (-item.priority_score, item.id))
    hole_stats = build_course_hole_stats(scoped_rounds, scoped_holes)
    profile = {
        "course": course,
        "high_risk_holes": hole_stats.head(5).to_dicts() if not hole_stats.is_empty() else [],
    }
    return AnalysisResult(
        analysis_type="course",
        scope=scope,
        data_quality=data_quality(scoped_rounds, scoped_holes, scoped_shots),
        player_profile=profile,
        insights=candidates[:5],
        primary_priority=candidates[0] if candidates else None,
        limitations=["Course recommendations do not infer aim lines or hazard geometry."],
    )


def analyze_round(
    rounds: pl.DataFrame,
    holes: pl.DataFrame,
    shots: pl.DataFrame,
    *,
    round_id: int,
    history_options: dict[str, Any] | None = None,
) -> dict[str, Any]:
    target = (
        rounds.filter(pl.col("round_id") == round_id)
        if "round_id" in rounds.columns
        else pl.DataFrame()
    )
    if target.is_empty():
        raise ValueError(f"Round {round_id} was not found in the local dataset.")
    target_holes = (
        holes.filter(pl.col("round_id") == round_id)
        if "round_id" in holes.columns
        else pl.DataFrame()
    )
    target_shots = (
        shots.filter(pl.col("round_id") == round_id)
        if "round_id" in shots.columns
        else pl.DataFrame()
    )
    history = analyze_player(rounds, holes, shots, **(history_options or {}))
    summary = build_summary_stats(target, target_holes, target_shots)
    decisions = history.insights[:2]
    return {
        "analysis_version": ANALYSIS_VERSION,
        "analysis_type": "round",
        "round_id": round_id,
        "summary": summary,
        "sections": {
            "what_went_well": "No penalties were recorded."
            if float(summary.get("penalties", 0)) == 0
            else "Review the round summary for recorded strengths.",
            "where_the_score_was_lost": f"The round included {summary.get('double_bogeys_or_worse_per_18', 0)} doubles-or-worse per 18.",
            "the_two_decisions_worth_changing": [
                item.model_dump(mode="json") for item in decisions
            ],
            "one_practice_focus": history.primary_priority.model_dump(mode="json")
            if history.primary_priority
            else None,
            "one_question_for_the_golfer": "Were costly misses caused by wind, an intended aggressive line, or execution?",
        },
        "historical_scope": history.scope.model_dump(mode="json"),
    }
