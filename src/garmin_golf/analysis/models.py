from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field

AnalysisStatus = Literal["unavailable", "limited", "usable", "strong"]
Confidence = Literal["low", "medium", "high"]


def _default_player_context() -> dict[str, float | None]:
    return {"handicap_index": None, "target_handicap_index": None}


class Scope(BaseModel):
    period: dict[str, str | None]
    rounds: int
    holes: int
    shots: int
    courses: int
    excluded_rounds: int
    player_context: dict[str, float | None] = Field(default_factory=_default_player_context)


class DomainQuality(BaseModel):
    status: AnalysisStatus
    available: int
    total: int
    coverage_pct: float
    limitations: list[str] = Field(default_factory=list)


class Candidate(BaseModel):
    id: str
    category: str
    title: str
    observation: str
    scope: dict[str, int]
    metrics: dict[str, Any]
    baseline: dict[str, Any]
    comparison: dict[str, Any]
    impact_score: float
    recurrence_score: float
    confidence_score: float
    actionability_score: float
    priority_score: float
    confidence: Confidence
    limitations: list[str]
    candidate_actions: list[dict[str, Any]]
    success_metrics: list[str]


class AnalysisResult(BaseModel):
    analysis_version: str = "1.0"
    analysis_type: str
    scope: Scope
    data_quality: dict[str, DomainQuality]
    player_profile: dict[str, Any] = Field(default_factory=dict)
    insights: list[Candidate] = Field(default_factory=list)
    primary_priority: Candidate | None = None
    active_experiments: list[dict[str, Any]] = Field(default_factory=list)
    limitations: list[str] = Field(default_factory=list)
