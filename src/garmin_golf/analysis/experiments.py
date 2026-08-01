# ruff: noqa: E501
from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

import polars as pl

from ..storage import Storage
from .models import Candidate

EXPERIMENT_SCHEMA_VERSION = "1.0"


def list_experiments(storage: Storage) -> list[dict[str, Any]]:
    table = storage.read_table("experiments")
    return table.sort("created_at", descending=True).to_dicts() if not table.is_empty() else []


def get_experiment(storage: Storage, experiment_id: str) -> dict[str, Any]:
    table = storage.read_table("experiments")
    if table.is_empty() or "experiment_id" not in table.columns:
        raise ValueError(f"Experiment {experiment_id} was not found.")
    matching = table.filter(pl.col("experiment_id") == experiment_id)
    if matching.is_empty():
        raise ValueError(f"Experiment {experiment_id} was not found.")
    return matching.row(0, named=True)


def start_experiment(
    storage: Storage, candidate: Candidate, baseline_round_ids: list[int]
) -> dict[str, Any]:
    if not candidate.candidate_actions or not candidate.success_metrics:
        raise ValueError("This insight cannot be turned into an experiment.")
    action = candidate.candidate_actions[0]
    now = datetime.now(UTC).isoformat()
    row: dict[str, Any] = {
        "experiment_id": f"exp_{uuid4().hex[:12]}",
        "schema_version": EXPERIMENT_SCHEMA_VERSION,
        "status": "active",
        "created_at": now,
        "reviewed_at": None,
        "created_from_insight_id": candidate.id,
        "category": candidate.category,
        "hypothesis": candidate.observation,
        "action_payload": json.dumps(action, sort_keys=True),
        "eligibility_payload": json.dumps(action.get("eligibility", {}), sort_keys=True),
        "baseline_payload": json.dumps(candidate.metrics, sort_keys=True),
        "baseline_round_ids": json.dumps(baseline_round_ids),
        "success_metrics": json.dumps(candidate.success_metrics),
        "review_after_rounds": 5,
        "review_result": None,
    }
    storage.upsert_rows("experiments", [row], unique_by=["experiment_id"])
    return row


def cancel_experiment(storage: Storage, experiment_id: str) -> dict[str, Any]:
    row = get_experiment(storage, experiment_id)
    if row.get("status") != "active":
        raise ValueError("Only active experiments can be cancelled.")
    row["status"] = "cancelled"
    row["reviewed_at"] = datetime.now(UTC).isoformat()
    storage.upsert_rows("experiments", [row], unique_by=["experiment_id"])
    return row


def review_experiment(
    storage: Storage, experiment_id: str, available_round_ids: list[int]
) -> dict[str, Any]:
    row = get_experiment(storage, experiment_id)
    if row.get("status") != "active":
        raise ValueError("Only active experiments can be reviewed.")
    baseline_ids = set(json.loads(str(row.get("baseline_round_ids") or "[]")))
    follow_up = [round_id for round_id in available_round_ids if round_id not in baseline_ids]
    required = int(row.get("review_after_rounds") or 5)
    result = "insufficient_data" if len(follow_up) < required else "inconclusive"
    row["review_result"] = json.dumps({"outcome": result, "eligible_rounds": len(follow_up)})
    row["status"] = result
    row["reviewed_at"] = datetime.now(UTC).isoformat()
    storage.upsert_rows("experiments", [row], unique_by=["experiment_id"])
    return row
