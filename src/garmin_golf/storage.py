from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import polars as pl

from .config import Settings


class Storage:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.settings.data_dir.mkdir(parents=True, exist_ok=True)
        self.settings.raw_dir.mkdir(parents=True, exist_ok=True)
        self.settings.parquet_dir.mkdir(parents=True, exist_ok=True)

    def write_json_snapshot(self, relative_path: Path, payload: dict[str, Any]) -> Path:
        destination = self.settings.raw_dir / relative_path
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
        return destination

    def write_bytes_snapshot(self, relative_path: Path, payload: bytes) -> Path:
        destination = self.settings.raw_dir / relative_path
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(payload)
        return destination

    def read_table(self, table_name: str) -> pl.DataFrame:
        path = self.table_path(table_name)
        if not path.exists():
            return pl.DataFrame()
        return pl.read_parquet(path)

    def upsert_rows(
        self,
        table_name: str,
        rows: list[dict[str, Any]],
        *,
        unique_by: list[str],
    ) -> pl.DataFrame:
        existing = self.read_table(table_name)
        if not rows:
            return existing

        incoming = pl.from_dicts(rows)
        if existing.is_empty():
            merged = incoming
        else:
            merged = pl.concat([existing, incoming], how="diagonal_relaxed")
        merged = merged.unique(subset=unique_by, keep="last", maintain_order=True)
        merged.write_parquet(self.table_path(table_name))
        return merged

    def table_path(self, table_name: str) -> Path:
        return self.settings.parquet_dir / f"{table_name}.parquet"
