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
        preserve_columns: list[str] | None = None,
    ) -> pl.DataFrame:
        existing = self.read_table(table_name)
        if not rows:
            return existing

        incoming = pl.from_dicts(rows)
        if preserve_columns and not existing.is_empty():
            incoming = self._with_preserved_columns(
                existing,
                incoming,
                unique_by=unique_by,
                preserve_columns=preserve_columns,
            )
        if existing.is_empty():
            merged = incoming
        else:
            merged = pl.concat([existing, incoming], how="diagonal_relaxed")
        merged = merged.unique(subset=unique_by, keep="last", maintain_order=True)
        merged.write_parquet(self.table_path(table_name))
        return merged

    def table_path(self, table_name: str) -> Path:
        return self.settings.parquet_dir / f"{table_name}.parquet"

    def _with_preserved_columns(
        self,
        existing: pl.DataFrame,
        incoming: pl.DataFrame,
        *,
        unique_by: list[str],
        preserve_columns: list[str],
    ) -> pl.DataFrame:
        if not all(column in existing.columns for column in unique_by):
            return incoming
        if not all(column in incoming.columns for column in unique_by):
            return incoming

        available_columns = [column for column in preserve_columns if column in existing.columns]
        if not available_columns:
            return incoming

        preserved = existing.select(unique_by + available_columns).rename(
            {column: f"__preserved_{column}" for column in available_columns}
        )
        joined = incoming.join(preserved, on=unique_by, how="left")
        expressions: list[pl.Expr] = []
        for column in available_columns:
            preserved_column = f"__preserved_{column}"
            if column in joined.columns:
                expressions.append(pl.col(preserved_column).fill_null(pl.col(column)).alias(column))
            else:
                expressions.append(pl.col(preserved_column).alias(column))
        return joined.with_columns(expressions).drop(
            [f"__preserved_{column}" for column in available_columns]
        )
