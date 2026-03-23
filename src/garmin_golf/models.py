from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

JsonDict = dict[str, Any]


@dataclass(slots=True)
class SyncResult:
    rounds_synced: int = 0
    holes_synced: int = 0
    shots_synced: int = 0
    raw_files_written: int = 0


@dataclass(slots=True)
class RawSnapshot:
    relative_path: Path
    payload: JsonDict


@dataclass(slots=True)
class DateRange:
    date_from: date | None = None
    date_to: date | None = None
