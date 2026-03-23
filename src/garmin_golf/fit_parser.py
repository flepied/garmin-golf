from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from io import BytesIO
from pathlib import Path
from zipfile import ZipFile

import fitdecode

from .models import JsonDict


@dataclass(slots=True)
class FitInspection:
    archive_members: list[str]
    message_counts: dict[str, int]
    unknown_message_counts: dict[str, int]
    session: JsonDict | None
    lap_count: int
    record_count: int

    def as_dict(self) -> JsonDict:
        return {
            "archive_members": self.archive_members,
            "message_counts": self.message_counts,
            "unknown_message_counts": self.unknown_message_counts,
            "session": self.session,
            "lap_count": self.lap_count,
            "record_count": self.record_count,
        }


def inspect_activity_archive(archive_bytes: bytes) -> FitInspection:
    with ZipFile(BytesIO(archive_bytes)) as archive:
        members = archive.namelist()
        fit_member = next((name for name in members if name.lower().endswith(".fit")), None)
        if fit_member is None:
            msg = "No FIT file was found in the activity archive."
            raise ValueError(msg)
        fit_bytes = archive.read(fit_member)

    inspection = inspect_fit_bytes(fit_bytes)
    inspection.archive_members = members
    return inspection


def inspect_fit_file(path: Path) -> FitInspection:
    return inspect_fit_bytes(path.read_bytes(), archive_members=[path.name])


def inspect_fit_bytes(
    fit_bytes: bytes,
    *,
    archive_members: list[str] | None = None,
) -> FitInspection:
    message_counts: Counter[str] = Counter()
    unknown_message_counts: Counter[str] = Counter()
    session: JsonDict | None = None
    lap_count = 0
    record_count = 0

    with fitdecode.FitReader(BytesIO(fit_bytes)) as fit:
        for frame in fit:
            if not isinstance(frame, fitdecode.FitDataMessage):
                continue
            message_counts[frame.name] += 1
            if frame.name.startswith("unknown_"):
                unknown_message_counts[frame.name] += 1
            if frame.name == "session" and session is None:
                session = {field.name: field.value for field in frame.fields}
            elif frame.name == "lap":
                lap_count += 1
            elif frame.name == "record":
                record_count += 1

    return FitInspection(
        archive_members=archive_members or [],
        message_counts=dict(sorted(message_counts.items())),
        unknown_message_counts=dict(sorted(unknown_message_counts.items())),
        session=session,
        lap_count=lap_count,
        record_count=record_count,
    )
