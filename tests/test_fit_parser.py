from io import BytesIO
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile

import fitdecode
import pytest

from garmin_golf.fit_parser import inspect_activity_archive, inspect_fit_file


def test_inspect_activity_archive_rejects_missing_fit() -> None:
    buffer = BytesIO()
    with ZipFile(buffer, "w", compression=ZIP_DEFLATED) as archive:
        archive.writestr("notes.txt", "missing fit")

    with pytest.raises(ValueError):
        inspect_activity_archive(buffer.getvalue())


def test_inspect_activity_archive_uses_fit_reader(monkeypatch: pytest.MonkeyPatch) -> None:
    buffer = BytesIO()
    with ZipFile(buffer, "w", compression=ZIP_DEFLATED) as archive:
        archive.writestr("activity.fit", b"fake-fit")

    class FakeField:
        def __init__(self, name: str, value: object) -> None:
            self.name = name
            self.value = value

    class FakeMessage:
        def __init__(self, name: str, fields: list[FakeField]) -> None:
            self.name = name
            self.fields = fields

    fake_frames = [
        FakeMessage("session", [FakeField("sport_profile_name", "Golf")]),
        FakeMessage("lap", [FakeField("lap_index", 1)]),
        FakeMessage("record", [FakeField("distance", 1.2)]),
        FakeMessage("unknown_140", [FakeField("unknown_20", 9)]),
    ]

    class FakeReader:
        def __init__(self, _: BytesIO) -> None:
            self.frames = fake_frames

        def __enter__(self) -> list[FakeMessage]:
            return self.frames

        def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
            return None

    monkeypatch.setattr(fitdecode, "FitReader", FakeReader)
    monkeypatch.setattr(fitdecode, "FitDataMessage", FakeMessage)

    inspection = inspect_activity_archive(buffer.getvalue())

    assert inspection.archive_members == ["activity.fit"]
    assert inspection.lap_count == 1
    assert inspection.record_count == 1
    assert inspection.session == {"sport_profile_name": "Golf"}
    assert inspection.unknown_message_counts == {"unknown_140": 1}


def test_inspect_fit_file_reads_plain_fit(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    fit_path = tmp_path / "scorecard.fit"
    fit_path.write_bytes(b"fake-fit")

    class FakeField:
        def __init__(self, name: str, value: object) -> None:
            self.name = name
            self.value = value

    class FakeMessage:
        def __init__(self, name: str, fields: list[FakeField]) -> None:
            self.name = name
            self.fields = fields

    fake_frames = [FakeMessage("unknown_147", [FakeField("unknown_254", 1)])]

    class FakeReader:
        def __init__(self, _: BytesIO) -> None:
            self.frames = fake_frames

        def __enter__(self) -> list[FakeMessage]:
            return self.frames

        def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
            return None

    monkeypatch.setattr(fitdecode, "FitReader", FakeReader)
    monkeypatch.setattr(fitdecode, "FitDataMessage", FakeMessage)

    inspection = inspect_fit_file(fit_path)

    assert inspection.archive_members == ["scorecard.fit"]
    assert inspection.unknown_message_counts == {"unknown_147": 1}
