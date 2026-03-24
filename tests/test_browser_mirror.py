import json
from pathlib import Path

from _pytest.monkeypatch import MonkeyPatch
from typer.testing import CliRunner

from garmin_golf.browser_import import import_browser_export_payload
from garmin_golf.browser_mirror import (
    BrowserMirror,
    BrowserMirrorError,
    MirrorManifestEntry,
    MirrorRunResult,
    build_browser_export_payload,
    load_manifest,
    record_manifest_entry,
    save_manifest,
    should_skip_scorecard,
    validate_scorecards_url,
)
from garmin_golf.cli import app
from garmin_golf.config import Settings
from garmin_golf.storage import Storage


def _summary_row(scorecard_id: int = 42) -> dict[str, object]:
    return {
        "id": scorecard_id,
        "courseName": "Blue Hills",
        "totalScore": 84,
        "startTime": "2025-06-01T08:30:00.000",
    }


def _detail_payload(scorecard_id: int = 42) -> dict[str, object]:
    return {
        "scorecardDetails": [
            {
                "scorecard": {
                    "id": scorecard_id,
                    "startTime": "2025-06-01T08:30:00.000",
                    "courseName": "Blue Hills",
                    "teeName": "White",
                    "totalScore": 84,
                    "totalPar": 72,
                    "holes": [
                        {
                            "number": 1,
                            "par": 4,
                            "strokes": 5,
                            "putts": 2,
                            "fairwayHit": True,
                            "greenInRegulation": False,
                            "penalties": 0,
                        }
                    ],
                }
            }
        ]
    }


def _shot_payload(scorecard_id: int = 42) -> dict[str, object]:
    return {
        "holeShots": [
            {
                "holeNumber": 1,
                "shots": [
                    {
                        "id": 123,
                        "shotNumber": 1,
                        "shotOrder": 1,
                        "clubId": 7,
                        "club": "Driver",
                        "meters": 211.5,
                        "shotType": "TEE",
                        "autoShotType": "FULL",
                        "shotSource": "WATCH",
                        "shotTime": "2025-06-01T08:31:00.000",
                        "shotTimeZoneOffset": 120,
                        "scorecardId": scorecard_id,
                        "playerProfileId": 99,
                        "startLoc": {"lat": 1.0, "lon": 2.0, "x": 10, "y": 20},
                        "endLoc": {"lat": 3.0, "lon": 4.0, "x": 30, "y": 40},
                    }
                ],
            }
        ]
    }


def test_validate_scorecards_url() -> None:
    assert (
        validate_scorecards_url("https://connect.garmin.com/app/scorecards/flepied")
        == "https://connect.garmin.com/app/scorecards/flepied"
    )


def test_validate_scorecards_url_rejects_invalid_path() -> None:
    try:
        validate_scorecards_url("https://connect.garmin.com/app/scorecard/42")
    except ValueError as exc:
        assert "scorecards" in str(exc)
    else:  # pragma: no cover - defensive assertion
        raise AssertionError("Expected validate_scorecards_url to reject a scorecard detail URL.")


def test_manifest_round_trip_controls_incremental_skip(tmp_path: Path) -> None:
    manifest_path = tmp_path / "index.json"
    export_path = tmp_path / "42.json"
    manifest = load_manifest(manifest_path)

    assert should_skip_scorecard(42, manifest, export_path) is False

    export_path.write_text("{}", encoding="utf-8")
    record_manifest_entry(
        manifest,
        tmp_path,
        MirrorManifestEntry(
            scorecard_id=42,
            export_filename="42.json",
            mirrored_at="2026-03-23T20:00:00+00:00",
        ),
    )
    save_manifest(manifest_path, manifest)

    persisted = load_manifest(manifest_path)
    assert should_skip_scorecard(42, persisted, export_path) is True


def test_mirror_scorecards_command_writes_exports_and_imports(
    monkeypatch: MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("GARMIN_GOLF_DATA_DIR", str(tmp_path / "data"))
    output_dir = tmp_path / "mirror"

    class FakeMirror:
        def __init__(
            self,
            timeout_seconds: int,
            *,
            debugger_address: str | None = None,
            console: object,
        ) -> None:
            self.timeout_seconds = timeout_seconds
            self.debugger_address = debugger_address
            self.console = console

        def mirror(
            self,
            listing_url: str,
            *,
            storage: Storage,
            output_dir: Path,
            force: bool = False,
        ) -> MirrorRunResult:
            assert listing_url == "https://connect.garmin.com/app/scorecards/flepied"
            assert force is False
            output_dir.mkdir(parents=True, exist_ok=True)
            payload = build_browser_export_payload(
                summary_payload={"scorecardSummaries": [_summary_row()]},
                summary_row=_summary_row(),
                detail_payload=_detail_payload(),
                shot_payload=_shot_payload(),
                source="garmin-connect-browser",
            )
            export_path = output_dir / "42.json"
            export_path.write_text(json.dumps(payload), encoding="utf-8")
            import_result = import_browser_export_payload(storage, payload)
            return MirrorRunResult(
                discovered=1,
                exported=1,
                skipped=0,
                rounds_imported=import_result.rounds_imported,
                holes_imported=import_result.holes_imported,
                shots_imported=import_result.shots_imported,
            )

    monkeypatch.setattr("garmin_golf.cli.BrowserMirror", FakeMirror)

    runner = CliRunner()
    result = runner.invoke(
        app,
        [
            "mirror",
            "scorecards",
            "--url",
            "https://connect.garmin.com/app/scorecards/flepied",
            "--debugger-address",
            "127.0.0.1:9222",
            "--out-dir",
            str(output_dir),
        ],
    )

    assert result.exit_code == 0
    assert "discovered=1, exported=1, skipped=0" in result.stdout
    assert (output_dir / "42.json").exists()

    storage = Storage(Settings())
    rounds = storage.read_table("rounds")
    holes = storage.read_table("holes")
    shots = storage.read_table("shots")

    assert rounds.height == 1
    assert holes.height == 1
    assert shots.height == 1


def test_mirror_scorecards_command_json(
    monkeypatch: MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("GARMIN_GOLF_DATA_DIR", str(tmp_path / "data"))
    output_dir = tmp_path / "mirror"

    class FakeMirror:
        def __init__(
            self,
            timeout_seconds: int,
            *,
            debugger_address: str | None = None,
            console: object,
        ) -> None:
            self.timeout_seconds = timeout_seconds
            self.debugger_address = debugger_address
            self.console = console

        def mirror(
            self,
            listing_url: str,
            *,
            storage: Storage,
            output_dir: Path,
            force: bool = False,
        ) -> MirrorRunResult:
            assert listing_url == "https://connect.garmin.com/app/scorecards/flepied"
            assert force is False
            return MirrorRunResult(
                discovered=1,
                exported=1,
                skipped=0,
                rounds_imported=1,
                holes_imported=1,
                shots_imported=1,
            )

    monkeypatch.setattr("garmin_golf.cli.BrowserMirror", FakeMirror)

    runner = CliRunner()
    result = runner.invoke(
        app,
        [
            "mirror",
            "scorecards",
            "--url",
            "https://connect.garmin.com/app/scorecards/flepied",
            "--debugger-address",
            "127.0.0.1:9222",
            "--out-dir",
            str(output_dir),
            "--json",
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload == {
        "discovered": 1,
        "exported": 1,
        "skipped": 0,
        "rounds_imported": 1,
        "holes_imported": 1,
        "shots_imported": 1,
        "output_dir": str(output_dir),
    }


def test_browser_mirror_requires_debugger_address() -> None:
    try:
        BrowserMirror()
    except BrowserMirrorError as exc:
        assert "--debugger-address" in str(exc)
    else:  # pragma: no cover - defensive assertion
        raise AssertionError("Expected BrowserMirror to require a debugger address.")


def test_wait_for_authenticated_listing() -> None:
    mirror = BrowserMirror(debugger_address="127.0.0.1:9222")

    class FakeSession:
        def __init__(self) -> None:
            self.calls = 0

        def evaluate(self, expression: str) -> dict[str, str]:
            self.calls += 1
            return {
                "url": "https://connect.garmin.com/app/scorecards/flepied",
                "bodyText": "Scorecards",
            }

    session = FakeSession()
    mirror._wait_for_authenticated_listing(
        session,
        "https://connect.garmin.com/app/scorecards/flepied",
    )

    assert session.calls == 1
