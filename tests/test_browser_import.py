import json
from pathlib import Path

from _pytest.monkeypatch import MonkeyPatch
from typer.testing import CliRunner

from garmin_golf.browser_export import BROWSER_EXPORT_SCRIPT
from garmin_golf.cli import app
from garmin_golf.config import Settings
from garmin_golf.storage import Storage


def test_browser_script_command_writes_script(tmp_path: Path) -> None:
    out = tmp_path / "export.js"
    runner = CliRunner()

    result = runner.invoke(app, ["export", "browser-script", "--out", str(out)])

    assert result.exit_code == 0
    assert out.read_text(encoding="utf-8").startswith("(function gcExportGolfScores()")
    assert "scorecard/summary" in BROWSER_EXPORT_SCRIPT
    assert "/app/scorecard/" in BROWSER_EXPORT_SCRIPT
    assert "connect-csrf-token" in BROWSER_EXPORT_SCRIPT


def test_import_browser_export_command(monkeypatch: MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("GARMIN_GOLF_DATA_DIR", str(tmp_path / "data"))
    export_path = tmp_path / "garmin-golf-export.json"
    export_payload = {
        "summary": {
            "scorecardSummaries": [
                {"id": 42, "courseName": "Blue Hills", "totalScore": 84},
            ]
        },
        "details": [
            {
                "scorecardDetails": [
                    {
                        "scorecard": {
                            "id": 42,
                            "startTime": "2025-06-01T08:30:00.000",
                            "courseName": "Blue Hills",
                            "teeName": "White",
                            "totalScore": 84,
                            "totalPar": 72,
                            "holes": [
                                {
                                    "number": 1,
                                    "fairwayShotOutcome": "LEFT",
                                    "handicapScore": 1,
                                    "lastModifiedDt": "2025-06-01T08:35:00.000",
                                    "par": 4,
                                    "pinPositionLat": 12.34,
                                    "pinPositionLon": 56.78,
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
        ],
        "shots": [
            {
                "scorecardId": 42,
                "payload": {
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
                                    "scorecardId": 42,
                                    "playerProfileId": 99,
                                    "startLoc": {"lat": 1.0, "lon": 2.0, "x": 10, "y": 20},
                                    "endLoc": {"lat": 3.0, "lon": 4.0, "x": 30, "y": 40},
                                }
                            ],
                        }
                    ]
                },
            }
        ],
    }
    export_path.write_text(json.dumps(export_payload), encoding="utf-8")

    runner = CliRunner()
    result = runner.invoke(app, ["sync", "import-browser-export", "--path", str(export_path)])

    assert result.exit_code == 0
    assert "1 rounds, 1 holes, 1 shots" in result.stdout

    storage = Storage(Settings())
    rounds = storage.read_table("rounds")
    holes = storage.read_table("holes")
    shots = storage.read_table("shots")

    round_row = rounds.row(0, named=True)
    hole_row = holes.row(0, named=True)
    shot_row = shots.row(0, named=True)

    assert round_row["summary_json"] is not None
    assert round_row["scorecard_json"] is not None
    assert hole_row["fairway_shot_outcome"] == "LEFT"
    assert hole_row["handicap_score"] == 1
    assert hole_row["pin_position_lat"] == 12.34
    assert hole_row["pin_position_lon"] == 56.78
    assert hole_row["hole_json"] is not None
    assert shot_row["shot_id"] == 123
    assert shot_row["club_id"] == 7
    assert shot_row["shot_type"] == "TEE"
    assert shot_row["auto_shot_type"] == "FULL"
    assert shot_row["shot_source"] == "WATCH"
    assert shot_row["shot_time_zone_offset"] == 120
    assert shot_row["start_x"] == 10
    assert shot_row["start_y"] == 20
    assert shot_row["end_x"] == 30
    assert shot_row["end_y"] == 40
    assert shot_row["shot_json"] is not None
