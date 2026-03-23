import json
from pathlib import Path

from _pytest.monkeypatch import MonkeyPatch
from typer.testing import CliRunner

from garmin_golf.browser_export import BROWSER_EXPORT_SCRIPT
from garmin_golf.cli import app


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
                                    "shotNumber": 1,
                                    "club": "Driver",
                                    "distanceMeters": 211.5,
                                    "lie": "Tee",
                                    "result": "Fairway",
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
