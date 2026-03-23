from datetime import date
from pathlib import Path

from _pytest.monkeypatch import MonkeyPatch
from typer.testing import CliRunner

from garmin_golf.cli import _resolve_date_window, app
from garmin_golf.config import Settings
from garmin_golf.storage import Storage


def test_stats_summary_command(monkeypatch: MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("GARMIN_GOLF_DATA_DIR", str(tmp_path))
    storage = Storage(Settings())
    storage.upsert_rows(
        "rounds",
        [{"round_id": 1, "total_score": 82, "total_par": 72}],
        unique_by=["round_id"],
    )
    storage.upsert_rows(
        "holes",
        [
            {
                "round_id": 1,
                "hole_number": 1,
                "putts": 2,
                "gir": True,
                "fairway_hit": True,
                "penalties": 0,
            }
        ],
        unique_by=["round_id", "hole_number"],
    )
    storage.upsert_rows(
        "shots",
        [
            {
                "round_id": 1,
                "hole_number": 1,
                "shot_number": 1,
                "shot_type": "TEE",
                "club": "Driver",
                "distance_meters": 200.0,
            }
        ],
        unique_by=["round_id", "hole_number", "shot_number"],
    )

    runner = CliRunner()
    result = runner.invoke(app, ["stats", "summary"])

    assert result.exit_code == 0
    assert "rounds_played" in result.stdout
    assert "average_tee_shot_distance_m" in result.stdout


def test_stats_summary_command_with_date_range(monkeypatch: MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("GARMIN_GOLF_DATA_DIR", str(tmp_path))
    storage = Storage(Settings())
    storage.upsert_rows(
        "rounds",
        [
            {"round_id": 1, "played_on": "2025-06-01", "total_score": 82, "total_par": 72},
            {"round_id": 2, "played_on": "2024-06-01", "total_score": 90, "total_par": 72},
        ],
        unique_by=["round_id"],
    )
    storage.upsert_rows(
        "holes",
        [
            {
                "round_id": 1,
                "hole_number": 1,
                "putts": 2,
                "gir": True,
                "fairway_hit": True,
                "penalties": 0,
            },
            {
                "round_id": 2,
                "hole_number": 1,
                "putts": 3,
                "gir": False,
                "fairway_hit": False,
                "penalties": 1,
            },
        ],
        unique_by=["round_id", "hole_number"],
    )

    runner = CliRunner()
    result = runner.invoke(app, ["stats", "summary", "--from", "2025-01-01", "--to", "2025-12-31"])

    assert result.exit_code == 0
    assert "rounds_played" in result.stdout
    assert "1" in result.stdout


def test_resolve_date_window_last_12_months() -> None:
    date_from, date_to = _resolve_date_window(
        date_from=None,
        date_to=None,
        period="last-12-months",
        today=date(2026, 3, 23),
    )

    assert date_from == date(2025, 3, 24)
    assert date_to == date(2026, 3, 23)


def test_resolve_date_window_last_year() -> None:
    date_from, date_to = _resolve_date_window(
        date_from=None,
        date_to=None,
        period="last-year",
        today=date(2026, 3, 23),
    )

    assert date_from == date(2025, 1, 1)
    assert date_to == date(2025, 12, 31)


def test_config_init_command(monkeypatch: MonkeyPatch, tmp_path: Path) -> None:
    config_file = tmp_path / "config.toml"
    monkeypatch.setenv("GARMIN_GOLF_CONFIG_FILE", str(config_file))

    runner = CliRunner()
    result = runner.invoke(app, ["config", "init"])

    assert result.exit_code == 0
    assert config_file.exists()
    assert 'garmin_email = "you@example.com"' in config_file.read_text(encoding="utf-8")
