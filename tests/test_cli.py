from pathlib import Path

from _pytest.monkeypatch import MonkeyPatch
from typer.testing import CliRunner

from garmin_golf.cli import app
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

    runner = CliRunner()
    result = runner.invoke(app, ["stats", "summary"])

    assert result.exit_code == 0
    assert "rounds_played" in result.stdout


def test_config_init_command(monkeypatch: MonkeyPatch, tmp_path: Path) -> None:
    config_file = tmp_path / "config.toml"
    monkeypatch.setenv("GARMIN_GOLF_CONFIG_FILE", str(config_file))

    runner = CliRunner()
    result = runner.invoke(app, ["config", "init"])

    assert result.exit_code == 0
    assert config_file.exists()
    assert 'garmin_email = "you@example.com"' in config_file.read_text(encoding="utf-8")
