from datetime import date
from pathlib import Path

from _pytest.monkeypatch import MonkeyPatch
from typer.testing import CliRunner

from garmin_golf.cli import _resolve_date_window, app
from garmin_golf.config import Settings
from garmin_golf.storage import Storage


def test_auth_command_is_not_exposed() -> None:
    runner = CliRunner()
    result = runner.invoke(app, ["--help"])

    assert result.exit_code == 0
    assert " auth " not in result.stdout
    assert " sync " not in result.stdout
    assert " export " not in result.stdout
    assert " inspect " not in result.stdout


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


def test_stats_rounds_command(monkeypatch: MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("GARMIN_GOLF_DATA_DIR", str(tmp_path))
    storage = Storage(Settings())
    storage.upsert_rows(
        "rounds",
        [
            {"round_id": 1, "played_on": "2025-06-01", "course_name": "Blue Hills"},
            {"round_id": 2, "played_on": "2025-07-01", "course_name": "Red Oaks"},
        ],
        unique_by=["round_id"],
    )

    runner = CliRunner()
    result = runner.invoke(app, ["stats", "rounds"])

    assert result.exit_code == 0
    assert "Local Rounds" in result.stdout
    assert "2025-07-01" in result.stdout
    assert "Red Oaks" in result.stdout
    assert result.stdout.index("2025-07-01") < result.stdout.index("2025-06-01")


def test_stats_rounds_command_uses_location_name_fallback(
    monkeypatch: MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("GARMIN_GOLF_DATA_DIR", str(tmp_path))
    storage = Storage(Settings())
    storage.upsert_rows(
        "rounds",
        [
            {
                "round_id": 123,
                "played_on": "2025-06-01",
                "course_name": None,
                "location_name": "Chateaufort",
            }
        ],
        unique_by=["round_id"],
    )

    runner = CliRunner()
    result = runner.invoke(app, ["stats", "rounds"])

    assert result.exit_code == 0
    assert "Chateaufort" in result.stdout


def test_stats_rounds_command_with_date_range(monkeypatch: MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("GARMIN_GOLF_DATA_DIR", str(tmp_path))
    storage = Storage(Settings())
    storage.upsert_rows(
        "rounds",
        [
            {"round_id": 1, "played_on": "2025-06-01", "course_name": "Blue Hills"},
            {"round_id": 2, "played_on": "2024-06-01", "course_name": "Red Oaks"},
        ],
        unique_by=["round_id"],
    )

    runner = CliRunner()
    result = runner.invoke(app, ["stats", "rounds", "--from", "2025-01-01", "--to", "2025-12-31"])

    assert result.exit_code == 0
    assert "Blue Hills" in result.stdout
    assert "Red Oaks" not in result.stdout


def test_stats_rounds_command_with_period_filter(
    monkeypatch: MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("GARMIN_GOLF_DATA_DIR", str(tmp_path))
    storage = Storage(Settings())
    storage.upsert_rows(
        "rounds",
        [
            {"round_id": 1, "played_on": "2026-03-01", "course_name": "Blue Hills"},
            {"round_id": 2, "played_on": "2024-03-01", "course_name": "Red Oaks"},
        ],
        unique_by=["round_id"],
    )

    runner = CliRunner()
    result = runner.invoke(app, ["stats", "rounds", "--period", "last-12-months"])

    assert result.exit_code == 0
    assert "Blue Hills" in result.stdout
    assert "Red Oaks" not in result.stdout


def test_stats_rounds_command_rejects_period_with_explicit_dates(
    monkeypatch: MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("GARMIN_GOLF_DATA_DIR", str(tmp_path))
    storage = Storage(Settings())
    storage.upsert_rows(
        "rounds",
        [{"round_id": 1, "played_on": "2025-06-01", "course_name": "Blue Hills"}],
        unique_by=["round_id"],
    )

    runner = CliRunner()
    result = runner.invoke(
        app,
        ["stats", "rounds", "--period", "last-year", "--from", "2025-01-01"],
    )

    assert result.exit_code != 0
    assert "Use either --period or --from/--to, not both." in result.output


def test_stats_rounds_command_with_no_local_rounds(
    monkeypatch: MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("GARMIN_GOLF_DATA_DIR", str(tmp_path))

    runner = CliRunner()
    result = runner.invoke(app, ["stats", "rounds"])

    assert result.exit_code == 0
    assert "No local rounds found." in result.stdout
    assert "garmin-golf mirror scorecards" in result.stdout


def test_stats_rounds_command_with_no_matching_filtered_rounds(
    monkeypatch: MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("GARMIN_GOLF_DATA_DIR", str(tmp_path))
    storage = Storage(Settings())
    storage.upsert_rows(
        "rounds",
        [{"round_id": 1, "played_on": "2024-06-01", "course_name": "Blue Hills"}],
        unique_by=["round_id"],
    )

    runner = CliRunner()
    result = runner.invoke(app, ["stats", "rounds", "--from", "2025-01-01"])

    assert result.exit_code == 0
    assert "No rounds matched the selected date window." in result.stdout


def test_stats_rounds_command_sorts_undated_rounds_last(
    monkeypatch: MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("GARMIN_GOLF_DATA_DIR", str(tmp_path))
    storage = Storage(Settings())
    storage.upsert_rows(
        "rounds",
        [
            {"round_id": 1, "played_on": None, "course_name": "No Date"},
            {"round_id": 2, "played_on": "2025-06-01", "course_name": "Dated"},
        ],
        unique_by=["round_id"],
    )

    runner = CliRunner()
    result = runner.invoke(app, ["stats", "rounds"])

    assert result.exit_code == 0
    assert result.stdout.index("Dated") < result.stdout.index("No Date")


def test_stats_rounds_command_prefers_scorecard_over_matching_activity(
    monkeypatch: MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("GARMIN_GOLF_DATA_DIR", str(tmp_path))
    storage = Storage(Settings())
    storage.upsert_rows(
        "rounds",
        [
            {
                "round_id": 1001,
                "scorecard_id": 1001,
                "played_on": "2025-06-01",
                "start_time": "2025-06-01T07:00:00.000Z",
                "course_name": "Golf National",
                "player_profile_id": 77,
            },
            {
                "round_id": 2001,
                "activity_id": 2001,
                "played_on": "2025-06-01",
                "start_time": "2025-06-01T08:00:00.0",
                "course_name": "Chateaufort Golf",
                "location_name": "Chateaufort",
                "data_source": "activities",
                "player_profile_id": 77,
            },
        ],
        unique_by=["round_id"],
    )

    runner = CliRunner()
    result = runner.invoke(app, ["stats", "rounds"])

    assert result.exit_code == 0
    assert "Golf National" in result.stdout
    assert "Chateaufort Golf" not in result.stdout
    assert "2001" not in result.stdout


def test_stats_courses_command(monkeypatch: MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("GARMIN_GOLF_DATA_DIR", str(tmp_path))
    storage = Storage(Settings())
    storage.upsert_rows(
        "rounds",
        [
            {"round_id": 1, "played_on": "2025-06-01", "course_name": "Blue Hills"},
            {"round_id": 2, "played_on": "2025-06-08", "course_name": "Blue Hills"},
            {"round_id": 3, "played_on": "2025-06-03", "course_name": "Red Oaks"},
        ],
        unique_by=["round_id"],
    )

    runner = CliRunner()
    result = runner.invoke(app, ["stats", "courses"])

    assert result.exit_code == 0
    assert "Local Courses" in result.stdout
    assert "Blue Hills" in result.stdout
    assert "Red Oaks" in result.stdout
    assert result.stdout.index("Blue Hills") < result.stdout.index("Red Oaks")


def test_stats_course_command(monkeypatch: MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("GARMIN_GOLF_DATA_DIR", str(tmp_path))
    storage = Storage(Settings())
    storage.upsert_rows(
        "rounds",
        [
            {
                "round_id": 1,
                "played_on": "2025-06-01",
                "course_name": "Blue Hills",
                "total_score": 82,
                "total_par": 72,
            },
            {
                "round_id": 2,
                "played_on": "2025-06-08",
                "course_name": "Blue Hills",
                "total_score": 86,
                "total_par": 72,
            },
        ],
        unique_by=["round_id"],
    )
    storage.upsert_rows(
        "holes",
        [
            {
                "round_id": 1,
                "hole_number": 1,
                "par": 4,
                "strokes": 5,
                "putts": 2,
                "gir": False,
                "fairway_hit": False,
                "penalties": 1,
            },
            {
                "round_id": 2,
                "hole_number": 1,
                "par": 4,
                "strokes": 6,
                "putts": 3,
                "gir": False,
                "fairway_hit": False,
                "penalties": 0,
            },
            {
                "round_id": 1,
                "hole_number": 2,
                "par": 3,
                "strokes": 3,
                "putts": 1,
                "gir": True,
                "fairway_hit": None,
                "penalties": 0,
            },
            {
                "round_id": 2,
                "hole_number": 2,
                "par": 3,
                "strokes": 4,
                "putts": 2,
                "gir": False,
                "fairway_hit": None,
                "penalties": 0,
            },
        ],
        unique_by=["round_id", "hole_number"],
    )

    runner = CliRunner()
    result = runner.invoke(app, ["stats", "course", "--course", "Blue Hills"])

    assert result.exit_code == 0
    assert "Course Summary: Blue Hills" in result.stdout
    assert "Next Round Focus: Blue Hills" in result.stdout
    assert "Course Holes: Blue Hills" in result.stdout
    assert "hardest_holes" in result.stdout
    assert "1 (1.50 to par)" in result.stdout


def test_stats_course_command_rejects_unknown_course(
    monkeypatch: MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("GARMIN_GOLF_DATA_DIR", str(tmp_path))
    storage = Storage(Settings())
    storage.upsert_rows(
        "rounds",
        [{"round_id": 1, "played_on": "2025-06-01", "course_name": "Blue Hills"}],
        unique_by=["round_id"],
    )

    runner = CliRunner()
    result = runner.invoke(app, ["stats", "course", "--course", "Unknown"])

    assert result.exit_code != 0
    assert "Course not found: Unknown." in result.output


def test_stats_summary_command_deduplicates_matching_activity_round(
    monkeypatch: MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("GARMIN_GOLF_DATA_DIR", str(tmp_path))
    storage = Storage(Settings())
    storage.upsert_rows(
        "rounds",
        [
            {
                "round_id": 1001,
                "scorecard_id": 1001,
                "played_on": "2025-06-01",
                "start_time": "2025-06-01T07:00:00.000Z",
                "course_name": "Golf National",
                "total_score": 82,
                "total_par": 72,
                "player_profile_id": 77,
            },
            {
                "round_id": 2001,
                "activity_id": 2001,
                "played_on": "2025-06-01",
                "start_time": "2025-06-01T08:00:00.0",
                "course_name": "Chateaufort Golf",
                "data_source": "activities",
                "player_profile_id": 77,
            },
        ],
        unique_by=["round_id"],
    )
    storage.upsert_rows(
        "holes",
        [{"round_id": 1001, "hole_number": 1, "putts": 2, "gir": True, "fairway_hit": True}],
        unique_by=["round_id", "hole_number"],
    )

    runner = CliRunner()
    result = runner.invoke(app, ["stats", "summary"])

    assert result.exit_code == 0
    assert "rounds_played" in result.stdout
    assert "average_putts_per_round" in result.stdout
    assert "2.0" in result.stdout


def test_stats_round_command_accepts_matching_activity_id(
    monkeypatch: MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("GARMIN_GOLF_DATA_DIR", str(tmp_path))
    storage = Storage(Settings())
    storage.upsert_rows(
        "rounds",
        [
            {
                "round_id": 1001,
                "scorecard_id": 1001,
                "played_on": "2025-06-01",
                "start_time": "2025-06-01T07:00:00.000Z",
                "course_name": "Golf National",
                "total_score": 82,
                "total_par": 72,
                "player_profile_id": 77,
            },
            {
                "round_id": 2001,
                "activity_id": 2001,
                "played_on": "2025-06-01",
                "start_time": "2025-06-01T08:00:00.0",
                "course_name": "Chateaufort Golf",
                "data_source": "activities",
                "player_profile_id": 77,
            },
        ],
        unique_by=["round_id"],
    )
    storage.upsert_rows(
        "holes",
        [{"round_id": 1001, "hole_number": 1, "putts": 2, "gir": True, "fairway_hit": True}],
        unique_by=["round_id", "hole_number"],
    )

    runner = CliRunner()
    result = runner.invoke(app, ["stats", "round", "--round-id", "2001"])

    assert result.exit_code == 0
    assert "Round 1001" in result.stdout
    assert "round_id" in result.stdout
    assert "1001" in result.stdout


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
    assert 'data_dir = "/home/you/garmin-golf-data"' in config_file.read_text(encoding="utf-8")
