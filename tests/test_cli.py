import json
from datetime import date
from pathlib import Path

from _pytest.monkeypatch import MonkeyPatch
from typer.testing import CliRunner

from garmin_golf.cli import (
    _build_club_inventory_table,
    _resolve_date_window,
    _shots_with_configured_club_names,
    _shots_with_normalized_shot_types,
    app,
)
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


def test_config_set_club_name_command_creates_override(
    monkeypatch: MonkeyPatch, tmp_path: Path
) -> None:
    config_file = tmp_path / "config.toml"
    monkeypatch.setenv("GARMIN_GOLF_CONFIG_FILE", str(config_file))

    runner = CliRunner()
    result = runner.invoke(
        app,
        ["config", "set-club-name", "--club-id", "10400977", "--name", "58 Wedge"],
    )

    assert result.exit_code == 0
    assert '"10400977" = "58 Wedge"' in config_file.read_text(encoding="utf-8")


def test_config_set_club_name_command_updates_existing_override(
    monkeypatch: MonkeyPatch, tmp_path: Path
) -> None:
    config_file = tmp_path / "config.toml"
    config_file.write_text('[club_name_overrides]\n"10400977" = "Lob Wedge"\n', encoding="utf-8")
    monkeypatch.setenv("GARMIN_GOLF_CONFIG_FILE", str(config_file))

    runner = CliRunner()
    result = runner.invoke(
        app,
        ["config", "set-club-name", "--club-id", "10400977", "--name", "58 Wedge"],
    )

    assert result.exit_code == 0
    content = config_file.read_text(encoding="utf-8")
    assert '"10400977" = "58 Wedge"' in content
    assert '"10400977" = "Lob Wedge"' not in content


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


def test_stats_summary_command_json(monkeypatch: MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("GARMIN_GOLF_DATA_DIR", str(tmp_path))
    storage = Storage(Settings())
    storage.upsert_rows(
        "rounds",
        [{"round_id": 1, "total_score": 82, "total_par": 72}],
        unique_by=["round_id"],
    )

    runner = CliRunner()
    result = runner.invoke(app, ["stats", "summary", "--json"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["rounds_played"] == 1
    assert "Golf Summary" not in result.stdout


def test_stats_practice_focus_command(monkeypatch: MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("GARMIN_GOLF_DATA_DIR", str(tmp_path))
    storage = Storage(Settings())
    storage.upsert_rows(
        "rounds",
        [{"round_id": 1, "played_on": "2025-06-01", "total_score": 84, "total_par": 72}],
        unique_by=["round_id"],
    )
    storage.upsert_rows(
        "holes",
        [
            {
                "round_id": 1,
                "hole_number": hole,
                "par": 4,
                "strokes": 5 if hole <= 6 else 4,
                "putts": 3 if hole <= 2 else 2,
                "gir": hole > 8,
                "fairway_hit": hole > 6,
                "penalties": 1 if hole == 1 else 0,
            }
            for hole in range(1, 19)
        ],
        unique_by=["round_id", "hole_number"],
    )

    runner = CliRunner()
    result = runner.invoke(app, ["stats", "practice-focus"])

    assert result.exit_code == 0
    assert "Practice Focus" in result.stdout
    assert "priority_1" in result.stdout
    assert "estimated_strokes_to_save_per_18" in result.stdout


def test_stats_trends_command_json(monkeypatch: MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("GARMIN_GOLF_DATA_DIR", str(tmp_path))
    storage = Storage(Settings())
    storage.upsert_rows(
        "rounds",
        [
            {
                "round_id": 1,
                "played_on": "2025-06-01",
                "course_name": "Blue Hills",
                "total_score": 90,
                "total_par": 72,
            },
            {
                "round_id": 2,
                "played_on": "2025-06-08",
                "course_name": "Blue Hills",
                "total_score": 81,
                "total_par": 72,
            },
            {
                "round_id": 3,
                "played_on": "2025-06-15",
                "course_name": "Red Oaks",
                "total_score": 72,
                "total_par": 72,
            },
        ],
        unique_by=["round_id"],
    )
    storage.upsert_rows(
        "holes",
        [
            {
                "round_id": round_id,
                "hole_number": hole,
                "par": 4,
                "strokes": 5 if round_id == 1 else 4 if round_id == 3 or hole <= 9 else 5,
                "putts": 2,
                "gir": round_id == 3 or (round_id == 2 and hole <= 9),
                "fairway_hit": round_id == 3 or (round_id == 2 and hole <= 9),
                "penalties": (
                    1
                    if (round_id == 1 and hole <= 2) or (round_id == 2 and hole == 1)
                    else 0
                ),
            }
            for round_id in (1, 2, 3)
            for hole in range(1, 19)
        ],
        unique_by=["round_id", "hole_number"],
    )

    runner = CliRunner()
    result = runner.invoke(app, ["stats", "trends", "--window", "5", "--json"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert len(payload) == 3
    assert payload[0]["round_id"] == 3
    assert payload[0]["course_name"] == "Red Oaks"
    assert payload[0]["window_average_to_par"] == 9.0
    assert payload[0]["delta_average_to_par"] is None


def test_stats_trends_command(monkeypatch: MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("GARMIN_GOLF_DATA_DIR", str(tmp_path))
    storage = Storage(Settings())
    storage.upsert_rows(
        "rounds",
        [
            {
                "round_id": 1,
                "played_on": "2025-06-01",
                "course_name": "Blue Hills",
                "total_score": 90,
                "total_par": 72,
            },
            {
                "round_id": 2,
                "played_on": "2025-06-08",
                "course_name": "Blue Hills",
                "total_score": 81,
                "total_par": 72,
            },
        ],
        unique_by=["round_id"],
    )
    storage.upsert_rows(
        "holes",
        [
            {
                "round_id": round_id,
                "hole_number": hole,
                "par": 4,
                "strokes": 5 if round_id == 1 else 4 if hole <= 9 else 5,
                "putts": 2,
                "gir": round_id == 2 and hole <= 9,
                "fairway_hit": round_id == 2 and hole <= 9,
                "penalties": 1 if round_id == 1 and hole <= 2 else 0,
            }
            for round_id in (1, 2)
            for hole in range(1, 19)
        ],
        unique_by=["round_id", "hole_number"],
    )

    runner = CliRunner()
    result = runner.invoke(app, ["stats", "trends", "--window", "5"], terminal_width=200)

    assert result.exit_code == 0
    assert "Round Trends" in result.stdout
    assert "Last 5" in result.stdout


def test_stats_trends_command_metric_json(monkeypatch: MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("GARMIN_GOLF_DATA_DIR", str(tmp_path))
    storage = Storage(Settings())
    storage.upsert_rows(
        "rounds",
        [
            {
                "round_id": 1,
                "played_on": "2025-06-01",
                "course_name": "Blue Hills",
                "total_score": 90,
                "total_par": 72,
            },
            {
                "round_id": 2,
                "played_on": "2025-06-08",
                "course_name": "Blue Hills",
                "total_score": 81,
                "total_par": 72,
            },
            {
                "round_id": 3,
                "played_on": "2025-06-15",
                "course_name": "Red Oaks",
                "total_score": 72,
                "total_par": 72,
            },
        ],
        unique_by=["round_id"],
    )
    storage.upsert_rows(
        "holes",
        [
            {
                "round_id": round_id,
                "hole_number": hole,
                "par": 4,
                "strokes": 5 if round_id == 1 else 4 if round_id == 3 or hole <= 9 else 5,
                "putts": 2,
                "gir": round_id == 3 or (round_id == 2 and hole <= 9),
                "fairway_hit": round_id == 3 or (round_id == 2 and hole <= 9),
                "penalties": (
                    1
                    if (round_id == 1 and hole <= 2) or (round_id == 2 and hole == 1)
                    else 0
                ),
            }
            for round_id in (1, 2, 3)
            for hole in range(1, 19)
        ],
        unique_by=["round_id", "hole_number"],
    )

    runner = CliRunner()
    result = runner.invoke(
        app,
        ["stats", "trends", "--metric", "gir_pct", "--window", "5", "--json"],
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert len(payload) == 3
    assert payload[0]["metric"] == "gir_pct"
    assert payload[0]["round_value"] == 100.0
    assert payload[0]["window_value"] == 50.0
    assert payload[0]["delta_value"] is None
    assert set(payload[0]) == {
        "played_on",
        "round_id",
        "course_name",
        "window",
        "metric",
        "round_value",
        "window_value",
        "delta_value",
    }


def test_stats_trends_command_rejects_unknown_metric(
    monkeypatch: MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("GARMIN_GOLF_DATA_DIR", str(tmp_path))
    storage = Storage(Settings())
    storage.upsert_rows(
        "rounds",
        [{"round_id": 1, "played_on": "2025-06-01", "total_score": 90, "total_par": 72}],
        unique_by=["round_id"],
    )

    runner = CliRunner()
    result = runner.invoke(app, ["stats", "trends", "--metric", "bad_metric"])

    assert result.exit_code != 0
    assert "Unsupported trend metric: bad_metric." in result.output


def test_stats_second_shots_command(monkeypatch: MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("GARMIN_GOLF_DATA_DIR", str(tmp_path))
    storage = Storage(Settings())
    storage.upsert_rows(
        "rounds",
        [{"round_id": 1, "played_on": "2025-06-01", "total_score": 84, "total_par": 72}],
        unique_by=["round_id"],
    )
    storage.upsert_rows(
        "holes",
        [
            {"round_id": 1, "hole_number": 1, "par": 4, "strokes": 4},
            {"round_id": 1, "hole_number": 2, "par": 5, "strokes": 6},
        ],
        unique_by=["round_id", "hole_number"],
    )
    storage.upsert_rows(
        "shots",
        [
            {
                "round_id": 1,
                "hole_number": 1,
                "shot_number": 2,
                "club": "8 Iron",
                "distance_meters": 135.0,
            },
            {
                "round_id": 1,
                "hole_number": 2,
                "shot_number": 2,
                "club": "3 Wood",
                "distance_meters": 205.0,
            },
        ],
        unique_by=["round_id", "hole_number", "shot_number"],
    )

    runner = CliRunner()
    result = runner.invoke(app, ["stats", "second-shots"])

    assert result.exit_code == 0
    assert "Second Shots" in result.stdout
    assert "3 Wood" in result.stdout
    assert "8 Iron" in result.stdout


def test_stats_second_shots_command_json(monkeypatch: MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("GARMIN_GOLF_DATA_DIR", str(tmp_path))
    storage = Storage(Settings())
    storage.upsert_rows(
        "rounds",
        [{"round_id": 1, "played_on": "2025-06-01", "total_score": 84, "total_par": 72}],
        unique_by=["round_id"],
    )
    storage.upsert_rows(
        "holes",
        [{"round_id": 1, "hole_number": 1, "par": 4, "strokes": 4}],
        unique_by=["round_id", "hole_number"],
    )
    storage.upsert_rows(
        "shots",
        [
            {
                "round_id": 1,
                "hole_number": 1,
                "shot_number": 2,
                "club": "8 Iron",
                "distance_meters": 135.0,
            }
        ],
        unique_by=["round_id", "hole_number", "shot_number"],
    )

    runner = CliRunner()
    result = runner.invoke(app, ["stats", "second-shots", "--json"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert isinstance(payload, list)
    assert payload[0]["club"] == "8 Iron"


def test_stats_second_shots_command_uses_club_name_overrides(
    monkeypatch: MonkeyPatch, tmp_path: Path
) -> None:
    data_dir = tmp_path / "data"
    config_file = tmp_path / "config.toml"
    config_file.write_text('[club_name_overrides]\n"10400977" = "56 Wedge"\n', encoding="utf-8")
    monkeypatch.setenv("GARMIN_GOLF_DATA_DIR", str(data_dir))
    monkeypatch.setenv("GARMIN_GOLF_CONFIG_FILE", str(config_file))

    storage = Storage(Settings())
    storage.upsert_rows(
        "rounds",
        [{"round_id": 1, "played_on": "2025-06-01", "total_score": 84, "total_par": 72}],
        unique_by=["round_id"],
    )
    storage.upsert_rows(
        "holes",
        [{"round_id": 1, "hole_number": 1, "par": 4, "strokes": 4}],
        unique_by=["round_id", "hole_number"],
    )
    storage.upsert_rows(
        "shots",
        [
            {
                "round_id": 1,
                "hole_number": 1,
                "shot_number": 2,
                "club_id": 10400977,
                "distance_meters": 70.0,
            }
        ],
        unique_by=["round_id", "hole_number", "shot_number"],
    )

    runner = CliRunner()
    result = runner.invoke(app, ["stats", "second-shots"])

    assert result.exit_code == 0
    assert "56" in result.stdout
    assert "Wedge" in result.stdout


def test_stats_clubs_command_shows_default_and_configured_names(
    monkeypatch: MonkeyPatch, tmp_path: Path
) -> None:
    data_dir = tmp_path / "data"
    config_file = tmp_path / "config.toml"
    config_file.write_text('[club_name_overrides]\n"10400977" = "56 Wedge"\n', encoding="utf-8")
    monkeypatch.setenv("GARMIN_GOLF_DATA_DIR", str(data_dir))
    monkeypatch.setenv("GARMIN_GOLF_CONFIG_FILE", str(config_file))

    storage = Storage(Settings())
    storage.upsert_rows(
        "shots",
        [
            {
                "round_id": 1,
                "hole_number": 1,
                "shot_number": 2,
                "club_id": 10400977,
                "club": "Lob Wedge",
            }
        ],
        unique_by=["round_id", "hole_number", "shot_number"],
    )

    runner = CliRunner()
    result = runner.invoke(app, ["stats", "clubs"], terminal_width=200)

    assert result.exit_code == 0
    assert "Clubs" in result.stdout
    inventory = _build_club_inventory_table(
        _shots_with_normalized_shot_types(storage.read_table("shots")),
        _shots_with_configured_club_names(storage.read_table("shots")),
    )
    row = inventory.row(0, named=True)
    assert row["default_name"] == "Lob Wedge"
    assert row["configured_name"] == "56 Wedge"


def test_stats_clubs_command_treats_first_shots_as_tee(
    monkeypatch: MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("GARMIN_GOLF_DATA_DIR", str(tmp_path))
    storage = Storage(Settings())
    storage.upsert_rows(
        "shots",
        [
            {
                "round_id": 1,
                "hole_number": 1,
                "shot_number": 1,
                "club_id": 10400961,
                "club": "Driver",
                "shot_type": "APPROACH",
                "distance_meters": 200.0,
            },
            {
                "round_id": 1,
                "hole_number": 1,
                "shot_number": 2,
                "club_id": 10400961,
                "club": "Driver",
                "shot_type": "APPROACH",
                "distance_meters": 150.0,
            },
        ],
        unique_by=["round_id", "hole_number", "shot_number"],
    )

    runner = CliRunner()
    result = runner.invoke(app, ["stats", "clubs"], terminal_width=200)

    assert result.exit_code == 0
    inventory = _build_club_inventory_table(
        _shots_with_normalized_shot_types(storage.read_table("shots")),
        _shots_with_configured_club_names(storage.read_table("shots")),
    )
    row = inventory.row(0, named=True)
    assert row["shot_type_breakdown"] == "APPROACH: 1, TEE: 1"


def test_stats_clubs_command_json(monkeypatch: MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("GARMIN_GOLF_DATA_DIR", str(tmp_path))
    storage = Storage(Settings())
    storage.upsert_rows(
        "shots",
        [
            {
                "round_id": 1,
                "hole_number": 1,
                "shot_number": 2,
                "club_id": 10400977,
                "club": "Lob Wedge",
            }
        ],
        unique_by=["round_id", "hole_number", "shot_number"],
    )

    runner = CliRunner()
    result = runner.invoke(app, ["stats", "clubs", "--json"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert isinstance(payload, list)
    assert payload[0]["default_name"] == "Lob Wedge"


def test_stats_clubs_command_by_context_json(
    monkeypatch: MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("GARMIN_GOLF_DATA_DIR", str(tmp_path))
    storage = Storage(Settings())
    storage.upsert_rows(
        "holes",
        [
            {"round_id": 1, "hole_number": 1, "par": 4, "strokes": 4},
            {"round_id": 1, "hole_number": 2, "par": 3, "strokes": 3},
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
                "club_id": 10400961,
                "club": "Driver",
                "shot_type": "TEE",
                "lie": "TEE_BOX",
                "distance_meters": 220.0,
            },
            {
                "round_id": 1,
                "hole_number": 2,
                "shot_number": 1,
                "club_id": 10400965,
                "club": "7 Iron",
                "shot_type": "APPROACH",
                "lie": "TEE_BOX",
                "distance_meters": 155.0,
            },
        ],
        unique_by=["round_id", "hole_number", "shot_number"],
    )

    runner = CliRunner()
    result = runner.invoke(app, ["stats", "clubs", "--by-context", "--json"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert isinstance(payload, list)
    assert payload[0]["context"] in {"tee_par_3", "tee_par_4"}
    assert {row["context"] for row in payload} == {"tee_par_3", "tee_par_4"}


def test_stats_clubs_command_filters_by_course(
    monkeypatch: MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("GARMIN_GOLF_DATA_DIR", str(tmp_path))
    storage = Storage(Settings())
    storage.upsert_rows(
        "rounds",
        [
            {"round_id": 1, "played_on": "2025-06-01", "course_name": "Course A"},
            {"round_id": 2, "played_on": "2025-06-02", "course_name": "Course B"},
        ],
        unique_by=["round_id"],
    )
    storage.upsert_rows(
        "shots",
        [
            {"round_id": 1, "hole_number": 1, "shot_number": 1, "club_id": 1, "club": "Driver"},
            {"round_id": 2, "hole_number": 1, "shot_number": 1, "club_id": 2, "club": "3 Wood"},
        ],
        unique_by=["round_id", "hole_number", "shot_number"],
    )

    runner = CliRunner()
    result = runner.invoke(app, ["stats", "clubs", "--course", "Course A", "--json"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert len(payload) == 1
    assert payload[0]["default_name"] == "Driver"


def test_stats_clubs_command_filters_by_course_and_hole(
    monkeypatch: MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("GARMIN_GOLF_DATA_DIR", str(tmp_path))
    storage = Storage(Settings())
    storage.upsert_rows(
        "rounds",
        [{"round_id": 1, "played_on": "2025-06-01", "course_name": "Course A"}],
        unique_by=["round_id"],
    )
    storage.upsert_rows(
        "shots",
        [
            {"round_id": 1, "hole_number": 3, "shot_number": 1, "club_id": 1, "club": "Driver"},
            {"round_id": 1, "hole_number": 7, "shot_number": 1, "club_id": 2, "club": "3 Wood"},
        ],
        unique_by=["round_id", "hole_number", "shot_number"],
    )

    runner = CliRunner()
    result = runner.invoke(
        app,
        ["stats", "clubs", "--course", "Course A", "--hole", "7", "--json"],
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert len(payload) == 1
    assert payload[0]["default_name"] == "3 Wood"


def test_stats_clubs_command_by_context_filters_by_course_and_hole(
    monkeypatch: MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("GARMIN_GOLF_DATA_DIR", str(tmp_path))
    storage = Storage(Settings())
    storage.upsert_rows(
        "rounds",
        [
            {"round_id": 1, "played_on": "2025-06-01", "course_name": "Course A"},
            {"round_id": 2, "played_on": "2025-06-02", "course_name": "Course B"},
        ],
        unique_by=["round_id"],
    )
    storage.upsert_rows(
        "holes",
        [
            {"round_id": 1, "hole_number": 7, "par": 4, "strokes": 4},
            {"round_id": 2, "hole_number": 7, "par": 4, "strokes": 4},
        ],
        unique_by=["round_id", "hole_number"],
    )
    storage.upsert_rows(
        "shots",
        [
            {
                "round_id": 1,
                "hole_number": 7,
                "shot_number": 1,
                "club_id": 1,
                "club": "Driver",
                "shot_type": "TEE",
                "lie": "TEE_BOX",
                "distance_meters": 220.0,
            },
            {
                "round_id": 2,
                "hole_number": 7,
                "shot_number": 1,
                "club_id": 2,
                "club": "3 Wood",
                "shot_type": "TEE",
                "lie": "TEE_BOX",
                "distance_meters": 210.0,
            },
        ],
        unique_by=["round_id", "hole_number", "shot_number"],
    )

    runner = CliRunner()
    result = runner.invoke(
        app,
        ["stats", "clubs", "--by-context", "--course", "Course A", "--hole", "7", "--json"],
    )

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert len(payload) == 1
    assert payload[0]["club"] == "Driver"


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


def test_stats_rounds_command_json(monkeypatch: MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("GARMIN_GOLF_DATA_DIR", str(tmp_path))
    storage = Storage(Settings())
    storage.upsert_rows(
        "rounds",
        [{"round_id": 1, "played_on": "2025-06-01", "course_name": "Blue Hills"}],
        unique_by=["round_id"],
    )

    runner = CliRunner()
    result = runner.invoke(app, ["stats", "rounds", "--json"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload == [
        {
            "round_id": 1,
            "played_on": "2025-06-01",
            "display_course_name": "Blue Hills",
        }
    ]


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


def test_stats_rounds_command_with_no_local_rounds_json(
    monkeypatch: MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("GARMIN_GOLF_DATA_DIR", str(tmp_path))

    runner = CliRunner()
    result = runner.invoke(app, ["stats", "rounds", "--json"])

    assert result.exit_code == 0
    assert json.loads(result.stdout) == []


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


def test_stats_courses_command_json(monkeypatch: MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("GARMIN_GOLF_DATA_DIR", str(tmp_path))
    storage = Storage(Settings())
    storage.upsert_rows(
        "rounds",
        [
            {"round_id": 1, "played_on": "2025-06-01", "course_name": "Blue Hills"},
            {"round_id": 2, "played_on": "2025-06-08", "course_name": "Blue Hills"},
        ],
        unique_by=["round_id"],
    )

    runner = CliRunner()
    result = runner.invoke(app, ["stats", "courses", "--json"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload[0]["course_name"] == "Blue Hills"
    assert payload[0]["rounds_played"] == 2


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


def test_stats_course_command_json(monkeypatch: MonkeyPatch, tmp_path: Path) -> None:
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
            }
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
            }
        ],
        unique_by=["round_id", "hole_number"],
    )

    runner = CliRunner()
    result = runner.invoke(app, ["stats", "course", "--course", "Blue Hills", "--json"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert set(payload) == {"summary", "focus", "holes"}
    assert payload["holes"][0]["hole_number"] == 1


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


def test_stats_round_command_json(monkeypatch: MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("GARMIN_GOLF_DATA_DIR", str(tmp_path))
    storage = Storage(Settings())
    storage.upsert_rows(
        "rounds",
        [
            {
                "round_id": 1001,
                "played_on": "2025-06-01",
                "course_name": "Golf National",
                "total_score": 82,
                "total_par": 72,
            }
        ],
        unique_by=["round_id"],
    )
    storage.upsert_rows(
        "holes",
        [
            {
                "round_id": 1001,
                "hole_number": 1,
                "par": 4,
                "strokes": 5,
                "putts": 2,
                "gir": False,
                "fairway_hit": True,
                "penalties": 1,
            }
        ],
        unique_by=["round_id", "hole_number"],
    )
    storage.upsert_rows(
        "shots",
        [
            {
                "round_id": 1001,
                "hole_number": 1,
                "shot_number": 1,
                "shot_type": "TEE",
                "club": "Driver",
                "distance_meters": 200.0,
            },
            {
                "round_id": 1001,
                "hole_number": 1,
                "shot_number": 2,
                "shot_type": "APPROACH",
                "club": "8 Iron",
                "distance_meters": 135.0,
            },
        ],
        unique_by=["round_id", "hole_number", "shot_number"],
    )

    runner = CliRunner()
    result = runner.invoke(app, ["stats", "round", "--round-id", "1001", "--json"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert set(payload) == {"summary", "holes", "clubs", "second_shots"}
    assert payload["summary"]["round_id"] == 1001
    assert payload["holes"][0]["hole_number"] == 1
    assert {row["club"] for row in payload["clubs"]} == {"Driver", "8 Iron"}


def test_stats_round_command_shows_holes_clubs_and_second_shots(
    monkeypatch: MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("GARMIN_GOLF_DATA_DIR", str(tmp_path))
    storage = Storage(Settings())
    storage.upsert_rows(
        "rounds",
        [
            {
                "round_id": 1001,
                "played_on": "2025-06-01",
                "course_name": "Golf National",
                "total_score": 82,
                "total_par": 72,
            }
        ],
        unique_by=["round_id"],
    )
    storage.upsert_rows(
        "holes",
        [
            {
                "round_id": 1001,
                "hole_number": 1,
                "par": 4,
                "strokes": 5,
                "putts": 2,
                "gir": False,
                "fairway_hit": True,
                "penalties": 1,
            },
            {
                "round_id": 1001,
                "hole_number": 2,
                "par": 5,
                "strokes": 4,
                "putts": 1,
                "gir": True,
                "fairway_hit": False,
                "penalties": 0,
            },
        ],
        unique_by=["round_id", "hole_number"],
    )
    storage.upsert_rows(
        "shots",
        [
            {
                "round_id": 1001,
                "hole_number": 1,
                "shot_number": 1,
                "shot_type": "TEE",
                "club": "Driver",
                "distance_meters": 200.0,
            },
            {
                "round_id": 1001,
                "hole_number": 1,
                "shot_number": 2,
                "shot_type": "APPROACH",
                "club": "8 Iron",
                "distance_meters": 135.0,
            },
            {
                "round_id": 1001,
                "hole_number": 2,
                "shot_number": 1,
                "shot_type": "TEE",
                "club": "3 Wood",
                "distance_meters": 210.0,
            },
            {
                "round_id": 1001,
                "hole_number": 2,
                "shot_number": 2,
                "shot_type": "APPROACH",
                "club": "Hybrid",
                "distance_meters": 160.0,
            },
        ],
        unique_by=["round_id", "hole_number", "shot_number"],
    )

    runner = CliRunner()
    result = runner.invoke(app, ["stats", "round", "--round-id", "1001"], terminal_width=200)

    assert result.exit_code == 0
    assert "Round 1001 (Golf National | 2025-06-01)" in result.stdout
    assert "Round 1001 (Golf National | 2025-06-01): Holes" in result.stdout
    assert "Round 1001 (Golf National | 2025-06-01): Clubs" in result.stdout
    assert "Round 1001 (Golf National | 2025-06-01): Second Shots" in result.stdout
    assert "Driver" in result.stdout
    assert "Hybrid" in result.stdout


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


def test_config_show_path_command_json(monkeypatch: MonkeyPatch, tmp_path: Path) -> None:
    config_file = tmp_path / "config.toml"
    monkeypatch.setenv("GARMIN_GOLF_CONFIG_FILE", str(config_file))

    runner = CliRunner()
    result = runner.invoke(app, ["config", "show-path", "--json"])

    assert result.exit_code == 0
    assert json.loads(result.stdout) == {"config_path": str(config_file)}
