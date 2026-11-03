import json
from pathlib import Path

import polars as pl
import pytest
from _pytest.monkeypatch import MonkeyPatch
from typer.testing import CliRunner

from garmin_golf.cli import app
from garmin_golf.config import Settings
from garmin_golf.history import by_nine, hole_distances, rarity, round_value
from garmin_golf.storage import Storage


@pytest.fixture
def history(monkeypatch: MonkeyPatch, tmp_path: Path) -> Storage:
    monkeypatch.setenv("GARMIN_GOLF_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("GARMIN_GOLF_CONFIG_FILE", str(tmp_path / "config.toml"))
    store = Storage(Settings())
    store.upsert_rows(
        "rounds",
        [
            {
                "round_id": rid,
                "played_on": day,
                "course_name": course,
                "total_score": 72 + rid,
                "total_par": 72,
                "exclude_from_stats": rid == 3,
            }
            for rid, day, course in [
                (1, "2024-01-01", "A"),
                (2, "2025-01-01", "A"),
                (3, "2025-02-01", "A"),
                (4, "2025-03-01", "B"),
                (5, "2025-04-01", "A"),
                (6, "2025-05-01", "A"),
            ]
        ],
        unique_by=["round_id"],
    )
    store.upsert_rows(
        "holes",
        [
            {
                "round_id": rid,
                "hole_number": hole,
                "par": 4,
                "strokes": 4 + rid % 3,
                "putts": 2,
                "penalties": 0,
                "gir": True,
                "fairway_hit": True,
                "pin_position_lat": 48.003,
                "pin_position_lon": 2.0,
            }
            for rid in range(1, 7)
            for hole in range(1, 19)
        ],
        unique_by=["round_id", "hole_number"],
    )
    store.upsert_rows(
        "shots",
        [
            {
                "round_id": rid,
                "hole_number": hole,
                "shot_number": shot,
                "club_id": shot,
                "club": {1: "Driver", 2: "Iron", 3: "Putter"}[shot],
                "shot_type": {1: "TEE", 2: "APPROACH", 3: "PUTT"}[shot],
                "distance_meters": 200.0 if shot == 1 else 5.0,
                "start_lat": 48.0,
                "start_lon": 2.0,
                "end_lat": 48.002,
                "end_lon": 2.0,
            }
            for rid in range(1, 7)
            for hole in range(1, 19)
            for shot in (1, 2, 3)
        ],
        unique_by=["round_id", "hole_number", "shot_number"],
    )
    return store


def invoke(*args: str) -> object:
    result = CliRunner().invoke(app, [*args, "--json"])
    assert result.exit_code == 0, result.output + str(result.exception)
    return json.loads(result.stdout)


def test_round_baseline_excludes_target_future_and_excluded(history: Storage) -> None:
    result = invoke("stats", "round", "--round-id", "5", "--vs-baseline", "--last-rounds", "1")
    assert isinstance(result, dict)
    assert result["vs_course_baseline"]["window"]["round_ids"] == [2]
    assert result["vs_course_baseline"]["holes"][0]["delta_to_par"] == 0
    assert result["vs_course_baseline"]["holes"][0]["exploratory"] is True
    assert result["by_nine"]["front_nine"]["to_par"] == 18
    assert result["by_nine"]["back_nine"]["complete"] is True
    assert result["putting"]


def test_course_windows_and_all_default(history: Storage) -> None:
    all_result = invoke("stats", "course", "--course", "A")
    explicit = invoke("stats", "course", "--course", "A", "--all")
    assert all_result == explicit
    assert isinstance(explicit, dict)
    assert explicit["window"]["rounds"] == 4
    recent = invoke("stats", "course", "--course", "A", "--last-rounds", "2", "--hole", "1")
    assert isinstance(recent, dict)
    assert recent["window"]["round_ids"] == [6, 5]
    assert len(recent["holes"]) == 1
    assert recent["tee_distances"][0]["n"] == 2
    assert recent["tee_distances"][0]["observed_tee_bearing_deg"] == 0


@pytest.mark.parametrize("command", ["course", "clubs", "trends"])
def test_conflicting_windows(history: Storage, command: str) -> None:
    args = ["stats", command, "--all", "--last-rounds", "2", "--json"]
    if command == "course":
        args += ["--course", "A"]
    result = CliRunner().invoke(app, args)
    assert result.exit_code == 2
    assert "not together" in result.output


@pytest.mark.parametrize("command", ["tee-shots", "second-shots", "approaches"])
def test_shot_scopes(history: Storage, command: str) -> None:
    scoped = invoke("stats", command, "--course", "A", "--hole", "1")
    assert isinstance(scoped, list)
    if command == "tee-shots":
        assert sum(row["tee_shots"] for row in scoped) == 4
    elif command == "second-shots":
        assert sum(row["second_shots"] for row in scoped) == 4
    else:
        assert len(scoped) == 4
        assert {row["hole_number"] for row in scoped} == {1}
        assert {row["round_id"] for row in scoped} == {1, 2, 5, 6}
    bad = CliRunner().invoke(app, ["stats", command, "--course", "missing", "--json"])
    assert bad.exit_code == 2


def test_rarity_and_analyze_course_window(history: Storage) -> None:
    result = invoke("stats", "rarity", "--metric", "front_back_gap", "--round-id", "5", "--all")
    assert isinstance(result, dict)
    assert result["n"] == 2
    assert result["value"] == 0
    assert result["percentile"] == 50
    assert result["z"] is None
    analysis = invoke("analyze", "course", "--course", "A", "--all")
    assert isinstance(analysis, dict)
    assert analysis["scope"]["rounds"] == 4


def test_context_update_clear_and_preserve(history: Storage) -> None:
    annotated = invoke(
        "stats",
        "annotate-round",
        "--round-id",
        "5",
        "--wind-speed-kmh",
        "15",
        "--wind-dir-deg",
        "270",
        "--temp-c",
        "8",
        "--source",
        "manual",
    )
    assert isinstance(annotated, dict)
    assert annotated["context"]["wind_speed_kmh"] == 15
    history.upsert_rows(
        "rounds",
        [{"round_id": 5, "course_name": "A", "played_on": "2025-04-01"}],
        unique_by=["round_id"],
        preserve_columns=["wind_speed_kmh", "wind_dir_deg", "temp_c", "source"],
    )
    result = invoke("stats", "round", "--round-id", "5")
    assert isinstance(result, dict)
    assert result["context"]["source"] == "manual"
    cleared = invoke("stats", "annotate-round", "--round-id", "5", "--clear-context")
    assert isinstance(cleared, dict)
    assert all(v is None for v in cleared["context"].values())


def test_missing_samples_and_geometry() -> None:
    assert rarity(None, [])["z"] is None
    assert rarity(2, [1, 2, 3])["percentile"] == 50
    holes = pl.DataFrame({"hole_number": [1], "strokes": [None], "par": [4]})
    assert by_nine(holes)["front_nine"]["to_par"] is None
    assert round_value(holes, "front_back_gap") is None
    assert hole_distances(pl.DataFrame()) == []


def test_round_with_no_matching_baseline_is_not_zero(history: Storage) -> None:
    result = invoke(
        "stats",
        "round",
        "--round-id",
        "5",
        "--vs-baseline",
        "--from",
        "2025-03-01",
        "--to",
        "2025-03-31",
    )
    assert isinstance(result, dict)
    comparison = result["vs_course_baseline"]
    assert comparison["window"]["rounds"] == 0
    assert all(row["delta_to_par"] is None for row in comparison["holes"])
    assert all(row["rounds_played"] == 0 for row in comparison["holes"])


@pytest.mark.parametrize(
    "command,count_key",
    [("tee-shots", "tee_shots"), ("second-shots", "second_shots"), ("putting", "holes")],
)
def test_shot_windows_after_course_and_exclusion(
    history: Storage, command: str, count_key: str
) -> None:
    history.upsert_rows(
        "rounds",
        [
            {
                "round_id": 7,
                "played_on": "2025-06-01",
                "course_name": "A",
                "exclude_from_stats": True,
            }
        ],
        unique_by=["round_id"],
    )
    recent = invoke("stats", command, "--course", "A", "--hole", "1", "--last-rounds", "3")
    assert isinstance(recent, list)
    assert sum(row[count_key] for row in recent) == 3
    all_result = invoke("stats", command, "--course", "A", "--hole", "1", "--all")
    default = invoke("stats", command, "--course", "A", "--hole", "1")
    assert all_result == default
    assert isinstance(all_result, list)
    assert sum(row[count_key] for row in all_result) == 4
    dated = invoke(
        "stats",
        command,
        "--course",
        "A",
        "--hole",
        "1",
        "--from",
        "2025-04-01",
        "--to",
        "2025-05-01",
    )
    assert isinstance(dated, list)
    assert sum(row[count_key] for row in dated) == 2


@pytest.mark.parametrize("command", ["tee-shots", "second-shots", "putting"])
@pytest.mark.parametrize(
    "options",
    [
        ["--all", "--last-rounds", "2"],
        ["--all", "--period", "this-year"],
        ["--last-rounds", "2", "--from", "2025-01-01"],
        ["--last-rounds", "0"],
    ],
)
def test_shot_window_conflicts_on_empty_store(
    monkeypatch: MonkeyPatch, tmp_path: Path, command: str, options: list[str]
) -> None:
    monkeypatch.setenv("GARMIN_GOLF_DATA_DIR", str(tmp_path))
    result = CliRunner().invoke(app, ["stats", command, *options, "--json"])
    assert result.exit_code == 2


@pytest.mark.parametrize("command", ["tee-shots", "second-shots", "putting"])
def test_empty_shot_windows(history: Storage, command: str) -> None:
    assert invoke("stats", command, "--from", "2030-01-01") == []


def test_putting_hole_scopes_and_unknown_course(history: Storage) -> None:
    scoped = invoke("stats", "putting", "--course", "B", "--hole", "2", "--all")
    assert isinstance(scoped, list)
    assert sum(row["holes"] for row in scoped) == 1
    global_hole = invoke("stats", "putting", "--hole", "2", "--all")
    assert isinstance(global_hole, list)
    assert sum(row["holes"] for row in global_hole) == 5
    unknown = CliRunner().invoke(app, ["stats", "putting", "--course", "missing", "--json"])
    assert unknown.exit_code == 2


def test_round_exposes_gps_geometry_and_weather_separately(history: Storage) -> None:
    result = invoke("stats", "round", "--round-id", "5")
    assert isinstance(result, dict)
    geometry = result["geometry"]
    assert geometry["round_id"] == 5
    assert geometry["played_on"] == "2025-04-01"
    assert len(geometry["holes"]) == 18
    hole = geometry["holes"][0]
    assert hole["round_id"] == 5
    assert hole["source"] == "tee_to_recorded_pin"
    assert hole["bearing_deg"] == 0
    assert len(hole["shots"]) == 2
    assert hole["start"] == {"lat": 48.0, "lon": 2.0}
    assert all(value is None for value in result["context"].values())


def test_tee_distance_rarity(history: Storage) -> None:
    shots = history.read_table("shots").with_columns(
        pl.when((pl.col("shot_number") == 1) & (pl.col("hole_number") == 1))
        .then(100.0 + pl.col("round_id") * 10)
        .otherwise(pl.col("distance_meters"))
        .alias("distance_meters")
    )
    history.upsert_rows(
        "shots", shots.to_dicts(), unique_by=["round_id", "hole_number", "shot_number"]
    )
    result = invoke(
        "stats",
        "rarity",
        "--metric",
        "tee_distance_m",
        "--round-id",
        "5",
        "--course",
        "A",
        "--hole",
        "1",
        "--all",
    )
    assert isinstance(result, dict)
    assert result["value"] == 150
    assert result["mean"] == 115
    assert result["n"] == 2
    assert result["percentile"] == 100
    assert result["z"] == pytest.approx(35 / (50**0.5))
    assert result["window"]["round_ids"] == [2, 1]
    assert result["hole_number"] == 1
    assert result["exploratory"] is True
    recent = invoke(
        "stats",
        "rarity",
        "--metric",
        "tee_distance_m",
        "--round-id",
        "5",
        "--hole",
        "1",
        "--last-rounds",
        "1",
    )
    assert isinstance(recent, dict)
    assert recent["window"]["round_ids"] == [2]


@pytest.mark.parametrize("distance", [None, 0.0, -1.0, float("nan"), float("inf")])
def test_tee_rarity_skips_invalid_samples(history: Storage, distance: float | None) -> None:
    history.upsert_rows(
        "shots",
        [
            {
                "round_id": 2,
                "hole_number": 1,
                "shot_number": 1,
                "shot_type": "TEE",
                "distance_meters": distance,
            }
        ],
        unique_by=["round_id", "hole_number", "shot_number"],
    )
    result = invoke(
        "stats",
        "rarity",
        "--metric",
        "tee_distance_m",
        "--round-id",
        "5",
        "--hole",
        "1",
        "--last-rounds",
        "1",
    )
    assert isinstance(result, dict)
    assert result["window"]["round_ids"] == [1]
    assert result["z"] is None
    missing = invoke(
        "stats",
        "rarity",
        "--metric",
        "tee_distance_m",
        "--round-id",
        "2",
        "--hole",
        "1",
        "--from",
        "2030-01-01",
    )
    assert isinstance(missing, dict)
    assert missing["value"] is None
    assert missing["n"] == 0
    assert missing["percentile"] is None


@pytest.mark.parametrize(
    "options",
    [
        ["--metric", "tee_distance_m"],
        ["--metric", "front_back_gap", "--hole", "1"],
        ["--metric", "tee_distance_m", "--hole", "0"],
        ["--metric", "tee_distance_m", "--hole", "1", "--course", "B"],
        ["--metric", "tee_distance_m", "--hole", "1", "--all", "--last-rounds", "1"],
    ],
)
def test_tee_rarity_invalid_options(history: Storage, options: list[str]) -> None:
    result = CliRunner().invoke(app, ["stats", "rarity", "--round-id", "5", *options, "--json"])
    assert result.exit_code == 2
