import math

import polars as pl
import pytest

from garmin_golf.round_geometry import build_hole_directions


def _hole(**values: object) -> pl.DataFrame:
    return pl.DataFrame([{"round_id": 1, "hole_number": 1, **values}])


def _shot(**values: object) -> dict[str, object]:
    return {
        "round_id": 1,
        "hole_number": 1,
        "shot_number": 1,
        "shot_type": "TEE",
        "start_lat": 0.0,
        "start_lon": 0.0,
        "end_lat": 0.0,
        "end_lon": 0.001,
        **values,
    }


@pytest.mark.parametrize(
    "lat,lon,bearing",
    [(0.001, 0.0, 0.0), (0.0, 0.001, 90.0), (-0.001, 0.0, 180.0), (0.0, -0.001, 270.0)],
)
def test_pin_bearing_and_tee_shot_differ(lat: float, lon: float, bearing: float) -> None:
    result = build_hole_directions(
        _hole(pin_position_lat=lat, pin_position_lon=lon), pl.DataFrame([_shot()])
    )[0]
    assert result["source"] == "tee_to_recorded_pin"
    assert result["bearing_deg"] == pytest.approx(bearing)
    assert result["straight_line_distance_m"] == pytest.approx(111.195, abs=0.01)
    assert result["shots"][0]["bearing_deg"] == pytest.approx(90)
    assert result["observation_rounds"] == 1


def test_last_endpoint_fallback_and_individual_dogleg_shots() -> None:
    shots = pl.DataFrame(
        [
            _shot(),
            _shot(
                shot_number=2, shot_type="APPROACH", start_lon=0.001, end_lat=0.001, end_lon=0.001
            ),
            _shot(shot_number=3, shot_type="PUTT", end_lat=0.001, end_lon=0.001),
        ]
    )
    result = build_hole_directions(_hole(), shots)[0]
    assert result["source"] == "tee_to_last_recorded_endpoint"
    assert result["endpoint_shot_number"] == 3
    assert result["bearing_deg"] == pytest.approx(45, abs=0.001)
    assert [row["bearing_deg"] for row in result["shots"]] == pytest.approx([90, 0])
    assert len(result["shots"]) == 2


def test_tee_only_fallback_and_semicircles() -> None:
    semicircle = (2**31) / 180
    shots = pl.DataFrame(
        [
            _shot(
                start_lat=48 * semicircle,
                start_lon=2 * semicircle,
                end_lat=48.001 * semicircle,
                end_lon=2 * semicircle,
            )
        ]
    )
    result = build_hole_directions(_hole(), shots)[0]
    assert result["source"] == "tee_shot_start_to_end"
    assert result["bearing_deg"] == 0
    assert result["start"]["lat"] == pytest.approx(48)
    assert result["straight_line_distance_m"] == pytest.approx(111.195, abs=0.01)


@pytest.mark.parametrize(
    "values",
    [
        {"start_lat": None},
        {"end_lon": math.nan},
        {"end_lon": math.inf},
        {"end_lat": 2**40},
        {"end_lat": 0, "end_lon": 0},
        {"end_lat": 0, "end_lon": 0.000001},
    ],
)
def test_missing_or_degenerate_coordinates(values: dict[str, object]) -> None:
    result = build_hole_directions(_hole(), pl.DataFrame([_shot(**values)]))[0]
    assert result["bearing_deg"] is None
    assert result["source"] == "unavailable"
    assert result["unavailable_reason"]


def test_no_tee_no_guessed_start_and_no_shots() -> None:
    result = build_hole_directions(
        _hole(pin_position_lat=0.001, pin_position_lon=0.001), pl.DataFrame([_shot(shot_number=2)])
    )[0]
    assert result["bearing_deg"] is None
    assert result["shots"][0]["bearing_deg"] == 90
    assert build_hole_directions(_hole(), pl.DataFrame())[0]["bearing_deg"] is None
    assert build_hole_directions(pl.DataFrame(), pl.DataFrame()) == []


def test_round_and_hole_provenance() -> None:
    rows = build_hole_directions(
        pl.DataFrame(),
        pl.DataFrame(
            [
                _shot(),
                _shot(round_id=2, hole_number=1, end_lat=0.001, end_lon=0),
                _shot(round_id=2, hole_number=2, end_lat=-0.001, end_lon=0),
            ]
        ),
    )
    assert [(row["round_id"], row["hole_number"], row["bearing_deg"]) for row in rows] == [
        (1, 1, 90),
        (2, 1, 0),
        (2, 2, 180),
    ]
