from garmin_golf.normalize import (
    normalize_holes,
    normalize_round,
    normalize_round_from_activity,
    normalize_shots,
)


def test_normalize_round_and_holes() -> None:
    summary = {"id": 42, "courseName": "Blue Hills", "totalScore": 84}
    detail = {
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
                        },
                        {
                            "number": 2,
                            "par": 3,
                            "strokes": 3,
                            "putts": 1,
                            "fairwayHit": False,
                            "greenInRegulation": True,
                            "penalties": 0,
                        },
                    ],
                }
            }
        ]
    }

    round_row = normalize_round(summary, detail)
    hole_rows = normalize_holes(42, detail)

    assert round_row["round_id"] == 42
    assert round_row["played_on"] == "2025-06-01"
    assert round_row["course_name"] == "Blue Hills"
    assert len(hole_rows) == 2
    assert hole_rows[0]["strokes"] == 5
    assert hole_rows[1]["gir"] is True


def test_normalize_shots() -> None:
    payload = {
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
                        "startLocation": {"lat": 1.0, "lon": 2.0},
                        "endLocation": {"lat": 3.0, "lon": 4.0},
                    }
                ],
            }
        ]
    }

    rows = normalize_shots(42, 1, payload)

    assert len(rows) == 1
    assert rows[0]["club"] == "Driver"
    assert rows[0]["distance_meters"] == 211.5


def test_normalize_round_from_activity() -> None:
    summary = {
        "activityId": 123,
        "activityName": "Chateaufort Golf",
        "startTimeLocal": "2026-03-05 08:21:52",
        "distance": 8729.11,
        "duration": 13786.044,
    }
    detail = {
        "activityId": 123,
        "activityName": "Chateaufort Golf",
        "locationName": "Chateaufort",
        "userProfileId": 99,
        "summaryDTO": {
            "startTimeLocal": "2026-03-05T08:21:52.0",
            "distance": 8729.11,
            "duration": 13786.044,
            "movingDuration": 7325.0,
            "elapsedDuration": 13786.044,
            "calories": 1153,
            "averageHR": 113,
            "maxHR": 142,
        },
        "metadataDTO": {
            "deviceMetaDataDTO": {"deviceId": "3363222668"},
        },
    }

    round_row = normalize_round_from_activity(summary, detail)

    assert round_row["round_id"] == 123
    assert round_row["played_on"] == "2026-03-05"
    assert round_row["data_source"] == "activities"
    assert round_row["location_name"] == "Chateaufort"
