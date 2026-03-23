from __future__ import annotations

from collections.abc import Callable, Sequence
from datetime import date
from typing import Any

from rich.console import Console
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from .config import Settings
from .models import JsonDict

try:
    from garminconnect import (
        Garmin,
        GarminConnectAuthenticationError,
        GarminConnectConnectionError,
        GarminConnectTooManyRequestsError,
    )
except ImportError:  # pragma: no cover - exercised only before dependencies are installed
    Garmin = Any
    GarminConnectAuthenticationError = RuntimeError
    GarminConnectConnectionError = RuntimeError
    GarminConnectTooManyRequestsError = RuntimeError


GolfCall = Callable[..., Any]


class GarminGolfClient:
    def __init__(self, settings: Settings, console: Console | None = None) -> None:
        self.settings = settings
        self.console = console or Console()
        self._client: Garmin | None = None

    def login(self) -> None:
        if not self.settings.garmin_email or not self.settings.garmin_password:
            msg = (
                "Missing Garmin credentials. Set GARMIN_GOLF_GARMIN_EMAIL and "
                "GARMIN_GOLF_GARMIN_PASSWORD."
            )
            raise ValueError(msg)

        self._client = Garmin(self.settings.garmin_email, self.settings.garmin_password)
        token_dir = self.settings.token_dir
        token_dir.mkdir(parents=True, exist_ok=True)
        try:
            self._client.login(str(token_dir))
        except FileNotFoundError:
            self._client.login()
            self._client.garth.dump(str(token_dir))

    @property
    def client(self) -> Garmin:
        if self._client is None:
            msg = "Not authenticated. Run the login flow first."
            raise RuntimeError(msg)
        return self._client

    @retry(
        retry=retry_if_exception_type(
            (GarminConnectConnectionError, GarminConnectTooManyRequestsError)
        ),
        wait=wait_exponential(multiplier=1, min=1, max=30),
        stop=stop_after_attempt(5),
        reraise=True,
    )
    def list_scorecards(self) -> list[JsonDict]:
        payload = self._call_first_supported(
            [
                ("get_golf_summary", (), {}),
                ("get_golf_scorecards", (), {}),
                ("get_golf_scorecard_summary", (), {}),
                ("get_golf_scorecard_summaries", (), {}),
            ]
        )
        if isinstance(payload, dict):
            summaries = payload.get("scorecardSummaries")
            if isinstance(summaries, list):
                return [item for item in summaries if isinstance(item, dict)]
        if isinstance(payload, list):
            return [item for item in payload if isinstance(item, dict)]
        return []

    @retry(
        retry=retry_if_exception_type(
            (GarminConnectConnectionError, GarminConnectTooManyRequestsError)
        ),
        wait=wait_exponential(multiplier=1, min=1, max=30),
        stop=stop_after_attempt(5),
        reraise=True,
    )
    def get_scorecard_detail(self, scorecard_id: int) -> JsonDict:
        payload = self._call_first_supported(
            [
                ("get_golf_scorecard", (scorecard_id,), {}),
                ("get_golf_scorecard_detail", (scorecard_id,), {}),
                ("get_golf_scorecard_details", ((scorecard_id,),), {}),
            ]
        )
        return payload if isinstance(payload, dict) else {"payload": payload}

    @retry(
        retry=retry_if_exception_type(
            (GarminConnectConnectionError, GarminConnectTooManyRequestsError)
        ),
        wait=wait_exponential(multiplier=1, min=1, max=30),
        stop=stop_after_attempt(5),
        reraise=True,
    )
    def get_hole_shots(self, scorecard_id: int, hole_number: int) -> JsonDict:
        payload = self._call_first_supported(
            [
                ("get_golf_shot_data", (scorecard_id, hole_number), {}),
                ("get_golf_shots", (scorecard_id, hole_number), {}),
                ("get_golf_shot_details", (scorecard_id, hole_number), {}),
            ]
        )
        return payload if isinstance(payload, dict) else {"payload": payload}

    def _call_first_supported(
        self,
        candidates: Sequence[tuple[str, tuple[Any, ...], dict[str, Any]]],
    ) -> Any:
        errors: list[str] = []
        for method_name, args, kwargs in candidates:
            method = getattr(self.client, method_name, None)
            if not callable(method):
                continue
            try:
                return method(*args, **kwargs)
            except TypeError as exc:
                errors.append(f"{method_name}: {exc}")
                continue
            except GarminConnectAuthenticationError:
                raise
        msg = "No supported golf method was found on the installed garminconnect client."
        if errors:
            msg = f"{msg} Signature mismatches: {'; '.join(errors)}"
        raise NotImplementedError(msg)

    @retry(
        retry=retry_if_exception_type(
            (GarminConnectConnectionError, GarminConnectTooManyRequestsError)
        ),
        wait=wait_exponential(multiplier=1, min=1, max=30),
        stop=stop_after_attempt(5),
        reraise=True,
    )
    def list_golf_activities(
        self,
        *,
        date_from: date | None = None,
        date_to: date | None = None,
        page_size: int = 100,
        max_pages: int = 20,
    ) -> list[JsonDict]:
        golf_activities: list[JsonDict] = []
        for page in range(max_pages):
            payload = self.client.get_activities(page * page_size, page_size)
            if not isinstance(payload, list) or not payload:
                break

            for activity in payload:
                if not isinstance(activity, dict):
                    continue
                activity_type = activity.get("activityType")
                if not isinstance(activity_type, dict) or activity_type.get("typeKey") != "golf":
                    continue
                played_on = parse_round_date(activity.get("startTimeLocal"))
                if date_to is not None and played_on is not None and played_on > date_to:
                    continue
                if date_from is not None and played_on is not None and played_on < date_from:
                    return golf_activities
                golf_activities.append(activity)
        return golf_activities

    @retry(
        retry=retry_if_exception_type(
            (GarminConnectConnectionError, GarminConnectTooManyRequestsError)
        ),
        wait=wait_exponential(multiplier=1, min=1, max=30),
        stop=stop_after_attempt(5),
        reraise=True,
    )
    def get_activity_detail(self, activity_id: int) -> JsonDict:
        payload = self.client.get_activity(str(activity_id))
        return payload if isinstance(payload, dict) else {"payload": payload}

    @retry(
        retry=retry_if_exception_type(
            (GarminConnectConnectionError, GarminConnectTooManyRequestsError)
        ),
        wait=wait_exponential(multiplier=1, min=1, max=30),
        stop=stop_after_attempt(5),
        reraise=True,
    )
    def download_activity_original(self, activity_id: int) -> bytes:
        payload = self.client.download_activity(
            str(activity_id),
            self.client.ActivityDownloadFormat.ORIGINAL,
        )
        if not isinstance(payload, bytes):
            msg = f"Unexpected download payload type for activity {activity_id}: {type(payload)!r}"
            raise TypeError(msg)
        return payload


def parse_round_date(value: str | None) -> date | None:
    if not value:
        return None
    try:
        return date.fromisoformat(value[:10])
    except ValueError:
        return None
