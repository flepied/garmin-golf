from __future__ import annotations

import json
import time
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import quote, urlparse
from urllib.request import Request, urlopen

from rich.console import Console

from .browser_import import import_browser_export_payload
from .models import JsonDict
from .storage import Storage

CONNECT_URL = "https://connect.garmin.com"
SUMMARY_URL_TEMPLATE = (
    f"{CONNECT_URL}/golf-api/gcs-golfcommunity/api/v2/scorecard/summary"
    "?per-page=10000&user-locale={locale}"
)
DETAIL_URL_TEMPLATE = (
    f"{CONNECT_URL}/golf-api/gcs-golfcommunity/api/v2/scorecard/detail"
    "?scorecard-ids={scorecard_id}&include-next-previous-ids=true"
    "&user-locale={locale}&include-longest-shot-distance=true"
)
SHOT_URL_TEMPLATE = (
    f"{CONNECT_URL}/golf-api/gcs-golfcommunity/api/v2/shot/scorecard/{{scorecard_id}}/hole"
    "?image-size=IMG_730X730"
)


@dataclass(slots=True)
class MirrorRunResult:
    discovered: int = 0
    exported: int = 0
    skipped: int = 0
    rounds_imported: int = 0
    holes_imported: int = 0
    shots_imported: int = 0


@dataclass(slots=True)
class MirrorManifestEntry:
    scorecard_id: int
    export_filename: str
    mirrored_at: str


class BrowserMirrorError(RuntimeError):
    pass


class ChromeDebuggerSession:
    def __init__(self, debugger_address: str, console: Console | None = None) -> None:
        self.debugger_address = debugger_address
        self.console = console or Console()
        self._page_id: str | None = None
        self._ws: Any = None
        self._next_id = 0

    def __enter__(self) -> ChromeDebuggerSession:
        target = self._http_json(
            f"/json/new?{quote('about:blank', safe=':/?=&')}",
            method="PUT",
        )
        if not isinstance(target, dict):
            raise BrowserMirrorError("Chrome debugger did not return a page target.")
        page_id = target.get("id")
        ws_url = target.get("webSocketDebuggerUrl")
        if not isinstance(page_id, str) or not isinstance(ws_url, str):
            raise BrowserMirrorError("Chrome debugger target is missing id or websocket URL.")
        self._page_id = page_id
        self._ws = self._connect(ws_url)
        self._send("Runtime.enable")
        self._send("Page.enable")
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        try:
            if self._ws is not None:
                self._ws.close()
        finally:
            self._ws = None
            if self._page_id is not None:
                with suppress(Exception):
                    self._http_json(f"/json/close/{self._page_id}")
                self._page_id = None

    def navigate(self, url: str) -> None:
        self._send("Page.navigate", {"url": url})

    def evaluate(self, expression: str) -> Any:
        response = self._send(
            "Runtime.evaluate",
            {
                "expression": expression,
                "awaitPromise": True,
                "returnByValue": True,
            },
        )
        result = response.get("result", {}).get("result", {})
        if "value" in result:
            return result["value"]
        if result.get("type") == "undefined":
            return None
        raise BrowserMirrorError(f"Chrome debugger evaluation failed: {result!r}")

    def _connect(self, ws_url: str) -> Any:
        try:
            import websocket
        except ImportError as exc:  # pragma: no cover - depends on runtime env
            raise BrowserMirrorError("websocket-client is not installed.") from exc
        return websocket.create_connection(ws_url, timeout=30, suppress_origin=True)

    def _send(self, method: str, params: dict[str, Any] | None = None) -> JsonDict:
        if self._ws is None:
            raise BrowserMirrorError("Chrome debugger websocket is not connected.")
        self._next_id += 1
        message = {"id": self._next_id, "method": method}
        if params:
            message["params"] = params
        self._ws.send(json.dumps(message))
        while True:
            raw = self._ws.recv()
            payload = json.loads(raw)
            if payload.get("id") != self._next_id:
                continue
            if "error" in payload:
                raise BrowserMirrorError(
                    f"Chrome debugger command {method} failed: {payload['error']!r}"
                )
            return payload

    def _http_json(self, path: str, method: str = "GET") -> Any:
        url = f"http://{self.debugger_address}{path}"
        request = Request(url, method=method)
        with urlopen(request, timeout=30) as response:
            return json.loads(response.read().decode("utf-8"))


class BrowserMirror:
    def __init__(
        self,
        timeout_seconds: int = 300,
        *,
        garmin_email: str | None = None,
        garmin_password: str | None = None,
        debugger_address: str | None = None,
        console: Console | None = None,
    ) -> None:
        self.timeout_seconds = timeout_seconds
        self.garmin_email = garmin_email
        self.garmin_password = garmin_password
        self.debugger_address = debugger_address
        self.console = console or Console()
        if not debugger_address:
            raise BrowserMirrorError(
                "Browser mirroring now requires --debugger-address attached to a logged-in Chrome."
            )

    def mirror(
        self,
        listing_url: str,
        *,
        storage: Storage,
        output_dir: Path,
        force: bool = False,
    ) -> MirrorRunResult:
        validate_scorecards_url(listing_url)
        manifest_path = output_dir / "index.json"
        manifest = load_manifest(manifest_path)
        output_dir.mkdir(parents=True, exist_ok=True)
        return self._mirror_via_debugger(
            listing_url,
            storage=storage,
            output_dir=output_dir,
            manifest=manifest,
            manifest_path=manifest_path,
            force=force,
        )

    def _mirror_via_debugger(
        self,
        listing_url: str,
        *,
        storage: Storage,
        output_dir: Path,
        manifest: JsonDict,
        manifest_path: Path,
        force: bool,
    ) -> MirrorRunResult:
        self.console.print(
            f"[cyan]Attaching to Chrome debugger at[/cyan] {self.debugger_address}"
        )
        with ChromeDebuggerSession(self.debugger_address or "", console=self.console) as session:
            self.console.print(
                "[cyan]Opening Garmin scorecards in a debugger-controlled tab.[/cyan]"
            )
            session.navigate(listing_url)
            self._wait_for_authenticated_listing(session, listing_url)
            locale = self._get_locale(session)
            csrf_token = self._find_csrf_token(session)
            summary_payload = self._fetch_json(
                session,
                SUMMARY_URL_TEMPLATE.format(locale=locale),
                csrf_token,
            )
            summaries = summary_payload.get("scorecardSummaries")
            if not isinstance(summaries, list):
                raise BrowserMirrorError("Garmin summary payload is missing scorecardSummaries.")

            result = MirrorRunResult(discovered=len(summaries))
            for summary_row in summaries:
                if not isinstance(summary_row, dict):
                    continue
                scorecard_id = summary_row.get("id")
                if not isinstance(scorecard_id, int):
                    continue
                export_filename = f"{scorecard_id}.json"
                export_path = output_dir / export_filename
                if not force and should_skip_scorecard(scorecard_id, manifest, export_path):
                    result.skipped += 1
                    continue

                detail_payload = self._fetch_json(
                    session,
                    DETAIL_URL_TEMPLATE.format(scorecard_id=scorecard_id, locale=locale),
                    csrf_token,
                )
                shot_payload = self._fetch_json(
                    session,
                    SHOT_URL_TEMPLATE.format(scorecard_id=scorecard_id),
                    csrf_token,
                )
                export_payload = build_browser_export_payload(
                    summary_payload=summary_payload,
                    summary_row=summary_row,
                    detail_payload=detail_payload,
                    shot_payload=shot_payload,
                    source="garmin-connect-browser",
                )
                export_path.write_text(
                    json.dumps(export_payload, indent=2, sort_keys=True),
                    encoding="utf-8",
                )
                import_result = import_browser_export_payload(storage, export_payload)
                result.exported += 1
                result.rounds_imported += import_result.rounds_imported
                result.holes_imported += import_result.holes_imported
                result.shots_imported += import_result.shots_imported
                record_manifest_entry(
                    manifest,
                    output_dir,
                    MirrorManifestEntry(
                        scorecard_id=scorecard_id,
                        export_filename=export_filename,
                        mirrored_at=export_payload["exportedAt"],
                    ),
                )

            save_manifest(manifest_path, manifest)
            return result

    def _wait_for_authenticated_listing(
        self,
        session: ChromeDebuggerSession,
        listing_url: str,
    ) -> None:
        expected = urlparse(listing_url)
        deadline = time.monotonic() + self.timeout_seconds
        while time.monotonic() < deadline:
            state = session.evaluate(
                """
                (() => ({
                  url: window.location.href,
                  bodyText: document.body ? document.body.innerText : ''
                }))()
                """
            )
            if isinstance(state, dict):
                parsed = urlparse(str(state.get("url", "")))
                body_text = str(state.get("bodyText", ""))
                if (
                    parsed.scheme == expected.scheme
                    and parsed.netloc == expected.netloc
                    and parsed.path == expected.path
                    and "Vérification de sécurité en cours" not in body_text
                ):
                    return
            time.sleep(1)
        raise BrowserMirrorError(
            f"Timed out after {self.timeout_seconds}s waiting for "
            "authenticated Garmin scorecards page."
        )

    def _get_locale(self, session: ChromeDebuggerSession) -> str:
        locale = session.evaluate("((navigator.language || 'en').split('-')[0] || 'en')")
        if isinstance(locale, str) and locale:
            return locale
        return "en"

    def _find_csrf_token(self, session: ChromeDebuggerSession) -> str:
        token = session.evaluate(
            """
            (() => {
              function tryParseJson(value) {
                if (typeof value !== "string" || !value) return null;
                try { return JSON.parse(value); } catch { return null; }
              }
              function findTokenInObject(value) {
                if (!value || typeof value !== "object") return null;
                for (const [key, nested] of Object.entries(value)) {
                  if (typeof nested === "string" && key.toLowerCase().includes("csrf") && nested) {
                    return nested;
                  }
                  if (nested && typeof nested === "object") {
                    const token = findTokenInObject(nested);
                    if (token) return token;
                  }
                }
                return null;
              }
              const meta = document.querySelector(
                'meta[name="connect-csrf-token"], meta[name="csrf-token"]'
              );
              if (meta && meta.content) return meta.content;
              for (const storage of [window.localStorage, window.sessionStorage]) {
                try {
                  for (let index = 0; index < storage.length; index += 1) {
                    const key = storage.key(index);
                    if (!key) continue;
                    const value = storage.getItem(key);
                    if (!value) continue;
                    if (key.toLowerCase().includes("csrf")) return value;
                    const parsed = tryParseJson(value);
                    const token = findTokenInObject(parsed);
                    if (token) return token;
                  }
                } catch {}
              }
              return null;
            })()
            """
        )
        if isinstance(token, str) and token:
            return token
        raise BrowserMirrorError("Could not discover a Garmin CSRF token after login.")

    def _fetch_json(
        self,
        session: ChromeDebuggerSession,
        url: str,
        csrf_token: str,
    ) -> JsonDict:
        expression = f"""
        (async () => {{
          const res = await fetch({json.dumps(url)}, {{
            method: "GET",
            credentials: "include",
            headers: {{
              accept: "*/*",
              "connect-csrf-token": {json.dumps(csrf_token)}
            }}
          }});
          const text = await res.text();
          let jsonPayload = null;
          try {{
            jsonPayload = JSON.parse(text);
          }} catch {{}}
          return {{
            ok: res.ok,
            status: res.status,
            json: jsonPayload,
            text
          }};
        }})()
        """
        response = session.evaluate(expression)
        if not isinstance(response, dict):
            raise BrowserMirrorError(f"Unexpected fetch result for {url}.")
        if response.get("ok") is not True or not isinstance(response.get("json"), dict):
            status = response.get("status")
            raise BrowserMirrorError(f"Fetch failed for {url}: HTTP {status}")
        return response["json"]


def validate_scorecards_url(url: str) -> str:
    parsed = urlparse(url)
    if parsed.scheme != "https" or parsed.netloc != "connect.garmin.com":
        raise ValueError("Scorecards URL must use https://connect.garmin.com/...")
    path_parts = [part for part in parsed.path.split("/") if part]
    if len(path_parts) != 3 or path_parts[:2] != ["app", "scorecards"] or not path_parts[2]:
        raise ValueError(
            "Scorecards URL must look like https://connect.garmin.com/app/scorecards/<username>."
        )
    return url


def build_browser_export_payload(
    *,
    summary_payload: JsonDict,
    summary_row: JsonDict,
    detail_payload: JsonDict,
    shot_payload: JsonDict,
    source: str,
) -> JsonDict:
    scorecard_id = summary_row.get("id")
    if not isinstance(scorecard_id, int):
        raise ValueError("Summary row is missing an integer scorecard id.")
    return {
        "exportedAt": datetime.now(UTC).isoformat(),
        "source": source,
        "summary": {
            **summary_payload,
            "scorecardSummaries": [summary_row],
        },
        "details": [detail_payload],
        "shots": [
            {
                "scorecardId": scorecard_id,
                "payload": shot_payload,
            }
        ],
    }


def load_manifest(path: Path) -> JsonDict:
    if not path.exists():
        return {"scorecards": {}}
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        return {"scorecards": {}}
    scorecards = payload.get("scorecards")
    if not isinstance(scorecards, dict):
        payload["scorecards"] = {}
    return payload


def save_manifest(path: Path, manifest: JsonDict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")


def should_skip_scorecard(scorecard_id: int, manifest: JsonDict, export_path: Path) -> bool:
    scorecards = manifest.get("scorecards")
    if not isinstance(scorecards, dict):
        return False
    return str(scorecard_id) in scorecards and export_path.exists()


def record_manifest_entry(manifest: JsonDict, output_dir: Path, entry: MirrorManifestEntry) -> None:
    scorecards = manifest.setdefault("scorecards", {})
    if not isinstance(scorecards, dict):
        manifest["scorecards"] = {}
        scorecards = manifest["scorecards"]
    scorecards[str(entry.scorecard_id)] = {
        "export_filename": entry.export_filename,
        "mirrored_at": entry.mirrored_at,
        "path": str((output_dir / entry.export_filename).name),
    }
