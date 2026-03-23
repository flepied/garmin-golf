from __future__ import annotations

BROWSER_EXPORT_SCRIPT = r"""
(function gcExportGolfScores() {
  const connectURL = "https://connect.garmin.com";
  if (!window.location.href.startsWith(connectURL)) {
    alert("Open this from a logged-in Garmin Connect page.");
    return;
  }

  const scorecardPageMatch = window.location.pathname.match(/\/app\/scorecard\/(\d+)/);
  const scorecardIdFromPage = scorecardPageMatch ? Number(scorecardPageMatch[1]) : null;
  const locale = (navigator.language || "en").split("-")[0] || "en";
  const summaryUrl =
    `${connectURL}/golf-api/gcs-golfcommunity/api/v2/scorecard/summary?per-page=10000&user-locale=${encodeURIComponent(locale)}`;

  function tryParseJson(value) {
    if (typeof value !== "string" || !value) {
      return null;
    }
    try {
      return JSON.parse(value);
    } catch {
      return null;
    }
  }

  function findTokenInObject(value) {
    if (!value || typeof value !== "object") {
      return null;
    }
    for (const [key, nested] of Object.entries(value)) {
      if (typeof nested === "string" && key.toLowerCase().includes("csrf") && nested) {
        return nested;
      }
      if (nested && typeof nested === "object") {
        const token = findTokenInObject(nested);
        if (token) {
          return token;
        }
      }
    }
    return null;
  }

  function findCsrfToken() {
    const meta = document.querySelector('meta[name="connect-csrf-token"], meta[name="csrf-token"]');
    if (meta && meta.content) {
      return meta.content;
    }

    for (const storage of [window.localStorage, window.sessionStorage]) {
      try {
        for (let index = 0; index < storage.length; index += 1) {
          const key = storage.key(index);
          if (!key) {
            continue;
          }
          const value = storage.getItem(key);
          if (!value) {
            continue;
          }
          if (key.toLowerCase().includes("csrf")) {
            return value;
          }
          const parsed = tryParseJson(value);
          const token = findTokenInObject(parsed);
          if (token) {
            return token;
          }
        }
      } catch {}
    }

    const globals = [
      window.__INITIAL_STATE__,
      window.__PRELOADED_STATE__,
      window.INITIAL_STATE,
      window.PRELOADED_STATE
    ];
    for (const candidate of globals) {
      const token = findTokenInObject(candidate);
      if (token) {
        return token;
      }
    }

    return null;
  }

  function downloadJson(filename, data) {
    const blob = new Blob([JSON.stringify(data, null, 2)], { type: "application/json" });
    const url = URL.createObjectURL(blob);
    const anchor = document.createElement("a");
    anchor.href = url;
    anchor.download = filename;
    document.body.appendChild(anchor);
    anchor.click();
    anchor.remove();
    URL.revokeObjectURL(url);
  }

  async function getJson(url, csrfToken) {
    const res = await fetch(url, {
      method: "GET",
      credentials: "include",
      headers: {
        accept: "*/*",
        ...(csrfToken ? { "connect-csrf-token": csrfToken } : {})
      }
    });
    if (!res.ok) {
      throw new Error(`HTTP ${res.status} for ${url}`);
    }
    return await res.json();
  }

  (async function run() {
    const csrfToken =
      findCsrfToken() ||
      window.prompt(
        "Garmin CSRF token required. Paste the connect-csrf-token value from DevTools > Network:",
        ""
      );

    if (!csrfToken) {
      alert("Missing Garmin CSRF token.");
      return;
    }
    if (!scorecardIdFromPage) {
      alert("Open an exact Garmin scorecard page like /app/scorecard/<id>.");
      return;
    }

    const summary = await getJson(summaryUrl, csrfToken);
    const detailUrl =
      `${connectURL}/golf-api/gcs-golfcommunity/api/v2/scorecard/detail` +
      `?scorecard-ids=${encodeURIComponent(scorecardIdFromPage)}` +
      `&include-next-previous-ids=true&user-locale=${encodeURIComponent(locale)}&include-longest-shot-distance=true`;
    const shotUrl =
      `${connectURL}/golf-api/gcs-golfcommunity/api/v2/shot/scorecard/` +
      `${encodeURIComponent(scorecardIdFromPage)}/hole?image-size=IMG_730X730`;

    const detail = await getJson(detailUrl, csrfToken);
    const shotPayload = await getJson(shotUrl, csrfToken);

    downloadJson("garmin-golf-export.json", {
      exportedAt: new Date().toISOString(),
      source: "garmin-connect-browser",
      summary: {
        ...summary,
        scorecardSummaries: ((summary && summary.scorecardSummaries) || []).filter(
          card => card && card.id === scorecardIdFromPage
        )
      },
      details: [detail],
      shots: [
        {
          scorecardId: scorecardIdFromPage,
          payload: shotPayload
        }
      ]
    });
  })().catch(function(error) {
    alert(`Garmin export failed: ${String(error)}`);
  });
})();
""".strip()
