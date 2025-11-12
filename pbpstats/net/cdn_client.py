# -*- coding: utf-8 -*-
from __future__ import annotations

import time
from typing import Any, Dict, Optional

import requests

CDN_PBP_URL = (
    "https://cdn.nba.com/static/json/liveData/playbyplay/playbyplay_{game_id}.json"
)
RETRYABLE_STATUS = {429, 500, 502, 503, 504}
MAX_RETRIES = 3
BACKOFF_BASE = 0.5


def _should_retry_http_error(error: requests.HTTPError) -> bool:
    response = error.response
    if response is None or response.status_code is None:
        return False
    status = response.status_code
    return status >= 500 or status in RETRYABLE_STATUS


def _sleep_with_backoff(attempt: int) -> None:
    delay = BACKOFF_BASE * (2 ** attempt)
    time.sleep(delay)


def get_pbp_actions(
    game_id: str, session: Optional[requests.Session] = None
) -> Dict[str, Any]:
    """
    Fetch CDN liveData PBP for a 10-digit GAME_ID and return the parsed JSON.
    Expected shape: {"meta": {...}, "game": {"gameId": "...", "actions": [ ... ]}}
    Raises requests.HTTPError on non-200, ValueError if JSON missing required keys.
    """
    if session is None:
        session = requests.Session()
    url = CDN_PBP_URL.format(game_id=game_id)
    headers = {
        "User-Agent": "pbpstats/cdn-client",
        "Accept-Encoding": "gzip, deflate",
    }
    last_error: Optional[Exception] = None
    for attempt in range(MAX_RETRIES):
        try:
            resp = session.get(url, headers=headers, timeout=(5, 15))
            resp.raise_for_status()
            break
        except requests.HTTPError as exc:
            last_error = exc
            if not _should_retry_http_error(exc) or attempt == MAX_RETRIES - 1:
                raise
        except requests.RequestException as exc:
            last_error = exc
            if attempt == MAX_RETRIES - 1:
                raise
        _sleep_with_backoff(attempt)
    else:
        if last_error is not None:
            raise last_error
        raise requests.HTTPError("Failed to fetch CDN PBP data")
    try:
        data = resp.json()
    except ValueError as exc:
        raise ValueError("Malformed CDN PBP: bad JSON") from exc
    game = data.get("game") or {}
    actions = game.get("actions")
    if not isinstance(actions, list):
        raise ValueError("Malformed CDN PBP: 'game.actions' is missing or not a list")
    return data  # caller will read data["game"]["actions"]
