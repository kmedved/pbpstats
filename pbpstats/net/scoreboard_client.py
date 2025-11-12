# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import Any, Dict, List, Optional

import requests

SCOREBOARD_URL = (
    "https://cdn.nba.com/static/json/liveData/scoreboard/scoreboard_{yyyymmdd}.json"
)


def get_games_for_date(
    yyyymmdd: str, session: Optional[requests.Session] = None
) -> List[Dict[str, Any]]:
    """
    Return list of game dicts for a given date (YYYYMMDD) from the CDN scoreboard.
    Each dict includes keys like gameId, gameStatus, homeTeam, awayTeam, etc.
    """
    if session is None:
        session = requests.Session()
    headers = {
        "User-Agent": "pbpstats/scoreboard-client",
        "Accept": "application/json, text/plain, */*",
        "Referer": "https://www.nba.com",
    }
    url = SCOREBOARD_URL.format(yyyymmdd=yyyymmdd)
    resp = session.get(url, headers=headers, timeout=(5, 15))
    resp.raise_for_status()
    data = resp.json()
    return (data.get("scoreboard") or {}).get("games", []) or []
