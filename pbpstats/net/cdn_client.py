# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import Any, Dict

import requests

CDN_PBP_URL = (
    "https://cdn.nba.com/static/json/liveData/playbyplay/playbyplay_{game_id}.json"
)


def get_pbp_actions(
    game_id: str, session: requests.Session | None = None
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
    resp = session.get(url, headers=headers, timeout=(5, 15))
    resp.raise_for_status()
    data = resp.json()
    game = data.get("game") or {}
    actions = game.get("actions")
    if not isinstance(actions, list):
        raise ValueError("Malformed CDN PBP: 'game.actions' is missing or not a list")
    return data  # caller will read data["game"]["actions"]

