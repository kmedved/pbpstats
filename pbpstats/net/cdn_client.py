# -*- coding: utf-8 -*-
"""
CDN client for fetching NBA play-by-play data from the liveData endpoint.
"""
from __future__ import annotations
from typing import Any, Dict, List, Optional
import requests

CDN_PBP_URL = "https://cdn.nba.com/static/json/liveData/playbyplay/playbyplay_{game_id}.json"


def get_pbp_actions(
    game_id: str, session: Optional[requests.Session] = None
) -> Dict[str, Any]:
    """
    Fetch CDN liveData PBP for a 10-digit GAME_ID and return the parsed JSON.

    Expected shape: {"meta": {...}, "game": {"gameId": "...", "actions": [ ... ]}}

    :param str game_id: 10-digit NBA game ID
    :param requests.Session session: Optional session for connection pooling
    :returns: Dict containing the full CDN response with game.actions
    :raises requests.HTTPError: on non-200 status codes
    :raises ValueError: if JSON missing required keys
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
        raise ValueError(
            "Malformed CDN PBP: 'game.actions' is missing or not a list"
        )

    return data  # caller will read data["game"]["actions"]
