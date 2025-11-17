from __future__ import annotations

from typing import Any, Dict, List, Optional, Sequence, Tuple

import requests

from pbpstats import HEADERS, REQUEST_TIMEOUT

GAMEROTATION_URL = "https://stats.nba.com/stats/gamerotation"

# Cache rotation payloads since we only need to hit the network once per game.
_ROTATION_CACHE: Dict[Tuple[str, str, str, str], Dict[str, Any]] = {}


def get_rotation_rows(
    game_id: str,
    *,
    season: Optional[str] = None,
    season_type: Optional[str] = None,
    league_id: str = "00",
    session: Optional[requests.Session] = None,
) -> List[Dict[str, Any]]:
    """
    Fetches the raw rotation rows for a game from stats.nba.com.

    The response includes one row per stint for every player on both teams.
    """
    payload = _get_rotation_payload(
        game_id,
        season=season,
        season_type=season_type,
        league_id=league_id,
        session=session,
    )
    return _extract_rotation_rows(payload)


def get_lineups_by_period(
    game_id: str,
    period: int,
    *,
    season: Optional[str] = None,
    season_type: Optional[str] = None,
    league_id: str = "00",
    session: Optional[requests.Session] = None,
) -> List[Dict[str, Any]]:
    """
    Convenience wrapper that returns only the rotation rows overlapping a period.
    """
    rows = get_rotation_rows(
        game_id,
        season=season,
        season_type=season_type,
        league_id=league_id,
        session=session,
    )
    period_start, period_end = _period_window_bounds(period, league_id)
    filtered: List[Dict[str, Any]] = []
    for row in rows:
        start = float(row.get("IN_TIME_REAL", 0.0)) / 10.0
        end = float(row.get("OUT_TIME_REAL", 0.0)) / 10.0
        if end <= start:
            continue
        # Stint overlaps current period window.
        if start < period_end and end > period_start:
            filtered.append(row)
    return filtered


def _get_rotation_payload(
    game_id: str,
    *,
    season: Optional[str],
    season_type: Optional[str],
    league_id: str,
    session: Optional[requests.Session],
) -> Dict[str, Any]:
    cache_key = (game_id, season or "", season_type or "", league_id)
    if cache_key in _ROTATION_CACHE:
        return _ROTATION_CACHE[cache_key]

    if session is None:
        session = requests.Session()

    params = {"GameID": game_id, "LeagueID": league_id}
    if season:
        params["Season"] = season
    if season_type:
        params["SeasonType"] = season_type

    response = session.get(
        GAMEROTATION_URL, params=params, headers=HEADERS, timeout=REQUEST_TIMEOUT
    )
    response.raise_for_status()
    payload = response.json()
    _ROTATION_CACHE[cache_key] = payload
    return payload


def _extract_rotation_rows(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for result_set in payload.get("resultSets", []):
        if result_set.get("name") not in {"HomeTeam", "AwayTeam"}:
            continue
        headers: Sequence[str] = result_set.get("headers", [])
        for row in result_set.get("rowSet", []):
            rows.append(dict(zip(headers, row)))
    return rows


def _period_window_bounds(period: int, league_id: str) -> Tuple[float, float]:
    """
    Returns the absolute seconds from game start for a period start/end pair.
    """
    regulation_length = 720.0
    if league_id == "10":  # WNBA
        regulation_length = 600.0

    overtime_length = 300.0
    regulation_periods = 4

    if period <= regulation_periods:
        start = regulation_length * (period - 1)
        end = start + regulation_length
    else:
        start = regulation_length * regulation_periods + overtime_length * (
            period - regulation_periods - 1
        )
        end = start + overtime_length
    return start, end
