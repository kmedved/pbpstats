import json
import os
from typing import Optional

from nba_api.stats.endpoints import PlayByPlayV2, BoxScoreTraditionalV2, ShotChartDetail


def _load_from_cache(path: str) -> Optional[dict]:
    if os.path.exists(path):
        with open(path, 'r') as f:
            return json.load(f)
    return None


def _save_to_cache(path: str, data: dict) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'w') as f:
        json.dump(data, f)


def _cached_request(filename: str, cache_dir: Optional[str], fetch_func):
    if cache_dir is not None:
        cache_path = os.path.join(cache_dir, filename)
        cached = _load_from_cache(cache_path)
        if cached is not None:
            return cached
    else:
        cache_path = None

    data = fetch_func()
    if cache_path is not None:
        _save_to_cache(cache_path, data)
    return data


def fetch_pbp(game_id: str, cache_dir: Optional[str] = None) -> dict:
    """Fetch play-by-play data for a game."""
    return _cached_request(
        f"pbp_{game_id}.json",
        cache_dir,
        lambda: PlayByPlayV2(game_id=game_id).get_normalized_dict(),
    )


def fetch_boxscore(game_id: str, cache_dir: Optional[str] = None) -> dict:
    """Fetch boxscore data for a game."""
    return _cached_request(
        f"boxscore_{game_id}.json",
        cache_dir,
        lambda: BoxScoreTraditionalV2(game_id=game_id).get_normalized_dict(),
    )


def fetch_shot_chart(game_id: str, team_id: int, cache_dir: Optional[str] = None) -> dict:
    """Fetch shot chart data for a team in a game."""
    return _cached_request(
        f"shots_{game_id}_{team_id}.json",
        cache_dir,
        lambda: ShotChartDetail(game_id=game_id, team_id=team_id).get_normalized_dict(),
    )
