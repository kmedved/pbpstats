"""
Local loaders for offline stats.nba.com shots responses.

These helpers allow callers to plug cached JSON into ``StatsNbaShotsLoader``
without making network requests.
"""
from pathlib import Path
from typing import Optional, Tuple
import json


def load_response(game_id: str, data_type: str, file_directory: Optional[str] = None):
    """
    Load cached response JSON for the given game and data type.

    Looks for ``<file_directory>/raw_responses/<game_id>_<data_type>.json``.
    Returns ``None`` if the file is missing or unreadable.
    """
    base_dir = Path(file_directory) if file_directory else Path(".")
    path = base_dir / "raw_responses" / f"{game_id}_{data_type}.json"
    try:
        with path.open() as f:
            return json.load(f)
    except Exception:
        return None


class LocalShotsJsonLoader:
    """Loader for stats.nba shotchartdetail-style JSON from disk."""

    def __init__(self, file_directory: Optional[str] = None):
        self.file_directory = file_directory

    def load_data(self, game_id: str) -> Tuple[dict, dict]:
        game_id = str(game_id).zfill(10)
        empty = {"resultSets": [{"headers": [], "rowSet": []}]}

        data = load_response(game_id, "shots", self.file_directory)
        if not data:
            # No cached shots: treat as (home empty, away empty)
            return empty, empty

        # If the cached structure already separates home/away, return it.
        if isinstance(data, (list, tuple)) and len(data) == 2:
            home_data = data[0] or empty
            away_data = data[1] or empty
            return home_data, away_data

        # Otherwise treat as combined, assign to "home", leave away empty.
        return data, empty


class LocalShotsJsonLoaderStub:
    """Stub loader for shots - returns empty structure."""

    def __init__(self, file_directory: Optional[str] = None):
        self.file_directory = file_directory

    def load_data(self, game_id: str) -> Tuple[dict, dict]:
        empty = {"resultSets": [{"headers": [], "rowSet": []}]}
        return empty, empty
