"""User-facing API for nba_stats_parser."""

from __future__ import annotations

import os
import pandas as pd

from . import fetcher, parser, outputs


class Game:
    """Represents a single NBA game."""

    def __init__(self, game_id: str, cache_dir: str | None = None):
        self.game_id = game_id
        self.cache_dir = cache_dir
        self._possessions = None
        self._raw_boxscore = None
        self._raw_pbp = None
        self._raw_shot_charts = None

    def _load_data(self) -> None:
        if self._raw_pbp is None:
            self._raw_pbp = fetcher.fetch_pbp(self.game_id, self.cache_dir)
        if self._raw_boxscore is None:
            self._raw_boxscore = fetcher.fetch_boxscore(self.game_id, self.cache_dir)
        if self._raw_shot_charts is None:
            # pull team ids from boxscore once available
            if self._raw_boxscore is None:
                self._raw_boxscore = fetcher.fetch_boxscore(self.game_id, self.cache_dir)
            team_ids = [row[1] for row in self._raw_boxscore["TeamStats"][:2]]
            self._raw_shot_charts = {
                tid: fetcher.fetch_shot_chart(self.game_id, tid, self.cache_dir)
                for tid in team_ids
            }

    @property
    def possessions(self):
        if self._possessions is None:
            self._load_data()
            self._possessions = parser.parse_game_data(
                self.game_id,
                self._raw_pbp,
                self._raw_boxscore,
                self._raw_shot_charts,
                os.path.join(os.path.dirname(__file__), "overrides"),
            )
        return self._possessions

    def get_boxscore(self) -> pd.DataFrame:
        if self._raw_boxscore is None:
            self._raw_boxscore = fetcher.fetch_boxscore(self.game_id, self.cache_dir)
        return outputs.generate_boxscore(self._raw_boxscore)

    def get_rapm_data(self) -> pd.DataFrame:
        return outputs.generate_rapm_data(self.possessions)

__all__ = ["Game"]
