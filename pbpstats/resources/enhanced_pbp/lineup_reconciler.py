from __future__ import annotations

from collections import defaultdict
from typing import Any, Dict, List, Optional, Sequence, Tuple

import requests

from pbpstats.net.stats_lineups import get_rotation_rows
from pbpstats.resources.enhanced_pbp import StartOfPeriod


class StatsLineupReconciler:
    """
    Builds on-court lineups from stats.nba.com rotation data and injects them
    into EnhancedPbpItem objects when enabled.
    """

    _EPS = 0.05

    def __init__(
        self,
        game_id: str,
        *,
        season: Optional[str],
        season_type: Optional[str],
        league_id: str,
        session: Optional[requests.Session] = None,
    ) -> None:
        self.game_id = game_id
        self.season = season
        self.season_type = season_type
        self.league_id = league_id
        self.session = session or requests.Session()
        self._team_segments: Dict[int, Dict[int, List[Tuple[float, float, List[int]]]]] = {}
        self._team_ids: List[int] = []
        self._period_boundaries: List[Tuple[int, float, float]] = []
        self._regulation_length = 720.0
        if league_id == "10":  # WNBA
            self._regulation_length = 600.0
        self._overtime_length = 300.0

    def apply(self, events: Sequence[Any]) -> None:
        if not events:
            return

        if not self._team_segments:
            self._build_segments()
        if not self._team_segments:
            return

        for event in events:
            period = getattr(event, "period", None)
            if period is None:
                continue
            seconds_remaining = getattr(event, "seconds_remaining", None)
            if seconds_remaining is None:
                continue
            elapsed = self._elapsed_in_period(period, seconds_remaining)
            lineups = {}
            for team_id in self._team_ids:
                players = self._lookup_team_lineup(team_id, period, elapsed)
                if players is None:
                    lineups = {}
                    break
                lineups[team_id] = players
            if len(lineups) != len(self._team_ids):
                continue
            event._current_players_override = lineups
            if isinstance(event, StartOfPeriod):
                event.period_starters = lineups

    # ------------------------------------------------------------------ #
    # Internal helpers
    # ------------------------------------------------------------------ #

    def _build_segments(self) -> None:
        rotation_rows = get_rotation_rows(
            self.game_id,
            season=self.season,
            season_type=self.season_type,
            league_id=self.league_id,
            session=self.session,
        )
        if not rotation_rows:
            return

        stints_by_team: Dict[int, List[Dict[str, float]]] = defaultdict(list)
        max_end_time = 0.0

        for row in rotation_rows:
            try:
                team_id = int(row["TEAM_ID"])
                player_id = int(row["PERSON_ID"])
            except (KeyError, TypeError, ValueError):
                continue
            start = float(row.get("IN_TIME_REAL", 0.0)) / 10.0
            end = float(row.get("OUT_TIME_REAL", 0.0)) / 10.0
            if end <= start + self._EPS:
                continue
            stints_by_team[team_id].append(
                {"player_id": player_id, "start": start, "end": end}
            )
            max_end_time = max(max_end_time, end)

        if not stints_by_team:
            return

        self._team_ids = sorted(stints_by_team.keys())
        self._period_boundaries = self._build_period_boundaries(max_end_time)

        for team_id, stints in stints_by_team.items():
            self._team_segments[team_id] = self._build_team_segments(stints)

    def _build_period_boundaries(self, max_end_time: float) -> List[Tuple[int, float, float]]:
        boundaries: List[Tuple[int, float, float]] = []
        start = 0.0
        period = 1
        while start < max_end_time + self._EPS:
            length = (
                self._regulation_length if period <= 4 else self._overtime_length
            )
            end = start + length
            boundaries.append((period, start, end))
            start = end
            period += 1
        return boundaries

    def _build_team_segments(
        self, stints: Sequence[Dict[str, float]]
    ) -> Dict[int, List[Tuple[float, float, List[int]]]]:
        boundaries = sorted(
            {
                value
                for stint in stints
                for value in (stint["start"], stint["end"])
                if value is not None
            }
        )
        segments_by_period: Dict[int, List[Tuple[float, float, List[int]]]] = defaultdict(
            list
        )

        for idx in range(len(boundaries) - 1):
            start = boundaries[idx]
            end = boundaries[idx + 1]
            if end <= start + self._EPS:
                continue
            midpoint = (start + end) / 2.0
            players = [
                int(stint["player_id"])
                for stint in stints
                if (stint["start"] - self._EPS) <= midpoint < (stint["end"] - self._EPS)
            ]
            if len(players) != 5:
                continue
            pieces = self._split_interval_by_period(start, end)
            for period, piece_start, piece_end, period_start in pieces:
                start_elapsed = piece_start - period_start
                end_elapsed = piece_end - period_start
                if end_elapsed <= start_elapsed + self._EPS:
                    continue
                segments_by_period[period].append((start_elapsed, end_elapsed, players))

        for period_segments in segments_by_period.values():
            period_segments.sort(key=lambda item: item[0])

        return segments_by_period

    def _split_interval_by_period(
        self, start: float, end: float
    ) -> List[Tuple[int, float, float, float]]:
        pieces: List[Tuple[int, float, float, float]] = []
        current_start = start
        while current_start < end - self._EPS:
            period, period_start, period_end = self._period_for_time(current_start)
            piece_end = min(end, period_end)
            pieces.append((period, current_start, piece_end, period_start))
            current_start = piece_end
        return pieces

    def _period_for_time(self, game_time: float) -> Tuple[int, float, float]:
        if not self._period_boundaries:
            raise ValueError("Period boundaries have not been initialized.")
        for period, start, end in self._period_boundaries:
            # Allow timestamps equal to the end of a period to count towards that period.
            if start - self._EPS <= game_time < end + self._EPS:
                if game_time >= end:
                    next_length = (
                        self._overtime_length if period >= 4 else self._regulation_length
                    )
                    next_period = period + 1
                    next_start = end
                    next_end = end + next_length
                    self._period_boundaries.append((next_period, next_start, next_end))
                    return next_period, next_start, next_end
                return period, start, end
        # If timestamp exceeds known boundaries, treat it as overtime extension.
        last_period, last_start, last_end = self._period_boundaries[-1]
        length = self._overtime_length
        while game_time >= last_end:
            last_period += 1
            last_start = last_end
            last_end = last_start + length
            self._period_boundaries.append((last_period, last_start, last_end))
        return last_period, last_start, last_end

    def _lookup_team_lineup(
        self, team_id: int, period: int, elapsed: float
    ) -> Optional[List[int]]:
        segments = self._team_segments.get(team_id, {}).get(period)
        if not segments:
            return None
        for start, end, players in segments:
            if start - self._EPS <= elapsed < end + self._EPS:
                return players
        return None

    def _elapsed_in_period(self, period: int, seconds_remaining: float) -> float:
        length = self._regulation_length if period <= 4 else self._overtime_length
        elapsed = length - float(seconds_remaining)
        # Clamp to valid range to avoid falling outside lookup windows.
        if elapsed < 0.0:
            elapsed = 0.0
        if elapsed > length:
            elapsed = length - self._EPS
        return elapsed
