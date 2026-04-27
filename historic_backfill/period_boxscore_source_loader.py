"""Period boxscore source loader — parquet-only.

Reads pre-scraped period starters from one or more parquet files and
synthesizes V3-shaped responses for the pbpstats starter fallback chain.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Iterable, Tuple

import pandas as pd

STARTER_LOOKUP_COLUMNS = [
    "game_id",
    "period",
    "resolved",
    "away_team_id",
    "home_team_id",
    *[f"away_player{i}" for i in range(1, 6)],
    *[f"home_player{i}" for i in range(1, 6)],
]


def _build_v3_response_from_starters(
    away_team_id: int,
    away_player_ids: list,
    home_team_id: int,
    home_player_ids: list,
    source_name: str,
) -> Dict[str, Any]:
    """Synthesize a V3-shaped response dict from resolved starter lists."""
    def _team_block(team_id, player_ids):
        return {
            "teamId": int(team_id),
            "players": [
                {"personId": int(pid), "statistics": {"minutes": "0:01"}}
                for pid in player_ids
            ],
        }
    return {
        "periodStarterSource": {"name": source_name},
        "boxScoreTraditional": {
            "awayTeam": _team_block(away_team_id, away_player_ids),
            "homeTeam": _team_block(home_team_id, home_player_ids),
        }
    }


class _ParquetStarterLookup:
    """In-memory lookup from a period_starters parquet file."""

    def __init__(
        self,
        parquet_path: Path,
        allowed_seasons: set[int] | None = None,
        allowed_game_ids: set[str] | None = None,
    ):
        self._source_name = parquet_path.stem.removeprefix("period_starters_")
        self._lookup: Dict[Tuple[str, int], Tuple[int, tuple[int, ...], int, tuple[int, ...]]] = {}
        if not parquet_path.exists():
            return

        parquet_read_kwargs: Dict[str, Any] = {"columns": STARTER_LOOKUP_COLUMNS}
        if allowed_game_ids:
            normalized_allowed_ids = {str(game_id).zfill(10) for game_id in allowed_game_ids}
            raw_allowed_ids = {game_id.lstrip("0") or "0" for game_id in normalized_allowed_ids}
            parquet_read_kwargs["filters"] = self._build_game_id_filters(parquet_path, raw_allowed_ids)

        df = pd.read_parquet(parquet_path, **parquet_read_kwargs)
        if "resolved" in df.columns:
            df = df[df["resolved"] == True]  # noqa: E712

        if df.empty:
            return

        normalized_game_ids = df["game_id"].astype(str).str.zfill(10)
        if allowed_game_ids:
            df = df[normalized_game_ids.isin(allowed_game_ids)].copy()
            normalized_game_ids = normalized_game_ids.loc[df.index]
        elif allowed_seasons:
            yy = normalized_game_ids.str[3:5].astype(int)
            seasons = pd.Series(
                yy.where(yy >= 50, yy + 100) + 1901,
                index=df.index,
            )
            df = df[seasons.isin(allowed_seasons)].copy()
            normalized_game_ids = normalized_game_ids.loc[df.index]

        if df.empty:
            return

        df = df.copy()
        df["game_id"] = normalized_game_ids

        for row in df.itertuples(index=False):
            away_ids = tuple(
                int(getattr(row, f"away_player{i}"))
                for i in range(1, 6)
                if pd.notna(getattr(row, f"away_player{i}", None))
            )
            home_ids = tuple(
                int(getattr(row, f"home_player{i}"))
                for i in range(1, 6)
                if pd.notna(getattr(row, f"home_player{i}", None))
            )
            if len(away_ids) == 5 and len(home_ids) == 5:
                self._lookup[(str(row.game_id), int(row.period))] = (
                    int(row.away_team_id),
                    away_ids,
                    int(row.home_team_id),
                    home_ids,
                )

    @staticmethod
    def _build_game_id_filters(parquet_path: Path, raw_allowed_ids: set[str]) -> list[tuple[str, str, list[Any]]]:
        try:
            import pyarrow.parquet as pq
        except ImportError:
            return [("game_id", "in", sorted(raw_allowed_ids))]

        game_id_type = pq.read_schema(parquet_path).field("game_id").type
        if getattr(game_id_type, "id", None) in {"int32", "int64"} or "int" in str(game_id_type):
            return [("game_id", "in", sorted({int(game_id) for game_id in raw_allowed_ids}))]
        return [("game_id", "in", sorted(raw_allowed_ids))]

    def get(self, game_id: str, period: int) -> Dict[str, Any] | None:
        starter_data = self._lookup.get((str(game_id).zfill(10), int(period)))
        if starter_data is None:
            return None
        away_team_id, away_ids, home_team_id, home_ids = starter_data
        return _build_v3_response_from_starters(
            away_team_id,
            list(away_ids),
            home_team_id,
            list(home_ids),
            self._source_name,
        )

    def __len__(self) -> int:
        return len(self._lookup)


class PeriodBoxscoreSourceLoader:
    """Parquet-backed period starter loader.

    Reads from one or more pre-scraped parquet files in precedence order.
    No live API calls, no DB cache. Returns None for any (game_id, period)
    not present in any resolved parquet row.
    """

    def __init__(
        self,
        parquet_path: Path | None = None,
        parquet_paths: Iterable[Path] | None = None,
        allowed_seasons: Iterable[int] | None = None,
        allowed_game_ids: Iterable[str | int] | None = None,
    ):
        ordered_paths: list[Path] = []
        seen_paths: set[Path] = set()
        normalized_game_ids = (
            {str(int(game_id)).zfill(10) for game_id in allowed_game_ids}
            if allowed_game_ids is not None
            else None
        )
        normalized_seasons = (
            {int(season) for season in allowed_seasons}
            if allowed_seasons is not None
            else None
        )

        raw_paths: list[Path] = []
        if parquet_paths is not None:
            raw_paths.extend(Path(path) for path in parquet_paths)
        elif parquet_path is not None:
            raw_paths.append(Path(parquet_path))

        for raw_path in raw_paths:
            path = Path(raw_path)
            if path in seen_paths:
                continue
            seen_paths.add(path)
            if path.exists():
                ordered_paths.append(path)

        self._parquet_lookups = [
            _ParquetStarterLookup(
                path,
                allowed_seasons=normalized_seasons,
                allowed_game_ids=normalized_game_ids,
            )
            for path in ordered_paths
        ]

    def load_data(self, game_id: str, period: int, mode: str) -> Dict[str, Any] | None:
        if mode != "rt2_start_window":
            return None
        for lookup in self._parquet_lookups:
            payload = lookup.get(game_id, period)
            if payload is not None:
                return payload
        return None
