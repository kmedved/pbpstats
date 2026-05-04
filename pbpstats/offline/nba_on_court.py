from __future__ import annotations

from typing import Callable, Optional

import pandas as pd

import pbpstats
from pbpstats.game_id import normalize_game_id
from pbpstats.offline.processor import FetchPbpV3Fn, get_possessions_from_df
from pbpstats.resources.possessions.possessions import Possessions

NBA_ON_COURT_INSTALL_MESSAGE = (
    "nba-on-court is required for this offline source adapter. "
    "Install it with: pip install git+https://github.com/shufinskiy/nba-on-court.git"
)
SUPPORTED_LEAGUES = {pbpstats.NBA_STRING, pbpstats.WNBA_STRING}


def _resolve_league(league: str) -> str:
    resolved = str(league).lower()
    if resolved not in SUPPORTED_LEAGUES:
        raise ValueError(
            "nba-on-court offline loading supports only "
            f"{sorted(SUPPORTED_LEAGUES)}; got {league!r}"
        )
    return resolved


def _import_load_nba_data() -> Callable:
    try:
        from nba_on_court.nba_on_court import load_nba_data
    except ImportError as exc:
        raise ImportError(NBA_ON_COURT_INSTALL_MESSAGE) from exc
    return load_nba_data


def _normalize_game_id_column(df: pd.DataFrame, league: str) -> pd.DataFrame:
    if "GAME_ID" not in df.columns:
        raise ValueError("nba-on-court PBP data is missing required GAME_ID column")
    result = df.copy()
    result["GAME_ID"] = result["GAME_ID"].map(
        lambda value: normalize_game_id(value, league=league)
    )
    return result


def load_nba_on_court_pbp(
    season: int,
    league: str = pbpstats.NBA_STRING,
    season_type: str = "rg",
    path: Optional[str] = None,
) -> pd.DataFrame:
    """
    Load nba-on-court / nba_data stats play-by-play rows for a season.
    """
    resolved_league = _resolve_league(league)
    load_nba_data = _import_load_nba_data()
    kwargs = {
        "seasons": season,
        "data": "nbastats",
        "seasontype": season_type,
        "league": resolved_league,
        "in_memory": True,
        "use_pandas": True,
    }
    if path is not None:
        kwargs["path"] = path

    game_df = load_nba_data(**kwargs)
    if game_df is None:
        raise ValueError(
            "nba-on-court returned no PBP data for "
            f"league={resolved_league!r}, season={season!r}, season_type={season_type!r}"
        )
    if not isinstance(game_df, pd.DataFrame):
        raise TypeError(
            "nba-on-court returned %s; expected pandas.DataFrame"
            % type(game_df).__name__
        )
    return _normalize_game_id_column(game_df, resolved_league)


def get_possessions_from_nba_on_court(
    season: int,
    game_id: object,
    league: str = pbpstats.NBA_STRING,
    season_type: str = "rg",
    path: Optional[str] = None,
    fetch_pbp_v3_fn: Optional[FetchPbpV3Fn] = None,
    rebound_deletions_list: Optional[list[dict]] = None,
    boxscore_source_loader=None,
    period_boxscore_source_loader=None,
    file_directory: Optional[str] = None,
) -> Possessions:
    """
    Load one nba-on-court / nba_data game and process it into possessions.
    """
    resolved_league = _resolve_league(league)
    normalized_game_id = normalize_game_id(game_id, league=resolved_league)
    season_df = load_nba_on_court_pbp(
        season,
        league=resolved_league,
        season_type=season_type,
        path=path,
    )
    season_df = _normalize_game_id_column(season_df, resolved_league)
    game_df = season_df[season_df["GAME_ID"] == normalized_game_id].copy()
    if game_df.empty:
        raise ValueError(
            "No nba-on-court PBP rows found for "
            f"league={resolved_league!r}, season={season!r}, "
            f"season_type={season_type!r}, game_id={normalized_game_id!r}"
        )

    return get_possessions_from_df(
        game_df,
        fetch_pbp_v3_fn=fetch_pbp_v3_fn,
        rebound_deletions_list=rebound_deletions_list,
        boxscore_source_loader=boxscore_source_loader,
        period_boxscore_source_loader=period_boxscore_source_loader,
        file_directory=file_directory,
        league=resolved_league,
    )
