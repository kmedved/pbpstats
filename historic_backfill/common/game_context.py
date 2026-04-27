from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Tuple

import pandas as pd

from historic_backfill.audits.core.minutes_plus_minus import _prepare_darko_df
from historic_backfill.runners.cautious_rerun import (
    install_local_boxscore_wrapper,
    load_v9b_namespace,
)


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_PARQUET_PATH = ROOT / "data" / "playbyplayv2.parq"
DEFAULT_PBP_V3_PATH = ROOT / "data" / "playbyplayv3.parq"
DEFAULT_DB_PATH = ROOT / "data" / "nba_raw.db"
DEFAULT_FILE_DIRECTORY = ROOT / "data"

_GAME_CONTEXT_NAMESPACE_CACHE: Dict[Tuple[Path, Path, int], Dict[str, Any]] = {}
_GAME_CONTEXT_SEASON_PBP_CACHE: Dict[Tuple[Path, int], pd.DataFrame] = {}


def _normalize_game_id(value: str | int) -> str:
    return str(int(value)).zfill(10)


def _season_from_game_id(game_id: str | int) -> int:
    gid = _normalize_game_id(game_id)
    yy = int(gid[3:5])
    return 1901 + yy if yy >= 50 else 2001 + yy


def _load_current_game_possessions(namespace: dict, game_id: str, parquet_path: Path):
    season = _season_from_game_id(game_id)
    season_df = namespace["load_pbp_from_parquet"](str(parquet_path), season=season)
    single_game_df = season_df[season_df["GAME_ID"] == game_id].copy()
    if single_game_df.empty:
        raise ValueError(f"Game {game_id} not found in parquet")

    df_box = namespace["fetch_boxscore_stats"](game_id)
    summary = namespace["fetch_game_summary"](game_id)
    h_tm_id, v_tm_id = namespace["_resolve_game_team_ids"](summary, df_box)
    if h_tm_id and v_tm_id:
        single_game_df = namespace["normalize_single_game_team_events"](
            single_game_df,
            home_team_id=h_tm_id,
            away_team_id=v_tm_id,
            boxscore_player_ids=df_box["PLAYER_ID"].tolist(),
        )
    single_game_df = namespace["normalize_single_game_player_ids"](
        single_game_df,
        official_boxscore=df_box,
    )
    single_game_df = namespace["apply_pbp_row_overrides"](single_game_df)
    possessions = namespace["get_possessions_from_df"](
        single_game_df,
        fetch_pbp_v3_fn=namespace["fetch_pbp_v3"],
    )
    name_map = {
        int(pid): str(name)
        for pid, name in zip(df_box["PLAYER_ID"].astype(int), df_box["PLAYER_NAME"])
    }
    return possessions, name_map


def _load_game_context(
    game_id: str | int,
    parquet_path: Path,
    db_path: Path,
    file_directory: Path = DEFAULT_FILE_DIRECTORY,
) -> Tuple[pd.DataFrame, Any, Dict[int, str]]:
    normalized_game_id = _normalize_game_id(game_id)
    season = _season_from_game_id(normalized_game_id)
    db_path = db_path.resolve()
    parquet_path = parquet_path.resolve()
    file_directory = file_directory.resolve()

    namespace_key = (db_path, file_directory, season)
    namespace = _GAME_CONTEXT_NAMESPACE_CACHE.get(namespace_key)
    if namespace is None:
        namespace = load_v9b_namespace()
        namespace["DB_PATH"] = db_path
        install_local_boxscore_wrapper(
            namespace,
            db_path,
            file_directory=file_directory,
            allowed_seasons=[season],
        )
        _GAME_CONTEXT_NAMESPACE_CACHE[namespace_key] = namespace

    season_pbp_key = (parquet_path, season)
    season_pbp_df = _GAME_CONTEXT_SEASON_PBP_CACHE.get(season_pbp_key)
    if season_pbp_df is None:
        season_pbp_df = namespace["load_pbp_from_parquet"](
            str(parquet_path),
            season=season,
        )
        _GAME_CONTEXT_SEASON_PBP_CACHE[season_pbp_key] = season_pbp_df

    darko_df, possessions = namespace["generate_darko_hybrid"](
        normalized_game_id,
        season_pbp_df,
    )

    prepared_darko = _prepare_darko_df(darko_df)
    name_map = {
        int(player_id): str(player_name)
        for player_id, player_name in zip(
            prepared_darko["player_id"].astype(int),
            prepared_darko["player_name"],
        )
    }
    return darko_df, possessions, name_map
