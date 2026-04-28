from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Iterable, Tuple

import pandas as pd

from historic_backfill.audits.core.minutes_plus_minus import _prepare_darko_df
from pbpstats.offline.row_overrides import normalize_game_id as _normalize_offline_game_id


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_PARQUET_PATH = ROOT / "data" / "playbyplayv2.parq"
DEFAULT_PBP_V3_PATH = ROOT / "data" / "playbyplayv3.parq"
DEFAULT_DB_PATH = ROOT / "data" / "nba_raw.db"
DEFAULT_FILE_DIRECTORY = ROOT / "data"

_GAME_CONTEXT_NAMESPACE_CACHE: Dict[Tuple[Any, ...], Dict[str, Any]] = {}
_GAME_CONTEXT_SEASON_PBP_CACHE: Dict[Tuple[Path, int], pd.DataFrame] = {}


def _normalize_game_id(value: object) -> str:
    return _normalize_offline_game_id(value)


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
    pbp_row_overrides_path: Path | None = None,
    pbp_stat_overrides_path: Path | None = None,
    boxscore_source_overrides_path: Path | None = None,
    period_starter_parquet_paths: Iterable[Path] | None = None,
) -> Tuple[pd.DataFrame, Any, Dict[int, str]]:
    normalized_game_id = _normalize_game_id(game_id)
    season = _season_from_game_id(normalized_game_id)
    db_path = db_path.resolve()
    parquet_path = parquet_path.resolve()
    file_directory = file_directory.resolve()
    resolved_pbp_row_overrides_path = (
        Path(pbp_row_overrides_path).resolve()
        if pbp_row_overrides_path is not None
        else None
    )
    resolved_pbp_stat_overrides_path = (
        Path(pbp_stat_overrides_path).resolve()
        if pbp_stat_overrides_path is not None
        else None
    )
    resolved_boxscore_source_overrides_path = (
        Path(boxscore_source_overrides_path).resolve()
        if boxscore_source_overrides_path is not None
        else None
    )
    resolved_period_starter_parquet_paths = (
        tuple(Path(path).resolve() for path in period_starter_parquet_paths)
        if period_starter_parquet_paths is not None
        else None
    )
    if (resolved_pbp_row_overrides_path is None) != (
        resolved_pbp_stat_overrides_path is None
    ):
        raise ValueError(
            "pbp_row_overrides_path and pbp_stat_overrides_path must be provided together"
        )
    if (
        resolved_boxscore_source_overrides_path is not None
        and not resolved_boxscore_source_overrides_path.exists()
    ):
        raise FileNotFoundError(
            f"Boxscore source overrides path not found: {resolved_boxscore_source_overrides_path}"
        )
    for path in resolved_period_starter_parquet_paths or ():
        if not path.exists():
            raise FileNotFoundError(f"Period starter parquet not found: {path}")

    namespace_key = (
        db_path,
        file_directory,
        season,
        resolved_pbp_row_overrides_path,
        resolved_pbp_stat_overrides_path,
        resolved_boxscore_source_overrides_path,
        resolved_period_starter_parquet_paths,
    )
    namespace = _GAME_CONTEXT_NAMESPACE_CACHE.get(namespace_key)
    if namespace is None:
        from historic_backfill.runners.cautious_rerun import (
            install_runtime_catalog_wrappers,
            install_local_boxscore_wrapper,
            _load_raw_response,
            load_v9b_namespace,
        )
        from historic_backfill.catalogs.boxscore_source_overrides import (
            load_boxscore_source_overrides,
            validate_boxscore_source_overrides,
        )

        namespace = load_v9b_namespace()
        namespace["DB_PATH"] = db_path
        if (
            resolved_pbp_row_overrides_path is not None
            and resolved_pbp_stat_overrides_path is not None
        ):
            install_runtime_catalog_wrappers(
                namespace,
                pbp_row_overrides_path=resolved_pbp_row_overrides_path,
                pbp_stat_overrides_path=resolved_pbp_stat_overrides_path,
            )
        boxscore_source_overrides = (
            load_boxscore_source_overrides(resolved_boxscore_source_overrides_path)
            if resolved_boxscore_source_overrides_path is not None
            else None
        )
        if resolved_boxscore_source_overrides_path is not None:
            validate_boxscore_source_overrides(resolved_boxscore_source_overrides_path)
        if boxscore_source_overrides is not None:
            namespace["_boxscore_source_overrides"] = boxscore_source_overrides
        original_load_response = namespace.get("load_response")
        original_apply_boxscore_response_overrides = namespace.get(
            "apply_boxscore_response_overrides"
        )

        if original_apply_boxscore_response_overrides is not None:

            def apply_boxscore_response_overrides_snapshot(
                response_game_id: str | int,
                data: Any,
                overrides: Any | None = None,
            ) -> Any:
                return original_apply_boxscore_response_overrides(
                    response_game_id,
                    data,
                    overrides=(
                        boxscore_source_overrides
                        if overrides is None
                        else overrides
                    ),
                )

            namespace["apply_boxscore_response_overrides"] = (
                apply_boxscore_response_overrides_snapshot
            )

        def load_response_snapshot(
            response_game_id: str | int,
            endpoint: str,
            team_id: int | None = None,
        ) -> Any:
            if team_id is not None and original_load_response is not None:
                return original_load_response(response_game_id, endpoint, team_id=team_id)
            return _load_raw_response(
                db_path,
                _normalize_game_id(response_game_id),
                endpoint,
                boxscore_source_overrides=boxscore_source_overrides,
            )

        namespace["load_response"] = load_response_snapshot
        install_local_boxscore_wrapper(
            namespace,
            db_path,
            file_directory=file_directory,
            allowed_seasons=[season],
            period_starter_parquet_paths=resolved_period_starter_parquet_paths,
            boxscore_source_overrides=boxscore_source_overrides,
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
