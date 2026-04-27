from __future__ import annotations

from collections.abc import Collection

import pandas as pd


_TEAM_STYLE_EVENT_TYPES = frozenset({4, 5, 6, 7})


def _get_series(df: pd.DataFrame, column: str, dtype: str = "object") -> pd.Series:
    if column in df.columns:
        return df[column]
    return pd.Series(index=df.index, dtype=dtype)


def _blank_mask(series: pd.Series) -> pd.Series:
    return series.fillna("").astype(str).str.strip().eq("")


def _series_compatible_scalar(series: pd.Series, value: int) -> int | str:
    if str(series.dtype).startswith("string"):
        return str(int(value))
    non_missing = series.dropna()
    if not non_missing.empty and non_missing.map(lambda item: isinstance(item, str)).any():
        return str(int(value))
    return int(value)


def _boxscore_player_id_set(boxscore_player_ids: Collection[int] | None) -> set[int]:
    if not boxscore_player_ids:
        return set()

    player_ids = set()
    for player_id in boxscore_player_ids:
        try:
            player_id = int(player_id)
        except (TypeError, ValueError):
            continue
        if player_id != 0:
            player_ids.add(player_id)
    return player_ids


def normalize_single_game_team_events(
    game_df: pd.DataFrame,
    home_team_id: int,
    away_team_id: int,
    boxscore_player_ids: Collection[int] | None = None,
) -> pd.DataFrame:
    """
    Repair malformed team-style rows before pbpstats parses the event stream.

    Historical stats.nba.com rows sometimes encode a team event with:
    - a one-sided home/visitor description
    - EVENTMSGTYPE in {4, 5, 6, 7}
    - PLAYER1_TEAM_ID missing
    - PLAYER1_ID set to a pseudo team code instead of 0

    For those rows, assign the side-implied team id and zero out PLAYER1_ID only
    when the value is not a known boxscore player id.
    """
    if game_df.empty or not home_team_id or not away_team_id:
        return game_df.copy()

    normalized = game_df.copy()

    event_types = pd.to_numeric(_get_series(normalized, "EVENTMSGTYPE"), errors="coerce")
    player1_ids = pd.to_numeric(_get_series(normalized, "PLAYER1_ID"), errors="coerce")
    player1_team_ids = pd.to_numeric(_get_series(normalized, "PLAYER1_TEAM_ID"), errors="coerce")
    player1_team_id_series = _get_series(normalized, "PLAYER1_TEAM_ID")
    player1_id_series = _get_series(normalized, "PLAYER1_ID")

    home_desc = _get_series(normalized, "HOMEDESCRIPTION")
    visitor_desc = _get_series(normalized, "VISITORDESCRIPTION")

    home_only = ~_blank_mask(home_desc) & _blank_mask(visitor_desc)
    visitor_only = _blank_mask(home_desc) & ~_blank_mask(visitor_desc)
    missing_team = player1_team_ids.isna()
    allowed_event = event_types.isin(_TEAM_STYLE_EVENT_TYPES)

    repairable = allowed_event & missing_team & (home_only | visitor_only)
    if not repairable.any():
        return normalized

    real_player_ids = _boxscore_player_id_set(boxscore_player_ids)
    real_player_mask = player1_ids.isin(real_player_ids)
    pseudo_team_code_mask = player1_ids.notna() & (player1_ids != 0) & ~real_player_mask

    home_fix = repairable & home_only
    away_fix = repairable & visitor_only

    normalized.loc[home_fix, "PLAYER1_TEAM_ID"] = _series_compatible_scalar(player1_team_id_series, home_team_id)
    normalized.loc[away_fix, "PLAYER1_TEAM_ID"] = _series_compatible_scalar(player1_team_id_series, away_team_id)

    normalized.loc[home_fix & pseudo_team_code_mask, "PLAYER1_ID"] = _series_compatible_scalar(player1_id_series, 0)
    normalized.loc[away_fix & pseudo_team_code_mask, "PLAYER1_ID"] = _series_compatible_scalar(player1_id_series, 0)

    return normalized
