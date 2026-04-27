from __future__ import annotations

import copy
from pathlib import Path
from typing import Any, Dict

import pandas as pd


BOXSCORE_SOURCE_COLUMNS = [
    "GAME_ID",
    "TEAM_ID",
    "TEAM_ABBREVIATION",
    "TEAM_CITY",
    "PLAYER_ID",
    "PLAYER_NAME",
    "NICKNAME",
    "START_POSITION",
    "COMMENT",
    "MIN",
    "FGM",
    "FGA",
    "FG_PCT",
    "FG3M",
    "FG3A",
    "FG3_PCT",
    "FTM",
    "FTA",
    "FT_PCT",
    "OREB",
    "DREB",
    "REB",
    "AST",
    "STL",
    "BLK",
    "TO",
    "PF",
    "PTS",
    "PLUS_MINUS",
]

BOXSCORE_SOURCE_OVERRIDE_COLUMNS = ["game_id", *BOXSCORE_SOURCE_COLUMNS, "notes"]

TEXT_COLUMNS = {
    "TEAM_ABBREVIATION",
    "TEAM_CITY",
    "PLAYER_NAME",
    "NICKNAME",
    "START_POSITION",
    "COMMENT",
    "MIN",
}

FLOAT_COLUMNS = {"FG_PCT", "FG3_PCT", "FT_PCT"}

INT_COLUMNS = set(BOXSCORE_SOURCE_COLUMNS) - TEXT_COLUMNS - FLOAT_COLUMNS

_BOXSCORE_SOURCE_OVERRIDES: pd.DataFrame | None = None
_BOXSCORE_SOURCE_OVERRIDE_PATH: Path | None = None


def _empty_boxscore_source_overrides_df() -> pd.DataFrame:
    return pd.DataFrame(columns=BOXSCORE_SOURCE_OVERRIDE_COLUMNS)


def _normalize_game_id(value: Any) -> str:
    return str(int(value)).zfill(10)


def _safe_int(value: Any) -> int:
    try:
        if pd.isna(value):
            return 0
    except Exception:
        pass
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _normalize_boxscore_source_overrides_df(
    overrides: pd.DataFrame | None,
) -> pd.DataFrame:
    if overrides is None or overrides.empty:
        return _empty_boxscore_source_overrides_df()

    normalized = overrides.copy().reindex(
        columns=BOXSCORE_SOURCE_OVERRIDE_COLUMNS, fill_value=""
    )
    game_ids = pd.to_numeric(normalized["game_id"], errors="coerce")
    normalized["game_id"] = game_ids.apply(
        lambda value: _normalize_game_id(int(value)) if pd.notna(value) else ""
    )

    for column in INT_COLUMNS:
        normalized[column] = (
            pd.to_numeric(normalized[column], errors="coerce").fillna(0).astype(int)
        )

    for column in FLOAT_COLUMNS:
        normalized[column] = pd.to_numeric(normalized[column], errors="coerce").fillna(0.0)

    for column in TEXT_COLUMNS:
        normalized[column] = normalized[column].fillna("").astype(str)

    normalized["notes"] = normalized["notes"].fillna("").astype(str)
    normalized = normalized[
        (normalized["game_id"] != "")
        & (normalized["TEAM_ID"] > 0)
        & (normalized["PLAYER_ID"] > 0)
    ].copy()
    return normalized.reset_index(drop=True)


def load_boxscore_source_overrides(
    filepath: str | Path | None = None,
) -> pd.DataFrame:
    path = (
        Path(filepath)
        if filepath is not None
        else Path(__file__).with_name("boxscore_source_overrides.csv")
    )
    if not path.exists():
        return _empty_boxscore_source_overrides_df()
    return _normalize_boxscore_source_overrides_df(pd.read_csv(path))


def get_boxscore_source_overrides(
    filepath: str | Path | None = None,
) -> pd.DataFrame:
    global _BOXSCORE_SOURCE_OVERRIDES, _BOXSCORE_SOURCE_OVERRIDE_PATH

    path = (
        Path(filepath)
        if filepath is not None
        else Path(__file__).with_name("boxscore_source_overrides.csv")
    )
    if _BOXSCORE_SOURCE_OVERRIDES is not None and _BOXSCORE_SOURCE_OVERRIDE_PATH == path:
        return _BOXSCORE_SOURCE_OVERRIDES.copy()

    overrides = load_boxscore_source_overrides(path)
    _BOXSCORE_SOURCE_OVERRIDES = overrides
    _BOXSCORE_SOURCE_OVERRIDE_PATH = path
    return overrides.copy()


def set_boxscore_source_overrides(overrides: pd.DataFrame | None) -> None:
    global _BOXSCORE_SOURCE_OVERRIDES, _BOXSCORE_SOURCE_OVERRIDE_PATH

    _BOXSCORE_SOURCE_OVERRIDES = _normalize_boxscore_source_overrides_df(overrides)
    _BOXSCORE_SOURCE_OVERRIDE_PATH = None


def apply_boxscore_response_overrides(
    game_id: str | int,
    data: Dict[str, Any] | None,
    overrides: pd.DataFrame | None = None,
) -> Dict[str, Any] | None:
    if data is None:
        return None

    normalized_game_id = _normalize_game_id(game_id)
    overrides = (
        _normalize_boxscore_source_overrides_df(overrides)
        if overrides is not None
        else get_boxscore_source_overrides()
    )
    if overrides.empty:
        return data

    game_overrides = overrides[overrides["game_id"] == normalized_game_id]
    if game_overrides.empty:
        return data

    adjusted = copy.deepcopy(data)
    result_sets = adjusted.get("resultSets")
    if not result_sets:
        return adjusted

    result_set = result_sets[0]
    headers = list(result_set.get("headers", []))
    if not headers:
        return adjusted

    override_keys = {
        (_safe_int(row.TEAM_ID), _safe_int(row.PLAYER_ID))
        for row in game_overrides.itertuples(index=False)
    }

    existing_rows = []
    for row in result_set.get("rowSet", []):
        record = dict(zip(headers, row))
        key = (_safe_int(record.get("TEAM_ID")), _safe_int(record.get("PLAYER_ID")))
        if key in override_keys:
            continue
        existing_rows.append(row)

    for override in game_overrides.itertuples(index=False):
        override_record = {
            column: getattr(override, column)
            for column in BOXSCORE_SOURCE_COLUMNS
        }
        existing_rows.append([override_record.get(header, "") for header in headers])

    result_set["rowSet"] = existing_rows
    return adjusted
