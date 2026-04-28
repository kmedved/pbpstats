from __future__ import annotations

import copy
import math
from pathlib import Path
from typing import Any, Dict

import pandas as pd

from pbpstats.offline.row_overrides import normalize_game_id


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
    "GAME_ID",
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
    return normalize_game_id(value)


def _normalize_game_id_or_blank(value: Any) -> str:
    text = str(value if value is not None else "").strip()
    if not text:
        return ""
    try:
        return _normalize_game_id(text)
    except ValueError:
        return ""


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
    normalized["game_id"] = normalized["game_id"].map(_normalize_game_id_or_blank)
    normalized["GAME_ID"] = normalized["GAME_ID"].map(_normalize_game_id_or_blank)

    for column in INT_COLUMNS:
        normalized[column] = (
            pd.to_numeric(normalized[column], errors="coerce").fillna(0).astype(int)
        )

    for column in FLOAT_COLUMNS:
        normalized[column] = pd.to_numeric(normalized[column], errors="coerce").fillna(
            0.0
        )

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
    return _normalize_boxscore_source_overrides_df(pd.read_csv(path, dtype=str))


def _parse_int_field(value: Any, *, field: str, row_number: int, path: Path) -> int:
    text = str(value if value is not None else "").strip()
    if not text:
        raise ValueError(f"{path} row {row_number} missing required field {field}")
    try:
        parsed = float(text)
    except ValueError as exc:
        raise ValueError(
            f"{path} row {row_number} has invalid integer {field}: {text!r}"
        ) from exc
    if not parsed.is_integer():
        raise ValueError(f"{path} row {row_number} has non-integer {field}: {text!r}")
    return int(parsed)


def _parse_float_field(value: Any, *, field: str, row_number: int, path: Path) -> float:
    text = str(value if value is not None else "").strip()
    if not text:
        text = "0"
    try:
        parsed = float(text)
    except ValueError as exc:
        raise ValueError(
            f"{path} row {row_number} has invalid float {field}: {text!r}"
        ) from exc
    if not math.isfinite(parsed):
        raise ValueError(
            f"{path} row {row_number} has non-finite float {field}: {text!r}"
        )
    return parsed


def validate_boxscore_source_overrides(path: str | Path) -> None:
    catalog_path = Path(path)
    df = pd.read_csv(catalog_path, dtype=str).fillna("")
    missing_columns = set(BOXSCORE_SOURCE_OVERRIDE_COLUMNS) - set(df.columns)
    if missing_columns:
        raise ValueError(f"{catalog_path} missing columns: {sorted(missing_columns)}")

    seen_keys: set[tuple[str, int, int]] = set()
    for row_number, row in enumerate(df.to_dict(orient="records"), start=2):
        game_id = normalize_game_id(row.get("game_id"))
        row_game_id = normalize_game_id(row.get("GAME_ID"))
        if game_id != row_game_id:
            raise ValueError(
                f"{catalog_path} row {row_number} game_id={game_id} "
                f"does not match GAME_ID={row_game_id}"
            )

        team_id = _parse_int_field(
            row.get("TEAM_ID"),
            field="TEAM_ID",
            row_number=row_number,
            path=catalog_path,
        )
        player_id = _parse_int_field(
            row.get("PLAYER_ID"),
            field="PLAYER_ID",
            row_number=row_number,
            path=catalog_path,
        )
        if team_id <= 0:
            raise ValueError(
                f"{catalog_path} row {row_number} TEAM_ID must be positive"
            )
        if player_id <= 0:
            raise ValueError(
                f"{catalog_path} row {row_number} PLAYER_ID must be positive"
            )

        for field in sorted(INT_COLUMNS - {"GAME_ID", "TEAM_ID", "PLAYER_ID"}):
            _parse_int_field(
                row.get(field), field=field, row_number=row_number, path=catalog_path
            )
        for field in sorted(FLOAT_COLUMNS):
            _parse_float_field(
                row.get(field), field=field, row_number=row_number, path=catalog_path
            )

        key = (game_id, team_id, player_id)
        if key in seen_keys:
            raise ValueError(f"{catalog_path} row {row_number} duplicates {key}")
        seen_keys.add(key)


def get_boxscore_source_overrides(
    filepath: str | Path | None = None,
) -> pd.DataFrame:
    global _BOXSCORE_SOURCE_OVERRIDES, _BOXSCORE_SOURCE_OVERRIDE_PATH

    if filepath is None and _BOXSCORE_SOURCE_OVERRIDES is not None:
        return _BOXSCORE_SOURCE_OVERRIDES.copy()

    path = (
        Path(filepath)
        if filepath is not None
        else Path(__file__).with_name("boxscore_source_overrides.csv")
    )
    if (
        _BOXSCORE_SOURCE_OVERRIDES is not None
        and _BOXSCORE_SOURCE_OVERRIDE_PATH == path
    ):
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
            column: getattr(override, column) for column in BOXSCORE_SOURCE_COLUMNS
        }
        existing_rows.append([override_record.get(header, "") for header in headers])

    result_set["rowSet"] = existing_rows
    return adjusted
