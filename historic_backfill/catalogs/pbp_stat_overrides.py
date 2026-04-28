from __future__ import annotations

import math
from pathlib import Path
from typing import Dict, Iterable, List

import pandas as pd

from pbpstats.offline.row_overrides import normalize_game_id


DEFAULT_PBP_STAT_OVERRIDES_PATH = Path(__file__).resolve().parent / "pbp_stat_overrides.csv"
PBP_STAT_OVERRIDE_COLUMNS = [
    "game_id",
    "team_id",
    "player_id",
    "stat_key",
    "stat_value",
    "notes",
]


def _parse_int(value: object, *, field: str, row_number: int, strict: bool) -> int | None:
    raw = str(value if value is not None else "").strip()
    if not raw:
        if strict:
            raise ValueError(f"Row {row_number} is missing required field {field}")
        return None
    try:
        parsed = float(raw)
    except ValueError:
        if strict:
            raise ValueError(f"Row {row_number} has invalid integer {field}: {raw!r}")
        return None
    if not parsed.is_integer():
        if strict:
            raise ValueError(f"Row {row_number} has non-integer {field}: {raw!r}")
        return None
    return int(parsed)


def _parse_float(
    value: object,
    *,
    field: str,
    row_number: int,
    strict: bool,
) -> float | None:
    raw = str(value if value is not None else "").strip()
    if not raw:
        if strict:
            raise ValueError(f"Row {row_number} is missing required field {field}")
        return None
    try:
        parsed = float(raw)
    except ValueError:
        if strict:
            raise ValueError(f"Row {row_number} has invalid float {field}: {raw!r}")
        return None
    if not math.isfinite(parsed):
        if strict:
            raise ValueError(
                f"Row {row_number} has non-finite float {field}: {raw!r}"
            )
        return None
    return parsed


def load_pbp_stat_overrides(
    path: Path | str = DEFAULT_PBP_STAT_OVERRIDES_PATH,
    *,
    missing_ok: bool = False,
    strict: bool = True,
) -> Dict[str, List[dict]]:
    override_path = Path(path)
    if not override_path.exists():
        if missing_ok:
            return {}
        raise FileNotFoundError(f"PBP stat override catalog not found: {override_path}")

    df = pd.read_csv(
        override_path,
        dtype={
            "game_id": str,
            "team_id": str,
            "player_id": str,
            "stat_key": str,
            "stat_value": str,
            "notes": str,
        },
        keep_default_na=False,
    ).fillna("")
    missing_columns = set(PBP_STAT_OVERRIDE_COLUMNS) - set(df.columns)
    if strict and missing_columns:
        raise ValueError(
            f"PBP stat override catalog missing columns: {sorted(missing_columns)}"
        )

    normalized = df.reindex(columns=PBP_STAT_OVERRIDE_COLUMNS, fill_value="").copy()

    overrides: Dict[str, List[dict]] = {}
    seen_rows: set[tuple[str, int, int, str]] = set()
    for row_number, row in enumerate(normalized.to_dict(orient="records"), start=2):
        try:
            game_id = normalize_game_id(row.get("game_id"))
        except ValueError:
            if strict:
                raise ValueError(
                    f"Row {row_number} has invalid game_id: {row.get('game_id')!r}"
                )
            continue
        team_id = _parse_int(
            row.get("team_id"), field="team_id", row_number=row_number, strict=strict
        )
        player_id = _parse_int(
            row.get("player_id"), field="player_id", row_number=row_number, strict=strict
        )
        stat_key = str(row.get("stat_key", "")).strip()
        stat_value = _parse_float(
            row.get("stat_value"),
            field="stat_value",
            row_number=row_number,
            strict=strict,
        )
        if team_id is None or player_id is None or stat_value is None:
            continue
        if team_id <= 0:
            if strict:
                raise ValueError(f"Row {row_number} team_id must be positive")
            continue
        if player_id <= 0:
            if strict:
                raise ValueError(f"Row {row_number} player_id must be positive")
            continue
        if not stat_key:
            if strict:
                raise ValueError(f"Row {row_number} is missing required field stat_key")
            continue
        if stat_value == 0.0:
            if strict:
                raise ValueError(f"Row {row_number} stat_value must be non-zero")
            continue
        key = (game_id, team_id, player_id, stat_key)
        if strict and key in seen_rows:
            raise ValueError(f"Row {row_number} duplicates PBP stat override {key}")
        seen_rows.add(key)
        overrides.setdefault(game_id, []).append(
            {
                "team_id": team_id,
                "player_id": player_id,
                "stat_key": stat_key,
                "stat_value": stat_value,
                "notes": str(row["notes"]),
            }
        )
    return overrides


_PBP_STAT_OVERRIDES: Dict[str, List[dict]] | None = None
_PBP_STAT_OVERRIDE_PATH: Path | None = None


def get_pbp_stat_overrides(
    path: Path | str | None = None,
) -> Dict[str, List[dict]]:
    global _PBP_STAT_OVERRIDES, _PBP_STAT_OVERRIDE_PATH
    if path is None and _PBP_STAT_OVERRIDES is not None:
        return _PBP_STAT_OVERRIDES
    resolved_path = Path(
        DEFAULT_PBP_STAT_OVERRIDES_PATH if path is None else path
    ).resolve()
    if _PBP_STAT_OVERRIDES is None or _PBP_STAT_OVERRIDE_PATH != resolved_path:
        _PBP_STAT_OVERRIDES = load_pbp_stat_overrides(resolved_path, strict=True)
        _PBP_STAT_OVERRIDE_PATH = resolved_path
    return _PBP_STAT_OVERRIDES


def set_pbp_stat_overrides(overrides: Dict[str, List[dict]] | None) -> None:
    global _PBP_STAT_OVERRIDES, _PBP_STAT_OVERRIDE_PATH
    _PBP_STAT_OVERRIDES = {} if overrides is None else overrides
    _PBP_STAT_OVERRIDE_PATH = None


def apply_pbp_stat_overrides(
    game_id: str | int,
    stat_rows: Iterable[dict] | None,
    overrides: Dict[str, List[dict]] | None = None,
) -> List[dict]:
    normalized_game_id = normalize_game_id(game_id)
    override_source = get_pbp_stat_overrides() if overrides is None else overrides
    applicable = override_source.get(normalized_game_id)
    adjusted = [dict(row) for row in (stat_rows or [])]
    if not applicable:
        return adjusted

    for override in applicable:
        adjusted.append(
            {
                "player_id": int(override["player_id"]),
                "team_id": int(override["team_id"]),
                "stat_key": str(override["stat_key"]),
                "stat_value": float(override["stat_value"]),
            }
        )

    return adjusted
