from __future__ import annotations

from pathlib import Path
from typing import Dict, Iterable, List

import pandas as pd


DEFAULT_PBP_STAT_OVERRIDES_PATH = Path(__file__).resolve().parent / "pbp_stat_overrides.csv"
PBP_STAT_OVERRIDE_COLUMNS = [
    "game_id",
    "team_id",
    "player_id",
    "stat_key",
    "stat_value",
    "notes",
]


def load_pbp_stat_overrides(
    path: Path | str = DEFAULT_PBP_STAT_OVERRIDES_PATH,
) -> Dict[str, List[dict]]:
    override_path = Path(path)
    if not override_path.exists():
        return {}

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
    ).fillna("")

    normalized = df.reindex(columns=PBP_STAT_OVERRIDE_COLUMNS, fill_value="").copy()
    game_ids = pd.to_numeric(normalized["game_id"], errors="coerce")
    team_ids = pd.to_numeric(normalized["team_id"], errors="coerce")
    player_ids = pd.to_numeric(normalized["player_id"], errors="coerce")
    stat_values = pd.to_numeric(normalized["stat_value"], errors="coerce")

    normalized["game_id"] = game_ids.apply(
        lambda value: str(int(value)).zfill(10) if pd.notna(value) else ""
    )
    normalized["team_id"] = team_ids.fillna(0).astype(int)
    normalized["player_id"] = player_ids.fillna(0).astype(int)
    normalized["stat_key"] = normalized["stat_key"].astype(str).str.strip()
    normalized["stat_value"] = stat_values.fillna(0.0).astype(float)
    normalized["notes"] = normalized["notes"].astype(str).str.strip()

    normalized = normalized[
        (normalized["game_id"] != "")
        & (normalized["team_id"] > 0)
        & (normalized["player_id"] > 0)
        & (normalized["stat_key"] != "")
        & (normalized["stat_value"] != 0.0)
    ].copy()

    overrides: Dict[str, List[dict]] = {}
    for row in normalized.to_dict(orient="records"):
        overrides.setdefault(row["game_id"], []).append(
            {
                "team_id": int(row["team_id"]),
                "player_id": int(row["player_id"]),
                "stat_key": str(row["stat_key"]),
                "stat_value": float(row["stat_value"]),
                "notes": str(row["notes"]),
            }
        )
    return overrides


_PBP_STAT_OVERRIDES = load_pbp_stat_overrides()


def apply_pbp_stat_overrides(
    game_id: str | int,
    stat_rows: Iterable[dict] | None,
    overrides: Dict[str, List[dict]] | None = None,
) -> List[dict]:
    normalized_game_id = str(int(game_id)).zfill(10)
    applicable = (overrides or _PBP_STAT_OVERRIDES).get(normalized_game_id)
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
