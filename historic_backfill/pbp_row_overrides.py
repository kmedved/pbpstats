from __future__ import annotations

from pathlib import Path
from typing import Dict, List

import pandas as pd


def _resolve_default_pbp_row_overrides_path(module_file: str | Path = __file__) -> Path:
    module_path = Path(module_file).resolve()
    candidates = [module_path.parent / "pbp_row_overrides.csv"]
    if module_path.parent.name == "__pycache__":
        candidates.append(module_path.parent.parent / "pbp_row_overrides.csv")
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return candidates[0]


DEFAULT_PBP_ROW_OVERRIDES_PATH = _resolve_default_pbp_row_overrides_path()
PBP_ROW_OVERRIDE_ACTION_COLUMN = "PBP_ROW_OVERRIDE_ACTION"
PBP_ROW_OVERRIDE_NOTES_COLUMN = "PBP_ROW_OVERRIDE_NOTES"


_OPTIONAL_INSERT_FIELDS = {
    "period",
    "pctimestring",
    "wctimestring",
    "description_side",
    "player_out_id",
    "player_out_name",
    "player_out_team_id",
    "player_in_id",
    "player_in_name",
    "player_in_team_id",
}


def _coerce_optional_int(value: object) -> int | None:
    raw = str(value if value is not None else "").strip()
    if not raw:
        return None
    try:
        return int(float(raw))
    except ValueError:
        return None


def _clock_seconds_from_pctimestring(value: str) -> float | None:
    parts = str(value).strip().split(":")
    if len(parts) != 2:
        return None
    try:
        return float(int(parts[0]) * 60 + float(parts[1]))
    except ValueError:
        return None


def load_pbp_row_overrides(path: Path | str = DEFAULT_PBP_ROW_OVERRIDES_PATH) -> Dict[str, List[dict]]:
    override_path = Path(path)
    if not override_path.exists():
        return {}

    df = pd.read_csv(
        override_path,
        dtype={
            "game_id": str,
            "action": str,
            "event_num": str,
            "anchor_event_num": str,
            "notes": str,
            **{field: str for field in _OPTIONAL_INSERT_FIELDS},
        },
    ).fillna("")

    overrides: Dict[str, List[dict]] = {}
    for row in df.to_dict(orient="records"):
        raw_gid = str(row.get("game_id", "")).strip()
        raw_action = str(row.get("action", "")).strip().lower()
        raw_event = str(row.get("event_num", "")).strip()
        raw_anchor = str(row.get("anchor_event_num", "")).strip()
        if not raw_gid or not raw_action or not raw_event:
            continue
        try:
            game_id = str(int(float(raw_gid))).zfill(10)
            event_num = int(float(raw_event))
            anchor_event_num = int(float(raw_anchor)) if raw_anchor else None
        except ValueError:
            continue
        parsed_row = {
            "action": raw_action,
            "event_num": event_num,
            "anchor_event_num": anchor_event_num,
            "notes": str(row.get("notes", "")).strip(),
        }
        for field in _OPTIONAL_INSERT_FIELDS:
            parsed_row[field] = str(row.get(field, "")).strip()
        overrides.setdefault(game_id, []).append(parsed_row)
    return overrides


_PBP_ROW_OVERRIDES = load_pbp_row_overrides()


def _infer_sub_description_column(
    df: pd.DataFrame,
    player_out_team_id: int | None,
    requested_side: str,
) -> str:
    side = requested_side.strip().lower()
    side_to_column = {
        "home": "HOMEDESCRIPTION",
        "visitor": "VISITORDESCRIPTION",
        "neutral": "NEUTRALDESCRIPTION",
    }
    if side in side_to_column and side_to_column[side] in df.columns:
        return side_to_column[side]

    if player_out_team_id is not None and {"PLAYER1_TEAM_ID", "EVENTMSGTYPE"}.issubset(df.columns):
        team_ids = pd.to_numeric(df.get("PLAYER1_TEAM_ID"), errors="coerce")
        event_types = pd.to_numeric(df.get("EVENTMSGTYPE"), errors="coerce")
        sub_rows = df[(event_types == 8) & (team_ids == player_out_team_id)]
        for column in ("HOMEDESCRIPTION", "VISITORDESCRIPTION", "NEUTRALDESCRIPTION"):
            if column not in sub_rows.columns:
                continue
            descriptions = sub_rows[column].fillna("").astype(str).str.strip()
            if descriptions.str.startswith("SUB:").any():
                return column

    for column in ("HOMEDESCRIPTION", "VISITORDESCRIPTION", "NEUTRALDESCRIPTION"):
        if column in df.columns:
            return column
    return "HOMEDESCRIPTION"


def _build_synthetic_sub_row(
    df: pd.DataFrame,
    anchor_row: pd.Series,
    override: dict,
    game_id: str,
) -> pd.DataFrame | None:
    event_num = override["event_num"]
    anchor_event_num = override.get("anchor_event_num")
    player_out_id = _coerce_optional_int(override.get("player_out_id"))
    player_in_id = _coerce_optional_int(override.get("player_in_id"))
    player_out_team_id = _coerce_optional_int(override.get("player_out_team_id"))
    player_in_team_id = _coerce_optional_int(override.get("player_in_team_id"))
    if player_out_id is None or player_in_id is None:
        return None

    player_out_name = str(override.get("player_out_name", "")).strip()
    player_in_name = str(override.get("player_in_name", "")).strip()
    if not player_out_name or not player_in_name:
        return None

    period = _coerce_optional_int(override.get("period"))
    if period is None:
        period = _coerce_optional_int(anchor_row.get("PERIOD"))
    pctimestring = str(override.get("pctimestring", "")).strip() or str(anchor_row.get("PCTIMESTRING", "")).strip()
    wctimestring = str(override.get("wctimestring", "")).strip() or str(anchor_row.get("WCTIMESTRING", "")).strip()
    description = f"SUB: {player_in_name} FOR {player_out_name}"

    row = anchor_row.copy()
    assignments = {
        "GAME_ID": game_id,
        "EVENTNUM": event_num,
        "EVENTMSGTYPE": 8,
        "EVENTMSGACTIONTYPE": 0,
        "PERIOD": period,
        "PCTIMESTRING": pctimestring,
        "WCTIMESTRING": wctimestring,
        "SCORE": "",
        "SCOREMARGIN": "",
        "PLAYER1_ID": player_out_id,
        "PLAYER1_NAME": player_out_name,
        "PLAYER1_TEAM_ID": player_out_team_id or "",
        "PLAYER2_ID": player_in_id,
        "PLAYER2_NAME": player_in_name,
        "PLAYER2_TEAM_ID": player_in_team_id or player_out_team_id or "",
        "PLAYER3_ID": 0,
        "PLAYER3_NAME": "",
        "PLAYER3_TEAM_ID": "",
        "event_num": event_num,
        "period": period,
        "description": description,
        PBP_ROW_OVERRIDE_ACTION_COLUMN: override.get("action", ""),
        PBP_ROW_OVERRIDE_NOTES_COLUMN: override.get("notes", ""),
    }
    clock_seconds = _clock_seconds_from_pctimestring(pctimestring)
    if clock_seconds is not None:
        assignments["clock_seconds_remaining"] = clock_seconds

    for column in ("HOMEDESCRIPTION", "VISITORDESCRIPTION", "NEUTRALDESCRIPTION"):
        if column in row.index:
            row[column] = ""
    description_column = _infer_sub_description_column(
        df,
        player_out_team_id,
        str(override.get("description_side", "")),
    )
    assignments[description_column] = description

    for column, value in assignments.items():
        if column in row.index:
            row[column] = value

    return pd.DataFrame([row], columns=df.columns)


def apply_pbp_row_overrides(
    game_df: pd.DataFrame,
    overrides: Dict[str, List[dict]] | None = None,
) -> pd.DataFrame:
    if game_df.empty:
        return game_df

    game_id = str(game_df["GAME_ID"].iloc[0]).zfill(10)
    applicable = (overrides or _PBP_ROW_OVERRIDES).get(game_id)
    if not applicable:
        return game_df

    df = game_df.copy().reset_index(drop=True)
    event_nums = pd.to_numeric(df["EVENTNUM"], errors="coerce")

    for override in applicable:
        action = override["action"]
        event_num = override["event_num"]
        anchor_event_num = override.get("anchor_event_num")

        if action in {"insert_sub_before", "insert_sub_after"}:
            if anchor_event_num is None:
                continue
            if len(event_nums.index[event_nums == event_num]) != 0:
                continue
            for column in (PBP_ROW_OVERRIDE_ACTION_COLUMN, PBP_ROW_OVERRIDE_NOTES_COLUMN):
                if column not in df.columns:
                    df[column] = ""
            anchor_idx = event_nums.index[event_nums == anchor_event_num]
            if len(anchor_idx) != 1:
                continue
            anchor_idx = int(anchor_idx[0])
            row = _build_synthetic_sub_row(df, df.iloc[anchor_idx], override, game_id)
            if row is None:
                continue
            insert_at = anchor_idx if action == "insert_sub_before" else anchor_idx + 1
            df = pd.concat([df.iloc[:insert_at], row, df.iloc[insert_at:]], ignore_index=True)
            event_nums = pd.to_numeric(df["EVENTNUM"], errors="coerce")
            continue

        match_idx = event_nums.index[event_nums == event_num]
        if len(match_idx) != 1:
            continue
        row_idx = int(match_idx[0])

        if action == "drop":
            df = df.drop(index=row_idx).reset_index(drop=True)
            event_nums = pd.to_numeric(df["EVENTNUM"], errors="coerce")
            continue

        if action not in {"move_before", "move_after"} or anchor_event_num is None:
            continue

        anchor_idx = event_nums.index[event_nums == anchor_event_num]
        if len(anchor_idx) != 1:
            continue
        anchor_idx = int(anchor_idx[0])

        row = df.iloc[[row_idx]].copy()
        df = df.drop(index=row_idx).reset_index(drop=True)
        event_nums = pd.to_numeric(df["EVENTNUM"], errors="coerce")

        anchor_idx = int(event_nums.index[event_nums == anchor_event_num][0])
        insert_at = anchor_idx if action == "move_before" else anchor_idx + 1

        df = pd.concat([df.iloc[:insert_at], row, df.iloc[insert_at:]], ignore_index=True)
        event_nums = pd.to_numeric(df["EVENTNUM"], errors="coerce")

    return df.reset_index(drop=True)
