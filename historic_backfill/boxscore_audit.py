from __future__ import annotations

import json
import sqlite3
import zlib
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple

import pandas as pd
import pbpstats
from joblib import Parallel, delayed


AUDIT_STATS = [
    "PTS",
    "AST",
    "STL",
    "BLK",
    "TOV",
    "PF",
    "FGA",
    "FGM",
    "3PA",
    "3PM",
    "FTA",
    "FTM",
    "OREB",
    "DRB",
    "REB",
]

TEAM_AUDIT_COLUMNS = [
    "game_id",
    "team_id",
    "team_abbreviation",
    "has_mismatch",
    "max_abs_diff",
]
TEAM_AUDIT_COLUMNS += [f"PBP_{stat}" for stat in AUDIT_STATS]
TEAM_AUDIT_COLUMNS += [f"OFFICIAL_{stat}" for stat in AUDIT_STATS]
TEAM_AUDIT_COLUMNS += [f"DIFF_{stat}" for stat in AUDIT_STATS]

PLAYER_MISMATCH_COLUMNS = [
    "game_id",
    "team_id",
    "player_id",
    "player_name",
    "has_mismatch",
    "max_abs_diff",
]
PLAYER_MISMATCH_COLUMNS += [f"PBP_{stat}" for stat in AUDIT_STATS]
PLAYER_MISMATCH_COLUMNS += [f"OFFICIAL_{stat}" for stat in AUDIT_STATS]
PLAYER_MISMATCH_COLUMNS += [f"DIFF_{stat}" for stat in AUDIT_STATS]

AUDIT_ERROR_COLUMNS = ["game_id", "error"]
AUDIT_OVERRIDE_COLUMNS = [
    "game_id",
    "team_id",
    "player_id",
    "stat",
    "action",
    "notes",
]

SHOT_TYPES = [
    pbpstats.AT_RIM_STRING,
    pbpstats.SHORT_MID_RANGE_STRING,
    pbpstats.LONG_MID_RANGE_STRING,
    pbpstats.UNKNOWN_SHOT_DISTANCE_STRING,
    pbpstats.ARC_3_STRING,
    pbpstats.CORNER_3_STRING,
]
THREE_POINT_TYPES = [pbpstats.ARC_3_STRING, pbpstats.CORNER_3_STRING]

FT_MADE_KEYS = [
    pbpstats.FTS_MADE_STRING,
    pbpstats.FT_1_PT_MADE_STRING,
    pbpstats.FT_2_PT_MADE_STRING,
    pbpstats.FT_3_PT_MADE_STRING,
    pbpstats.TECHNICAL_FTS_MADE_STRING,
]
FT_MISSED_KEYS = [
    pbpstats.FTS_MISSED_STRING,
    pbpstats.FT_1_PT_MISSED_STRING,
    pbpstats.FT_2_PT_MISSED_STRING,
    pbpstats.FT_3_PT_MISSED_STRING,
]
TURNOVER_KEYS = [
    pbpstats.BAD_PASS_TURNOVER_STRING,
    pbpstats.LOST_BALL_TURNOVER_STRING,
    pbpstats.DEADBALL_TURNOVERS_STRING,
]
STEAL_KEYS = [pbpstats.BAD_PASS_STEAL_STRING, pbpstats.LOST_BALL_STEAL_STRING]
FOUL_KEYS = [
    pbpstats.PERSONAL_FOUL_TYPE_STRING,
    pbpstats.SHOOTING_FOUL_TYPE_STRING,
    pbpstats.LOOSE_BALL_FOUL_TYPE_STRING,
    pbpstats.OFFENSIVE_FOUL_TYPE_STRING,
    pbpstats.CHARGE_FOUL_TYPE_STRING,
    pbpstats.INBOUND_FOUL_TYPE_STRING,
    pbpstats.AWAY_FROM_PLAY_FOUL_TYPE_STRING,
    pbpstats.CLEAR_PATH_FOUL_TYPE_STRING,
    pbpstats.DOUBLE_FOUL_TYPE_STRING,
    pbpstats.PERSONAL_BLOCK_TYPE_STRING,
    pbpstats.PERSONAL_TAKE_TYPE_STRING,
    pbpstats.SHOOTING_BLOCK_TYPE_STRING,
    pbpstats.TRANSITION_TAKE_TYPE_STRING,
    pbpstats.FLAGRANT_1_FOUL_TYPE_STRING,
    pbpstats.FLAGRANT_2_FOUL_TYPE_STRING,
]

NOTEBOOK_MARKER = 'if __name__ == "__main__":\n    pass\n'
TEXT_COLUMNS = [
    "HOMEDESCRIPTION",
    "VISITORDESCRIPTION",
    "NEUTRALSITEDESCRIPTION",
    "PLAYER1_NAME",
    "PLAYER2_NAME",
    "PLAYER3_NAME",
]

_AUDIT_NAMESPACE: Dict[str, Any] | None = None
_AUDIT_DB_PATH: Path | None = None
_AUDIT_NOTEBOOK_DUMP: Path | None = None
_AUDIT_OVERRIDES: pd.DataFrame | None = None
_AUDIT_OVERRIDE_PATH: Path | None = None


class _BoxscoreSourceLoader:
    def __init__(self, data: Dict[str, Any]):
        self._data = data

    def load_data(self) -> Dict[str, Any]:
        return self._data


def _empty_team_audit_df() -> pd.DataFrame:
    return pd.DataFrame(columns=TEAM_AUDIT_COLUMNS)


def _empty_player_mismatch_df() -> pd.DataFrame:
    return pd.DataFrame(columns=PLAYER_MISMATCH_COLUMNS)


def _empty_audit_error_df() -> pd.DataFrame:
    return pd.DataFrame(columns=AUDIT_ERROR_COLUMNS)


def _empty_audit_override_df() -> pd.DataFrame:
    return pd.DataFrame(columns=AUDIT_OVERRIDE_COLUMNS)


def _normalize_game_id(value: str | int) -> str:
    return str(int(value)).zfill(10)


def _load_raw_response(
    db_path: Path, game_id: str, endpoint: str
) -> Dict[str, Any] | None:
    game_id = _normalize_game_id(game_id)
    conn = sqlite3.connect(db_path, timeout=30)
    try:
        row = conn.execute(
            "SELECT data FROM raw_responses WHERE game_id=? AND endpoint=? AND team_id IS NULL",
            (game_id, endpoint),
        ).fetchone()
        if not row:
            return None
        blob = row[0]
        try:
            return json.loads(zlib.decompress(blob).decode())
        except (zlib.error, TypeError):
            if isinstance(blob, bytes):
                return json.loads(blob.decode())
            return json.loads(blob)
    finally:
        conn.close()


def _get_audit_namespace(notebook_dump: Path, db_path: Path) -> Dict[str, Any]:
    global _AUDIT_NAMESPACE, _AUDIT_DB_PATH, _AUDIT_NOTEBOOK_DUMP

    if (
        _AUDIT_NAMESPACE is not None
        and _AUDIT_DB_PATH == db_path
        and _AUDIT_NOTEBOOK_DUMP == notebook_dump
    ):
        return _AUDIT_NAMESPACE

    from cautious_rerun import install_local_boxscore_wrapper, load_v9b_namespace

    namespace = load_v9b_namespace()
    install_local_boxscore_wrapper(namespace, db_path)
    namespace["DB_PATH"] = db_path

    _AUDIT_NAMESPACE = namespace
    _AUDIT_DB_PATH = db_path
    _AUDIT_NOTEBOOK_DUMP = notebook_dump
    return namespace


def _load_single_game_pbp(parquet_path: Path, game_id: str) -> pd.DataFrame:
    parquet_game_id = str(int(game_id))
    try:
        df = pd.read_parquet(parquet_path, filters=[("GAME_ID", "==", parquet_game_id)])
    except Exception:
        df = pd.read_parquet(parquet_path)
        df = df[df["GAME_ID"].astype(str) == parquet_game_id].copy()

    if df.empty:
        return df

    df.columns = [c.upper() for c in df.columns]
    if "WCTIMESTRING" not in df.columns:
        df["WCTIMESTRING"] = "00:00 AM"

    for col in TEXT_COLUMNS:
        if col in df.columns:
            df[col] = df[col].fillna("")

    if "GAME_ID" in df.columns:
        df["GAME_ID"] = df["GAME_ID"].astype(str).str.zfill(10)

    return df


def _sum_existing(stats_wide: pd.DataFrame, keys: List[str]) -> pd.Series:
    existing = [k for k in keys if k in stats_wide.columns]
    if not existing:
        return pd.Series(0.0, index=stats_wide.index)
    return stats_wide[existing].sum(axis=1).astype(float)


def _normalize_audit_overrides_df(audit_overrides: pd.DataFrame | None) -> pd.DataFrame:
    if audit_overrides is None or audit_overrides.empty:
        return _empty_audit_override_df()

    overrides = audit_overrides.copy().reindex(columns=AUDIT_OVERRIDE_COLUMNS, fill_value="")
    game_ids = pd.to_numeric(overrides["game_id"], errors="coerce")
    overrides["game_id"] = game_ids.apply(
        lambda value: _normalize_game_id(int(value)) if pd.notna(value) else ""
    )
    overrides["team_id"] = pd.to_numeric(overrides["team_id"], errors="coerce").fillna(0).astype(int)
    overrides["player_id"] = pd.to_numeric(overrides["player_id"], errors="coerce").fillna(0).astype(int)
    overrides["stat"] = overrides["stat"].fillna("").astype(str).str.upper()
    overrides["action"] = overrides["action"].fillna("accept_pbp").astype(str).str.lower()
    overrides["notes"] = overrides["notes"].fillna("").astype(str)
    overrides = overrides[
        (overrides["game_id"] != "")
        & overrides["stat"].isin(AUDIT_STATS)
        & (overrides["player_id"] > 0)
        & overrides["action"].isin(["accept_pbp"])
    ].copy()
    return overrides.reset_index(drop=True)


def load_boxscore_audit_overrides(filepath: str | Path | None = None) -> pd.DataFrame:
    path = Path(filepath) if filepath is not None else Path(__file__).with_name("boxscore_audit_overrides.csv")
    if not path.exists():
        return _empty_audit_override_df()
    return _normalize_audit_overrides_df(pd.read_csv(path))


def get_boxscore_audit_overrides(filepath: str | Path | None = None) -> pd.DataFrame:
    global _AUDIT_OVERRIDES, _AUDIT_OVERRIDE_PATH

    path = Path(filepath) if filepath is not None else Path(__file__).with_name("boxscore_audit_overrides.csv")
    if _AUDIT_OVERRIDES is not None and _AUDIT_OVERRIDE_PATH == path:
        return _AUDIT_OVERRIDES.copy()

    overrides = load_boxscore_audit_overrides(path)
    _AUDIT_OVERRIDES = overrides
    _AUDIT_OVERRIDE_PATH = path
    return overrides.copy()


def set_boxscore_audit_overrides(audit_overrides: pd.DataFrame | None) -> None:
    global _AUDIT_OVERRIDES, _AUDIT_OVERRIDE_PATH

    _AUDIT_OVERRIDES = _normalize_audit_overrides_df(audit_overrides)
    _AUDIT_OVERRIDE_PATH = None


def _apply_player_audit_overrides(
    game_id: str,
    pbp_players: pd.DataFrame,
    official_players: pd.DataFrame,
    audit_overrides: pd.DataFrame,
) -> pd.DataFrame:
    if official_players.empty or audit_overrides.empty:
        return official_players

    game_overrides = audit_overrides[audit_overrides["game_id"] == game_id]
    if game_overrides.empty:
        return official_players

    adjusted = official_players.copy()
    pbp_lookup = pbp_players.set_index(["player_id", "team_id"]) if not pbp_players.empty else pd.DataFrame()

    for override in game_overrides.itertuples(index=False):
        key = (int(override.player_id), int(override.team_id))
        if pbp_lookup.empty or key not in pbp_lookup.index:
            continue

        mask = (adjusted["player_id"] == key[0]) & (adjusted["team_id"] == key[1])
        if not mask.any():
            continue

        pbp_row = pbp_lookup.loc[key]
        if isinstance(pbp_row, pd.DataFrame):
            pbp_value = float(pd.to_numeric(pbp_row[override.stat], errors="coerce").fillna(0.0).sum())
        else:
            pbp_value = float(pd.to_numeric(pd.Series([pbp_row.get(override.stat, 0.0)]), errors="coerce").fillna(0.0).iloc[0])

        adjusted.loc[mask, override.stat] = pbp_value
        if override.stat in ("OREB", "DRB"):
            adjusted.loc[mask, "REB"] = (
                pd.to_numeric(adjusted.loc[mask, "OREB"], errors="coerce").fillna(0.0)
                + pd.to_numeric(adjusted.loc[mask, "DRB"], errors="coerce").fillna(0.0)
            )

    return adjusted


def build_pbp_boxscore_from_stat_rows(stat_rows: Iterable[Dict[str, Any]]) -> pd.DataFrame:
    stats_df = pd.DataFrame(list(stat_rows))
    if stats_df.empty:
        return pd.DataFrame(columns=["player_id", "team_id", *AUDIT_STATS])

    stats_df["player_id"] = pd.to_numeric(stats_df["player_id"], errors="coerce").fillna(0).astype(int)
    stats_df["team_id"] = pd.to_numeric(stats_df["team_id"], errors="coerce").fillna(0).astype(int)
    stats_df["stat_value"] = pd.to_numeric(stats_df["stat_value"], errors="coerce").fillna(0.0)

    stats_wide = stats_df.pivot_table(
        index=["player_id", "team_id"],
        columns="stat_key",
        values="stat_value",
        aggfunc="sum",
        fill_value=0.0,
    )

    made_keys: List[str] = []
    missed_keys: List[str] = []
    blocked_attempt_keys: List[str] = []
    assist_keys: List[str] = []
    block_keys: List[str] = []
    oreb_keys = [pbpstats.FREE_THROW_STRING + pbpstats.OFFENSIVE_ABBREVIATION_PREFIX + pbpstats.REBOUNDS_STRING]
    dreb_keys = [pbpstats.FREE_THROW_STRING + pbpstats.DEFENSIVE_ABBREVIATION_PREFIX + pbpstats.REBOUNDS_STRING]

    for shot_type in SHOT_TYPES:
        made_keys.extend(
            [
                f"{pbpstats.ASSISTED_STRING}{shot_type}",
                f"{pbpstats.UNASSISTED_STRING}{shot_type}",
            ]
        )
        missed_keys.append(f"{pbpstats.MISSED_STRING}{shot_type}")
        blocked_attempt_keys.append(f"{shot_type}{pbpstats.BLOCKED_STRING}")
        assist_keys.append(f"{shot_type}{pbpstats.ASSISTS_STRING}")
        block_keys.append(f"{pbpstats.BLOCKED_STRING}{shot_type}")
        oreb_keys.extend(
            [
                f"{shot_type}{pbpstats.OFFENSIVE_ABBREVIATION_PREFIX}{pbpstats.REBOUNDS_STRING}",
                f"{shot_type}{pbpstats.BLOCKED_STRING}{pbpstats.OFFENSIVE_ABBREVIATION_PREFIX}{pbpstats.REBOUNDS_STRING}",
            ]
        )
        dreb_keys.extend(
            [
                f"{shot_type}{pbpstats.DEFENSIVE_ABBREVIATION_PREFIX}{pbpstats.REBOUNDS_STRING}",
                f"{shot_type}{pbpstats.BLOCKED_STRING}{pbpstats.DEFENSIVE_ABBREVIATION_PREFIX}{pbpstats.REBOUNDS_STRING}",
            ]
        )

    three_point_made_keys: List[str] = []
    three_point_attempt_keys: List[str] = []
    for shot_type in THREE_POINT_TYPES:
        three_point_made_keys.extend(
            [
                f"{pbpstats.ASSISTED_STRING}{shot_type}",
                f"{pbpstats.UNASSISTED_STRING}{shot_type}",
            ]
        )
        three_point_attempt_keys.extend(
            [
                f"{pbpstats.MISSED_STRING}{shot_type}",
                f"{shot_type}{pbpstats.BLOCKED_STRING}",
            ]
        )

    pbp_box = pd.DataFrame(index=stats_wide.index)
    pbp_box["FGM"] = _sum_existing(stats_wide, made_keys)
    pbp_box["FGA"] = pbp_box["FGM"] + _sum_existing(stats_wide, missed_keys + blocked_attempt_keys)
    pbp_box["3PM"] = _sum_existing(stats_wide, three_point_made_keys)
    pbp_box["3PA"] = pbp_box["3PM"] + _sum_existing(stats_wide, three_point_attempt_keys)
    pbp_box["FTM"] = _sum_existing(stats_wide, FT_MADE_KEYS)
    pbp_box["FTA"] = pbp_box["FTM"] + _sum_existing(stats_wide, FT_MISSED_KEYS)
    pbp_box["AST"] = _sum_existing(stats_wide, assist_keys)
    pbp_box["STL"] = _sum_existing(stats_wide, STEAL_KEYS)
    pbp_box["BLK"] = _sum_existing(stats_wide, block_keys)
    pbp_box["TOV"] = _sum_existing(stats_wide, TURNOVER_KEYS)
    pbp_box["PF"] = _sum_existing(stats_wide, FOUL_KEYS)
    pbp_box["OREB"] = _sum_existing(stats_wide, oreb_keys)
    pbp_box["DRB"] = _sum_existing(stats_wide, dreb_keys)
    pbp_box["REB"] = pbp_box["OREB"] + pbp_box["DRB"]
    pbp_box["PTS"] = (2.0 * (pbp_box["FGM"] - pbp_box["3PM"])) + (3.0 * pbp_box["3PM"]) + pbp_box["FTM"]

    return pbp_box.reset_index()[["player_id", "team_id", *AUDIT_STATS]]


def _prepare_official_boxscore(df_box: pd.DataFrame) -> pd.DataFrame:
    if df_box.empty:
        return pd.DataFrame(columns=["player_id", "team_id", "player_name", "team_abbreviation", *AUDIT_STATS])

    player_box = df_box.copy()
    player_box["PLAYER_ID"] = pd.to_numeric(player_box["PLAYER_ID"], errors="coerce").fillna(0).astype(int)
    player_box["TEAM_ID"] = pd.to_numeric(player_box["TEAM_ID"], errors="coerce").fillna(0).astype(int)
    player_box = player_box[player_box["PLAYER_ID"] != 0].copy()
    if player_box.empty:
        return pd.DataFrame(columns=["player_id", "team_id", "player_name", "team_abbreviation", *AUDIT_STATS])

    rename_map = {
        "PLAYER_ID": "player_id",
        "TEAM_ID": "team_id",
        "PLAYER_NAME": "player_name",
        "TEAM_ABBREVIATION": "team_abbreviation",
        "TO": "TOV",
        "DREB": "DRB",
    }
    player_box = player_box.rename(columns=rename_map)

    for stat in ["PTS", "AST", "STL", "BLK", "TOV", "PF", "FGA", "FGM", "FG3A", "FG3M", "FTA", "FTM", "OREB", "DRB", "REB"]:
        if stat in player_box.columns:
            player_box[stat] = pd.to_numeric(player_box[stat], errors="coerce").fillna(0.0)

    player_box["3PA"] = player_box.get("FG3A", 0.0)
    player_box["3PM"] = player_box.get("FG3M", 0.0)
    if "REB" not in player_box.columns:
        player_box["REB"] = player_box.get("OREB", 0.0) + player_box.get("DRB", 0.0)

    return player_box[["player_id", "team_id", "player_name", "team_abbreviation", *AUDIT_STATS]].copy()


def build_game_boxscore_audit(
    game_id: str,
    pbp_box_df: pd.DataFrame,
    official_box_df: pd.DataFrame,
    player_name_map: Dict[int, str] | None = None,
    audit_overrides: pd.DataFrame | None = None,
) -> Tuple[pd.DataFrame, pd.DataFrame, Dict[str, Any]]:
    game_id = _normalize_game_id(game_id)
    player_name_map = player_name_map or {}
    audit_overrides = (
        _normalize_audit_overrides_df(audit_overrides)
        if audit_overrides is not None
        else get_boxscore_audit_overrides()
    )

    pbp_players = pbp_box_df.copy()
    if pbp_players.empty:
        pbp_players = pd.DataFrame(columns=["player_id", "team_id", *AUDIT_STATS])
    pbp_players["player_id"] = pd.to_numeric(pbp_players["player_id"], errors="coerce").fillna(0).astype(int)
    pbp_players["team_id"] = pd.to_numeric(pbp_players["team_id"], errors="coerce").fillna(0).astype(int)
    pbp_players = pbp_players[pbp_players["player_id"] != 0].copy()
    if "REB" not in pbp_players.columns:
        pbp_players["REB"] = pbp_players.get("OREB", 0.0) + pbp_players.get("DRB", 0.0)

    official_players = _prepare_official_boxscore(official_box_df)
    official_players = _apply_player_audit_overrides(
        game_id=game_id,
        pbp_players=pbp_players,
        official_players=official_players,
        audit_overrides=audit_overrides,
    )

    pbp_team = pbp_players.groupby("team_id")[AUDIT_STATS].sum() if not pbp_players.empty else pd.DataFrame(columns=AUDIT_STATS)
    official_team = official_players.groupby("team_id")[AUDIT_STATS].sum() if not official_players.empty else pd.DataFrame(columns=AUDIT_STATS)
    team_index = pbp_team.index.union(official_team.index)
    pbp_team = pbp_team.reindex(team_index, fill_value=0.0)
    official_team = official_team.reindex(team_index, fill_value=0.0)

    team_audit = pd.DataFrame(index=team_index)
    team_audit["game_id"] = game_id
    team_audit["team_id"] = team_index.astype(int)
    if not official_players.empty:
        team_names = official_players.groupby("team_id")["team_abbreviation"].first().to_dict()
        team_audit["team_abbreviation"] = [team_names.get(int(team_id), "") for team_id in team_index]
    else:
        team_audit["team_abbreviation"] = ""

    diff_columns: List[str] = []
    for stat in AUDIT_STATS:
        team_audit[f"PBP_{stat}"] = pbp_team[stat].to_numpy(dtype=float)
        team_audit[f"OFFICIAL_{stat}"] = official_team[stat].to_numpy(dtype=float)
        diff_col = f"DIFF_{stat}"
        team_audit[diff_col] = team_audit[f"PBP_{stat}"] - team_audit[f"OFFICIAL_{stat}"]
        diff_columns.append(diff_col)

    team_audit["max_abs_diff"] = team_audit[diff_columns].abs().max(axis=1)
    team_audit["has_mismatch"] = team_audit["max_abs_diff"] > 0
    team_audit = team_audit[TEAM_AUDIT_COLUMNS].reset_index(drop=True)

    merged_players = pbp_players.merge(
        official_players,
        on=["player_id", "team_id"],
        how="outer",
        suffixes=("_pbp", "_official"),
    )
    if merged_players.empty:
        player_mismatches = _empty_player_mismatch_df()
    else:
        merged_players["game_id"] = game_id
        if "player_name" not in merged_players.columns:
            merged_players["player_name"] = ""
        merged_players["player_name"] = merged_players["player_name"].fillna(
            merged_players["player_id"].map(player_name_map)
        ).fillna("")

        player_diff_columns: List[str] = []
        for stat in AUDIT_STATS:
            pbp_col = f"{stat}_pbp"
            official_col = f"{stat}_official"
            if pbp_col not in merged_players.columns:
                merged_players[pbp_col] = 0.0
            if official_col not in merged_players.columns:
                merged_players[official_col] = 0.0
            merged_players[pbp_col] = pd.to_numeric(merged_players[pbp_col], errors="coerce").fillna(0.0)
            merged_players[official_col] = pd.to_numeric(merged_players[official_col], errors="coerce").fillna(0.0)
            merged_players[f"PBP_{stat}"] = merged_players[pbp_col]
            merged_players[f"OFFICIAL_{stat}"] = merged_players[official_col]
            diff_col = f"DIFF_{stat}"
            merged_players[diff_col] = merged_players[pbp_col] - merged_players[official_col]
            player_diff_columns.append(diff_col)

        merged_players["max_abs_diff"] = merged_players[player_diff_columns].abs().max(axis=1)
        merged_players["has_mismatch"] = merged_players["max_abs_diff"] > 0
        player_mismatches = merged_players[merged_players["has_mismatch"]].copy()
        if player_mismatches.empty:
            player_mismatches = _empty_player_mismatch_df()
        else:
            player_mismatches = player_mismatches[PLAYER_MISMATCH_COLUMNS].reset_index(drop=True)

    summary = {
        "game_id": game_id,
        "team_rows": int(len(team_audit)),
        "team_rows_with_mismatch": int(team_audit["has_mismatch"].sum()) if not team_audit.empty else 0,
        "player_rows_with_mismatch": int(len(player_mismatches)),
    }
    return team_audit, player_mismatches, summary


def _normalize_team_audit_df(team_audit: pd.DataFrame | None) -> pd.DataFrame:
    if team_audit is None or team_audit.empty:
        return _empty_team_audit_df()
    return team_audit.reindex(columns=TEAM_AUDIT_COLUMNS, fill_value=0).copy()


def _normalize_player_mismatches_df(player_mismatches: pd.DataFrame | None) -> pd.DataFrame:
    if player_mismatches is None or player_mismatches.empty:
        return _empty_player_mismatch_df()
    return player_mismatches.reindex(columns=PLAYER_MISMATCH_COLUMNS, fill_value=0).copy()


def _normalize_audit_errors_df(audit_errors: pd.DataFrame | None) -> pd.DataFrame:
    if audit_errors is None or audit_errors.empty:
        return _empty_audit_error_df()
    return audit_errors.reindex(columns=AUDIT_ERROR_COLUMNS, fill_value="").copy()


def summarize_boxscore_audit(
    team_audit: pd.DataFrame | None,
    player_mismatches: pd.DataFrame | None,
    audit_errors: pd.DataFrame | None,
    season: int,
    games_requested: int | None = None,
) -> Dict[str, Any]:
    team_audit = _normalize_team_audit_df(team_audit)
    player_mismatches = _normalize_player_mismatches_df(player_mismatches)
    audit_errors = _normalize_audit_errors_df(audit_errors)

    if games_requested is None:
        observed_games = set()
        if not team_audit.empty:
            observed_games.update(team_audit["game_id"].astype(str))
        if not player_mismatches.empty:
            observed_games.update(player_mismatches["game_id"].astype(str))
        if not audit_errors.empty:
            observed_games.update(audit_errors["game_id"].astype(str))
        games_requested = len(observed_games)

    audit_failure_games = int(audit_errors["game_id"].astype(str).nunique()) if not audit_errors.empty else 0
    team_diff_counts = {
        stat: int((team_audit.get(f"DIFF_{stat}", pd.Series(dtype=float)).abs() > 0).sum())
        for stat in AUDIT_STATS
    }
    player_diff_counts = {
        stat: int((player_mismatches.get(f"DIFF_{stat}", pd.Series(dtype=float)).abs() > 0).sum())
        for stat in AUDIT_STATS
    }

    return {
        "season": season,
        "games_requested": int(games_requested),
        "games_audited": int(max(games_requested - audit_failure_games, 0)),
        "audit_failures": int(len(audit_errors)),
        "games_with_team_mismatch": int(team_audit.groupby("game_id")["has_mismatch"].any().sum()) if not team_audit.empty else 0,
        "team_rows_with_mismatch": int(team_audit["has_mismatch"].sum()) if not team_audit.empty else 0,
        "games_with_player_mismatch": int(player_mismatches["game_id"].nunique()) if not player_mismatches.empty else 0,
        "player_rows_with_mismatch": int(len(player_mismatches)),
        "team_mismatch_counts_by_stat": team_diff_counts,
        "player_mismatch_counts_by_stat": player_diff_counts,
    }


def write_boxscore_audit_outputs(
    team_audit: pd.DataFrame | None,
    player_mismatches: pd.DataFrame | None,
    audit_errors: pd.DataFrame | None,
    season: int,
    output_dir: Path,
    games_requested: int | None = None,
) -> Dict[str, Any]:
    team_audit = _normalize_team_audit_df(team_audit)
    player_mismatches = _normalize_player_mismatches_df(player_mismatches)
    audit_errors = _normalize_audit_errors_df(audit_errors)
    summary = summarize_boxscore_audit(
        team_audit=team_audit,
        player_mismatches=player_mismatches,
        audit_errors=audit_errors,
        season=season,
        games_requested=games_requested,
    )

    output_dir.mkdir(parents=True, exist_ok=True)
    team_audit.to_csv(output_dir / f"boxscore_team_audit_{season}.csv", index=False)
    player_mismatches.to_csv(output_dir / f"boxscore_player_mismatches_{season}.csv", index=False)
    audit_errors.to_csv(output_dir / f"boxscore_audit_errors_{season}.csv", index=False)
    (output_dir / f"boxscore_audit_summary_{season}.json").write_text(
        json.dumps(summary, indent=2),
        encoding="utf-8",
    )
    return summary


def _audit_single_game_worker(
    game_id: str,
    db_path: str,
    parquet_path: str,
    notebook_dump: str,
) -> Dict[str, Any]:
    game_id = _normalize_game_id(game_id)
    db_path_obj = Path(db_path)
    parquet_path_obj = Path(parquet_path)
    notebook_dump_obj = Path(notebook_dump)

    try:
        namespace = _get_audit_namespace(notebook_dump_obj, db_path_obj)
        namespace["DB_PATH"] = db_path_obj
        if "clear_event_stats_errors" in namespace:
            namespace["clear_event_stats_errors"]()

        game_df = _load_single_game_pbp(parquet_path_obj, game_id)
        if game_df.empty:
            return {
                "game_id": game_id,
                "team_rows": [],
                "player_rows": [],
                "error": f"No PBP rows found for {game_id}",
            }

        captured: Dict[str, Any] = {}
        original_generate = namespace["_generate_darko_hybrid_with_fetchers"]

        def wrapped_generate(*args: Any, **kwargs: Any) -> Tuple[pd.DataFrame, Any]:
            darko_df, possessions = original_generate(*args, **kwargs)
            captured["darko_df"] = darko_df
            captured["possessions"] = possessions
            return darko_df, possessions

        namespace["_generate_darko_hybrid_with_fetchers"] = wrapped_generate
        try:
            _, _, error_msg, _, _, _ = namespace["_process_single_game_worker"](
                game_id,
                game_df,
                str(db_path_obj),
                False,
                2,
                None,
                None,
            )
        finally:
            namespace["_generate_darko_hybrid_with_fetchers"] = original_generate

        if error_msg is not None:
            raise RuntimeError(error_msg)

        official_box = namespace["fetch_boxscore_stats"](game_id)
        darko_df = captured.get("darko_df", pd.DataFrame())
        possessions = captured.get("possessions")
        if possessions is None:
            raise RuntimeError(f"Audit worker did not capture possessions for {game_id}")

        pbp_box = build_pbp_boxscore_from_stat_rows(possessions.player_stats)
        player_name_map = {}
        if not darko_df.empty and {"NbaDotComID", "FullName"}.issubset(darko_df.columns):
            player_name_map = {
                int(player_id): full_name
                for player_id, full_name in zip(darko_df["NbaDotComID"], darko_df["FullName"])
                if int(player_id) != 0
            }
        elif not official_box.empty and {"PLAYER_ID", "PLAYER_NAME"}.issubset(official_box.columns):
            player_name_map = {
                int(player_id): player_name
                for player_id, player_name in zip(official_box["PLAYER_ID"], official_box["PLAYER_NAME"])
                if int(player_id) != 0
            }

        team_audit, player_mismatches, _ = build_game_boxscore_audit(
            game_id,
            pbp_box,
            official_box,
            player_name_map=player_name_map,
        )
        return {
            "game_id": game_id,
            "team_rows": team_audit.to_dict("records"),
            "player_rows": player_mismatches.to_dict("records"),
            "error": None,
        }
    except Exception as exc:
        return {
            "game_id": game_id,
            "team_rows": [],
            "player_rows": [],
            "error": str(exc),
        }


def run_boxscore_audit(
    game_ids: Iterable[str | int],
    season: int,
    output_dir: Path,
    db_path: Path,
    parquet_path: Path,
    notebook_dump: Path,
    max_workers: int = 4,
) -> Dict[str, Any]:
    unique_game_ids = sorted({_normalize_game_id(game_id) for game_id in game_ids})
    if not unique_game_ids:
        team_audit = _empty_team_audit_df()
        player_mismatches = _empty_player_mismatch_df()
        audit_errors = _empty_audit_error_df()
    else:
        results = Parallel(n_jobs=max_workers, backend="loky", verbose=10)(
            delayed(_audit_single_game_worker)(
                game_id,
                str(db_path),
                str(parquet_path),
                str(notebook_dump),
            )
            for game_id in unique_game_ids
        )

        team_rows = [row for result in results for row in result["team_rows"]]
        player_rows = [row for result in results for row in result["player_rows"]]
        error_rows = [
            {"game_id": result["game_id"], "error": result["error"]}
            for result in results
            if result["error"] is not None
        ]

        team_audit = pd.DataFrame(team_rows, columns=TEAM_AUDIT_COLUMNS)
        player_mismatches = pd.DataFrame(player_rows, columns=PLAYER_MISMATCH_COLUMNS)
        audit_errors = pd.DataFrame(error_rows, columns=AUDIT_ERROR_COLUMNS)

    return write_boxscore_audit_outputs(
        team_audit=team_audit,
        player_mismatches=player_mismatches,
        audit_errors=audit_errors,
        season=season,
        output_dir=output_dir,
        games_requested=len(unique_game_ids),
    )
