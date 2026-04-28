#!/usr/bin/env python
# coding: utf-8

# In[1]:


from __future__ import annotations

import json
import sqlite3
import threading
import zlib
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from joblib import Parallel, delayed
from historic_backfill.catalogs.boxscore_source_overrides import apply_boxscore_response_overrides
from historic_backfill.catalogs.pbp_row_overrides import apply_pbp_row_overrides
from historic_backfill.catalogs.pbp_stat_overrides import apply_pbp_stat_overrides
from historic_backfill.common.player_id_normalization import normalize_single_game_player_ids
from historic_backfill.common.team_event_normalization import normalize_single_game_team_events
from pbpstats.offline.row_overrides import normalize_game_id
from historic_backfill.audits.core.boxscore import (
    AUDIT_ERROR_COLUMNS,
    FOUL_KEYS as AUDIT_FOUL_KEYS,
    PLAYER_MISMATCH_COLUMNS,
    TEAM_AUDIT_COLUMNS,
    build_game_boxscore_audit,
    build_pbp_boxscore_from_stat_rows,
    write_boxscore_audit_outputs,
)

pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)
try:
    pd.set_option('use_inf_as_na', True)
except Exception:
    pass

# --- PBPSTATS IMPORTS ---
import pbpstats
from pbpstats.offline import get_possessions_from_df
from pbpstats.offline.processor import set_rebound_strict_mode
from pbpstats.resources.possessions.possessions import Possessions
from pbpstats.resources.enhanced_pbp import (
    FieldGoal,
    FreeThrow,
    Rebound,
    Turnover,
    Foul,
    Violation,
)

# ==============================================================================
# DATABASE CONFIG
# ==============================================================================

ROOT = Path(__file__).resolve().parents[1]
DATA_ROOT = ROOT / "data"
DB_PATH = DATA_ROOT / "nba_raw.db"
DEFAULT_PARQUET_PATH = DATA_ROOT / "playbyplayv2.parq"
_local = threading.local()

# Thread-safe error logging (for single-process mode)
_error_lock = threading.Lock()
_event_stats_errors: List[Dict[str, str]] = []

# Thread-safe log of fallback rebound deletions for later inspection
_rebound_fallback_lock = threading.Lock()
_rebound_fallback_deletions: List[Dict[str, Any]] = []


def get_conn() -> sqlite3.Connection:
    """Get a thread-local database connection."""
    if not hasattr(_local, "conn"):
        _local.conn = sqlite3.connect(DB_PATH, timeout=30)
    return _local.conn


def get_v3_cache() -> Dict[str, pd.DataFrame]:
    """Get thread-local v3 cache."""
    if not hasattr(_local, "v3_cache"):
        _local.v3_cache = {}
    return _local.v3_cache


def load_response(game_id: str, endpoint: str, team_id: Optional[int] = None) -> Optional[Dict]:
    """Load and decompress a response from the database."""
    conn = get_conn()
    if team_id is None:
        row = conn.execute(
            "SELECT data FROM raw_responses WHERE game_id=? AND endpoint=? AND team_id IS NULL",
            (game_id, endpoint)
        ).fetchone()
    else:
        row = conn.execute(
            "SELECT data FROM raw_responses WHERE game_id=? AND endpoint=? AND team_id=?",
            (game_id, endpoint, team_id)
        ).fetchone()
    if row:
        blob = row[0]
        try:
            data = json.loads(zlib.decompress(blob).decode())
        except (zlib.error, TypeError):
            if isinstance(blob, bytes):
                data = json.loads(blob.decode())
            else:
                data = json.loads(blob)
        if endpoint == "boxscore":
            return apply_boxscore_response_overrides(game_id, data)
        return data
    return None


# ==============================================================================
# LOCAL DB FETCHERS
# ==============================================================================

def fetch_boxscore_stats(game_id: str) -> pd.DataFrame:
    """Load boxscore from local DB."""
    game_id = str(game_id).zfill(10)
    data = load_response(game_id, "boxscore")
    
    if not data:
        print(f"[DB] No boxscore found for {game_id}")
        return pd.DataFrame()
    
    try:
        result_set = data["resultSets"][0]
        headers = result_set["headers"]
        rows = result_set["rowSet"]
        return pd.DataFrame(rows, columns=headers)
    except (KeyError, IndexError) as e:
        print(f"[DB] Boxscore parse error for {game_id}: {e}")
        return pd.DataFrame()


def fetch_game_summary(game_id: str) -> dict:
    """Load game summary from local DB."""
    game_id = str(game_id).zfill(10)
    data = load_response(game_id, "summary")
    
    if not data:
        print(f"[DB] No summary found for {game_id}")
        return {}
    
    try:
        if "resultSets" in data and len(data["resultSets"]) > 0:
            headers = data["resultSets"][0]["headers"]
            if data["resultSets"][0]["rowSet"]:
                row = data["resultSets"][0]["rowSet"][0]
                return dict(zip(headers, row))
        return {}
    except (KeyError, IndexError) as e:
        print(f"[DB] Summary parse error for {game_id}: {e}")
        return {}


def _resolve_game_team_ids(summary: dict, df_box: pd.DataFrame) -> Tuple[int, int]:
    """Resolve home/away team ids from summary first, then boxscore."""
    if summary.get('HOME_TEAM_ID') and summary.get('VISITOR_TEAM_ID'):
        return int(summary['HOME_TEAM_ID']), int(summary['VISITOR_TEAM_ID'])

    if not df_box.empty and 'TEAM_ID' in df_box.columns:
        team_ids = (
            pd.to_numeric(df_box['TEAM_ID'], errors='coerce')
            .dropna()
            .astype(int)
            .unique()
            .tolist()
        )
        if len(team_ids) >= 2:
            return int(team_ids[0]), int(team_ids[1])

    return 0, 0

def fetch_pbp_v3(game_id: str, use_cache: bool = True) -> pd.DataFrame:
    """Load playbyplayv3 from local DB."""
    game_id = str(game_id).zfill(10)
    
    cache = get_v3_cache()
    if use_cache and game_id in cache:
        return cache[game_id]
    
    data = load_response(game_id, "pbpv3")
    
    if not data:
        print(f"[DB] No pbpv3 found for {game_id}")
        return pd.DataFrame()
    
    try:
        actions = data.get("game", {}).get("actions", [])
        df = pd.DataFrame(actions)
        
        if use_cache:
            cache[game_id] = df
        
        return df
    except Exception as e:
        print(f"[DB] PBPv3 parse error for {game_id}: {e}")
        return pd.DataFrame()


def clear_v3_cache(game_id: Optional[str] = None) -> None:
    """Clear thread-local v3 cache for a specific game or all games."""
    cache = get_v3_cache()
    if game_id is None:
        cache.clear()
    else:
        cache.pop(str(game_id).zfill(10), None)


def log_event_stats_error(game_id: str, event_repr: str, error_msg: str) -> None:
    """Log an event_stats error for later export (thread-safe)."""
    with _error_lock:
        _event_stats_errors.append({
            "game_id": str(game_id).zfill(10),
            "event": event_repr,
            "error": error_msg,
        })


def export_event_stats_errors(filepath: str = "event_stats_errors.csv") -> None:
    """Export all logged event_stats errors to CSV."""
    if not _event_stats_errors:
        print("No event_stats errors to export.")
        return
    df = pd.DataFrame(_event_stats_errors)
    df.to_csv(filepath, index=False)
    print(f"Exported {len(_event_stats_errors)} event_stats errors to {filepath}")


def clear_event_stats_errors() -> None:
    """Clear the event_stats error log."""
    _event_stats_errors.clear()


def export_rebound_fallback_deletions(filepath: str = "rebound_fallback_deletions.csv") -> None:
    """Export logged fallback rebound deletions to CSV, if any."""
    if not _rebound_fallback_deletions:
        print("No rebound fallback deletions to export.")
        return
    df = pd.DataFrame(_rebound_fallback_deletions)
    df.to_csv(filepath, index=False)
    print(f"Exported {len(_rebound_fallback_deletions)} rebound fallback deletions to {filepath}")


def clear_rebound_fallback_deletions() -> None:
    """Clear the rebound fallback deletion log."""
    with _rebound_fallback_lock:
        _rebound_fallback_deletions.clear()


# ==============================================================================
# VALIDATION OVERRIDES
# ==============================================================================

_validation_overrides: Dict[str, Dict] = {}


def load_validation_overrides(filepath: str = "validation_overrides.csv") -> Dict[str, Dict]:
    """
    Load manual validation overrides from CSV.
    
    CSV format:
        game_id,action,tolerance,notes
        0029900712,allow,5,Fortson missing from boxscore but present in PBP
        0020100543,skip,,Known bad data - skip validation entirely
    
    Actions:
        - allow: use custom tolerance for this game
        - skip: skip validation entirely (still process the game)
    """
    overrides: Dict[str, Dict] = {}
    path = Path(filepath)
    if not path.exists():
        return overrides
    
    df = pd.read_csv(path)
    for _, row in df.iterrows():
        raw_gid = row["game_id"]
        if pd.isna(raw_gid):
            continue
        game_id = str(int(float(raw_gid))).zfill(10)
        overrides[game_id] = {
            "action": row.get("action", "allow"),
            "tolerance": int(row["tolerance"]) if pd.notna(row.get("tolerance")) else None,
            "notes": row.get("notes", ""),
        }
    print(f"[OVERRIDES] Loaded {len(overrides)} validation overrides from {filepath}")
    return overrides


def set_validation_overrides(overrides: Dict[str, Dict]) -> None:
    """Set the global validation overrides."""
    global _validation_overrides
    _validation_overrides = overrides


def get_validation_overrides() -> Dict[str, Dict]:
    """Get the current validation overrides."""
    return _validation_overrides


# ==============================================================================
# LOCAL CSV & PBP LOADER
# ==============================================================================

def load_shufinskiy_pbp_df(csv_path: str) -> pd.DataFrame:
    """Legacy CSV loader - kept for backwards compatibility."""
    dtype_map = {
        "game_id": str,
        "period": int,
        "eventnum": int,
        "player1_id": "Int64",
        "player2_id": "Int64",
        "player3_id": "Int64",
        "player1_team_id": "Int64",
        "player2_team_id": "Int64",
        "player3_team_id": "Int64",
    }
    print(f"Loading local CSV {csv_path}...")
    df = pd.read_csv(csv_path, dtype=dtype_map)

    df.columns = [c.upper() for c in df.columns]

    if "WCTIMESTRING" not in df.columns:
        df["WCTIMESTRING"] = "00:00 AM"

    text_cols = [
        "HOMEDESCRIPTION",
        "VISITORDESCRIPTION",
        "NEUTRALSITEDESCRIPTION",
        "PLAYER1_NAME",
        "PLAYER2_NAME",
        "PLAYER3_NAME",
    ]
    for col in text_cols:
        if col in df.columns:
            df[col] = df[col].fillna("")

    if "GAME_ID" in df.columns:
        df["GAME_ID"] = df["GAME_ID"].astype(str).str.zfill(10)

    return df


def load_pbp_from_parquet(
    parquet_path: str = "playbyplayv2.parq",
    season: Optional[int] = None,
) -> pd.DataFrame:
    """
    Load play-by-play data from parquet file, optionally filtering by season.
    Uses predicate pushdown for efficient filtering.
    """
    print(f"Loading parquet {parquet_path}...")
    
    if season is not None:
        print(f"Filtering for season {season} at read time...")
        try:
            df = pd.read_parquet(parquet_path, filters=[("SEASON", "==", season)])
        except Exception:
            print(f"Filter pushdown failed, loading full file...")
            df = pd.read_parquet(parquet_path)
            season_columns = {
                str(column).upper(): column
                for column in df.columns
            }
            season_column = season_columns.get("SEASON")
            if season_column is not None:
                df = df[df[season_column] == season].copy()
        print(f"Found {len(df)} rows for season {season}")
    else:
        df = pd.read_parquet(parquet_path)
    
    df.columns = [c.upper() for c in df.columns]
    
    if "WCTIMESTRING" not in df.columns:
        df["WCTIMESTRING"] = "00:00 AM"
    
    text_cols = [
        "HOMEDESCRIPTION",
        "VISITORDESCRIPTION",
        "NEUTRALSITEDESCRIPTION",
        "PLAYER1_NAME",
        "PLAYER2_NAME",
        "PLAYER3_NAME",
    ]
    for col in text_cols:
        if col in df.columns:
            df[col] = df[col].fillna("")
    
    if "GAME_ID" in df.columns:
        df["GAME_ID"] = df["GAME_ID"].map(normalize_game_id)
    
    int_cols = ["EVENTNUM", "EVENTMSGTYPE", "EVENTMSGACTIONTYPE", "PERIOD"]
    for col in int_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)
    
    nullable_int_cols = [
        "PLAYER1_ID", "PLAYER2_ID", "PLAYER3_ID",
        "PLAYER1_TEAM_ID", "PLAYER2_TEAM_ID", "PLAYER3_TEAM_ID",
    ]
    for col in nullable_int_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
    
    return df


# ==============================================================================
# SAFE OFFENSE TEAM ID & POSSESSIONS USED COMPUTATION
# ==============================================================================

TEAM_STAT_ID = int(pbpstats.TEAM_STAT_PLAYER_ID)


def _safe_get_offense_team_id(poss, end_event) -> int:
    try:
        return int(end_event.get_offense_team_id())
    except Exception:
        pass

    try:
        if isinstance(end_event, (FieldGoal, FreeThrow, Turnover)):
            t = getattr(end_event, "team_id", None)
            if t is not None and t != 0:
                return int(t)

        if isinstance(end_event, Rebound):
            try:
                ms = end_event.missed_shot
                t = getattr(ms, "team_id", None)
                if t is not None and t != 0:
                    return int(t)
            except Exception:
                pass
    except Exception:
        pass

    for ev in reversed(getattr(poss, "events", [])):
        t = getattr(ev, "team_id", None)
        if t is not None and t != 0:
            try:
                return int(t)
            except Exception:
                return 0

    return 0


def _iter_enhanced_events_from_possessions(possessions_resource) -> List[object]:
    seen_ids = set()
    for poss in possessions_resource.items:
        for ev in poss.events:
            ev_id = id(ev)
            if ev_id in seen_ids:
                continue
            seen_ids.add(ev_id)
            yield ev


def _compute_possessions_used_exact(possessions_resource) -> Dict[tuple, float]:
    poss_used = defaultdict(float)
    for poss in possessions_resource.items:
        events = poss.events
        if not events:
            continue
        end_event = events[-1]
        if not getattr(end_event, "count_as_possession", False):
            continue

        offense_team_id = _safe_get_offense_team_id(poss, end_event)

        user_pid = None
        if isinstance(end_event, Turnover) and not end_event.is_no_turnover:
            user_pid = getattr(end_event, "player1_id", TEAM_STAT_ID)
        elif isinstance(end_event, Rebound):
            try:
                missed = end_event.missed_shot
                user_pid = getattr(missed, "player1_id", TEAM_STAT_ID)
            except Exception:
                user_pid = TEAM_STAT_ID
        elif isinstance(end_event, FieldGoal):
            user_pid = getattr(end_event, "player1_id", TEAM_STAT_ID)
        elif isinstance(end_event, FreeThrow):
            user_pid = getattr(end_event, "player1_id", TEAM_STAT_ID)
        elif isinstance(end_event, Foul):
            if getattr(end_event, "team_id", None) == offense_team_id:
                user_pid = getattr(end_event, "player1_id", TEAM_STAT_ID)
        elif isinstance(end_event, Violation):
            if getattr(end_event, "team_id", None) == offense_team_id:
                user_pid = getattr(end_event, "player1_id", TEAM_STAT_ID)
            else:
                user_pid = TEAM_STAT_ID
        else:
            user_pid = TEAM_STAT_ID

        if user_pid is None:
            user_pid = TEAM_STAT_ID

        poss_used[(int(user_pid), int(offense_team_id))] += 1.0

    return poss_used


def _compute_ts_attempts_exact(possessions_resource) -> Dict[tuple, float]:
    ts_attempts = defaultdict(float)
    for ev in _iter_enhanced_events_from_possessions(possessions_resource):
        team_id = getattr(ev, "team_id", None)
        if team_id is None:
            continue
        if isinstance(ev, FieldGoal):
            pid = getattr(ev, "player1_id", TEAM_STAT_ID)
            ts_attempts[(int(pid), int(team_id))] += 1.0
        elif isinstance(ev, FreeThrow):
            if not (ev.is_first_ft or ev.is_ft_1pt or ev.is_ft_2pt or ev.is_ft_3pt):
                continue
            ft_type = (ev.free_throw_type or "").lower()
            if "and 1" in ft_type or "shooting foul" in ft_type or "flagrant" in ft_type or "technical" in ft_type:
                continue
            pid = getattr(ev, "player1_id", TEAM_STAT_ID)
            ts_attempts[(int(pid), int(team_id))] += 1.0
    return ts_attempts


def _counts_to_series(counts_dict, index, name) -> pd.Series:
    vals = [float(counts_dict.get((int(pid), int(tid)), 0.0)) for pid, tid in index]
    return pd.Series(vals, index=index, name=name, dtype=float)


def _check_and_log_event_stats_errors(possessions_resource, game_id: str) -> int:
    """Pre-check all events for event_stats errors and log them."""
    error_count = 0
    for poss in possessions_resource.items:
        for ev in poss.events:
            try:
                _ = ev.event_stats
            except Exception as e:
                error_count += 1
                log_event_stats_error(
                    game_id=game_id,
                    event_repr=repr(ev),
                    error_msg=str(e),
                )
    return error_count


def _build_darko_stat_map() -> Dict[str, List[str]]:
    AT_RIM, SMR, LMR, ARC3, CORNER3 = pbpstats.AT_RIM_STRING, pbpstats.SHORT_MID_RANGE_STRING, pbpstats.LONG_MID_RANGE_STRING, pbpstats.ARC_3_STRING, pbpstats.CORNER_3_STRING
    OFF, DEF = pbpstats.OFFENSIVE_ABBREVIATION_PREFIX, pbpstats.DEFENSIVE_ABBREVIATION_PREFIX
    REB, REB_OPP = pbpstats.REBOUNDS_STRING, pbpstats.REBOUND_OPPORTUNITIES_STRING
    FT = pbpstats.FREE_THROW_STRING
    all_shot_types = [AT_RIM, SMR, LMR, ARC3, CORNER3]

    m = {}
    m["POSS_OFF"] = [pbpstats.OFFENSIVE_POSSESSION_STRING]
    m["POSS_DEF"] = [pbpstats.DEFENSIVE_POSSESSION_STRING]
    m["Seconds_Off"] = [pbpstats.SECONDS_PLAYED_OFFENSE_STRING]
    m["Seconds_Def"] = [pbpstats.SECONDS_PLAYED_DEFENSE_STRING]

    for bucket in [pbpstats.DARKO_0_3FT_STRING, pbpstats.DARKO_4_9FT_STRING, pbpstats.DARKO_10_17FT_STRING, pbpstats.DARKO_18_23FT_STRING]:
        prefix = bucket.replace("to", "_").replace("Ft", "ft")
        m[f"{prefix}_FGM"] = [f"Darko_{bucket}_Made"]
        m[f"{prefix}_FGA"] = [f"Darko_{bucket}_Att"]
        m[f"{prefix}_FGM_AST"] = [f"{pbpstats.ASSISTED_STRING}Darko_{bucket}"]
        m[f"{prefix}_FGM_UNAST"] = [f"{pbpstats.UNASSISTED_STRING}Darko_{bucket}"]
        m[f"AST_{prefix}"] = [f"Darko_{bucket}_Assists"]

    m["FGM_UNAST"] = m["0_3ft_FGM_UNAST"] + m["4_9ft_FGM_UNAST"] + m["10_17ft_FGM_UNAST"] + m["18_23ft_FGM_UNAST"]
    m["3PM_UNAST"] = [f"{pbpstats.UNASSISTED_STRING}{ARC3}", f"{pbpstats.UNASSISTED_STRING}{CORNER3}"]
    m["3PA_UNAST"] = m["3PM_UNAST"] + [f"{pbpstats.MISSED_STRING}{ARC3}", f"{pbpstats.MISSED_STRING}{CORNER3}", f"{ARC3}{pbpstats.BLOCKED_STRING}", f"{CORNER3}{pbpstats.BLOCKED_STRING}"]
    m["AST_3P"] = [f"{ARC3}{pbpstats.ASSISTS_STRING}", f"{CORNER3}{pbpstats.ASSISTS_STRING}"]
    m["OnCourt_Opp_FGA"] = [pbpstats.OPP_FGA_STRING]
    m["OnCourt_Opp_3p_Att"] = [pbpstats.OPP_3PA_STRING]
    m["OnCourt_Opp_3p_Made"] = [pbpstats.OPP_3PM_STRING]
    m["OnCourt_Opp_FT_Att"] = [pbpstats.OPP_FTA_STRING]
    m["OnCourt_Opp_FT_Made"] = [pbpstats.OPP_FTM_STRING]
    m["OnCourt_Opp_Points"] = [pbpstats.OPPONENT_POINTS]
    m["OnCourt_Team_FGA"] = [pbpstats.TEAM_FGA_STRING]
    m["OnCourt_Team_FGM"] = [pbpstats.TEAM_FGM_STRING]
    m["OnCourt_Team_3p_Made"] = [pbpstats.TEAM_3PM_STRING]
    m["OnCourt_Team_3p_Att"] = [pbpstats.TEAM_3PA_STRING]
    m["OnCourt_Team_FT_Made"] = [pbpstats.TEAM_FTM_STRING]
    m["OnCourt_Team_FT_Att"] = [pbpstats.TEAM_FTA_STRING]
    m["OnCourt_For_OREB_FGA"] = [pbpstats.ON_FLOOR_OFFENSIVE_REBOUND_FGA_STRING]
    m["OnCourt_For_DREB_FGA"] = [pbpstats.ON_FLOOR_DEFENSIVE_REBOUND_FGA_STRING]

    def get_keys(prefix, suffix):
        k = []
        for st in all_shot_types:
            k.append(f"{st}{prefix}{suffix}")
            k.append(f"{st}{pbpstats.BLOCKED_STRING}{prefix}{suffix}")
        return k

    m["OREB_FGA"] = get_keys(OFF, REB)
    m["OREB_FT"] = [f"{FT}{OFF}{REB}"]
    m["OREB"] = m["OREB_FGA"] + m["OREB_FT"]
    m["DREB_FGA"] = get_keys(DEF, REB)
    m["DREB_FT"] = [f"{FT}{DEF}{REB}"]
    m["DRB"] = m["DREB_FGA"] + m["DREB_FT"]
    m["_OREB_opp_FGA"] = get_keys(OFF, REB_OPP)
    m["_OREB_opp_FT"] = [f"{FT}{OFF}{REB_OPP}"]
    m["_DRB_opp_FGA"] = get_keys(DEF, REB_OPP)
    m["_DRB_opp_FT"] = [f"{FT}{DEF}{REB_OPP}"]
    m["_OREB_opp_total"] = m["_OREB_opp_FGA"] + m["_OREB_opp_FT"]
    m["_DRB_opp_total"] = m["_DRB_opp_FGA"] + m["_DRB_opp_FT"]
    m["TOV_Live"] = [pbpstats.LOST_BALL_TURNOVER_STRING, pbpstats.BAD_PASS_TURNOVER_STRING]
    m["TOV_Dead"] = [pbpstats.DEADBALL_TURNOVERS_STRING]
    m["TOV"] = m["TOV_Live"] + m["TOV_Dead"]
    m["STL"] = [pbpstats.LOST_BALL_STEAL_STRING, pbpstats.BAD_PASS_STEAL_STRING]
    m["BLK_Team"] = [f"{pbpstats.BLOCKED_STRING}{st}Recovered" for st in all_shot_types]
    m["_BLK_Total_PBP"] = [f"{pbpstats.BLOCKED_STRING}{st}" for st in all_shot_types]
    m["TM_BLK_OnCourt"] = ["OnCourtTeamBlock"]

    FOUL_TYPES = list(AUDIT_FOUL_KEYS)
    m["PF"] = FOUL_TYPES
    m["PF_DRAWN"] = [f"{ft}{pbpstats.FOULS_DRAWN_TYPE_STRING}" for ft in FOUL_TYPES]
    m["PF_Loose"] = [pbpstats.LOOSE_BALL_FOUL_TYPE_STRING]
    m["CHRG"] = [pbpstats.CHARGE_FOUL_TYPE_STRING + pbpstats.FOULS_DRAWN_TYPE_STRING]
    m["TECH"] = [pbpstats.TECHNICAL_FOULS_COMMITTED_STRING]
    m["FLAGRANT"] = [pbpstats.FLAGRANT_1_FOUL_TYPE_STRING, pbpstats.FLAGRANT_2_FOUL_TYPE_STRING]
    m["Goaltends"] = [pbpstats.DEFENSIVE_GOALTENDING_STRING]
    m["AndOnes"] = [pbpstats.THREE_POINT_AND1_FREE_THROW_STRING, pbpstats.TWO_POINT_AND1_FREE_THROW_STRING]
    m["Plus_Minus_raw"] = [pbpstats.PLUS_MINUS_STRING]
    return m


def _build_game_boxscore_audit_rows(
    game_id: str,
    stat_rows: List[Dict[str, Any]],
    official_box: pd.DataFrame,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, str]]]:
    if official_box.empty:
        return [], [], [{"game_id": str(game_id).zfill(10), "error": "Official boxscore unavailable"}]

    player_name_map: Dict[int, str] = {}
    if {"PLAYER_ID", "PLAYER_NAME"}.issubset(official_box.columns):
        player_ids = pd.to_numeric(official_box["PLAYER_ID"], errors="coerce").fillna(0).astype(int)
        player_names = official_box["PLAYER_NAME"].fillna("").astype(str)
        player_name_map = {
            int(player_id): player_name
            for player_id, player_name in zip(player_ids, player_names)
            if int(player_id) != 0
        }

    pbp_box = build_pbp_boxscore_from_stat_rows(stat_rows)
    team_audit, player_mismatches, _ = build_game_boxscore_audit(
        game_id,
        pbp_box,
        official_box,
        player_name_map=player_name_map,
    )
    return (
        team_audit.to_dict("records"),
        player_mismatches.to_dict("records"),
        [],
    )


# ==============================================================================
# HYBRID GENERATOR (LOCAL DB VERSION)
# ==============================================================================

def generate_darko_hybrid(
    game_id: str,
    season_pbp_df: pd.DataFrame,
    one_way_possessions: bool = False
) -> Tuple[pd.DataFrame, Possessions]:
    """Returns (darko_df, possessions) tuple using local DB for boxscore/summary."""

    df_box = fetch_boxscore_stats(game_id)
    summary = fetch_game_summary(game_id)

    if df_box.empty:
        print(f"Warning: Could not load boxscore stats for {game_id}.")
    else:
        df_box['PLAYER_ID'] = pd.to_numeric(df_box['PLAYER_ID'], errors='coerce').fillna(0).astype(int)
        df_box['TEAM_ID'] = pd.to_numeric(df_box['TEAM_ID'], errors='coerce').fillna(0).astype(int)

        pid_to_name = dict(zip(df_box['PLAYER_ID'], df_box['PLAYER_NAME']))
        df_box['IS_STARTER'] = df_box['START_POSITION'].apply(lambda x: 1 if x and str(x).strip() != '' else 0)
        pid_to_start = dict(zip(df_box['PLAYER_ID'], df_box['IS_STARTER']))
        pid_to_pos = dict(zip(df_box['PLAYER_ID'], df_box['START_POSITION']))

    single_game_df = season_pbp_df[season_pbp_df['GAME_ID'] == str(game_id).zfill(10)]
    if single_game_df.empty:
        raise ValueError(f"Game ID {game_id} not found in provided Local DataFrame.")

    h_tm_id, v_tm_id = _resolve_game_team_ids(summary, df_box)
    if h_tm_id and v_tm_id:
        boxscore_player_ids = df_box['PLAYER_ID'].tolist() if not df_box.empty else None
        single_game_df = normalize_single_game_team_events(
            single_game_df,
            home_team_id=h_tm_id,
            away_team_id=v_tm_id,
            boxscore_player_ids=boxscore_player_ids,
        )
    if not df_box.empty:
        single_game_df = normalize_single_game_player_ids(
            single_game_df,
            official_boxscore=df_box,
        )
    single_game_df = apply_pbp_row_overrides(single_game_df)

    # Use pbpstats.offline.get_possessions_from_df
    possessions = get_possessions_from_df(
        single_game_df,
        fetch_pbp_v3_fn=fetch_pbp_v3,
        rebound_deletions_list=_rebound_fallback_deletions,
    )
    
    error_count = _check_and_log_event_stats_errors(possessions, game_id)
    if error_count > 0:
        print(f"[WARNING] Game {game_id}: {error_count} event_stats errors logged")

    poss_used_counts = _compute_possessions_used_exact(possessions)
    ts_attempt_counts = _compute_ts_attempts_exact(possessions)

    poss_data = apply_pbp_stat_overrides(game_id, possessions.player_stats)
    possessions.manual_player_stats = poss_data
    if not poss_data:
        raise ValueError(f"No possession data calculated for {game_id}")

    df_poss = pd.DataFrame(poss_data)
    df_poss["player_id"] = df_poss["player_id"].astype(int)
    df_poss["team_id"] = df_poss["team_id"].astype(int)

    stats_wide = df_poss.pivot_table(
        index=["player_id", "team_id"],
        columns="stat_key",
        values="stat_value",
        aggfunc="sum",
        fill_value=0,
    )

    darko = pd.DataFrame(index=stats_wide.index)

    box_stat_map = {
        "PTS": "PTS", "AST": "AST", "STL": "STL", "BLK": "BLK", "TOV": "TO",
        "PF": "PF", "FGA": "FGA", "FGM": "FGM", "3PA": "FG3A", "3PM": "FG3M",
        "FTA": "FTA", "FTM": "FTM", "OREB": "OREB", "DRB": "DREB", "TRB": "REB"
    }

    if not df_box.empty:
        df_box_indexed = df_box.set_index(['PLAYER_ID', 'TEAM_ID'])
        df_box_indexed.index = df_box_indexed.index.set_levels([
            df_box_indexed.index.levels[0].astype(int),
            df_box_indexed.index.levels[1].astype(int)
        ])

        for darko_col, box_col in box_stat_map.items():
            if box_col in df_box_indexed.columns:
                darko[darko_col] = df_box_indexed[box_col].reindex(darko.index).fillna(0)

    stat_map = _build_darko_stat_map()
    all_indices = sorted(set(stats_wide.index).union(darko.index))
    stats_wide = stats_wide.reindex(all_indices).fillna(0)
    darko = darko.reindex(all_indices).fillna(0)

    base_stats: Dict[str, pd.Series] = {}
    for col_name, pbp_keys in stat_map.items():
        if col_name in darko.columns and col_name in box_stat_map:
            continue

        valid_keys = [k for k in pbp_keys if k in stats_wide.columns]
        if valid_keys:
            base_stats[col_name] = stats_wide[valid_keys].sum(axis=1)
        else:
            base_stats[col_name] = np.zeros(len(stats_wide))

    df_pbp_stats = pd.DataFrame(base_stats, index=darko.index)
    darko = pd.concat([darko, df_pbp_stats], axis=1)

    def get_meta(pid, mapping, default):
        return mapping.get(pid, default)

    if not df_box.empty:
        darko["FullName"] = [get_meta(idx[0], pid_to_name, "Unknown") for idx in darko.index]
        darko["Position"] = [get_meta(idx[0], pid_to_pos, "N/A") for idx in darko.index]
        darko["Starts"] = [get_meta(idx[0], pid_to_start, 0) for idx in darko.index]
    else:
        darko["FullName"] = "Unknown"
        darko["Position"] = "N/A"
        darko["Starts"] = 0

    is_team_stat = darko.index.get_level_values("player_id") == 0
    team_ids_str = darko.index.get_level_values("team_id").astype(str)
    darko.loc[is_team_stat, "FullName"] = "Team Stats (" + team_ids_str[is_team_stat] + ")"

    darko["NbaDotComID"] = darko.index.get_level_values("player_id")
    darko["Team_SingleGame"] = darko.index.get_level_values("team_id")
    darko["Game_SingleGame"] = int(game_id)
    darko["Source"] = "Local_DB"

    season_start_year = int(("19" if str(game_id)[3] == "9" else "20") + str(game_id)[3:5])
    darko["Year"] = season_start_year + 1
    darko["season"] = darko["Year"]

    if summary.get('GAME_DATE_EST'):
        darko["Date"] = summary['GAME_DATE_EST'].split('T')[0]
    else:
        darko["Date"] = f"01/01/{season_start_year+1}"

    if not (h_tm_id and v_tm_id) and not df_box.empty:
        team_ids = df_box['TEAM_ID'].unique()
        h_tm_id, v_tm_id = (team_ids[0], team_ids[1]) if len(team_ids) >= 2 else (0, 0)
    elif not (h_tm_id and v_tm_id):
        team_ids = darko.index.get_level_values('team_id').unique()
        h_tm_id, v_tm_id = (team_ids[0], team_ids[1]) if len(team_ids) >= 2 else (0, 0)

    darko["h_tm_id"] = h_tm_id
    darko["v_tm_id"] = v_tm_id
    darko["home_fl"] = (darko["Team_SingleGame"] == h_tm_id).astype(int)

    darko["Minutes"] = (darko["Seconds_Off"] + darko["Seconds_Def"]) / 60.0
    darko["POSS"] = (darko["POSS_OFF"] + darko["POSS_DEF"]) / (2.0 if not one_way_possessions else 1.0)

    darko["TSAttempts"] = _counts_to_series(ts_attempt_counts, darko.index, "TSAttempts")
    darko["PossessionsUsed"] = _counts_to_series(poss_used_counts, darko.index, "PossessionsUsed")

    if "BLK" not in darko.columns:
        darko["BLK"] = darko.get("_BLK_Total_PBP", 0)

    total_blk_pbp = darko.get("_BLK_Total_PBP", darko["BLK"])
    darko["BLK_Opp"] = np.maximum(total_blk_pbp - darko.get("BLK_Team", 0), 0)

    derived = {}
    derived["TSpct"] = np.where(darko["TSAttempts"] > 0, darko["PTS"] / (2.0 * darko["TSAttempts"]), 0.0)
    derived["USG"] = np.where(darko["POSS_OFF"] > 0, darko["PossessionsUsed"] / darko["POSS_OFF"], 0.0)

    pace_denom = 2.0 if not one_way_possessions else 1.0
    derived["Pace"] = np.where(darko["Minutes"] > 0, (48.0 * darko["POSS"]) / darko["Minutes"] / pace_denom, 0.0)

    derived["G"] = np.where(darko["Minutes"] > 0, 1, 0)
    derived["DNP"] = np.where(darko["Minutes"] == 0, 1, 0)
    derived["Inactive"] = 0

    reb_configs = [
        ("ORBpct", "OREB", "_OREB_opp_total"), ("DRBPct", "DRB", "_DRB_opp_total"),
        ("OREBPct_FGA", "OREB_FGA", "_OREB_opp_FGA"), ("OREBPct_FT", "OREB_FT", "_OREB_opp_FT"),
        ("DRBPct_FGA", "DREB_FGA", "_DRB_opp_FGA"), ("DRBPct_FT", "DREB_FT", "_DRB_opp_FT"),
    ]
    for out, num, den in reb_configs:
        if num in darko.columns and den in darko.columns:
            derived[out] = np.where(darko[den] > 0, darko[num] / darko[den], 0.0)
        else:
            derived[out] = 0.0

    derived["Plus_Minus"] = darko.get("Plus_Minus_raw", 0)
    team_fgm = darko.get("OnCourt_Team_FGM", 0)
    team_3pm = darko.get("OnCourt_Team_3p_Made", 0)
    team_ftm = darko.get("OnCourt_Team_FT_Made", 0)
    team_pts = 2.0 * team_fgm + team_3pm + team_ftm
    derived["OnCourt_Team_Points"] = np.where((team_pts == 0) & (darko["OnCourt_Opp_Points"] > 0), darko["OnCourt_Opp_Points"] + derived["Plus_Minus"], team_pts)

    per_100_cols = ["PTS", "AST", "TOV", "FGM", "FGA", "3PM", "3PA", "OREB", "DRB", "BLK", "STL", "FTM", "FTA", "CHRG", "BLK_Opp", "BLK_Team", "Goaltends", "TOV_Live", "TOV_Dead", "AndOnes"]
    def_stats = {"DRB", "BLK", "STL", "CHRG", "BLK_Opp", "BLK_Team", "Goaltends"}
    for col in per_100_cols:
        if col not in darko.columns:
            continue
        denom = "POSS_DEF" if col in def_stats else "POSS_OFF"
        out = f"{col}_100p"
        if col == "DRB":
            out = "DREB_100p"
        if col == "AndOnes":
            out = "AndOne_100p"
        derived[out] = np.where(darko[denom] > 0, (darko[col] / darko[denom]) * 100.0, 0.0)

    shooting_rates = [("FGPct", "FGM", "FGA"), ("3PPct", "3PM", "3PA"), ("FT%", "FTM", "FTA"), ("FTR_Att", "FTA", "FGA"), ("FTR_Made", "FTM", "FGA"), ("TOVpct", "TOV", "PossessionsUsed"), ("STLpct", "STL", "POSS_DEF")]
    for out, num, den in shooting_rates:
        if num in darko.columns and den in darko.columns:
            derived[out] = np.where(darko[den] > 0, darko[num] / darko[den], 0.0)
        else:
            derived[out] = 0.0

    opp_2pa = darko["OnCourt_Opp_FGA"] - darko["OnCourt_Opp_3p_Att"]
    opp_2pa = np.where(opp_2pa < 0, 0, opp_2pa)
    derived["BLKPct"] = np.where(opp_2pa > 0, darko["BLK"] / opp_2pa, 0.0)

    denom_ast = team_fgm - darko["FGM"]
    derived["ASTpct"] = np.where(denom_ast > 0, darko["AST"] / denom_ast, 0.0)

    buckets = ["0_3ft", "4_9ft", "10_17ft", "18_23ft"]
    for bucket in buckets:
        fgm, fga = darko.get(f"{bucket}_FGM", 0), darko.get(f"{bucket}_FGA", 0)
        fgm_ast, fgm_unast = darko.get(f"{bucket}_FGM_AST", 0), darko.get(f"{bucket}_FGM_UNAST", 0)
        derived[f"{bucket}_FGPct"] = np.where(fga > 0, fgm / fga, 0.0)
        derived[f"{bucket}_FGM_100p"] = np.where(darko["POSS_OFF"] > 0, (fgm / darko["POSS_OFF"]) * 100.0, 0.0)
        derived[f"{bucket}_FGA_100p"] = np.where(darko["POSS_OFF"] > 0, (fga / darko["POSS_OFF"]) * 100.0, 0.0)
        fga_unast = fga - fgm_ast
        derived[f"{bucket}_FGA_UNAST"] = fga_unast
        derived[f"{bucket}_FG_UNAST_Pct"] = np.where(fga_unast > 0, fgm_unast / fga_unast, 0.0)
        derived[f"{bucket}_FGM_100p_UNAST"] = np.where(darko["POSS_OFF"] > 0, (fgm_unast / darko["POSS_OFF"]) * 100.0, 0.0)
        derived[f"{bucket}_FGA_UNAST_100p"] = np.where(darko["POSS_OFF"] > 0, (fga_unast / darko["POSS_OFF"]) * 100.0, 0.0)
        derived[f"{bucket}_FGM_UNAST_100p"] = derived[f"{bucket}_FGM_100p_UNAST"]
        if bucket == "0_3ft":
            derived["0_3ft_FGA_100p_UNAST"] = derived["0_3ft_FGA_UNAST_100p"]
        derived[f"{bucket}_FGM_100p_AST"] = np.where(darko["POSS_OFF"] > 0, (fgm_ast / darko["POSS_OFF"]) * 100.0, 0.0)
        ast_col = f"AST_{bucket}"
        if ast_col in darko.columns:
            derived[f"{ast_col}_100p"] = np.where(darko["POSS_OFF"] > 0, (darko[ast_col] / darko["POSS_OFF"]) * 100.0, 0.0)

    fga_unast_global = darko["FGA"] - (darko["FGM"] - darko.get("FGM_UNAST", 0))
    derived["FGA_UNAST"] = fga_unast_global
    derived["FG_UNAST_Pct"] = np.where(fga_unast_global > 0, darko.get("FGM_UNAST", 0) / fga_unast_global, 0.0)
    derived["FGM_UNAST_100p"] = np.where(darko["POSS_OFF"] > 0, (darko.get("FGM_UNAST", 0) / darko["POSS_OFF"]) * 100.0, 0.0)
    derived["FGA_UNAST_100p"] = np.where(darko["POSS_OFF"] > 0, (fga_unast_global / darko["POSS_OFF"]) * 100.0, 0.0)

    derived["3P_UNAST_Pct"] = np.where(darko["3PA_UNAST"] > 0, darko["3PM_UNAST"] / darko["3PA_UNAST"], 0.0)
    derived["3PM_UNAST_100p"] = np.where(darko["POSS_OFF"] > 0, (darko["3PM_UNAST"] / darko["POSS_OFF"]) * 100.0, 0.0)
    derived["3PA_UNAST_100p"] = np.where(darko["POSS_OFF"] > 0, (darko["3PA_UNAST"] / darko["POSS_OFF"]) * 100.0, 0.0)

    fgm_ast_global = darko["FGM"] - darko.get("FGM_UNAST", 0)
    fg3_ast = darko["3PM"] - darko.get("3PM_UNAST", 0)
    derived["FGM_AST"] = fgm_ast_global
    derived["3PM_AST"] = fg3_ast
    derived["FGM_100p_AST"] = np.where(darko["POSS_OFF"] > 0, (fgm_ast_global / darko["POSS_OFF"]) * 100.0, 0.0)
    derived["3PM_100p_AST"] = np.where(darko["POSS_OFF"] > 0, (fg3_ast / darko["POSS_OFF"]) * 100.0, 0.0)
    derived["AST_3P_100p"] = np.where(darko["POSS_OFF"] > 0, (darko["AST_3P"] / darko["POSS_OFF"]) * 100.0, 0.0)

    derived["DRB_FT"] = darko["DREB_FT"]
    derived["Player_Code"] = darko["FullName"].astype(str) + " " + darko["NbaDotComID"].astype(str)

    df_derived = pd.DataFrame(derived, index=darko.index)
    darko = pd.concat([darko, df_derived], axis=1)
    darko = darko.drop(columns=[c for c in darko.columns if c.startswith("_")], errors="ignore")

    schema_cols = [
        "Date", "NbaDotComID", "Team_SingleGame", "Game_SingleGame", "FullName", "Player_Code", "Year", "Position", "Source",
        "G", "Inactive", "DNP", "Starts", "POSS", "POSS_OFF", "POSS_DEF", "Minutes", "Pace",
        "TSAttempts", "TSpct", "PossessionsUsed", "USG",
        "PTS", "PTS_100p",
        "ORBpct", "OREB_100p", "OREBPct_FGA", "OREB_FGA_100p", "OREBPct_FT", "OREB_FT_100p", "OREB", "OREB_FGA", "OREB_FT",
        "DRBPct", "DREB_100p", "DRBPct_FGA", "DRB_FGA_100p", "DRBPct_FT", "DRB_FT_100p", "DRB", "DREB_FGA", "DRB_FT",
        "ASTpct", "AST_100p", "AST",
        "PF_100p", "PF", "PF_DRAWN_100p", "PF_DRAWN", "PF_Loose_100p", "PF_Loose", "CHRG_100p", "CHRG",
        "TECH_100p", "TECH", "FLAGRANT_100p", "FLAGRANT",
        "BLKPct", "BLK_Opp_100p", "BLK_Team_100p", "BLK_Opp", "BLK_Team", "BLK", "TM_BLK_OnCourt", "Goaltends_100p", "Goaltends",
        "STLpct", "STL_100p", "STL",
        "TOVpct", "TOV_100p", "TOV_Live_100p", "TOV_Dead_100p", "TOV", "TOV_Live", "TOV_Dead",
        "FTM_100p", "FTA_100p", "FTM", "FTA", "FT%", "FTR_Att", "FTR_Made", "AndOne_100p", "AndOnes",
        "FGM", "FGA", "FGM_100p", "FGA_100p", "FGPct",
        "0_3ft_FGM", "0_3ft_FGA", "0_3ft_FGM_100p", "0_3ft_FGA_100p", "0_3ft_FGPct",
        "4_9ft_FGM", "4_9ft_FGA", "4_9ft_FGM_100p", "4_9ft_FGA_100p", "4_9ft_FGPct",
        "10_17ft_FGM", "10_17ft_FGA", "10_17ft_FGM_100p", "10_17ft_FGA_100p", "10_17ft_FGPct",
        "18_23ft_FGM", "18_23ft_FGA", "18_23ft_FGM_100p", "18_23ft_FGA_100p", "18_23ft_FGPct",
        "3PM", "3PA", "3PM_100p", "3PA_100p", "3PPct",
        "FGM_UNAST", "FGA_UNAST", "FGM_UNAST_100p", "FGA_UNAST_100p", "FG_UNAST_Pct",
        "0_3ft_FGM_UNAST", "0_3ft_FGA_UNAST", "0_3ft_FGM_100p_UNAST", "0_3ft_FGA_UNAST_100p", "0_3ft_FG_UNAST_Pct", "0_3ft_FGA_100p_UNAST",
        "4_9ft_FGM_UNAST", "4_9ft_FGA_UNAST", "4_9ft_FGM_100p_UNAST", "4_9ft_FGA_UNAST_100p", "4_9ft_FG_UNAST_Pct",
        "10_17ft_FGM_UNAST", "10_17ft_FGA_UNAST", "10_17ft_FGM_100p_UNAST", "10_17ft_FGA_UNAST_100p", "10_17ft_FG_UNAST_Pct",
        "10_17ft_FGM_UNAST_100p",
        "18_23ft_FGM_UNAST", "18_23ft_FGA_UNAST", "18_23ft_FGM_100p_UNAST", "18_23ft_FGA_UNAST_100p", "18_23ft_FG_UNAST_Pct", "18_23ft_FGM_UNAST_100p",
        "3PM_UNAST", "3PA_UNAST", "3PM_UNAST_100p", "3PA_UNAST_100p", "3P_UNAST_Pct",
        "FGM_AST", "FGM_100p_AST",
        "0_3ft_FGM_AST", "4_9ft_FGM_AST", "10_17ft_FGM_AST", "18_23ft_FGM_AST", "3PM_AST",
        "0_3ft_FGM_100p_AST", "4_9ft_FGM_100p_AST", "10_17ft_FGM_100p_AST", "18_23ft_FGM_100p_AST",
        "3PM_100p_AST", "AST_3P_100p", "AST_3P",
        "AST_0_3ft", "AST_0_3ft_100p", "AST_4_9ft", "AST_4_9ft_100p", "AST_10_17ft", "AST_10_17ft_100p", "AST_18_23ft", "AST_18_23ft_100p",
        "OnCourt_Opp_FT_Made", "OnCourt_Opp_FT_Att", "OnCourt_Opp_3p_Made", "OnCourt_Opp_3p_Att", "OnCourt_Opp_FGA", "OnCourt_Opp_Points",
        "OnCourt_For_OREB_FGA", "OnCourt_For_DREB_FGA",
        "OnCourt_Team_FT_Made", "OnCourt_Team_FT_Att", "OnCourt_Team_3p_Made", "OnCourt_Team_3p_Att", "OnCourt_Team_Points",
        "Plus_Minus", "season", "h_tm_id", "v_tm_id", "home_fl",
    ]

    for c in schema_cols:
        if c not in darko.columns:
            darko[c] = 0

    return darko.reset_index(drop=True)[schema_cols], possessions


def assert_team_totals_match(
    game_id: str,
    darko_df: pd.DataFrame,
    possessions_resource: Possessions,
    tolerance: int = 2,
    overrides: Optional[Dict[str, Dict]] = None,
) -> None:
    """Assert that PBP-derived team PTS matches official boxscore within tolerance."""
    game_id = str(game_id).zfill(10)
    
    override = (overrides or _validation_overrides).get(game_id)
    if override:
        action = override.get("action", "allow")
        if action == "skip":
            print(f"[VALIDATION SKIPPED] Game {game_id}: {override.get('notes', 'manual override')}")
            return
        elif action == "allow" and override.get("tolerance") is not None:
            tolerance = override["tolerance"]
            print(f"[VALIDATION OVERRIDE] Game {game_id}: using tolerance={tolerance}")
    
    df_box = fetch_boxscore_stats(game_id)
    if df_box.empty:
        raise AssertionError(f"[VALIDATION] Boxscore fetch failed for {game_id}")

    player_box = df_box[df_box["PLAYER_ID"] != 0].copy()
    if player_box.empty:
        raise AssertionError(f"[VALIDATION] No player rows in boxscore for {game_id}")

    player_box["PTS"] = pd.to_numeric(player_box["PTS"], errors="coerce").fillna(0)
    official_team_pts = player_box.groupby("TEAM_ID")["PTS"].sum().astype(float)

    pbp_team_pts = defaultdict(float)
    for poss in possessions_resource.items:
        for ev in poss.events:
            if isinstance(ev, FieldGoal) and ev.is_made:
                team_id = getattr(ev, "team_id", None)
                if team_id:
                    pbp_team_pts[int(team_id)] += ev.shot_value
            elif isinstance(ev, FreeThrow) and ev.is_made:
                team_id = getattr(ev, "team_id", None)
                if team_id:
                    pbp_team_pts[int(team_id)] += 1

    pbp_pts_series = pd.Series(pbp_team_pts, name="PTS_PBP")

    joined = (
        pbp_pts_series.to_frame("PTS_PBP")
        .join(official_team_pts.to_frame("PTS_OFFICIAL"), how="outer")
        .fillna(0)
    )

    if joined.empty:
        raise AssertionError(f"[VALIDATION] No team data for {game_id}")

    joined["PTS_DIFF"] = joined["PTS_PBP"] - joined["PTS_OFFICIAL"]
    max_diff = joined["PTS_DIFF"].abs().max()

    if max_diff > tolerance:
        raise AssertionError(
            f"[VALIDATION FAILED] Game {game_id}: max PTS diff {max_diff} > {tolerance}\n"
            f"{joined}"
        )

    print(f"[VALIDATION OK] Game {game_id}: PBP-derived PTS within Â±{tolerance} of official.")


# ==============================================================================
# PARALLEL-SAFE VERSION (with fetchers as parameters)
# ==============================================================================

def _generate_darko_hybrid_with_fetchers(
    game_id: str,
    season_pbp_df: pd.DataFrame,
    fetch_boxscore_fn,
    fetch_summary_fn,
    fetch_pbp_v3_fn,
    error_list: List[Dict],
    rebound_deletions_list: Optional[List[Dict]] = None,
    one_way_possessions: bool = False,
) -> Tuple[pd.DataFrame, Possessions]:
    """Version of generate_darko_hybrid that uses provided fetcher functions."""
    
    df_box = fetch_boxscore_fn(game_id)
    summary = fetch_summary_fn(game_id)

    pid_to_name, pid_to_start, pid_to_pos = {}, {}, {}
    if not df_box.empty:
        df_box['PLAYER_ID'] = pd.to_numeric(df_box['PLAYER_ID'], errors='coerce').fillna(0).astype(int)
        df_box['TEAM_ID'] = pd.to_numeric(df_box['TEAM_ID'], errors='coerce').fillna(0).astype(int)
        pid_to_name = dict(zip(df_box['PLAYER_ID'], df_box['PLAYER_NAME']))
        df_box['IS_STARTER'] = df_box['START_POSITION'].apply(lambda x: 1 if x and str(x).strip() != '' else 0)
        pid_to_start = dict(zip(df_box['PLAYER_ID'], df_box['IS_STARTER']))
        pid_to_pos = dict(zip(df_box['PLAYER_ID'], df_box['START_POSITION']))

    single_game_df = season_pbp_df[season_pbp_df['GAME_ID'] == str(game_id).zfill(10)]
    if single_game_df.empty:
        raise ValueError(f"Game ID {game_id} not found in provided DataFrame.")

    h_tm_id, v_tm_id = _resolve_game_team_ids(summary, df_box)
    if h_tm_id and v_tm_id:
        boxscore_player_ids = df_box['PLAYER_ID'].tolist() if not df_box.empty else None
        single_game_df = normalize_single_game_team_events(
            single_game_df,
            home_team_id=h_tm_id,
            away_team_id=v_tm_id,
            boxscore_player_ids=boxscore_player_ids,
        )
    if not df_box.empty:
        single_game_df = normalize_single_game_player_ids(
            single_game_df,
            official_boxscore=df_box,
        )
    single_game_df = apply_pbp_row_overrides(single_game_df)

    # Use pbpstats.offline.get_possessions_from_df
    possessions = get_possessions_from_df(
        single_game_df,
        fetch_pbp_v3_fn=fetch_pbp_v3_fn,
        rebound_deletions_list=rebound_deletions_list,
    )
    
    # Check for event_stats errors
    for poss in possessions.items:
        for ev in poss.events:
            try:
                _ = ev.event_stats
            except Exception as e:
                error_list.append({
                    "game_id": str(game_id).zfill(10),
                    "event": repr(ev),
                    "error": str(e),
                })

    poss_used_counts = _compute_possessions_used_exact(possessions)
    ts_attempt_counts = _compute_ts_attempts_exact(possessions)

    poss_data = apply_pbp_stat_overrides(game_id, possessions.player_stats)
    possessions.manual_player_stats = poss_data
    if not poss_data:
        raise ValueError(f"No possession data calculated for {game_id}")

    df_poss = pd.DataFrame(poss_data)
    df_poss["player_id"] = df_poss["player_id"].astype(int)
    df_poss["team_id"] = df_poss["team_id"].astype(int)

    stats_wide = df_poss.pivot_table(
        index=["player_id", "team_id"],
        columns="stat_key",
        values="stat_value",
        aggfunc="sum",
        fill_value=0,
    )

    darko = pd.DataFrame(index=stats_wide.index)

    box_stat_map = {
        "PTS": "PTS", "AST": "AST", "STL": "STL", "BLK": "BLK", "TOV": "TO",
        "PF": "PF", "FGA": "FGA", "FGM": "FGM", "3PA": "FG3A", "3PM": "FG3M",
        "FTA": "FTA", "FTM": "FTM", "OREB": "OREB", "DRB": "DREB", "TRB": "REB"
    }

    if not df_box.empty:
        df_box_indexed = df_box.set_index(['PLAYER_ID', 'TEAM_ID'])
        df_box_indexed.index = df_box_indexed.index.set_levels([
            df_box_indexed.index.levels[0].astype(int),
            df_box_indexed.index.levels[1].astype(int)
        ])
        for darko_col, box_col in box_stat_map.items():
            if box_col in df_box_indexed.columns:
                darko[darko_col] = df_box_indexed[box_col].reindex(darko.index).fillna(0)

    stat_map = _build_darko_stat_map()
    all_indices = sorted(set(stats_wide.index).union(darko.index))
    stats_wide = stats_wide.reindex(all_indices).fillna(0)
    darko = darko.reindex(all_indices).fillna(0)

    base_stats: Dict[str, pd.Series] = {}
    for col_name, pbp_keys in stat_map.items():
        if col_name in darko.columns and col_name in box_stat_map:
            continue
        valid_keys = [k for k in pbp_keys if k in stats_wide.columns]
        if valid_keys:
            base_stats[col_name] = stats_wide[valid_keys].sum(axis=1)
        else:
            base_stats[col_name] = np.zeros(len(stats_wide))

    df_pbp_stats = pd.DataFrame(base_stats, index=darko.index)
    darko = pd.concat([darko, df_pbp_stats], axis=1)

    def get_meta(pid, mapping, default):
        return mapping.get(pid, default)

    if not df_box.empty:
        darko["FullName"] = [get_meta(idx[0], pid_to_name, "Unknown") for idx in darko.index]
        darko["Position"] = [get_meta(idx[0], pid_to_pos, "N/A") for idx in darko.index]
        darko["Starts"] = [get_meta(idx[0], pid_to_start, 0) for idx in darko.index]
    else:
        darko["FullName"] = "Unknown"
        darko["Position"] = "N/A"
        darko["Starts"] = 0

    is_team_stat = darko.index.get_level_values("player_id") == 0
    team_ids_str = darko.index.get_level_values("team_id").astype(str)
    darko.loc[is_team_stat, "FullName"] = "Team Stats (" + team_ids_str[is_team_stat] + ")"

    darko["NbaDotComID"] = darko.index.get_level_values("player_id")
    darko["Team_SingleGame"] = darko.index.get_level_values("team_id")
    darko["Game_SingleGame"] = int(game_id)
    darko["Source"] = "Local_DB"

    season_start_year = int(("19" if str(game_id)[3] == "9" else "20") + str(game_id)[3:5])
    darko["Year"] = season_start_year + 1
    darko["season"] = darko["Year"]

    if summary.get('GAME_DATE_EST'):
        darko["Date"] = summary['GAME_DATE_EST'].split('T')[0]
    else:
        darko["Date"] = f"01/01/{season_start_year+1}"

    if not (h_tm_id and v_tm_id) and not df_box.empty:
        team_ids = df_box['TEAM_ID'].unique()
        h_tm_id, v_tm_id = (team_ids[0], team_ids[1]) if len(team_ids) >= 2 else (0, 0)
    elif not (h_tm_id and v_tm_id):
        team_ids = darko.index.get_level_values('team_id').unique()
        h_tm_id, v_tm_id = (team_ids[0], team_ids[1]) if len(team_ids) >= 2 else (0, 0)

    darko["h_tm_id"] = h_tm_id
    darko["v_tm_id"] = v_tm_id
    darko["home_fl"] = (darko["Team_SingleGame"] == h_tm_id).astype(int)

    darko["Minutes"] = (darko["Seconds_Off"] + darko["Seconds_Def"]) / 60.0
    darko["POSS"] = (darko["POSS_OFF"] + darko["POSS_DEF"]) / (2.0 if not one_way_possessions else 1.0)

    darko["TSAttempts"] = _counts_to_series(ts_attempt_counts, darko.index, "TSAttempts")
    darko["PossessionsUsed"] = _counts_to_series(poss_used_counts, darko.index, "PossessionsUsed")

    if "BLK" not in darko.columns:
        darko["BLK"] = darko.get("_BLK_Total_PBP", 0)

    total_blk_pbp = darko.get("_BLK_Total_PBP", darko["BLK"])
    darko["BLK_Opp"] = np.maximum(total_blk_pbp - darko.get("BLK_Team", 0), 0)

    # Derived stats
    derived = {}
    derived["TSpct"] = np.where(darko["TSAttempts"] > 0, darko["PTS"] / (2.0 * darko["TSAttempts"]), 0.0)
    derived["USG"] = np.where(darko["POSS_OFF"] > 0, darko["PossessionsUsed"] / darko["POSS_OFF"], 0.0)
    pace_denom = 2.0 if not one_way_possessions else 1.0
    derived["Pace"] = np.where(darko["Minutes"] > 0, (48.0 * darko["POSS"]) / darko["Minutes"] / pace_denom, 0.0)
    derived["G"] = np.where(darko["Minutes"] > 0, 1, 0)
    derived["DNP"] = np.where(darko["Minutes"] == 0, 1, 0)
    derived["Inactive"] = 0

    reb_configs = [
        ("ORBpct", "OREB", "_OREB_opp_total"), ("DRBPct", "DRB", "_DRB_opp_total"),
        ("OREBPct_FGA", "OREB_FGA", "_OREB_opp_FGA"), ("OREBPct_FT", "OREB_FT", "_OREB_opp_FT"),
        ("DRBPct_FGA", "DREB_FGA", "_DRB_opp_FGA"), ("DRBPct_FT", "DREB_FT", "_DRB_opp_FT"),
    ]
    for out, num, den in reb_configs:
        if num in darko.columns and den in darko.columns:
            derived[out] = np.where(darko[den] > 0, darko[num] / darko[den], 0.0)
        else:
            derived[out] = 0.0

    derived["Plus_Minus"] = darko.get("Plus_Minus_raw", 0)
    team_fgm = darko.get("OnCourt_Team_FGM", 0)
    team_3pm = darko.get("OnCourt_Team_3p_Made", 0)
    team_ftm = darko.get("OnCourt_Team_FT_Made", 0)
    team_pts = 2.0 * team_fgm + team_3pm + team_ftm
    derived["OnCourt_Team_Points"] = np.where((team_pts == 0) & (darko["OnCourt_Opp_Points"] > 0), darko["OnCourt_Opp_Points"] + derived["Plus_Minus"], team_pts)

    per_100_cols = ["PTS", "AST", "TOV", "FGM", "FGA", "3PM", "3PA", "OREB", "DRB", "BLK", "STL", "FTM", "FTA", "CHRG", "BLK_Opp", "BLK_Team", "Goaltends", "TOV_Live", "TOV_Dead", "AndOnes"]
    def_stats = {"DRB", "BLK", "STL", "CHRG", "BLK_Opp", "BLK_Team", "Goaltends"}
    for col in per_100_cols:
        if col not in darko.columns:
            continue
        denom = "POSS_DEF" if col in def_stats else "POSS_OFF"
        out = f"{col}_100p"
        if col == "DRB":
            out = "DREB_100p"
        if col == "AndOnes":
            out = "AndOne_100p"
        derived[out] = np.where(darko[denom] > 0, (darko[col] / darko[denom]) * 100.0, 0.0)

    shooting_rates = [("FGPct", "FGM", "FGA"), ("3PPct", "3PM", "3PA"), ("FT%", "FTM", "FTA"), ("FTR_Att", "FTA", "FGA"), ("FTR_Made", "FTM", "FGA"), ("TOVpct", "TOV", "PossessionsUsed"), ("STLpct", "STL", "POSS_DEF")]
    for out, num, den in shooting_rates:
        if num in darko.columns and den in darko.columns:
            derived[out] = np.where(darko[den] > 0, darko[num] / darko[den], 0.0)
        else:
            derived[out] = 0.0

    opp_2pa = darko["OnCourt_Opp_FGA"] - darko["OnCourt_Opp_3p_Att"]
    opp_2pa = np.where(opp_2pa < 0, 0, opp_2pa)
    derived["BLKPct"] = np.where(opp_2pa > 0, darko["BLK"] / opp_2pa, 0.0)

    denom_ast = team_fgm - darko["FGM"]
    derived["ASTpct"] = np.where(denom_ast > 0, darko["AST"] / denom_ast, 0.0)

    buckets = ["0_3ft", "4_9ft", "10_17ft", "18_23ft"]
    for bucket in buckets:
        fgm, fga = darko.get(f"{bucket}_FGM", 0), darko.get(f"{bucket}_FGA", 0)
        fgm_ast, fgm_unast = darko.get(f"{bucket}_FGM_AST", 0), darko.get(f"{bucket}_FGM_UNAST", 0)
        derived[f"{bucket}_FGPct"] = np.where(fga > 0, fgm / fga, 0.0)
        derived[f"{bucket}_FGM_100p"] = np.where(darko["POSS_OFF"] > 0, (fgm / darko["POSS_OFF"]) * 100.0, 0.0)
        derived[f"{bucket}_FGA_100p"] = np.where(darko["POSS_OFF"] > 0, (fga / darko["POSS_OFF"]) * 100.0, 0.0)
        fga_unast = fga - fgm_ast
        derived[f"{bucket}_FGA_UNAST"] = fga_unast
        derived[f"{bucket}_FG_UNAST_Pct"] = np.where(fga_unast > 0, fgm_unast / fga_unast, 0.0)
        derived[f"{bucket}_FGM_100p_UNAST"] = np.where(darko["POSS_OFF"] > 0, (fgm_unast / darko["POSS_OFF"]) * 100.0, 0.0)
        derived[f"{bucket}_FGA_UNAST_100p"] = np.where(darko["POSS_OFF"] > 0, (fga_unast / darko["POSS_OFF"]) * 100.0, 0.0)
        derived[f"{bucket}_FGM_UNAST_100p"] = derived[f"{bucket}_FGM_100p_UNAST"]
        if bucket == "0_3ft":
            derived["0_3ft_FGA_100p_UNAST"] = derived["0_3ft_FGA_UNAST_100p"]
        derived[f"{bucket}_FGM_100p_AST"] = np.where(darko["POSS_OFF"] > 0, (fgm_ast / darko["POSS_OFF"]) * 100.0, 0.0)
        ast_col = f"AST_{bucket}"
        if ast_col in darko.columns:
            derived[f"{ast_col}_100p"] = np.where(darko["POSS_OFF"] > 0, (darko[ast_col] / darko["POSS_OFF"]) * 100.0, 0.0)

    fga_unast_global = darko["FGA"] - (darko["FGM"] - darko.get("FGM_UNAST", 0))
    derived["FGA_UNAST"] = fga_unast_global
    derived["FG_UNAST_Pct"] = np.where(fga_unast_global > 0, darko.get("FGM_UNAST", 0) / fga_unast_global, 0.0)
    derived["FGM_UNAST_100p"] = np.where(darko["POSS_OFF"] > 0, (darko.get("FGM_UNAST", 0) / darko["POSS_OFF"]) * 100.0, 0.0)
    derived["FGA_UNAST_100p"] = np.where(darko["POSS_OFF"] > 0, (fga_unast_global / darko["POSS_OFF"]) * 100.0, 0.0)

    derived["3P_UNAST_Pct"] = np.where(darko["3PA_UNAST"] > 0, darko["3PM_UNAST"] / darko["3PA_UNAST"], 0.0)
    derived["3PM_UNAST_100p"] = np.where(darko["POSS_OFF"] > 0, (darko["3PM_UNAST"] / darko["POSS_OFF"]) * 100.0, 0.0)
    derived["3PA_UNAST_100p"] = np.where(darko["POSS_OFF"] > 0, (darko["3PA_UNAST"] / darko["POSS_OFF"]) * 100.0, 0.0)

    fgm_ast_global = darko["FGM"] - darko.get("FGM_UNAST", 0)
    fg3_ast = darko["3PM"] - darko.get("3PM_UNAST", 0)
    derived["FGM_AST"] = fgm_ast_global
    derived["3PM_AST"] = fg3_ast
    derived["FGM_100p_AST"] = np.where(darko["POSS_OFF"] > 0, (fgm_ast_global / darko["POSS_OFF"]) * 100.0, 0.0)
    derived["3PM_100p_AST"] = np.where(darko["POSS_OFF"] > 0, (fg3_ast / darko["POSS_OFF"]) * 100.0, 0.0)
    derived["AST_3P_100p"] = np.where(darko["POSS_OFF"] > 0, (darko["AST_3P"] / darko["POSS_OFF"]) * 100.0, 0.0)

    derived["DRB_FT"] = darko["DREB_FT"]
    derived["Player_Code"] = darko["FullName"].astype(str) + " " + darko["NbaDotComID"].astype(str)

    df_derived = pd.DataFrame(derived, index=darko.index)
    darko = pd.concat([darko, df_derived], axis=1)
    darko = darko.drop(columns=[c for c in darko.columns if c.startswith("_")], errors="ignore")

    schema_cols = [
        "Date", "NbaDotComID", "Team_SingleGame", "Game_SingleGame", "FullName", "Player_Code", "Year", "Position", "Source",
        "G", "Inactive", "DNP", "Starts", "POSS", "POSS_OFF", "POSS_DEF", "Minutes", "Pace",
        "TSAttempts", "TSpct", "PossessionsUsed", "USG",
        "PTS", "PTS_100p",
        "ORBpct", "OREB_100p", "OREBPct_FGA", "OREB_FGA_100p", "OREBPct_FT", "OREB_FT_100p", "OREB", "OREB_FGA", "OREB_FT",
        "DRBPct", "DREB_100p", "DRBPct_FGA", "DRB_FGA_100p", "DRBPct_FT", "DRB_FT_100p", "DRB", "DREB_FGA", "DRB_FT",
        "ASTpct", "AST_100p", "AST",
        "PF_100p", "PF", "PF_DRAWN_100p", "PF_DRAWN", "PF_Loose_100p", "PF_Loose", "CHRG_100p", "CHRG",
        "TECH_100p", "TECH", "FLAGRANT_100p", "FLAGRANT",
        "BLKPct", "BLK_Opp_100p", "BLK_Team_100p", "BLK_Opp", "BLK_Team", "BLK", "TM_BLK_OnCourt", "Goaltends_100p", "Goaltends",
        "STLpct", "STL_100p", "STL",
        "TOVpct", "TOV_100p", "TOV_Live_100p", "TOV_Dead_100p", "TOV", "TOV_Live", "TOV_Dead",
        "FTM_100p", "FTA_100p", "FTM", "FTA", "FT%", "FTR_Att", "FTR_Made", "AndOne_100p", "AndOnes",
        "FGM", "FGA", "FGM_100p", "FGA_100p", "FGPct",
        "0_3ft_FGM", "0_3ft_FGA", "0_3ft_FGM_100p", "0_3ft_FGA_100p", "0_3ft_FGPct",
        "4_9ft_FGM", "4_9ft_FGA", "4_9ft_FGM_100p", "4_9ft_FGA_100p", "4_9ft_FGPct",
        "10_17ft_FGM", "10_17ft_FGA", "10_17ft_FGM_100p", "10_17ft_FGA_100p", "10_17ft_FGPct",
        "18_23ft_FGM", "18_23ft_FGA", "18_23ft_FGM_100p", "18_23ft_FGA_100p", "18_23ft_FGPct",
        "3PM", "3PA", "3PM_100p", "3PA_100p", "3PPct",
        "FGM_UNAST", "FGA_UNAST", "FGM_UNAST_100p", "FGA_UNAST_100p", "FG_UNAST_Pct",
        "0_3ft_FGM_UNAST", "0_3ft_FGA_UNAST", "0_3ft_FGM_100p_UNAST", "0_3ft_FGA_UNAST_100p", "0_3ft_FG_UNAST_Pct", "0_3ft_FGA_100p_UNAST",
        "4_9ft_FGM_UNAST", "4_9ft_FGA_UNAST", "4_9ft_FGM_100p_UNAST", "4_9ft_FGA_UNAST_100p", "4_9ft_FG_UNAST_Pct",
        "10_17ft_FGM_UNAST", "10_17ft_FGA_UNAST", "10_17ft_FGM_100p_UNAST", "10_17ft_FGA_UNAST_100p", "10_17ft_FG_UNAST_Pct",
        "10_17ft_FGM_UNAST_100p",
        "18_23ft_FGM_UNAST", "18_23ft_FGA_UNAST", "18_23ft_FGM_100p_UNAST", "18_23ft_FGA_UNAST_100p", "18_23ft_FG_UNAST_Pct", "18_23ft_FGM_UNAST_100p",
        "3PM_UNAST", "3PA_UNAST", "3PM_UNAST_100p", "3PA_UNAST_100p", "3P_UNAST_Pct",
        "FGM_AST", "FGM_100p_AST",
        "0_3ft_FGM_AST", "4_9ft_FGM_AST", "10_17ft_FGM_AST", "18_23ft_FGM_AST", "3PM_AST",
        "0_3ft_FGM_100p_AST", "4_9ft_FGM_100p_AST", "10_17ft_FGM_100p_AST", "18_23ft_FGM_100p_AST",
        "3PM_100p_AST", "AST_3P_100p", "AST_3P",
        "AST_0_3ft", "AST_0_3ft_100p", "AST_4_9ft", "AST_4_9ft_100p", "AST_10_17ft", "AST_10_17ft_100p", "AST_18_23ft", "AST_18_23ft_100p",
        "OnCourt_Opp_FT_Made", "OnCourt_Opp_FT_Att", "OnCourt_Opp_3p_Made", "OnCourt_Opp_3p_Att", "OnCourt_Opp_FGA", "OnCourt_Opp_Points",
        "OnCourt_For_OREB_FGA", "OnCourt_For_DREB_FGA",
        "OnCourt_Team_FT_Made", "OnCourt_Team_FT_Att", "OnCourt_Team_3p_Made", "OnCourt_Team_3p_Att", "OnCourt_Team_Points",
        "Plus_Minus", "season", "h_tm_id", "v_tm_id", "home_fl",
    ]

    for c in schema_cols:
        if c not in darko.columns:
            darko[c] = 0

    return darko.reset_index(drop=True)[schema_cols], possessions


def _assert_team_totals_with_fetcher(
    game_id: str,
    darko_df: pd.DataFrame,
    possessions_resource: Possessions,
    fetch_boxscore_fn,
    tolerance: int = 2,
    overrides: Optional[Dict[str, Dict]] = None,
) -> None:
    """Version of assert_team_totals_match that uses provided fetcher and supports overrides."""
    game_id = str(game_id).zfill(10)
    
    override = (overrides or {}).get(game_id)
    if override:
        action = override.get("action", "allow")
        if action == "skip":
            print(f"[VALIDATION SKIPPED] Game {game_id}: {override.get('notes', 'manual override')}")
            return
        elif action == "allow" and override.get("tolerance") is not None:
            tolerance = override["tolerance"]
            print(f"[VALIDATION OVERRIDE] Game {game_id}: using tolerance={tolerance}")
    
    df_box = fetch_boxscore_fn(game_id)
    if df_box.empty:
        raise AssertionError(f"[VALIDATION] Boxscore fetch failed for {game_id}")

    player_box = df_box[df_box["PLAYER_ID"] != 0].copy()
    if player_box.empty:
        raise AssertionError(f"[VALIDATION] No player rows in boxscore for {game_id}")

    player_box["PTS"] = pd.to_numeric(player_box["PTS"], errors="coerce").fillna(0)
    official_team_pts = player_box.groupby("TEAM_ID")["PTS"].sum().astype(float)

    pbp_team_pts = defaultdict(float)
    for poss in possessions_resource.items:
        for ev in poss.events:
            if isinstance(ev, FieldGoal) and ev.is_made:
                team_id = getattr(ev, "team_id", None)
                if team_id:
                    pbp_team_pts[int(team_id)] += ev.shot_value
            elif isinstance(ev, FreeThrow) and ev.is_made:
                team_id = getattr(ev, "team_id", None)
                if team_id:
                    pbp_team_pts[int(team_id)] += 1

    pbp_pts_series = pd.Series(pbp_team_pts, name="PTS_PBP")

    joined = (
        pbp_pts_series.to_frame("PTS_PBP")
        .join(official_team_pts.to_frame("PTS_OFFICIAL"), how="outer")
        .fillna(0)
    )

    if joined.empty:
        raise AssertionError(f"[VALIDATION] No team data for {game_id}")

    joined["PTS_DIFF"] = joined["PTS_PBP"] - joined["PTS_OFFICIAL"]
    max_diff = joined["PTS_DIFF"].abs().max()

    if max_diff > tolerance:
        raise AssertionError(
            f"[VALIDATION FAILED] Game {game_id}: max PTS diff {max_diff} > {tolerance}\n{joined}"
        )


# ==============================================================================
# PARALLEL PROCESSING (JOBLIB)
# ==============================================================================

def _process_single_game_worker(
    game_id: str,
    game_df: pd.DataFrame,
    db_path: str,
    validate: bool = True,
    tolerance: int = 2,
    overrides: Optional[Dict[str, Dict]] = None,
    strict_mode: Optional[bool] = None,
    run_boxscore_audit: bool = False,
) -> Tuple[str, Optional[pd.DataFrame], Optional[str], List[Dict], List[Dict], Dict[str, List[Dict[str, Any]]]]:
    """
    Worker function for parallel processing.
    Fully self-contained - creates own DB connection.
    Returns (game_id, darko_df, error_msg, event_stats_errors, rebound_deletions, audit_payload).
    """
    game_id = str(game_id).zfill(10)
    local_errors: List[Dict] = []
    local_rebound_deletions: List[Dict] = []
    audit_payload: Dict[str, List[Dict[str, Any]]] = {
        "team_rows": [],
        "player_rows": [],
        "audit_errors": [],
    }
    
    # Set strict mode inside the worker process
    if strict_mode is not None:
        set_rebound_strict_mode(strict_mode)
    
    local_conn = sqlite3.connect(db_path, timeout=30)
    
    def local_load_response(gid: str, endpoint: str, team_id: Optional[int] = None) -> Optional[Dict]:
        if team_id is None:
            row = local_conn.execute(
                "SELECT data FROM raw_responses WHERE game_id=? AND endpoint=? AND team_id IS NULL",
                (gid, endpoint)
            ).fetchone()
        else:
            row = local_conn.execute(
                "SELECT data FROM raw_responses WHERE game_id=? AND endpoint=? AND team_id=?",
                (gid, endpoint, team_id)
            ).fetchone()
        if row:
            blob = row[0]
            try:
                data = json.loads(zlib.decompress(blob).decode())
            except (zlib.error, TypeError):
                if isinstance(blob, bytes):
                    data = json.loads(blob.decode())
                else:
                    data = json.loads(blob)
            if endpoint == "boxscore":
                return apply_boxscore_response_overrides(gid, data)
            return data
        return None
    
    def local_fetch_boxscore(gid: str) -> pd.DataFrame:
        gid = str(gid).zfill(10)
        data = local_load_response(gid, "boxscore")
        if not data:
            return pd.DataFrame()
        try:
            result_set = data["resultSets"][0]
            return pd.DataFrame(result_set["rowSet"], columns=result_set["headers"])
        except (KeyError, IndexError):
            return pd.DataFrame()
    
    def local_fetch_summary(gid: str) -> dict:
        gid = str(gid).zfill(10)
        data = local_load_response(gid, "summary")
        if not data:
            return {}
        try:
            if "resultSets" in data and len(data["resultSets"]) > 0:
                headers = data["resultSets"][0]["headers"]
                if data["resultSets"][0]["rowSet"]:
                    row = data["resultSets"][0]["rowSet"][0]
                    return dict(zip(headers, row))
            return {}
        except (KeyError, IndexError):
            return {}
    
    def local_fetch_pbp_v3(gid: str) -> pd.DataFrame:
        gid = str(gid).zfill(10)
        data = local_load_response(gid, "pbpv3")
        if not data:
            return pd.DataFrame()
        try:
            actions = data.get("game", {}).get("actions", [])
            return pd.DataFrame(actions)
        except Exception:
            return pd.DataFrame()
    
    try:
        darko_df, possessions = _generate_darko_hybrid_with_fetchers(
            game_id, 
            game_df,
            local_fetch_boxscore,
            local_fetch_summary,
            local_fetch_pbp_v3,
            local_errors,
            local_rebound_deletions,
        )
        
        if validate:
            _assert_team_totals_with_fetcher(
                game_id, darko_df, possessions, local_fetch_boxscore, tolerance,
                overrides=overrides
            )

        if run_boxscore_audit:
            try:
                official_box = local_fetch_boxscore(game_id)
                team_rows, player_rows, audit_errors = _build_game_boxscore_audit_rows(
                    game_id,
                    getattr(possessions, "manual_player_stats", possessions.player_stats),
                    official_box,
                )
                audit_payload = {
                    "team_rows": team_rows,
                    "player_rows": player_rows,
                    "audit_errors": audit_errors,
                }
            except Exception as audit_exc:
                audit_payload["audit_errors"].append({
                    "game_id": game_id,
                    "error": str(audit_exc),
                })

        return (game_id, darko_df, None, local_errors, local_rebound_deletions, audit_payload)
    
    except Exception as e:
        if run_boxscore_audit:
            audit_payload["audit_errors"].append({
                "game_id": game_id,
                "error": f"Game processing failed: {e}",
            })
        return (game_id, None, str(e), local_errors, local_rebound_deletions, audit_payload)
    finally:
        local_conn.close()


def process_single_game(
    game_id: str,
    season_pbp_df: pd.DataFrame,
    validate: bool = True,
    tolerance: int = 2,
) -> Tuple[str, Optional[pd.DataFrame], Optional[str]]:
    """
    Process a single game (non-parallel version).
    Returns (game_id, darko_df, error_msg).
    """
    game_id = str(game_id).zfill(10)
    try:
        darko_df, possessions = generate_darko_hybrid(game_id, season_pbp_df)
        
        if validate:
            assert_team_totals_match(game_id, darko_df, possessions, tolerance=tolerance)
        
        clear_v3_cache(game_id)
        return (game_id, darko_df, None)
    
    except Exception as e:
        clear_v3_cache(game_id)
        return (game_id, None, str(e))


def process_games_parallel(
    game_ids: List[str],
    season_pbp_df: pd.DataFrame,
    max_workers: int = -1,
    validate: bool = True,
    tolerance: int = 2,
    backend: str = "loky",
    overrides: Optional[Dict[str, Dict]] = None,
    strict_mode: Optional[bool] = None,
    run_boxscore_audit: bool = False,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Process multiple games in parallel using joblib.
    
    Args:
        backend: "loky" (default, works in notebooks), "multiprocessing", or "threading"
        overrides: Validation overrides dict (game_id -> override settings)
        strict_mode: If True, conservative rebound handling; if False, aggressive
        run_boxscore_audit: Whether to write per-game team/player boxscore audit files
    
    Returns:
        (combined_darko_df, error_df, team_audit_df, player_mismatch_df, audit_error_df)
    """
    db_path_str = str(DB_PATH)
    
    print(f"[PREP] Pre-filtering {len(game_ids)} games...")
    game_dfs: Dict[str, pd.DataFrame] = {
        str(gid).zfill(10): group.copy()
        for gid, group in season_pbp_df.groupby("GAME_ID")
    }
    
    print(f"[RUN] Processing with {max_workers} workers (backend={backend})...")
    
    results_list = Parallel(n_jobs=max_workers, backend=backend, verbose=10)(
        delayed(_process_single_game_worker)(
            gid,
            game_dfs[str(gid).zfill(10)],
            db_path_str,
            validate,
            tolerance,
            overrides,
            strict_mode,
            run_boxscore_audit,
        )
        for gid in game_ids
    )
    
    results: List[pd.DataFrame] = []
    errors: List[Dict] = []
    all_event_errors: List[Dict] = []
    all_rebound_deletions: List[Dict] = []
    all_team_audit_rows: List[Dict[str, Any]] = []
    all_player_mismatch_rows: List[Dict[str, Any]] = []
    all_audit_error_rows: List[Dict[str, Any]] = []
    
    for game_id, df, error, event_errors, rebound_deletions, audit_payload in results_list:
        all_event_errors.extend(event_errors)
        all_rebound_deletions.extend(rebound_deletions)
        if run_boxscore_audit:
            all_team_audit_rows.extend(audit_payload.get("team_rows", []))
            all_player_mismatch_rows.extend(audit_payload.get("player_rows", []))
            all_audit_error_rows.extend(audit_payload.get("audit_errors", []))
        
        if error is None:
            results.append(df)
        else:
            errors.append({"game_id": game_id, "error": error})
            print(f"[FAILED] {game_id}: {error}")
    
    global _event_stats_errors
    _event_stats_errors.extend(all_event_errors)
    
    global _rebound_fallback_deletions
    with _rebound_fallback_lock:
        _rebound_fallback_deletions.extend(all_rebound_deletions)
    
    combined_df = pd.concat(results, ignore_index=True) if results else pd.DataFrame()
    error_df = pd.DataFrame(errors)
    team_audit_df = pd.DataFrame(all_team_audit_rows, columns=TEAM_AUDIT_COLUMNS)
    player_mismatch_df = pd.DataFrame(all_player_mismatch_rows, columns=PLAYER_MISMATCH_COLUMNS)
    audit_error_df = pd.DataFrame(all_audit_error_rows, columns=AUDIT_ERROR_COLUMNS)
    
    print(f"[DONE] {len(results)} succeeded, {len(errors)} failed")
    
    return combined_df, error_df, team_audit_df, player_mismatch_df, audit_error_df


# ==============================================================================
# MAIN
# ==============================================================================

def process_season(
    season: int,
    parquet_path: str = "playbyplayv2.parq",
    output_dir: str = ".",
    validate: bool = True,
    tolerance: int = 2,
    max_workers: int = -1,
    overrides_path: str = "validation_overrides.csv",
    strict_mode: bool | None = None,
    run_boxscore_audit: bool = False,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Process all games for a single season.
    
    Args:
        season: Season year (e.g., 1998 for 1998-99)
        parquet_path: Path to the parquet file
        output_dir: Directory to save output files
        validate: Whether to validate against boxscore
        tolerance: Points tolerance for validation
        max_workers: Number of parallel workers (-1 = all cores)
        overrides_path: Path to validation overrides CSV
        strict_mode: If True, conservative rebound handling; if False, aggressive
    
    Returns:
        (combined_darko_df, error_df)
    """
    overrides = load_validation_overrides(overrides_path)
    clear_rebound_fallback_deletions()

    # Decide strict mode for this run
    if strict_mode is None:
        strict_mode = True
    print(f"[CONFIG] Season {season}: REBOUND_STRICT_MODE={strict_mode}")
    
    season_df = load_pbp_from_parquet(parquet_path, season=season)
    
    if season_df.empty:
        print(f"[ERROR] No data found for season {season}")
        return pd.DataFrame(), pd.DataFrame()
    
    game_ids = season_df["GAME_ID"].unique().tolist()
    print(f"Processing {len(game_ids)} games for season {season} with {max_workers} workers...")
    
    combined_df, error_df, team_audit_df, player_mismatch_df, audit_error_df = process_games_parallel(
        game_ids,
        season_df,
        max_workers=max_workers,
        validate=validate,
        tolerance=tolerance,
        overrides=overrides,
        strict_mode=strict_mode,
        run_boxscore_audit=run_boxscore_audit,
    )
    
    if not combined_df.empty:
        output_file = f"{output_dir}/darko_{season}.parquet"
        combined_df.to_parquet(output_file, index=False)
        print(f"[OUTPUT] Saved {len(combined_df)} rows to {output_file}")
    
    if not error_df.empty:
        error_file = f"{output_dir}/errors_{season}.csv"
        error_df.to_csv(error_file, index=False)
        print(f"[ERRORS] {len(error_df)} game errors saved to {error_file}")
    
    export_rebound_fallback_deletions(f"rebound_fallback_deletions_{season}.csv")

    if run_boxscore_audit:
        audit_summary = write_boxscore_audit_outputs(
            team_audit=team_audit_df,
            player_mismatches=player_mismatch_df,
            audit_errors=audit_error_df,
            season=season,
            output_dir=Path(output_dir),
            games_requested=len(game_ids),
        )
        print(
            f"[AUDIT] Saved season {season}: games_with_team_mismatch={audit_summary['games_with_team_mismatch']} "
            f"player_rows_with_mismatch={audit_summary['player_rows_with_mismatch']} "
            f"audit_failures={audit_summary['audit_failures']}"
        )
    
    return combined_df, error_df


def main():
    parquet_path = "playbyplayv2.parq"
    output_dir = "."
    season = 1998
    
    try:
        combined_df, error_df = process_season(
            season=season,
            parquet_path=parquet_path,
            output_dir=output_dir,
            validate=True,
            tolerance=2,
        )
        
        export_event_stats_errors(f"event_stats_errors_{season}.csv")
        
        print(f"\n[SUMMARY] Season {season}: "
              f"{len(combined_df)} player-game rows, {len(error_df)} failed games")

    except FileNotFoundError as e:
        print(f"File not found: {e}")
    except Exception as e:
        print(f"Error: {e}")
        raise
    finally:
        if _event_stats_errors:
            export_event_stats_errors(f"event_stats_errors_{season}.csv")


def main_multi_season():
    """Process multiple seasons."""
    parquet_path = "playbyplayv2.parq"
    output_dir = "."
    
    seasons = list(range(1996, 2025))
    
    all_results = []
    all_errors = []
    
    for season in seasons:
        print(f"\n{'='*60}")
        print(f"PROCESSING SEASON {season}-{str(season+1)[-2:]}")
        print(f"{'='*60}\n")
        
        try:
            clear_event_stats_errors()
            
            combined_df, error_df = process_season(
                season=season,
                parquet_path=parquet_path,
                output_dir=output_dir,
                validate=True,
                tolerance=2,
            )
            
            if not combined_df.empty:
                all_results.append(combined_df)
            if not error_df.empty:
                all_errors.append(error_df)
            
            export_event_stats_errors(f"event_stats_errors_{season}.csv")
            
        except Exception as e:
            print(f"[ERROR] Season {season} failed: {e}")
            continue
    
    if all_results:
        combined_all = pd.concat(all_results, ignore_index=True)
        combined_all.to_parquet(f"{output_dir}/darko_all_seasons.parquet", index=False)
        print(f"\n[FINAL] Combined {len(combined_all)} rows across all seasons")
    
    if all_errors:
        errors_all = pd.concat(all_errors, ignore_index=True)
        errors_all.to_csv(f"{output_dir}/errors_all_seasons.csv", index=False)
        print(f"[FINAL] {len(errors_all)} total errors across all seasons")


if __name__ == "__main__":
    pass

'''

# In[2]:


#from pbpstats.offline.processor import set_rebound_strict_mode

def run_all_seasons(
    parquet_path: str = "playbyplayv2.parq",
    output_root: str = ".",
    validate: bool = True,
    tolerance: int = 2,
    max_workers: int = 25,
    strict_mode: bool = False,
):
    """
    Run DARKO single-game generator for all seasons 1997â€“2025.
    Uses your existing process_season() and writes one parquet per season.
    """
    all_results = []
    all_errors = []

    for season in range(1997, 2026):  # 1997-98 through 2024-25
        print(f"\n{'='*60}")
        print(f"PROCESSING SEASON {season}-{str(season+1)[-2:]}")
        print(f"{'='*60}\n")

        # Optional: adjust strict mode by era
        # Example: be more aggressive on pre-2010 seasons
        #if season < 2010:
        #    set_rebound_strict_mode(False)  # allow deleting player orphan rebounds
        #else:
        #    set_rebound_strict_mode(True)   # conservative for modern seasons

        try:
            clear_event_stats_errors()

            combined_df, error_df = process_season(
                season=season,
                parquet_path=parquet_path,
                output_dir=output_root,
                validate=validate,
                tolerance=tolerance,
                max_workers=max_workers,
                overrides_path="validation_overrides.csv",
                strict_mode=strict_mode,
            )

            if not combined_df.empty:
                all_results.append(combined_df)
            if not error_df.empty:
                all_errors.append(error_df)

            export_event_stats_errors(f"event_stats_errors_{season}.csv")

        except Exception as e:
            print(f"[ERROR] Season {season} failed: {e}")
            continue

    # Optionally combine everything into one big parquet / CSV
    if all_results:
        combined_all = pd.concat(all_results, ignore_index=True)
        combined_all.to_parquet(f"{output_root}/darko_all_seasons.parquet", index=False)
        print(f"\n[FINAL] Combined {len(combined_all)} rows across all seasons")

    if all_errors:
        errors_all = pd.concat(all_errors, ignore_index=True)
        errors_all.to_csv(f"{output_root}/errors_all_seasons.csv", index=False)
        print(f"[FINAL] {len(errors_all)} total errors across all seasons")


run_all_seasons(
    parquet_path="playbyplayv2.parq",
    output_root=".",    # or some other directory
    validate=True,
    tolerance=2,
    max_workers=25,     # or whatever you found works best
)


# In[ ]:


# Aggressive mode for old seasons
combined_df, error_df = process_season(
    season=1997,
    strict_mode=False,
)


# In[ ]:


season = 1997
parquet_path = "playbyplayv2.parq"
season_df = load_pbp_from_parquet(parquet_path, season=season)


# In[ ]:

from pbpstats.offline import get_possessions_from_df

game = season_df[season_df["GAME_ID"].astype(int) == 49600063].copy()
poss = get_possessions_from_df(game, fetch_pbp_v3_fn=fetch_pbp_v3)
print(f"Success! {len(poss.items)} possessions")


# In[ ]:

game = season_df[season_df["GAME_ID"].astype(int) == 49600063].copy()
game["EVENTNUM"] = game["EVENTNUM"].astype(int)
game = game.sort_values(["PERIOD", "EVENTNUM"])

mask = (game["EVENTNUM"] >= 212) & (game["EVENTNUM"] <= 230)
cols = ["EVENTNUM", "PERIOD", "PCTIMESTRING", "EVENTMSGTYPE", "EVENTMSGACTIONTYPE",
        "HOMEDESCRIPTION", "VISITORDESCRIPTION", "PLAYER1_NAME", "PLAYER1_ID"]
print(game[mask][cols].to_string())


# In[ ]:


df_v3 = fetch_pbp_v3("0049600063")
df_v3["actionNumber"] = df_v3["actionNumber"].astype(int)
mask = (df_v3["actionNumber"] >= 205) & (df_v3["actionNumber"] <= 230)
cols = ["actionNumber", "actionId", "period", "clock", "actionType", "subType", "description", "personId"]
print(df_v3[mask][cols].to_string()).to


# In[ ]:


#df_v3[mask][cols].to_clipboard()


# In[ ]:


from pbpstats.offline.ordering import dedupe_with_v3, patch_start_of_periods, reorder_with_v3

game = season_df[season_df["GAME_ID"].astype(int) == 49600063].copy()
game_id = "0049600063"

df = dedupe_with_v3(game, game_id, fetch_pbp_v3)
print(f"After dedupe: {len(df)} rows")

df = df.drop_duplicates(subset=["GAME_ID", "EVENTNUM"], keep="first")
df = patch_start_of_periods(df, game_id, fetch_pbp_v3)

df_ordered = reorder_with_v3(df, game_id, fetch_pbp_v3)
print(f"After reorder: {len(df_ordered)} rows")

# Check events 210-220
mask = (df_ordered["EVENTNUM"] >= 40) & (df_ordered["EVENTNUM"] <= 50)
cols = ["EVENTNUM", "PERIOD", "PCTIMESTRING", "EVENTMSGTYPE", "HOMEDESCRIPTION", "VISITORDESCRIPTION"]
print("\nRow order after full pipeline:")
print(df_ordered[mask][cols].to_string())


# In[ ]:


from pbpstats.offline.processor import set_rebound_strict_mode
from pbpstats.offline import get_possessions_from_df

set_rebound_strict_mode(False)

game = season_df[season_df["GAME_ID"].astype(int) == 49600063].copy()
# Pass None for fetch_pbp_v3_fn to skip v3 ordering
poss = get_possessions_from_df(game, fetch_pbp_v3_fn=None)
print(f"Success! {len(poss.items)} possessions")


# In[ ]:


# Aggressive mode for old seasons
combined_df, error_df = process_season(
    season=1997,
    strict_mode=False,
)


# In[ ]:

'''
