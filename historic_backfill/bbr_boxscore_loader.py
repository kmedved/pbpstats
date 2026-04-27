from __future__ import annotations

import re
import sqlite3
import unicodedata
from functools import lru_cache
from pathlib import Path

import pandas as pd

from audit_minutes_plus_minus import load_official_boxscore_df, parse_official_minutes
from bbr_pbp_lookup import (
    DEFAULT_BBR_DB_PATH,
    DEFAULT_NBA_RAW_DB_PATH,
    find_bbr_game_for_nba_game,
)


ROOT = Path(__file__).resolve().parent
DEFAULT_PLAYER_CROSSWALK_PATH = (
    ROOT.parent / "fixed_data" / "crosswalks" / "player_master_crosswalk.csv"
)


def _open_sqlite_readonly(path: Path | str) -> sqlite3.Connection:
    db_path = Path(path)
    return sqlite3.connect(f"file:{db_path}?mode=ro&immutable=1", uri=True)


def _normalize_person_name(value: str) -> str:
    ascii_text = (
        unicodedata.normalize("NFKD", str(value))
        .encode("ascii", "ignore")
        .decode("ascii")
        .lower()
    )
    return re.sub(r"[^a-z0-9]+", "", ascii_text)


@lru_cache(maxsize=4)
def _load_bbr_to_nba_map(crosswalk_path: str) -> dict[str, int]:
    path = Path(crosswalk_path)
    if not path.exists():
        return {}

    crosswalk = pd.read_csv(path, usecols=["bbr_id", "nba_id", "alt_nba_id"])
    mapping: dict[str, int] = {}
    for row in crosswalk.itertuples(index=False):
        bbr_id = str(getattr(row, "bbr_id", "") or "").strip()
        if not bbr_id:
            continue
        for candidate in (getattr(row, "nba_id", None), getattr(row, "alt_nba_id", None)):
            player_id = pd.to_numeric(candidate, errors="coerce")
            if pd.notna(player_id) and int(player_id) > 0:
                mapping.setdefault(bbr_id, int(player_id))
                break
    return mapping


def _build_official_name_fallback_map(official_df: pd.DataFrame) -> dict[int, dict[str, int]]:
    fallback: dict[int, dict[str, int]] = {}
    if official_df.empty:
        return fallback

    cleaned = official_df.copy()
    cleaned["normalized_name"] = cleaned["player_name"].map(_normalize_person_name)
    cleaned = cleaned[cleaned["normalized_name"] != ""].copy()
    if cleaned.empty:
        return fallback

    for team_id, group in cleaned.groupby("team_id"):
        unique_group = group.groupby("normalized_name").filter(lambda g: len(g) == 1)
        fallback[int(team_id)] = {
            row.normalized_name: int(row.player_id)
            for row in unique_group.itertuples(index=False)
        }
    return fallback


def load_bbr_boxscore_df(
    nba_game_id: str | int,
    *,
    nba_raw_db_path: Path | str = DEFAULT_NBA_RAW_DB_PATH,
    bbr_db_path: Path | str = DEFAULT_BBR_DB_PATH,
    crosswalk_path: Path | str = DEFAULT_PLAYER_CROSSWALK_PATH,
) -> pd.DataFrame:
    empty = pd.DataFrame(
        columns=[
            "game_id",
            "player_id",
            "team_id",
            "player_name_bbr_box",
            "Minutes_bbr_box",
            "Plus_Minus_bbr_box",
            "bbr_game_id",
        ]
    )

    try:
        context, matches = find_bbr_game_for_nba_game(
            str(nba_game_id).zfill(10),
            nba_raw_db_path=nba_raw_db_path,
            bbr_db_path=bbr_db_path,
        )
    except Exception:
        return empty

    if len(matches) != 1:
        return empty

    match = matches[0]
    conn = _open_sqlite_readonly(bbr_db_path)
    try:
        player_basic = pd.read_sql_query(
            """
            SELECT team, player, player_id AS bbr_player_id, mp, plus_minus
            FROM player_basic
            WHERE game_id = ?
            ORDER BY team, row_index
            """,
            conn,
            params=[match.bbr_game_id],
        )
    finally:
        conn.close()

    if player_basic.empty:
        return empty

    team_map = {
        str(match.away_team): int(context.away_team_id),
        str(match.home_team): int(context.home_team_id),
    }
    bbr_to_nba = _load_bbr_to_nba_map(str(Path(crosswalk_path)))
    official_df = load_official_boxscore_df(Path(nba_raw_db_path), str(nba_game_id).zfill(10))
    official_name_map = _build_official_name_fallback_map(official_df)

    player_basic["team_id"] = (
        player_basic["team"].map(team_map).fillna(0).astype(int)
    )
    player_basic["player_id"] = (
        player_basic["bbr_player_id"]
        .fillna("")
        .astype(str)
        .map(bbr_to_nba)
        .fillna(0)
        .astype(int)
    )
    unresolved = player_basic["player_id"] <= 0
    if unresolved.any():
        fallback_ids = []
        for row in player_basic.loc[unresolved].itertuples(index=False):
            team_name_map = official_name_map.get(int(row.team_id), {})
            fallback_ids.append(
                team_name_map.get(_normalize_person_name(str(row.player or "")), 0)
            )
        player_basic.loc[unresolved, "player_id"] = fallback_ids

    player_basic["Minutes_bbr_box"] = player_basic["mp"].map(parse_official_minutes)
    player_basic["Plus_Minus_bbr_box"] = (
        pd.to_numeric(player_basic["plus_minus"], errors="coerce").fillna(0.0).astype(float)
    )
    player_basic["game_id"] = str(nba_game_id).zfill(10)
    player_basic["player_name_bbr_box"] = player_basic["player"].fillna("").astype(str)
    player_basic["bbr_game_id"] = match.bbr_game_id

    player_basic = player_basic[
        (player_basic["team_id"] > 0) & (player_basic["player_id"] > 0)
    ].copy()
    if player_basic.empty:
        return empty

    return player_basic[
        [
            "game_id",
            "player_id",
            "team_id",
            "player_name_bbr_box",
            "Minutes_bbr_box",
            "Plus_Minus_bbr_box",
            "bbr_game_id",
        ]
    ].reset_index(drop=True)
