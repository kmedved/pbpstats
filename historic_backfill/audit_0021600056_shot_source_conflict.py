from __future__ import annotations

import json
import sqlite3
import zlib
from pathlib import Path

import pandas as pd

from bbr_pbp_lookup import (
    DEFAULT_BBR_DB_PATH,
    DEFAULT_NBA_RAW_DB_PATH,
    find_bbr_game_for_nba_game,
    load_bbr_play_by_play_rows,
)


ROOT = Path(__file__).resolve().parent
DEFAULT_OUTPUT_DIR = ROOT / "audit_0021600056_shot_source_conflict_20260315_v1"
DEFAULT_PARQUET_PATH = ROOT / "playbyplayv2.parq"
DEFAULT_NBA_RAW_DB_PATH = ROOT / "nba_raw.db"
DEFAULT_TPDEV_BOX_PATH = ROOT.parent / "fixed_data" / "raw_input_data" / "tpdev_data" / "tpdev_box.parq"
DEFAULT_STAT_OVERRIDES_PATH = ROOT / "pbp_stat_overrides.csv"
DEFAULT_STAT_NECESSITY_PATH = ROOT / "pbp_stat_override_necessity_20260315_v2" / "pbp_stat_override_necessity.csv"
DEFAULT_BBR_RECHECK_PATH = ROOT / "bbr_override_recheck_20260315_v3" / "pbp_stat_override_recheck.csv"

GAME_ID = "0021600056"
TARGET_TEAM_ID = 1610612766
TARGET_PERIOD = 1
TARGET_CLOCK = "5:36"
TARGET_EVENT_NUM = 73
TARGET_PLAYER_IDS = (201587, 202689)


def _open_sqlite_readonly(path: Path | str) -> sqlite3.Connection:
    db_path = Path(path)
    return sqlite3.connect(f"file:{db_path}?mode=ro&immutable=1", uri=True)


def _load_json_blob(raw_value):
    if isinstance(raw_value, memoryview):
        raw_value = raw_value.tobytes()
    if isinstance(raw_value, bytes):
        try:
            raw_value = zlib.decompress(raw_value).decode("utf-8")
        except zlib.error:
            raw_value = raw_value.decode("utf-8")
    if isinstance(raw_value, str):
        return json.loads(raw_value)
    raise TypeError(f"Unsupported raw response type: {type(raw_value)!r}")


def _rows_as_dicts(result_set: dict) -> list[dict]:
    headers = result_set.get("headers", [])
    return [dict(zip(headers, row)) for row in result_set.get("rowSet", [])]


def _load_raw_pbp_window(parquet_path: Path) -> pd.DataFrame:
    df = pd.read_parquet(
        parquet_path,
        filters=[("GAME_ID", "==", str(int(GAME_ID)))],
        columns=[
            "GAME_ID",
            "EVENTNUM",
            "EVENTMSGTYPE",
            "EVENTMSGACTIONTYPE",
            "PERIOD",
            "PCTIMESTRING",
            "HOMEDESCRIPTION",
            "VISITORDESCRIPTION",
            "PLAYER1_ID",
            "PLAYER1_NAME",
            "PLAYER1_TEAM_ID",
        ],
    )
    df["GAME_ID"] = df["GAME_ID"].astype(str).str.zfill(10)
    df["EVENTNUM_INT"] = pd.to_numeric(df["EVENTNUM"], errors="coerce").fillna(-1).astype(int)
    df = df[df["GAME_ID"] == GAME_ID]
    df = df[
        (df["PERIOD"].astype(str) == str(TARGET_PERIOD))
        & (
            (df["PCTIMESTRING"] == TARGET_CLOCK)
            | (df["EVENTNUM_INT"].between(TARGET_EVENT_NUM - 2, TARGET_EVENT_NUM + 2))
        )
    ]
    return df.sort_values(["EVENTNUM_INT"]).reset_index(drop=True)


def _load_official_shots_rows(nba_raw_db_path: Path) -> pd.DataFrame:
    conn = _open_sqlite_readonly(nba_raw_db_path)
    try:
        row = conn.execute(
            "SELECT data FROM raw_responses WHERE game_id = ? AND endpoint = 'shots' AND team_id = ?",
            (GAME_ID, TARGET_TEAM_ID),
        ).fetchone()
    finally:
        conn.close()
    payload = _load_json_blob(row[0])
    rows = _rows_as_dicts(payload["resultSets"][0])
    df = pd.DataFrame(rows)
    df["GAME_EVENT_ID"] = pd.to_numeric(df["GAME_EVENT_ID"], errors="coerce").fillna(-1).astype(int)
    df["PERIOD"] = pd.to_numeric(df["PERIOD"], errors="coerce").fillna(-1).astype(int)
    df["MINUTES_REMAINING"] = pd.to_numeric(df["MINUTES_REMAINING"], errors="coerce").fillna(-1).astype(int)
    df["SECONDS_REMAINING"] = pd.to_numeric(df["SECONDS_REMAINING"], errors="coerce").fillna(-1).astype(int)
    return df[
        (df["GAME_EVENT_ID"] == TARGET_EVENT_NUM)
        | (
            (df["PERIOD"] == TARGET_PERIOD)
            & (df["MINUTES_REMAINING"] == 5)
            & (df["SECONDS_REMAINING"] == 36)
        )
    ].reset_index(drop=True)


def _load_official_box_rows(nba_raw_db_path: Path) -> pd.DataFrame:
    conn = _open_sqlite_readonly(nba_raw_db_path)
    try:
        row = conn.execute(
            "SELECT data FROM raw_responses WHERE game_id = ? AND endpoint = 'boxscore' AND team_id IS NULL",
            (GAME_ID,),
        ).fetchone()
    finally:
        conn.close()
    payload = _load_json_blob(row[0])
    player_stats = next(result for result in payload["resultSets"] if result.get("name") == "PlayerStats")
    df = pd.DataFrame(_rows_as_dicts(player_stats))
    df["PLAYER_ID"] = pd.to_numeric(df["PLAYER_ID"], errors="coerce").fillna(0).astype(int)
    df["TEAM_ID"] = pd.to_numeric(df["TEAM_ID"], errors="coerce").fillna(0).astype(int)
    df = df[(df["TEAM_ID"] == TARGET_TEAM_ID) & (df["PLAYER_ID"].isin(TARGET_PLAYER_IDS))].copy()
    return df[
        [
            "TEAM_ID",
            "PLAYER_ID",
            "PLAYER_NAME",
            "FGM",
            "FGA",
            "FG3M",
            "FG3A",
            "OREB",
            "DREB",
            "REB",
            "AST",
            "TO",
            "PTS",
        ]
    ].reset_index(drop=True)


def _load_bbr_rows(bbr_db_path: Path, nba_raw_db_path: Path) -> tuple[pd.DataFrame, pd.DataFrame, str]:
    _, matches = find_bbr_game_for_nba_game(GAME_ID, nba_raw_db_path=nba_raw_db_path, bbr_db_path=bbr_db_path)
    if len(matches) != 1:
        raise ValueError(f"Expected exactly one BBR match for {GAME_ID}, found {len(matches)}")
    bbr_game_id = matches[0].bbr_game_id

    bbr_pbp = pd.DataFrame(
        load_bbr_play_by_play_rows(
            bbr_game_id,
            bbr_db_path=bbr_db_path,
            period=TARGET_PERIOD,
            clock=TARGET_CLOCK,
        )
    )

    conn = _open_sqlite_readonly(bbr_db_path)
    try:
        bbr_box = pd.read_sql_query(
            """
            SELECT team, player, player_id AS bbr_slug, fg, fga, fg3, fg3a, orb, drb, trb, ast, tov, pts
            FROM player_basic
            WHERE game_id = ? AND team = 'CHO' AND player IN ('Nicolas Batum', 'Kemba Walker')
            ORDER BY player
            """,
            conn,
            params=(bbr_game_id,),
        )
    finally:
        conn.close()

    return bbr_pbp, bbr_box, bbr_game_id


def _load_tpdev_rows(tpdev_box_path: Path) -> pd.DataFrame:
    df = pd.read_parquet(
        tpdev_box_path,
        filters=[
            ("Game_SingleGame", "==", int(GAME_ID)),
            ("Team_SingleGame", "==", TARGET_TEAM_ID),
            ("NbaDotComID", "in", list(TARGET_PLAYER_IDS)),
        ],
        columns=["Game_SingleGame", "Team_SingleGame", "NbaDotComID", "FullName", "FGA", "FGM", "10_17ft_FGA", "PTS", "AST", "TOV", "OREB", "DRB"],
    )
    return df.sort_values(["NbaDotComID"]).reset_index(drop=True)


def _load_override_rows() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    overrides = pd.read_csv(DEFAULT_STAT_OVERRIDES_PATH, dtype=str).fillna("")
    overrides["game_id"] = overrides["game_id"].astype(str).str.zfill(10)
    overrides = overrides[overrides["game_id"] == GAME_ID].reset_index(drop=True)

    necessity = pd.read_csv(DEFAULT_STAT_NECESSITY_PATH, dtype=str).fillna("")
    necessity["game_id"] = necessity["game_id"].astype(str).str.zfill(10)
    necessity = necessity[necessity["game_id"] == GAME_ID].reset_index(drop=True)

    bbr_recheck = pd.read_csv(DEFAULT_BBR_RECHECK_PATH, dtype=str).fillna("")
    bbr_recheck["game_id"] = bbr_recheck["game_id"].astype(str).str.zfill(10)
    bbr_recheck = bbr_recheck[bbr_recheck["game_id"] == GAME_ID].reset_index(drop=True)
    return overrides, necessity, bbr_recheck


def build_audit() -> tuple[dict, dict[str, pd.DataFrame]]:
    raw_pbp = _load_raw_pbp_window(DEFAULT_PARQUET_PATH)
    official_shots = _load_official_shots_rows(DEFAULT_NBA_RAW_DB_PATH)
    official_box = _load_official_box_rows(DEFAULT_NBA_RAW_DB_PATH)
    bbr_pbp, bbr_box, bbr_game_id = _load_bbr_rows(DEFAULT_BBR_DB_PATH, DEFAULT_NBA_RAW_DB_PATH)
    tpdev_box = _load_tpdev_rows(DEFAULT_TPDEV_BOX_PATH)
    overrides, necessity, bbr_recheck = _load_override_rows()

    raw_target = raw_pbp[raw_pbp["EVENTNUM_INT"] == TARGET_EVENT_NUM]
    if raw_target.empty:
        raw_target = raw_pbp[
            (raw_pbp["PCTIMESTRING"] == TARGET_CLOCK)
            & (pd.to_numeric(raw_pbp["PLAYER1_ID"], errors="coerce").fillna(0).astype(int).isin(TARGET_PLAYER_IDS))
        ]

    tpdev_fga = {
        str(row["FullName"]).rsplit(" ", 1)[0]: int(row["FGA"])
        for _, row in tpdev_box.iterrows()
    }
    official_box_fga = {
        row["PLAYER_NAME"]: int(row["FGA"])
        for _, row in official_box.iterrows()
    }
    bbr_box_fga = {
        row["player"]: int(row["fga"])
        for _, row in bbr_box.iterrows()
    }

    summary = {
        "game_id": GAME_ID,
        "bbr_game_id": bbr_game_id,
        "focus": {
            "period": TARGET_PERIOD,
            "clock": TARGET_CLOCK,
            "event_num": TARGET_EVENT_NUM,
            "team_id": TARGET_TEAM_ID,
        },
        "conflict": {
            "raw_nba_pbp_player": str(raw_target.iloc[0]["PLAYER1_NAME"]) if not raw_target.empty else "",
            "bbr_pbp_text": str(bbr_pbp.iloc[0].get("home_play", "") or bbr_pbp.iloc[0].get("away_play", "")) if not bbr_pbp.empty else "",
            "official_shots_player": str(official_shots.iloc[0]["PLAYER_NAME"]) if not official_shots.empty else "",
            "official_shots_distance": int(official_shots.iloc[0]["SHOT_DISTANCE"]) if not official_shots.empty else None,
        },
        "box_alignment": {
            "official_box_fga": official_box_fga,
            "bbr_box_fga": bbr_box_fga,
            "tpdev_box_fga": tpdev_fga,
            "tpdev_10_17ft_fga": {
                str(row["FullName"]).rsplit(" ", 1)[0]: int(row["10_17ft_FGA"])
                for _, row in tpdev_box.iterrows()
            },
        },
        "source_split": {
            "supports_batum_reassignment": [
                "official_shots_cache",
                "official_boxscore",
                "bbr_boxscore",
            ],
            "supports_walker_miss": [
                "raw_nba_pbp",
                "bbr_pbp",
                "original_tpdev_box",
            ],
        },
        "current_override_position": "Current overrides intentionally follow the official shots cache and boxscore side of the split, but this game remains a true source conflict: both PBP feeds and original tpdev support Walker, while the official shots cache plus official and BBR boxscores support Batum.",
        "override_row_count": int(len(overrides)),
        "unsupported_override_row_count": int((necessity["status"] == "unsupported_stat_key").sum()),
    }

    tables = {
        "raw_pbp_window": raw_pbp,
        "bbr_pbp_rows": bbr_pbp,
        "official_shots_rows": official_shots,
        "official_box_rows": official_box,
        "bbr_box_rows": bbr_box,
        "tpdev_box_rows": tpdev_box,
        "override_rows": overrides,
        "override_necessity_rows": necessity,
        "bbr_recheck_rows": bbr_recheck,
    }
    return summary, tables


def main() -> int:
    output_dir = DEFAULT_OUTPUT_DIR.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    summary, tables = build_audit()
    (output_dir / "summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")

    for name, df in tables.items():
        df.to_csv(output_dir / f"{name}.csv", index=False)

    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
