from __future__ import annotations

import argparse
import json
import sqlite3
from collections import Counter
from pathlib import Path
from typing import Any

import pandas as pd

from bbr_pbp_lookup import DEFAULT_BBR_DB_PATH, DEFAULT_NBA_RAW_DB_PATH, find_bbr_game_for_nba_game, load_bbr_play_by_play_rows
from bbr_pbp_stats import BBR_BASIC_STATS, aggregate_bbr_player_stats, normalize_person_name
from boxscore_audit import _load_single_game_pbp, build_pbp_boxscore_from_stat_rows
from cautious_rerun import DEFAULT_PARQUET, install_local_boxscore_wrapper, load_v9b_namespace


ROOT = Path(__file__).resolve().parent
DEFAULT_OUTPUT_DIR = ROOT / "bbr_override_recheck_20260315_v1"
DEFAULT_ROW_OVERRIDES_PATH = ROOT / "pbp_row_overrides.csv"
DEFAULT_STAT_OVERRIDES_PATH = ROOT / "pbp_stat_overrides.csv"
DEFAULT_AUDIT_OVERRIDES_PATH = ROOT / "boxscore_audit_overrides.csv"
DEFAULT_BOXSCORE_SOURCE_OVERRIDES_PATH = ROOT / "boxscore_source_overrides.csv"
DEFAULT_VALIDATION_OVERRIDES_PATH = ROOT / "validation_overrides.csv"
DEFAULT_MANUAL_POSS_FIXES_PATH = ROOT / "manual_poss_fixes.json"
DEFAULT_PLAYER_CROSSWALK_PATH = ROOT.parent / "fixed_data" / "crosswalks" / "player_master_crosswalk.csv"

COMPARISON_STATS = ["PTS", "AST", "STL", "BLK", "TOV", "FGA", "FGM", "3PA", "3PM", "FTA", "FTM", "OREB", "DRB", "REB"]

STAT_KEY_TO_BASIC_STATS = {
    "UnknownDistance2ptOffRebounds": ["OREB", "REB"],
    "UnknownDistance2ptDefRebounds": ["DRB", "REB"],
    "DeadBallTurnovers": ["TOV"],
    "BadPassTurnovers": ["TOV"],
    "LostBallTurnovers": ["TOV"],
    "BadPassSteals": ["STL"],
    "LostBallSteals": ["STL"],
    "UnknownDistance2ptAssists": ["AST"],
    "Arc3Assists": ["AST"],
    "AssistedUnknownDistance2pt": ["FGM", "FGA", "PTS"],
    "AssistedArc3": ["FGM", "FGA", "3PM", "3PA", "PTS"],
    "MissedArc3": ["FGA", "3PA"],
    "MissedLongMidRange": ["FGA"],
    "FtsMade": ["FTM", "FTA", "PTS"],
    "FtsMissed": ["FTA"],
}


def _open_sqlite_readonly(path: Path | str) -> sqlite3.Connection:
    db_path = Path(path)
    return sqlite3.connect(f"file:{db_path}?mode=ro&immutable=1", uri=True)


def _normalize_game_id(value: str | int) -> str:
    return str(int(value)).zfill(10)


def _season_from_game_id(game_id: str | int) -> int:
    gid = _normalize_game_id(game_id)
    prefix = gid[:3]
    if prefix not in {"002", "004"}:
        raise ValueError(f"Unsupported NBA game id format: {gid}")
    yy = int(gid[3:5])
    return 1901 + yy if yy >= 50 else 2001 + yy


def _load_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path, dtype=str).fillna("")


def _load_player_crosswalk(path: Path = DEFAULT_PLAYER_CROSSWALK_PATH) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(columns=["player_id", "bbr_slug"])

    df = pd.read_csv(path, dtype=str).fillna("")
    if "bbr_id" not in df.columns:
        return pd.DataFrame(columns=["player_id", "bbr_slug"])

    rows: list[dict[str, Any]] = []
    for _, row in df.iterrows():
        bbr_slug = str(row.get("bbr_id", "")).strip()
        if not bbr_slug:
            continue
        for key in ("nba_id", "alt_nba_id"):
            raw = str(row.get(key, "")).strip()
            if not raw:
                continue
            try:
                player_id = int(float(raw))
            except ValueError:
                continue
            if player_id <= 0:
                continue
            rows.append({"player_id": player_id, "bbr_slug": bbr_slug})

    if not rows:
        return pd.DataFrame(columns=["player_id", "bbr_slug"])

    crosswalk = pd.DataFrame(rows).drop_duplicates(subset=["player_id", "bbr_slug"]).reset_index(drop=True)
    return crosswalk


def _summarize_override_files() -> dict[str, pd.DataFrame]:
    return {
        "pbp_row_overrides": _load_csv(DEFAULT_ROW_OVERRIDES_PATH),
        "pbp_stat_overrides": _load_csv(DEFAULT_STAT_OVERRIDES_PATH),
        "boxscore_audit_overrides": _load_csv(DEFAULT_AUDIT_OVERRIDES_PATH),
        "boxscore_source_overrides": _load_csv(DEFAULT_BOXSCORE_SOURCE_OVERRIDES_PATH),
        "validation_overrides": _load_csv(DEFAULT_VALIDATION_OVERRIDES_PATH),
    }


def _prepare_single_game_df(game_df: pd.DataFrame) -> pd.DataFrame:
    df = game_df.copy()
    df.columns = [c.upper() for c in df.columns]
    if "WCTIMESTRING" not in df.columns:
        df["WCTIMESTRING"] = "00:00 AM"

    for col in [
        "HOMEDESCRIPTION",
        "VISITORDESCRIPTION",
        "NEUTRALSITEDESCRIPTION",
        "PLAYER1_NAME",
        "PLAYER2_NAME",
        "PLAYER3_NAME",
    ]:
        if col in df.columns:
            df[col] = df[col].fillna("")

    if "GAME_ID" in df.columns:
        df["GAME_ID"] = df["GAME_ID"].astype(str).str.zfill(10)

    for col in ["EVENTNUM", "EVENTMSGTYPE", "EVENTMSGACTIONTYPE", "PERIOD"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)

    for col in [
        "PLAYER1_ID",
        "PLAYER2_ID",
        "PLAYER3_ID",
        "PLAYER1_TEAM_ID",
        "PLAYER2_TEAM_ID",
        "PLAYER3_TEAM_ID",
    ]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    return df


class GameStatsContext:
    def __init__(self, *, parquet_path: Path, nba_raw_db_path: Path, bbr_db_path: Path):
        self.parquet_path = parquet_path
        self.nba_raw_db_path = nba_raw_db_path
        self.bbr_db_path = bbr_db_path
        self.namespace = load_v9b_namespace()
        install_local_boxscore_wrapper(self.namespace, self.nba_raw_db_path)
        self.namespace["DB_PATH"] = self.nba_raw_db_path
        self._parser_cache: dict[str, pd.DataFrame] = {}
        self._official_cache: dict[str, pd.DataFrame] = {}
        self._bbr_cache: dict[str, pd.DataFrame] = {}
        self._bbr_game_map: dict[str, str] = {}
        self._player_crosswalk = _load_player_crosswalk()

    def official_box(self, game_id: str) -> pd.DataFrame:
        gid = _normalize_game_id(game_id)
        cached = self._official_cache.get(gid)
        if cached is not None:
            return cached.copy()

        df_box = self.namespace["fetch_boxscore_stats"](gid).copy()
        if df_box.empty:
            result = pd.DataFrame(columns=["player_id", "team_id", "player_name", *COMPARISON_STATS])
        else:
            result = pd.DataFrame(
                {
                    "player_id": pd.to_numeric(df_box["PLAYER_ID"], errors="coerce").fillna(0).astype(int),
                    "team_id": pd.to_numeric(df_box["TEAM_ID"], errors="coerce").fillna(0).astype(int),
                    "player_name": df_box["PLAYER_NAME"].fillna("").astype(str),
                    "PTS": pd.to_numeric(df_box["PTS"], errors="coerce").fillna(0).astype(int),
                    "AST": pd.to_numeric(df_box["AST"], errors="coerce").fillna(0).astype(int),
                    "STL": pd.to_numeric(df_box["STL"], errors="coerce").fillna(0).astype(int),
                    "BLK": pd.to_numeric(df_box["BLK"], errors="coerce").fillna(0).astype(int),
                    "TOV": pd.to_numeric(df_box["TO"], errors="coerce").fillna(0).astype(int),
                    "FGA": pd.to_numeric(df_box["FGA"], errors="coerce").fillna(0).astype(int),
                    "FGM": pd.to_numeric(df_box["FGM"], errors="coerce").fillna(0).astype(int),
                    "3PA": pd.to_numeric(df_box["FG3A"], errors="coerce").fillna(0).astype(int),
                    "3PM": pd.to_numeric(df_box["FG3M"], errors="coerce").fillna(0).astype(int),
                    "FTA": pd.to_numeric(df_box["FTA"], errors="coerce").fillna(0).astype(int),
                    "FTM": pd.to_numeric(df_box["FTM"], errors="coerce").fillna(0).astype(int),
                    "OREB": pd.to_numeric(df_box["OREB"], errors="coerce").fillna(0).astype(int),
                    "DRB": pd.to_numeric(df_box["DREB"], errors="coerce").fillna(0).astype(int),
                    "REB": pd.to_numeric(df_box["REB"], errors="coerce").fillna(0).astype(int),
                }
            )
            result = result[result["player_id"] > 0].reset_index(drop=True)

        self._official_cache[gid] = result.copy()
        return result

    def parser_box(self, game_id: str) -> pd.DataFrame:
        gid = _normalize_game_id(game_id)
        cached = self._parser_cache.get(gid)
        if cached is not None:
            return cached.copy()

        game_df = _load_single_game_pbp(self.parquet_path, gid)
        if game_df.empty:
            result = pd.DataFrame(columns=["player_id", "team_id", *COMPARISON_STATS])
        else:
            game_df = _prepare_single_game_df(game_df)
            _, possessions = self.namespace["generate_darko_hybrid"](gid, game_df)
            stat_rows = getattr(possessions, "manual_player_stats", possessions.player_stats)
            result = build_pbp_boxscore_from_stat_rows(stat_rows)
            result = result[result["player_id"] > 0].reset_index(drop=True)
            for stat in COMPARISON_STATS:
                result[stat] = pd.to_numeric(result[stat], errors="coerce").fillna(0).astype(int)

        self._parser_cache[gid] = result.copy()
        return result

    def bbr_box(self, game_id: str) -> pd.DataFrame:
        gid = _normalize_game_id(game_id)
        cached = self._bbr_cache.get(gid)
        if cached is not None:
            return cached.copy()

        context, matches = find_bbr_game_for_nba_game(gid, nba_raw_db_path=self.nba_raw_db_path, bbr_db_path=self.bbr_db_path)
        if len(matches) != 1:
            raise ValueError(f"Expected exactly one BBR match for {gid}, found {len(matches)}")
        match = matches[0]
        self._bbr_game_map[gid] = match.bbr_game_id

        play_rows = load_bbr_play_by_play_rows(match.bbr_game_id, bbr_db_path=self.bbr_db_path)
        bbr_stats = aggregate_bbr_player_stats(play_rows)

        conn = _open_sqlite_readonly(self.bbr_db_path)
        try:
            player_basic = pd.read_sql_query(
                """
                SELECT team, player, player_id AS bbr_slug
                FROM player_basic
                WHERE game_id = ?
                """,
                conn,
                params=(match.bbr_game_id,),
            )
        finally:
            conn.close()

        team_code_to_id = {
            match.home_team: int(context.home_team_id),
            match.away_team: int(context.away_team_id),
        }
        player_basic["team_id"] = player_basic["team"].map(team_code_to_id).fillna(0).astype(int)
        player_basic["player_key"] = player_basic["player"].map(normalize_person_name)

        official = self.official_box(gid)[["player_id", "team_id", "player_name"]].copy()
        official["player_key"] = official["player_name"].map(normalize_person_name)

        mapping = official.merge(
            self._player_crosswalk,
            on="player_id",
            how="left",
        )
        mapping = mapping.merge(
            player_basic[["team_id", "player", "player_key", "bbr_slug"]].rename(columns={"bbr_slug": "bbr_slug_name"}),
            on=["team_id", "player_key"],
            how="left",
        )
        mapping["bbr_slug"] = mapping["bbr_slug"].fillna(mapping["bbr_slug_name"])
        mapping["bbr_player_name"] = mapping["player"]
        mapping = mapping.drop(columns=["player", "bbr_slug_name"]).drop_duplicates(subset=["player_id", "team_id"])

        result = mapping.merge(bbr_stats, on="bbr_slug", how="left")
        for stat in COMPARISON_STATS:
            result[stat] = pd.to_numeric(result[stat], errors="coerce").fillna(0).astype(int)
        result = result[["player_id", "team_id", "player_name", "bbr_slug", *COMPARISON_STATS]]

        self._bbr_cache[gid] = result.copy()
        return result

    def merged_player_stats(self, game_id: str) -> pd.DataFrame:
        gid = _normalize_game_id(game_id)
        official = self.official_box(gid).copy()
        parser = self.parser_box(gid).copy()
        bbr = self.bbr_box(gid).copy()

        merged = official.merge(
            parser.rename(columns={stat: f"PARSER_{stat}" for stat in COMPARISON_STATS}),
            on=["player_id", "team_id"],
            how="outer",
        )
        merged = merged.merge(
            bbr.rename(columns={stat: f"BBR_{stat}" for stat in COMPARISON_STATS}),
            on=["player_id", "team_id", "player_name"],
            how="outer",
        )

        for stat in COMPARISON_STATS:
            merged[stat] = pd.to_numeric(merged.get(stat, 0), errors="coerce").fillna(0).astype(int)
            merged[f"PARSER_{stat}"] = pd.to_numeric(merged.get(f"PARSER_{stat}", 0), errors="coerce").fillna(0).astype(int)
            merged[f"BBR_{stat}"] = pd.to_numeric(merged.get(f"BBR_{stat}", 0), errors="coerce").fillna(0).astype(int)

        merged["player_name"] = merged["player_name"].fillna("")
        merged["player_id"] = pd.to_numeric(merged["player_id"], errors="coerce").fillna(0).astype(int)
        merged["team_id"] = pd.to_numeric(merged["team_id"], errors="coerce").fillna(0).astype(int)
        return merged


def _expanded_override_stat_rows(
    override_name: str,
    override_df: pd.DataFrame,
    context: GameStatsContext,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if override_df.empty:
        return rows

    for override in override_df.to_dict(orient="records"):
        game_id = _normalize_game_id(override["game_id"])
        merged = context.merged_player_stats(game_id)

        player_id_key = "PLAYER_ID" if override_name == "boxscore_source_overrides" else "player_id"
        team_id_key = "TEAM_ID" if override_name == "boxscore_source_overrides" else "team_id"
        player_id = int(float(override[player_id_key])) if override.get(player_id_key) else 0
        team_id = int(float(override[team_id_key])) if override.get(team_id_key) else 0
        matched = merged[(merged["player_id"] == player_id) & (merged["team_id"] == team_id)]
        if matched.empty:
            rows.append(
                {
                    "override_file": override_name,
                    "game_id": game_id,
                    "team_id": team_id,
                    "player_id": player_id,
                    "player_name": "",
                    "check_stat": "",
                    "status": "unmapped_player",
                    "notes": override.get("notes", ""),
                }
            )
            continue

        player_row = matched.iloc[0]

        if override_name == "boxscore_audit_overrides":
            impacted_stats = [str(override["stat"]).upper()]
        elif override_name == "boxscore_source_overrides":
            impacted_stats = COMPARISON_STATS[:]
        else:
            impacted_stats = STAT_KEY_TO_BASIC_STATS.get(str(override["stat_key"]), [])

        if not impacted_stats:
            rows.append(
                {
                    "override_file": override_name,
                    "game_id": game_id,
                    "team_id": team_id,
                    "player_id": player_id,
                    "player_name": player_row["player_name"],
                    "check_stat": "",
                    "status": "unsupported_stat_key",
                    "notes": override.get("notes", ""),
                }
            )
            continue

        for stat in impacted_stats:
            parser_value = int(player_row.get(f"PARSER_{stat}", 0))
            official_value = int(player_row.get(stat, 0))
            bbr_value = int(player_row.get(f"BBR_{stat}", 0))

            if parser_value == bbr_value and official_value == bbr_value:
                status = "all_match"
            elif parser_value == bbr_value:
                status = "parser_matches_bbr"
            elif official_value == bbr_value:
                status = "official_matches_bbr"
            else:
                status = "parser_official_bbr_disagree"

            rows.append(
                {
                    "override_file": override_name,
                    "game_id": game_id,
                    "team_id": team_id,
                    "player_id": player_id,
                    "player_name": player_row["player_name"],
                    "check_stat": stat,
                    "parser_value": parser_value,
                    "official_value": official_value,
                    "bbr_value": bbr_value,
                    "status": status,
                    "notes": override.get("notes", ""),
                }
            )

    return rows


def _override_game_set(frame: pd.DataFrame) -> set[str]:
    if frame.empty or "game_id" not in frame.columns:
        return set()
    return {_normalize_game_id(value) for value in frame["game_id"].tolist()}


def _row_override_game_rows(
    row_overrides: pd.DataFrame,
    context: GameStatsContext,
    *,
    stat_override_games: set[str],
    audit_override_games: set[str],
    source_override_games: set[str],
    validation_override_games: set[str],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if row_overrides.empty:
        return rows

    grouped = row_overrides.groupby(row_overrides["game_id"].map(_normalize_game_id))
    for game_id, game_rows in grouped:
        merged = context.merged_player_stats(game_id)
        other_override_files: list[str] = []
        if game_id in stat_override_games:
            other_override_files.append("pbp_stat_overrides")
        if game_id in audit_override_games:
            other_override_files.append("boxscore_audit_overrides")
        if game_id in source_override_games:
            other_override_files.append("boxscore_source_overrides")
        if game_id in validation_override_games:
            other_override_files.append("validation_overrides")

        mismatch_cells = 0
        mismatch_players = 0
        for _, player_row in merged.iterrows():
            player_mismatch = False
            for stat in COMPARISON_STATS:
                if int(player_row.get(f"PARSER_{stat}", 0)) != int(player_row.get(f"BBR_{stat}", 0)):
                    mismatch_cells += 1
                    player_mismatch = True
            if player_mismatch:
                mismatch_players += 1

        row_override_only = not other_override_files
        if mismatch_cells == 0:
            status = "parser_matches_bbr"
        elif row_override_only:
            status = "needs_review_row_only"
        else:
            status = "needs_review_mixed_overrides"

        rows.append(
            {
                "game_id": game_id,
                "row_override_count": int(len(game_rows)),
                "row_override_only": row_override_only,
                "other_override_files": ",".join(other_override_files),
                "mismatch_players_parser_vs_bbr": mismatch_players,
                "mismatch_cells_parser_vs_bbr": mismatch_cells,
                "status": status,
            }
        )
    return rows


def _validation_rows(validation_df: pd.DataFrame, context: GameStatsContext) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if validation_df.empty:
        return rows

    for override in validation_df.to_dict(orient="records"):
        game_id = _normalize_game_id(override["game_id"])
        merged = context.merged_player_stats(game_id)
        mismatch_cells = 0
        for _, player_row in merged.iterrows():
            for stat in COMPARISON_STATS:
                mismatch_cells += int(int(player_row.get(f"PARSER_{stat}", 0)) != int(player_row.get(f"BBR_{stat}", 0)))
        rows.append(
            {
                "game_id": game_id,
                "action": override.get("action", ""),
                "tolerance": override.get("tolerance", ""),
                "status": "parser_matches_bbr" if mismatch_cells == 0 else "needs_review",
                "notes": override.get("notes", ""),
            }
        )
    return rows


def main() -> int:
    parser = argparse.ArgumentParser(description="Recheck local override inventory against BBR play-by-play data")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--parquet-path", type=Path, default=DEFAULT_PARQUET)
    parser.add_argument("--nba-raw-db-path", type=Path, default=DEFAULT_NBA_RAW_DB_PATH)
    parser.add_argument("--bbr-db-path", type=Path, default=DEFAULT_BBR_DB_PATH)
    args = parser.parse_args()

    output_dir = args.output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    overrides = _summarize_override_files()
    stat_override_games = _override_game_set(overrides["pbp_stat_overrides"])
    audit_override_games = _override_game_set(overrides["boxscore_audit_overrides"])
    source_override_games = _override_game_set(overrides["boxscore_source_overrides"])
    validation_override_games = _override_game_set(overrides["validation_overrides"])
    unique_games: set[str] = set()
    for frame in overrides.values():
        if not frame.empty and "game_id" in frame.columns:
            unique_games.update(_normalize_game_id(value) for value in frame["game_id"].tolist())

    context = GameStatsContext(
        parquet_path=args.parquet_path.resolve(),
        nba_raw_db_path=args.nba_raw_db_path.resolve(),
        bbr_db_path=args.bbr_db_path.resolve(),
    )

    row_game_rows = _row_override_game_rows(
        overrides["pbp_row_overrides"],
        context,
        stat_override_games=stat_override_games,
        audit_override_games=audit_override_games,
        source_override_games=source_override_games,
        validation_override_games=validation_override_games,
    )
    stat_rows = _expanded_override_stat_rows("pbp_stat_overrides", overrides["pbp_stat_overrides"], context)
    audit_rows = _expanded_override_stat_rows("boxscore_audit_overrides", overrides["boxscore_audit_overrides"], context)
    source_rows = _expanded_override_stat_rows("boxscore_source_overrides", overrides["boxscore_source_overrides"], context)
    validation_rows = _validation_rows(overrides["validation_overrides"], context)

    pd.DataFrame(row_game_rows).to_csv(output_dir / "row_override_game_recheck.csv", index=False)
    pd.DataFrame(stat_rows).to_csv(output_dir / "pbp_stat_override_recheck.csv", index=False)
    pd.DataFrame(audit_rows).to_csv(output_dir / "boxscore_audit_override_recheck.csv", index=False)
    pd.DataFrame(source_rows).to_csv(output_dir / "boxscore_source_override_recheck.csv", index=False)
    pd.DataFrame(validation_rows).to_csv(output_dir / "validation_override_recheck.csv", index=False)

    summary = {
        "games_checked": len(unique_games),
        "row_override_games": len(row_game_rows),
        "row_override_status_counts": Counter(row["status"] for row in row_game_rows),
        "pbp_stat_override_rows": len(stat_rows),
        "pbp_stat_override_status_counts": Counter(row["status"] for row in stat_rows),
        "boxscore_audit_override_rows": len(audit_rows),
        "boxscore_audit_override_status_counts": Counter(row["status"] for row in audit_rows),
        "boxscore_source_override_rows": len(source_rows),
        "boxscore_source_override_status_counts": Counter(row["status"] for row in source_rows),
        "validation_override_rows": len(validation_rows),
        "validation_override_status_counts": Counter(row["status"] for row in validation_rows),
        "manual_poss_fixes_path": str(DEFAULT_MANUAL_POSS_FIXES_PATH),
    }
    (output_dir / "summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")

    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
