from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import pandas as pd

ROOT = Path(__file__).resolve().parent
DEFAULT_OUTPUT_DIR = ROOT / "override_provenance_20260315_v1"
DEFAULT_ROW_OVERRIDES_PATH = ROOT / "pbp_row_overrides.csv"
DEFAULT_STAT_OVERRIDES_PATH = ROOT / "pbp_stat_overrides.csv"
DEFAULT_AUDIT_OVERRIDES_PATH = ROOT / "boxscore_audit_overrides.csv"
DEFAULT_BOXSCORE_SOURCE_OVERRIDES_PATH = ROOT / "boxscore_source_overrides.csv"
DEFAULT_VALIDATION_OVERRIDES_PATH = ROOT / "validation_overrides.csv"
DEFAULT_MANUAL_POSS_FIXES_PATH = ROOT / "manual_poss_fixes.json"
DEFAULT_ROW_NECESSITY_PATH = ROOT / "pbp_row_override_necessity_20260315_v3" / "pbp_row_override_necessity.csv"
DEFAULT_STAT_NECESSITY_PATH = ROOT / "pbp_stat_override_necessity_20260315_v2" / "pbp_stat_override_necessity.csv"
DEFAULT_BBR_RECHECK_DIR = ROOT / "bbr_override_recheck_20260315_v3"
DEFAULT_ROW_WINDOW_AUDIT_PATH = ROOT / "pbp_row_override_bbr_window_audit_20260315_v1" / "pbp_row_override_bbr_window_audit.csv"
DEFAULT_TPDEV_BOX_CANDIDATES = [
    ROOT.parent / "fixed_data" / "raw_input_data" / "tpdev_data" / "tpdev_box.parq",
    ROOT.parent / "calculated_data" / "temp" / "tpdev_box.parq",
]

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


def _normalize_game_id(value: str | int) -> str:
    return str(int(value)).zfill(10)


def _load_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path, dtype=str).fillna("")


def _season_from_game_id(game_id: str | int) -> int:
    gid = _normalize_game_id(game_id)
    yy = int(gid[3:5])
    return 1901 + yy if yy >= 50 else 2001 + yy


def _find_tpdev_box_path() -> Path | None:
    for path in DEFAULT_TPDEV_BOX_CANDIDATES:
        if path.exists():
            return path
    return None


class TpdevBoxLookup:
    def __init__(self, path: Path | None):
        self.path = path
        self._cache: dict[tuple[int, int, int], pd.DataFrame] = {}

    def player_rows(self, game_id: str, team_id: str | int, player_id: str | int) -> pd.DataFrame:
        if self.path is None:
            return pd.DataFrame()
        key = (int(game_id), int(float(team_id)), int(float(player_id)))
        cached = self._cache.get(key)
        if cached is not None:
            return cached.copy()
        filters = [
            ("Game_SingleGame", "==", key[0]),
            ("Team_SingleGame", "==", key[1]),
            ("NbaDotComID", "==", key[2]),
        ]
        columns = [
            "Game_SingleGame",
            "Team_SingleGame",
            "NbaDotComID",
            "FullName",
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
            "10_17ft_FGA",
        ]
        df = pd.read_parquet(self.path, columns=columns, filters=filters)
        self._cache[key] = df.copy()
        return df


def _basis_tags(notes: str, override_file: str) -> list[str]:
    lowered = (notes or "").lower()
    tags: list[str] = []
    if "bbr play-by-play" in lowered or "bbr play by play" in lowered:
        tags.append("bbr_pbp")
    if "bbr boxscore" in lowered or "basketball reference boxscore" in lowered:
        tags.append("bbr_boxscore")
    if "historical pbp" in lowered or "raw nba pbp" in lowered or "play-by-play" in lowered:
        tags.append("raw_nba_pbp")
    if "official shots cache" in lowered or "shots cache" in lowered:
        tags.append("official_shots_cache")
    if "official boxscore" in lowered or "cached nba boxscore" in lowered or "nba boxscore" in lowered:
        tags.append("official_boxscore")
    if "incomplete historical pbp" in lowered:
        tags.append("incomplete_historical_pbp")
    if "placeholder" in lowered or "orphan" in lowered or "stray" in lowered:
        tags.append("placeholder_orphan_row")
    if "plus-minus" in lowered:
        tags.append("plus_minus_patch")
    if "distance-tagged" in lowered or "10-foot" in lowered:
        tags.append("shot_distance_feature")
    if override_file == "pbp_row_overrides" and not tags:
        tags.append("raw_pbp_order_fix")
    if not tags:
        tags.append("manual_review")
    return tags


def _scope_fields(override_file: str) -> tuple[str, str]:
    if override_file in {"pbp_row_overrides", "pbp_stat_overrides", "boxscore_source_overrides"}:
        return "production_output", "yes"
    if override_file == "manual_poss_fixes":
        return "production_output", "yes"
    if override_file == "boxscore_audit_overrides":
        return "audit_only", "no"
    if override_file == "validation_overrides":
        return "validation_only", "indirect"
    return "unknown", "unknown"


def _load_bbr_recheck_tables(recheck_dir: Path) -> dict[str, pd.DataFrame]:
    return {
        "pbp_stat_overrides": _load_csv(recheck_dir / "pbp_stat_override_recheck.csv"),
        "boxscore_audit_overrides": _load_csv(recheck_dir / "boxscore_audit_override_recheck.csv"),
        "boxscore_source_overrides": _load_csv(recheck_dir / "boxscore_source_override_recheck.csv"),
        "validation_overrides": _load_csv(recheck_dir / "validation_override_recheck.csv"),
    }


def _row_necessity_lookup(path: Path) -> dict[str, dict[str, Any]]:
    df = _load_csv(path)
    if df.empty:
        return {}
    return {str(row["game_id"]).zfill(10): row for row in df.to_dict(orient="records")}


def _stat_necessity_lookup(path: Path) -> dict[tuple[str, str, str, str, str], dict[str, Any]]:
    df = _load_csv(path)
    if df.empty:
        return {}
    lookup = {}
    for row in df.to_dict(orient="records"):
        key = (
            str(row["game_id"]).zfill(10),
            str(row["team_id"]),
            str(row["player_id"]),
            str(row["stat_key"]),
            str(row["stat_value"]),
        )
        lookup[key] = row
    return lookup


def _row_window_audit_lookup(path: Path) -> dict[tuple[str, str, str, str], dict[str, Any]]:
    df = _load_csv(path)
    if df.empty:
        return {}
    lookup = {}
    for row in df.to_dict(orient="records"):
        key = (
            _normalize_game_id(row["game_id"]),
            str(row["action"]),
            str(row["event_num"]),
            str(row["anchor_event_num"]),
        )
        lookup[key] = row
    return lookup


def _summarize_bbr_matches(matches: pd.DataFrame) -> tuple[str, str]:
    if matches.empty:
        return "", ""
    statuses = sorted(set(matches["status"].astype(str)))
    stats = sorted(set(matches.get("check_stat", pd.Series(dtype=str)).astype(str)))
    return "|".join(statuses), "|".join(stat for stat in stats if stat)


def _tpdev_basic_value(df: pd.DataFrame, stat: str) -> str:
    if df.empty:
        return ""
    if stat == "REB":
        if "OREB" not in df.columns or "DRB" not in df.columns:
            return ""
        try:
            value = float(df.iloc[0]["OREB"]) + float(df.iloc[0]["DRB"])
        except (TypeError, ValueError):
            return ""
    else:
        if stat not in df.columns:
            return ""
        value = df.iloc[0][stat]
    try:
        return str(int(float(value)))
    except (TypeError, ValueError):
        return str(value)


def _build_row_override_rows(
    row_overrides: pd.DataFrame,
    row_necessity: dict[str, dict[str, Any]],
    row_window_audit: dict[tuple[str, str, str, str], dict[str, Any]],
) -> list[dict[str, Any]]:
    rows = []
    for row in row_overrides.to_dict(orient="records"):
        game_id = _normalize_game_id(row["game_id"])
        necessity = row_necessity.get(game_id, {})
        audit_row = row_window_audit.get(
            (
                game_id,
                str(row["action"]),
                str(row["event_num"]),
                str(row.get("anchor_event_num", "")),
            ),
            {},
        )
        scope, affects = _scope_fields("pbp_row_overrides")
        rows.append(
            {
                "override_file": "pbp_row_overrides",
                "season": _season_from_game_id(game_id),
                "game_id": game_id,
                "team_id": "",
                "player_id": "",
                "override_key": f"{row['action']}:{row['event_num']}:{row.get('anchor_event_num', '')}",
                "override_kind": "row_order",
                "production_scope": scope,
                "affects_production_output": affects,
                "basis_tags": "|".join(_basis_tags(row.get("notes", ""), "pbp_row_overrides")),
                "notes": row.get("notes", ""),
                "necessity_status": necessity.get("status", ""),
                "necessity_changed_stats": "",
                "necessity_changed_pipeline_metrics": necessity.get("changed_pipeline_metrics", ""),
                "bbr_recheck_status": audit_row.get("bbr_status", ""),
                "bbr_recheck_stats": "|".join(
                    part
                    for part in [
                        f"period:{audit_row.get('period', '')}" if audit_row.get("period", "") else "",
                        f"target_clock:{audit_row.get('target_clock', '')}" if audit_row.get("target_clock", "") else "",
                        f"anchor_clock:{audit_row.get('anchor_clock', '')}" if audit_row.get("anchor_clock", "") else "",
                        f"bbr_rows:{audit_row.get('bbr_row_count', '')}" if audit_row.get("bbr_row_count", "") else "",
                    ]
                    if part
                ),
                "tpdev_status": "",
                "tpdev_details": "",
            }
        )
    return rows


def _build_stat_override_rows(
    stat_overrides: pd.DataFrame,
    stat_necessity: dict[tuple[str, str, str, str, str], dict[str, Any]],
    bbr_rechecks: pd.DataFrame,
    tpdev_lookup: TpdevBoxLookup,
) -> list[dict[str, Any]]:
    rows = []
    for row in stat_overrides.to_dict(orient="records"):
        game_id = _normalize_game_id(row["game_id"])
        team_id = str(row["team_id"])
        player_id = str(row["player_id"])
        necessity = stat_necessity.get((game_id, team_id, player_id, str(row["stat_key"]), str(row["stat_value"])), {})
        matches = pd.DataFrame()
        impacted_stats = STAT_KEY_TO_BASIC_STATS.get(row["stat_key"], [])
        if not bbr_rechecks.empty:
            matches = bbr_rechecks[
                (bbr_rechecks["game_id"].map(_normalize_game_id) == game_id)
                & (bbr_rechecks["team_id"] == team_id)
                & (bbr_rechecks["player_id"] == player_id)
                & (bbr_rechecks["check_stat"].isin(impacted_stats))
            ]
        bbr_status, bbr_stats = _summarize_bbr_matches(matches)

        tpdev_rows = tpdev_lookup.player_rows(game_id, team_id, player_id)
        tpdev_stats = []
        for stat in impacted_stats:
            if stat == "DRB":
                tpdev_stat = "DRB"
            else:
                tpdev_stat = stat
            value = _tpdev_basic_value(tpdev_rows, tpdev_stat)
            if value != "":
                tpdev_stats.append(f"{tpdev_stat}:{value}")
        scope, affects = _scope_fields("pbp_stat_overrides")
        rows.append(
            {
                "override_file": "pbp_stat_overrides",
                "season": _season_from_game_id(game_id),
                "game_id": game_id,
                "team_id": team_id,
                "player_id": player_id,
                "override_key": f"{row['stat_key']}:{row['stat_value']}",
                "override_kind": "stat_credit",
                "production_scope": scope,
                "affects_production_output": affects,
                "basis_tags": "|".join(_basis_tags(row.get("notes", ""), "pbp_stat_overrides")),
                "notes": row.get("notes", ""),
                "necessity_status": necessity.get("status", ""),
                "necessity_changed_stats": necessity.get("changed_stats", ""),
                "necessity_changed_pipeline_metrics": necessity.get("changed_pipeline_metrics", ""),
                "bbr_recheck_status": bbr_status,
                "bbr_recheck_stats": bbr_stats,
                "tpdev_status": "player_present" if not tpdev_rows.empty else "player_missing",
                "tpdev_details": "|".join(tpdev_stats),
            }
        )
    return rows


def _build_boxscore_source_rows(
    source_overrides: pd.DataFrame,
    bbr_rechecks: pd.DataFrame,
    tpdev_lookup: TpdevBoxLookup,
) -> list[dict[str, Any]]:
    rows = []
    for row in source_overrides.to_dict(orient="records"):
        game_id = _normalize_game_id(row["game_id"])
        team_id = str(row["TEAM_ID"])
        player_id = str(row["PLAYER_ID"])
        matches = pd.DataFrame()
        if not bbr_rechecks.empty:
            matches = bbr_rechecks[
                (bbr_rechecks["game_id"].map(_normalize_game_id) == game_id)
                & (bbr_rechecks["team_id"] == team_id)
                & (bbr_rechecks["player_id"] == player_id)
            ]
        bbr_status, bbr_stats = _summarize_bbr_matches(matches)
        tpdev_rows = tpdev_lookup.player_rows(game_id, team_id, player_id)
        tpdev_details = []
        for stat, tpdev_stat in [("PTS", "PTS"), ("AST", "AST"), ("TOV", "TOV"), ("OREB", "OREB"), ("DREB", "DRB"), ("REB", "REB")]:
            value = _tpdev_basic_value(tpdev_rows, tpdev_stat)
            if value != "":
                tpdev_details.append(f"{stat}:{value}")
        scope, affects = _scope_fields("boxscore_source_overrides")
        rows.append(
            {
                "override_file": "boxscore_source_overrides",
                "season": _season_from_game_id(game_id),
                "game_id": game_id,
                "team_id": team_id,
                "player_id": player_id,
                "override_key": row.get("PLAYER_NAME", ""),
                "override_kind": "official_boxscore_patch",
                "production_scope": scope,
                "affects_production_output": affects,
                "basis_tags": "|".join(_basis_tags(row.get("notes", ""), "boxscore_source_overrides")),
                "notes": row.get("notes", ""),
                "necessity_status": "",
                "necessity_changed_stats": "",
                "necessity_changed_pipeline_metrics": "",
                "bbr_recheck_status": bbr_status,
                "bbr_recheck_stats": bbr_stats,
                "tpdev_status": "player_present" if not tpdev_rows.empty else "player_missing",
                "tpdev_details": "|".join(tpdev_details),
            }
        )
    return rows


def _build_boxscore_audit_rows(
    audit_overrides: pd.DataFrame,
    bbr_rechecks: pd.DataFrame,
    tpdev_lookup: TpdevBoxLookup,
) -> list[dict[str, Any]]:
    rows = []
    for row in audit_overrides.to_dict(orient="records"):
        game_id = _normalize_game_id(row["game_id"])
        team_id = str(row["team_id"])
        player_id = str(row["player_id"])
        matches = pd.DataFrame()
        if not bbr_rechecks.empty:
            matches = bbr_rechecks[
                (bbr_rechecks["game_id"].map(_normalize_game_id) == game_id)
                & (bbr_rechecks["team_id"] == team_id)
                & (bbr_rechecks["player_id"] == player_id)
                & (bbr_rechecks["check_stat"] == row["stat"])
            ]
        bbr_status, bbr_stats = _summarize_bbr_matches(matches)
        tpdev_rows = tpdev_lookup.player_rows(game_id, team_id, player_id)
        tpdev_stat = "DRB" if row["stat"] == "DRB" else row["stat"]
        scope, affects = _scope_fields("boxscore_audit_overrides")
        rows.append(
            {
                "override_file": "boxscore_audit_overrides",
                "season": _season_from_game_id(game_id),
                "game_id": game_id,
                "team_id": team_id,
                "player_id": player_id,
                "override_key": f"{row['stat']}:{row['action']}",
                "override_kind": "audit_exception",
                "production_scope": scope,
                "affects_production_output": affects,
                "basis_tags": "|".join(_basis_tags(row.get("notes", ""), "boxscore_audit_overrides")),
                "notes": row.get("notes", ""),
                "necessity_status": "",
                "necessity_changed_stats": "",
                "necessity_changed_pipeline_metrics": "",
                "bbr_recheck_status": bbr_status,
                "bbr_recheck_stats": bbr_stats,
                "tpdev_status": "player_present" if not tpdev_rows.empty else "player_missing",
                "tpdev_details": f"{tpdev_stat}:{_tpdev_basic_value(tpdev_rows, tpdev_stat)}" if not tpdev_rows.empty else "",
            }
        )
    return rows


def _build_validation_rows(validation_overrides: pd.DataFrame, bbr_rechecks: pd.DataFrame) -> list[dict[str, Any]]:
    rows = []
    for row in validation_overrides.to_dict(orient="records"):
        game_id = _normalize_game_id(row["game_id"])
        matches = pd.DataFrame()
        if not bbr_rechecks.empty:
            matches = bbr_rechecks[bbr_rechecks["game_id"].map(_normalize_game_id) == game_id]
        bbr_status, bbr_stats = _summarize_bbr_matches(matches)
        scope, affects = _scope_fields("validation_overrides")
        rows.append(
            {
                "override_file": "validation_overrides",
                "season": _season_from_game_id(game_id),
                "game_id": game_id,
                "team_id": "",
                "player_id": "",
                "override_key": f"{row.get('action', '')}:{row.get('tolerance', '')}",
                "override_kind": "validation_tolerance",
                "production_scope": scope,
                "affects_production_output": affects,
                "basis_tags": "|".join(_basis_tags(row.get("notes", ""), "validation_overrides")),
                "notes": row.get("notes", ""),
                "necessity_status": "",
                "necessity_changed_stats": "",
                "necessity_changed_pipeline_metrics": "",
                "bbr_recheck_status": bbr_status,
                "bbr_recheck_stats": bbr_stats,
                "tpdev_status": "",
                "tpdev_details": "",
            }
        )
    return rows


def _build_manual_poss_rows(manual_poss_fixes: dict[str, Any]) -> list[dict[str, Any]]:
    rows = []
    for game_id, payload in manual_poss_fixes.items():
        norm_game_id = _normalize_game_id(game_id)
        scope, affects = _scope_fields("manual_poss_fixes")
        rows.append(
            {
                "override_file": "manual_poss_fixes",
                "season": _season_from_game_id(norm_game_id),
                "game_id": norm_game_id,
                "team_id": "",
                "player_id": "",
                "override_key": f"clusters={len(payload.get('clusters', []))};delete_events={len(payload.get('delete_events', []))}",
                "override_kind": "possession_repair",
                "production_scope": scope,
                "affects_production_output": affects,
                "basis_tags": "manual_possession_cluster",
                "notes": json.dumps(payload, sort_keys=True),
                "necessity_status": "",
                "necessity_changed_stats": "",
                "necessity_changed_pipeline_metrics": "",
                "bbr_recheck_status": "",
                "bbr_recheck_stats": "",
                "tpdev_status": "",
                "tpdev_details": "",
            }
        )
    return rows


def build_report() -> tuple[pd.DataFrame, dict[str, Any]]:
    row_overrides = _load_csv(DEFAULT_ROW_OVERRIDES_PATH)
    stat_overrides = _load_csv(DEFAULT_STAT_OVERRIDES_PATH)
    source_overrides = _load_csv(DEFAULT_BOXSCORE_SOURCE_OVERRIDES_PATH)
    audit_overrides = _load_csv(DEFAULT_AUDIT_OVERRIDES_PATH)
    validation_overrides = _load_csv(DEFAULT_VALIDATION_OVERRIDES_PATH)
    manual_poss_fixes = json.loads(DEFAULT_MANUAL_POSS_FIXES_PATH.read_text(encoding="utf-8"))

    row_necessity = _row_necessity_lookup(DEFAULT_ROW_NECESSITY_PATH)
    stat_necessity = _stat_necessity_lookup(DEFAULT_STAT_NECESSITY_PATH)
    row_window_audit = _row_window_audit_lookup(DEFAULT_ROW_WINDOW_AUDIT_PATH)
    bbr_rechecks = _load_bbr_recheck_tables(DEFAULT_BBR_RECHECK_DIR)
    tpdev_lookup = TpdevBoxLookup(_find_tpdev_box_path())

    rows = []
    rows.extend(_build_row_override_rows(row_overrides, row_necessity, row_window_audit))
    rows.extend(_build_stat_override_rows(stat_overrides, stat_necessity, bbr_rechecks["pbp_stat_overrides"], tpdev_lookup))
    rows.extend(_build_boxscore_source_rows(source_overrides, bbr_rechecks["boxscore_source_overrides"], tpdev_lookup))
    rows.extend(_build_boxscore_audit_rows(audit_overrides, bbr_rechecks["boxscore_audit_overrides"], tpdev_lookup))
    rows.extend(_build_validation_rows(validation_overrides, bbr_rechecks["validation_overrides"]))
    rows.extend(_build_manual_poss_rows(manual_poss_fixes))

    report = pd.DataFrame(rows).sort_values(["override_file", "season", "game_id", "team_id", "player_id", "override_key"]).reset_index(drop=True)
    summary = {
        "rows": int(len(report)),
        "counts_by_file": report["override_file"].value_counts(dropna=False).to_dict(),
        "counts_by_kind": report["override_kind"].value_counts(dropna=False).to_dict(),
        "counts_by_scope": report["production_scope"].value_counts(dropna=False).to_dict(),
        "counts_by_basis_tag": report["basis_tags"].str.split("|").explode().value_counts(dropna=False).to_dict(),
        "row_necessity_status": report[report["override_file"] == "pbp_row_overrides"]["necessity_status"].value_counts(dropna=False).to_dict(),
        "stat_necessity_status": report[report["override_file"] == "pbp_stat_overrides"]["necessity_status"].value_counts(dropna=False).to_dict(),
    }
    return report, summary


def main() -> int:
    parser = argparse.ArgumentParser(description="Build a unified provenance report for all manual historical override layers")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    args = parser.parse_args()

    report, summary = build_report()
    output_dir = args.output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    report.to_csv(output_dir / "override_provenance_report.csv", index=False)
    (output_dir / "summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
