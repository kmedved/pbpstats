from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import pandas as pd

ROOT = Path(__file__).resolve().parent
DEFAULT_SHORTLIST_PATH = ROOT / "override_review_shortlist_20260315_v1" / "override_review_shortlist.csv"
DEFAULT_PROVENANCE_PATH = ROOT / "override_provenance_20260315_v1" / "override_provenance_report.csv"
DEFAULT_RECHECK_DIR = ROOT / "bbr_override_recheck_20260315_v3"
DEFAULT_OUTPUT_DIR = ROOT / "override_consensus_20260315_v1"

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


def _load_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path, dtype=str).fillna("")


def _normalize_game_id(value: str | int) -> str:
    return str(int(float(value))).zfill(10)


def _parse_tpdev_details(details: str) -> dict[str, int]:
    parsed: dict[str, int] = {}
    if not details:
        return parsed
    for chunk in str(details).split("|"):
        if ":" not in chunk:
            continue
        key, value = chunk.split(":", 1)
        key = key.strip()
        value = value.strip()
        if not key or not value:
            continue
        try:
            parsed[key] = int(float(value))
        except ValueError:
            continue
    return parsed


def _load_recheck_tables(recheck_dir: Path) -> dict[str, pd.DataFrame]:
    return {
        "pbp_stat_overrides": _load_csv(recheck_dir / "pbp_stat_override_recheck.csv"),
        "boxscore_audit_overrides": _load_csv(recheck_dir / "boxscore_audit_override_recheck.csv"),
        "boxscore_source_overrides": _load_csv(recheck_dir / "boxscore_source_override_recheck.csv"),
    }


def _stat_match_flags(matches: pd.DataFrame, impacted_stats: list[str], tpdev_details: str) -> dict[str, Any]:
    if matches.empty:
        return {
            "parser_matches_official_all": False,
            "parser_matches_bbr_pbp_all": False,
            "official_matches_bbr_pbp_all": False,
            "tpdev_present": bool(tpdev_details),
            "tpdev_matches_parser_all": False,
            "tpdev_matches_official_all": False,
            "stat_pairs": [],
        }

    if impacted_stats:
        matches = matches[matches["check_stat"].isin(impacted_stats)].copy()
    if matches.empty:
        return {
            "parser_matches_official_all": False,
            "parser_matches_bbr_pbp_all": False,
            "official_matches_bbr_pbp_all": False,
            "tpdev_present": bool(tpdev_details),
            "tpdev_matches_parser_all": False,
            "tpdev_matches_official_all": False,
            "stat_pairs": [],
        }

    for col in ["parser_value", "official_value", "bbr_value"]:
        matches[col] = pd.to_numeric(matches[col], errors="coerce")

    tpdev_map = _parse_tpdev_details(tpdev_details)
    stat_pairs: list[str] = []
    tpdev_parser_flags: list[bool] = []
    tpdev_official_flags: list[bool] = []

    for _, row in matches.iterrows():
        stat = str(row["check_stat"])
        parser_value = int(row["parser_value"]) if pd.notna(row["parser_value"]) else 0
        official_value = int(row["official_value"]) if pd.notna(row["official_value"]) else 0
        bbr_value = int(row["bbr_value"]) if pd.notna(row["bbr_value"]) else 0
        pair = f"{stat}: parser={parser_value} official={official_value} bbr_pbp={bbr_value}"
        if stat in tpdev_map:
            tpdev_value = tpdev_map[stat]
            pair += f" tpdev_box={tpdev_value}"
            tpdev_parser_flags.append(tpdev_value == parser_value)
            tpdev_official_flags.append(tpdev_value == official_value)
        stat_pairs.append(pair)

    return {
        "parser_matches_official_all": bool((matches["parser_value"] == matches["official_value"]).all()),
        "parser_matches_bbr_pbp_all": bool((matches["parser_value"] == matches["bbr_value"]).all()),
        "official_matches_bbr_pbp_all": bool((matches["official_value"] == matches["bbr_value"]).all()),
        "tpdev_present": bool(tpdev_map),
        "tpdev_matches_parser_all": bool(tpdev_parser_flags) and all(tpdev_parser_flags),
        "tpdev_matches_official_all": bool(tpdev_official_flags) and all(tpdev_official_flags),
        "stat_pairs": stat_pairs,
    }


def _classify_stat_like_override(
    override_file: str,
    notes: str,
    flags: dict[str, Any],
) -> tuple[str, str]:
    notes_lower = (notes or "").lower()

    if override_file == "boxscore_audit_overrides":
        if flags["parser_matches_bbr_pbp_all"] and flags["tpdev_matches_parser_all"]:
            return "keep_audit_pbp_semantic_fix_with_tpdev_support", "keep_audit_override"
        if flags["parser_matches_bbr_pbp_all"]:
            return "keep_audit_pbp_semantic_fix", "keep_audit_override"
        if flags["tpdev_matches_parser_all"]:
            return "keep_audit_semantic_fix_with_tpdev_support", "keep_audit_override"
        return "audit_override_still_reviewable", "manual_review"

    if "incomplete historical pbp" in notes_lower:
        if flags["parser_matches_official_all"] and not flags["tpdev_present"]:
            return "keep_incomplete_historical_pbp_patch", "keep_production_override"
        return "keep_incomplete_historical_pbp_patch_mixed_sources", "keep_production_override"

    if "official shots cache assigns" in notes_lower:
        return "documented_shot_source_conflict", "keep_production_override_and_document"

    if flags["parser_matches_official_all"] and flags["tpdev_matches_parser_all"] and not flags["parser_matches_bbr_pbp_all"]:
        return "keep_box_and_tpdev_over_bbr_pbp", "keep_production_override"

    if flags["parser_matches_official_all"] and not flags["parser_matches_bbr_pbp_all"] and not flags["tpdev_present"]:
        return "keep_box_over_bbr_pbp_tpdev_missing", "keep_production_override"

    if flags["parser_matches_official_all"] and flags["tpdev_present"] and not flags["tpdev_matches_parser_all"]:
        return "documented_box_vs_pbp_source_conflict", "keep_production_override_and_document"

    if flags["parser_matches_bbr_pbp_all"] and not flags["parser_matches_official_all"]:
        return "parser_and_bbr_pbp_over_official", "manual_review"

    return "stat_override_still_reviewable", "manual_review"


def _classify_row_override(status: str, notes: str, pipeline_metrics: str) -> tuple[str, str]:
    notes_lower = (notes or "").lower()
    metrics_lower = (pipeline_metrics or "").lower()

    if status in {"bbr_supports_move_after_or_raw", "bbr_supports_move_before_or_raw"}:
        return "strong_bbr_window_support", "keep_row_override"

    if status == "missing_bbr_clock":
        if "error:" in metrics_lower or "rebound_deletions:" in metrics_lower:
            return "keep_unverifiable_bbr_clock_gap", "keep_row_override"
        return "missing_bbr_clock_manual_review", "manual_review"

    if status == "bbr_keeps_target_like_event":
        if any(
            token in notes_lower
            for token in [
                "duplicate",
                "after-the-buzzer",
                "after the buzzer",
                "impossible",
                "stray",
                "placeholder",
                "orphan",
                "unresolved",
                "stat override",
                "restore the missing board",
                "restore the official",
                "already covered by stat override",
            ]
        ):
            return "semantic_fix_despite_bbr_event_presence", "keep_row_override"
        return "bbr_keeps_event_manual_review", "manual_review"

    return "row_override_still_reviewable", "manual_review"


def build_report(
    shortlist_path: Path = DEFAULT_SHORTLIST_PATH,
    provenance_path: Path = DEFAULT_PROVENANCE_PATH,
    recheck_dir: Path = DEFAULT_RECHECK_DIR,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    shortlist = _load_csv(shortlist_path)
    provenance = _load_csv(provenance_path)
    rechecks = _load_recheck_tables(recheck_dir)

    if shortlist.empty or provenance.empty:
        raise FileNotFoundError("Shortlist or provenance report is missing/empty")

    key_cols = ["override_file", "game_id", "team_id", "player_id", "override_key"]
    merged = shortlist.merge(provenance, on=key_cols, how="left", suffixes=("", "_prov"))

    report_rows: list[dict[str, Any]] = []
    for _, row in merged.iterrows():
        override_file = row["override_file"]
        game_id = _normalize_game_id(row["game_id"])
        team_id = row.get("team_id", "")
        player_id = row.get("player_id", "")
        override_key = row["override_key"]
        notes = row.get("notes", "")
        review_priority = int(float(row.get("review_priority", 0) or 0))
        tpdev_details = row.get("tpdev_details", "")
        bbr_status = row.get("bbr_recheck_status", "")
        pipeline_metrics = row.get("necessity_changed_pipeline_metrics", "")

        consensus_class = ""
        recommended_action = ""
        evidence_summary = ""

        if override_file == "pbp_row_overrides":
            consensus_class, recommended_action = _classify_row_override(bbr_status, notes, pipeline_metrics)
            evidence_summary = f"bbr_window={bbr_status or 'none'}"
            if pipeline_metrics:
                evidence_summary += f"; pipeline={pipeline_metrics}"
        else:
            impacted_stats: list[str]
            if override_file == "pbp_stat_overrides":
                stat_key = override_key.split(":", 1)[0]
                impacted_stats = STAT_KEY_TO_BASIC_STATS.get(stat_key, [])
                recheck_df = rechecks["pbp_stat_overrides"]
            elif override_file == "boxscore_audit_overrides":
                impacted_stats = [override_key.split(":", 1)[0]]
                recheck_df = rechecks["boxscore_audit_overrides"]
            else:
                impacted_stats = []
                recheck_df = rechecks["boxscore_source_overrides"]

            match_df = pd.DataFrame()
            if not recheck_df.empty:
                match_df = recheck_df[
                    (recheck_df["game_id"].map(_normalize_game_id) == game_id)
                    & (recheck_df["team_id"] == str(team_id))
                    & (recheck_df["player_id"] == str(player_id))
                ].copy()
                if impacted_stats:
                    match_df = match_df[match_df["check_stat"].isin(impacted_stats)]

            flags = _stat_match_flags(match_df, impacted_stats, tpdev_details)
            consensus_class, recommended_action = _classify_stat_like_override(override_file, notes, flags)
            evidence_summary = "; ".join(flags["stat_pairs"])

        report_rows.append(
            {
                "override_file": override_file,
                "season": row.get("season", ""),
                "game_id": game_id,
                "team_id": team_id,
                "player_id": player_id,
                "override_key": override_key,
                "review_priority": review_priority,
                "consensus_class": consensus_class,
                "recommended_action": recommended_action,
                "bbr_recheck_status": bbr_status,
                "tpdev_status": row.get("tpdev_status", ""),
                "tpdev_details": tpdev_details,
                "necessity_status": row.get("necessity_status", ""),
                "necessity_changed_pipeline_metrics": pipeline_metrics,
                "notes": notes,
                "evidence_summary": evidence_summary,
            }
        )

    report = pd.DataFrame(report_rows).sort_values(
        ["review_priority", "override_file", "season", "game_id", "team_id", "player_id", "override_key"],
        ascending=[True, True, True, True, True, True, True],
    ).reset_index(drop=True)

    summary = {
        "rows": int(len(report)),
        "counts_by_file": report["override_file"].value_counts(dropna=False).to_dict(),
        "counts_by_consensus_class": report["consensus_class"].value_counts(dropna=False).to_dict(),
        "counts_by_recommended_action": report["recommended_action"].value_counts(dropna=False).to_dict(),
        "manual_review_rows": int((report["recommended_action"] == "manual_review").sum()),
    }
    return report, summary


def main() -> int:
    parser = argparse.ArgumentParser(description="Build a consensus report for the arguable manual override shortlist")
    parser.add_argument("--shortlist-path", type=Path, default=DEFAULT_SHORTLIST_PATH)
    parser.add_argument("--provenance-path", type=Path, default=DEFAULT_PROVENANCE_PATH)
    parser.add_argument("--recheck-dir", type=Path, default=DEFAULT_RECHECK_DIR)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    args = parser.parse_args()

    report, summary = build_report(
        shortlist_path=args.shortlist_path,
        provenance_path=args.provenance_path,
        recheck_dir=args.recheck_dir,
    )
    output_dir = args.output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    report.to_csv(output_dir / "override_consensus_report.csv", index=False)
    (output_dir / "summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
