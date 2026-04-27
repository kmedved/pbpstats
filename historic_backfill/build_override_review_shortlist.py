from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parent
DEFAULT_PROVENANCE_PATH = ROOT / "override_provenance_20260315_v1" / "override_provenance_report.csv"
DEFAULT_OUTPUT_DIR = ROOT / "override_review_shortlist_20260315_v1"


ROW_REVIEW_STATUSES = {
    "bbr_keeps_target_like_event",
    "bbr_supports_move_after_or_raw",
    "bbr_supports_move_before_or_raw",
    "missing_bbr_clock",
}

STAT_REVIEW_STATUSES = {
    "parser_official_bbr_disagree",
    "unsupported_stat_key",
}

SOURCE_REVIEW_STATUSES = {
    "parser_official_bbr_disagree",
}


def _priority(row: pd.Series) -> int:
    override_file = row["override_file"]
    status = row["bbr_recheck_status"]
    if override_file == "boxscore_source_overrides":
        return 1
    if override_file == "pbp_stat_overrides" and status == "parser_official_bbr_disagree":
        return 2
    if override_file == "pbp_stat_overrides" and status == "unsupported_stat_key":
        return 3
    if override_file == "pbp_row_overrides" and status == "bbr_keeps_target_like_event":
        return 4
    if override_file == "pbp_row_overrides":
        return 5
    if override_file == "boxscore_audit_overrides" and status == "parser_official_bbr_disagree":
        return 6
    return 9


def main() -> int:
    parser = argparse.ArgumentParser(description="Build a compact shortlist of overrides that still deserve manual review.")
    parser.add_argument("--provenance-path", type=Path, default=DEFAULT_PROVENANCE_PATH)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    args = parser.parse_args()

    df = pd.read_csv(args.provenance_path.resolve(), dtype=str).fillna("")
    shortlist = df[
        ((df["override_file"] == "pbp_row_overrides") & (df["bbr_recheck_status"].isin(ROW_REVIEW_STATUSES)))
        | ((df["override_file"] == "pbp_stat_overrides") & (df["bbr_recheck_status"].isin(STAT_REVIEW_STATUSES)))
        | ((df["override_file"] == "boxscore_source_overrides") & (df["bbr_recheck_status"].isin(SOURCE_REVIEW_STATUSES)))
        | ((df["override_file"] == "boxscore_audit_overrides") & (df["bbr_recheck_status"] == "parser_official_bbr_disagree"))
    ].copy()

    shortlist["review_priority"] = shortlist.apply(_priority, axis=1)
    shortlist = shortlist.sort_values(
        ["review_priority", "override_file", "season", "game_id", "team_id", "player_id", "override_key"]
    ).reset_index(drop=True)

    output_dir = args.output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    shortlist.to_csv(output_dir / "override_review_shortlist.csv", index=False)

    summary = {
        "rows": int(len(shortlist)),
        "games": int(shortlist["game_id"].nunique()),
        "counts_by_file": shortlist["override_file"].value_counts(dropna=False).to_dict(),
        "counts_by_status": shortlist["bbr_recheck_status"].value_counts(dropna=False).to_dict(),
    }
    (output_dir / "summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
