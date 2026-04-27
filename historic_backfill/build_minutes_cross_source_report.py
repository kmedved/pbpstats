from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Dict

import pandas as pd

from audit_minutes_plus_minus import (
    _prepare_darko_df,
    load_official_boxscore_batch_df,
)
from bbr_boxscore_loader import (
    DEFAULT_BBR_DB_PATH,
    DEFAULT_PLAYER_CROSSWALK_PATH,
    load_bbr_boxscore_df,
)
from minute_reference_sources import (
    DEFAULT_PBPSTATS_PLAYER_BOX_PATH,
    DEFAULT_TPDEV_BOX_CDN_PATH,
    DEFAULT_TPDEV_BOX_NEW_PATH,
    DEFAULT_TPDEV_BOX_PATH,
    DEFAULT_TPDEV_PBP_PATH,
    load_pbpstats_player_box_frame,
    load_tpdev_box_frame,
    load_tpdev_pbp_minutes_frame,
)


ROOT = Path(__file__).resolve().parent
DEFAULT_DB_PATH = ROOT / "nba_raw.db"
MINUTE_MATCH_TOLERANCE = 1.0 / 60.0
SOURCE_LABELS = (
    "tpdev_pbp",
    "pbpstats_box",
    "tpdev_box",
    "tpdev_box_new",
    "tpdev_box_cdn",
    "bbr_box",
)


def build_minutes_cross_source_report(
    darko_df: pd.DataFrame,
    db_path: Path,
    tpdev_box_path: Path = DEFAULT_TPDEV_BOX_PATH,
    tpdev_box_new_path: Path = DEFAULT_TPDEV_BOX_NEW_PATH,
    tpdev_box_cdn_path: Path = DEFAULT_TPDEV_BOX_CDN_PATH,
    tpdev_pbp_path: Path = DEFAULT_TPDEV_PBP_PATH,
    pbpstats_player_box_path: Path = DEFAULT_PBPSTATS_PLAYER_BOX_PATH,
    bbr_db_path: Path = DEFAULT_BBR_DB_PATH,
    player_crosswalk_path: Path = DEFAULT_PLAYER_CROSSWALK_PATH,
) -> pd.DataFrame:
    prepared = _prepare_darko_df(darko_df)
    if prepared.empty:
        return pd.DataFrame()
    game_ids = sorted(prepared["game_id"].unique())

    official = load_official_boxscore_batch_df(db_path, game_ids)

    merged = prepared.merge(
        official,
        on=["game_id", "player_id", "team_id"],
        how="outer",
        suffixes=("_output", "_official"),
    )

    for label, path in [
        ("tpdev_box", tpdev_box_path),
        ("tpdev_box_new", tpdev_box_new_path),
        ("tpdev_box_cdn", tpdev_box_cdn_path),
    ]:
        merged = merged.merge(
            load_tpdev_box_frame(path, label, game_ids=game_ids),
            on=["game_id", "player_id", "team_id"],
            how="left",
        )

    merged = merged.merge(
        load_pbpstats_player_box_frame(pbpstats_player_box_path, game_ids=game_ids),
        on=["game_id", "player_id", "team_id"],
        how="left",
    )
    merged = merged.merge(
        load_tpdev_pbp_minutes_frame(tpdev_pbp_path, game_ids=game_ids),
        on=["game_id", "player_id", "team_id"],
        how="left",
    )

    bbr_frames = [
        load_bbr_boxscore_df(
            game_id,
            nba_raw_db_path=db_path,
            bbr_db_path=bbr_db_path,
            crosswalk_path=player_crosswalk_path,
        )
        for game_id in game_ids
    ]
    bbr = pd.concat(bbr_frames, ignore_index=True) if bbr_frames else pd.DataFrame()
    if not bbr.empty:
        merged = merged.merge(
            bbr,
            on=["game_id", "player_id", "team_id"],
            how="left",
        )

    merged["player_name"] = (
        merged.get("player_name_output")
        .fillna(merged.get("player_name_official"))
        .fillna(merged.get("player_name_pbpstats_box"))
        .fillna("")
        .astype(str)
    )
    merged["Minutes_output"] = pd.to_numeric(
        merged.get("Minutes_output", 0.0), errors="coerce"
    ).fillna(0.0)
    merged["Minutes_official"] = pd.to_numeric(
        merged.get("Minutes_official", 0.0), errors="coerce"
    ).fillna(0.0)
    merged["Plus_Minus_output"] = pd.to_numeric(
        merged.get("Plus_Minus_output", 0.0), errors="coerce"
    ).fillna(0.0)
    merged["Plus_Minus_official"] = pd.to_numeric(
        merged.get("Plus_Minus_official", 0.0), errors="coerce"
    ).fillna(0.0)
    merged["Minutes_pbpstats_box"] = pd.to_numeric(
        merged.get("Minutes_pbpstats_box", 0.0), errors="coerce"
    ).fillna(0.0)
    merged["Minutes_tpdev_pbp"] = pd.to_numeric(
        merged.get("Minutes_tpdev_pbp", 0.0), errors="coerce"
    ).fillna(0.0)
    merged["Plus_Minus_pbpstats_box"] = 0.0
    merged["Plus_Minus_tpdev_pbp"] = 0.0

    for label in SOURCE_LABELS:
        minutes_col = f"Minutes_{label}"
        pm_col = f"Plus_Minus_{label}"
        merged[minutes_col] = pd.to_numeric(merged.get(minutes_col, 0.0), errors="coerce").fillna(0.0)
        merged[pm_col] = pd.to_numeric(merged.get(pm_col, 0.0), errors="coerce").fillna(0.0)

        merged[f"Minutes_diff_vs_{label}"] = merged["Minutes_output"] - merged[minutes_col]
        merged[f"Minutes_abs_diff_vs_{label}"] = merged[f"Minutes_diff_vs_{label}"].abs()
        merged[f"Minutes_match_vs_{label}"] = (
            merged[f"Minutes_abs_diff_vs_{label}"] <= MINUTE_MATCH_TOLERANCE
        )
        merged[f"Plus_Minus_diff_vs_{label}"] = merged["Plus_Minus_output"] - merged[pm_col]
        merged[f"Plus_Minus_match_vs_{label}"] = merged[f"Plus_Minus_diff_vs_{label}"] == 0

        merged[f"Official_minutes_match_vs_{label}"] = (
            (merged["Minutes_official"] - merged[minutes_col]).abs() <= MINUTE_MATCH_TOLERANCE
        )
        merged[f"Official_plus_minus_match_vs_{label}"] = (
            merged["Plus_Minus_official"] - merged[pm_col] == 0
        )

    merged["Minutes_diff_vs_official"] = merged["Minutes_output"] - merged["Minutes_official"]
    merged["Minutes_abs_diff_vs_official"] = merged["Minutes_diff_vs_official"].abs()
    merged["Minutes_match_vs_official"] = (
        merged["Minutes_abs_diff_vs_official"] <= MINUTE_MATCH_TOLERANCE
    )
    merged["Plus_Minus_diff_vs_official"] = (
        merged["Plus_Minus_output"] - merged["Plus_Minus_official"]
    )
    merged["Plus_Minus_match_vs_official"] = merged["Plus_Minus_diff_vs_official"] == 0

    return merged.sort_values(["game_id", "team_id", "player_id"]).reset_index(drop=True)


def summarize_minutes_cross_source_report(report_df: pd.DataFrame) -> Dict[str, Any]:
    if report_df.empty:
        return {
            "rows": 0,
            "minute_match_tolerance": MINUTE_MATCH_TOLERANCE,
        }

    summary: Dict[str, Any] = {
        "rows": int(len(report_df)),
        "minute_match_tolerance": MINUTE_MATCH_TOLERANCE,
        "output_minutes_match_official": int(report_df["Minutes_match_vs_official"].sum()),
        "output_plus_minus_match_official": int(report_df["Plus_Minus_match_vs_official"].sum()),
        "rows_where_output_minutes_differs_from_official": int(
            (~report_df["Minutes_match_vs_official"]).sum()
        ),
        "rows_where_output_plus_minus_differs_from_official": int(
            (~report_df["Plus_Minus_match_vs_official"]).sum()
        ),
    }

    for label in SOURCE_LABELS:
        if f"Minutes_match_vs_{label}" not in report_df.columns:
            continue
        summary[f"output_minutes_match_{label}"] = int(report_df[f"Minutes_match_vs_{label}"].sum())
        summary[f"output_plus_minus_match_{label}"] = int(report_df[f"Plus_Minus_match_vs_{label}"].sum())
        summary[f"official_minutes_match_{label}"] = int(
            report_df[f"Official_minutes_match_vs_{label}"].sum()
        )
        summary[f"official_plus_minus_match_{label}"] = int(
            report_df[f"Official_plus_minus_match_vs_{label}"].sum()
        )
        summary[f"rows_where_official_and_{label}_agree_but_output_minutes_differs"] = int(
            (
                report_df[f"Official_minutes_match_vs_{label}"]
                & ~report_df["Minutes_match_vs_official"]
            ).sum()
        )
        summary[f"rows_where_output_matches_{label}_not_official_minutes"] = int(
            (
                report_df[f"Minutes_match_vs_{label}"]
                & ~report_df["Minutes_match_vs_official"]
            ).sum()
        )

    bucket_specs = [
        ("seconds_1", MINUTE_MATCH_TOLERANCE, 2.0 / 60.0),
        ("seconds_2_to_6", 2.0 / 60.0, 0.1),
        ("seconds_6_to_15", 0.1, 0.25),
        ("seconds_15_to_30", 0.25, 0.5),
        ("minutes_0_5_to_2", 0.5, 2.0),
        ("minutes_over_2", 2.0, float("inf")),
    ]
    output_minute_diffs = report_df["Minutes_abs_diff_vs_official"]
    summary["minute_diff_buckets_vs_official"] = {
        name: int(((output_minute_diffs > lo) & (output_minute_diffs <= hi)).sum())
        for name, lo, hi in bucket_specs
    }
    return summary


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compare current DARKO minutes/plus-minus against official and tpdev outputs."
    )
    parser.add_argument("--darko-parquet", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB_PATH)
    parser.add_argument("--tpdev-box-path", type=Path, default=DEFAULT_TPDEV_BOX_PATH)
    parser.add_argument("--tpdev-box-new-path", type=Path, default=DEFAULT_TPDEV_BOX_NEW_PATH)
    parser.add_argument("--tpdev-box-cdn-path", type=Path, default=DEFAULT_TPDEV_BOX_CDN_PATH)
    parser.add_argument("--tpdev-pbp-path", type=Path, default=DEFAULT_TPDEV_PBP_PATH)
    parser.add_argument("--pbpstats-player-box-path", type=Path, default=DEFAULT_PBPSTATS_PLAYER_BOX_PATH)
    parser.add_argument("--bbr-db-path", type=Path, default=DEFAULT_BBR_DB_PATH)
    parser.add_argument("--player-crosswalk-path", type=Path, default=DEFAULT_PLAYER_CROSSWALK_PATH)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    darko_df = pd.read_parquet(args.darko_parquet)
    report_df = build_minutes_cross_source_report(
        darko_df=darko_df,
        db_path=args.db_path,
        tpdev_box_path=args.tpdev_box_path,
        tpdev_box_new_path=args.tpdev_box_new_path,
        tpdev_box_cdn_path=args.tpdev_box_cdn_path,
        tpdev_pbp_path=args.tpdev_pbp_path,
        pbpstats_player_box_path=args.pbpstats_player_box_path,
        bbr_db_path=args.bbr_db_path,
        player_crosswalk_path=args.player_crosswalk_path,
    )
    summary = summarize_minutes_cross_source_report(report_df)

    report_df.to_csv(args.output_dir / "minutes_cross_source_report.csv", index=False)
    (args.output_dir / "minutes_cross_source_summary.json").write_text(
        json.dumps(summary, indent=2),
        encoding="utf-8",
    )

    mismatch_df = report_df[~report_df["Minutes_match_vs_official"]].copy()
    mismatch_df.to_csv(
        args.output_dir / "minutes_cross_source_mismatches.csv",
        index=False,
    )

    plus_minus_df = report_df[~report_df["Plus_Minus_match_vs_official"]].copy()
    plus_minus_df.to_csv(
        args.output_dir / "plus_minus_cross_source_mismatches.csv",
        index=False,
    )

    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
