from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import pandas as pd


ROOT = Path(__file__).resolve().parent

CORE_KEYS = [
    "minutes_mismatches",
    "minutes_outliers",
    "plus_minus_mismatches",
    "event_on_court_issue_rows",
    "event_on_court_issue_games",
    "problem_games",
]
SELECTED_CROSS_KEYS = [
    "rows_where_output_minutes_differs_from_official",
    "rows_where_output_plus_minus_differs_from_official",
    "rows_where_official_and_tpdev_pbp_agree_but_output_minutes_differs",
    "rows_where_output_matches_tpdev_pbp_not_official_minutes",
]
SELECTED_FAMILIES = [
    "after_cluster_low_confidence",
    "no_deadball_local_signal",
    "ambiguous_runner_up",
]


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build a consolidated residual dashboard for an intraperiod proving run."
    )
    parser.add_argument("--loop-output-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--family-register-dir", type=Path)
    parser.add_argument("--baseline-dir", type=Path)
    return parser.parse_args()


def _flatten_block_summary(block_summary: dict[str, Any], baseline_summary: dict[str, Any] | None) -> dict[str, Any]:
    record: dict[str, Any] = {
        "block_id": str(block_summary.get("block_id") or ""),
        "label": str(block_summary.get("label") or ""),
    }
    for key in CORE_KEYS:
        record[key] = int(block_summary.get(key, 0) or 0)
        record[f"{key}_delta_vs_baseline"] = (
            record[key] - int(baseline_summary.get(key, 0) or 0)
            if baseline_summary is not None
            else 0
        )

    cross_summary = block_summary.get("cross_source_summary") or {}
    baseline_cross_summary = (baseline_summary or {}).get("cross_source_summary") or {}
    for key in SELECTED_CROSS_KEYS:
        record[key] = int(cross_summary.get(key, 0) or 0)
        record[f"{key}_delta_vs_baseline"] = (
            record[key] - int(baseline_cross_summary.get(key, 0) or 0)
            if baseline_summary is not None
            else 0
        )

    minute_buckets = cross_summary.get("minute_diff_buckets_vs_official") or {}
    baseline_minute_buckets = (baseline_cross_summary.get("minute_diff_buckets_vs_official") or {})
    record["minute_diff_buckets_vs_official.minutes_over_2"] = int(
        minute_buckets.get("minutes_over_2", 0) or 0
    )
    record["minute_diff_buckets_vs_official.minutes_over_2_delta_vs_baseline"] = (
        record["minute_diff_buckets_vs_official.minutes_over_2"]
        - int(baseline_minute_buckets.get("minutes_over_2", 0) or 0)
        if baseline_summary is not None
        else 0
    )
    return record


def main() -> int:
    args = parse_args()
    loop_output_dir = args.loop_output_dir.resolve()
    output_dir = args.output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    family_register_dir = (
        args.family_register_dir.resolve()
        if args.family_register_dir is not None
        else loop_output_dir / "family_register"
    )
    baseline_dir = args.baseline_dir.resolve() if args.baseline_dir is not None else None

    block_rows: list[dict[str, Any]] = []
    for block_summary_path in sorted(loop_output_dir.glob("blocks/*/block_summary.json")):
        block_summary = _load_json(block_summary_path)
        block_key = f"{block_summary['block_id']}_{block_summary['label'].replace(' ', '_')}"
        baseline_summary = None
        if baseline_dir is not None:
            baseline_summary_path = baseline_dir / "blocks" / block_key / "block_summary.json"
            if baseline_summary_path.exists():
                baseline_summary = _load_json(baseline_summary_path)
        row = _flatten_block_summary(block_summary, baseline_summary)
        row["block_key"] = block_key
        block_rows.append(row)

    block_df = pd.DataFrame(block_rows)
    block_df.to_csv(output_dir / "block_residuals.csv", index=False)

    family_counts: dict[str, int] = {}
    top_actionable_games: list[dict[str, Any]] = []
    top_manual_review_games: list[dict[str, Any]] = []
    family_summary = {}
    if (family_register_dir / "summary.json").exists():
        family_summary = _load_json(family_register_dir / "summary.json")
        family_counts = {
            family: int((family_summary.get("family_counts") or {}).get(family, 0) or 0)
            for family in SELECTED_FAMILIES
        }
        top_actionable_games = list(family_summary.get("top_actionable_games") or [])
        top_manual_review_games = list(family_summary.get("top_manual_review_games") or [])

    family_counts_df = pd.DataFrame(
        [{"family": family, "rows": count} for family, count in family_counts.items()]
    )
    family_counts_df.to_csv(output_dir / "selected_family_counts.csv", index=False)

    totals = {}
    for key in CORE_KEYS:
        totals[key] = int(block_df[key].sum()) if not block_df.empty else 0
        delta_key = f"{key}_delta_vs_baseline"
        totals[delta_key] = int(block_df[delta_key].sum()) if not block_df.empty else 0
    for key in SELECTED_CROSS_KEYS + ["minute_diff_buckets_vs_official.minutes_over_2"]:
        totals[key] = int(block_df[key].sum()) if not block_df.empty else 0
        delta_key = f"{key}_delta_vs_baseline"
        totals[delta_key] = int(block_df[delta_key].sum()) if not block_df.empty else 0

    summary = {
        "loop_output_dir": str(loop_output_dir),
        "baseline_dir": str(baseline_dir) if baseline_dir is not None else None,
        "blocks": block_df[
            [
                "block_key",
                "minutes_mismatches",
                "minutes_outliers",
                "plus_minus_mismatches",
                "event_on_court_issue_rows",
                "event_on_court_issue_games",
                "problem_games",
                "rows_where_output_minutes_differs_from_official",
                "rows_where_output_plus_minus_differs_from_official",
                "rows_where_official_and_tpdev_pbp_agree_but_output_minutes_differs",
                "rows_where_output_matches_tpdev_pbp_not_official_minutes",
                "minute_diff_buckets_vs_official.minutes_over_2",
                "minutes_mismatches_delta_vs_baseline",
                "minutes_outliers_delta_vs_baseline",
                "plus_minus_mismatches_delta_vs_baseline",
                "event_on_court_issue_rows_delta_vs_baseline",
                "event_on_court_issue_games_delta_vs_baseline",
                "problem_games_delta_vs_baseline",
                "rows_where_output_minutes_differs_from_official_delta_vs_baseline",
                "rows_where_output_plus_minus_differs_from_official_delta_vs_baseline",
                "rows_where_official_and_tpdev_pbp_agree_but_output_minutes_differs_delta_vs_baseline",
                "rows_where_output_matches_tpdev_pbp_not_official_minutes_delta_vs_baseline",
                "minute_diff_buckets_vs_official.minutes_over_2_delta_vs_baseline",
            ]
        ].to_dict(orient="records"),
        "totals": totals,
        "selected_family_counts": family_counts,
        "family_register_summary": {
            "rows": int(family_summary.get("rows", 0) or 0),
            "manual_review_rows": int(family_summary.get("manual_review_rows", 0) or 0),
            "auto_apply_rows": int(family_summary.get("auto_apply_rows", 0) or 0),
            "known_negative_tripwire_rows": int(
                family_summary.get("known_negative_tripwire_rows", 0) or 0
            ),
            "high_signal_rows": int(family_summary.get("high_signal_rows", 0) or 0),
        },
        "top_actionable_games": top_actionable_games,
        "top_manual_review_games": top_manual_review_games,
    }
    (output_dir / "summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
