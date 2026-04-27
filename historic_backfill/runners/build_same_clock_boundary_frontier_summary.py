from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import pandas as pd


def _normalize_game_id(value: Any) -> str:
    return str(int(value)).zfill(10)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Summarize active same-clock boundary suite outputs into case and family residue artifacts."
    )
    parser.add_argument(
        "--suite-dir",
        type=Path,
        action="append",
        required=True,
        help="Suite output directory containing summary.json from run_same_clock_boundary_queue_suite.py. May be passed multiple times.",
    )
    parser.add_argument("--output-dir", type=Path, required=True)
    return parser.parse_args()


def _load_cases(suite_dir: Path) -> list[dict[str, Any]]:
    summary_path = suite_dir / "summary.json"
    if not summary_path.exists():
        return []
    payload = json.loads(summary_path.read_text(encoding="utf-8"))
    rows = payload.get("cases", []) if isinstance(payload, dict) else []
    results: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        record = dict(row)
        record["suite_dir"] = str(suite_dir.resolve())
        record["game_id"] = _normalize_game_id(record["game_id"])
        record["season"] = int(record["season"])
        record["period"] = int(record["period"])
        record["team_id"] = int(record["team_id"])
        record["family_rank"] = int(record.get("family_rank", 0))
        record["issue_rows"] = int(record.get("issue_rows", 0))
        record["minutes_mismatch_rows"] = int(record.get("minutes_mismatch_rows", 0))
        record["minute_outlier_rows"] = int(record.get("minute_outlier_rows", 0))
        record["plus_minus_mismatch_rows"] = int(record.get("plus_minus_mismatch_rows", 0))
        record["event_issue_rows"] = int(record.get("event_issue_rows", 0))
        record["game_max_minutes_abs_diff"] = float(record.get("game_max_minutes_abs_diff", 0.0))
        record["rows_where_official_and_tpdev_pbp_agree_but_output_minutes_differs"] = int(
            record.get("rows_where_official_and_tpdev_pbp_agree_but_output_minutes_differs", 0)
        )
        record["is_manifest_positive"] = bool(record.get("is_manifest_positive", False))
        results.append(record)
    return results


def _residue_shape(group: pd.DataFrame) -> str:
    max_event = int(group["event_issue_rows"].max())
    max_minutes = int(group["minutes_mismatch_rows"].max())
    max_plus_minus = int(group["plus_minus_mismatch_rows"].max())
    max_abs_diff = float(group["game_max_minutes_abs_diff"].max())
    if max_event > 0 and max_minutes == 0 and max_plus_minus > 0 and max_abs_diff <= 0.01:
        return "same_clock_on_court_contradiction_with_tiny_minute_drift"
    if max_event > 0 and max_minutes > 0 and max_plus_minus == 0:
        return "boundary_on_court_and_minute_residue_without_plus_minus_drift"
    if max_event == 0 and max_minutes == 0 and max_plus_minus == 0:
        return "resolved_control"
    if max_event > 0 and max_minutes > 0 and max_plus_minus > 0:
        return "mixed_boundary_residue"
    if max_event == 0 and max_minutes == 0 and max_plus_minus > 0:
        return "plus_minus_only_residue"
    return "mixed_or_small_residue"


def _recommended_next_track(group: pd.DataFrame) -> str:
    family = str(group["same_clock_family"].iloc[0])
    shape = _residue_shape(group)
    if shape == "resolved_control":
        return "keep_as_non_regression_control"
    if family == "foul_free_throw_sub_same_clock_ordering":
        return "inspect_same_clock_foul_ft_anchor_events"
    if family == "cluster_start_vs_cluster_end_timing":
        return "inspect_cluster_entry_vs_exit_boundary"
    return "inspect_case_locally"


def main() -> int:
    args = parse_args()
    output_dir = args.output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    rows: list[dict[str, Any]] = []
    for suite_dir in args.suite_dir:
        rows.extend(_load_cases(suite_dir.resolve()))

    case_df = pd.DataFrame(rows)
    if case_df.empty:
        case_df.to_csv(output_dir / "same_clock_boundary_case_summary.csv", index=False)
        summary = {"cases": 0, "families": {}}
        (output_dir / "same_clock_boundary_family_summary.json").write_text(
            json.dumps(summary, indent=2),
            encoding="utf-8",
        )
        print(json.dumps(summary, indent=2))
        return 0

    case_df = case_df.sort_values(
        ["same_clock_family", "family_rank", "season", "game_id", "period", "team_id"]
    ).reset_index(drop=True)
    case_df.to_csv(output_dir / "same_clock_boundary_case_summary.csv", index=False)

    family_summary: dict[str, Any] = {}
    for family, group in case_df.groupby("same_clock_family", dropna=False):
        family_summary[str(family)] = {
            "cases": int(len(group)),
            "manifest_positive_cases": int(group["is_manifest_positive"].sum()),
            "max_event_issue_rows": int(group["event_issue_rows"].max()),
            "max_minutes_mismatch_rows": int(group["minutes_mismatch_rows"].max()),
            "max_plus_minus_mismatch_rows": int(group["plus_minus_mismatch_rows"].max()),
            "max_game_minutes_abs_diff": float(group["game_max_minutes_abs_diff"].max()),
            "residue_shape": _residue_shape(group),
            "recommended_next_track": _recommended_next_track(group),
            "teaching_cases": group.loc[
                group["is_manifest_positive"],
                ["game_id", "season", "period", "team_id", "family_rank"],
            ].to_dict("records"),
            "cases_detail": group[
                [
                    "game_id",
                    "season",
                    "period",
                    "team_id",
                    "family_rank",
                    "is_manifest_positive",
                    "issue_rows",
                    "event_issue_rows",
                    "minutes_mismatch_rows",
                    "minute_outlier_rows",
                    "plus_minus_mismatch_rows",
                    "game_max_minutes_abs_diff",
                    "rows_where_official_and_tpdev_pbp_agree_but_output_minutes_differs",
                    "suite_dir",
                ]
            ].to_dict("records"),
        }

    summary = {
        "cases": int(len(case_df)),
        "families": family_summary,
    }
    (output_dir / "same_clock_boundary_family_summary.json").write_text(
        json.dumps(summary, indent=2),
        encoding="utf-8",
    )
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
