from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


DELTA_KEYS = [
    "minutes_mismatches",
    "minutes_outliers",
    "plus_minus_mismatches",
    "event_on_court_issue_rows",
    "event_on_court_issue_games",
    "problem_games",
]

CROSS_SOURCE_DELTA_KEYS = [
    "rows_where_output_minutes_differs_from_official",
    "rows_where_output_plus_minus_differs_from_official",
    "rows_where_official_and_tpdev_pbp_agree_but_output_minutes_differs",
    "rows_where_output_matches_tpdev_pbp_not_official_minutes",
]


def _extract_cross_source_metrics(summary: dict[str, Any] | None) -> dict[str, Any] | None:
    if summary is None:
        return None
    minute_buckets = summary.get("minute_diff_buckets_vs_official") or {}
    extracted = {
        key: summary.get(key)
        for key in CROSS_SOURCE_DELTA_KEYS
    }
    extracted["minute_diff_buckets_vs_official.minutes_over_2"] = minute_buckets.get("minutes_over_2")
    return extracted


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compare an intraperiod proving-loop output against a baseline block bundle."
    )
    parser.add_argument("--loop-output-dir", type=Path, required=True)
    parser.add_argument("--baseline-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    loop_output_dir = args.loop_output_dir.resolve()
    baseline_dir = args.baseline_dir.resolve()
    output_dir = args.output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    block_rows: list[dict[str, Any]] = []
    for loop_block_summary_path in sorted(loop_output_dir.glob("blocks/*/block_summary.json")):
        block_dir = loop_block_summary_path.parent
        block_key = block_dir.name
        baseline_block_summary_path = baseline_dir / "blocks" / block_key / "block_summary.json"
        loop_summary = _load_json(loop_block_summary_path)
        baseline_summary = _load_json(baseline_block_summary_path) if baseline_block_summary_path.exists() else None
        delta_vs_baseline = (
            {
                key: int(loop_summary.get(key, 0)) - int(baseline_summary.get(key, 0))
                for key in DELTA_KEYS
            }
            if baseline_summary is not None
            else None
        )
        loop_cross_source = _extract_cross_source_metrics(loop_summary.get("cross_source_summary"))
        baseline_cross_source = _extract_cross_source_metrics(
            baseline_summary.get("cross_source_summary") if baseline_summary is not None else None
        )
        cross_source_delta_vs_baseline = (
            {
                key: int(loop_cross_source.get(key, 0) or 0) - int(baseline_cross_source.get(key, 0) or 0)
                for key in sorted(set(loop_cross_source) | set(baseline_cross_source))
            }
            if loop_cross_source is not None and baseline_cross_source is not None
            else None
        )
        row = {
            "block_key": block_key,
            "loop_summary": loop_summary,
            "baseline_summary": baseline_summary,
            "delta_vs_baseline": delta_vs_baseline,
            "cross_source_delta_vs_baseline": cross_source_delta_vs_baseline,
        }
        block_rows.append(row)

    summary = {
        "loop_output_dir": str(loop_output_dir),
        "baseline_dir": str(baseline_dir),
        "blocks_compared": len(block_rows),
        "blocks": block_rows,
    }
    (output_dir / "summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
