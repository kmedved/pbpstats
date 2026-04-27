from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import pandas as pd


CASEBOOK_BUCKETS: dict[str, dict[str, Any]] = {
    "opening_cluster_controls": {
        "recommended_next_track": "keep_as_non_regression_controls",
        "case_keys": [
            ("0021800748", 3, 1610612746),
            ("0020400650", 3, 1610612750),
            ("0021900523", 3, 1610612763),
            ("0021700813", 4, 1610612764),
        ],
    },
    "teaching_cases": {
        "recommended_next_track": "inspect_as_primary_upstream_fix_teachers",
        "case_keys": [
            ("0021700917", 1, 1610612744),
            ("0021700236", 1, 1610612751),
            ("0029800063", 2, 1610612742),
            ("0029800063", 4, 1610612742),
            ("0020400526", 3, 1610612764),
        ],
    },
    "companion_cases": {
        "recommended_next_track": "use_as_same_family_companions_after_primary_teachers",
        "case_keys": [
            ("0021700514", 2, 1610612762),
        ],
    },
    "noisy_edge_variants": {
        "recommended_next_track": "keep_visible_but_do_not_teach_runtime_rule_from_them_yet",
        "case_keys": [
            ("0021801067", 3, 1610612738),
            ("0021900333", 4, 1610612756),
        ],
    },
    "modern_residual_shape": {
        "recommended_next_track": "treat_as_separate_modern_residual_until_reframed_as_upstream_bug",
        "case_keys": [
            ("0021900487", 2, 1610612763),
            ("0021900920", 2, 1610612763),
        ],
    },
}


def _normalize_game_id(value: Any) -> str:
    return str(int(value)).zfill(10)


def _load_csv(path: Path) -> pd.DataFrame:
    return pd.read_csv(path) if path.exists() else pd.DataFrame()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build a casebook artifact for the current same-clock boundary frontier."
    )
    parser.add_argument("--frontier-summary-dir", type=Path, required=True)
    parser.add_argument("--queue-dir", type=Path, required=True)
    parser.add_argument("--inspection-dir", type=Path, action="append", default=[])
    parser.add_argument("--output-dir", type=Path, required=True)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    output_dir = args.output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    case_df = _load_csv(args.frontier_summary_dir.resolve() / "same_clock_boundary_case_summary.csv")
    queue_df = _load_csv(args.queue_dir.resolve() / "same_clock_boundary_queue_all.csv")
    if not case_df.empty:
        case_df = case_df.copy()
        case_df["game_id"] = case_df["game_id"].map(_normalize_game_id)
        case_df["period"] = pd.to_numeric(case_df["period"], errors="coerce").fillna(0).astype(int)
        case_df["team_id"] = pd.to_numeric(case_df["team_id"], errors="coerce").fillna(0).astype(int)
    if not queue_df.empty:
        queue_df = queue_df.copy()
        queue_df["game_id"] = queue_df["game_id"].map(_normalize_game_id)
        queue_df["period"] = pd.to_numeric(queue_df["period"], errors="coerce").fillna(0).astype(int)
        queue_df["team_id"] = pd.to_numeric(queue_df["team_id"], errors="coerce").fillna(0).astype(int)

    inspection_index: dict[tuple[str, int, int], str] = {}
    for inspection_root in args.inspection_dir:
        for child in inspection_root.resolve().glob("*_P*_T*"):
            try:
                game_id, rest = child.name.split("_P", 1)
                period_text, team_text = rest.split("_T", 1)
                inspection_index[(_normalize_game_id(game_id), int(period_text), int(team_text))] = str(child)
            except Exception:
                continue

    rows: list[dict[str, Any]] = []
    bucket_summary: dict[str, Any] = {}
    for bucket, meta in CASEBOOK_BUCKETS.items():
        bucket_rows: list[dict[str, Any]] = []
        for game_id, period, team_id in meta["case_keys"]:
            case_key = (_normalize_game_id(game_id), int(period), int(team_id))
            case_row = (
                case_df.loc[
                    (case_df["game_id"] == case_key[0])
                    & (case_df["period"] == case_key[1])
                    & (case_df["team_id"] == case_key[2])
                ]
                .head(1)
                .to_dict(orient="records")
            )
            queue_row = (
                queue_df.loc[
                    (queue_df["game_id"] == case_key[0])
                    & (queue_df["period"] == case_key[1])
                    & (queue_df["team_id"] == case_key[2])
                ]
                .head(1)
                .to_dict(orient="records")
            )
            record = {
                "bucket": bucket,
                "recommended_next_track": meta["recommended_next_track"],
                "game_id": case_key[0],
                "period": case_key[1],
                "team_id": case_key[2],
                "case_summary": case_row[0] if case_row else {},
                "queue_row": queue_row[0] if queue_row else {},
                "inspection_dir": inspection_index.get(case_key, ""),
            }
            bucket_rows.append(record)
            rows.append(record)
        bucket_summary[bucket] = {
            "recommended_next_track": meta["recommended_next_track"],
            "cases": bucket_rows,
        }

    flat_rows = []
    for row in rows:
        case_summary = row["case_summary"]
        queue_row = row["queue_row"]
        flat_rows.append(
            {
                "bucket": row["bucket"],
                "recommended_next_track": row["recommended_next_track"],
                "game_id": row["game_id"],
                "period": row["period"],
                "team_id": row["team_id"],
                "same_clock_family": case_summary.get("same_clock_family") or queue_row.get("same_clock_family") or "",
                "is_manifest_positive": case_summary.get("is_manifest_positive"),
                "family_rank": case_summary.get("family_rank") or queue_row.get("family_rank"),
                "event_issue_rows": case_summary.get("event_issue_rows"),
                "minutes_mismatch_rows": case_summary.get("minutes_mismatch_rows"),
                "plus_minus_mismatch_rows": case_summary.get("plus_minus_mismatch_rows"),
                "game_max_minutes_abs_diff": case_summary.get("game_max_minutes_abs_diff"),
                "queue_role": queue_row.get("queue_role", ""),
                "inspection_dir": row["inspection_dir"],
            }
        )

    pd.DataFrame(flat_rows).to_csv(output_dir / "same_clock_boundary_casebook.csv", index=False)
    summary = {
        "bucket_counts": {bucket: len(meta["cases"]) for bucket, meta in bucket_summary.items()},
        "buckets": bucket_summary,
    }
    (output_dir / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
