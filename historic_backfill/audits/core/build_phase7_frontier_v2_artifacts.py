from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import pandas as pd

from historic_backfill.audits.core.reviewed_release_policy import REVIEWED_POLICY_COLUMNS, normalize_game_id


INVENTORY_COLUMNS = [
    "game_id",
    "block_key",
    "season",
    "lane",
    "recommended_next_action",
    "has_event_on_court_issue",
    "has_material_minute_issue",
    "has_severe_minute_issue",
    "n_actionable_event_rows",
    "max_abs_minute_diff",
    "n_pm_reference_delta_rows",
    "notes",
]

SHORTLIST_COLUMNS = [
    "game_id",
    "block_key",
    "current_lane",
    "current_blocker_status",
    "recommended_execution_lane",
    "next_step",
    "evidence_basis",
]

ADDITION_COLUMNS = [
    "game_id",
    "block_key",
    "season",
    "lane",
    "recommended_next_action",
    "notes",
    "current_blocker_status",
    "recommended_execution_lane",
    "next_step",
    "evidence_basis",
    "release_gate_status",
    "release_reason_code",
    "execution_lane",
    "blocks_release",
    "research_open",
    "expected_primary_quality_status",
]

RAW_METRIC_COLUMNS = [
    "has_event_on_court_issue",
    "has_material_minute_issue",
    "has_severe_minute_issue",
    "n_actionable_event_rows",
    "max_abs_minute_diff",
    "n_pm_reference_delta_rows",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build Phase 7 v2 frontier inventory/shortlist/overlay artifacts and a coverage checkpoint summary."
    )
    parser.add_argument("--raw-game-quality-csv", type=Path, required=True)
    parser.add_argument("--base-inventory-csv", type=Path, required=True)
    parser.add_argument("--base-shortlist-csv", type=Path, required=True)
    parser.add_argument("--base-overlay-csv", type=Path, required=True)
    parser.add_argument("--additions-csv", type=Path, required=True)
    parser.add_argument("--output-inventory-csv", type=Path, required=True)
    parser.add_argument("--output-shortlist-csv", type=Path, required=True)
    parser.add_argument("--output-overlay-csv", type=Path, required=True)
    parser.add_argument("--coverage-summary-json", type=Path, required=True)
    parser.add_argument("--policy-decision-id", default="reviewed_release_policy_20260323_v2")
    parser.add_argument("--reviewed-at", default="2026-03-23")
    parser.add_argument("--expected-overlay-row-count", type=int)
    parser.add_argument("--expected-covered-count", type=int)
    parser.add_argument("--expected-uncovered-game-ids", nargs="*")
    parser.add_argument("--require-full-coverage", action="store_true", default=False)
    return parser.parse_args()


def _load_csv(path: Path, *, required_columns: list[str]) -> pd.DataFrame:
    df = pd.read_csv(path, dtype={"game_id": str}).fillna("")
    missing = [column for column in required_columns if column not in df.columns]
    if missing:
        raise ValueError(f"{path} missing columns: {missing}")
    if not df.empty:
        df["game_id"] = df["game_id"].map(normalize_game_id)
    return df


def _load_base_decisions(
    *,
    inventory_csv: Path,
    shortlist_csv: Path,
    overlay_csv: Path,
) -> pd.DataFrame:
    inventory_df = _load_csv(inventory_csv, required_columns=INVENTORY_COLUMNS)
    shortlist_df = _load_csv(shortlist_csv, required_columns=SHORTLIST_COLUMNS)
    overlay_df = _load_csv(overlay_csv, required_columns=REVIEWED_POLICY_COLUMNS)

    inventory_ids = set(inventory_df["game_id"])
    shortlist_ids = set(shortlist_df["game_id"])
    overlay_ids = set(overlay_df["game_id"])
    if not (inventory_ids == shortlist_ids == overlay_ids):
        raise ValueError(
            "Base frontier set mismatch: "
            f"inventory_only={sorted(inventory_ids - shortlist_ids - overlay_ids)}, "
            f"shortlist_only={sorted(shortlist_ids - inventory_ids - overlay_ids)}, "
            f"overlay_only={sorted(overlay_ids - inventory_ids - shortlist_ids)}"
        )

    merged = inventory_df.merge(
        shortlist_df,
        on=["game_id", "block_key"],
        how="inner",
    ).merge(
        overlay_df[
            [
                "game_id",
                "release_gate_status",
                "release_reason_code",
                "execution_lane",
                "blocks_release",
                "research_open",
                "expected_primary_quality_status",
                "notes",
            ]
        ].rename(columns={"notes": "overlay_notes"}),
        on="game_id",
        how="inner",
    )
    if not (merged["lane"] == merged["current_lane"]).all():
        mismatch_df = merged.loc[merged["lane"] != merged["current_lane"], ["game_id", "lane", "current_lane"]]
        raise ValueError(f"Base lane mismatch: {mismatch_df.to_dict(orient='records')}")
    merged["notes"] = merged["notes"].where(merged["notes"].astype(str) != "", merged["overlay_notes"])
    return merged[
        [
            "game_id",
            "block_key",
            "season",
            "lane",
            "recommended_next_action",
            "notes",
            "current_blocker_status",
            "recommended_execution_lane",
            "next_step",
            "evidence_basis",
            "release_gate_status",
            "release_reason_code",
            "execution_lane",
            "blocks_release",
            "research_open",
            "expected_primary_quality_status",
        ]
    ].copy()


def _load_additions(path: Path) -> pd.DataFrame:
    additions_df = _load_csv(path, required_columns=ADDITION_COLUMNS)
    if additions_df.empty:
        raise ValueError("Additions CSV must be non-empty")
    return additions_df[ADDITION_COLUMNS].copy()


def _boolish(value: Any) -> bool:
    text = str(value).strip().lower()
    if text in {"true", "1", "yes", "y"}:
        return True
    if text in {"false", "0", "no", "n", ""}:
        return False
    raise ValueError(f"Unsupported boolean value: {value!r}")


def main() -> int:
    args = parse_args()
    base_df = _load_base_decisions(
        inventory_csv=args.base_inventory_csv.resolve(),
        shortlist_csv=args.base_shortlist_csv.resolve(),
        overlay_csv=args.base_overlay_csv.resolve(),
    )
    additions_df = _load_additions(args.additions_csv.resolve())
    overlap = sorted(set(base_df["game_id"]) & set(additions_df["game_id"]))
    if overlap:
        raise ValueError(f"Additions overlap existing reviewed set: {overlap}")

    decisions_df = pd.concat([base_df, additions_df], ignore_index=True)
    if decisions_df["game_id"].duplicated().any():
        duplicate_ids = sorted(decisions_df.loc[decisions_df["game_id"].duplicated(), "game_id"].unique().tolist())
        raise ValueError(f"Combined reviewed decisions contain duplicate game_ids: {duplicate_ids}")

    raw_game_quality_df = _load_csv(
        args.raw_game_quality_csv.resolve(),
        required_columns=["game_id", "primary_quality_status", *RAW_METRIC_COLUMNS],
    )
    raw_open_df = raw_game_quality_df.loc[raw_game_quality_df["primary_quality_status"] == "open"].copy()
    raw_open_ids = set(raw_open_df["game_id"])

    reviewed_ids = set(decisions_df["game_id"])
    stale_reviewed_ids = sorted(reviewed_ids - raw_open_ids)
    if stale_reviewed_ids:
        raise ValueError(f"Reviewed frontier rows are no longer raw-open: {stale_reviewed_ids}")

    inventory_df = decisions_df.merge(
        raw_open_df[["game_id", *RAW_METRIC_COLUMNS]],
        on="game_id",
        how="inner",
    )
    if len(inventory_df) != len(decisions_df):
        missing_metrics = sorted(reviewed_ids - set(inventory_df["game_id"]))
        raise ValueError(f"Missing raw-open metrics for reviewed games: {missing_metrics}")

    shortlist_df = decisions_df.copy()
    shortlist_df["current_lane"] = shortlist_df["lane"]
    overlay_df = decisions_df.copy()
    overlay_df["policy_decision_id"] = str(args.policy_decision_id)
    overlay_df["policy_source"] = "reviewed_override"
    overlay_df["evidence_artifact"] = str(args.output_shortlist_csv.resolve())
    overlay_df["reviewed_at"] = str(args.reviewed_at)

    inventory_out = inventory_df[INVENTORY_COLUMNS].sort_values(["block_key", "season", "game_id"]).reset_index(drop=True)
    shortlist_out = shortlist_df[SHORTLIST_COLUMNS].sort_values(["block_key", "game_id"]).reset_index(drop=True)
    overlay_out = overlay_df[REVIEWED_POLICY_COLUMNS].sort_values(["game_id"]).reset_index(drop=True)

    output_sets = [set(inventory_out["game_id"]), set(shortlist_out["game_id"]), set(overlay_out["game_id"])]
    if not (output_sets[0] == output_sets[1] == output_sets[2]):
        raise ValueError("Output inventory/shortlist/overlay game sets do not match")

    covered_raw_open_ids = sorted(raw_open_ids & reviewed_ids)
    uncovered_raw_open_ids = sorted(raw_open_ids - reviewed_ids)

    if args.expected_overlay_row_count is not None and len(overlay_out) != int(args.expected_overlay_row_count):
        raise ValueError(
            f"Expected overlay row count {args.expected_overlay_row_count}, found {len(overlay_out)}"
        )
    if args.expected_covered_count is not None and len(covered_raw_open_ids) != int(args.expected_covered_count):
        raise ValueError(
            f"Expected covered raw-open count {args.expected_covered_count}, found {len(covered_raw_open_ids)}"
        )
    if args.expected_uncovered_game_ids is not None:
        expected_uncovered = sorted(normalize_game_id(value) for value in args.expected_uncovered_game_ids)
        if uncovered_raw_open_ids != expected_uncovered:
            raise ValueError(
                f"Unexpected uncovered raw-open set: expected {expected_uncovered}, found {uncovered_raw_open_ids}"
            )
    if args.require_full_coverage and uncovered_raw_open_ids:
        raise ValueError(f"Full coverage required but uncovered raw-open games remain: {uncovered_raw_open_ids}")

    release_blocking_overlay_game_ids = sorted(
        overlay_out.loc[overlay_out["blocks_release"].map(_boolish), "game_id"].astype(str).tolist()
    )
    research_open_overlay_game_ids = sorted(
        overlay_out.loc[overlay_out["research_open"].map(_boolish), "game_id"].astype(str).tolist()
    )

    args.output_inventory_csv.parent.mkdir(parents=True, exist_ok=True)
    args.output_shortlist_csv.parent.mkdir(parents=True, exist_ok=True)
    args.output_overlay_csv.parent.mkdir(parents=True, exist_ok=True)
    inventory_out.to_csv(args.output_inventory_csv, index=False)
    shortlist_out.to_csv(args.output_shortlist_csv, index=False)
    overlay_out.to_csv(args.output_overlay_csv, index=False)

    summary = {
        "raw_game_quality_csv": str(args.raw_game_quality_csv.resolve()),
        "base_inventory_csv": str(args.base_inventory_csv.resolve()),
        "base_shortlist_csv": str(args.base_shortlist_csv.resolve()),
        "base_overlay_csv": str(args.base_overlay_csv.resolve()),
        "additions_csv": str(args.additions_csv.resolve()),
        "output_inventory_csv": str(args.output_inventory_csv.resolve()),
        "output_shortlist_csv": str(args.output_shortlist_csv.resolve()),
        "output_overlay_csv": str(args.output_overlay_csv.resolve()),
        "reviewed_policy_overlay_version": str(args.policy_decision_id),
        "frontier_inventory_snapshot_id": args.output_inventory_csv.resolve().stem,
        "overlay_row_count": int(len(overlay_out)),
        "raw_open_game_count": int(len(raw_open_df)),
        "reviewed_overlay_row_count": int(len(overlay_out)),
        "covered_raw_open_count": int(len(covered_raw_open_ids)),
        "uncovered_raw_open_count": int(len(uncovered_raw_open_ids)),
        "covered_raw_open_game_ids": covered_raw_open_ids,
        "uncovered_raw_open_game_ids": uncovered_raw_open_ids,
        "stale_reviewed_game_ids": stale_reviewed_ids,
        "release_blocking_game_ids": release_blocking_overlay_game_ids,
        "release_blocking_overlay_game_ids": release_blocking_overlay_game_ids,
        "research_open_game_ids": research_open_overlay_game_ids,
        "research_open_overlay_game_ids": research_open_overlay_game_ids,
        "full_coverage": not uncovered_raw_open_ids,
    }
    args.coverage_summary_json.parent.mkdir(parents=True, exist_ok=True)
    args.coverage_summary_json.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
