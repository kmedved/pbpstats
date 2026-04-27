from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import pandas as pd

from reviewed_release_policy import REVIEWED_POLICY_COLUMNS


POLICY_DECISION_ID = "reviewed_release_policy_20260322_v1"
REVIEWED_AT = "2026-03-22"


LANE_TO_POLICY = {
    "same_clock_control_guardrail": {
        "release_gate_status": "accepted_boundary_difference",
        "release_reason_code": "same_clock_control",
        "execution_lane": "policy_frontier_non_local",
        "blocks_release": False,
        "research_open": False,
    },
    "rebound_credit_survivor": {
        "release_gate_status": "accepted_boundary_difference",
        "release_reason_code": "same_clock_rebound_survivor",
        "execution_lane": "policy_frontier_non_local",
        "blocks_release": False,
        "research_open": False,
    },
    "contradiction_period_start_boundary": {
        "release_gate_status": "accepted_unresolvable_contradiction",
        "release_reason_code": "period_start_contradiction",
        "execution_lane": "accepted_contradiction",
        "blocks_release": False,
        "research_open": False,
    },
    "contradiction_mixed_source_case": {
        "release_gate_status": "documented_hold",
        "release_reason_code": "mixed_source_boundary_tail",
        "execution_lane": "documented_hold",
        "blocks_release": False,
        "research_open": True,
    },
    "severe_minute_insufficient_local_context": {
        "release_gate_status": "documented_hold",
        "release_reason_code": "severe_minute_insufficient_local_context",
        "execution_lane": "documented_hold",
        "blocks_release": False,
        "research_open": True,
    },
    "candidate_systematic_defect": {
        "release_gate_status": "documented_hold",
        "release_reason_code": "scrambled_pbp_missing_subs_blockA",
        "execution_lane": "documented_hold",
        "blocks_release": False,
        "research_open": True,
    },
    "same_clock_accumulator_holdout": {
        "release_gate_status": "documented_hold",
        "release_reason_code": "same_clock_accumulator_nonlocal",
        "execution_lane": "documented_hold",
        "blocks_release": False,
        "research_open": True,
    },
    "special_holdout_material_minute": {
        "release_gate_status": "documented_hold",
        "release_reason_code": "source_limited_tradeoff_hold",
        "execution_lane": "documented_hold",
        "blocks_release": False,
        "research_open": True,
    },
}


def _normalize_game_id(value: Any) -> str:
    return str(int(value)).zfill(10)


def _assert_unique_game_ids(df: pd.DataFrame, *, label: str) -> None:
    duplicate_game_ids = sorted(df.loc[df["game_id"].duplicated(), "game_id"].astype(str).unique().tolist())
    if duplicate_game_ids:
        raise ValueError(f"{label} contains duplicate game_id rows: {duplicate_game_ids}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build a reviewed frontier policy overlay from the authoritative frontier inventory and shortlist."
    )
    parser.add_argument("--inventory-csv", type=Path, required=True)
    parser.add_argument("--shortlist-csv", type=Path, required=True)
    parser.add_argument("--output-csv", type=Path, required=True)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    inventory_df = pd.read_csv(args.inventory_csv)
    shortlist_df = pd.read_csv(args.shortlist_csv)

    if inventory_df.empty or shortlist_df.empty:
        raise ValueError("Inventory and shortlist inputs must be non-empty")

    inventory_df["game_id"] = inventory_df["game_id"].map(_normalize_game_id)
    shortlist_df["game_id"] = shortlist_df["game_id"].map(_normalize_game_id)
    _assert_unique_game_ids(inventory_df, label="Inventory CSV")
    _assert_unique_game_ids(shortlist_df, label="Shortlist CSV")

    inventory_games = set(inventory_df["game_id"])
    shortlist_games = set(shortlist_df["game_id"])
    if inventory_games != shortlist_games:
        raise ValueError(
            f"Inventory/shortlist game mismatch: inventory_only={sorted(inventory_games - shortlist_games)}, "
            f"shortlist_only={sorted(shortlist_games - inventory_games)}"
        )

    merged = inventory_df.merge(
        shortlist_df[["game_id", "current_lane", "current_blocker_status", "recommended_execution_lane", "evidence_basis"]],
        on="game_id",
        how="inner",
    )
    if not (merged["lane"] == merged["current_lane"]).all():
        mismatch_df = merged.loc[merged["lane"] != merged["current_lane"], ["game_id", "lane", "current_lane"]]
        raise ValueError(f"Inventory/shortlist lane mismatch: {mismatch_df.to_dict(orient='records')}")

    overlay_rows: list[dict[str, Any]] = []
    for row in merged.to_dict(orient="records"):
        lane = str(row["lane"])
        if lane not in LANE_TO_POLICY:
            raise ValueError(f"Unsupported reviewed frontier lane: {lane}")
        expected_primary_quality_status = "open"
        if not str(row["current_blocker_status"]).startswith("open"):
            raise ValueError(
                f"Expected shortlist blocker status to stay open-like for {row['game_id']}, got {row['current_blocker_status']}"
            )
        policy = LANE_TO_POLICY[lane]
        overlay_rows.append(
            {
                "policy_decision_id": POLICY_DECISION_ID,
                "game_id": row["game_id"],
                "release_gate_status": policy["release_gate_status"],
                "release_reason_code": policy["release_reason_code"],
                "execution_lane": policy["execution_lane"],
                "blocks_release": policy["blocks_release"],
                "research_open": policy["research_open"],
                "policy_source": "reviewed_override",
                "expected_primary_quality_status": expected_primary_quality_status,
                "evidence_artifact": str(args.shortlist_csv.resolve()),
                "reviewed_at": REVIEWED_AT,
                "notes": str(row.get("notes") or ""),
            }
        )

    overlay_df = pd.DataFrame(overlay_rows, columns=REVIEWED_POLICY_COLUMNS).sort_values("game_id")
    _assert_unique_game_ids(overlay_df, label="Reviewed policy overlay")
    args.output_csv.parent.mkdir(parents=True, exist_ok=True)
    overlay_df.to_csv(args.output_csv, index=False)

    release_blocking_game_ids = sorted(
        overlay_df.loc[overlay_df["blocks_release"], "game_id"].astype(str).tolist()
    )
    research_open_game_ids = sorted(
        overlay_df.loc[overlay_df["research_open"], "game_id"].astype(str).tolist()
    )
    summary = {
        "policy_decision_id": POLICY_DECISION_ID,
        "reviewed_policy_overlay_version": POLICY_DECISION_ID,
        "reviewed_at": REVIEWED_AT,
        "frontier_inventory_snapshot_id": args.inventory_csv.resolve().stem,
        "inventory_csv": str(args.inventory_csv.resolve()),
        "shortlist_csv": str(args.shortlist_csv.resolve()),
        "output_csv": str(args.output_csv.resolve()),
        "overlay_row_count": int(len(overlay_df)),
        "execution_lane_counts": overlay_df["execution_lane"].value_counts().sort_index().to_dict(),
        "release_gate_status_counts": overlay_df["release_gate_status"].value_counts().sort_index().to_dict(),
        "release_blocking_game_ids": release_blocking_game_ids,
        "research_open_game_ids": research_open_game_ids,
    }
    summary_path = args.output_csv.with_suffix(".summary.json")
    summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
