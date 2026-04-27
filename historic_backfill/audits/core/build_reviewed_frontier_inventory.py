from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

from historic_backfill.audits.core.reviewed_release_policy import ensure_release_policy_columns, load_reviewed_policy_overlay, normalize_game_id


OUTPUT_COLUMNS = [
    "state_context",
    "game_id",
    "block_key",
    "season",
    "current_lane",
    "current_blocker_status",
    "release_gate_status",
    "release_reason_code",
    "execution_lane",
    "policy_source",
    "blocks_release",
    "research_open",
    "has_event_on_court_issue",
    "has_material_minute_issue",
    "has_severe_minute_issue",
    "n_actionable_event_rows",
    "max_abs_minute_diff",
    "n_pm_reference_delta_rows",
    "recommended_next_action",
    "next_step",
    "notes",
    "evidence_basis",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build raw/release/reviewed frontier inventories from residual bundles and the reviewed policy overlay."
    )
    parser.add_argument("--residual-dir", type=Path, required=True, action="append")
    parser.add_argument("--inventory-csv", type=Path, required=True)
    parser.add_argument("--shortlist-csv", type=Path, required=True)
    parser.add_argument("--reviewed-policy-overlay-csv", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    return parser.parse_args()


def _load_run_counts(path: Path) -> dict[str, int]:
    summary = json.loads(path.read_text(encoding="utf-8"))
    return {
        "failed_games": int(summary.get("raw_counts", {}).get("failed_games", 0) or 0),
        "event_stats_errors": int(summary.get("raw_counts", {}).get("event_stats_errors", 0) or 0),
    }


def _load_game_quality_frames(residual_dirs: list[Path]) -> tuple[pd.DataFrame, dict[str, int]]:
    frames: list[pd.DataFrame] = []
    run_counts = {"failed_games": 0, "event_stats_errors": 0}
    for bundle_order, residual_dir in enumerate(residual_dirs):
        game_quality_path = residual_dir / "game_quality.csv"
        summary_path = residual_dir / "summary.json"
        game_quality_df = pd.read_csv(game_quality_path)
        game_quality_df = ensure_release_policy_columns(game_quality_df)
        if not game_quality_df.empty:
            game_quality_df["game_id"] = game_quality_df["game_id"].map(normalize_game_id)
            game_quality_df["residual_bundle"] = residual_dir.name
            game_quality_df["bundle_order"] = bundle_order
            frames.append(game_quality_df)
        counts = _load_run_counts(summary_path)
        run_counts["failed_games"] += counts["failed_games"]
        run_counts["event_stats_errors"] += counts["event_stats_errors"]

    game_quality_df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    if not game_quality_df.empty:
        game_quality_df = (
            game_quality_df.sort_values("bundle_order")
            .drop_duplicates(subset=["game_id"], keep="last")
            .reset_index(drop=True)
        )
    return game_quality_df, run_counts


def main() -> int:
    args = parse_args()
    residual_dirs = [path.resolve() for path in args.residual_dir]
    output_dir = args.output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    overlay_by_game = load_reviewed_policy_overlay(args.reviewed_policy_overlay_csv.resolve())
    overlay_df = pd.DataFrame(list(overlay_by_game.values()))
    if overlay_df.empty:
        raise ValueError("Reviewed policy overlay must be non-empty for frontier inventory build")
    overlay_df["game_id"] = overlay_df["game_id"].map(normalize_game_id)
    if overlay_df["policy_decision_id"].nunique() != 1:
        raise ValueError(
            "Reviewed policy overlay must contain exactly one policy_decision_id value, found "
            f"{sorted(overlay_df['policy_decision_id'].astype(str).unique().tolist())}"
        )
    reviewed_policy_overlay_version = str(overlay_df["policy_decision_id"].iloc[0])
    frontier_inventory_snapshot_id = args.inventory_csv.resolve().stem

    inventory_df = pd.read_csv(args.inventory_csv)
    shortlist_df = pd.read_csv(args.shortlist_csv)
    inventory_df["game_id"] = inventory_df["game_id"].map(normalize_game_id)
    shortlist_df["game_id"] = shortlist_df["game_id"].map(normalize_game_id)
    inventory_df = inventory_df.rename(columns={"lane": "current_lane"})

    overlay_game_ids = set(overlay_df["game_id"])
    inventory_game_ids = set(inventory_df["game_id"])
    if overlay_game_ids != inventory_game_ids:
        raise ValueError(
            f"Overlay/frontier inventory mismatch: overlay_only={sorted(overlay_game_ids - inventory_game_ids)}, "
            f"inventory_only={sorted(inventory_game_ids - overlay_game_ids)}"
        )

    game_quality_df, run_counts = _load_game_quality_frames(residual_dirs)
    raw_open_df = game_quality_df.loc[game_quality_df["primary_quality_status"] == "open"].copy()
    raw_open_game_ids = set(raw_open_df["game_id"])

    stale_overlay_games = sorted(overlay_game_ids - raw_open_game_ids)
    if stale_overlay_games:
        raise ValueError(f"Reviewed overlay rows no longer map to raw-open games: {stale_overlay_games}")

    merged = raw_open_df.merge(
        inventory_df[
            [
                "game_id",
                "block_key",
                "season",
                "current_lane",
                "recommended_next_action",
                "notes",
            ]
        ],
        on="game_id",
        how="left",
    ).merge(
        shortlist_df[
            [
                "game_id",
                "current_blocker_status",
                "next_step",
                "evidence_basis",
            ]
        ],
        on="game_id",
        how="left",
    )

    merged["state_context"] = "live_state"
    merged["current_lane"] = merged["current_lane"].fillna("")
    merged["current_blocker_status"] = merged["current_blocker_status"].fillna("")
    merged["recommended_next_action"] = merged["recommended_next_action"].fillna("")
    merged["next_step"] = merged["next_step"].fillna("")
    merged["notes"] = merged["notes"].fillna("")
    merged["evidence_basis"] = merged["evidence_basis"].fillna("")
    accepted_contradiction_mask = merged["execution_lane"] == "accepted_contradiction"
    merged.loc[accepted_contradiction_mask, "recommended_next_action"] = "accept_unresolvable_contradiction"
    merged.loc[
        accepted_contradiction_mask,
        "next_step",
    ] = "Keep as accepted contradiction; no local override retry and no research-open hold."

    raw_open_inventory = merged[OUTPUT_COLUMNS].sort_values(["block_key", "season", "game_id"]).reset_index(drop=True)
    release_blocker_inventory = raw_open_inventory.loc[raw_open_inventory["blocks_release"].fillna(False)].copy()
    reviewed_frontier_inventory = raw_open_inventory.loc[~raw_open_inventory["blocks_release"].fillna(False)].copy()

    if not release_blocker_inventory.empty:
        raise ValueError(
            "Reviewed policy overlay incomplete for current corpus; unexpected release blockers remain: "
            f"{release_blocker_inventory['game_id'].tolist()}"
        )

    raw_open_inventory.to_csv(output_dir / "raw_open_inventory.csv", index=False)
    release_blocker_inventory.to_csv(output_dir / "release_blocker_inventory.csv", index=False)
    reviewed_frontier_inventory.to_csv(output_dir / "reviewed_frontier_inventory.csv", index=False)

    research_open_game_ids = reviewed_frontier_inventory.loc[
        reviewed_frontier_inventory["research_open"].fillna(False),
        "game_id",
    ].astype(str).tolist()
    research_open_game_ids = sorted(research_open_game_ids)
    research_open_game_count = len(research_open_game_ids)
    release_blocking_game_count = int(raw_open_inventory["blocks_release"].fillna(False).sum())
    release_blocking_game_ids = sorted(
        raw_open_inventory.loc[raw_open_inventory["blocks_release"].fillna(False), "game_id"].astype(str).tolist()
    )
    summary = {
        "residual_dirs": [str(path) for path in residual_dirs],
        "reviewed_policy_overlay_csv": str(args.reviewed_policy_overlay_csv.resolve()),
        "reviewed_policy_overlay_version": reviewed_policy_overlay_version,
        "frontier_inventory_snapshot_id": frontier_inventory_snapshot_id,
        "authoritative_frontier_inventory_csv": str(args.inventory_csv.resolve()),
        "authoritative_shortlist_csv": str(args.shortlist_csv.resolve()),
        "run_counts": run_counts,
        "total_live_raw_open_games": int(len(raw_open_inventory)),
        "release_blocking_game_count": release_blocking_game_count,
        "release_blocking_game_ids": release_blocking_game_ids,
        "research_open_game_count": research_open_game_count,
        "research_open_game_ids": research_open_game_ids,
        "release_gate_status_counts": (
            raw_open_inventory["release_gate_status"].value_counts().sort_index().to_dict()
            if not raw_open_inventory.empty
            else {}
        ),
        "execution_lane_counts": (
            raw_open_inventory["execution_lane"].value_counts().sort_index().to_dict()
            if not raw_open_inventory.empty
            else {}
        ),
        "policy_source_counts": (
            raw_open_inventory["policy_source"].value_counts().sort_index().to_dict() if not raw_open_inventory.empty else {}
        ),
        "total_live_reviewed_frontier_games": int(len(reviewed_frontier_inventory)),
        "darko_quality_flag_strategy": "sidecar_game_quality_join_on_game_id",
        "darko_adds_parquet_column": False,
        "tier1_release_ready": bool(
            run_counts["failed_games"] == 0
            and run_counts["event_stats_errors"] == 0
            and release_blocking_game_count == 0
        ),
        "tier2_frontier_closed": bool(
            run_counts["failed_games"] == 0
            and run_counts["event_stats_errors"] == 0
            and release_blocking_game_count == 0
            and research_open_game_count == 0
        ),
    }
    (output_dir / "summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
