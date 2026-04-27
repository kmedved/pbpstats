from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

from reviewed_release_policy import ensure_release_policy_columns, load_reviewed_policy_overlay


ROOT = Path(__file__).resolve().parent

OUTPUT_COLUMNS = [
    "state_context",
    "game_id",
    "block_key",
    "season_group",
    "source_bundle",
    "primary_quality_status",
    "release_gate_status",
    "release_reason_code",
    "execution_lane",
    "blocks_release",
    "research_open",
    "policy_source",
    "has_active_correction",
    "has_open_actionable_residual",
    "has_source_limited_residual",
    "has_boundary_difference",
    "has_material_minute_issue",
    "has_severe_minute_issue",
    "has_event_on_court_issue",
    "n_active_corrections",
    "n_actionable_event_rows",
    "max_abs_minute_diff",
    "sum_abs_minute_diff_over_0_1",
    "n_pm_reference_delta_rows",
]

ABSENT_ROW_DEFAULTS = {
    "primary_quality_status": "exact",
    "release_gate_status": "exact",
    "release_reason_code": "exact",
    "execution_lane": "exact",
    "blocks_release": False,
    "research_open": False,
    "policy_source": "auto_default",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build a combined sparse reviewed-release quality sidecar from block-level game_quality.csv files."
    )
    parser.add_argument("--residual-dir", type=Path, required=True, action="append")
    parser.add_argument("--reviewed-policy-overlay-csv", type=Path)
    parser.add_argument("--frontier-inventory-csv", type=Path)
    parser.add_argument("--output-dir", type=Path, required=True)
    return parser.parse_args()


def _normalize_game_id(value: object) -> str:
    return str(int(value)).zfill(10)


def _bundle_metadata(bundle_name: str) -> tuple[str, str]:
    if "_" not in bundle_name:
        return "", bundle_name
    block_key, season_group = bundle_name.split("_", 1)
    return block_key, season_group


def _bool_series(series: pd.Series) -> pd.Series:
    return series.astype(str).str.lower().isin({"true", "1", "yes", "y"})


def _overlay_version(overlay_csv: Path | None) -> str:
    overlay_by_game = load_reviewed_policy_overlay(overlay_csv)
    versions = sorted(
        {
            str(policy.get("policy_decision_id") or "")
            for policy in overlay_by_game.values()
            if str(policy.get("policy_decision_id") or "")
        }
    )
    if len(versions) > 1:
        raise ValueError(f"Expected exactly one reviewed policy overlay version, found {versions}")
    return versions[0] if versions else ""


def main() -> int:
    args = parse_args()
    output_dir = args.output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    reviewed_policy_overlay_version = _overlay_version(
        args.reviewed_policy_overlay_csv.resolve() if args.reviewed_policy_overlay_csv is not None else None
    )
    frontier_inventory_snapshot_id = (
        args.frontier_inventory_csv.resolve().stem if args.frontier_inventory_csv is not None else ""
    )

    frames: list[pd.DataFrame] = []
    for residual_dir in [path.resolve() for path in args.residual_dir]:
        game_quality_path = residual_dir / "game_quality.csv"
        if not game_quality_path.exists():
            raise FileNotFoundError(f"Missing game_quality.csv: {game_quality_path}")
        df = pd.read_csv(game_quality_path, dtype={"game_id": str})
        if df.empty:
            continue
        df = ensure_release_policy_columns(df)
        block_key, season_group = _bundle_metadata(residual_dir.name)
        df["game_id"] = df["game_id"].map(_normalize_game_id)
        df["state_context"] = "live_state"
        df["block_key"] = block_key
        df["season_group"] = season_group
        df["source_bundle"] = residual_dir.name
        for column in ["blocks_release", "research_open"]:
            if column in df.columns:
                df[column] = _bool_series(df[column])
        frames.append(df)

    sidecar_df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=OUTPUT_COLUMNS)
    if not sidecar_df.empty and sidecar_df["game_id"].duplicated().any():
        duplicates = sidecar_df.loc[sidecar_df["game_id"].duplicated(), "game_id"].tolist()
        raise ValueError(f"Duplicate game_id rows in release quality sidecar build: {duplicates}")

    if not sidecar_df.empty:
        sidecar_df = sidecar_df[OUTPUT_COLUMNS].sort_values(["block_key", "game_id"]).reset_index(drop=True)
    sidecar_df.to_csv(output_dir / "game_quality_sparse.csv", index=False)

    summary = {
        "residual_dirs": [str(path.resolve()) for path in args.residual_dir],
        "reviewed_policy_overlay_version": reviewed_policy_overlay_version,
        "frontier_inventory_snapshot_id": frontier_inventory_snapshot_id,
        "row_count": int(len(sidecar_df)),
        "unique_game_count": int(sidecar_df["game_id"].nunique()) if not sidecar_df.empty else 0,
        "coverage": "sparse_problem_or_reviewed_games_only",
        "default_absent_row_values": ABSENT_ROW_DEFAULTS,
        "release_gate_status_counts": (
            sidecar_df["release_gate_status"].value_counts().sort_index().to_dict() if not sidecar_df.empty else {}
        ),
        "execution_lane_counts": (
            sidecar_df["execution_lane"].value_counts().sort_index().to_dict() if not sidecar_df.empty else {}
        ),
        "policy_source_counts": (
            sidecar_df["policy_source"].value_counts().sort_index().to_dict() if not sidecar_df.empty else {}
        ),
        "release_blocking_game_count": int(sidecar_df["blocks_release"].sum()) if not sidecar_df.empty else 0,
        "release_blocking_game_ids": (
            sorted(sidecar_df.loc[sidecar_df["blocks_release"], "game_id"].astype(str).tolist())
            if not sidecar_df.empty
            else []
        ),
        "research_open_game_count": int(sidecar_df["research_open"].sum()) if not sidecar_df.empty else 0,
        "research_open_game_ids": (
            sorted(sidecar_df.loc[sidecar_df["research_open"], "game_id"].astype(str).tolist())
            if not sidecar_df.empty
            else []
        ),
        "reviewed_override_game_count": int((sidecar_df["policy_source"] == "reviewed_override").sum())
        if not sidecar_df.empty
        else 0,
    }
    (output_dir / "summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    join_contract = {
        "join_key": "game_id",
        "join_strategy": "left_join_sparse_game_quality_sidecar",
        "join_target": "historical_darko_player_rows",
        "coverage": "sparse_problem_or_reviewed_games_only",
        "consumer_rule": "If a game_id is absent from the sidecar, apply default_absent_row_values.",
        "default_absent_row_values": ABSENT_ROW_DEFAULTS,
        "notes": [
            "This pass does not add a per-row quality column to parquet.",
            "Release-facing game flags come from the sparse sidecar.",
            "Reviewed frontier and PM artifacts remain separate audit surfaces.",
        ],
    }
    (output_dir / "join_contract.json").write_text(json.dumps(join_contract, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    integration_notes = """# Reviewed Release Quality Sidecar
This artifact is the canonical downstream join surface for the reviewed March 22 release-policy layer.

Files:
- `game_quality_sparse.csv`
- `summary.json`
- `join_contract.json`

Join rule:
- left join on `game_id`
- if no row is present, treat the game as the default exact / non-blocking case

Default absent-row values:
- `primary_quality_status = exact`
- `release_gate_status = exact`
- `release_reason_code = exact`
- `execution_lane = exact`
- `blocks_release = false`
- `research_open = false`
- `policy_source = auto_default`

This sidecar is intentionally sparse. It contains reviewed/open/corrected/source-limited/boundary games emitted by the release-policy residual bundles, not every historical game row.
"""
    (output_dir / "integration_notes.md").write_text(integration_notes, encoding="utf-8")

    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
