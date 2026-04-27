from __future__ import annotations

import argparse
import json
import shutil
from pathlib import Path

import pandas as pd

from build_lineup_residual_outputs import GAME_QUALITY_OUTPUT_COLUMNS
from reviewed_release_policy import apply_release_policy, load_reviewed_policy_overlay, normalize_game_id


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Build a reviewed release-policy residual bundle from an existing raw residual bundle "
            "without changing the underlying residual register files."
        )
    )
    parser.add_argument("--source-bundle-dir", type=Path, required=True)
    parser.add_argument("--reviewed-policy-overlay-csv", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    return parser.parse_args()


def _overlay_version(overlay_by_game: dict[str, dict[str, object]]) -> str:
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


def _copy_passthrough_files(source_dir: Path, output_dir: Path) -> None:
    for path in sorted(source_dir.iterdir()):
        if path.name in {"game_quality.csv", "summary.json"}:
            continue
        if path.is_file():
            shutil.copy2(path, output_dir / path.name)


def main() -> int:
    args = parse_args()
    source_dir = args.source_bundle_dir.resolve()
    output_dir = args.output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    overlay_by_game = load_reviewed_policy_overlay(args.reviewed_policy_overlay_csv.resolve())
    reviewed_policy_overlay_version = _overlay_version(overlay_by_game)

    source_game_quality = pd.read_csv(source_dir / "game_quality.csv", dtype={"game_id": str})
    source_game_quality["game_id"] = source_game_quality["game_id"].map(normalize_game_id)

    rows: list[dict[str, object]] = []
    for row in source_game_quality.to_dict(orient="records"):
        row.update(apply_release_policy(row, overlay_by_game))
        rows.append(row)
    output_game_quality = pd.DataFrame(rows)[GAME_QUALITY_OUTPUT_COLUMNS].sort_values("game_id").reset_index(drop=True)

    _copy_passthrough_files(source_dir, output_dir)
    output_game_quality.to_csv(output_dir / "game_quality.csv", index=False)

    source_summary = json.loads((source_dir / "summary.json").read_text(encoding="utf-8"))
    release_blocking_game_ids = sorted(
        output_game_quality.loc[output_game_quality["blocks_release"].fillna(False), "game_id"].astype(str).tolist()
    )
    research_open_game_ids = sorted(
        output_game_quality.loc[output_game_quality["research_open"].fillna(False), "game_id"].astype(str).tolist()
    )
    failed_games = int(source_summary.get("raw_counts", {}).get("failed_games", 0) or 0)
    event_stats_errors = int(source_summary.get("raw_counts", {}).get("event_stats_errors", 0) or 0)
    summary = {
        "run_dir": str(source_summary.get("run_dir") or ""),
        "manifest_path": str(source_summary.get("manifest_path") or ""),
        "reviewed_policy_overlay_csv": str(args.reviewed_policy_overlay_csv.resolve()),
        "reviewed_policy_overlay_version": reviewed_policy_overlay_version,
        "raw_counts": source_summary.get("raw_counts") or {},
        "blocker_counts": source_summary.get("blocker_counts") or {},
        "quality_status_counts": output_game_quality["primary_quality_status"].value_counts().sort_index().to_dict(),
        "raw_quality_status_counts": output_game_quality["primary_quality_status"].value_counts().sort_index().to_dict(),
        "release_gate_status_counts": output_game_quality["release_gate_status"].value_counts().sort_index().to_dict(),
        "execution_lane_counts": output_game_quality["execution_lane"].value_counts().sort_index().to_dict(),
        "release_blocking_game_count": len(release_blocking_game_ids),
        "release_blocking_game_ids": release_blocking_game_ids,
        "research_open_game_count": len(research_open_game_ids),
        "research_open_game_ids": research_open_game_ids,
        "tier1_release_ready": bool(
            failed_games == 0 and event_stats_errors == 0 and len(release_blocking_game_ids) == 0
        ),
        "tier2_frontier_closed": bool(
            failed_games == 0
            and event_stats_errors == 0
            and len(release_blocking_game_ids) == 0
            and len(research_open_game_ids) == 0
        ),
    }
    (output_dir / "summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
