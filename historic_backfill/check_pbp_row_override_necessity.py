from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import pandas as pd

from cautious_rerun import DEFAULT_DB, DEFAULT_PARQUET
from override_necessity_utils import (
    DEFAULT_VALIDATION_OVERRIDES_PATH,
    compare_boxes as _compare_boxes,
    diff_pipeline_metrics,
    load_namespace_for_necessity,
    load_single_game_df,
    run_game_variant,
)
from recheck_overrides_against_bbr_pbp import _normalize_game_id


ROOT = Path(__file__).resolve().parent
DEFAULT_OVERRIDES_PATH = ROOT / "pbp_row_overrides.csv"
DEFAULT_OUTPUT_DIR = ROOT / "pbp_row_override_necessity_20260315_v3"


def _load_overrides(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, dtype=str).fillna("")
    df["norm_game_id"] = df["game_id"].map(_normalize_game_id)
    return df


def _result_row(
    game_id: str,
    game_rows: pd.DataFrame,
    *,
    changed_players: int = 0,
    changed_cells: int = 0,
    changed_pipeline_metrics: list[str] | None = None,
    with_metrics=None,
    without_metrics=None,
) -> dict[str, Any]:
    changed_pipeline_metrics = changed_pipeline_metrics or []
    row = {
        "game_id": game_id,
        "row_override_count": int(len(game_rows)),
        "changed_players": changed_players,
        "changed_cells": changed_cells,
        "changed_pipeline_metrics": "|".join(changed_pipeline_metrics),
        "status": "redundant",
        "with_override_error": getattr(with_metrics, "error", ""),
        "without_override_error": getattr(without_metrics, "error", ""),
        "with_darko_rows": getattr(with_metrics, "darko_rows", 0),
        "without_darko_rows": getattr(without_metrics, "darko_rows", 0),
        "with_event_stats_errors": getattr(with_metrics, "event_stats_errors", 0),
        "without_event_stats_errors": getattr(without_metrics, "event_stats_errors", 0),
        "with_rebound_deletions": getattr(with_metrics, "rebound_deletions", 0),
        "without_rebound_deletions": getattr(without_metrics, "rebound_deletions", 0),
        "with_audit_team_rows": getattr(with_metrics, "audit_team_rows", 0),
        "without_audit_team_rows": getattr(without_metrics, "audit_team_rows", 0),
        "with_audit_player_rows": getattr(with_metrics, "audit_player_rows", 0),
        "without_audit_player_rows": getattr(without_metrics, "audit_player_rows", 0),
        "with_audit_errors": getattr(with_metrics, "audit_errors", 0),
        "without_audit_errors": getattr(without_metrics, "audit_errors", 0),
    }

    if row["with_override_error"]:
        row["status"] = "needs_review_with_override_error"
        return row

    if row["without_override_error"] or changed_cells or changed_pipeline_metrics:
        row["status"] = "active"
        return row

    return row


def main() -> int:
    parser = argparse.ArgumentParser(description="Check which pbp_row_overrides still change full-game output")
    parser.add_argument("--overrides-path", type=Path, default=DEFAULT_OVERRIDES_PATH)
    parser.add_argument("--parquet-path", type=Path, default=DEFAULT_PARQUET)
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB)
    parser.add_argument("--validation-overrides-path", type=Path, default=DEFAULT_VALIDATION_OVERRIDES_PATH)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--tolerance", type=int, default=2)
    args = parser.parse_args()

    output_dir = args.output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    overrides = _load_overrides(args.overrides_path.resolve())
    if overrides.empty:
        payload = {"games": 0, "status_counts": {}}
        (output_dir / "summary.json").write_text(json.dumps(payload, indent=2), encoding="utf-8")
        print(json.dumps(payload, indent=2))
        return 0

    namespace_with, validation_overrides = load_namespace_for_necessity(
        db_path=args.db_path.resolve(),
        validation_overrides_path=args.validation_overrides_path.resolve(),
    )
    namespace_without, _ = load_namespace_for_necessity(
        db_path=args.db_path.resolve(),
        validation_overrides_path=args.validation_overrides_path.resolve(),
    )
    namespace_without["apply_pbp_row_overrides"] = lambda game_df: game_df

    rows: list[dict[str, Any]] = []
    for game_id, game_rows in overrides.groupby("norm_game_id"):
        game_df = load_single_game_df(args.parquet_path.resolve(), game_id)
        with_metrics, box_with = run_game_variant(
            namespace_with,
            game_id,
            game_df,
            validation_overrides=validation_overrides,
            tolerance=args.tolerance,
        )
        without_metrics, box_without = run_game_variant(
            namespace_without,
            game_id,
            game_df,
            validation_overrides=validation_overrides,
            tolerance=args.tolerance,
        )

        changed_players = 0
        changed_cells = 0
        if box_with is not None and box_without is not None:
            changed_players, changed_cells = _compare_boxes(box_with, box_without)

        changed_pipeline_metrics = diff_pipeline_metrics(with_metrics, without_metrics)
        rows.append(
            _result_row(
                game_id,
                game_rows,
                changed_players=changed_players,
                changed_cells=changed_cells,
                changed_pipeline_metrics=changed_pipeline_metrics,
                with_metrics=with_metrics,
                without_metrics=without_metrics,
            )
        )

    result = pd.DataFrame(rows).sort_values(["status", "game_id"]).reset_index(drop=True)
    result.to_csv(output_dir / "pbp_row_override_necessity.csv", index=False)

    summary = {
        "games": int(len(result)),
        "status_counts": result["status"].value_counts(dropna=False).to_dict(),
    }
    (output_dir / "summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
