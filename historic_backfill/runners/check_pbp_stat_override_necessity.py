from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import pandas as pd

from historic_backfill.runners.cautious_rerun import DEFAULT_DB, DEFAULT_PARQUET
from historic_backfill.common.override_necessity_utils import (
    DEFAULT_VALIDATION_OVERRIDES_PATH,
    diff_pipeline_metrics,
    load_namespace_for_necessity,
    load_single_game_df,
    run_game_variant,
)
from historic_backfill.audits.cross_source.recheck_overrides_against_bbr_pbp import STAT_KEY_TO_BASIC_STATS, _normalize_game_id


ROOT = Path(__file__).resolve().parent
DEFAULT_OVERRIDES_PATH = ROOT / "pbp_stat_overrides.csv"
DEFAULT_OUTPUT_DIR = ROOT / "pbp_stat_override_necessity_20260315_v2"


def _load_overrides(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, dtype=str).fillna("")
    df["norm_game_id"] = df["game_id"].map(_normalize_game_id)
    return df


def _result_row(
    row: dict[str, Any],
    *,
    box_with: pd.DataFrame | None,
    with_metrics,
    box_without: pd.DataFrame | None,
    without_metrics,
) -> dict[str, Any]:
    impacted_stats = STAT_KEY_TO_BASIC_STATS.get(row["stat_key"], [])
    if not impacted_stats:
        return {
            **row,
            "status": "unsupported_stat_key",
            "changed_stats": "",
            "changed_pipeline_metrics": "",
        }

    player_id = int(float(row["player_id"]))
    team_id = int(float(row["team_id"]))
    changed_stats: list[str] = []
    if box_with is not None and box_without is not None:
        matched_with = box_with[(box_with["player_id"] == player_id) & (box_with["team_id"] == team_id)]
        matched_without = box_without[(box_without["player_id"] == player_id) & (box_without["team_id"] == team_id)]
        for stat in impacted_stats:
            with_value = int(matched_with.iloc[0][stat]) if not matched_with.empty else 0
            without_value = int(matched_without.iloc[0][stat]) if not matched_without.empty else 0
            if with_value != without_value:
                changed_stats.append(f"{stat}:{without_value}->{with_value}")

    changed_pipeline_metrics = diff_pipeline_metrics(with_metrics, without_metrics)

    return {
        **row,
        "status": "active" if changed_stats or changed_pipeline_metrics else "redundant",
        "changed_stats": ",".join(changed_stats),
        "changed_pipeline_metrics": "|".join(changed_pipeline_metrics),
        "with_override_error": getattr(with_metrics, "error", ""),
        "without_override_error": getattr(without_metrics, "error", ""),
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


def main() -> int:
    parser = argparse.ArgumentParser(description="Check which pbp_stat_overrides still change full-game output")
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
        payload = {"rows": 0, "status_counts": {}}
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
    namespace_without["apply_pbp_stat_overrides"] = lambda game_id, rows: list(rows or [])

    cache: dict[str, tuple[pd.DataFrame | None, object, pd.DataFrame | None, object]] = {}
    for game_id in sorted(overrides["norm_game_id"].unique()):
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
        cache[game_id] = (box_with, with_metrics, box_without, without_metrics)

    rows: list[dict] = []
    for row in overrides.to_dict(orient="records"):
        game_id = row["norm_game_id"]
        box_with, with_metrics, box_without, without_metrics = cache[game_id]
        rows.append(
            _result_row(
                row,
                box_with=box_with,
                with_metrics=with_metrics,
                box_without=box_without,
                without_metrics=without_metrics,
            )
        )

    result = pd.DataFrame(rows)
    result.to_csv(output_dir / "pbp_stat_override_necessity.csv", index=False)

    summary = {
        "rows": int(len(result)),
        "status_counts": result["status"].value_counts(dropna=False).to_dict(),
    }
    (output_dir / "summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
