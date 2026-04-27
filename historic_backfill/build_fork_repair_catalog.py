from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent
BUNDLE_ROOT = ROOT.parent
DEFAULT_OUTPUT_DIR = ROOT / "fork_repair_catalog_20260315_v1"


def _resolve_pbpstats_root() -> Path:
    candidates: list[Path] = []
    env_path = os.environ.get("PBPSTATS_REPO")
    if env_path:
        candidates.append(Path(env_path).expanduser())
    candidates.append(BUNDLE_ROOT / "pbpstats")

    for candidate in candidates:
        package_root = candidate / "pbpstats"
        if package_root.exists():
            return package_root.resolve()
        if (candidate / "offline").exists():
            return candidate.resolve()

    raise FileNotFoundError(
        "Could not resolve pbpstats package root. Set PBPSTATS_REPO or place the fork at ../pbpstats."
    )


PBPSTATS_ROOT = _resolve_pbpstats_root()


def _pattern_rows(processor_path: Path) -> list[dict]:
    rows: list[dict] = []
    for lineno, line in enumerate(processor_path.read_text(encoding="utf-8").splitlines(), start=1):
        stripped = line.strip()
        if not stripped.startswith("# --- PATTERN"):
            continue
        label = stripped.replace("# --- ", "").replace(" ---", "")
        raw_id = (
            label.lower()
            .replace("pattern ", "")
            .replace(": ", "_")
            .replace(" ", "_")
            .replace("/", "_")
            .replace("+", "plus")
            .replace("-", "m")
            .replace(".", "_")
            .replace(",", "")
        )
        rule_id = f"processor.{raw_id}"
        rows.append(
            {
                "rule_id": rule_id,
                "file": str(processor_path),
                "line": lineno,
                "category": "ordering_repair",
                "scope": "historical_general",
                "proof_level": "season_canary_plus_regression_tests",
                "default_recommendation": "keep_in_fork_existing_only",
                "description": label,
            }
        )
    return rows


def build_catalog() -> tuple[pd.DataFrame, dict]:
    processor_path = PBPSTATS_ROOT / "offline" / "processor.py"
    rows = _pattern_rows(processor_path)
    rows.extend(
        [
            {
                "rule_id": "processor.silent_ft.reversed_andone_block",
                "file": str(processor_path),
                "line": 145,
                "category": "ordering_repair",
                "scope": "historical_general",
                "proof_level": "season_canary_plus_regression_tests",
                "default_recommendation": "keep_in_fork_existing_only",
                "description": "Reverse malformed and-one or 1-of-1 free-throw blocks so the rebound stays attached to the missed FT",
            },
            {
                "rule_id": "processor.silent_ft.reversed_two_shot_block",
                "file": str(processor_path),
                "line": 209,
                "category": "ordering_repair",
                "scope": "historical_general",
                "proof_level": "season_canary_plus_regression_tests",
                "default_recommendation": "keep_in_fork_existing_only",
                "description": "Reverse malformed two-shot FT blocks with stranded real rebounds before the missed last FT",
            },
        ]
    )
    rows.extend(
        [
            {
                "rule_id": "ordering.patch_start_of_periods_preserve_raw_order",
                "file": str(PBPSTATS_ROOT / "offline" / "ordering.py"),
                "line": 139,
                "category": "ordering_repair",
                "scope": "historical_general",
                "proof_level": "season_canary_plus_regression_tests",
                "default_recommendation": "keep_in_fork",
                "description": "Insert synthetic period starts without globally re-sorting the game",
            },
            {
                "rule_id": "turnover.no_turnover_dead_ball_gate",
                "file": str(PBPSTATS_ROOT / "resources" / "enhanced_pbp" / "turnover.py"),
                "line": 71,
                "category": "feed_semantics_guard",
                "scope": "2017_plus_gate",
                "proof_level": "full_impacted_game_audit",
                "default_recommendation": "keep_in_fork",
                "description": 'Ignore mislabeled dead-ball "No Turnover" rows from 2017-18 onward',
            },
            {
                "rule_id": "free_throw.ambiguous_final_ft_followed_by_rebound",
                "file": str(PBPSTATS_ROOT / "resources" / "enhanced_pbp" / "stats_nba" / "free_throw.py"),
                "line": 30,
                "category": "feed_semantics_guard",
                "scope": "modern_general",
                "proof_level": "season_canary_plus_regression_tests",
                "default_recommendation": "keep_in_fork",
                "description": "Treat ambiguous final free throws followed immediately by a rebound as missed",
            },
            {
                "rule_id": "foul.invalid_committing_actor_guard",
                "file": str(PBPSTATS_ROOT / "resources" / "enhanced_pbp" / "stats_nba" / "foul.py"),
                "line": 19,
                "category": "malformed_event_guard",
                "scope": "legacy_general",
                "proof_level": "season_canary_plus_regression_tests",
                "default_recommendation": "keep_in_fork",
                "description": "Return safe base stats for foul rows with no valid committing team or player",
            },
            {
                "rule_id": "substitution.blank_placeholder_noop",
                "file": str(PBPSTATS_ROOT / "resources" / "enhanced_pbp" / "stats_nba" / "substitution.py"),
                "line": 26,
                "category": "malformed_event_guard",
                "scope": "legacy_general",
                "proof_level": "season_canary_plus_regression_tests",
                "default_recommendation": "keep_in_fork",
                "description": "Treat blank placeholder substitutions as no-op swaps",
            },
            {
                "rule_id": "enhanced_pbp_item.incomplete_lineup_empty_base_stats",
                "file": str(PBPSTATS_ROOT / "resources" / "enhanced_pbp" / "enhanced_pbp_item.py"),
                "line": 0,
                "category": "malformed_event_guard",
                "scope": "legacy_general",
                "proof_level": "season_canary_plus_regression_tests",
                "default_recommendation": "keep_in_fork",
                "description": "Return empty base stats when lineup context is incomplete instead of indexing missing teams",
            },
        ]
    )

    catalog = pd.DataFrame(rows).sort_values(["file", "line", "rule_id"]).reset_index(drop=True)
    summary = {
        "rows": int(len(catalog)),
        "counts_by_category": catalog["category"].value_counts(dropna=False).to_dict(),
        "counts_by_scope": catalog["scope"].value_counts(dropna=False).to_dict(),
        "counts_by_proof_level": catalog["proof_level"].value_counts(dropna=False).to_dict(),
    }
    return catalog, summary


def main() -> int:
    parser = argparse.ArgumentParser(description="Build a catalog of custom historical repair logic in the pbpstats fork")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    args = parser.parse_args()

    catalog, summary = build_catalog()
    output_dir = args.output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    catalog.to_csv(output_dir / "fork_repair_catalog.csv", index=False)
    (output_dir / "summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
