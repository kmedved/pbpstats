from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

from historic_backfill.runners.cautious_rerun import (
    AUDIT_PROFILES,
    DEFAULT_AUDIT_PROFILE,
    DEFAULT_DB,
    DEFAULT_FILE_DIRECTORY,
    DEFAULT_OVERRIDES,
    DEFAULT_PARQUET,
    RUNTIME_INPUT_CACHE_MODES,
)
from historic_backfill.runners.run_intraperiod_manual_review_queue import _game_summary_from_run, _normalize_game_id


ROOT = Path(__file__).resolve().parent
DEFAULT_MANIFEST_PATH = ROOT / "golden_canary_manifest_20260321_v1.json"
DEFAULT_ORCHESTRATOR_RUNTIME_INPUT_CACHE_MODE = "reuse-validated-cache"
VALID_STABILITY_CLASSES = {"stable", "unstable_control"}


def _run_command(args: list[str], *, log_path: Path, env_updates: dict[str, str] | None = None) -> None:
    env = os.environ.copy()
    if env_updates:
        env.update(env_updates)
    result = subprocess.run(args, cwd=ROOT, text=True, capture_output=True, env=env)
    log_path.write_text(
        result.stdout + ("\n" if result.stdout and result.stderr else "") + result.stderr,
        encoding="utf-8",
    )
    if result.returncode != 0:
        raise RuntimeError(f"Command failed: {' '.join(args)}\nSee {log_path}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run the dated golden canary suite over positive canaries, dirty fixes, and anti-canaries."
    )
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--manifest-path", type=Path, default=DEFAULT_MANIFEST_PATH)
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB)
    parser.add_argument("--parquet-path", type=Path, default=DEFAULT_PARQUET)
    parser.add_argument("--overrides-path", type=Path, default=DEFAULT_OVERRIDES)
    parser.add_argument("--file-directory", type=Path, default=DEFAULT_FILE_DIRECTORY)
    parser.add_argument("--max-workers", type=int, default=8)
    parser.add_argument("--severe-minute-threshold", type=float, default=0.5)
    parser.add_argument("--pbpstats-repo", type=Path)
    parser.add_argument(
        "--runtime-input-cache-mode",
        choices=sorted(RUNTIME_INPUT_CACHE_MODES),
        default=DEFAULT_ORCHESTRATOR_RUNTIME_INPUT_CACHE_MODE,
    )
    parser.add_argument("--audit-profile", choices=sorted(AUDIT_PROFILES), default=DEFAULT_AUDIT_PROFILE)
    return parser.parse_args()


def _collect_cases(manifest: dict[str, Any]) -> list[dict[str, Any]]:
    sections = [
        "positive_canaries",
        "fixed_dirty_games",
        "failed_patch_anti_canaries",
        "source_limited_negative_controls",
        "pm_only_boundary_controls",
    ]
    cases: list[dict[str, Any]] = []
    for section in sections:
        for item in manifest.get(section, []):
            case = dict(item)
            case["category"] = section
            stability_class = str(case.get("stability_class") or "stable")
            if stability_class not in VALID_STABILITY_CLASSES:
                raise ValueError(
                    f"Unsupported stability_class {stability_class!r} for "
                    f"{section} case {case.get('game_id', '<unknown>')}"
                )
            case["stability_class"] = stability_class
            cases.append(case)
    return cases


def _case_expectations(case: dict[str, Any], severe_minute_threshold: float) -> dict[str, float | int]:
    defaults = {
        "max_minutes_mismatch_rows": float("inf"),
        "max_minute_outlier_rows": 0 if case["category"] == "positive_canaries" else float("inf"),
        "max_plus_minus_mismatch_rows": 0 if case["category"] == "positive_canaries" else float("inf"),
        "max_game_max_minutes_abs_diff": severe_minute_threshold,
        "max_event_issue_rows": 0 if case["category"] == "positive_canaries" else float("inf"),
        "max_boxscore_audit_failures": 0,
    }
    expectations = dict(defaults)
    for key in list(expectations):
        if key in case and case[key] not in (None, ""):
            expectations[key] = case[key]
    return expectations


def _case_fail_reasons(
    row: dict[str, Any],
    expectations: dict[str, float | int],
) -> list[str]:
    fail_reasons: list[str] = []
    comparisons = {
        "minutes_mismatch_rows": expectations["max_minutes_mismatch_rows"],
        "minute_outlier_rows": expectations["max_minute_outlier_rows"],
        "plus_minus_mismatch_rows": expectations["max_plus_minus_mismatch_rows"],
        "game_max_minutes_abs_diff": expectations["max_game_max_minutes_abs_diff"],
        "event_issue_rows": expectations["max_event_issue_rows"],
        "boxscore_audit_failures": expectations["max_boxscore_audit_failures"],
    }
    for metric, allowed in comparisons.items():
        actual = row[metric]
        if actual > allowed:
            fail_reasons.append(f"{metric} {actual} > {allowed}")
    return fail_reasons


def main() -> int:
    args = parse_args()
    output_dir = args.output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    manifest = json.loads(args.manifest_path.resolve().read_text(encoding="utf-8"))
    cases = _collect_cases(manifest)
    selected_game_ids = sorted({_normalize_game_id(case["game_id"]) for case in cases})
    pbpstats_repo = args.pbpstats_repo
    if pbpstats_repo is None:
        env_repo = os.environ.get("PBPSTATS_REPO")
        if env_repo:
            pbpstats_repo = Path(env_repo)
        else:
            sibling_repo = ROOT.parent / "pbpstats"
            if sibling_repo.exists():
                pbpstats_repo = sibling_repo
    env_updates = {}
    if pbpstats_repo is not None:
        env_updates["PBPSTATS_REPO"] = str(pbpstats_repo.resolve())

    rerun_dir = output_dir / "rerun"
    rerun_dir.mkdir(parents=True, exist_ok=True)
    _run_command(
        [
            sys.executable,
            str(ROOT / "rerun_selected_games.py"),
            "--game-ids",
            *selected_game_ids,
            "--output-dir",
            str(rerun_dir),
            "--db-path",
            str(args.db_path.resolve()),
            "--parquet-path",
            str(args.parquet_path.resolve()),
            "--overrides-path",
            str(args.overrides_path.resolve()),
            "--file-directory",
            str(args.file_directory.resolve()),
            "--max-workers",
            str(args.max_workers),
            "--runtime-input-cache-mode",
            str(args.runtime_input_cache_mode),
            "--audit-profile",
            str(args.audit_profile),
            "--run-boxscore-audit",
        ],
        log_path=rerun_dir / "rerun.log",
        env_updates=env_updates,
    )

    case_rows: list[dict[str, Any]] = []
    unexpected_case_failures = 0
    stable_case_failures = 0
    unstable_control_case_count = 0
    unstable_control_failures = 0
    for case in cases:
        game_id = _normalize_game_id(case["game_id"])
        summary = _game_summary_from_run(rerun_dir, game_id)
        minutes = summary["minutes_plus_minus"]
        event_on_court = summary["event_on_court"]
        boxscore = summary["boxscore_audit"] or {}
        stability_class = str(case.get("stability_class") or "stable")
        row = {
            "category": case["category"],
            "game_id": game_id,
            "period": int(case.get("period") or 0),
            "notes": str(case.get("notes") or ""),
            "stability_class": stability_class,
            "minutes_mismatch_rows": int(minutes["minutes_mismatch_rows"]),
            "minute_outlier_rows": int(minutes["minute_outlier_rows"]),
            "plus_minus_mismatch_rows": int(minutes["plus_minus_mismatch_rows"]),
            "game_max_minutes_abs_diff": float(minutes["game_max_minutes_abs_diff"]),
            "event_issue_rows": int(event_on_court["issue_rows"]),
            "boxscore_audit_failures": int(boxscore.get("audit_failures", 0) or 0),
            "boxscore_player_rows_with_mismatch": int(boxscore.get("player_rows_with_mismatch", 0) or 0),
            "boxscore_team_rows_with_mismatch": int(boxscore.get("team_rows_with_mismatch", 0) or 0),
        }
        expectations = _case_expectations(case, float(args.severe_minute_threshold))
        row.update(
            {
                "expected_max_minutes_mismatch_rows": expectations["max_minutes_mismatch_rows"],
                "expected_max_minute_outlier_rows": expectations["max_minute_outlier_rows"],
                "expected_max_plus_minus_mismatch_rows": expectations["max_plus_minus_mismatch_rows"],
                "expected_max_game_max_minutes_abs_diff": expectations["max_game_max_minutes_abs_diff"],
                "expected_max_event_issue_rows": expectations["max_event_issue_rows"],
                "expected_max_boxscore_audit_failures": expectations["max_boxscore_audit_failures"],
            }
        )
        fail_reasons = _case_fail_reasons(row, expectations)
        row["case_pass"] = len(fail_reasons) == 0
        row["case_fail_reasons"] = fail_reasons
        if stability_class == "unstable_control":
            unstable_control_case_count += 1
        if fail_reasons:
            unexpected_case_failures += 1
            if stability_class == "unstable_control":
                unstable_control_failures += 1
            else:
                stable_case_failures += 1
        case_rows.append(row)

    summary_json = json.loads((rerun_dir / "summary.json").read_text(encoding="utf-8"))
    failed_games = int(summary_json.get("failed_games", 0) or 0)
    event_stats_errors = int(summary_json.get("event_stats_errors", 0) or 0)
    base_runtime_pass = failed_games == 0 and event_stats_errors == 0
    suite_pass_all_cases = base_runtime_pass and unexpected_case_failures == 0
    suite_pass_stable_cases_only = base_runtime_pass and stable_case_failures == 0
    suite_summary = {
        "manifest_path": str(args.manifest_path.resolve()),
        "total_cases": len(case_rows),
        "total_games": len(selected_game_ids),
        "failed_games": failed_games,
        "event_stats_errors": event_stats_errors,
        "runtime_input_cache_mode": summary_json.get("runtime_input_cache_mode", ""),
        "runtime_input_provenance_path": summary_json.get("runtime_input_provenance_path", ""),
        "runtime_file_directory": summary_json.get("runtime_file_directory", ""),
        "unexpected_case_failures": unexpected_case_failures,
        "suite_pass": suite_pass_all_cases,
        "suite_pass_all_cases": suite_pass_all_cases,
        "suite_pass_stable_cases_only": suite_pass_stable_cases_only,
        "unstable_control_case_count": unstable_control_case_count,
        "unstable_control_failures": unstable_control_failures,
        "categories": {
            category: int(sum(1 for row in case_rows if row["category"] == category))
            for category in sorted({row["category"] for row in case_rows})
        },
        "cases": case_rows,
    }
    (output_dir / "summary.json").write_text(json.dumps(suite_summary, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(suite_summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
