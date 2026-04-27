from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path
from typing import Any

from build_intraperiod_residual_dashboard import _load_json as _load_json_file
from cautious_rerun import (
    AUDIT_PROFILES,
    DEFAULT_AUDIT_PROFILE,
    DEFAULT_DB,
    DEFAULT_FILE_DIRECTORY,
    DEFAULT_OVERRIDES,
    DEFAULT_PARQUET,
    RUNTIME_INPUT_CACHE_MODES,
)
from run_intraperiod_manual_review_queue import _game_summary_from_run, _normalize_game_id


ROOT = Path(__file__).resolve().parent
DEFAULT_MANIFEST_PATH = ROOT / "same_clock_canary_manifest_20260320_v1" / "same_clock_canary_manifest.json"
DEFAULT_ORCHESTRATOR_RUNTIME_INPUT_CACHE_MODE = "reuse-validated-cache"


def _run_command(args: list[str], *, log_path: Path) -> None:
    result = subprocess.run(
        args,
        cwd=ROOT,
        text=True,
        capture_output=True,
    )
    log_path.write_text(
        result.stdout + ("\n" if result.stdout and result.stderr else "") + result.stderr,
        encoding="utf-8",
    )
    if result.returncode != 0:
        raise RuntimeError(f"Command failed: {' '.join(args)}\nSee {log_path}")


def _season_from_game_id(game_id: str | int) -> int:
    gid = _normalize_game_id(game_id)
    yy = int(gid[3:5])
    return 1901 + yy if yy >= 50 else 2001 + yy


def _summarize_cases(output_dir: Path, cases: list[dict[str, Any]]) -> dict[str, Any]:
    rows = []
    for case in cases:
        game_id = _normalize_game_id(case["game_id"])
        summary = _game_summary_from_run(output_dir, game_id)
        rows.append(
            {
                "game_id": game_id,
                "period": int(case.get("period") or 0),
                "team_id": int(case.get("team_id") or 0),
                "family": str(case.get("family") or ""),
                "case_role": str(case.get("case_role") or ""),
                "season": _season_from_game_id(game_id),
                "minutes_mismatch_rows": int(summary["minutes_plus_minus"]["minutes_mismatch_rows"]),
                "minute_outlier_rows": int(summary["minutes_plus_minus"]["minute_outlier_rows"]),
                "plus_minus_mismatch_rows": int(summary["minutes_plus_minus"]["plus_minus_mismatch_rows"]),
                "game_max_minutes_abs_diff": float(
                    summary["minutes_plus_minus"]["game_max_minutes_abs_diff"]
                ),
                "event_issue_rows": int(summary["event_on_court"]["issue_rows"]),
                "rows_where_official_and_tpdev_pbp_agree_but_output_minutes_differs": int(
                    summary["cross_source"][
                        "rows_where_official_and_tpdev_pbp_agree_but_output_minutes_differs"
                    ]
                ),
                "boxscore_audit": summary["boxscore_audit"],
            }
        )
    return {
        "cases": rows,
        "total_cases": len(rows),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run a same-clock canary suite from the current canary manifest."
    )
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--manifest-path", type=Path, default=DEFAULT_MANIFEST_PATH)
    parser.add_argument(
        "--family",
        action="append",
        help="Same-clock family to run. May be passed multiple times. Defaults to all positive families.",
    )
    parser.add_argument(
        "--include-reviewed-rejects",
        action="store_true",
        default=False,
        help="Include reviewed manual rejects as supplemental cases.",
    )
    parser.add_argument(
        "--include-negative-micro",
        action="store_true",
        default=False,
        help="Include negative micro-canaries from the manifest.",
    )
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB)
    parser.add_argument("--parquet-path", type=Path, default=DEFAULT_PARQUET)
    parser.add_argument("--overrides-path", type=Path, default=DEFAULT_OVERRIDES)
    parser.add_argument("--file-directory", type=Path, default=DEFAULT_FILE_DIRECTORY)
    parser.add_argument("--max-workers", type=int, default=8)
    parser.add_argument(
        "--runtime-input-cache-mode",
        choices=sorted(RUNTIME_INPUT_CACHE_MODES),
        default=DEFAULT_ORCHESTRATOR_RUNTIME_INPUT_CACHE_MODE,
    )
    parser.add_argument("--audit-profile", choices=sorted(AUDIT_PROFILES), default=DEFAULT_AUDIT_PROFILE)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    output_dir = args.output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    manifest = _load_json_file(args.manifest_path.resolve())
    requested_families = set(args.family or [])
    positive_canaries = manifest.get("positive_canaries") or {}

    cases: list[dict[str, Any]] = []
    for family, family_cases in positive_canaries.items():
        if requested_families and family not in requested_families:
            continue
        for case in family_cases:
            cases.append({**case, "family": family, "case_role": "positive"})

    if args.include_reviewed_rejects:
        for case in manifest.get("reviewed_manual_rejects", []):
            cases.append({**case, "family": "reviewed_manual_reject", "case_role": "reviewed_reject"})

    if args.include_negative_micro:
        for case in manifest.get("negative_micro_canaries", []):
            cases.append({**case, "period": 0, "team_id": 0, "case_role": "negative_micro"})

    selected_cases = []
    selected_game_ids: set[str] = set()
    seen_case_keys: set[tuple[str, int, int, str]] = set()
    for case in cases:
        game_id = _normalize_game_id(case["game_id"])
        period = int(case.get("period") or 0)
        team_id = int(case.get("team_id") or 0)
        family = str(case.get("family") or "")
        case_key = (game_id, period, team_id, family)
        if case_key in seen_case_keys:
            continue
        seen_case_keys.add(case_key)
        selected_cases.append(case)
        selected_game_ids.add(game_id)

    selected_game_ids = sorted(selected_game_ids)
    (output_dir / "selected_cases.json").write_text(
        json.dumps(selected_cases, indent=2, sort_keys=True),
        encoding="utf-8",
    )

    if not selected_game_ids:
        summary = {"cases": [], "total_cases": 0}
        (output_dir / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
        print(json.dumps(summary, indent=2))
        return 0

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
    )

    combined_parquet = rerun_dir / "darko_selected_games.parquet"
    if combined_parquet.exists():
        cross_dir = rerun_dir / "cross_source"
        cross_dir.mkdir(parents=True, exist_ok=True)
        _run_command(
            [
                sys.executable,
                str(ROOT / "build_minutes_cross_source_report.py"),
                "--darko-parquet",
                str(combined_parquet),
                "--output-dir",
                str(cross_dir),
            ],
            log_path=cross_dir / "run.log",
        )

    summary = _summarize_cases(rerun_dir, selected_cases)
    (output_dir / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
