from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path
from typing import Any

import pandas as pd

from cautious_rerun import AUDIT_PROFILES, DEFAULT_AUDIT_PROFILE, RUNTIME_INPUT_CACHE_MODES


ROOT = Path(__file__).resolve().parent
DEFAULT_MANIFEST_PATH = ROOT / "intraperiod_canary_manifest_1998_2020.json"
DEFAULT_BASELINE_DIR = ROOT / "intraperiod_baseline_blocks_1998_2020_20260320_v1"
DEFAULT_ORCHESTRATOR_RUNTIME_INPUT_CACHE_MODE = "reuse-validated-cache"


def _normalize_game_id(value: str | int) -> str:
    return str(int(value)).zfill(10)


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_game_ids(path: Path, game_ids: list[str]) -> None:
    path.write_text(
        "\n".join(_normalize_game_id(game_id) for game_id in game_ids) + ("\n" if game_ids else ""),
        encoding="utf-8",
    )


def _combine_selected_parquet(run_dir: Path) -> Path | None:
    parquet_path = run_dir / "darko_selected_games.parquet"
    return parquet_path if parquet_path.exists() else None


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


def _combine_block_parquet(block_dir: Path, seasons: list[int]) -> Path | None:
    frames: list[pd.DataFrame] = []
    for season in seasons:
        parquet_path = block_dir / f"darko_{season}.parquet"
        if parquet_path.exists():
            frames.append(pd.read_parquet(parquet_path))
    if not frames:
        return None
    combined = pd.concat(frames, ignore_index=True)
    combined_path = block_dir / "darko_block.parquet"
    combined.to_parquet(combined_path, index=False)
    return combined_path


def _collect_problem_game_ids(block_dir: Path, seasons: list[int]) -> list[str]:
    game_ids: set[str] = set()
    for season in seasons:
        path = block_dir / f"lineup_problem_games_{season}.txt"
        if not path.exists():
            continue
        for line in path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line:
                game_ids.add(_normalize_game_id(line))
    return sorted(game_ids)


def _aggregate_block_summary(block_dir: Path, block: dict[str, Any]) -> dict[str, Any]:
    seasons = [int(season) for season in block["seasons"]]
    season_summaries: list[dict[str, Any]] = []
    for season in seasons:
        path = block_dir / f"summary_{season}.json"
        if path.exists():
            season_summaries.append(_load_json(path))

    minutes_mismatches = 0
    minutes_outliers = 0
    plus_minus_mismatches = 0
    event_issue_rows = 0
    event_issue_games = 0
    problem_games = 0
    failed_games = 0
    event_stats_errors = 0
    for summary in season_summaries:
        failed_games += int(summary.get("failed_games", 0))
        event_stats_errors += int(summary.get("event_stats_errors", 0))
        lineup_audit = summary.get("lineup_audit") or {}
        minutes_pm = lineup_audit.get("minutes_plus_minus") or {}
        event_on_court = lineup_audit.get("event_on_court") or {}
        minutes_mismatches += int(minutes_pm.get("minutes_mismatches", 0))
        minutes_outliers += int(minutes_pm.get("minutes_outliers", 0))
        plus_minus_mismatches += int(minutes_pm.get("plus_minus_mismatches", 0))
        event_issue_rows += int(event_on_court.get("issue_rows", 0))
        event_issue_games += int(event_on_court.get("issue_games", 0))
        problem_games += int(lineup_audit.get("problem_games", 0))

    cross_source_summary_path = block_dir / "cross_source" / "minutes_cross_source_summary.json"
    candidate_summary_path = block_dir / "candidates" / "intraperiod_missing_sub_summary.json"

    return {
        "block_id": str(block["block_id"]),
        "label": str(block["label"]),
        "seasons": seasons,
        "failed_games": failed_games,
        "event_stats_errors": event_stats_errors,
        "minutes_mismatches": minutes_mismatches,
        "minutes_outliers": minutes_outliers,
        "plus_minus_mismatches": plus_minus_mismatches,
        "event_on_court_issue_rows": event_issue_rows,
        "event_on_court_issue_games": event_issue_games,
        "problem_games": problem_games,
        "cross_source_summary": (
            _load_json(cross_source_summary_path) if cross_source_summary_path.exists() else None
        ),
        "candidate_summary": (
            _load_json(candidate_summary_path) if candidate_summary_path.exists() else None
        ),
    }


def _apply_baseline_deltas(block_summary: dict[str, Any], baseline_block_dir: Path | None) -> dict[str, Any]:
    if baseline_block_dir is None:
        return block_summary
    baseline_summary_path = baseline_block_dir / "block_summary.json"
    if not baseline_summary_path.exists():
        return block_summary
    baseline = _load_json(baseline_summary_path)
    delta_keys = [
        "minutes_mismatches",
        "minutes_outliers",
        "plus_minus_mismatches",
        "event_on_court_issue_rows",
        "event_on_court_issue_games",
        "problem_games",
    ]
    block_summary["delta_vs_baseline"] = {
        key: int(block_summary.get(key, 0)) - int(baseline.get(key, 0))
        for key in delta_keys
    }
    return block_summary


def _run_micro_group(
    *,
    label: str,
    ids_path: Path,
    output_dir: Path,
    max_workers: int,
    run_boxscore_audit: bool,
    runtime_input_cache_mode: str,
    audit_profile: str,
) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    rerun_args = [
        sys.executable,
        str(ROOT / "rerun_selected_games.py"),
        "--game-ids-file",
        str(ids_path),
        "--output-dir",
        str(output_dir),
        "--max-workers",
        str(max_workers),
        "--runtime-input-cache-mode",
        str(runtime_input_cache_mode),
        "--audit-profile",
        str(audit_profile),
    ]
    if run_boxscore_audit:
        rerun_args.append("--run-boxscore-audit")
    _run_command(rerun_args, log_path=output_dir / "rerun.log")

    combined_parquet = _combine_selected_parquet(output_dir)
    if combined_parquet is not None:
        cross_dir = output_dir / "cross_source"
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

    candidate_dir = output_dir / "candidates"
    candidate_dir.mkdir(parents=True, exist_ok=True)
    _run_command(
        [
            sys.executable,
            str(ROOT / "suggest_intraperiod_missing_subs.py"),
            "--game-ids-file",
            str(ids_path),
            "--output-dir",
            str(candidate_dir),
            "--emit-override-proposals",
            "--emit-override-note-proposals",
        ],
        log_path=candidate_dir / "run.log",
    )

    run_summary = _load_json(output_dir / "summary.json") if (output_dir / "summary.json").exists() else {}
    cross_summary_path = output_dir / "cross_source" / "minutes_cross_source_summary.json"
    candidate_summary_path = candidate_dir / "intraperiod_missing_sub_summary.json"
    return {
        "label": label,
        "run_summary": run_summary,
        "cross_source_summary": _load_json(cross_summary_path) if cross_summary_path.exists() else None,
        "candidate_summary": _load_json(candidate_summary_path) if candidate_summary_path.exists() else None,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run the intraperiod proving ladder across 1998-2020."
    )
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--manifest-path", type=Path, default=DEFAULT_MANIFEST_PATH)
    parser.add_argument("--baseline-dir", type=Path)
    parser.add_argument("--max-workers", type=int, default=8)
    parser.add_argument("--skip-boxscore-audit", action="store_true", default=False)
    parser.add_argument(
        "--runtime-input-cache-mode",
        choices=sorted(RUNTIME_INPUT_CACHE_MODES),
        default=DEFAULT_ORCHESTRATOR_RUNTIME_INPUT_CACHE_MODE,
    )
    parser.add_argument("--audit-profile", choices=sorted(AUDIT_PROFILES), default=DEFAULT_AUDIT_PROFILE)
    parser.add_argument("--stop-after-block", choices=["A", "B", "C", "D", "E"])
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    output_dir = args.output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    manifest = _load_json(args.manifest_path.resolve())
    baseline_dir = args.baseline_dir.resolve() if args.baseline_dir is not None else None
    if baseline_dir is None and DEFAULT_BASELINE_DIR.exists():
        baseline_dir = DEFAULT_BASELINE_DIR.resolve()

    (output_dir / "manifest.json").write_text(
        json.dumps(manifest, indent=2),
        encoding="utf-8",
    )

    positive_ids = [
        _normalize_game_id(item["game_id"])
        for item in manifest.get("micro_canaries", [])
        if str(item.get("role")) == "positive"
    ]
    negative_ids = [
        _normalize_game_id(item["game_id"])
        for item in manifest.get("micro_canaries", [])
        if str(item.get("role")) != "positive"
    ]
    micro_dir = output_dir / "micro"
    micro_dir.mkdir(parents=True, exist_ok=True)
    positive_ids_path = micro_dir / "positive_game_ids.txt"
    negative_ids_path = micro_dir / "negative_game_ids.txt"
    _write_game_ids(positive_ids_path, positive_ids)
    _write_game_ids(negative_ids_path, negative_ids)

    micro_summaries: list[dict[str, Any]] = []
    if positive_ids:
        positive_dir = micro_dir / "positive"
        micro_summaries.append(
            _run_micro_group(
                label="positive",
                ids_path=positive_ids_path,
                output_dir=positive_dir,
                max_workers=args.max_workers,
                run_boxscore_audit=not args.skip_boxscore_audit,
                runtime_input_cache_mode=args.runtime_input_cache_mode,
                audit_profile=args.audit_profile,
            )
        )
    if negative_ids:
        negative_dir = micro_dir / "negative"
        micro_summaries.append(
            _run_micro_group(
                label="negative",
                ids_path=negative_ids_path,
                output_dir=negative_dir,
                max_workers=args.max_workers,
                run_boxscore_audit=not args.skip_boxscore_audit,
                runtime_input_cache_mode=args.runtime_input_cache_mode,
                audit_profile=args.audit_profile,
            )
        )

    block_summaries: list[dict[str, Any]] = []
    for block in manifest.get("blocks", []):
        block_id = str(block["block_id"])
        block_label = str(block["label"])
        block_dir = output_dir / "blocks" / f"{block_id}_{block_label.replace(' ', '_')}"
        block_dir.mkdir(parents=True, exist_ok=True)
        seasons = [int(season) for season in block["seasons"]]

        rerun_args = [
            sys.executable,
            str(ROOT / "cautious_rerun.py"),
            "--seasons",
            *[str(season) for season in seasons],
            "--output-dir",
            str(block_dir),
            "--max-workers",
            str(args.max_workers),
            "--runtime-input-cache-mode",
            str(args.runtime_input_cache_mode),
            "--audit-profile",
            str(args.audit_profile),
        ]
        if not args.skip_boxscore_audit:
            rerun_args.append("--run-boxscore-audit")
        _run_command(rerun_args, log_path=block_dir / "rerun.log")

        combined_parquet = _combine_block_parquet(block_dir, seasons)
        if combined_parquet is not None:
            cross_dir = block_dir / "cross_source"
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

        problem_game_ids = _collect_problem_game_ids(block_dir, seasons)
        problem_ids_path = block_dir / "problem_game_ids.txt"
        _write_game_ids(problem_ids_path, problem_game_ids)
        if problem_game_ids:
            candidate_dir = block_dir / "candidates"
            candidate_dir.mkdir(parents=True, exist_ok=True)
            _run_command(
                [
                    sys.executable,
                    str(ROOT / "suggest_intraperiod_missing_subs.py"),
                    "--game-ids-file",
                    str(problem_ids_path),
                    "--output-dir",
                    str(candidate_dir),
                    "--emit-override-proposals",
                    "--emit-override-note-proposals",
                ],
                log_path=candidate_dir / "run.log",
            )

        baseline_block_dir = None
        if baseline_dir is not None:
            baseline_block_dir = baseline_dir / "blocks" / f"{block_id}_{block_label.replace(' ', '_')}"
        block_summary = _aggregate_block_summary(block_dir, block)
        block_summary = _apply_baseline_deltas(block_summary, baseline_block_dir)
        (block_dir / "block_summary.json").write_text(
            json.dumps(block_summary, indent=2),
            encoding="utf-8",
        )
        block_summaries.append(block_summary)

        if args.stop_after_block == block_id:
            break

    canary_register_dir = output_dir / "canary_register"
    canary_register_dir.mkdir(parents=True, exist_ok=True)
    _run_command(
        [
            sys.executable,
            str(ROOT / "build_intraperiod_canary_register.py"),
            "--loop-output-dir",
            str(output_dir),
            "--output-dir",
            str(canary_register_dir),
            "--manifest-path",
            str(args.manifest_path.resolve()),
        ],
        log_path=canary_register_dir / "run.log",
    )
    family_register_dir = output_dir / "family_register"
    family_register_dir.mkdir(parents=True, exist_ok=True)
    _run_command(
        [
            sys.executable,
            str(ROOT / "build_intraperiod_family_register.py"),
            "--loop-output-dir",
            str(output_dir),
            "--output-dir",
            str(family_register_dir),
            "--manifest-path",
            str(args.manifest_path.resolve()),
        ],
        log_path=family_register_dir / "run.log",
    )
    event_on_court_family_register_dir = output_dir / "event_on_court_family_register"
    event_on_court_family_register_dir.mkdir(parents=True, exist_ok=True)
    _run_command(
        [
            sys.executable,
            str(ROOT / "build_event_on_court_family_register.py"),
            "--loop-output-dir",
            str(output_dir),
            "--output-dir",
            str(event_on_court_family_register_dir),
            "--family-register-dir",
            str(family_register_dir),
        ],
        log_path=event_on_court_family_register_dir / "run.log",
    )
    same_clock_register_dir = output_dir / "same_clock_attribution"
    same_clock_register_dir.mkdir(parents=True, exist_ok=True)
    _run_command(
        [
            sys.executable,
            str(ROOT / "build_same_clock_attribution_register.py"),
            "--loop-output-dir",
            str(output_dir),
            "--output-dir",
            str(same_clock_register_dir),
            "--family-register-dir",
            str(family_register_dir),
        ],
        log_path=same_clock_register_dir / "run.log",
    )
    same_clock_boundary_queue_dir = output_dir / "same_clock_boundary_queue"
    same_clock_boundary_queue_dir.mkdir(parents=True, exist_ok=True)
    _run_command(
        [
            sys.executable,
            str(ROOT / "build_same_clock_boundary_queue.py"),
            "--event-on-court-family-register-dir",
            str(event_on_court_family_register_dir),
            "--same-clock-register-dir",
            str(same_clock_register_dir),
            "--output-dir",
            str(same_clock_boundary_queue_dir),
        ],
        log_path=same_clock_boundary_queue_dir / "run.log",
    )
    residual_dashboard_dir = output_dir / "residual_dashboard"
    residual_dashboard_dir.mkdir(parents=True, exist_ok=True)
    residual_dashboard_args = [
        sys.executable,
        str(ROOT / "build_intraperiod_residual_dashboard.py"),
        "--loop-output-dir",
        str(output_dir),
        "--output-dir",
        str(residual_dashboard_dir),
        "--family-register-dir",
        str(family_register_dir),
    ]
    if baseline_dir is not None:
        residual_dashboard_args.extend(["--baseline-dir", str(baseline_dir)])
    _run_command(
        residual_dashboard_args,
        log_path=residual_dashboard_dir / "run.log",
    )

    summary = {
        "micro_summaries": micro_summaries,
        "blocks_completed": len(block_summaries),
        "block_summaries": block_summaries,
        "canary_register_summary": _load_json(canary_register_dir / "summary.json"),
        "family_register_summary": _load_json(family_register_dir / "summary.json"),
        "event_on_court_family_register_summary": _load_json(
            event_on_court_family_register_dir / "summary.json"
        ),
        "same_clock_attribution_summary": _load_json(same_clock_register_dir / "summary.json"),
        "same_clock_boundary_queue_summary": _load_json(
            same_clock_boundary_queue_dir / "summary.json"
        ),
        "residual_dashboard_summary": _load_json(residual_dashboard_dir / "summary.json"),
    }
    (output_dir / "summary.json").write_text(
        json.dumps(summary, indent=2),
        encoding="utf-8",
    )
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
