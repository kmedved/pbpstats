from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path
from typing import Any

import pandas as pd


ROOT = Path(__file__).resolve().parent
DEFAULT_MANIFEST_PATH = ROOT / "intraperiod_canary_manifest_1998_2020.json"
DEFAULT_OUTPUT_DIR = ROOT / "intraperiod_baseline_blocks_1998_2020_20260320_v1"
DEFAULT_BLOCK_SOURCE_DIRS = {
    "A": ROOT / "audit_1998_2000_intraperiod_v4_20260319_v1",
    "B": ROOT / "audit_minutes_fix_2000_2005_v6_runtime_20260318_v1",
    "C": ROOT / "audit_minutes_fix_2006_2010_current_runtime_20260317_v1",
    "D": ROOT / "full_history_1997_2020_20260316_v1",
    "E": ROOT / "full_history_1997_2020_20260316_v1",
}


def _normalize_game_id(value: str | int) -> str:
    return str(int(value)).zfill(10)


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


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


def _combine_season_parquets(source_dir: Path, seasons: list[int], output_path: Path) -> None:
    frames: list[pd.DataFrame] = []
    for season in seasons:
        parquet_path = source_dir / f"darko_{season}.parquet"
        if not parquet_path.exists():
            raise FileNotFoundError(parquet_path)
        frames.append(pd.read_parquet(parquet_path))
    pd.concat(frames, ignore_index=True).to_parquet(output_path, index=False)


def _collect_problem_game_ids(minutes_audit_csv: Path) -> list[str]:
    audit_df = pd.read_csv(minutes_audit_csv)
    if audit_df.empty:
        return []
    mask = audit_df["has_minutes_mismatch"].fillna(False) | audit_df["has_plus_minus_mismatch"].fillna(False)
    game_ids = {_normalize_game_id(game_id) for game_id in audit_df.loc[mask, "game_id"].tolist()}
    return sorted(game_ids)


def _aggregate_failed_and_event_errors(source_dir: Path, seasons: list[int]) -> tuple[int, int]:
    failed_games = 0
    event_stats_errors = 0
    for season in seasons:
        summary_path = source_dir / f"summary_{season}.json"
        if not summary_path.exists():
            continue
        summary = _load_json(summary_path)
        failed_games += int(summary.get("failed_games", 0))
        event_stats_errors += int(summary.get("event_stats_errors", 0))
    return failed_games, event_stats_errors


def _load_source_lineup_audit_summary(source_dir: Path, seasons: list[int]) -> dict[str, Any] | None:
    season_summaries: list[dict[str, Any]] = []
    for season in seasons:
        summary_path = source_dir / f"summary_{season}.json"
        if not summary_path.exists():
            return None
        summary = _load_json(summary_path)
        if summary.get("lineup_audit") is None:
            return None
        season_summaries.append(summary)

    minutes_mismatches = 0
    minutes_outliers = 0
    plus_minus_mismatches = 0
    event_issue_rows = 0
    event_issue_games = 0
    problem_games = 0
    for summary in season_summaries:
        lineup_audit = summary.get("lineup_audit") or {}
        minutes_pm = lineup_audit.get("minutes_plus_minus") or {}
        event_on_court = lineup_audit.get("event_on_court") or {}
        minutes_mismatches += int(minutes_pm.get("minutes_mismatches", 0))
        minutes_outliers += int(minutes_pm.get("minutes_outliers", 0))
        plus_minus_mismatches += int(minutes_pm.get("plus_minus_mismatches", 0))
        event_issue_rows += int(event_on_court.get("issue_rows", 0))
        event_issue_games += int(event_on_court.get("issue_games", 0))
        problem_games += int(lineup_audit.get("problem_games", 0))

    return {
        "minutes_summary": {
            "minutes_mismatches": minutes_mismatches,
            "minutes_outliers": minutes_outliers,
            "plus_minus_mismatches": plus_minus_mismatches,
        },
        "event_summary": {
            "issue_rows": event_issue_rows,
            "issue_games": event_issue_games,
        },
        "problem_games": problem_games,
    }


def build_block_baseline(
    *,
    block: dict[str, Any],
    source_dir: Path,
    output_root: Path,
) -> dict[str, Any]:
    block_id = str(block["block_id"])
    block_label = str(block["label"])
    seasons = [int(season) for season in block["seasons"]]
    block_dir = output_root / "blocks" / f"{block_id}_{block_label.replace(' ', '_')}"
    block_dir.mkdir(parents=True, exist_ok=True)

    darko_block_path = block_dir / "darko_block.parquet"
    _combine_season_parquets(source_dir, seasons, darko_block_path)

    source_lineup_audit = _load_source_lineup_audit_summary(source_dir, seasons)
    if source_lineup_audit is not None:
        minutes_summary = source_lineup_audit["minutes_summary"]
        event_summary = source_lineup_audit["event_summary"]
        problem_games = int(source_lineup_audit["problem_games"])
        problem_game_ids: list[str] = []
        for season in seasons:
            path = source_dir / f"lineup_problem_games_{season}.txt"
            if not path.exists():
                continue
            problem_game_ids.extend([line.strip() for line in path.read_text(encoding="utf-8").splitlines() if line.strip()])
        problem_game_ids = sorted({_normalize_game_id(game_id) for game_id in problem_game_ids})
        (block_dir / "problem_game_ids.txt").write_text(
            "\n".join(problem_game_ids) + ("\n" if problem_game_ids else ""),
            encoding="utf-8",
        )
    else:
        minutes_dir = block_dir / "minutes_pm"
        minutes_dir.mkdir(parents=True, exist_ok=True)
        _run_command(
            [
                sys.executable,
                str(ROOT / "audit_minutes_plus_minus.py"),
                str(darko_block_path),
                "--output-dir",
                str(minutes_dir),
            ],
            log_path=minutes_dir / "run.log",
        )
        minutes_summary = _load_json(minutes_dir / "summary.json")
        problem_game_ids = _collect_problem_game_ids(minutes_dir / "minutes_plus_minus_audit.csv")
        (block_dir / "problem_game_ids.txt").write_text(
            "\n".join(problem_game_ids) + ("\n" if problem_game_ids else ""),
            encoding="utf-8",
        )

        event_dir = block_dir / "event_on_court"
        if problem_game_ids:
            event_dir.mkdir(parents=True, exist_ok=True)
            _run_command(
                [
                    sys.executable,
                    str(ROOT / "audit_event_player_on_court.py"),
                    "--game-ids",
                    *problem_game_ids,
                    "--output-dir",
                    str(event_dir),
                ],
                log_path=event_dir / "run.log",
            )
            event_summary = _load_json(event_dir / "summary.json")
        else:
            event_dir.mkdir(parents=True, exist_ok=True)
            event_summary = {"games": 0, "issue_rows": 0, "issue_games": 0, "status_counts": {}}
            (event_dir / "summary.json").write_text(
                json.dumps(event_summary, indent=2, sort_keys=True),
                encoding="utf-8",
            )

    cross_dir = block_dir / "cross_source"
    cross_dir.mkdir(parents=True, exist_ok=True)
    _run_command(
        [
            sys.executable,
            str(ROOT / "build_minutes_cross_source_report.py"),
            "--darko-parquet",
            str(darko_block_path),
            "--output-dir",
            str(cross_dir),
        ],
        log_path=cross_dir / "run.log",
    )
    cross_source_summary = _load_json(cross_dir / "minutes_cross_source_summary.json")

    failed_games, event_stats_errors = _aggregate_failed_and_event_errors(source_dir, seasons)
    block_summary = {
        "block_id": block_id,
        "label": block_label,
        "seasons": seasons,
        "failed_games": failed_games,
        "event_stats_errors": event_stats_errors,
        "minutes_mismatches": int(minutes_summary.get("minutes_mismatches", 0)),
        "minutes_outliers": int(minutes_summary.get("minutes_outliers", 0)),
        "plus_minus_mismatches": int(minutes_summary.get("plus_minus_mismatches", 0)),
        "event_on_court_issue_rows": int(event_summary.get("issue_rows", 0)),
        "event_on_court_issue_games": int(event_summary.get("issue_games", 0)),
        "problem_games": int(problem_games if source_lineup_audit is not None else len(problem_game_ids)),
        "cross_source_summary": cross_source_summary,
        "candidate_summary": None,
        "baseline_source_dir": str(source_dir),
    }
    (block_dir / "block_summary.json").write_text(
        json.dumps(block_summary, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    return block_summary


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build block baselines for the intraperiod proving ladder.")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--manifest-path", type=Path, default=DEFAULT_MANIFEST_PATH)
    parser.add_argument("--blocks", nargs="*")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    output_dir = args.output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    manifest = _load_json(args.manifest_path.resolve())

    requested_blocks = {str(block_id) for block_id in (args.blocks or [])}
    block_summaries: list[dict[str, Any]] = []
    for block in manifest.get("blocks", []):
        block_id = str(block["block_id"])
        if requested_blocks and block_id not in requested_blocks:
            continue
        source_dir = DEFAULT_BLOCK_SOURCE_DIRS.get(block_id)
        if source_dir is None:
            raise KeyError(f"No default source dir configured for block {block_id}")
        if not source_dir.exists():
            raise FileNotFoundError(source_dir)
        block_summaries.append(
            build_block_baseline(
                block=block,
                source_dir=source_dir.resolve(),
                output_root=output_dir,
            )
        )

    summary = {
        "blocks_built": len(block_summaries),
        "block_summaries": block_summaries,
    }
    (output_dir / "summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
