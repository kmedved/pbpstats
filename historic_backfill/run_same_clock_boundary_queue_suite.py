from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path
from typing import Any

import pandas as pd

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


def _load_queue(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path) if path.exists() else pd.DataFrame()
    if df.empty:
        return df
    df = df.copy()
    df["game_id"] = df["game_id"].map(_normalize_game_id)
    df["period"] = pd.to_numeric(df["period"], errors="coerce").fillna(0).astype(int)
    df["team_id"] = pd.to_numeric(df["team_id"], errors="coerce").fillna(0).astype(int)
    df["family_rank"] = pd.to_numeric(df["family_rank"], errors="coerce").fillna(0).astype(int)
    df["issue_rows"] = pd.to_numeric(df["issue_rows"], errors="coerce").fillna(0).astype(int)
    return df


def _summarize_cases(output_dir: Path, cases_df: pd.DataFrame) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    for case in cases_df.to_dict(orient="records"):
        game_id = _normalize_game_id(case["game_id"])
        summary = _game_summary_from_run(output_dir, game_id)
        rows.append(
            {
                "game_id": game_id,
                "season": int(case["season"]),
                "period": int(case["period"]),
                "team_id": int(case["team_id"]),
                "same_clock_family": str(case["same_clock_family"]),
                "family_rank": int(case["family_rank"]),
                "is_manifest_positive": bool(case.get("is_manifest_positive", False)),
                "issue_rows": int(case["issue_rows"]),
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
                "notes": str(case.get("notes") or ""),
            }
        )
    return {
        "cases": rows,
        "total_cases": len(rows),
        "family_counts": (
            pd.DataFrame(rows)["same_clock_family"].value_counts().sort_index().to_dict()
            if rows
            else {}
        ),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run same-clock canary suites from the cleaned same-clock boundary queue."
    )
    parser.add_argument("--queue-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument(
        "--family",
        action="append",
        help="Queue family to run. May be passed multiple times. Defaults to all families in the queue.",
    )
    parser.add_argument("--max-cases-per-family", type=int, default=5)
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

    queue_df = _load_queue(args.queue_dir.resolve() / "same_clock_boundary_queue.csv")
    if queue_df.empty:
        summary = {"cases": [], "total_cases": 0, "family_counts": {}}
        (output_dir / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
        print(json.dumps(summary, indent=2))
        return 0

    requested_families = set(args.family or [])
    selected_df = queue_df.copy()
    if requested_families:
        selected_df = selected_df.loc[selected_df["same_clock_family"].isin(requested_families)].copy()
    selected_df = selected_df.loc[selected_df["family_rank"] <= int(args.max_cases_per_family)].copy()
    selected_df = selected_df.sort_values(
        ["same_clock_family", "family_rank", "season", "game_id", "period", "team_id"]
    ).reset_index(drop=True)

    (output_dir / "selected_cases.csv").write_text(selected_df.to_csv(index=False), encoding="utf-8")
    (output_dir / "selected_cases.json").write_text(
        json.dumps(selected_df.to_dict(orient="records"), indent=2),
        encoding="utf-8",
    )

    selected_game_ids = sorted(set(selected_df["game_id"].tolist()))
    if not selected_game_ids:
        summary = {"cases": [], "total_cases": 0, "family_counts": {}}
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

    summary = _summarize_cases(rerun_dir, selected_df)
    (output_dir / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
