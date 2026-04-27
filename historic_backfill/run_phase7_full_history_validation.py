from __future__ import annotations

import argparse
import hashlib
import json
import os
import resource
import shutil
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow.parquet as pq


ROOT = Path(__file__).resolve().parent
PHASE7_SEASONS = list(range(1997, 2021))

DEFAULT_OUTPUT_DIR = ROOT / "phase7_full_history_validation_20260323_v1"
DEFAULT_BASELINE_DIR = (
    ROOT.parent
    / "artifacts"
    / "baselines"
    / "full_history_1997_2020_20260322_v1"
    / "full_history_1997_2020_20260322_v1"
)
DEFAULT_CANDIDATE_RUN_DIR = ROOT / "full_history_1997_2020_20260322_v1"
DEFAULT_RAW_RESIDUAL_DIR = ROOT / "phase7_raw_residuals_1997_2020_20260323_v1"
DEFAULT_FRONTIER_DIFF_DIR = ROOT / "phase7_frontier_diff_20260323_v1"
DEFAULT_REVIEWED_RESIDUAL_DIR = ROOT / "H_1997-2020_20260323_v1"
DEFAULT_REVIEWED_FRONTIER_DIR = ROOT / "phase7_reviewed_frontier_inventory_20260323_v1"
DEFAULT_REVIEWED_PM_DIR = ROOT / "phase7_reviewed_pm_reference_report_1997_2020_20260323_v1"
DEFAULT_REVIEWED_SIDECAR_DIR = ROOT / "reviewed_release_quality_sidecar_20260323_v1"
DEFAULT_SIDECAR_SMOKE_DIR = ROOT / "reviewed_release_quality_sidecar_join_smoke_20260323_v1"
DEFAULT_STAGED_BASELINE_DIR = (
    ROOT.parent
    / "artifacts"
    / "baselines"
    / "full_history_1997_2020_20260322_v1"
    / "full_history_1997_2020_20260322_v1"
)
DEFAULT_COMPILE_SUMMARY_JSON = ROOT / "overrides" / "correction_manifest_compile_summary.json"
DEFAULT_GOLDEN_CANARY_SUMMARY_JSON = ROOT / "golden_canary_suite_20260322_v4" / "summary.json"
DEFAULT_REVIEWED_POLICY_OVERLAY_CSV = ROOT / "reviewed_frontier_policy_overlay_20260322_v1.csv"
DEFAULT_FRONTIER_INVENTORY_CSV = ROOT / "phase6_open_blocker_inventory_20260322_v1.csv"
DEFAULT_SHORTLIST_CSV = ROOT / "phase6_true_blocker_shortlist_20260322_v1.csv"
DEFAULT_PBPSTATS_REPO = ROOT.parent / "pbpstats"
DEFAULT_RUNTIME_INPUT_CACHE_MODE = "reuse-validated-cache"
EXPECTED_CANDIDATE_TOTAL_ROWS = 685969
EXPECTED_BASELINE_TOTAL_ROWS = 685882
EXPECTED_OUTSIDE_OVERLAY_GAME_ID = "0029800606"

FRONTIER_METRIC_COLUMNS = [
    "has_event_on_court_issue",
    "has_material_minute_issue",
    "has_severe_minute_issue",
    "n_actionable_event_rows",
    "max_abs_minute_diff",
    "n_pm_reference_delta_rows",
]
BOOLEAN_FRONTIER_COLUMNS = {
    "has_event_on_court_issue",
    "has_material_minute_issue",
    "has_severe_minute_issue",
}
CONSOLIDATED_KEY_COLUMNS = ["Game_SingleGame", "Team_SingleGame", "NbaDotComID"]
COUNTING_STAT_COLUMNS = ["PTS", "AST", "STL", "BLK", "TOV", "PF", "FGM", "FGA", "3PM", "3PA", "FTM", "FTA", "OREB", "DRB", "REB"]
REPORT_ONLY_SEASON_COMPARE_PREFIXES = (
    "rebound_fallback_deletions regressed",
    "games_with_team_mismatch regressed",
    "player_rows_with_mismatch regressed",
)


def _current_peak_rss_mb() -> float | None:
    try:
        raw_value = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    except Exception:
        return None
    bytes_value = raw_value if sys.platform == "darwin" else raw_value * 1024
    return round(bytes_value / (1024 * 1024), 3)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run the Phase 7 full-history validation gate: preflight, full-history rerun, "
            "baseline comparison, raw-open frontier diff, and conditional reviewed-artifact rebuild."
        )
    )
    parser.add_argument("--resume-from", choices=["start", "post_rerun"], default="start")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--baseline-dir", type=Path, default=DEFAULT_BASELINE_DIR)
    parser.add_argument("--candidate-run-dir", type=Path, default=DEFAULT_CANDIDATE_RUN_DIR)
    parser.add_argument("--raw-residual-dir", type=Path, default=DEFAULT_RAW_RESIDUAL_DIR)
    parser.add_argument("--frontier-diff-dir", type=Path, default=DEFAULT_FRONTIER_DIFF_DIR)
    parser.add_argument("--reviewed-residual-dir", type=Path, default=DEFAULT_REVIEWED_RESIDUAL_DIR)
    parser.add_argument("--reviewed-frontier-dir", type=Path, default=DEFAULT_REVIEWED_FRONTIER_DIR)
    parser.add_argument("--reviewed-pm-dir", type=Path, default=DEFAULT_REVIEWED_PM_DIR)
    parser.add_argument("--reviewed-sidecar-dir", type=Path, default=DEFAULT_REVIEWED_SIDECAR_DIR)
    parser.add_argument("--sidecar-smoke-dir", type=Path, default=DEFAULT_SIDECAR_SMOKE_DIR)
    parser.add_argument("--staged-baseline-dir", type=Path, default=DEFAULT_STAGED_BASELINE_DIR)
    parser.add_argument("--compile-summary-json", type=Path, default=DEFAULT_COMPILE_SUMMARY_JSON)
    parser.add_argument("--golden-canary-summary-json", type=Path, default=DEFAULT_GOLDEN_CANARY_SUMMARY_JSON)
    parser.add_argument("--reviewed-policy-overlay-csv", type=Path, default=DEFAULT_REVIEWED_POLICY_OVERLAY_CSV)
    parser.add_argument("--frontier-inventory-csv", type=Path, default=DEFAULT_FRONTIER_INVENTORY_CSV)
    parser.add_argument("--shortlist-csv", type=Path, default=DEFAULT_SHORTLIST_CSV)
    parser.add_argument("--max-workers", type=int, default=8)
    parser.add_argument("--fallback-max-workers", type=int, default=0)
    parser.add_argument("--runtime-input-cache-mode", default=DEFAULT_RUNTIME_INPUT_CACHE_MODE)
    parser.add_argument("--audit-profile", choices=["full", "counting_only"], default="full")
    parser.add_argument("--expected-candidate-total-rows", type=int, default=EXPECTED_CANDIDATE_TOTAL_ROWS)
    parser.add_argument("--expected-baseline-total-rows", type=int, default=EXPECTED_BASELINE_TOTAL_ROWS)
    parser.add_argument("--expected-outside-overlay-game-id", default=EXPECTED_OUTSIDE_OVERLAY_GAME_ID)
    parser.add_argument("--skip-baseline-stage", action="store_true", default=False)
    return parser.parse_args(argv)


def _normalize_game_id(value: object) -> str:
    return str(int(value)).zfill(10)


def _season_from_game_id(game_id: str) -> int:
    normalized = _normalize_game_id(game_id)
    suffix = int(normalized[3:5])
    return (1900 + suffix + 1) if suffix >= 96 else (2000 + suffix + 1)


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def _ensure_output_target_unused(path: Path) -> None:
    if not path.exists():
        return
    if path.is_dir():
        try:
            next(path.iterdir())
        except StopIteration:
            return
    raise FileExistsError(f"Output target already exists and is not empty: {path}")


def _resolve_pbpstats_repo() -> Path:
    candidates: list[Path] = []
    env_path = Path((os.environ.get("PBPSTATS_REPO") or "")).expanduser()
    if str(env_path) not in {"", "."}:
        candidates.append(env_path)
    candidates.append(DEFAULT_PBPSTATS_REPO)

    for candidate in candidates:
        if (candidate / "pbpstats").exists():
            return candidate.resolve()

    raise FileNotFoundError(
        "Could not locate the editable pbpstats repo. "
        "Set PBPSTATS_REPO or restore the sibling ../pbpstats checkout."
    )


def _run_command(
    args: list[str],
    *,
    log_path: Path,
    allow_exit_codes: set[int] | None = None,
) -> subprocess.CompletedProcess[str]:
    allow_exit_codes = allow_exit_codes or {0}
    _ensure_parent(log_path)
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
    if result.returncode not in allow_exit_codes:
        raise RuntimeError(f"Command failed: {' '.join(args)}\nSee {log_path}")
    return result


def _looks_like_worker_pressure_failure(log_text: str) -> bool:
    lowered = log_text.lower()
    needles = [
        "brokenprocesspool",
        "terminated abruptly",
        "resource temporarily unavailable",
        "too many open files",
        "cannot allocate memory",
        "killed",
        "worker exited unexpectedly",
    ]
    return any(needle in lowered for needle in needles)


def _check_compile_summary(path: Path) -> dict[str, Any]:
    summary = _load_json(path)
    observed: dict[str, Any] = {}
    for key in [
        "active_corrections",
        "active_period_start_corrections",
        "active_window_corrections",
    ]:
        value = summary.get(key)
        if value is None:
            raise ValueError(f"Compile summary missing required key: {key}")
        observed[key] = int(value or 0)
        if observed[key] < 0:
            raise ValueError(f"Compile summary has negative count for {key}: {observed[key]}")
    return observed


def _parse_boolish(value: object) -> bool:
    text = str(value).strip().lower()
    if text in {"true", "1", "yes", "y"}:
        return True
    if text in {"false", "0", "no", "n", ""}:
        return False
    raise ValueError(f"Unsupported boolean value: {value!r}")


def _load_reviewed_frontier_expectations(
    *,
    overlay_csv: Path,
    inventory_csv: Path,
) -> dict[str, Any]:
    overlay_df = pd.read_csv(overlay_csv, dtype={"game_id": str}).fillna("")
    inventory_df = pd.read_csv(inventory_csv, dtype={"game_id": str}).fillna("")
    if not overlay_df.empty:
        overlay_df["game_id"] = overlay_df["game_id"].map(_normalize_game_id)
    if not inventory_df.empty:
        inventory_df["game_id"] = inventory_df["game_id"].map(_normalize_game_id)

    overlay_ids = set(overlay_df["game_id"]) if not overlay_df.empty else set()
    inventory_ids = set(inventory_df["game_id"]) if not inventory_df.empty else set()
    if overlay_ids != inventory_ids:
        raise ValueError(
            "Reviewed overlay and frontier inventory game sets differ: "
            f"overlay_only={sorted(overlay_ids - inventory_ids)}, "
            f"inventory_only={sorted(inventory_ids - overlay_ids)}"
        )

    overlay_versions = sorted(
        value for value in overlay_df.get("policy_decision_id", pd.Series(dtype=str)).astype(str).unique().tolist() if value
    )
    if len(overlay_versions) > 1:
        raise ValueError(f"Reviewed overlay contains multiple policy_decision_id values: {overlay_versions}")

    release_blocking_game_ids = sorted(
        overlay_df.loc[overlay_df["blocks_release"].map(_parse_boolish), "game_id"].astype(str).tolist()
    ) if not overlay_df.empty else []
    research_open_game_ids = sorted(
        overlay_df.loc[overlay_df["research_open"].map(_parse_boolish), "game_id"].astype(str).tolist()
    ) if not overlay_df.empty else []

    return {
        "overlay_row_count": int(len(overlay_df)),
        "reviewed_policy_overlay_version": overlay_versions[0] if overlay_versions else "",
        "frontier_inventory_snapshot_id": inventory_csv.resolve().stem,
        "release_blocking_game_ids": release_blocking_game_ids,
        "research_open_game_ids": research_open_game_ids,
        "tier2_frontier_closed": not research_open_game_ids,
    }


def _check_canary_summary(path: Path) -> dict[str, Any]:
    summary = _load_json(path)
    required_true = [
        "suite_pass",
        "suite_pass_all_cases",
        "suite_pass_stable_cases_only",
    ]
    for key in required_true:
        if summary.get(key) is not True:
            raise ValueError(f"Golden Canary summary failed required gate {key}=true: {summary.get(key)!r}")
    return {
        "summary_path": str(path.resolve()),
        "suite_pass": bool(summary["suite_pass"]),
        "suite_pass_all_cases": bool(summary["suite_pass_all_cases"]),
        "suite_pass_stable_cases_only": bool(summary["suite_pass_stable_cases_only"]),
    }


def _validate_baseline_dir(path: Path) -> dict[str, Any]:
    required = [
        path / "darko_1997_2020.parquet",
        path / "summary_1997.json",
        path / "summary_2020.json",
    ]
    missing = [str(item) for item in required if not item.exists()]
    if missing:
        raise FileNotFoundError(f"Baseline dir is missing required files: {missing}")
    return {"baseline_dir": str(path.resolve()), "required_files_present": True}


def _season_parquet_path(candidate_run_dir: Path, season: int) -> Path:
    return candidate_run_dir / f"darko_{season}.parquet"


def _season_summary_path(candidate_run_dir: Path, season: int) -> Path:
    return candidate_run_dir / f"summary_{season}.json"


def _read_parquet_metadata(path: Path) -> tuple[int, list[str]]:
    parquet_file = pq.ParquetFile(path)
    return int(parquet_file.metadata.num_rows), parquet_file.schema_arrow.names


def _load_consolidated_stat_frame(path: Path) -> pd.DataFrame:
    _, columns = _read_parquet_metadata(path)
    available_columns = set(columns)
    read_columns = [column for column in CONSOLIDATED_KEY_COLUMNS if column in available_columns]
    read_columns.extend(
        column
        for column in COUNTING_STAT_COLUMNS
        if column != "REB" and column in available_columns and column not in read_columns
    )
    if "REB" in available_columns:
        read_columns.append("REB")

    frame = pd.read_parquet(path, columns=read_columns)
    if "REB" not in frame.columns:
        if not {"OREB", "DRB"}.issubset(frame.columns):
            raise ValueError(
                f"Parquet is missing REB and cannot derive it from OREB + DRB: {path}"
            )
        frame["REB"] = (
            pd.to_numeric(frame["OREB"], errors="coerce").fillna(0)
            + pd.to_numeric(frame["DRB"], errors="coerce").fillna(0)
        )
    return frame[CONSOLIDATED_KEY_COLUMNS + COUNTING_STAT_COLUMNS].copy()


def _validate_candidate_run_completion(candidate_run_dir: Path) -> dict[str, Any]:
    if not candidate_run_dir.exists():
        raise FileNotFoundError(f"Candidate run dir does not exist: {candidate_run_dir}")

    per_season: list[dict[str, Any]] = []
    missing: list[str] = []
    total_rows = 0
    for season in PHASE7_SEASONS:
        parquet_path = _season_parquet_path(candidate_run_dir, season)
        summary_path = _season_summary_path(candidate_run_dir, season)
        if not parquet_path.exists():
            missing.append(str(parquet_path))
            continue
        if not summary_path.exists():
            missing.append(str(summary_path))
            continue

        summary_payload = _load_json(summary_path)
        parquet_rows, parquet_columns = _read_parquet_metadata(parquet_path)
        total_rows += parquet_rows
        failed_games = int(summary_payload.get("failed_games", 0) or 0)
        event_stats_errors = int(summary_payload.get("event_stats_errors", 0) or 0)
        summary_player_rows = int(summary_payload.get("player_rows", parquet_rows) or 0)
        per_season.append(
            {
                "season": season,
                "summary_path": str(summary_path.resolve()),
                "parquet_path": str(parquet_path.resolve()),
                "summary_player_rows": summary_player_rows,
                "parquet_rows": parquet_rows,
                "failed_games": failed_games,
                "event_stats_errors": event_stats_errors,
                "column_count": len(parquet_columns),
            }
        )

    if missing:
        raise FileNotFoundError(f"Candidate run is incomplete; missing files: {missing}")

    if any(item["failed_games"] != 0 for item in per_season):
        raise ValueError(
            "Candidate run contains season-level failed games: "
            f"{[(item['season'], item['failed_games']) for item in per_season if item['failed_games'] != 0]}"
        )
    if any(item["event_stats_errors"] != 0 for item in per_season):
        raise ValueError(
            "Candidate run contains season-level event_stats_errors: "
            f"{[(item['season'], item['event_stats_errors']) for item in per_season if item['event_stats_errors'] != 0]}"
        )

    return {
        "candidate_run_dir": str(candidate_run_dir.resolve()),
        "season_count": len(per_season),
        "total_rows_from_season_parquets": total_rows,
        "per_season": per_season,
    }


def _schema_hash(columns: list[str]) -> str:
    return hashlib.sha256("\n".join(columns).encode("utf-8")).hexdigest()


def stitch_consolidated_candidate_parquet(
    *,
    candidate_run_dir: Path,
    expected_total_rows: int,
) -> dict[str, Any]:
    season_inputs: list[dict[str, Any]] = []
    ordered_columns: list[str] | None = None
    total_rows = 0
    for season in PHASE7_SEASONS:
        parquet_path = _season_parquet_path(candidate_run_dir, season)
        rows, columns = _read_parquet_metadata(parquet_path)
        if ordered_columns is None:
            ordered_columns = list(columns)
        elif list(columns) != ordered_columns:
            raise ValueError(f"Season parquet column mismatch for {parquet_path}")
        season_inputs.append(
            {
                "season": season,
                "parquet_path": str(parquet_path.resolve()),
                "row_count": rows,
            }
        )
        total_rows += rows

    if len(season_inputs) != len(PHASE7_SEASONS):
        raise ValueError(f"Expected {len(PHASE7_SEASONS)} season parquets, found {len(season_inputs)}")
    if total_rows != int(expected_total_rows):
        raise ValueError(f"Candidate season parquet row total drifted: expected {expected_total_rows}, found {total_rows}")

    if ordered_columns is None:
        raise ValueError("Could not determine candidate parquet schema")

    consolidated_path = candidate_run_dir / "darko_1997_2020.parquet"
    already_present = consolidated_path.exists()
    if already_present:
        consolidated_rows, consolidated_columns = _read_parquet_metadata(consolidated_path)
        if consolidated_rows != total_rows or list(consolidated_columns) != ordered_columns:
            raise ValueError(
                "Existing consolidated parquet does not match the season parquet inputs: "
                f"rows={consolidated_rows} vs {total_rows}"
            )
    else:
        writer: pq.ParquetWriter | None = None
        try:
            for item in season_inputs:
                table = pq.read_table(item["parquet_path"])
                if writer is None:
                    writer = pq.ParquetWriter(consolidated_path, table.schema)
                writer.write_table(table)
        finally:
            if writer is not None:
                writer.close()

    summary = {
        "candidate_run_dir": str(candidate_run_dir.resolve()),
        "consolidated_parquet": str(consolidated_path.resolve()),
        "already_present": already_present,
        "season_input_count": len(season_inputs),
        "per_season_rows": season_inputs,
        "expected_total_rows": int(expected_total_rows),
        "total_rows": total_rows,
        "column_count": len(ordered_columns),
        "schema_hash": _schema_hash(ordered_columns),
        "ordered_columns": ordered_columns,
    }
    _write_json(candidate_run_dir / "consolidated_parquet_summary.json", summary)
    return summary


def compare_consolidated_parquets(
    *,
    baseline_parquet: Path,
    candidate_parquet: Path,
    output_dir: Path,
    expected_baseline_total_rows: int,
) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)

    baseline_rows, baseline_columns = _read_parquet_metadata(baseline_parquet)
    candidate_rows, candidate_columns = _read_parquet_metadata(candidate_parquet)
    if baseline_rows != int(expected_baseline_total_rows):
        raise ValueError(
            f"Baseline consolidated parquet row count drifted: expected {expected_baseline_total_rows}, found {baseline_rows}"
        )

    missing_columns = [column for column in baseline_columns if column not in candidate_columns]
    extra_columns = [column for column in candidate_columns if column not in baseline_columns]
    ordered_column_set_match = list(baseline_columns) == list(candidate_columns)

    baseline_keys = pd.read_parquet(baseline_parquet, columns=CONSOLIDATED_KEY_COLUMNS)
    candidate_keys = pd.read_parquet(candidate_parquet, columns=CONSOLIDATED_KEY_COLUMNS)
    baseline_duplicate_key_count = int(baseline_keys.duplicated(subset=CONSOLIDATED_KEY_COLUMNS).sum())
    candidate_duplicate_key_count = int(candidate_keys.duplicated(subset=CONSOLIDATED_KEY_COLUMNS).sum())

    baseline_unique = baseline_keys.drop_duplicates(subset=CONSOLIDATED_KEY_COLUMNS)
    candidate_unique = candidate_keys.drop_duplicates(subset=CONSOLIDATED_KEY_COLUMNS)
    membership = baseline_unique.merge(
        candidate_unique,
        on=CONSOLIDATED_KEY_COLUMNS,
        how="outer",
        indicator=True,
    )
    added_keys = membership.loc[membership["_merge"] == "right_only", CONSOLIDATED_KEY_COLUMNS].copy()
    removed_keys = membership.loc[membership["_merge"] == "left_only", CONSOLIDATED_KEY_COLUMNS].copy()
    shared_key_count = int((membership["_merge"] == "both").sum())
    added_keys.head(20).to_csv(output_dir / "consolidated_compare_added_keys_sample.csv", index=False)
    removed_keys.head(20).to_csv(output_dir / "consolidated_compare_removed_keys_sample.csv", index=False)

    baseline_stats = _load_consolidated_stat_frame(baseline_parquet)
    candidate_stats = _load_consolidated_stat_frame(candidate_parquet)
    merged = baseline_stats.merge(
        candidate_stats,
        on=CONSOLIDATED_KEY_COLUMNS,
        how="inner",
        suffixes=("_baseline", "_candidate"),
    )
    stat_diffs: dict[str, dict[str, Any]] = {}
    any_stat_diff_mask = pd.Series(False, index=merged.index)
    for column in COUNTING_STAT_COLUMNS:
        baseline_values = pd.to_numeric(merged[f"{column}_baseline"], errors="coerce").fillna(0)
        candidate_values = pd.to_numeric(merged[f"{column}_candidate"], errors="coerce").fillna(0)
        deltas = candidate_values - baseline_values
        diff_mask = deltas != 0
        any_stat_diff_mask = any_stat_diff_mask | diff_mask
        stat_diffs[column] = {
            "diff_count": int(diff_mask.sum()),
            "max_abs_diff": float(deltas.abs().max()) if not deltas.empty else 0.0,
        }
    stat_diff_examples = merged.loc[any_stat_diff_mask].head(20).copy()
    stat_diff_examples.to_csv(output_dir / "consolidated_compare_stat_diff_examples.csv", index=False)

    gate_failures: list[str] = []
    if missing_columns:
        gate_failures.append(f"candidate_missing_columns:{missing_columns}")
    if baseline_duplicate_key_count:
        gate_failures.append(f"baseline_duplicate_keys:{baseline_duplicate_key_count}")
    if candidate_duplicate_key_count:
        gate_failures.append(f"candidate_duplicate_keys:{candidate_duplicate_key_count}")
    if any(item["diff_count"] > 0 for item in stat_diffs.values()):
        gate_failures.append("counting_stat_differences")

    summary = {
        "baseline_parquet": str(baseline_parquet.resolve()),
        "candidate_parquet": str(candidate_parquet.resolve()),
        "baseline_row_count": int(baseline_rows),
        "candidate_row_count": int(candidate_rows),
        "row_count_delta": int(candidate_rows - baseline_rows),
        "baseline_column_count": len(baseline_columns),
        "candidate_column_count": len(candidate_columns),
        "ordered_column_set_match": bool(ordered_column_set_match),
        "missing_columns": missing_columns,
        "extra_columns": extra_columns,
        "baseline_duplicate_key_count": baseline_duplicate_key_count,
        "candidate_duplicate_key_count": candidate_duplicate_key_count,
        "added_key_count": int(len(added_keys)),
        "removed_key_count": int(len(removed_keys)),
        "shared_key_count": shared_key_count,
        "counting_stat_diff_counts": stat_diffs,
        "counting_stat_diff_row_count": int(any_stat_diff_mask.sum()),
        "added_key_sample_csv": str((output_dir / "consolidated_compare_added_keys_sample.csv").resolve()),
        "removed_key_sample_csv": str((output_dir / "consolidated_compare_removed_keys_sample.csv").resolve()),
        "stat_diff_examples_csv": str((output_dir / "consolidated_compare_stat_diff_examples.csv").resolve()),
        "gate_failures": gate_failures,
        "passed": not gate_failures,
    }
    _write_json(output_dir / "consolidated_parquet_compare.json", summary)
    return summary


def _season_args() -> list[str]:
    return [str(season) for season in PHASE7_SEASONS]


def _collect_compare_regressions(compare_payload: dict[str, Any]) -> list[dict[str, Any]]:
    regressions: list[dict[str, Any]] = []
    for season_payload in compare_payload.get("seasons", []):
        season_regressions = season_payload.get("regressions") or []
        if season_regressions:
            regressions.append(
                {
                    "season": int(season_payload["season"]),
                    "regressions": [str(item) for item in season_regressions],
                }
            )
    return regressions


def _split_compare_regressions(
    regressions: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    blocking: list[dict[str, Any]] = []
    report_only: list[dict[str, Any]] = []
    for season_payload in regressions:
        blocking_messages: list[str] = []
        report_only_messages: list[str] = []
        for message in season_payload.get("regressions", []):
            text = str(message)
            if text.startswith(REPORT_ONLY_SEASON_COMPARE_PREFIXES):
                report_only_messages.append(text)
            else:
                blocking_messages.append(text)
        if blocking_messages:
            blocking.append(
                {
                    "season": int(season_payload["season"]),
                    "regressions": blocking_messages,
                }
            )
        if report_only_messages:
            report_only.append(
                {
                    "season": int(season_payload["season"]),
                    "regressions": report_only_messages,
                }
            )
    return blocking, report_only


def _bool_series(series: pd.Series) -> pd.Series:
    return series.astype(str).str.lower().isin({"true", "1", "yes", "y"})


def build_raw_frontier_diff(
    *,
    current_inventory_csv: Path,
    fresh_game_quality_csv: Path,
    output_dir: Path,
) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)

    current_df = pd.read_csv(current_inventory_csv, dtype={"game_id": str})
    current_df["game_id"] = current_df["game_id"].map(_normalize_game_id)
    current_metrics_df = current_df[["game_id", "block_key", "season", *FRONTIER_METRIC_COLUMNS]].copy()
    for column in BOOLEAN_FRONTIER_COLUMNS:
        current_metrics_df[column] = _bool_series(current_metrics_df[column])

    fresh_df = pd.read_csv(fresh_game_quality_csv, dtype={"game_id": str})
    fresh_df["game_id"] = fresh_df["game_id"].map(_normalize_game_id)
    fresh_open_df = fresh_df.loc[fresh_df["primary_quality_status"] == "open", ["game_id", *FRONTIER_METRIC_COLUMNS]].copy()
    for column in BOOLEAN_FRONTIER_COLUMNS:
        fresh_open_df[column] = _bool_series(fresh_open_df[column])
    fresh_open_df["season"] = fresh_open_df["game_id"].map(_season_from_game_id)

    current_game_ids = set(current_metrics_df["game_id"])
    fresh_game_ids = set(fresh_open_df["game_id"])

    left_open_set_game_ids = sorted(current_game_ids - fresh_game_ids)
    joined_open_set_game_ids = sorted(fresh_game_ids - current_game_ids)
    unchanged_open_set_game_ids = sorted(current_game_ids & fresh_game_ids)

    current_metrics_df.to_csv(output_dir / "current_reviewed_frontier_snapshot.csv", index=False)
    fresh_open_df.to_csv(output_dir / "fresh_raw_open_inventory.csv", index=False)

    frontier_diff_rows: list[dict[str, Any]] = []
    for game_id in left_open_set_game_ids:
        current_row = current_metrics_df.loc[current_metrics_df["game_id"] == game_id].iloc[0]
        frontier_diff_rows.append(
            {
                "game_id": game_id,
                "change_type": "left_open_set",
                "season": int(current_row["season"]),
                "block_key": str(current_row["block_key"]),
            }
        )
    for game_id in joined_open_set_game_ids:
        fresh_row = fresh_open_df.loc[fresh_open_df["game_id"] == game_id].iloc[0]
        frontier_diff_rows.append(
            {
                "game_id": game_id,
                "change_type": "joined_open_set",
                "season": int(fresh_row["season"]),
                "block_key": "",
            }
        )
    for game_id in unchanged_open_set_game_ids:
        current_row = current_metrics_df.loc[current_metrics_df["game_id"] == game_id].iloc[0]
        frontier_diff_rows.append(
            {
                "game_id": game_id,
                "change_type": "unchanged_open_set",
                "season": int(current_row["season"]),
                "block_key": str(current_row["block_key"]),
            }
        )
    pd.DataFrame(frontier_diff_rows).sort_values(["change_type", "season", "game_id"]).to_csv(
        output_dir / "frontier_diff.csv",
        index=False,
    )

    unchanged_current = current_metrics_df.loc[current_metrics_df["game_id"].isin(unchanged_open_set_game_ids)].copy()
    unchanged_fresh = fresh_open_df.loc[fresh_open_df["game_id"].isin(unchanged_open_set_game_ids)].copy()
    unchanged_metric_diffs = unchanged_current.merge(
        unchanged_fresh,
        on="game_id",
        how="inner",
        suffixes=("_current", "_fresh"),
    )
    metric_shift_flags: list[str] = []
    for column in FRONTIER_METRIC_COLUMNS:
        if column in BOOLEAN_FRONTIER_COLUMNS:
            shift_col = f"{column}_changed"
            unchanged_metric_diffs[shift_col] = (
                unchanged_metric_diffs[f"{column}_current"].astype(bool)
                != unchanged_metric_diffs[f"{column}_fresh"].astype(bool)
            )
        else:
            shift_col = f"{column}_delta"
            unchanged_metric_diffs[shift_col] = (
                pd.to_numeric(unchanged_metric_diffs[f"{column}_fresh"], errors="coerce").fillna(0)
                - pd.to_numeric(unchanged_metric_diffs[f"{column}_current"], errors="coerce").fillna(0)
            )
            unchanged_metric_diffs[f"{column}_changed"] = unchanged_metric_diffs[shift_col] != 0
        metric_shift_flags.append(f"{column}_changed")
    unchanged_metric_diffs["any_metric_shift"] = unchanged_metric_diffs[metric_shift_flags].any(axis=1)
    unchanged_metric_diffs.to_csv(output_dir / "unchanged_metric_diffs.csv", index=False)

    changed_metric_games = sorted(
        unchanged_metric_diffs.loc[unchanged_metric_diffs["any_metric_shift"], "game_id"].astype(str).tolist()
    )
    summary = {
        "current_frontier_inventory_csv": str(current_inventory_csv.resolve()),
        "fresh_game_quality_csv": str(fresh_game_quality_csv.resolve()),
        "current_open_game_count": int(len(current_metrics_df)),
        "fresh_open_game_count": int(len(fresh_open_df)),
        "exact_match": bool(current_game_ids == fresh_game_ids),
        "left_open_set_game_ids": left_open_set_game_ids,
        "joined_open_set_game_ids": joined_open_set_game_ids,
        "unchanged_open_set_game_ids": unchanged_open_set_game_ids,
        "unchanged_metric_shift_game_ids": changed_metric_games,
    }
    _write_json(output_dir / "summary.json", summary)
    return summary


def _stage_baseline(candidate_run_dir: Path, staged_baseline_dir: Path) -> dict[str, Any]:
    staged_root = staged_baseline_dir.parent
    if staged_root.exists():
        raise FileExistsError(f"Staged baseline wrapper already exists: {staged_root}")
    staged_root.mkdir(parents=True, exist_ok=False)
    shutil.copytree(candidate_run_dir, staged_baseline_dir, dirs_exist_ok=False)
    return {
        "staged_baseline_root": str(staged_root.resolve()),
        "staged_baseline_dir": str(staged_baseline_dir.resolve()),
        "staged": True,
    }


def _candidate_parquet_path(candidate_run_dir: Path) -> Path:
    parquet_path = candidate_run_dir / "darko_1997_2020.parquet"
    if not parquet_path.exists():
        raise FileNotFoundError(f"Missing consolidated candidate parquet: {parquet_path}")
    return parquet_path


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    output_dir = args.output_dir.resolve()
    _ensure_output_target_unused(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    overall_start = time.perf_counter()
    pbpstats_repo = _resolve_pbpstats_repo()
    os.environ["PBPSTATS_REPO"] = str(pbpstats_repo)

    summary: dict[str, Any] = {
        "inputs": {
            "resume_from": args.resume_from,
            "baseline_dir": str(args.baseline_dir.resolve()),
            "candidate_run_dir": str(args.candidate_run_dir.resolve()),
            "raw_residual_dir": str(args.raw_residual_dir.resolve()),
            "frontier_diff_dir": str(args.frontier_diff_dir.resolve()),
            "reviewed_residual_dir": str(args.reviewed_residual_dir.resolve()),
            "reviewed_frontier_dir": str(args.reviewed_frontier_dir.resolve()),
            "reviewed_pm_dir": str(args.reviewed_pm_dir.resolve()),
            "reviewed_sidecar_dir": str(args.reviewed_sidecar_dir.resolve()),
            "sidecar_smoke_dir": str(args.sidecar_smoke_dir.resolve()),
            "staged_baseline_dir": str(args.staged_baseline_dir.resolve()),
            "reviewed_policy_overlay_csv": str(args.reviewed_policy_overlay_csv.resolve()),
            "frontier_inventory_csv": str(args.frontier_inventory_csv.resolve()),
            "shortlist_csv": str(args.shortlist_csv.resolve()),
            "golden_canary_summary_json": str(args.golden_canary_summary_json.resolve()),
            "compile_summary_json": str(args.compile_summary_json.resolve()),
            "pbpstats_repo": str(pbpstats_repo),
            "runtime_input_cache_mode": str(args.runtime_input_cache_mode),
            "audit_profile": str(args.audit_profile),
            "expected_candidate_total_rows": int(args.expected_candidate_total_rows),
            "expected_baseline_total_rows": int(args.expected_baseline_total_rows),
        },
        "preflight": {},
        "rerun": {},
        "consolidation": {},
        "compare": {},
        "consolidated_compare": {},
        "raw_frontier_diff": {},
        "reviewed_rebuild": {"attempted": False, "passed": False},
        "promotion": {"attempted": False, "promoted": False},
        "timings": {},
        "resource_usage": {},
        "stop_reason": "",
        "phase7_passed": False,
    }

    def write_summary() -> None:
        summary["timings"]["total_wall_seconds"] = round(time.perf_counter() - overall_start, 6)
        summary["resource_usage"]["peak_rss_mb"] = _current_peak_rss_mb()
        _write_json(output_dir / "summary.json", summary)

    try:
        protected_outputs = [
            args.raw_residual_dir.resolve(),
            args.frontier_diff_dir.resolve(),
            args.reviewed_residual_dir.resolve(),
            args.reviewed_frontier_dir.resolve(),
            args.reviewed_pm_dir.resolve(),
            args.reviewed_sidecar_dir.resolve(),
            args.sidecar_smoke_dir.resolve(),
        ]
        if args.resume_from == "start":
            protected_outputs.insert(0, args.candidate_run_dir.resolve())
        for path in protected_outputs:
            _ensure_output_target_unused(path)
        if not args.skip_baseline_stage:
            _ensure_output_target_unused(args.staged_baseline_dir.resolve().parent)

        preflight_start = time.perf_counter()
        build_override_log = output_dir / "build_override_runtime_views.log"
        _run_command(
            [sys.executable, str(ROOT / "build_override_runtime_views.py")],
            log_path=build_override_log,
        )
        summary["preflight"]["compile_summary"] = _check_compile_summary(args.compile_summary_json.resolve())
        summary["preflight"]["golden_canary"] = _check_canary_summary(args.golden_canary_summary_json.resolve())
        summary["preflight"]["baseline"] = _validate_baseline_dir(args.baseline_dir.resolve())
        summary["preflight"]["passed"] = True
        summary["timings"]["preflight_seconds"] = round(time.perf_counter() - preflight_start, 6)
        write_summary()

        if args.resume_from == "post_rerun":
            rerun_start = time.perf_counter()
            candidate_completion = _validate_candidate_run_completion(args.candidate_run_dir.resolve())
            summary["rerun"] = {
                "passed": True,
                "skipped": True,
                "resume_from": "post_rerun",
                "requested_max_workers": int(args.max_workers),
                "fallback_max_workers": int(args.fallback_max_workers),
                "runtime_input_cache_mode": str(args.runtime_input_cache_mode),
                "audit_profile": str(args.audit_profile),
                "candidate_run_dir": str(args.candidate_run_dir.resolve()),
                "candidate_parquet": str((args.candidate_run_dir.resolve() / "darko_1997_2020.parquet").resolve()),
                "candidate_run_completion": candidate_completion,
            }
            summary["timings"]["rerun_seconds"] = round(time.perf_counter() - rerun_start, 6)
        else:
            rerun_attempts: list[dict[str, Any]] = []
            worker_values = [int(args.max_workers)]
            if int(args.fallback_max_workers) and int(args.fallback_max_workers) != int(args.max_workers):
                worker_values.append(int(args.fallback_max_workers))

            rerun_succeeded = False
            actual_max_workers = int(args.max_workers)
            last_failure_text = ""
            rerun_start = time.perf_counter()
            for index, max_workers in enumerate(worker_values):
                rerun_log = output_dir / f"cautious_rerun_max_workers_{max_workers}.log"
                rerun_args = [
                    sys.executable,
                    str(ROOT / "cautious_rerun.py"),
                    "--seasons",
                    *[str(season) for season in PHASE7_SEASONS],
                    "--output-dir",
                    str(args.candidate_run_dir.resolve()),
                    "--run-boxscore-audit",
                    "--runtime-input-cache-mode",
                    str(args.runtime_input_cache_mode),
                    "--audit-profile",
                    str(args.audit_profile),
                    "--max-workers",
                    str(max_workers),
                ]
                result = _run_command(rerun_args, log_path=rerun_log, allow_exit_codes={0, 1})
                rerun_attempts.append(
                    {
                        "max_workers": max_workers,
                        "returncode": result.returncode,
                        "log_path": str(rerun_log.resolve()),
                    }
                )
                if result.returncode == 0:
                    rerun_succeeded = True
                    actual_max_workers = max_workers
                    break
                last_failure_text = rerun_log.read_text(encoding="utf-8")
                if index == 0 and max_workers == int(args.max_workers) and len(worker_values) > 1:
                    if _looks_like_worker_pressure_failure(last_failure_text):
                        if args.candidate_run_dir.exists():
                            shutil.rmtree(args.candidate_run_dir)
                        continue
                raise RuntimeError(f"Full-history rerun failed with max_workers={max_workers}\nSee {rerun_log}")

            if not rerun_succeeded:
                raise RuntimeError("Full-history rerun did not succeed")

            candidate_completion = _validate_candidate_run_completion(args.candidate_run_dir.resolve())
            summary["rerun"] = {
                "passed": True,
                "skipped": False,
                "requested_max_workers": int(args.max_workers),
                "actual_max_workers": actual_max_workers,
                "fallback_max_workers": int(args.fallback_max_workers),
                "runtime_input_cache_mode": str(args.runtime_input_cache_mode),
                "audit_profile": str(args.audit_profile),
                "attempts": rerun_attempts,
                "candidate_run_dir": str(args.candidate_run_dir.resolve()),
                "candidate_parquet": str((args.candidate_run_dir.resolve() / "darko_1997_2020.parquet").resolve()),
                "candidate_run_completion": candidate_completion,
            }
            summary["timings"]["rerun_seconds"] = round(time.perf_counter() - rerun_start, 6)
        write_summary()

        consolidation_start = time.perf_counter()
        consolidation_summary = stitch_consolidated_candidate_parquet(
            candidate_run_dir=args.candidate_run_dir.resolve(),
            expected_total_rows=int(args.expected_candidate_total_rows),
        )
        summary["consolidation"] = {
            "passed": True,
            **consolidation_summary,
        }
        summary["rerun"]["candidate_parquet"] = consolidation_summary["consolidated_parquet"]
        summary["timings"]["consolidation_seconds"] = round(time.perf_counter() - consolidation_start, 6)
        write_summary()

        compare_start = time.perf_counter()
        compare_args = [
            sys.executable,
            str(ROOT / "compare_run_outputs.py"),
            "--baseline-dir",
            str(args.baseline_dir.resolve()),
            "--candidate-dir",
            str(args.candidate_run_dir.resolve()),
            "--normalization-profile",
            "invalid_team_tech",
            "--seasons",
            *[str(season) for season in PHASE7_SEASONS],
            "--json",
        ]
        compare_log = output_dir / "compare_run_outputs.log"
        compare_result = _run_command(compare_args, log_path=compare_log, allow_exit_codes={0, 1})
        compare_payload = json.loads(compare_result.stdout)
        compare_json_path = args.candidate_run_dir.resolve() / "compare_vs_20260322.json"
        _write_json(compare_json_path, compare_payload)
        regressions = _collect_compare_regressions(compare_payload)
        blocking_regressions, report_only_regressions = _split_compare_regressions(regressions)
        summary["compare"] = {
            "passed": not blocking_regressions,
            "compare_json_path": str(compare_json_path.resolve()),
            "normalization_profile": compare_payload.get("normalization_profile"),
            "raw_returncode": int(compare_result.returncode),
            "regression_count": len(regressions),
            "blocking_regression_count": len(blocking_regressions),
            "report_only_regression_count": len(report_only_regressions),
            "regressions": regressions,
            "blocking_regressions": blocking_regressions,
            "report_only_regressions": report_only_regressions,
        }
        summary["timings"]["compare_seconds"] = round(time.perf_counter() - compare_start, 6)
        write_summary()
        if blocking_regressions:
            summary["stop_reason"] = "season_compare_regression"
            write_summary()
            return 1

        consolidated_compare_start = time.perf_counter()
        consolidated_compare_summary = compare_consolidated_parquets(
            baseline_parquet=args.baseline_dir.resolve() / "darko_1997_2020.parquet",
            candidate_parquet=_candidate_parquet_path(args.candidate_run_dir.resolve()),
            output_dir=output_dir,
            expected_baseline_total_rows=int(args.expected_baseline_total_rows),
        )
        summary["consolidated_compare"] = consolidated_compare_summary
        summary["timings"]["consolidated_compare_seconds"] = round(
            time.perf_counter() - consolidated_compare_start,
            6,
        )
        write_summary()
        if not consolidated_compare_summary["passed"]:
            summary["stop_reason"] = "consolidated_compare_regression"
            write_summary()
            return 1

        raw_frontier_start = time.perf_counter()
        raw_residual_log = output_dir / "build_raw_residual_outputs.log"
        _run_command(
            [
                sys.executable,
                str(ROOT / "build_lineup_residual_outputs.py"),
                "--run-dir",
                str(args.candidate_run_dir.resolve()),
                "--output-dir",
                str(args.raw_residual_dir.resolve()),
            ],
            log_path=raw_residual_log,
        )
        raw_frontier_summary = build_raw_frontier_diff(
            current_inventory_csv=args.frontier_inventory_csv.resolve(),
            fresh_game_quality_csv=args.raw_residual_dir.resolve() / "game_quality.csv",
            output_dir=args.frontier_diff_dir.resolve(),
        )
        summary["raw_frontier_diff"] = raw_frontier_summary
        summary["timings"]["raw_frontier_seconds"] = round(time.perf_counter() - raw_frontier_start, 6)
        write_summary()
        if not raw_frontier_summary["exact_match"]:
            summary["stop_reason"] = "raw_open_frontier_drift"
            write_summary()
            return 2

        reviewed_rebuild_start = time.perf_counter()
        reviewed_residual_log = output_dir / "build_reviewed_residual_outputs.log"
        _run_command(
            [
                sys.executable,
                str(ROOT / "build_lineup_residual_outputs.py"),
                "--run-dir",
                str(args.candidate_run_dir.resolve()),
                "--output-dir",
                str(args.reviewed_residual_dir.resolve()),
                "--reviewed-policy-overlay-csv",
                str(args.reviewed_policy_overlay_csv.resolve()),
            ],
            log_path=reviewed_residual_log,
        )

        reviewed_frontier_log = output_dir / "build_reviewed_frontier_inventory.log"
        _run_command(
            [
                sys.executable,
                str(ROOT / "build_reviewed_frontier_inventory.py"),
                "--residual-dir",
                str(args.reviewed_residual_dir.resolve()),
                "--inventory-csv",
                str(args.frontier_inventory_csv.resolve()),
                "--shortlist-csv",
                str(args.shortlist_csv.resolve()),
                "--reviewed-policy-overlay-csv",
                str(args.reviewed_policy_overlay_csv.resolve()),
                "--output-dir",
                str(args.reviewed_frontier_dir.resolve()),
            ],
            log_path=reviewed_frontier_log,
        )

        reviewed_pm_log = output_dir / "build_reviewed_pm_report.log"
        _run_command(
            [
                sys.executable,
                str(ROOT / "build_plus_minus_reference_report.py"),
                "--residual-dir",
                str(args.reviewed_residual_dir.resolve()),
                "--lane-map-csv",
                str(args.frontier_inventory_csv.resolve()),
                "--reviewed-policy-overlay-csv",
                str(args.reviewed_policy_overlay_csv.resolve()),
                "--output-dir",
                str(args.reviewed_pm_dir.resolve()),
            ],
            log_path=reviewed_pm_log,
        )

        reviewed_sidecar_log = output_dir / "build_reviewed_sidecar.log"
        _run_command(
            [
                sys.executable,
                str(ROOT / "build_reviewed_release_quality_sidecar.py"),
                "--residual-dir",
                str(args.reviewed_residual_dir.resolve()),
                "--reviewed-policy-overlay-csv",
                str(args.reviewed_policy_overlay_csv.resolve()),
                "--frontier-inventory-csv",
                str(args.frontier_inventory_csv.resolve()),
                "--output-dir",
                str(args.reviewed_sidecar_dir.resolve()),
            ],
            log_path=reviewed_sidecar_log,
        )

        sidecar_smoke_log = output_dir / "smoke_test_sidecar_join.log"
        _run_command(
            [
                sys.executable,
                str(ROOT / "smoke_test_reviewed_release_quality_sidecar_join.py"),
                "--darko-parquet",
                str(_candidate_parquet_path(args.candidate_run_dir.resolve())),
                "--sidecar-csv",
                str(args.reviewed_sidecar_dir.resolve() / "game_quality_sparse.csv"),
                "--sidecar-summary-json",
                str(args.reviewed_sidecar_dir.resolve() / "summary.json"),
                "--join-contract-json",
                str(args.reviewed_sidecar_dir.resolve() / "join_contract.json"),
                "--output-dir",
                str(args.sidecar_smoke_dir.resolve()),
            ],
            log_path=sidecar_smoke_log,
        )

        reviewed_residual_summary = _load_json(args.reviewed_residual_dir.resolve() / "summary.json")
        reviewed_frontier_summary = _load_json(args.reviewed_frontier_dir.resolve() / "summary.json")
        reviewed_pm_summary = _load_json(args.reviewed_pm_dir.resolve() / "summary.json")
        reviewed_sidecar_summary = _load_json(args.reviewed_sidecar_dir.resolve() / "summary.json")
        sidecar_smoke_summary = _load_json(args.sidecar_smoke_dir.resolve() / "summary.json")
        reviewed_game_quality_df = pd.read_csv(args.reviewed_residual_dir.resolve() / "game_quality.csv", dtype={"game_id": str})
        reviewed_game_quality_df["game_id"] = reviewed_game_quality_df["game_id"].map(_normalize_game_id)
        reviewed_expectations = _load_reviewed_frontier_expectations(
            overlay_csv=args.reviewed_policy_overlay_csv.resolve(),
            inventory_csv=args.frontier_inventory_csv.resolve(),
        )

        release_blocking_game_count = int(reviewed_residual_summary.get("release_blocking_game_count", 0) or 0)
        tier1_release_ready = bool(reviewed_residual_summary.get("tier1_release_ready"))
        tier2_frontier_closed = bool(reviewed_frontier_summary.get("tier2_frontier_closed"))
        reviewed_overlay_version = str(reviewed_sidecar_summary.get("reviewed_policy_overlay_version") or "")
        frontier_inventory_snapshot_id = str(reviewed_sidecar_summary.get("frontier_inventory_snapshot_id") or "")
        research_open_game_ids = sorted(str(value) for value in reviewed_frontier_summary.get("research_open_game_ids", []))
        outside_overlay_rows = reviewed_game_quality_df.loc[
            reviewed_game_quality_df["game_id"] == str(args.expected_outside_overlay_game_id)
        ].copy()
        outside_overlay_policy_source_ok = (
            not outside_overlay_rows.empty
            and str(outside_overlay_rows.iloc[0].get("policy_source") or "") == "auto_default"
        )
        reviewed_rebuild_passed = (
            release_blocking_game_count == len(reviewed_expectations["release_blocking_game_ids"])
            and tier1_release_ready
            and bool(sidecar_smoke_summary.get("join_passed"))
            and reviewed_overlay_version == reviewed_expectations["reviewed_policy_overlay_version"]
            and frontier_inventory_snapshot_id == reviewed_expectations["frontier_inventory_snapshot_id"]
            and str(reviewed_pm_summary.get("reviewed_policy_overlay_version") or "")
            == reviewed_expectations["reviewed_policy_overlay_version"]
            and str(reviewed_pm_summary.get("frontier_inventory_snapshot_id") or "")
            == reviewed_expectations["frontier_inventory_snapshot_id"]
            and int(reviewed_frontier_summary.get("release_blocking_game_count", 0) or 0)
            == len(reviewed_expectations["release_blocking_game_ids"])
            and sorted(str(value) for value in reviewed_frontier_summary.get("release_blocking_game_ids", []))
            == reviewed_expectations["release_blocking_game_ids"]
            and int(reviewed_frontier_summary.get("research_open_game_count", 0) or 0)
            == len(reviewed_expectations["research_open_game_ids"])
            and research_open_game_ids == reviewed_expectations["research_open_game_ids"]
            and sorted(str(value) for value in reviewed_sidecar_summary.get("research_open_game_ids", []))
            == reviewed_expectations["research_open_game_ids"]
            and sorted(str(value) for value in reviewed_sidecar_summary.get("release_blocking_game_ids", []))
            == reviewed_expectations["release_blocking_game_ids"]
            and int(reviewed_sidecar_summary.get("release_blocking_game_count", 0) or 0)
            == len(reviewed_expectations["release_blocking_game_ids"])
            and tier2_frontier_closed is reviewed_expectations["tier2_frontier_closed"]
            and outside_overlay_policy_source_ok
        )
        summary["reviewed_rebuild"] = {
            "attempted": True,
            "passed": reviewed_rebuild_passed,
            "reviewed_residual_summary_json": str((args.reviewed_residual_dir.resolve() / "summary.json")),
            "reviewed_frontier_summary_json": str((args.reviewed_frontier_dir.resolve() / "summary.json")),
            "reviewed_pm_summary_json": str((args.reviewed_pm_dir.resolve() / "summary.json")),
            "reviewed_sidecar_summary_json": str((args.reviewed_sidecar_dir.resolve() / "summary.json")),
            "sidecar_smoke_summary_json": str((args.sidecar_smoke_dir.resolve() / "summary.json")),
            "release_blocking_game_count": release_blocking_game_count,
            "tier1_release_ready": tier1_release_ready,
            "tier2_frontier_closed": tier2_frontier_closed,
            "reviewed_policy_overlay_version": reviewed_overlay_version,
            "frontier_inventory_snapshot_id": frontier_inventory_snapshot_id,
            "research_open_game_ids": research_open_game_ids,
            "expected_release_blocking_game_ids": reviewed_expectations["release_blocking_game_ids"],
            "expected_research_open_game_ids": reviewed_expectations["research_open_game_ids"],
            "outside_overlay_game_id": str(args.expected_outside_overlay_game_id),
            "outside_overlay_policy_source_ok": outside_overlay_policy_source_ok,
            "reviewed_frontier_release_blocking_game_count": int(
                reviewed_frontier_summary.get("release_blocking_game_count", 0) or 0
            ),
            "reviewed_pm_release_blocker_game_count": int(
                reviewed_pm_summary.get("release_blocker_game_count", 0) or 0
            ),
            "expected_overlay_row_count": reviewed_expectations["overlay_row_count"],
        }
        summary["timings"]["reviewed_rebuild_seconds"] = round(
            time.perf_counter() - reviewed_rebuild_start,
            6,
        )
        write_summary()
        if not reviewed_rebuild_passed:
            summary["stop_reason"] = "reviewed_release_artifact_validation_failed"
            write_summary()
            return 3

        promotion_start = time.perf_counter()
        summary["promotion"]["attempted"] = True
        if args.skip_baseline_stage:
            summary["promotion"]["promoted"] = True
            summary["promotion"]["skip_baseline_stage"] = True
        else:
            summary["promotion"].update(
                _stage_baseline(args.candidate_run_dir.resolve(), args.staged_baseline_dir.resolve())
            )
            summary["promotion"]["promoted"] = True

        summary["phase7_passed"] = True
        summary["timings"]["promotion_seconds"] = round(time.perf_counter() - promotion_start, 6)
        write_summary()
        return 0
    except Exception as exc:
        if not summary["stop_reason"]:
            summary["stop_reason"] = type(exc).__name__
        summary["error"] = str(exc)
        write_summary()
        raise


if __name__ == "__main__":
    raise SystemExit(main())
