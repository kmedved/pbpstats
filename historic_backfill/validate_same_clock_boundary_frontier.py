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
BUNDLE_ROOT = ROOT.parent
CURRENT_FRONTIER_ROOT = BUNDLE_ROOT / "artifacts" / "current_frontier"
DEFAULT_QUEUE_DIR = ROOT / "same_clock_boundary_queue_20260320_v2"
DEFAULT_REGISTER_PATH = (
    ROOT
    / "intraperiod_proving_1998_2020_20260319_v2"
    / "same_clock_attribution"
    / "same_clock_attribution_register.csv"
)
DEFAULT_DB_PATH = ROOT / "nba_raw.db"
DEFAULT_PARQUET_PATH = ROOT / "playbyplayv2.parq"
DEFAULT_OVERRIDES = ROOT / "validation_overrides.csv"
DEFAULT_FILE_DIRECTORY = ROOT
DEFAULT_BLOCK_DIRS = {
    "A": ROOT / "intraperiod_proving_1998_2020_20260319_v2" / "blocks" / "A_1998-2000",
    "B": ROOT / "intraperiod_proving_1998_2020_20260319_v2" / "blocks" / "B_2001-2005",
    "C": ROOT / "intraperiod_proving_1998_2020_20260319_v2" / "blocks" / "C_2006-2010",
    "D": ROOT / "intraperiod_proving_1998_2020_20260319_v2" / "blocks" / "D_2011-2016",
    "E": ROOT / "intraperiod_proving_1998_2020_20260319_v2" / "blocks" / "E_2017-2020",
}
MIGRATED_QUEUE_DIR_CANDIDATES = [
    CURRENT_FRONTIER_ROOT / "same_clock_boundary_queue_20260320_v2",
    CURRENT_FRONTIER_ROOT / "same_clock_boundary_queue_20260320_v2" / "same_clock_boundary_queue_20260320_v2",
]
MIGRATED_REGISTER_PATH_CANDIDATES = [
    CURRENT_FRONTIER_ROOT
    / "intraperiod_proving_1998_2020_20260319_v2"
    / "same_clock_attribution"
    / "same_clock_attribution_register.csv",
    CURRENT_FRONTIER_ROOT
    / "intraperiod_proving_1998_2020_20260319_v2"
    / "intraperiod_proving_1998_2020_20260319_v2"
    / "same_clock_attribution"
    / "same_clock_attribution_register.csv",
]
DEFAULT_ORCHESTRATOR_RUNTIME_INPUT_CACHE_MODE = "reuse-validated-cache"


def _normalize_game_id(value: Any) -> str:
    return str(int(value)).zfill(10)


def _season_from_game_id(game_id: str | int) -> int:
    gid = _normalize_game_id(game_id)
    yy = int(gid[3:5])
    return 1901 + yy if yy >= 50 else 2001 + yy


def _read_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding="utf-8"))


def _resolve_existing_path(candidates: list[Path]) -> Path:
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return candidates[0]


def _resolve_queue_dir(queue_dir: Path) -> Path:
    candidate = queue_dir.resolve()
    if (candidate / "same_clock_boundary_queue.csv").exists():
        return candidate
    matches = sorted(candidate.rglob("same_clock_boundary_queue.csv"))
    if matches:
        return matches[0].parent
    return candidate


def _resolve_register_path(register_path: Path) -> Path:
    candidate = register_path.resolve()
    if candidate.exists():
        return candidate
    return _resolve_existing_path([candidate, *MIGRATED_REGISTER_PATH_CANDIDATES])


def _resolve_block_dirs(register_path: Path) -> dict[str, Path]:
    default_block_root = ROOT / "intraperiod_proving_1998_2020_20260319_v2" / "blocks"
    register_parent_root = register_path.parent.parent
    block_root = _resolve_existing_path(
        [
            default_block_root,
            register_parent_root / "blocks",
            register_parent_root.parent / "blocks",
        ]
    )
    return {
        "A": block_root / "A_1998-2000",
        "B": block_root / "B_2001-2005",
        "C": block_root / "C_2006-2010",
        "D": block_root / "D_2011-2016",
        "E": block_root / "E_2017-2020",
    }


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


def _extract_minutes_metrics(csv_path: Path, game_id: str) -> dict[str, Any]:
    summary = {
        "rows": 0,
        "minutes_mismatch_rows": 0,
        "minute_outlier_rows": 0,
        "plus_minus_mismatch_rows": 0,
        "game_max_minutes_abs_diff": 0.0,
    }
    if not csv_path.exists():
        return summary
    df = pd.read_csv(csv_path)
    if df.empty:
        return summary
    game_df = df.loc[df["game_id"].apply(_normalize_game_id) == _normalize_game_id(game_id)].copy()
    if game_df.empty:
        return summary
    summary["rows"] = int(len(game_df))
    summary["minutes_mismatch_rows"] = int(game_df["has_minutes_mismatch"].fillna(False).sum())
    summary["minute_outlier_rows"] = int(game_df["is_minutes_outlier"].fillna(False).sum())
    summary["plus_minus_mismatch_rows"] = int(game_df["has_plus_minus_mismatch"].fillna(False).sum())
    summary["game_max_minutes_abs_diff"] = float(
        pd.to_numeric(game_df["Minutes_abs_diff"], errors="coerce").fillna(0.0).max()
    )
    return summary


def _extract_event_metrics(csv_path: Path, game_id: str) -> dict[str, Any]:
    summary = {"issue_rows": 0, "issue_status_counts": {}}
    if not csv_path.exists():
        return summary
    df = pd.read_csv(csv_path)
    if df.empty:
        return summary
    game_df = df.loc[df["game_id"].apply(_normalize_game_id) == _normalize_game_id(game_id)].copy()
    if game_df.empty:
        return summary
    summary["issue_rows"] = int(len(game_df))
    summary["issue_status_counts"] = (
        game_df["status"].fillna("").value_counts().sort_index().to_dict()
    )
    return summary


def _extract_cross_source_metrics(csv_path: Path, game_id: str) -> dict[str, Any]:
    summary = {
        "rows": 0,
        "rows_where_official_and_tpdev_pbp_agree_but_output_minutes_differs": 0,
        "rows_where_output_matches_tpdev_pbp_not_official_minutes": 0,
    }
    if not csv_path.exists():
        return summary
    df = pd.read_csv(csv_path)
    if df.empty:
        return summary
    game_df = df.loc[df["game_id"].apply(_normalize_game_id) == _normalize_game_id(game_id)].copy()
    if game_df.empty:
        return summary
    summary["rows"] = int(len(game_df))
    minutes_output = pd.to_numeric(game_df.get("Minutes_output"), errors="coerce")
    minutes_tpdev = pd.to_numeric(game_df.get("Minutes_tpdev_pbp"), errors="coerce")
    minutes_official = pd.to_numeric(game_df.get("Minutes_official"), errors="coerce")
    output_ne_official = (minutes_output - minutes_official).abs() > (1.0 / 60.0)
    official_eq_tpdev = (minutes_official - minutes_tpdev).abs() <= (1.0 / 60.0)
    output_eq_tpdev = (minutes_output - minutes_tpdev).abs() <= (1.0 / 60.0)
    summary["rows_where_official_and_tpdev_pbp_agree_but_output_minutes_differs"] = int(
        (official_eq_tpdev & output_ne_official).sum()
    )
    summary["rows_where_output_matches_tpdev_pbp_not_official_minutes"] = int(
        (output_eq_tpdev & output_ne_official).sum()
    )
    return summary


def _game_summary_from_run(output_dir: Path, game_id: str) -> dict[str, Any]:
    season = _season_from_game_id(game_id)
    summary = _read_json(output_dir / f"summary_{season}.json", {})
    return {
        "season": season,
        "boxscore_audit": summary.get("boxscore_audit") or {},
        "minutes_plus_minus": _extract_minutes_metrics(
            output_dir / f"minutes_plus_minus_audit_{season}.csv",
            game_id,
        ),
        "event_on_court": _extract_event_metrics(
            output_dir / f"event_player_on_court_issues_{season}.csv",
            game_id,
        ),
        "cross_source": _extract_cross_source_metrics(
            output_dir / "cross_source" / "minutes_cross_source_report.csv",
            game_id,
        ),
    }


def _compare_game(before: dict[str, Any], after: dict[str, Any]) -> dict[str, Any]:
    return {
        "minutes_mismatch_rows_delta": int(after["minutes_plus_minus"]["minutes_mismatch_rows"])
        - int(before["minutes_plus_minus"]["minutes_mismatch_rows"]),
        "minute_outlier_rows_delta": int(after["minutes_plus_minus"]["minute_outlier_rows"])
        - int(before["minutes_plus_minus"]["minute_outlier_rows"]),
        "plus_minus_mismatch_rows_delta": int(after["minutes_plus_minus"]["plus_minus_mismatch_rows"])
        - int(before["minutes_plus_minus"]["plus_minus_mismatch_rows"]),
        "game_max_minutes_abs_diff_delta": float(after["minutes_plus_minus"]["game_max_minutes_abs_diff"])
        - float(before["minutes_plus_minus"]["game_max_minutes_abs_diff"]),
        "event_issue_rows_delta": int(after["event_on_court"]["issue_rows"])
        - int(before["event_on_court"]["issue_rows"]),
        "rows_where_official_and_tpdev_pbp_agree_but_output_minutes_differs_delta": int(
            after["cross_source"]["rows_where_official_and_tpdev_pbp_agree_but_output_minutes_differs"]
        )
        - int(before["cross_source"]["rows_where_official_and_tpdev_pbp_agree_but_output_minutes_differs"]),
    }


def _load_minutes_row(minutes_path: Path, game_id: str, player_id: int) -> dict[str, Any] | None:
    if not minutes_path.exists():
        return None
    df = pd.read_csv(minutes_path)
    if df.empty:
        return None
    df["game_id"] = df["game_id"].astype(str).str.zfill(10)
    subset = df[(df["game_id"] == game_id) & (df["player_id"] == int(player_id))]
    if subset.empty:
        return None
    return subset.iloc[0].to_dict()


def _load_issue_rows(
    issues_path: Path,
    game_id: str,
    team_id: int,
    event_nums: list[int],
    player_ids: list[int],
) -> pd.DataFrame:
    if not issues_path.exists():
        return pd.DataFrame()
    df = pd.read_csv(issues_path)
    if df.empty:
        return df
    df["game_id"] = df["game_id"].astype(str).str.zfill(10)
    return df[
        (df["game_id"] == game_id)
        & (df["team_id"] == int(team_id))
        & (df["event_num"].isin(event_nums))
        & (df["player_id"].isin(player_ids))
    ].copy()


def _player_check(minutes_dir: Path, game_id: str, player_id: int) -> dict[str, Any] | None:
    season = _season_from_game_id(game_id)
    row = _load_minutes_row(minutes_dir / f"minutes_plus_minus_audit_{season}.csv", game_id, player_id)
    if row is None:
        return None
    return {
        "player_id": int(row["player_id"]),
        "player_name": str(row.get("player_name") or row["player_id"]),
        "minutes_output": float(row["Minutes_output"]),
        "minutes_official": float(row["Minutes_official"]),
        "minutes_diff": float(row["Minutes_diff"]),
        "plus_minus_output": float(row["Plus_Minus_output"]),
        "plus_minus_official": float(row["Plus_Minus_official"]),
        "plus_minus_diff": float(row["Plus_Minus_diff"]),
        "has_minutes_mismatch": bool(row["has_minutes_mismatch"]),
        "has_plus_minus_mismatch": bool(row["has_plus_minus_mismatch"]),
    }


def _parse_cluster_row(register_row: dict[str, Any]) -> dict[str, Any]:
    cluster_events = json.loads(register_row["cluster_events_json"])
    cluster_event_nums = sorted(
        {
            int(event.get("event_num") or 0)
            for event in cluster_events
            if int(register_row["cluster_start_event_num"])
            <= int(event.get("event_num") or 0)
            <= int(register_row["cluster_end_event_num"])
        }
    )
    return {
        "team_id": int(register_row["team_id"]),
        "incoming_player_id": int(register_row["player_in_id"]),
        "outgoing_player_id": int(register_row["player_out_id"]),
        "cluster_clock": str(register_row["cluster_clock"]),
        "cluster_start_event_num": int(register_row["cluster_start_event_num"]),
        "cluster_end_event_num": int(register_row["cluster_end_event_num"]),
        "cluster_event_nums": cluster_event_nums,
    }


def _extract_targeted_results(
    output_dir: Path,
    *,
    game_id: str,
    team_id: int,
    cluster_event_nums: list[int],
    incoming_player_id: int,
    outgoing_player_id: int,
) -> dict[str, Any]:
    season = _season_from_game_id(game_id)
    issues_path = output_dir / f"event_player_on_court_issues_{season}.csv"
    target_player_ids = [player_id for player_id in [incoming_player_id, outgoing_player_id] if player_id > 0]
    issues_df = _load_issue_rows(
        issues_path,
        game_id,
        team_id,
        cluster_event_nums,
        target_player_ids,
    )
    return {
        "target_issue_rows": int(len(issues_df)),
        "target_off_court_rows": int(
            len(issues_df[issues_df["status"] == "off_court_event_credit"])
        ),
        "incoming_player": _player_check(output_dir, game_id, incoming_player_id),
        "outgoing_player": _player_check(output_dir, game_id, outgoing_player_id),
    }


def _load_queue(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path) if path.exists() else pd.DataFrame()
    if df.empty:
        return df
    df = df.copy()
    df["game_id"] = df["game_id"].map(_normalize_game_id)
    for col in ["season", "period", "team_id", "family_rank", "issue_rows"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)
    df["is_manifest_positive"] = df["is_manifest_positive"].fillna(False).astype(bool)
    return df


def _load_register(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path) if path.exists() else pd.DataFrame()
    if df.empty:
        return df
    df = df.copy()
    df["game_id"] = df["game_id"].map(_normalize_game_id)
    for col in ["period", "team_id"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)
    return df


def _select_queue_rows(queue_df: pd.DataFrame, args: argparse.Namespace) -> pd.DataFrame:
    selected_df = queue_df.copy()
    if args.family:
        selected_df = selected_df.loc[selected_df["same_clock_family"].isin(args.family)].copy()
    if args.only_manifest_positives:
        selected_df = selected_df.loc[selected_df["is_manifest_positive"]].copy()
    if args.max_cases_per_family > 0:
        selected_df = (
            selected_df.sort_values(
                ["same_clock_family", "family_rank", "season", "game_id", "period", "team_id"]
            )
            .groupby("same_clock_family", group_keys=False)
            .head(int(args.max_cases_per_family))
            .copy()
        )
    return selected_df.reset_index(drop=True)


def _merge_queue_with_register(queue_df: pd.DataFrame, register_df: pd.DataFrame) -> pd.DataFrame:
    join_cols = ["game_id", "period", "team_id", "same_clock_family"]
    register_cols = join_cols + [
        "player_in_id",
        "player_out_id",
        "cluster_clock",
        "cluster_start_event_num",
        "cluster_end_event_num",
        "cluster_events_json",
        "current_parser_ordering_outcome_json",
        "pre_cluster_lineup_json",
        "post_cluster_lineup_json",
    ]
    merged = queue_df.merge(register_df[register_cols], on=join_cols, how="left", indicator=True)
    if not bool((merged["_merge"] == "both").all()):
        missing = merged.loc[merged["_merge"] != "both", join_cols]
        raise ValueError(f"Missing same-clock register rows for queue lanes: {missing.to_dict(orient='records')}")
    return merged.drop(columns=["_merge"])


def _family_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    summary: dict[str, Any] = {}
    for family, group_df in pd.DataFrame(rows).groupby("same_clock_family"):
        summary[str(family)] = {
            "cases": int(len(group_df)),
            "manifest_positives": int(group_df["is_manifest_positive"].fillna(False).sum()),
            "counting_stats_clean": bool(group_df["counting_stats_clean"].all()),
            "event_issue_rows_delta_total": int(group_df["delta_event_issue_rows"].sum()),
            "plus_minus_mismatch_rows_delta_total": int(group_df["delta_plus_minus_mismatch_rows"].sum()),
            "minutes_mismatch_rows_delta_total": int(group_df["delta_minutes_mismatch_rows"].sum()),
            "minute_outlier_rows_delta_total": int(group_df["delta_minute_outlier_rows"].sum()),
            "game_max_minutes_abs_diff_delta_max": float(
                group_df["delta_game_max_minutes_abs_diff"].max()
            ),
            "cases_with_event_improvement": int((group_df["delta_event_issue_rows"] < 0).sum()),
            "cases_with_plus_minus_improvement": int(
                (group_df["delta_plus_minus_mismatch_rows"] < 0).sum()
            ),
            "cases_with_any_regression": int(
                (
                    (group_df["delta_event_issue_rows"] > 0)
                    | (group_df["delta_plus_minus_mismatch_rows"] > 0)
                    | (group_df["delta_minutes_mismatch_rows"] > 0)
                    | (group_df["delta_minute_outlier_rows"] > 0)
                    | (group_df["delta_game_max_minutes_abs_diff"] > 0.0)
                ).sum()
            ),
        }
    return summary


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate the cleaned same-clock boundary frontier against existing proving-block baselines."
    )
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--queue-dir", type=Path, default=DEFAULT_QUEUE_DIR)
    parser.add_argument("--register-path", type=Path, default=DEFAULT_REGISTER_PATH)
    parser.add_argument("--family", action="append")
    parser.add_argument("--max-cases-per-family", type=int, default=0)
    parser.add_argument("--only-manifest-positives", action="store_true", default=False)
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB_PATH)
    parser.add_argument("--parquet-path", type=Path, default=DEFAULT_PARQUET_PATH)
    parser.add_argument("--overrides-path", type=Path, default=DEFAULT_OVERRIDES)
    parser.add_argument("--file-directory", type=Path, default=DEFAULT_FILE_DIRECTORY)
    parser.add_argument("--max-workers", type=int, default=2)
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

    queue_dir = _resolve_queue_dir(
        _resolve_existing_path([args.queue_dir.resolve(), *MIGRATED_QUEUE_DIR_CANDIDATES])
    )
    register_path = _resolve_register_path(args.register_path.resolve())
    block_dirs = _resolve_block_dirs(register_path)

    queue_df = _load_queue(queue_dir / "same_clock_boundary_queue.csv")
    register_df = _load_register(register_path)
    if queue_df.empty or register_df.empty:
        summary = {
            "queue_dir": str(queue_dir),
            "register_path": str(register_path),
            "cases": [],
            "family_summary": {},
            "total_cases": 0,
        }
        (output_dir / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
        print(json.dumps(summary, indent=2))
        return 0

    selected_df = _select_queue_rows(queue_df, args)
    selected_df = _merge_queue_with_register(selected_df, register_df)
    (output_dir / "selected_lanes.csv").write_text(selected_df.to_csv(index=False), encoding="utf-8")
    (output_dir / "selected_lanes.json").write_text(
        json.dumps(selected_df.to_dict(orient="records"), indent=2),
        encoding="utf-8",
    )

    selected_game_ids = sorted(set(selected_df["game_id"].tolist()))
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
            "--allow-unreadable-csv-fallback",
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

    comparison_rows: list[dict[str, Any]] = []
    for lane in selected_df.to_dict(orient="records"):
        game_id = _normalize_game_id(lane["game_id"])
        season = int(lane["season"] or _season_from_game_id(game_id))
        block_key = str(lane["block_key"])
        block_id = block_key.split("_", 1)[0]
        baseline_dir = block_dirs[block_id]
        before = _game_summary_from_run(baseline_dir, game_id)
        after = _game_summary_from_run(rerun_dir, game_id)
        delta = _compare_game(before, after)
        cluster = _parse_cluster_row(lane)
        before_targeted = _extract_targeted_results(
            baseline_dir,
            game_id=game_id,
            team_id=int(cluster["team_id"]),
            cluster_event_nums=list(cluster["cluster_event_nums"]),
            incoming_player_id=int(cluster["incoming_player_id"]),
            outgoing_player_id=int(cluster["outgoing_player_id"]),
        )
        after_targeted = _extract_targeted_results(
            rerun_dir,
            game_id=game_id,
            team_id=int(cluster["team_id"]),
            cluster_event_nums=list(cluster["cluster_event_nums"]),
            incoming_player_id=int(cluster["incoming_player_id"]),
            outgoing_player_id=int(cluster["outgoing_player_id"]),
        )
        boxscore = after.get("boxscore_audit") or {}
        comparison_rows.append(
            {
                "game_id": game_id,
                "season": season,
                "period": int(lane["period"]),
                "team_id": int(lane["team_id"]),
                "same_clock_family": str(lane["same_clock_family"]),
                "family_rank": int(lane["family_rank"]),
                "issue_rows": int(lane["issue_rows"]),
                "is_manifest_positive": bool(lane.get("is_manifest_positive", False)),
                "block_key": block_key,
                "baseline_source_dir": str(baseline_dir),
                "baseline": before,
                "postpatch": after,
                "delta": delta,
                "baseline_targeted": before_targeted,
                "postpatch_targeted": after_targeted,
                "target_issue_rows_delta": int(after_targeted["target_issue_rows"])
                - int(before_targeted["target_issue_rows"]),
                "target_off_court_rows_delta": int(after_targeted["target_off_court_rows"])
                - int(before_targeted["target_off_court_rows"]),
                "counting_stats_clean": (
                    int(boxscore.get("games_with_team_mismatch", 0) or 0) == 0
                    and int(boxscore.get("player_rows_with_mismatch", 0) or 0) == 0
                    and int(boxscore.get("audit_failures", 0) or 0) == 0
                ),
                "delta_minutes_mismatch_rows": int(delta["minutes_mismatch_rows_delta"]),
                "delta_minute_outlier_rows": int(delta["minute_outlier_rows_delta"]),
                "delta_plus_minus_mismatch_rows": int(delta["plus_minus_mismatch_rows_delta"]),
                "delta_event_issue_rows": int(delta["event_issue_rows_delta"]),
                "delta_game_max_minutes_abs_diff": float(delta["game_max_minutes_abs_diff_delta"]),
            }
        )

    summary = {
        "queue_dir": str(queue_dir),
        "register_path": str(register_path),
        "total_cases": len(comparison_rows),
        "unique_games": len(selected_game_ids),
        "cases": comparison_rows,
        "family_summary": _family_summary(comparison_rows) if comparison_rows else {},
        "acceptance": {
            "counting_stats_clean": all(row["counting_stats_clean"] for row in comparison_rows),
            "manifest_positives_no_minutes_regression": all(
                (not row["is_manifest_positive"]) or row["delta_game_max_minutes_abs_diff"] <= 0.0
                for row in comparison_rows
            ),
            "no_lane_regressed_on_event_or_minutes": all(
                row["delta_event_issue_rows"] <= 0
                and row["delta_minutes_mismatch_rows"] <= 0
                and row["delta_minute_outlier_rows"] <= 0
                and row["delta_game_max_minutes_abs_diff"] <= 0.0
                for row in comparison_rows
            ),
        },
    }
    (output_dir / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
