from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path
from typing import Any

import pandas as pd

from historic_backfill.audits.cross_source.period_starters import (
    DEFAULT_DB_PATH,
    DEFAULT_PARQUET_PATH,
)
from historic_backfill.runners.cautious_rerun import (
    AUDIT_PROFILES,
    DEFAULT_AUDIT_PROFILE,
    DEFAULT_FILE_DIRECTORY,
    DEFAULT_OVERRIDES,
    RUNTIME_INPUT_CACHE_MODES,
)
from historic_backfill.runners.run_intraperiod_manual_review_queue import (
    _compare_game,
    _game_summary_from_run,
    _normalize_game_id,
)


ROOT = Path(__file__).resolve().parent
DEFAULT_MANIFEST_PATH = ROOT / "same_clock_canary_manifest_non_opening_ft_sub_20260320_v1.json"
DEFAULT_REGISTER_PATH = (
    ROOT
    / "intraperiod_proving_1998_2020_20260319_v2"
    / "same_clock_attribution"
    / "same_clock_attribution_register.csv"
)
DEFAULT_CANARY_BASELINE_DIR = ROOT / "same_clock_canary_suite_foul_ft_sub_20260320_v1" / "rerun"
DEFAULT_BLOCK_BASELINES = {
    2000: ROOT / "intraperiod_proving_1998_2020_20260319_v2" / "blocks" / "A_1998-2000",
    2018: ROOT / "intraperiod_proving_1998_2020_20260319_v2" / "blocks" / "E_2017-2020",
    2019: ROOT / "intraperiod_proving_1998_2020_20260319_v2" / "blocks" / "E_2017-2020",
    2020: ROOT / "intraperiod_proving_1998_2020_20260319_v2" / "blocks" / "E_2017-2020",
}
DEFAULT_ORCHESTRATOR_RUNTIME_INPUT_CACHE_MODE = "reuse-validated-cache"


def _read_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
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


def _season_from_game_id(game_id: str | int) -> int:
    gid = _normalize_game_id(game_id)
    yy = int(gid[3:5])
    return 1901 + yy if yy >= 50 else 2001 + yy


def _load_manifest(path: Path) -> list[dict[str, Any]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    rows: list[dict[str, Any]] = []
    negative_keys = {
        (_normalize_game_id(item["game_id"]), int(item["period"]))
        for item in payload.get("guardrails", [])
    }
    for section in ("positive_core_canaries", "companion_canaries"):
        for item in payload.get(section, []):
            game_id = _normalize_game_id(item["game_id"])
            period = int(item["period"])
            rows.append(
                {
                    **item,
                    "game_id": game_id,
                    "period": period,
                    "season": _season_from_game_id(game_id),
                    "is_negative_tripwire": bool(item.get("guardrail", False))
                    or (game_id, period) in negative_keys,
                }
            )
    return rows


def _load_register_rows(path: Path, canaries: list[dict[str, Any]]) -> dict[tuple[str, int], dict[str, Any]]:
    df = pd.read_csv(path)
    df["game_id"] = df["game_id"].astype(str).str.zfill(10)
    keyed: dict[tuple[str, int], dict[str, Any]] = {}
    for canary in canaries:
        subset = df[
            (df["game_id"] == canary["game_id"])
            & (df["period"] == int(canary["period"]))
            & (df["same_clock_family"] == "foul_free_throw_sub_same_clock_ordering")
        ].copy()
        if subset.empty:
            raise ValueError(
                f"No same-clock register row found for {canary['game_id']} P{canary['period']}"
            )
        subset = subset.sort_values(
            ["is_known_negative_tripwire", "game_event_issue_rows", "game_plus_minus_mismatch_rows"],
            ascending=[True, False, False],
        )
        keyed[(canary["game_id"], int(canary["period"]))] = subset.iloc[0].to_dict()
    return keyed


def _parse_cluster_row(register_row: dict[str, Any]) -> dict[str, Any]:
    cluster_events = json.loads(register_row["cluster_events_json"])
    cluster_event_nums = sorted(
        {
            int(event.get("event_num") or 0)
            for event in cluster_events
            if int(register_row["cluster_start_event_num"]) <= int(event.get("event_num") or 0) <= int(register_row["cluster_end_event_num"])
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
        "cluster_events": cluster_events,
        "pre_cluster_lineup": json.loads(register_row["pre_cluster_lineup_json"]),
        "post_cluster_lineup": json.loads(register_row["post_cluster_lineup_json"]),
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


def _game_present(output_dir: Path, game_id: str) -> bool:
    season = _season_from_game_id(game_id)
    minutes_path = output_dir / f"minutes_plus_minus_audit_{season}.csv"
    if not minutes_path.exists():
        return False
    df = pd.read_csv(minutes_path, usecols=["game_id"])
    if df.empty:
        return False
    return bool(df["game_id"].astype(str).str.zfill(10).eq(game_id).any())


def _baseline_output_dir_for_game(game_id: str) -> Path:
    if _game_present(DEFAULT_CANARY_BASELINE_DIR, game_id):
        return DEFAULT_CANARY_BASELINE_DIR
    season = _season_from_game_id(game_id)
    block_dir = DEFAULT_BLOCK_BASELINES.get(season)
    if block_dir is None:
        raise ValueError(f"No baseline output dir configured for season {season} ({game_id})")
    return block_dir


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
        "target_off_court_rows": int(len(issues_df[issues_df["status"] == "off_court_event_credit"])),
        "incoming_player": _player_check(output_dir, game_id, incoming_player_id),
        "outgoing_player": _player_check(output_dir, game_id, outgoing_player_id),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate same-clock scoring overlay canaries against baseline outputs."
    )
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--manifest-path", type=Path, default=DEFAULT_MANIFEST_PATH)
    parser.add_argument("--register-path", type=Path, default=DEFAULT_REGISTER_PATH)
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB_PATH)
    parser.add_argument("--parquet-path", type=Path, default=DEFAULT_PARQUET_PATH)
    parser.add_argument("--overrides-path", type=Path, default=DEFAULT_OVERRIDES)
    parser.add_argument("--file-directory", type=Path, default=DEFAULT_FILE_DIRECTORY)
    parser.add_argument("--max-workers", type=int, default=4)
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

    canaries = _load_manifest(args.manifest_path.resolve())
    register_rows = _load_register_rows(args.register_path.resolve(), canaries)
    parsed_rows = {
        key: _parse_cluster_row(value)
        for key, value in register_rows.items()
    }

    selected_game_ids = sorted({row["game_id"] for row in canaries})
    (output_dir / "selected_cases.json").write_text(
        json.dumps(canaries, indent=2, sort_keys=True),
        encoding="utf-8",
    )

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
    for canary in canaries:
        game_id = canary["game_id"]
        period = int(canary["period"])
        baseline_dir = _baseline_output_dir_for_game(game_id)
        before = _game_summary_from_run(baseline_dir, game_id)
        after = _game_summary_from_run(rerun_dir, game_id)
        delta = _compare_game(before, after)
        cluster = parsed_rows[(game_id, period)]
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
        counting_stats_clean = (
            int(boxscore.get("games_with_team_mismatch", 0) or 0) == 0
            and int(boxscore.get("player_rows_with_mismatch", 0) or 0) == 0
            and int(boxscore.get("audit_failures", 0) or 0) == 0
        )
        comparison_rows.append(
            {
                "game_id": game_id,
                "season": int(canary["season"]),
                "period": period,
                "role": str(canary.get("role") or ""),
                "is_negative_tripwire": bool(canary["is_negative_tripwire"]),
                "baseline_source_dir": str(baseline_dir),
                "team_id": int(cluster["team_id"]),
                "cluster_clock": str(cluster["cluster_clock"]),
                "cluster_start_event_num": int(cluster["cluster_start_event_num"]),
                "cluster_end_event_num": int(cluster["cluster_end_event_num"]),
                "cluster_event_nums": list(cluster["cluster_event_nums"]),
                "incoming_player_id": int(cluster["incoming_player_id"]),
                "outgoing_player_id": int(cluster["outgoing_player_id"]),
                "baseline": before,
                "postpatch": after,
                "delta": delta,
                "baseline_targeted": before_targeted,
                "postpatch_targeted": after_targeted,
                "target_issue_rows_delta": int(after_targeted["target_issue_rows"]) - int(before_targeted["target_issue_rows"]),
                "target_off_court_rows_delta": int(after_targeted["target_off_court_rows"]) - int(before_targeted["target_off_court_rows"]),
                "counting_stats_clean": counting_stats_clean,
            }
        )

    summary = {
        "manifest_path": str(args.manifest_path.resolve()),
        "register_path": str(args.register_path.resolve()),
        "baseline_canary_dir": str(DEFAULT_CANARY_BASELINE_DIR),
        "cases": comparison_rows,
        "acceptance": {
            "counting_stats_clean": all(row["counting_stats_clean"] for row in comparison_rows),
            "positive_canaries_no_minutes_regression": all(
                row["is_negative_tripwire"]
                or float(row["delta"]["game_max_minutes_abs_diff_delta"]) <= 0.0
                for row in comparison_rows
            ),
            "negative_tripwires_no_metric_regression": all(
                (not row["is_negative_tripwire"])
                or (
                    int(row["delta"]["plus_minus_mismatch_rows_delta"]) <= 0
                    and int(row["delta"]["event_issue_rows_delta"]) <= 0
                    and float(row["delta"]["game_max_minutes_abs_diff_delta"]) <= 0.0
                )
                for row in comparison_rows
            ),
        },
    }
    (output_dir / "summary.json").write_text(
        json.dumps(summary, indent=2),
        encoding="utf-8",
    )
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
