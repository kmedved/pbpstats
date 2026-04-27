from __future__ import annotations

import argparse
import csv
import json
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any

import pandas as pd

from historic_backfill.runners.build_intraperiod_residual_dashboard import _load_json as _load_json_file
from historic_backfill.runners.cautious_rerun import (
    AUDIT_PROFILES,
    DEFAULT_DB,
    DEFAULT_FILE_DIRECTORY,
    DEFAULT_AUDIT_PROFILE,
    DEFAULT_OVERRIDES,
    DEFAULT_PARQUET,
    RUNTIME_INPUT_CACHE_MODES,
)
from historic_backfill.runners.run_intraperiod_proving_loop import (
    DEFAULT_BASELINE_DIR,
    DEFAULT_MANIFEST_PATH,
    _aggregate_block_summary,
    _combine_block_parquet,
)
from historic_backfill.audits.cross_source.trace_player_stints_game import _collect_game_events, _load_game_context, _normalize_lineups


ROOT = Path(__file__).resolve().parent
DEFAULT_LOOP_OUTPUT_DIR = ROOT / "intraperiod_proving_1998_2020_20260319_v2"
DEFAULT_ORCHESTRATOR_RUNTIME_INPUT_CACHE_MODE = "reuse-validated-cache"
DEFAULT_QUEUE = [
    {"game_id": "0020400335", "period": 2},
    {"game_id": "0021700653", "period": 4},
    {"game_id": "0020000628", "period": 2},
    {"game_id": "0021700236", "period": 1},
    {"game_id": "0021700886", "period": 3, "manual_only": True},
    {"game_id": "0021900622", "period": 3},
    {"game_id": "0041700117", "period": 2},
]


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


def _load_manifest_blocks(manifest_path: Path) -> dict[str, dict[str, Any]]:
    manifest = _read_json(manifest_path, {})
    result: dict[str, dict[str, Any]] = {}
    for block in manifest.get("blocks", []):
        block_id = str(block["block_id"])
        label = str(block["label"])
        key = f"{block_id}_{label.replace(' ', '_')}"
        result[key] = block
    return result


def _load_family_register(loop_output_dir: Path) -> pd.DataFrame:
    path = loop_output_dir / "family_register" / "intraperiod_family_register.csv"
    df = pd.read_csv(path) if path.exists() else pd.DataFrame()
    if df.empty:
        return df
    df = df.copy()
    df["game_id"] = df["game_id"].map(_normalize_game_id)
    for column in ["period", "team_id", "player_in_id", "player_out_id"]:
        if column in df.columns:
            df[column] = pd.to_numeric(df[column], errors="coerce").fillna(0).astype(int)
    return df


def _select_candidate(
    family_df: pd.DataFrame,
    *,
    game_id: str,
    period: int,
    team_id: int | None = None,
) -> dict[str, Any]:
    subset = family_df.loc[
        (family_df["game_id"] == _normalize_game_id(game_id))
        & (family_df["period"] == int(period))
    ].copy()
    if team_id is not None:
        subset = subset.loc[subset["team_id"] == int(team_id)].copy()
    if subset.empty:
        raise ValueError(f"No manual-review candidate found for {game_id} P{period}")
    subset = subset.sort_values(
        [
            "is_known_negative_tripwire",
            "local_confidence_score",
            "confidence_gap",
            "game_event_issue_rows",
            "approx_window_seconds",
        ],
        ascending=[True, False, False, False, False],
    )
    return subset.iloc[0].to_dict()


def _load_candidate_details(loop_output_dir: Path, family_candidate: dict[str, Any]) -> dict[str, Any]:
    block_key = str(family_candidate["block_key"])
    candidate_path = loop_output_dir / "blocks" / block_key / "candidates" / "intraperiod_missing_sub_candidates.csv"
    candidate_df = pd.read_csv(candidate_path) if candidate_path.exists() else pd.DataFrame()
    if candidate_df.empty:
        raise ValueError(f"No candidate CSV found for {block_key}")
    candidate_df = candidate_df.copy()
    candidate_df["game_id"] = candidate_df["game_id"].map(_normalize_game_id)
    for column in ["period", "team_id", "player_in_id", "player_out_id"]:
        candidate_df[column] = pd.to_numeric(candidate_df[column], errors="coerce").fillna(0).astype(int)
    subset = candidate_df.loc[
        (candidate_df["game_id"] == _normalize_game_id(family_candidate["game_id"]))
        & (candidate_df["period"] == int(family_candidate["period"]))
        & (candidate_df["team_id"] == int(family_candidate["team_id"]))
        & (candidate_df["player_in_id"] == int(family_candidate["player_in_id"]))
        & (candidate_df["player_out_id"] == int(family_candidate["player_out_id"]))
    ].copy()
    if subset.empty:
        raise ValueError(
            "Could not resolve detailed candidate row for "
            f"{family_candidate['game_id']} P{family_candidate['period']} T{family_candidate['team_id']}"
        )
    subset = subset.sort_values(
        ["local_confidence_score", "contradictions_removed", "best_vs_runner_up_confidence_gap"],
        ascending=[False, False, False],
    )
    return subset.iloc[0].to_dict()


def _event_index_lookup(events: list[object]) -> dict[tuple[int, int], list[int]]:
    lookup: dict[tuple[int, int], list[int]] = {}
    for index, event in enumerate(events):
        try:
            period = int(getattr(event, "period"))
            event_num = int(getattr(event, "event_num"))
        except (AttributeError, TypeError, ValueError):
            continue
        lookup.setdefault((period, event_num), []).append(index)
    return lookup


def _lineup_at_event(events: list[object], index: int, team_id: int) -> list[int]:
    if index < 0 or index >= len(events):
        return []
    current = _normalize_lineups(getattr(events[index], "current_players", {})).get(int(team_id), [])
    if current:
        return list(current)
    previous_event = getattr(events[index], "previous_event", None)
    if previous_event is None:
        return []
    return list(_normalize_lineups(getattr(previous_event, "current_players", {})).get(int(team_id), []))


def _build_override_window(
    candidate: dict[str, Any],
    *,
    parquet_path: Path,
    db_path: Path,
    file_directory: Path,
) -> tuple[dict[str, Any], dict[str, Any]]:
    game_id = _normalize_game_id(candidate["game_id"])
    period = int(candidate["period"])
    team_id = int(candidate["team_id"])
    player_in_id = int(candidate["player_in_id"])
    player_out_id = int(candidate["player_out_id"])
    deadball_start_event_num = int(float(candidate["deadball_window_start_event_num"]))
    deadball_end_event_num = int(float(candidate["deadball_window_end_event_num"]))
    evaluation_end_event_num = int(float(candidate["end_event_num"]))
    apply_position = str(candidate.get("deadball_apply_position") or "window_start")

    _, possessions, name_map = _load_game_context(
        game_id,
        parquet_path=parquet_path.resolve(),
        db_path=db_path.resolve(),
        file_directory=file_directory.resolve(),
    )
    events = _collect_game_events(possessions)
    event_positions = _event_index_lookup(events)
    start_indices = event_positions.get((period, deadball_start_event_num), [])
    end_indices = event_positions.get((period, deadball_end_event_num), [])
    eval_end_indices = event_positions.get((period, evaluation_end_event_num), [])
    if not start_indices or not end_indices or not eval_end_indices:
        raise ValueError(
            f"Could not resolve event indices for {game_id} P{period} "
            f"({deadball_start_event_num}-{deadball_end_event_num} -> {evaluation_end_event_num})"
        )

    anchor_start_index = start_indices[0]
    anchor_end_index = end_indices[-1]
    apply_start_index = anchor_start_index
    if apply_position == "after_window_end":
        apply_start_index = anchor_end_index + 1
    if apply_start_index >= len(events):
        raise ValueError(f"Apply index past event list for {game_id} P{period}")
    evaluation_end_index = eval_end_indices[-1]
    if evaluation_end_index < apply_start_index:
        raise ValueError(f"Invalid evaluation window for {game_id} P{period}")

    base_lineup = _lineup_at_event(events, apply_start_index, team_id)
    if player_out_id not in base_lineup:
        contradiction_start_event_num = int(float(candidate["start_event_num"]))
        contradiction_start_indices = event_positions.get((period, contradiction_start_event_num), [])
        if contradiction_start_indices:
            base_lineup = _lineup_at_event(events, contradiction_start_indices[0], team_id)
    if player_out_id not in base_lineup:
        raise ValueError(
            f"Outgoing player {player_out_id} not present in base lineup for {game_id} P{period} "
            f"{team_id}: {base_lineup}"
        )

    proposed_lineup = [player_in_id if player_id == player_out_id else int(player_id) for player_id in base_lineup]
    if len(proposed_lineup) != 5 or len(set(proposed_lineup)) != 5:
        raise ValueError(f"Malformed proposed lineup for {game_id}: {proposed_lineup}")

    window = {
        "period": period,
        "team_id": team_id,
        "start_event_num": int(getattr(events[apply_start_index], "event_num")),
        "end_event_num": int(getattr(events[evaluation_end_index], "event_num")),
        "lineup_player_ids": proposed_lineup,
    }
    diagnostics = {
        "game_id": game_id,
        "period": period,
        "team_id": team_id,
        "player_in_id": player_in_id,
        "player_out_id": player_out_id,
        "player_in_name": name_map.get(player_in_id, ""),
        "player_out_name": name_map.get(player_out_id, ""),
        "apply_position": apply_position,
        "anchor_start_event_num": deadball_start_event_num,
        "anchor_end_event_num": deadball_end_event_num,
        "apply_start_event_num": window["start_event_num"],
        "apply_start_clock": str(getattr(events[apply_start_index], "clock", "") or ""),
        "evaluation_end_event_num": window["end_event_num"],
        "evaluation_end_clock": str(getattr(events[evaluation_end_index], "clock", "") or ""),
        "base_lineup_player_ids": base_lineup,
        "proposed_lineup_player_ids": proposed_lineup,
        "local_confidence_score": float(candidate.get("local_confidence_score") or 0.0),
        "confidence_gap": float(candidate.get("confidence_gap") or 0.0),
        "approx_window_seconds": float(candidate.get("approx_window_seconds") or 0.0),
    }
    return window, diagnostics


def _prepare_temp_file_directory(case_dir: Path, live_file_directory: Path) -> Path:
    file_directory = case_dir / "file_directory"
    overrides_dir = file_directory / "overrides"
    overrides_dir.mkdir(parents=True, exist_ok=True)
    for filename in [
        "lineup_window_overrides.json",
        "lineup_window_override_notes.csv",
        "period_starters_overrides.json",
        "period_starters_override_notes.csv",
    ]:
        source = live_file_directory / "overrides" / filename
        if source.exists():
            shutil.copy2(source, overrides_dir / filename)
    return file_directory


def _write_temp_override(
    *,
    file_directory: Path,
    window: dict[str, Any],
    diagnostics: dict[str, Any],
) -> None:
    overrides_path = file_directory / "overrides" / "lineup_window_overrides.json"
    overrides = _read_json(overrides_path, {})
    game_id = diagnostics["game_id"]
    game_windows = list(overrides.get(game_id, []))
    game_windows.append(window)
    overrides[game_id] = game_windows
    overrides_path.write_text(json.dumps(overrides, indent=2, sort_keys=True), encoding="utf-8")

    notes_path = file_directory / "overrides" / "lineup_window_override_notes.csv"
    fieldnames = [
        "game_id",
        "period",
        "team_id",
        "start_event_num",
        "end_event_num",
        "source_type",
        "reason",
        "evidence_summary",
        "local_confidence_score",
        "external_alignment_score",
        "date_added",
        "notes",
    ]
    existing_rows: list[dict[str, Any]] = []
    if notes_path.exists():
        with notes_path.open(encoding="utf-8", newline="") as infile:
            reader = csv.DictReader(infile)
            existing_rows = list(reader)
    existing_rows.append(
        {
            "game_id": diagnostics["game_id"],
            "period": diagnostics["period"],
            "team_id": diagnostics["team_id"],
            "start_event_num": window["start_event_num"],
            "end_event_num": window["end_event_num"],
            "source_type": "manual_intraperiod_review_temp",
            "reason": f"Manual intraperiod review canary for {diagnostics['player_in_name']} in / {diagnostics['player_out_name']} out",
            "evidence_summary": (
                f"apply {diagnostics['apply_position']} after dead-ball "
                f"{diagnostics['anchor_start_event_num']}-{diagnostics['anchor_end_event_num']}; "
                f"window {window['start_event_num']}-{window['end_event_num']}"
            ),
            "local_confidence_score": diagnostics["local_confidence_score"],
            "external_alignment_score": "",
            "date_added": "2026-03-20",
            "notes": "temporary manual-review queue canary",
        }
    )
    with notes_path.open("w", encoding="utf-8", newline="") as outfile:
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(existing_rows)


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
    summary["issue_status_counts"] = game_df["status"].fillna("").value_counts().sort_index().to_dict()
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
    if minutes_output is not None and minutes_tpdev is not None and minutes_official is not None:
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
    boxscore_audit = summary.get("boxscore_audit") or {}
    lineup_audit = summary.get("lineup_audit") or {}
    return {
        "season": season,
        "boxscore_audit": boxscore_audit,
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
        "trace_summary": _read_json(output_dir / "trace" / game_id / "summary.json", {}),
    }


def _game_summary_from_loop(loop_output_dir: Path, block_key: str, game_id: str) -> dict[str, Any]:
    block_dir = loop_output_dir / "blocks" / block_key
    season = _season_from_game_id(game_id)
    return {
        "season": season,
        "minutes_plus_minus": _extract_minutes_metrics(
            block_dir / f"minutes_plus_minus_audit_{season}.csv",
            game_id,
        ),
        "event_on_court": _extract_event_metrics(
            block_dir / f"event_player_on_court_issues_{season}.csv",
            game_id,
        ),
        "cross_source": _extract_cross_source_metrics(
            block_dir / "cross_source" / "minutes_cross_source_report.csv",
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


def _compare_block_summary(current: dict[str, Any], baseline: dict[str, Any]) -> dict[str, Any]:
    keys = [
        "minutes_mismatches",
        "minutes_outliers",
        "plus_minus_mismatches",
        "event_on_court_issue_rows",
        "event_on_court_issue_games",
        "problem_games",
    ]
    result = {key: int(current.get(key, 0)) - int(baseline.get(key, 0)) for key in keys}
    current_cross = current.get("cross_source_summary") or {}
    baseline_cross = baseline.get("cross_source_summary") or {}
    result["rows_where_official_and_tpdev_pbp_agree_but_output_minutes_differs"] = int(
        current_cross.get("rows_where_official_and_tpdev_pbp_agree_but_output_minutes_differs", 0) or 0
    ) - int(
        baseline_cross.get("rows_where_official_and_tpdev_pbp_agree_but_output_minutes_differs", 0) or 0
    )
    return result


def _acceptance_for_case(
    *,
    block_id: str,
    one_game_summary: dict[str, Any],
    game_delta: dict[str, Any],
    block_delta_vs_loop: dict[str, Any],
    block_rerun_performed: bool,
) -> dict[str, Any]:
    boxscore = one_game_summary.get("boxscore_audit") or {}
    boxscore_clean = (
        int(boxscore.get("games_with_team_mismatch", 0) or 0) == 0
        and int(boxscore.get("player_rows_with_mismatch", 0) or 0) == 0
        and int(boxscore.get("audit_failures", 0) or 0) == 0
    )
    contradictions_reduced = int(game_delta["event_issue_rows_delta"]) < 0
    no_new_game_residue = float(game_delta["game_max_minutes_abs_diff_delta"]) <= 0.0

    if not block_rerun_performed:
        block_ok = False
    elif block_id == "A":
        block_ok = all(
            int(value) <= 0
            for key, value in block_delta_vs_loop.items()
            if key != "rows_where_official_and_tpdev_pbp_agree_but_output_minutes_differs"
        )
    elif block_id == "B":
        block_ok = (
            int(block_delta_vs_loop["minutes_mismatches"]) <= 0
            and int(block_delta_vs_loop["minutes_outliers"]) <= 0
            and int(block_delta_vs_loop["event_on_court_issue_rows"]) <= 0
            and int(block_delta_vs_loop["event_on_court_issue_games"]) <= 0
            and int(block_delta_vs_loop["plus_minus_mismatches"]) <= 0
        )
    else:
        block_ok = (
            int(block_delta_vs_loop["rows_where_official_and_tpdev_pbp_agree_but_output_minutes_differs"])
            <= 0
        )

    return {
        "boxscore_clean": boxscore_clean,
        "contradictions_reduced": contradictions_reduced,
        "no_new_game_residue": no_new_game_residue,
        "block_ok": block_ok,
        "block_rerun_performed": block_rerun_performed,
        "accepted": boxscore_clean and contradictions_reduced and no_new_game_residue and block_ok,
    }


def _promote_override(live_file_directory: Path, window: dict[str, Any], diagnostics: dict[str, Any]) -> None:
    overrides_path = live_file_directory / "overrides" / "lineup_window_overrides.json"
    overrides = _read_json(overrides_path, {})
    game_id = diagnostics["game_id"]
    game_windows = list(overrides.get(game_id, []))
    game_windows.append(window)
    overrides[game_id] = game_windows
    overrides_path.write_text(json.dumps(overrides, indent=2, sort_keys=True), encoding="utf-8")

    notes_path = live_file_directory / "overrides" / "lineup_window_override_notes.csv"
    fieldnames = [
        "game_id",
        "period",
        "team_id",
        "start_event_num",
        "end_event_num",
        "source_type",
        "reason",
        "evidence_summary",
        "local_confidence_score",
        "external_alignment_score",
        "date_added",
        "notes",
    ]
    existing_rows: list[dict[str, Any]] = []
    if notes_path.exists():
        with notes_path.open(encoding="utf-8", newline="") as infile:
            reader = csv.DictReader(infile)
            existing_rows = list(reader)
    existing_rows.append(
        {
            "game_id": diagnostics["game_id"],
            "period": diagnostics["period"],
            "team_id": diagnostics["team_id"],
            "start_event_num": window["start_event_num"],
            "end_event_num": window["end_event_num"],
            "source_type": "manual_intraperiod_override",
            "reason": f"Bounded intraperiod lineup repair for {diagnostics['player_in_name']} in / {diagnostics['player_out_name']} out",
            "evidence_summary": (
                f"dead-ball {diagnostics['anchor_start_event_num']}-{diagnostics['anchor_end_event_num']}; "
                f"apply {diagnostics['apply_position']}; window {window['start_event_num']}-{window['end_event_num']}"
            ),
            "local_confidence_score": diagnostics["local_confidence_score"],
            "external_alignment_score": "",
            "date_added": "2026-03-20",
            "notes": "promoted by intraperiod manual review queue",
        }
    )
    with notes_path.open("w", encoding="utf-8", newline="") as outfile:
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(existing_rows)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run the manual intraperiod override review queue with temp file-directory canaries."
    )
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--loop-output-dir", type=Path, default=DEFAULT_LOOP_OUTPUT_DIR)
    parser.add_argument("--manifest-path", type=Path, default=DEFAULT_MANIFEST_PATH)
    parser.add_argument("--baseline-dir", type=Path, default=DEFAULT_BASELINE_DIR)
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
    parser.add_argument("--only-game-id", type=str)
    parser.add_argument("--case-limit", type=int)
    parser.add_argument("--skip-block-rerun", action="store_true", default=False)
    parser.add_argument("--promote-accepted", action="store_true", default=False)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    output_dir = args.output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    loop_output_dir = args.loop_output_dir.resolve()
    manifest_blocks = _load_manifest_blocks(args.manifest_path.resolve())
    family_df = _load_family_register(loop_output_dir)
    baseline_dir = args.baseline_dir.resolve() if args.baseline_dir is not None else None

    queue = list(DEFAULT_QUEUE)
    if args.only_game_id:
        only_game_id = _normalize_game_id(args.only_game_id)
        queue = [entry for entry in queue if _normalize_game_id(entry["game_id"]) == only_game_id]
    if args.case_limit is not None:
        queue = queue[: max(args.case_limit, 0)]

    case_summaries: list[dict[str, Any]] = []
    for index, entry in enumerate(queue, start=1):
        game_id = _normalize_game_id(entry["game_id"])
        period = int(entry["period"])
        candidate = _select_candidate(
            family_df,
            game_id=game_id,
            period=period,
            team_id=entry.get("team_id"),
        )
        candidate = {
            **candidate,
            **_load_candidate_details(loop_output_dir, candidate),
        }
        block_key = str(candidate["block_key"])
        block_id = block_key.split("_", 1)[0]
        block_meta = manifest_blocks.get(block_key)
        if block_meta is None:
            raise ValueError(f"Could not resolve block metadata for {block_key}")

        case_slug = f"{index:02d}_{game_id}_P{period}_T{int(candidate['team_id'])}"
        case_dir = output_dir / case_slug
        case_dir.mkdir(parents=True, exist_ok=True)
        temp_file_directory = _prepare_temp_file_directory(case_dir, args.file_directory.resolve())
        window, diagnostics = _build_override_window(
            candidate,
            parquet_path=args.parquet_path.resolve(),
            db_path=args.db_path.resolve(),
            file_directory=args.file_directory.resolve(),
        )
        _write_temp_override(
            file_directory=temp_file_directory,
            window=window,
            diagnostics=diagnostics,
        )
        (case_dir / "candidate.json").write_text(json.dumps(candidate, indent=2, default=str), encoding="utf-8")
        (case_dir / "proposed_window.json").write_text(
            json.dumps({"window": window, "diagnostics": diagnostics}, indent=2),
            encoding="utf-8",
        )

        one_game_dir = case_dir / "one_game"
        one_game_dir.mkdir(parents=True, exist_ok=True)
        _run_command(
            [
                sys.executable,
                str(ROOT / "rerun_selected_games.py"),
                "--game-ids",
                game_id,
                "--output-dir",
                str(one_game_dir),
                "--db-path",
                str(args.db_path.resolve()),
                "--parquet-path",
                str(args.parquet_path.resolve()),
                "--overrides-path",
                str(args.overrides_path.resolve()),
                "--file-directory",
                str(temp_file_directory),
                "--max-workers",
                str(args.max_workers),
                "--runtime-input-cache-mode",
                str(args.runtime_input_cache_mode),
                "--audit-profile",
                str(args.audit_profile),
                "--run-boxscore-audit",
            ],
            log_path=one_game_dir / "rerun.log",
        )

        trace_root = case_dir / "trace"
        trace_root.mkdir(parents=True, exist_ok=True)
        _run_command(
            [
                sys.executable,
                str(ROOT / "trace_player_stints_game.py"),
                "--game-id",
                game_id,
                "--output-dir",
                str(trace_root),
                "--db-path",
                str(args.db_path.resolve()),
                "--parquet-path",
                str(args.parquet_path.resolve()),
                "--file-directory",
                str(temp_file_directory),
            ],
            log_path=trace_root / "run.log",
        )

        combined_parquet = one_game_dir / "darko_selected_games.parquet"
        if combined_parquet.exists():
            cross_dir = one_game_dir / "cross_source"
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

        before_game = _game_summary_from_loop(loop_output_dir, block_key, game_id)
        after_game = _game_summary_from_run(one_game_dir, game_id)
        game_delta = _compare_game(before_game, after_game)
        preliminary_boxscore_clean = (
            int((after_game.get("boxscore_audit") or {}).get("games_with_team_mismatch", 0) or 0) == 0
            and int((after_game.get("boxscore_audit") or {}).get("player_rows_with_mismatch", 0) or 0) == 0
            and int((after_game.get("boxscore_audit") or {}).get("audit_failures", 0) or 0) == 0
        )
        preliminary_contradictions_reduced = int(game_delta["event_issue_rows_delta"]) < 0
        preliminary_no_new_game_residue = float(game_delta["game_max_minutes_abs_diff_delta"]) <= 0.0
        preliminary_no_new_plus_minus_residue = int(game_delta["plus_minus_mismatch_rows_delta"]) <= 0

        block_summary = None
        block_delta_vs_loop = None
        loop_block_summary = _load_json_file(loop_output_dir / "blocks" / block_key / "block_summary.json")
        baseline_block_summary = None
        if baseline_dir is not None:
            baseline_path = baseline_dir / "blocks" / block_key / "block_summary.json"
            if baseline_path.exists():
                baseline_block_summary = _load_json_file(baseline_path)
        should_run_block = (
            not args.skip_block_rerun
            and preliminary_boxscore_clean
            and preliminary_contradictions_reduced
            and preliminary_no_new_game_residue
            and (block_id not in {"A", "B"} or preliminary_no_new_plus_minus_residue)
        )
        if should_run_block:
            block_dir = case_dir / "block"
            block_dir.mkdir(parents=True, exist_ok=True)
            seasons = [int(season) for season in block_meta["seasons"]]
            _run_command(
                [
                    sys.executable,
                    str(ROOT / "cautious_rerun.py"),
                    "--seasons",
                    *[str(season) for season in seasons],
                    "--output-dir",
                    str(block_dir),
                    "--db-path",
                    str(args.db_path.resolve()),
                    "--parquet-path",
                    str(args.parquet_path.resolve()),
                    "--overrides-path",
                    str(args.overrides_path.resolve()),
                    "--file-directory",
                    str(temp_file_directory),
                    "--max-workers",
                    str(args.max_workers),
                    "--runtime-input-cache-mode",
                    str(args.runtime_input_cache_mode),
                    "--audit-profile",
                    str(args.audit_profile),
                    "--run-boxscore-audit",
                ],
                log_path=block_dir / "rerun.log",
            )
            combined_block_parquet = _combine_block_parquet(block_dir, seasons)
            if combined_block_parquet is not None:
                cross_dir = block_dir / "cross_source"
                cross_dir.mkdir(parents=True, exist_ok=True)
                _run_command(
                    [
                        sys.executable,
                        str(ROOT / "build_minutes_cross_source_report.py"),
                        "--darko-parquet",
                        str(combined_block_parquet),
                        "--output-dir",
                        str(cross_dir),
                    ],
                    log_path=cross_dir / "run.log",
                )
            block_summary = _aggregate_block_summary(block_dir, block_meta)
            (block_dir / "block_summary.json").write_text(
                json.dumps(block_summary, indent=2),
                encoding="utf-8",
            )
            block_delta_vs_loop = _compare_block_summary(block_summary, loop_block_summary)
        else:
            block_delta_vs_loop = {
                "minutes_mismatches": 0,
                "minutes_outliers": 0,
                "plus_minus_mismatches": 0,
                "event_on_court_issue_rows": 0,
                "event_on_court_issue_games": 0,
                "problem_games": 0,
                "rows_where_official_and_tpdev_pbp_agree_but_output_minutes_differs": 0,
            }

        acceptance = _acceptance_for_case(
            block_id=block_id,
            one_game_summary=after_game,
            game_delta=game_delta,
            block_delta_vs_loop=block_delta_vs_loop,
            block_rerun_performed=bool(should_run_block),
        )

        if acceptance["accepted"] and args.promote_accepted:
            _promote_override(args.file_directory.resolve(), window, diagnostics)

        case_summary = {
            "case_slug": case_slug,
            "game_id": game_id,
            "period": period,
            "block_key": block_key,
            "manual_only": bool(entry.get("manual_only", False)),
            "candidate": candidate,
            "window": window,
            "diagnostics": diagnostics,
            "before_game": before_game,
            "after_game": after_game,
            "game_delta": game_delta,
            "loop_block_summary": loop_block_summary,
            "block_summary": block_summary,
            "baseline_block_summary": baseline_block_summary,
            "block_delta_vs_loop": block_delta_vs_loop,
            "block_rerun_skipped_due_to_failed_game_gate": bool(
                not args.skip_block_rerun and not should_run_block
            ),
            "acceptance": acceptance,
            "promoted": bool(acceptance["accepted"] and args.promote_accepted),
        }
        (case_dir / "summary.json").write_text(json.dumps(case_summary, indent=2, default=str), encoding="utf-8")
        case_summaries.append(case_summary)

    final_summary = {
        "loop_output_dir": str(loop_output_dir),
        "baseline_dir": str(baseline_dir) if baseline_dir is not None else None,
        "cases_requested": len(queue),
        "cases_completed": len(case_summaries),
        "accepted_cases": sum(1 for item in case_summaries if item["acceptance"]["accepted"]),
        "promoted_cases": sum(1 for item in case_summaries if item["promoted"]),
        "cases": [
            {
                "case_slug": item["case_slug"],
                "game_id": item["game_id"],
                "period": item["period"],
                "block_key": item["block_key"],
                "accepted": item["acceptance"]["accepted"],
                "promoted": item["promoted"],
                "event_issue_rows_delta": item["game_delta"]["event_issue_rows_delta"],
                "game_max_minutes_abs_diff_delta": item["game_delta"]["game_max_minutes_abs_diff_delta"],
                "block_delta_vs_loop": item["block_delta_vs_loop"],
            }
            for item in case_summaries
        ],
    }
    (output_dir / "summary.json").write_text(json.dumps(final_summary, indent=2), encoding="utf-8")
    print(json.dumps(final_summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
