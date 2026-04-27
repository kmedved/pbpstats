from __future__ import annotations

import argparse
import ast
import json
from functools import lru_cache
from pathlib import Path
from typing import Any

import pandas as pd

from historic_backfill.runners.cautious_rerun import DEFAULT_DB, DEFAULT_FILE_DIRECTORY, DEFAULT_PARQUET
from historic_backfill.runners.intraperiod_missing_sub_repair import (
    _approx_window_seconds,
    _build_substitution_context,
    _evaluate_player_out_candidate,
    _find_next_same_team_substitution_index,
    _find_period_end_index,
    _same_clock_window_bounds,
    collect_intraperiod_contradictions,
)
from historic_backfill.audits.cross_source.trace_player_stints_game import _collect_game_events, _load_game_context, _normalize_lineups


ROOT = Path(__file__).resolve().parent
SMALL_MINUTE_DRIFT_THRESHOLD = 0.25


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _normalize_game_id(value: Any) -> str:
    return str(int(value)).zfill(10)


def _safe_literal(value: Any) -> Any:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    if isinstance(value, (dict, list)):
        return value
    text = str(value).strip()
    if not text:
        return None
    try:
        return json.loads(text)
    except Exception:
        try:
            return ast.literal_eval(text)
        except Exception:
            return None


def _as_int(value: Any, default: int = 0) -> int:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return default
    try:
        return int(float(value))
    except Exception:
        return default


def _as_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


def _event_number(event: object) -> int | None:
    value = getattr(event, "event_num", None)
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _event_description(event: object) -> str:
    return str(
        getattr(event, "description", None)
        or getattr(event, "event_description", None)
        or ""
    )


def _lineup_for_team(event: object, team_id: int, *, previous: bool = False) -> list[int]:
    source = getattr(event, "previous_event", None) if previous else event
    lineups = _normalize_lineups(getattr(source, "current_players", {}))
    return list(lineups.get(int(team_id), []))


def _score_delta_for_window(events: list[object], start_index: int, end_index: int, team_id: int) -> int:
    score_delta = 0
    for event in events[start_index : end_index + 1]:
        if not bool(getattr(event, "is_made", False)):
            continue
        points = getattr(event, "points", None)
        try:
            points_int = int(points)
        except (TypeError, ValueError):
            points_int = 0
        if points_int <= 0:
            continue
        try:
            event_team_id = int(getattr(event, "team_id"))
        except (TypeError, ValueError):
            continue
        score_delta += points_int if event_team_id == int(team_id) else -points_int
    return score_delta


def _same_clock_family(cluster_events: list[object]) -> str:
    class_names = {event.__class__.__name__ for event in cluster_events}
    has_sub = any(name.endswith("Substitution") for name in class_names)
    has_field_goal = any(name.endswith("FieldGoal") for name in class_names)
    has_foul = any(name.endswith("Foul") for name in class_names)
    has_free_throw = any(name.endswith("FreeThrow") for name in class_names)
    if has_sub and has_field_goal:
        return "scorer_sub_same_clock_ordering"
    if has_sub and (has_foul or has_free_throw):
        return "foul_free_throw_sub_same_clock_ordering"
    return "cluster_start_vs_cluster_end_timing"


def _should_include_same_clock_row(row: pd.Series) -> bool:
    family = str(row.get("family") or "")
    max_diff = float(row.get("game_max_minutes_abs_diff") or 0.0)
    minute_outlier_rows = int(row.get("game_minute_outlier_rows") or 0)
    plus_minus_rows = int(row.get("game_plus_minus_mismatch_rows") or 0)
    event_issue_rows = int(row.get("game_event_issue_rows") or 0)
    if family == "no_deadball_local_signal":
        return True
    if family != "after_cluster_low_confidence":
        return False
    if minute_outlier_rows > 0 or max_diff > SMALL_MINUTE_DRIFT_THRESHOLD:
        return False
    return plus_minus_rows > 0 or event_issue_rows > 0


def _load_cross_source_rows(loop_output_dir: Path) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for csv_path in sorted(loop_output_dir.glob("blocks/*/cross_source/minutes_cross_source_report.csv")):
        df = pd.read_csv(csv_path)
        if df.empty:
            continue
        df = df.copy()
        df["block_key"] = csv_path.parent.parent.name
        df["game_id"] = df["game_id"].map(_normalize_game_id)
        df["player_id"] = pd.to_numeric(df["player_id"], errors="coerce").fillna(0).astype(int)
        frames.append(df)
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def _build_cross_source_map(cross_source_df: pd.DataFrame) -> dict[tuple[str, str, int], dict[str, float]]:
    mapping: dict[tuple[str, str, int], dict[str, float]] = {}
    if cross_source_df.empty:
        return mapping
    for row in cross_source_df.itertuples(index=False):
        mapping[(str(row.block_key), str(row.game_id), int(row.player_id))] = {
            "output_seconds": float(getattr(row, "Minutes_output", 0.0) or 0.0) * 60.0,
            "tpdev_pbp_seconds": float(getattr(row, "Minutes_tpdev_pbp", 0.0) or 0.0) * 60.0,
            "official_seconds": float(getattr(row, "Minutes_official", 0.0) or 0.0) * 60.0,
            "plus_minus_output": float(getattr(row, "Plus_Minus_output", 0.0) or 0.0),
            "plus_minus_official": float(getattr(row, "Plus_Minus_official", 0.0) or 0.0),
            "plus_minus_tpdev_pbp": float(getattr(row, "Plus_Minus_tpdev_pbp", 0.0) or 0.0),
        }
    return mapping


@lru_cache(maxsize=None)
def _load_game_events(
    game_id: str,
    parquet_path: str,
    db_path: str,
    file_directory: str,
) -> tuple[list[object], list[dict[str, Any]]]:
    _, possessions, _ = _load_game_context(
        game_id,
        parquet_path=Path(parquet_path),
        db_path=Path(db_path),
        file_directory=Path(file_directory),
    )
    events = _collect_game_events(possessions)
    contradictions = collect_intraperiod_contradictions(events, game_id=game_id)
    return events, contradictions


def _evaluation_context(
    candidate_row: pd.Series,
    contradictions: list[dict[str, Any]],
    events: list[object],
) -> tuple[list[dict[str, Any]], int, int]:
    period = int(candidate_row["period"])
    team_id = int(candidate_row["team_id"])
    player_in_id = int(candidate_row["player_in_id"])
    evidence_event_nums = {
        int(value)
        for value in (_safe_literal(candidate_row.get("evidence_event_nums")) or [])
        if value is not None
    }
    group_rows = [
        row
        for row in contradictions
        if int(row["period"]) == period
        and int(row["team_id"]) == team_id
        and int(row["player_id"]) == player_in_id
        and (
            not evidence_event_nums
            or (row.get("event_num") is not None and int(row["event_num"]) in evidence_event_nums)
        )
    ]
    if not group_rows:
        return [], -1, -1
    group_rows = sorted(group_rows, key=lambda row: (int(row["event_index"]), int(row["event_num"] or 0)))
    start_index = int(group_rows[0]["event_index"])
    next_sub_index = _find_next_same_team_substitution_index(
        events,
        start_index=start_index,
        period=period,
        team_id=team_id,
    )
    group_end_limit = (
        next_sub_index - 1
        if next_sub_index is not None
        else _find_period_end_index(events, start_index, period)
    )
    return group_rows, start_index, group_end_limit


def _build_deadball_anchor(candidate_row: pd.Series, events: list[object], start_index: int) -> dict[str, Any]:
    period = int(candidate_row["period"])
    event_num_lookup: dict[tuple[int, int], int] = {}
    for index, event in enumerate(events):
        event_num = _event_number(event)
        if event_num is None:
            continue
        event_period = int(getattr(event, "period", 0) or 0)
        event_num_lookup.setdefault((event_period, event_num), index)

    start_event_num = candidate_row.get("deadball_window_start_event_num")
    end_event_num = candidate_row.get("deadball_window_end_event_num")
    anchor_event_num = candidate_row.get("deadball_event_num")
    if pd.notna(start_event_num) and pd.notna(end_event_num):
        window_start_index = event_num_lookup.get((period, int(start_event_num)))
        window_end_index = event_num_lookup.get((period, int(end_event_num)))
        if window_start_index is not None and window_end_index is not None:
            return {
                "anchor_index": int(window_start_index),
                "anchor_end_index": int(window_end_index),
                "anchor_event_num": int(anchor_event_num) if pd.notna(anchor_event_num) else _event_number(events[window_start_index]),
                "anchor_clock": str(getattr(events[window_start_index], "clock", "") or ""),
                "window_start_event_num": int(start_event_num),
                "window_end_event_num": int(end_event_num),
                "same_clock_as_first_contradiction": (
                    str(getattr(events[window_start_index], "clock", "") or "")
                    == str(getattr(events[start_index], "clock", "") or "")
                ),
            }

    window_start_index, window_end_index = _same_clock_window_bounds(events, start_index)
    return {
        "anchor_index": int(window_start_index),
        "anchor_end_index": int(window_end_index),
        "anchor_event_num": _event_number(events[window_start_index]),
        "anchor_clock": str(getattr(events[window_start_index], "clock", "") or ""),
        "window_start_event_num": _event_number(events[window_start_index]),
        "window_end_event_num": _event_number(events[window_end_index]),
        "same_clock_as_first_contradiction": True,
    }


def _cluster_events_payload(events: list[object], start_index: int, end_index: int) -> list[dict[str, Any]]:
    payload = []
    for event in events[start_index : end_index + 1]:
        payload.append(
            {
                "event_num": _event_number(event),
                "clock": str(getattr(event, "clock", "") or ""),
                "event_class": event.__class__.__name__,
                "team_id": int(getattr(event, "team_id", 0) or 0),
                "incoming_player_id": getattr(event, "incoming_player_id", None),
                "outgoing_player_id": getattr(event, "outgoing_player_id", None),
                "player1_id": getattr(event, "player1_id", None),
                "player2_id": getattr(event, "player2_id", None),
                "player3_id": getattr(event, "player3_id", None),
                "description": _event_description(event),
            }
        )
    return payload


def _tpdev_alignment_delta(
    *,
    block_key: str,
    game_id: str,
    player_in_id: int,
    player_out_id: int,
    window_seconds: float,
    cross_source_map: dict[tuple[str, str, int], dict[str, float]],
) -> float | None:
    if player_out_id <= 0 or window_seconds <= 0:
        return None
    in_key = (block_key, game_id, int(player_in_id))
    out_key = (block_key, game_id, int(player_out_id))
    if in_key not in cross_source_map or out_key not in cross_source_map:
        return None
    player_in = cross_source_map[in_key]
    player_out = cross_source_map[out_key]
    if player_in["tpdev_pbp_seconds"] <= 0 and player_out["tpdev_pbp_seconds"] <= 0:
        return None
    current_gap = abs(player_in["output_seconds"] - player_in["tpdev_pbp_seconds"]) + abs(
        player_out["output_seconds"] - player_out["tpdev_pbp_seconds"]
    )
    projected_gap = abs(
        (player_in["output_seconds"] + window_seconds) - player_in["tpdev_pbp_seconds"]
    ) + abs(
        (player_out["output_seconds"] - window_seconds) - player_out["tpdev_pbp_seconds"]
    )
    return float(current_gap - projected_gap)


def _evaluate_same_clock_alternatives(
    candidate_row: pd.Series,
    *,
    events: list[object],
    contradictions: list[dict[str, Any]],
    cross_source_map: dict[tuple[str, str, int], dict[str, float]],
    block_key: str,
) -> dict[str, Any]:
    group_rows, start_index, group_end_limit = _evaluation_context(candidate_row, contradictions, events)
    if not group_rows:
        return {}
    player_in_id = int(candidate_row["player_in_id"])
    player_out_id = int(candidate_row.get("player_out_id") or 0)
    team_id = int(candidate_row["team_id"])
    period = int(candidate_row["period"])
    if player_out_id <= 0:
        return {}

    deadball_anchor = _build_deadball_anchor(candidate_row, events, start_index)
    contradiction_end_index = int(group_rows[-1]["event_index"])
    substitution_context = _build_substitution_context(
        contradictions,
        player_in_id=player_in_id,
        period=period,
        team_id=team_id,
        window_start_index=int(deadball_anchor["anchor_index"]),
        window_end_index=group_end_limit,
        current_team_lineup=list(group_rows[0]["current_team_lineup"]),
    )
    period_repeat_contradiction_support = int(
        sum(
            1
            for row in contradictions
            if int(row["period"]) == period
            and int(row["team_id"]) == team_id
            and int(row["player_id"]) == player_in_id
            and row["status"] in {"off_court_event_credit", "same_clock_boundary_conflict"}
        )
        >= 2
    )

    evaluations: dict[str, Any] = {}
    apply_positions = [
        ("window_start", int(deadball_anchor["anchor_index"])),
    ]
    after_window_index = int(deadball_anchor["anchor_end_index"]) + 1
    if after_window_index <= group_end_limit:
        apply_positions.append(("after_window_end", after_window_index))

    for apply_name, apply_start_index in apply_positions:
        evaluation = _evaluate_player_out_candidate(
            events,
            period=period,
            team_id=team_id,
            player_in_id=player_in_id,
            player_out_id=player_out_id,
            group_rows=group_rows,
            start_index=start_index,
            apply_start_index=apply_start_index,
            contradiction_end_index=contradiction_end_index,
            evaluation_end_index=group_end_limit,
            deadball_anchor=deadball_anchor,
            deadball_apply_position=apply_name,
            substitution_context=substitution_context,
            period_repeat_contradiction_support=period_repeat_contradiction_support,
        )
        window_seconds = _approx_window_seconds(events, apply_start_index, group_end_limit)
        plus_minus_delta = _score_delta_for_window(events, apply_start_index, group_end_limit, team_id)
        evaluation_payload = {
            "apply_position": apply_name,
            "apply_start_event_num": _event_number(events[apply_start_index]) if apply_start_index < len(events) else None,
            "apply_start_clock": str(getattr(events[apply_start_index], "clock", "") or "") if apply_start_index < len(events) else "",
            "evaluation_end_event_num": _event_number(events[group_end_limit]) if group_end_limit < len(events) else None,
            "evaluation_end_clock": str(getattr(events[group_end_limit], "clock", "") or "") if group_end_limit < len(events) else "",
            "window_seconds": window_seconds,
            "contradictions_removed": int(evaluation["contradictions_removed"]),
            "new_contradictions_introduced": int(evaluation["new_contradictions_introduced"]),
            "forward_simulation_contradiction_delta": int(
                evaluation["contradictions_removed"] - evaluation["new_contradictions_introduced"]
            ),
            "local_confidence_score": int(evaluation["local_confidence_score"]),
            "player_out_silence_support": int(evaluation["player_out_silence_support"]),
            "same_clock_cluster_consistency": int(evaluation["same_clock_cluster_consistency"]),
            "lineup_size_consistency": int(evaluation["lineup_size_consistency"]),
            "plus_minus_delta": int(plus_minus_delta),
            "minutes_delta_seconds": {
                str(player_in_id): float(window_seconds),
                str(player_out_id): float(-window_seconds),
            },
            "plus_minus_delta_points": {
                str(player_in_id): int(plus_minus_delta),
                str(player_out_id): int(-plus_minus_delta),
            },
            "tpdev_pbp_alignment_delta_seconds": _tpdev_alignment_delta(
                block_key=block_key,
                game_id=_normalize_game_id(candidate_row["game_id"]),
                player_in_id=player_in_id,
                player_out_id=player_out_id,
                window_seconds=window_seconds,
                cross_source_map=cross_source_map,
            ),
        }
        evaluations[apply_name] = evaluation_payload
    return evaluations


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build a same-clock attribution register from intraperiod proving outputs."
    )
    parser.add_argument("--loop-output-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--family-register-dir", type=Path)
    parser.add_argument("--parquet-path", type=Path, default=DEFAULT_PARQUET)
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB)
    parser.add_argument("--file-directory", type=Path, default=DEFAULT_FILE_DIRECTORY)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    loop_output_dir = args.loop_output_dir.resolve()
    output_dir = args.output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    family_register_dir = (
        args.family_register_dir.resolve()
        if args.family_register_dir is not None
        else loop_output_dir / "family_register"
    )

    family_register_path = family_register_dir / "intraperiod_family_register.csv"
    family_df = pd.read_csv(family_register_path) if family_register_path.exists() else pd.DataFrame()
    if family_df.empty:
        empty_df = pd.DataFrame()
        empty_df.to_csv(output_dir / "same_clock_attribution_register.csv", index=False)
        summary = {"rows": 0, "family_counts": {}, "top_games": []}
        (output_dir / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
        print(json.dumps(summary, indent=2))
        return 0
    family_df = family_df.copy()
    family_df["game_id"] = family_df["game_id"].map(_normalize_game_id)
    family_df["period"] = pd.to_numeric(family_df["period"], errors="coerce").fillna(0).astype(int)
    family_df["team_id"] = pd.to_numeric(family_df["team_id"], errors="coerce").fillna(0).astype(int)
    family_df["player_in_id"] = (
        pd.to_numeric(family_df["player_in_id"], errors="coerce").fillna(0).astype(int)
    )
    family_df["player_out_id"] = (
        pd.to_numeric(family_df["player_out_id"], errors="coerce").fillna(0).astype(int)
    )

    family_df = family_df.loc[family_df.apply(_should_include_same_clock_row, axis=1)].copy()
    cross_source_map = _build_cross_source_map(_load_cross_source_rows(loop_output_dir))

    candidate_frames: list[pd.DataFrame] = []
    for csv_path in sorted(loop_output_dir.glob("blocks/*/candidates/intraperiod_missing_sub_candidates.csv")):
        df = pd.read_csv(csv_path)
        if df.empty:
            continue
        df = df.copy()
        df["block_key"] = csv_path.parent.parent.name
        df["game_id"] = df["game_id"].map(_normalize_game_id)
        candidate_frames.append(df)
    candidate_df = pd.concat(candidate_frames, ignore_index=True) if candidate_frames else pd.DataFrame()

    join_cols = ["block_key", "game_id", "period", "team_id", "player_in_id", "player_out_id"]
    if not candidate_df.empty:
        candidate_df["period"] = pd.to_numeric(candidate_df["period"], errors="coerce").fillna(0).astype(int)
        candidate_df["team_id"] = pd.to_numeric(candidate_df["team_id"], errors="coerce").fillna(0).astype(int)
        candidate_df["player_in_id"] = pd.to_numeric(candidate_df["player_in_id"], errors="coerce").fillna(0).astype(int)
        candidate_df["player_out_id"] = pd.to_numeric(candidate_df["player_out_id"], errors="coerce").fillna(0).astype(int)
        family_df["player_out_id"] = pd.to_numeric(family_df["player_out_id"], errors="coerce").fillna(0).astype(int)
        family_df = family_df.merge(
            candidate_df[
                join_cols
                + [
                    "deadball_event_num",
                    "deadball_clock",
                    "deadball_window_start_event_num",
                    "deadball_window_end_event_num",
                    "deadball_apply_position",
                    "deadball_choice_kind",
                    "first_contradicted_event_num",
                    "last_contradicted_event_num",
                    "start_event_num",
                    "end_event_num",
                ]
            ],
            on=join_cols,
            how="left",
            suffixes=("", "_candidate"),
        )

    rows: list[dict[str, Any]] = []
    for record in family_df.to_dict(orient="records"):
        block_key = str(record["block_key"])
        game_id = _normalize_game_id(record["game_id"])
        period = int(record["period"])
        team_id = int(record["team_id"])
        player_in_id = int(record["player_in_id"])
        player_out_id = int(record.get("player_out_id") or 0)

        events, contradictions = _load_game_events(
            game_id,
            str(args.parquet_path.resolve()),
            str(args.db_path.resolve()),
            str(args.file_directory.resolve()),
        )
        group_rows, start_index, _ = _evaluation_context(pd.Series(record), contradictions, events)
        if not group_rows:
            continue
        deadball_anchor = _build_deadball_anchor(pd.Series(record), events, start_index)
        cluster_start = int(deadball_anchor["anchor_index"])
        cluster_end = int(deadball_anchor["anchor_end_index"])
        cluster_events = events[cluster_start : cluster_end + 1]

        same_clock_family = _same_clock_family(cluster_events)
        current_outcome = {
            "current_team_lineup": list(group_rows[0]["current_team_lineup"]),
            "previous_team_lineup": list(group_rows[0]["previous_team_lineup"]),
            "contradiction_status_counts": _safe_literal(record.get("contradiction_status_counts")) or {},
            "evidence_event_nums": _safe_literal(record.get("evidence_event_nums")) or [],
            "deadball_choice_kind": str(record.get("deadball_choice_kind") or ""),
            "deadball_apply_position": str(record.get("deadball_apply_position") or ""),
        }
        alternative_orderings = _evaluate_same_clock_alternatives(
            pd.Series(record),
            events=events,
            contradictions=contradictions,
            cross_source_map=cross_source_map,
            block_key=block_key,
        )

        rows.append(
            {
                "block_key": block_key,
                "game_id": game_id,
                "period": period,
                "team_id": team_id,
                "player_in_id": player_in_id,
                "player_out_id": player_out_id,
                "source_family": str(record.get("family") or ""),
                "same_clock_family": same_clock_family,
                "manual_review_bucket": _as_bool(record.get("manual_review_bucket")),
                "is_known_negative_tripwire": _as_bool(record.get("is_known_negative_tripwire")),
                "is_reviewed_manual_reject": _as_bool(record.get("is_reviewed_manual_reject")),
                "manual_review_disposition": str(record.get("manual_review_disposition") or ""),
                "manual_review_recommended_next_track": str(
                    record.get("manual_review_recommended_next_track") or ""
                ),
                "manifest_target_type": str(record.get("manifest_target_type") or ""),
                "cluster_clock": str(getattr(events[cluster_start], "clock", "") or ""),
                "cluster_start_event_num": deadball_anchor["window_start_event_num"],
                "cluster_end_event_num": deadball_anchor["window_end_event_num"],
                "pre_cluster_lineup_json": json.dumps(_lineup_for_team(events[cluster_start], team_id, previous=True)),
                "post_cluster_lineup_json": json.dumps(_lineup_for_team(events[cluster_end], team_id)),
                "cluster_event_classes_json": json.dumps(
                    [event.__class__.__name__ for event in cluster_events]
                ),
                "cluster_events_json": json.dumps(_cluster_events_payload(events, cluster_start, cluster_end)),
                "current_parser_ordering_outcome_json": json.dumps(current_outcome, sort_keys=True),
                "alternative_local_orderings_json": json.dumps(alternative_orderings, sort_keys=True),
                "game_max_minutes_abs_diff": float(record.get("game_max_minutes_abs_diff") or 0.0),
                "game_event_issue_rows": _as_int(record.get("game_event_issue_rows")),
                "game_plus_minus_mismatch_rows": _as_int(record.get("game_plus_minus_mismatch_rows")),
                "local_confidence_score": float(record.get("local_confidence_score") or 0.0),
                "confidence_gap": float(record.get("confidence_gap") or 0.0),
            }
        )

    register_df = pd.DataFrame(rows).sort_values(
        [
            "is_known_negative_tripwire",
            "is_reviewed_manual_reject",
            "local_confidence_score",
            "game_max_minutes_abs_diff",
            "game_event_issue_rows",
        ],
        ascending=[True, True, False, False, False],
    )
    register_df.to_csv(output_dir / "same_clock_attribution_register.csv", index=False)

    unreviewed_positive_df = register_df[
        (~register_df["is_known_negative_tripwire"])
        & (~register_df["is_reviewed_manual_reject"])
    ].head(25)
    unreviewed_positive_df.to_csv(
        output_dir / "same_clock_positive_shortlist.csv",
        index=False,
    )

    family_counts = (
        register_df["same_clock_family"].value_counts().sort_index().to_dict()
        if not register_df.empty
        else {}
    )
    source_family_counts = (
        register_df["source_family"].value_counts().sort_index().to_dict()
        if not register_df.empty
        else {}
    )
    summary = {
        "rows": int(len(register_df)),
        "same_clock_family_counts": family_counts,
        "source_family_counts": source_family_counts,
        "reviewed_manual_reject_rows": int(register_df["is_reviewed_manual_reject"].sum()),
        "top_games": register_df[
            [
                "block_key",
                "game_id",
                "period",
                "team_id",
                "same_clock_family",
                "source_family",
                "local_confidence_score",
                "game_max_minutes_abs_diff",
                "game_event_issue_rows",
                "is_known_negative_tripwire",
            ]
        ]
        .head(25)
        .to_dict(orient="records"),
        "top_unreviewed_games": unreviewed_positive_df[
            [
                "block_key",
                "game_id",
                "period",
                "team_id",
                "same_clock_family",
                "source_family",
                "local_confidence_score",
                "game_max_minutes_abs_diff",
                "game_event_issue_rows",
            ]
        ].to_dict(orient="records"),
    }
    (output_dir / "summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
