from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import pandas as pd

from historic_backfill.audits.cross_source.period_starters import DEFAULT_DB_PATH, DEFAULT_PARQUET_PATH, _normalize_game_id
from historic_backfill.runners.cautious_rerun import DEFAULT_FILE_DIRECTORY
from historic_backfill.audits.cross_source.trace_player_stints_game import (
    _collect_game_events,
    _load_game_context,
    build_player_stint_trace,
)


ROOT = Path(__file__).resolve().parent
DEFAULT_EVENT_ON_COURT_DIR = (
    ROOT / "intraperiod_proving_1998_2020_20260319_v2" / "event_on_court_family_register"
)
DEFAULT_SAME_CLOCK_DIR = (
    ROOT / "intraperiod_proving_1998_2020_20260319_v2" / "same_clock_attribution"
)


def _as_int(value: Any, default: int = 0) -> int:
    try:
        if pd.isna(value):
            return default
    except Exception:
        pass
    try:
        return int(float(value))
    except Exception:
        return default


def _load_csv(path: Path) -> pd.DataFrame:
    return pd.read_csv(path) if path.exists() else pd.DataFrame()


def _parse_case(value: str) -> tuple[str, int, int]:
    parts = str(value).split(":")
    if len(parts) != 3:
        raise ValueError(f"Case must be game_id:period:team_id, got {value!r}")
    return _normalize_game_id(parts[0]), _as_int(parts[1]), _as_int(parts[2])


def _load_selected_cases(args: argparse.Namespace) -> list[tuple[str, int, int]]:
    cases: list[tuple[str, int, int]] = []
    if args.case:
        cases.extend(_parse_case(value) for value in args.case)
    if args.queue_dir is not None:
        queue_df = _load_csv(args.queue_dir.resolve() / "same_clock_boundary_queue.csv")
        if not queue_df.empty:
            if args.family:
                queue_df = queue_df.loc[queue_df["same_clock_family"].astype(str) == str(args.family)].copy()
            queue_df = queue_df.sort_values(
                ["same_clock_family", "family_rank", "season", "game_id", "period", "team_id"]
            )
            if int(args.max_cases) > 0:
                queue_df = queue_df.head(int(args.max_cases))
            cases.extend(
                (
                    _normalize_game_id(row["game_id"]),
                    _as_int(row["period"]),
                    _as_int(row["team_id"]),
                )
                for _, row in queue_df.iterrows()
            )
    deduped: list[tuple[str, int, int]] = []
    seen = set()
    for case in cases:
        if case in seen:
            continue
        seen.add(case)
        deduped.append(case)
    return deduped


def _json_load(text: Any) -> Any:
    if not isinstance(text, str) or text == "":
        return text
    try:
        return json.loads(text)
    except Exception:
        return text


def _serialize_event(event: object) -> dict[str, Any]:
    return {
        "event_num": _as_int(getattr(event, "event_num", 0)),
        "period": _as_int(getattr(event, "period", 0)),
        "clock": str(getattr(event, "clock", "") or ""),
        "event_class": event.__class__.__name__,
        "team_id": _as_int(getattr(event, "team_id", 0)),
        "player1_id": _as_int(getattr(event, "player1_id", 0)),
        "player2_id": _as_int(getattr(event, "player2_id", 0)),
        "player3_id": _as_int(getattr(event, "player3_id", 0)),
        "description": str(getattr(event, "description", "") or ""),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build focused inspection artifacts for same-clock boundary cases."
    )
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--queue-dir", type=Path)
    parser.add_argument("--family")
    parser.add_argument("--max-cases", type=int, default=0)
    parser.add_argument("--case", action="append", help="game_id:period:team_id; may be passed multiple times")
    parser.add_argument("--same-clock-register-dir", type=Path, default=DEFAULT_SAME_CLOCK_DIR)
    parser.add_argument(
        "--event-on-court-family-register-dir", type=Path, default=DEFAULT_EVENT_ON_COURT_DIR
    )
    parser.add_argument("--parquet-path", type=Path, default=DEFAULT_PARQUET_PATH)
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB_PATH)
    parser.add_argument("--file-directory", type=Path, default=DEFAULT_FILE_DIRECTORY)
    parser.add_argument("--neighbor-window", type=int, default=3)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    output_dir = args.output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    cases = _load_selected_cases(args)
    if not cases:
        summary = {"cases": 0, "case_keys": []}
        (output_dir / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
        print(json.dumps(summary, indent=2))
        return 0

    same_clock_df = _load_csv(args.same_clock_register_dir.resolve() / "same_clock_attribution_register.csv")
    event_df = _load_csv(
        args.event_on_court_family_register_dir.resolve() / "event_on_court_family_register.csv"
    )
    if not same_clock_df.empty:
        same_clock_df = same_clock_df.copy()
        same_clock_df["game_id"] = same_clock_df["game_id"].map(_normalize_game_id)
        same_clock_df["period"] = pd.to_numeric(same_clock_df["period"], errors="coerce").fillna(0).astype(int)
        same_clock_df["team_id"] = pd.to_numeric(same_clock_df["team_id"], errors="coerce").fillna(0).astype(int)
    if not event_df.empty:
        event_df = event_df.copy()
        event_df["game_id"] = event_df["game_id"].map(_normalize_game_id)
        event_df["period"] = pd.to_numeric(event_df["period"], errors="coerce").fillna(0).astype(int)
        event_df["team_id"] = pd.to_numeric(event_df["team_id"], errors="coerce").fillna(0).astype(int)

    trace_cache: dict[str, tuple[pd.DataFrame, pd.DataFrame, dict[str, Any], list[object]]] = {}
    summaries: list[dict[str, Any]] = []
    for game_id, period, team_id in cases:
        if game_id not in trace_cache:
            stints_df, recon_df, trace_summary = build_player_stint_trace(
                game_id=game_id,
                parquet_path=args.parquet_path.resolve(),
                db_path=args.db_path.resolve(),
                file_directory=args.file_directory.resolve(),
            )
            _, possessions, _ = _load_game_context(
                game_id=game_id,
                parquet_path=args.parquet_path.resolve(),
                db_path=args.db_path.resolve(),
                file_directory=args.file_directory.resolve(),
            )
            trace_cache[game_id] = (
                stints_df,
                recon_df,
                trace_summary,
                _collect_game_events(possessions),
            )

        stints_df, recon_df, trace_summary, events = trace_cache[game_id]
        same_clock_rows = same_clock_df.loc[
            (same_clock_df["game_id"] == game_id)
            & (same_clock_df["period"] == period)
            & (same_clock_df["team_id"] == team_id)
        ].copy()
        event_rows = event_df.loc[
            (event_df["game_id"] == game_id)
            & (event_df["period"] == period)
            & (event_df["team_id"] == team_id)
        ].copy()

        case_dir = output_dir / f"{game_id}_P{period}_T{team_id}"
        case_dir.mkdir(parents=True, exist_ok=True)
        event_rows.to_csv(case_dir / "issue_rows.csv", index=False)
        if not same_clock_rows.empty:
            same_clock_rows.to_csv(case_dir / "same_clock_register_rows.csv", index=False)

        focus_player_ids = sorted(
            {
                _as_int(value)
                for value in pd.concat(
                    [
                        event_rows.get("player_id", pd.Series(dtype=float)),
                        same_clock_rows.get("player_in_id", pd.Series(dtype=float)),
                        same_clock_rows.get("player_out_id", pd.Series(dtype=float)),
                    ],
                    ignore_index=True,
                ).tolist()
                if _as_int(value) > 0
            }
        )
        recon_focus = recon_df.loc[
            (pd.to_numeric(recon_df.get("team_id"), errors="coerce").fillna(0).astype(int) == team_id)
            | (pd.to_numeric(recon_df.get("player_id"), errors="coerce").fillna(0).astype(int).isin(focus_player_ids))
        ].copy()
        stints_focus = stints_df.loc[
            (pd.to_numeric(stints_df.get("team_id"), errors="coerce").fillna(0).astype(int) == team_id)
            | (pd.to_numeric(stints_df.get("player_id"), errors="coerce").fillna(0).astype(int).isin(focus_player_ids))
        ].copy()
        recon_focus.to_csv(case_dir / "player_minutes_recon_focus.csv", index=False)
        stints_focus.to_csv(case_dir / "player_stints_focus.csv", index=False)

        cluster_events = []
        neighboring_events = []
        if not same_clock_rows.empty:
            row = same_clock_rows.iloc[0].to_dict()
            cluster_events = _json_load(row.get("cluster_events_json")) or []
            evidence_event_nums = set(
                _as_int(event_num)
                for event_num in (_json_load(row.get("current_parser_ordering_outcome_json")) or {}).get(
                    "evidence_event_nums", []
                )
            )
            cluster_start = _as_int(row.get("cluster_start_event_num"))
            cluster_end = _as_int(row.get("cluster_end_event_num"))
            anchor_event_nums = {value for value in [cluster_start, cluster_end] if value > 0} | evidence_event_nums
            event_index = {
                _as_int(getattr(event, "event_num", 0)): index
                for index, event in enumerate(events)
                if _as_int(getattr(event, "event_num", 0)) > 0
            }
            neighbor_indexes = set()
            for event_num in anchor_event_nums:
                if event_num not in event_index:
                    continue
                idx = event_index[event_num]
                for neighbor in range(max(0, idx - args.neighbor_window), min(len(events), idx + args.neighbor_window + 1)):
                    neighbor_indexes.add(neighbor)
            neighboring_events = [_serialize_event(events[idx]) for idx in sorted(neighbor_indexes)]
            (case_dir / "cluster_events.json").write_text(
                json.dumps(cluster_events, indent=2),
                encoding="utf-8",
            )
            (case_dir / "neighboring_events.json").write_text(
                json.dumps(neighboring_events, indent=2),
                encoding="utf-8",
            )

        case_summary = {
            "game_id": game_id,
            "period": period,
            "team_id": team_id,
            "focus_player_ids": focus_player_ids,
            "trace_summary": trace_summary,
            "same_clock_rows": int(len(same_clock_rows)),
            "event_issue_rows": int(len(event_rows)),
            "cluster_event_count": int(len(cluster_events)),
            "neighboring_event_count": int(len(neighboring_events)),
        }
        (case_dir / "summary.json").write_text(json.dumps(case_summary, indent=2), encoding="utf-8")
        summaries.append(case_summary)

    pd.DataFrame(summaries).to_csv(output_dir / "summary.csv", index=False)
    (output_dir / "summary.json").write_text(json.dumps(summaries, indent=2), encoding="utf-8")
    print(json.dumps(summaries, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
