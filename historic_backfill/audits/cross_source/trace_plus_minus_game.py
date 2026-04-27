from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

import pandas as pd

from historic_backfill.audits.core.minutes_plus_minus import load_official_boxscore_df
from historic_backfill.audits.cross_source.period_starters import (
    DEFAULT_DB_PATH,
    DEFAULT_PARQUET_PATH,
    _load_current_game_possessions,
    _normalize_game_id,
)
from historic_backfill.runners.cautious_rerun import (
    _ensure_local_pbpstats_importable,
    install_local_boxscore_wrapper,
    load_v9b_namespace,
)
from historic_backfill.catalogs.pbp_stat_overrides import apply_pbp_stat_overrides

_ensure_local_pbpstats_importable()

import pbpstats


ROOT = Path(__file__).resolve().parent


def _iter_linked_events(possessions) -> List[object]:
    if not getattr(possessions, "items", None):
        return []
    first_event = possessions.items[0].events[0]
    while getattr(first_event, "previous_event", None) is not None:
        first_event = first_event.previous_event

    events: List[object] = []
    seen_ids = set()
    event = first_event
    while event is not None and id(event) not in seen_ids:
        seen_ids.add(id(event))
        events.append(event)
        event = event.next_event
    return events


def _is_scoring_event(event: object) -> bool:
    return bool(
        (
            hasattr(event, "is_made")
            and getattr(event, "is_made", False)
            and event.__class__.__name__ in {"StatsFieldGoal", "StatsFreeThrow"}
        )
    )


def _event_for_plus_minus(event: object) -> object:
    if event.__class__.__name__ == "StatsFreeThrow":
        return getattr(event, "event_for_efficiency_stats", event)
    return event


def _pm_items_for_event(event: object) -> List[dict]:
    return [
        stat
        for stat in getattr(event, "event_stats", [])
        if stat.get("stat_key") == pbpstats.PLUS_MINUS_STRING
    ]


def _collect_same_clock_window(events: List[object], index: int) -> List[object]:
    event = events[index]
    period = getattr(event, "period", None)
    clock = getattr(event, "clock", None)

    start = index
    while start > 0:
        prev = events[start - 1]
        if getattr(prev, "period", None) != period or getattr(prev, "clock", None) != clock:
            break
        start -= 1

    end = index
    while end + 1 < len(events):
        nxt = events[end + 1]
        if getattr(nxt, "period", None) != period or getattr(nxt, "clock", None) != clock:
            break
        end += 1

    return events[start : end + 1]


def _name_list(player_ids: Iterable[int], name_map: Dict[int, str]) -> List[str]:
    return [name_map.get(int(player_id), str(player_id)) for player_id in player_ids]


def _serialize_lineups(lineups: Dict[int, List[int]], name_map: Dict[int, str]) -> Dict[str, dict]:
    serialized = {}
    for team_id, player_ids in (lineups or {}).items():
        team_key = str(int(team_id))
        clean_ids = [int(player_id) for player_id in player_ids]
        serialized[team_key] = {
            "ids": clean_ids,
            "names": _name_list(clean_ids, name_map),
        }
    return serialized


def _aggregate_plus_minus_from_player_stats(
    game_id: str, possessions, name_map: Dict[int, str]
) -> pd.DataFrame:
    adjusted_stats = apply_pbp_stat_overrides(game_id, possessions.player_stats)
    plus_minus_rows = [
        stat
        for stat in adjusted_stats
        if stat.get("stat_key") == pbpstats.PLUS_MINUS_STRING
    ]
    totals: Dict[Tuple[int, int], float] = {}
    for stat in plus_minus_rows:
        key = (int(stat["team_id"]), int(stat["player_id"]))
        totals[key] = totals.get(key, 0.0) + float(stat["stat_value"])

    rows = []
    for (team_id, player_id), plus_minus in sorted(totals.items()):
        rows.append(
            {
                "team_id": team_id,
                "player_id": player_id,
                "player_name": name_map.get(player_id, str(player_id)),
                "Plus_Minus_output": plus_minus,
            }
        )
    return pd.DataFrame(rows)


def build_plus_minus_trace(
    game_id: str | int,
    parquet_path: Path = DEFAULT_PARQUET_PATH,
    db_path: Path = DEFAULT_DB_PATH,
) -> Tuple[pd.DataFrame, pd.DataFrame, Dict[str, object]]:
    normalized_game_id = _normalize_game_id(game_id)
    namespace = load_v9b_namespace()
    install_local_boxscore_wrapper(namespace, db_path)
    possessions, name_map = _load_current_game_possessions(
        namespace, normalized_game_id, parquet_path
    )
    events = _iter_linked_events(possessions)

    output_pm = _aggregate_plus_minus_from_player_stats(
        normalized_game_id, possessions, name_map
    )
    official_pm = load_official_boxscore_df(db_path, normalized_game_id)[
        ["team_id", "player_id", "player_name", "Plus_Minus_official"]
    ].copy()
    merged_pm = output_pm.merge(
        official_pm,
        on=["team_id", "player_id"],
        how="outer",
        suffixes=("_output", "_official"),
    )
    merged_pm["player_name"] = (
        merged_pm.get("player_name_output")
        .fillna(merged_pm.get("player_name_official"))
        .fillna("")
        .astype(str)
    )
    merged_pm["Plus_Minus_output"] = pd.to_numeric(
        merged_pm.get("Plus_Minus_output", 0.0), errors="coerce"
    ).fillna(0.0)
    merged_pm["Plus_Minus_official"] = pd.to_numeric(
        merged_pm.get("Plus_Minus_official", 0.0), errors="coerce"
    ).fillna(0.0)
    merged_pm["Plus_Minus_diff"] = (
        merged_pm["Plus_Minus_output"] - merged_pm["Plus_Minus_official"]
    )
    merged_pm = merged_pm[
        ["team_id", "player_id", "player_name", "Plus_Minus_output", "Plus_Minus_official", "Plus_Minus_diff"]
    ].sort_values(["team_id", "player_id"]).reset_index(drop=True)

    mismatch_player_ids = set(
        merged_pm.loc[merged_pm["Plus_Minus_diff"] != 0, "player_id"].astype(int).tolist()
    )

    trace_rows = []
    for index, event in enumerate(events):
        if not _is_scoring_event(event):
            continue
        pm_items = _pm_items_for_event(event)
        if not pm_items:
            continue

        effective_event = _event_for_plus_minus(event)
        used_lineups = getattr(effective_event, "current_players", {}) or {}
        previous_lineups = getattr(getattr(effective_event, "previous_event", None), "current_players", {}) or {}
        same_clock_window = _collect_same_clock_window(events, index)
        window_events = [
            {
                "event_num": int(getattr(window_event, "event_num", 0) or 0),
                "class_name": window_event.__class__.__name__,
                "description": str(getattr(window_event, "description", "") or ""),
                "team_id": getattr(window_event, "team_id", None),
                "player1_id": getattr(window_event, "player1_id", None),
                "player1_name": name_map.get(
                    int(getattr(window_event, "player1_id", 0) or 0),
                    str(getattr(window_event, "player1_id", "") or ""),
                ),
            }
            for window_event in same_clock_window
        ]
        mismatch_players_on_floor = sorted(
            player_id
            for player_ids in used_lineups.values()
            for player_id in player_ids
            if int(player_id) in mismatch_player_ids
        )

        trace_rows.append(
            {
                "game_id": normalized_game_id,
                "period": int(getattr(event, "period", 0) or 0),
                "clock": str(getattr(event, "clock", "") or ""),
                "linked_order_index": index,
                "event_num": int(getattr(event, "event_num", 0) or 0),
                "class_name": event.__class__.__name__,
                "effective_event_num": int(getattr(effective_event, "event_num", 0) or 0),
                "scoring_team_id": int(getattr(event, "team_id", 0) or 0),
                "scoring_player_id": int(getattr(event, "player1_id", 0) or 0),
                "scoring_player_name": name_map.get(
                    int(getattr(event, "player1_id", 0) or 0),
                    str(getattr(event, "player1_id", "") or ""),
                ),
                "points": max(abs(float(stat["stat_value"])) for stat in pm_items),
                "description": str(getattr(event, "description", "") or ""),
                "has_same_clock_substitution": any(
                    window_event.__class__.__name__ == "StatsSubstitution"
                    for window_event in same_clock_window
                ),
                "same_clock_event_count": len(same_clock_window),
                "same_clock_window": json.dumps(window_events, ensure_ascii=False),
                "used_lineups": json.dumps(_serialize_lineups(used_lineups, name_map), ensure_ascii=False),
                "previous_lineups": json.dumps(_serialize_lineups(previous_lineups, name_map), ensure_ascii=False),
                "mismatch_players_on_floor_ids": json.dumps(mismatch_players_on_floor),
                "mismatch_players_on_floor_names": json.dumps(
                    _name_list(mismatch_players_on_floor, name_map), ensure_ascii=False
                ),
            }
        )

    trace_df = pd.DataFrame(trace_rows).sort_values(
        ["period", "linked_order_index", "event_num"]
    ).reset_index(drop=True)
    summary = {
        "game_id": normalized_game_id,
        "events": int(len(events)),
        "scoring_events": int(len(trace_df)),
        "plus_minus_mismatch_players": int((merged_pm["Plus_Minus_diff"] != 0).sum()),
        "plus_minus_mismatch_abs_total": float(merged_pm["Plus_Minus_diff"].abs().sum()),
        "same_clock_substitution_scoring_events": int(
            trace_df["has_same_clock_substitution"].sum()
        )
        if not trace_df.empty
        else 0,
    }
    return merged_pm, trace_df, summary


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Trace plus-minus attribution event-by-event for selected games."
    )
    parser.add_argument("--game-ids", nargs="+", required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--parquet-path", type=Path, default=DEFAULT_PARQUET_PATH)
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB_PATH)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    summaries = []
    for raw_game_id in args.game_ids:
        game_id = _normalize_game_id(raw_game_id)
        game_dir = args.output_dir / game_id
        game_dir.mkdir(parents=True, exist_ok=True)

        plus_minus_df, trace_df, summary = build_plus_minus_trace(
            game_id=game_id,
            parquet_path=args.parquet_path,
            db_path=args.db_path,
        )
        plus_minus_df.to_csv(game_dir / "plus_minus_players.csv", index=False)
        trace_df.to_csv(game_dir / "plus_minus_trace.csv", index=False)
        (game_dir / "summary.json").write_text(
            json.dumps(summary, indent=2), encoding="utf-8"
        )
        summaries.append(summary)

    combined_summary = pd.DataFrame(summaries).sort_values("game_id").reset_index(drop=True)
    combined_summary.to_csv(args.output_dir / "summary.csv", index=False)
    (args.output_dir / "summary.json").write_text(
        json.dumps(combined_summary.to_dict(orient="records"), indent=2),
        encoding="utf-8",
    )
    print(json.dumps(combined_summary.to_dict(orient="records"), indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
