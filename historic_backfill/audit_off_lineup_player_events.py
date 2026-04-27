from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List

import pandas as pd

from audit_period_starters_against_tpdev import DEFAULT_DB_PATH, DEFAULT_PARQUET_PATH, _normalize_game_id
from trace_player_stints_game import _collect_game_events, _load_game_context, _normalize_lineups


IGNORED_EVENT_CLASSES = {"StatsJumpBall", "StatsSubstitution"}


def _serialize_lineups(current_players: Dict[int, List[int]], name_map: Dict[int, str]) -> str:
    rows: List[Dict[str, Any]] = []
    for team_id, player_ids in sorted(current_players.items()):
        rows.append(
            {
                "team_id": int(team_id),
                "players": [
                    {"player_id": int(player_id), "player_name": name_map.get(int(player_id), str(player_id))}
                    for player_id in player_ids
                ],
            }
        )
    return json.dumps(rows, ensure_ascii=False)


def _nearest_substitution_context(
    events: List[object],
    event_index: int,
    player_id: int,
    period: int,
) -> Dict[str, Any]:
    context: Dict[str, Any] = {
        "prev_sub_in_event_num": None,
        "prev_sub_in_clock": "",
        "prev_sub_out_event_num": None,
        "prev_sub_out_clock": "",
        "next_sub_in_event_num": None,
        "next_sub_in_clock": "",
        "next_sub_out_event_num": None,
        "next_sub_out_clock": "",
    }

    for prior_index in range(event_index - 1, -1, -1):
        event = events[prior_index]
        if int(getattr(event, "period", 0) or 0) != int(period):
            continue
        if event.__class__.__name__ != "StatsSubstitution":
            continue
        outgoing_player_id = int(getattr(event, "player1_id", 0) or 0)
        incoming_player_id = int(getattr(event, "player2_id", 0) or 0)
        if context["prev_sub_out_event_num"] is None and outgoing_player_id == player_id:
            context["prev_sub_out_event_num"] = int(getattr(event, "event_num", 0) or 0)
            context["prev_sub_out_clock"] = str(getattr(event, "clock", "") or "")
        if context["prev_sub_in_event_num"] is None and incoming_player_id == player_id:
            context["prev_sub_in_event_num"] = int(getattr(event, "event_num", 0) or 0)
            context["prev_sub_in_clock"] = str(getattr(event, "clock", "") or "")
        if context["prev_sub_out_event_num"] is not None and context["prev_sub_in_event_num"] is not None:
            break

    for next_index in range(event_index + 1, len(events)):
        event = events[next_index]
        if int(getattr(event, "period", 0) or 0) != int(period):
            continue
        if event.__class__.__name__ != "StatsSubstitution":
            continue
        outgoing_player_id = int(getattr(event, "player1_id", 0) or 0)
        incoming_player_id = int(getattr(event, "player2_id", 0) or 0)
        if context["next_sub_out_event_num"] is None and outgoing_player_id == player_id:
            context["next_sub_out_event_num"] = int(getattr(event, "event_num", 0) or 0)
            context["next_sub_out_clock"] = str(getattr(event, "clock", "") or "")
        if context["next_sub_in_event_num"] is None and incoming_player_id == player_id:
            context["next_sub_in_event_num"] = int(getattr(event, "event_num", 0) or 0)
            context["next_sub_in_clock"] = str(getattr(event, "clock", "") or "")
        if context["next_sub_out_event_num"] is not None and context["next_sub_in_event_num"] is not None:
            break

    return context


def _find_off_lineup_player_rows(
    game_id: str | int,
    events: List[object],
    name_map: Dict[int, str],
) -> List[Dict[str, Any]]:
    normalized_game_id = _normalize_game_id(game_id)
    rows: List[Dict[str, Any]] = []

    for event_index, event in enumerate(events):
        event_class = event.__class__.__name__
        if event_class in IGNORED_EVENT_CLASSES:
            continue

        current_players = _normalize_lineups(getattr(event, "current_players", {}))
        if len(current_players) < 2:
            continue

        all_current_players = {
            int(player_id)
            for player_ids in current_players.values()
            for player_id in player_ids
        }
        if not all_current_players:
            continue

        for player_attr in ("player1_id", "player2_id", "player3_id"):
            player_id = int(getattr(event, player_attr, 0) or 0)
            if player_id <= 0 or player_id in all_current_players:
                continue
            sub_context = _nearest_substitution_context(
                events,
                event_index,
                player_id,
                int(getattr(event, "period", 0) or 0),
            )

            rows.append(
                {
                    "game_id": normalized_game_id,
                    "period": int(getattr(event, "period", 0) or 0),
                    "clock": str(getattr(event, "clock", "") or ""),
                    "event_num": int(getattr(event, "event_num", 0) or 0),
                    "event_class": event_class,
                    "team_id": int(getattr(event, "team_id", 0) or 0),
                    "player_attr": player_attr,
                    "player_id": player_id,
                    "player_name": name_map.get(player_id, str(player_id)),
                    "description": str(getattr(event, "description", "") or ""),
                    "current_lineups_json": _serialize_lineups(current_players, name_map),
                    **sub_context,
                }
            )

    return rows


def build_off_lineup_player_event_audit(
    game_ids: Iterable[str | int],
    parquet_path: Path = DEFAULT_PARQUET_PATH,
    db_path: Path = DEFAULT_DB_PATH,
) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []

    for raw_game_id in game_ids:
        game_id = _normalize_game_id(raw_game_id)
        _, possessions, name_map = _load_game_context(
            game_id,
            parquet_path=parquet_path,
            db_path=db_path,
        )
        events = _collect_game_events(possessions)
        rows.extend(_find_off_lineup_player_rows(game_id, events, name_map))

    if not rows:
        return pd.DataFrame(
            columns=[
                "game_id",
                "period",
                "clock",
                "event_num",
                "event_class",
                "team_id",
                "player_attr",
                "player_id",
                "player_name",
                "description",
                "current_lineups_json",
                "prev_sub_in_event_num",
                "prev_sub_in_clock",
                "prev_sub_out_event_num",
                "prev_sub_out_clock",
                "next_sub_in_event_num",
                "next_sub_in_clock",
                "next_sub_out_event_num",
                "next_sub_out_clock",
            ]
        )

    return pd.DataFrame(rows).sort_values(
        ["game_id", "period", "clock", "event_num", "player_attr"],
        ascending=[True, True, False, True, True],
    ).reset_index(drop=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Audit live events that reference players missing from both current lineups."
    )
    parser.add_argument("--game-ids", nargs="+", required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--parquet-path", type=Path, default=DEFAULT_PARQUET_PATH)
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB_PATH)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    audit_df = build_off_lineup_player_event_audit(
        game_ids=args.game_ids,
        parquet_path=args.parquet_path,
        db_path=args.db_path,
    )
    audit_df.to_csv(args.output_dir / "off_lineup_player_events.csv", index=False)

    summary_by_game = (
        audit_df.groupby("game_id")
        .agg(
            off_lineup_rows=("event_num", "size"),
            distinct_events=("event_num", "nunique"),
            players_affected=("player_id", "nunique"),
        )
        .reset_index()
        if not audit_df.empty
        else pd.DataFrame(columns=["game_id", "off_lineup_rows", "distinct_events", "players_affected"])
    )
    summary_by_game.to_csv(args.output_dir / "summary_by_game.csv", index=False)

    summary = {
        "games": int(summary_by_game["game_id"].nunique()) if not summary_by_game.empty else 0,
        "off_lineup_rows": int(len(audit_df)),
        "distinct_events": int(audit_df["event_num"].nunique()) if not audit_df.empty else 0,
        "players_affected": int(audit_df["player_id"].nunique()) if not audit_df.empty else 0,
        "event_class_counts": audit_df["event_class"].value_counts().to_dict() if not audit_df.empty else {},
        "player_attr_counts": audit_df["player_attr"].value_counts().to_dict() if not audit_df.empty else {},
    }
    (args.output_dir / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
