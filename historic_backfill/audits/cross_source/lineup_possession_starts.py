from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple

import pandas as pd

from historic_backfill.audits.cross_source.period_starters import (
    DEFAULT_DB_PATH,
    DEFAULT_PARQUET_PATH,
    DEFAULT_TPDEV_PBP_PATH,
    _normalize_game_id,
    build_period_starter_audit,
)
from historic_backfill.audits.cross_source.trace_player_stints_game import (
    DEFAULT_TPDEV_BOX_PATH,
    DEFAULT_PBPSTATS_BOX_PATH,
    DEFAULT_BBR_DB_PATH,
    DEFAULT_PLAYER_CROSSWALK_PATH,
    SECONDS_MATCH_TOLERANCE,
    _build_player_minutes_recon,
    _build_player_stints,
    _build_starter_mismatch_maps,
    _collect_game_events,
    _count_same_clock_substitution_scoring_events,
    _load_game_context,
    _normalize_lineups,
    _parse_clock_seconds_remaining,
)


TPDEV_TEAM_ROW_COLUMNS = [
    "game_id",
    "period",
    "team_id",
    "team_side",
    "time_remaining_start",
    "time_remaining_end",
    "length_seconds",
    "event_id",
    "poss_string",
    "offense_team_id",
    "tpdev_lineup_ids",
]


def _format_clock(seconds_remaining: float) -> str:
    total_seconds = max(0, int(round(float(seconds_remaining))))
    minutes = total_seconds // 60
    seconds = total_seconds % 60
    return f"{minutes}:{seconds:02d}"


def _event_window(events: List[object], index: int) -> List[object]:
    event = events[index]
    period = int(getattr(event, "period", 0) or 0)
    clock = str(getattr(event, "clock", "") or "")

    start = index
    while start > 0:
        previous = events[start - 1]
        if int(getattr(previous, "period", 0) or 0) != period:
            break
        if str(getattr(previous, "clock", "") or "") != clock:
            break
        start -= 1

    end = index
    while end + 1 < len(events):
        nxt = events[end + 1]
        if int(getattr(nxt, "period", 0) or 0) != period:
            break
        if str(getattr(nxt, "clock", "") or "") != clock:
            break
        end += 1

    return events[start : end + 1]


def _serialize_lineup(player_ids: Iterable[int], name_map: Dict[int, str]) -> str:
    return json.dumps(
        [{"player_id": int(player_id), "player_name": name_map.get(int(player_id), str(player_id))} for player_id in player_ids],
        ensure_ascii=False,
    )


def _load_tpdev_team_rows(tpdev_pbp_path: Path, game_id: str | int) -> pd.DataFrame:
    game_int = int(_normalize_game_id(game_id))
    df = pd.read_parquet(
        tpdev_pbp_path,
        filters=[("game_id", "==", game_int)],
        columns=[
            "game_id",
            "Quarter",
            "TimeRemainingStart",
            "LengthInSeconds",
            "event_id",
            "PossString",
            "offenseTeamId1",
            "h_tm_id",
            "v_tm_id",
            "h1",
            "h2",
            "h3",
            "h4",
            "h5",
            "v1",
            "v2",
            "v3",
            "v4",
            "v5",
        ],
    )
    if df.empty:
        return pd.DataFrame(columns=TPDEV_TEAM_ROW_COLUMNS)

    rows: List[Dict[str, Any]] = []
    for row in df.to_dict(orient="records"):
        game_id_text = _normalize_game_id(row["game_id"])
        period = int(row["Quarter"])
        time_remaining_start = float(row["TimeRemainingStart"])
        length_seconds = float(row["LengthInSeconds"])
        time_remaining_end = max(0.0, time_remaining_start - length_seconds)
        event_id = int(row["event_id"])
        poss_string = str(row.get("PossString") or "")
        offense_team_id = int(float(row["offenseTeamId1"])) if pd.notna(row.get("offenseTeamId1")) else 0

        for team_side, team_prefix, team_key in [("home", "h", "h_tm_id"), ("away", "v", "v_tm_id")]:
            team_id = int(float(row[team_key]))
            lineup_ids = [
                int(float(row[f"{team_prefix}{slot}"]))
                for slot in range(1, 6)
                if pd.notna(row.get(f"{team_prefix}{slot}"))
            ]
            rows.append(
                {
                    "game_id": game_id_text,
                    "period": period,
                    "team_id": team_id,
                    "team_side": team_side,
                    "time_remaining_start": time_remaining_start,
                    "time_remaining_end": time_remaining_end,
                    "length_seconds": length_seconds,
                    "event_id": event_id,
                    "poss_string": poss_string,
                    "offense_team_id": offense_team_id,
                    "tpdev_lineup_ids": lineup_ids,
                }
            )

    team_rows = pd.DataFrame(rows, columns=TPDEV_TEAM_ROW_COLUMNS)
    return team_rows.sort_values(
        ["period", "time_remaining_start", "event_id", "team_side"],
        ascending=[True, False, True, True],
    ).reset_index(drop=True)


def _parser_lineup_at_possession_start(
    events: List[object],
    period: int,
    time_remaining_start: float,
    team_id: int,
) -> Tuple[List[int], str, object | None, List[object]]:
    exact_candidates: List[Tuple[int, object, Dict[int, List[int]]]] = []
    prior_candidates: List[Tuple[int, object, Dict[int, List[int]]]] = []

    for index, event in enumerate(events):
        if int(getattr(event, "period", 0) or 0) != int(period):
            continue
        current_lineups = _normalize_lineups(getattr(event, "current_players", {}))
        if team_id not in current_lineups:
            continue

        event_seconds = _parse_clock_seconds_remaining(str(getattr(event, "clock", "") or "0:00"))
        if abs(event_seconds - time_remaining_start) <= 0.001:
            exact_candidates.append((index, event, current_lineups))
        elif event_seconds > time_remaining_start + 0.001:
            prior_candidates.append((index, event, current_lineups))

    if exact_candidates:
        index, event, current_lineups = exact_candidates[-1]
        return current_lineups.get(team_id, []), "exact_clock", event, _event_window(events, index)

    if prior_candidates:
        index, event, current_lineups = prior_candidates[-1]
        return current_lineups.get(team_id, []), "latest_prior_event", event, _event_window(events, index)

    return [], "no_anchor", None, []


def _lineup_match_disposition(
    parser_start_lineup_ids: Iterable[int],
    parser_end_lineup_ids: Iterable[int],
    tpdev_lineup_ids: Iterable[int],
) -> str:
    tpdev_set = {int(player_id) for player_id in tpdev_lineup_ids}
    start_set = {int(player_id) for player_id in parser_start_lineup_ids}
    end_set = {int(player_id) for player_id in parser_end_lineup_ids}
    start_match = start_set == tpdev_set
    end_match = end_set == tpdev_set
    if start_match and end_match:
        return "matches_start_and_end"
    if start_match:
        return "matches_start_only"
    if end_match:
        return "matches_end_only"
    return "matches_neither"


def _same_clock_window_json(window_events: List[object], name_map: Dict[int, str]) -> str:
    rows = []
    for event in window_events:
        player1_id = int(getattr(event, "player1_id", 0) or 0)
        player2_id = int(getattr(event, "player2_id", 0) or 0)
        rows.append(
            {
                "event_num": int(getattr(event, "event_num", 0) or 0),
                "class_name": event.__class__.__name__,
                "description": str(getattr(event, "description", "") or ""),
                "team_id": int(getattr(event, "team_id", 0) or 0),
                "player1_id": player1_id,
                "player1_name": name_map.get(player1_id, str(player1_id)) if player1_id else "",
                "player2_id": player2_id,
                "player2_name": name_map.get(player2_id, str(player2_id)) if player2_id else "",
            }
        )
    return json.dumps(rows, ensure_ascii=False)


def build_possession_start_lineup_audit(
    game_ids: Iterable[str | int],
    parquet_path: Path = DEFAULT_PARQUET_PATH,
    db_path: Path = DEFAULT_DB_PATH,
    tpdev_pbp_path: Path = DEFAULT_TPDEV_PBP_PATH,
    tpdev_box_path: Path = DEFAULT_TPDEV_BOX_PATH,
    bbr_db_path: Path = DEFAULT_BBR_DB_PATH,
    player_crosswalk_path: Path = DEFAULT_PLAYER_CROSSWALK_PATH,
) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []

    for raw_game_id in game_ids:
        game_id = _normalize_game_id(raw_game_id)
        darko_df, possessions, name_map = _load_game_context(
            game_id,
            parquet_path=parquet_path,
            db_path=db_path,
        )
        events = _collect_game_events(possessions)
        stints_df = _build_player_stints(events, name_map)
        starter_audit_df = build_period_starter_audit(
            [game_id],
            parquet_path=parquet_path,
            db_path=db_path,
            tpdev_pbp_path=tpdev_pbp_path,
        )
        missing_starter_players, extra_starter_players = _build_starter_mismatch_maps(
            starter_audit_df
        )
        recon_df = _build_player_minutes_recon(
            darko_df=darko_df,
            stints_df=stints_df,
            game_id=game_id,
            db_path=db_path,
            tpdev_box_path=tpdev_box_path,
            tpdev_pbp_path=tpdev_pbp_path,
            pbpstats_box_path=DEFAULT_PBPSTATS_BOX_PATH,
            bbr_db_path=bbr_db_path,
            player_crosswalk_path=player_crosswalk_path,
            same_clock_substitution_scoring_events=_count_same_clock_substitution_scoring_events(
                events
            ),
            missing_starter_players=missing_starter_players,
            extra_starter_players=extra_starter_players,
        )
        focus_df = recon_df[
            (recon_df["largest_discrepancy_cause"] == "wrong substitution clock attribution")
            & (recon_df["consensus_diff_seconds"].abs() > SECONDS_MATCH_TOLERANCE)
        ].copy()
        focus_players_by_team = {
            int(team_id): sorted(group["player_id"].astype(int).tolist())
            for team_id, group in focus_df.groupby("team_id")
        }
        if not focus_players_by_team:
            continue

        tpdev_team_rows = _load_tpdev_team_rows(tpdev_pbp_path, game_id)
        for row in tpdev_team_rows.to_dict(orient="records"):
            team_id = int(row["team_id"])
            focus_player_ids = focus_players_by_team.get(team_id, [])
            if not focus_player_ids:
                continue

            parser_start_lineup_ids, anchor_kind, anchor_event, anchor_window = _parser_lineup_at_possession_start(
                events=events,
                period=int(row["period"]),
                time_remaining_start=float(row["time_remaining_start"]),
                team_id=team_id,
            )
            parser_end_lineup_ids, end_anchor_kind, end_anchor_event, end_anchor_window = _parser_lineup_at_possession_start(
                events=events,
                period=int(row["period"]),
                time_remaining_start=float(row["time_remaining_end"]),
                team_id=team_id,
            )
            tpdev_lineup_ids = [int(player_id) for player_id in row["tpdev_lineup_ids"]]
            parser_set = set(parser_start_lineup_ids)
            parser_end_set = set(parser_end_lineup_ids)
            tpdev_set = set(tpdev_lineup_ids)
            focus_overlap = sorted(
                player_id
                for player_id in focus_player_ids
                if player_id in parser_set or player_id in parser_end_set or player_id in tpdev_set
            )
            if not focus_overlap:
                continue

            missing_from_parser = sorted(tpdev_set - parser_set)
            extra_in_parser = sorted(parser_set - tpdev_set)
            missing_from_parser_end = sorted(tpdev_set - parser_end_set)
            extra_in_parser_end = sorted(parser_end_set - tpdev_set)
            anchor_event_num = int(getattr(anchor_event, "event_num", 0) or 0) if anchor_event is not None else 0
            anchor_event_class = anchor_event.__class__.__name__ if anchor_event is not None else ""
            anchor_event_description = str(getattr(anchor_event, "description", "") or "") if anchor_event is not None else ""
            end_anchor_event_num = int(getattr(end_anchor_event, "event_num", 0) or 0) if end_anchor_event is not None else 0
            end_anchor_event_class = end_anchor_event.__class__.__name__ if end_anchor_event is not None else ""
            end_anchor_event_description = str(getattr(end_anchor_event, "description", "") or "") if end_anchor_event is not None else ""
            start_lineups_match = parser_set == tpdev_set
            end_lineups_match = parser_end_set == tpdev_set

            rows.append(
                {
                    "game_id": game_id,
                    "period": int(row["period"]),
                    "time_remaining_start": float(row["time_remaining_start"]),
                    "time_remaining_start_clock": _format_clock(float(row["time_remaining_start"])),
                    "time_remaining_end": float(row["time_remaining_end"]),
                    "time_remaining_end_clock": _format_clock(float(row["time_remaining_end"])),
                    "length_seconds": float(row["length_seconds"]),
                    "event_id": int(row["event_id"]),
                    "poss_string": str(row["poss_string"]),
                    "team_id": team_id,
                    "team_side": row["team_side"],
                    "offense_team_id": int(row["offense_team_id"]) if row["offense_team_id"] else 0,
                    "focus_player_ids": focus_overlap,
                    "focus_player_names": [name_map.get(player_id, str(player_id)) for player_id in focus_overlap],
                    "parser_lineup_ids": parser_start_lineup_ids,
                    "parser_lineup_names": [name_map.get(player_id, str(player_id)) for player_id in parser_start_lineup_ids],
                    "parser_end_lineup_ids": parser_end_lineup_ids,
                    "parser_end_lineup_names": [name_map.get(player_id, str(player_id)) for player_id in parser_end_lineup_ids],
                    "tpdev_lineup_ids": tpdev_lineup_ids,
                    "tpdev_lineup_names": [name_map.get(player_id, str(player_id)) for player_id in tpdev_lineup_ids],
                    "lineups_match": start_lineups_match,
                    "start_lineups_match": start_lineups_match,
                    "end_lineups_match": end_lineups_match,
                    "match_disposition": _lineup_match_disposition(
                        parser_start_lineup_ids=parser_start_lineup_ids,
                        parser_end_lineup_ids=parser_end_lineup_ids,
                        tpdev_lineup_ids=tpdev_lineup_ids,
                    ),
                    "missing_from_parser_ids": missing_from_parser,
                    "missing_from_parser_names": [name_map.get(player_id, str(player_id)) for player_id in missing_from_parser],
                    "extra_in_parser_ids": extra_in_parser,
                    "extra_in_parser_names": [name_map.get(player_id, str(player_id)) for player_id in extra_in_parser],
                    "missing_from_parser_end_ids": missing_from_parser_end,
                    "missing_from_parser_end_names": [name_map.get(player_id, str(player_id)) for player_id in missing_from_parser_end],
                    "extra_in_parser_end_ids": extra_in_parser_end,
                    "extra_in_parser_end_names": [name_map.get(player_id, str(player_id)) for player_id in extra_in_parser_end],
                    "anchor_kind": anchor_kind,
                    "anchor_event_num": anchor_event_num,
                    "anchor_event_class": anchor_event_class,
                    "anchor_event_description": anchor_event_description,
                    "end_anchor_kind": end_anchor_kind,
                    "end_anchor_event_num": end_anchor_event_num,
                    "end_anchor_event_class": end_anchor_event_class,
                    "end_anchor_event_description": end_anchor_event_description,
                    "parser_lineup_json": _serialize_lineup(parser_start_lineup_ids, name_map),
                    "parser_end_lineup_json": _serialize_lineup(parser_end_lineup_ids, name_map),
                    "tpdev_lineup_json": _serialize_lineup(tpdev_lineup_ids, name_map),
                    "same_clock_window_json": _same_clock_window_json(anchor_window, name_map),
                    "end_same_clock_window_json": _same_clock_window_json(end_anchor_window, name_map),
                }
            )

    if not rows:
        return pd.DataFrame(
            columns=[
                "game_id",
                "period",
                "time_remaining_start",
                "time_remaining_start_clock",
                "time_remaining_end",
                "time_remaining_end_clock",
                "length_seconds",
                "event_id",
                "poss_string",
                "team_id",
                "team_side",
                "offense_team_id",
                "focus_player_ids",
                "focus_player_names",
                "parser_lineup_ids",
                "parser_lineup_names",
                "parser_end_lineup_ids",
                "parser_end_lineup_names",
                "tpdev_lineup_ids",
                "tpdev_lineup_names",
                "lineups_match",
                "start_lineups_match",
                "end_lineups_match",
                "match_disposition",
                "missing_from_parser_ids",
                "missing_from_parser_names",
                "extra_in_parser_ids",
                "extra_in_parser_names",
                "missing_from_parser_end_ids",
                "missing_from_parser_end_names",
                "extra_in_parser_end_ids",
                "extra_in_parser_end_names",
                "anchor_kind",
                "anchor_event_num",
                "anchor_event_class",
                "anchor_event_description",
                "end_anchor_kind",
                "end_anchor_event_num",
                "end_anchor_event_class",
                "end_anchor_event_description",
                "parser_lineup_json",
                "parser_end_lineup_json",
                "tpdev_lineup_json",
                "same_clock_window_json",
                "end_same_clock_window_json",
            ]
        )

    return pd.DataFrame(rows).sort_values(
        ["game_id", "period", "time_remaining_start", "team_id"],
        ascending=[True, True, False, True],
    ).reset_index(drop=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compare parser lineups at tpdev possession starts for focused lineup-timing games."
    )
    parser.add_argument("--game-ids", nargs="+", required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--parquet-path", type=Path, default=DEFAULT_PARQUET_PATH)
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB_PATH)
    parser.add_argument("--tpdev-pbp-path", type=Path, default=DEFAULT_TPDEV_PBP_PATH)
    parser.add_argument("--tpdev-box-path", type=Path, default=DEFAULT_TPDEV_BOX_PATH)
    parser.add_argument("--bbr-db-path", type=Path, default=DEFAULT_BBR_DB_PATH)
    parser.add_argument(
        "--player-crosswalk-path", type=Path, default=DEFAULT_PLAYER_CROSSWALK_PATH
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    audit_df = build_possession_start_lineup_audit(
        game_ids=args.game_ids,
        parquet_path=args.parquet_path,
        db_path=args.db_path,
        tpdev_pbp_path=args.tpdev_pbp_path,
        tpdev_box_path=args.tpdev_box_path,
        bbr_db_path=args.bbr_db_path,
        player_crosswalk_path=args.player_crosswalk_path,
    )
    audit_df.to_csv(args.output_dir / "lineup_possession_start_audit.csv", index=False)
    audit_df.loc[~audit_df["lineups_match"]].to_csv(
        args.output_dir / "lineup_possession_start_mismatches.csv",
        index=False,
    )
    summary = {
        "rows": int(len(audit_df)),
        "mismatch_rows": int((~audit_df["lineups_match"]).sum()) if not audit_df.empty else 0,
        "games": int(audit_df["game_id"].nunique()) if not audit_df.empty else 0,
        "rows_with_exact_clock_anchor": int((audit_df["anchor_kind"] == "exact_clock").sum())
        if not audit_df.empty
        else 0,
        "rows_with_exact_clock_end_anchor": int((audit_df["end_anchor_kind"] == "exact_clock").sum())
        if not audit_df.empty
        else 0,
        "match_disposition_counts": audit_df["match_disposition"].value_counts().to_dict()
        if not audit_df.empty
        else {},
    }
    (args.output_dir / "summary.json").write_text(
        json.dumps(summary, indent=2),
        encoding="utf-8",
    )
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
