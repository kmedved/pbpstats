from __future__ import annotations

import argparse
import json
from collections import Counter
from pathlib import Path
from typing import Any, Dict, Iterable, List

import pandas as pd

from historic_backfill.common.game_context import (
    DEFAULT_DB_PATH,
    DEFAULT_FILE_DIRECTORY,
    DEFAULT_PARQUET_PATH,
    _normalize_game_id,
)
from historic_backfill.common.lineups import (
    _collect_game_events,
    _normalize_lineups,
)
from historic_backfill.common.game_context import _load_game_context


TEAM_ID_FLOOR = 1000000000


def _build_player_team_map(darko_df: pd.DataFrame) -> Dict[int, int]:
    if darko_df.empty:
        return {}
    mapping = (
        darko_df[["NbaDotComID", "Team_SingleGame"]]
        .dropna()
        .drop_duplicates()
        .copy()
    )
    mapping["NbaDotComID"] = pd.to_numeric(mapping["NbaDotComID"], errors="coerce").fillna(0).astype(int)
    mapping["Team_SingleGame"] = pd.to_numeric(mapping["Team_SingleGame"], errors="coerce").fillna(0).astype(int)
    mapping = mapping[(mapping["NbaDotComID"] > 0) & (mapping["Team_SingleGame"] > 0)]
    return dict(zip(mapping["NbaDotComID"], mapping["Team_SingleGame"]))


def _event_number(event: object) -> int | None:
    for attr in ("event_num", "event_number", "number"):
        value = getattr(event, attr, None)
        if value is None:
            continue
        try:
            return int(value)
        except (TypeError, ValueError):
            continue
    return None


def _event_description(event: object) -> str:
    for attr in ("description", "event_description", "text"):
        value = getattr(event, attr, None)
        if value:
            return str(value)
    return ""


def _player_name(event: object, field: str, player_id: int) -> str:
    for attr in (f"{field}_name", f"{field}_name_initial", f"{field}_name_i"):
        value = getattr(event, attr, None)
        if value:
            return str(value)
    return str(player_id)


def _lineup_for_team(lineups: Dict[int, List[int]], team_id: int) -> List[int]:
    return list(lineups.get(int(team_id), []))


def _is_technical_or_ejection_event(event: object) -> bool:
    class_name = event.__class__.__name__
    if class_name.endswith("Ejection"):
        return True
    return bool(
        getattr(event, "is_technical", False)
        or getattr(event, "is_double_technical", False)
    )


def _same_clock_events(events: List[object], event: object) -> List[object]:
    period = int(getattr(event, "period", 0) or 0)
    clock = str(getattr(event, "clock", "") or "")
    if not period or not clock:
        return []
    return [
        candidate
        for candidate in events
        if candidate is not event
        and int(getattr(candidate, "period", 0) or 0) == period
        and str(getattr(candidate, "clock", "") or "") == clock
    ]


def _has_same_clock_sub_role(
    events: List[object],
    event: object,
    team_id: int,
    player_id: int,
    role: str,
) -> bool:
    attr = "outgoing_player_id" if role == "out" else "incoming_player_id"
    for candidate in _same_clock_events(events, event):
        if candidate.__class__.__name__ != "StatsSubstitution":
            continue
        candidate_team_id = pd.to_numeric(getattr(candidate, "team_id", None), errors="coerce")
        candidate_player_id = pd.to_numeric(getattr(candidate, attr, None), errors="coerce")
        if pd.isna(candidate_team_id) or pd.isna(candidate_player_id):
            continue
        if int(candidate_team_id) == int(team_id) and int(candidate_player_id) == int(player_id):
            return True
    return False


def _is_technical_free_throw(event: object) -> bool:
    if event.__class__.__name__ != "StatsFreeThrow":
        return False
    return "technical" in _event_description(event).lower()


def _has_same_clock_live_credit(
    events: List[object],
    event: object,
    player_id: int,
) -> bool:
    for candidate in _same_clock_events(events, event):
        class_name = candidate.__class__.__name__
        if (
            class_name == "StatsSubstitution"
            or class_name == "StatsFreeThrow"
            or class_name.endswith("StartOfPeriod")
            or class_name.endswith("EndOfPeriod")
            or _is_technical_or_ejection_event(candidate)
        ):
            continue
        for field in ("player1", "player2", "player3"):
            candidate_player_id = pd.to_numeric(getattr(candidate, f"{field}_id", None), errors="coerce")
            if pd.isna(candidate_player_id):
                continue
            if int(candidate_player_id) == int(player_id):
                return True
    return False


def _has_same_clock_replacement_free_throw_context(
    events: List[object],
    event: object,
    team_id: int,
    player_id: int,
) -> bool:
    same_clock_events = _same_clock_events(events, event)
    outgoing_player_ids: list[int] = []
    for candidate in same_clock_events:
        if candidate.__class__.__name__ != "StatsSubstitution":
            continue
        candidate_team_id = pd.to_numeric(getattr(candidate, "team_id", None), errors="coerce")
        incoming_player_id = pd.to_numeric(getattr(candidate, "incoming_player_id", None), errors="coerce")
        outgoing_player_id = pd.to_numeric(getattr(candidate, "outgoing_player_id", None), errors="coerce")
        if pd.isna(candidate_team_id) or pd.isna(incoming_player_id) or pd.isna(outgoing_player_id):
            continue
        if int(candidate_team_id) == int(team_id) and int(incoming_player_id) == int(player_id):
            outgoing_player_ids.append(int(outgoing_player_id))

    if not outgoing_player_ids:
        return False

    outgoing_set = set(outgoing_player_ids)
    for candidate in same_clock_events:
        if candidate.__class__.__name__ != "StatsFoul":
            continue
        for field in ("player1", "player2", "player3"):
            candidate_player_id = pd.to_numeric(getattr(candidate, f"{field}_id", None), errors="coerce")
            if pd.isna(candidate_player_id):
                continue
            if int(candidate_player_id) in outgoing_set:
                return True
    return False


def _is_same_clock_control_eligible_credit(
    events: List[object],
    event: object,
    field: str,
    team_id: int,
    player_id: int,
) -> bool:
    class_name = event.__class__.__name__

    if (
        class_name == "StatsFoul"
        and _has_same_clock_sub_role(events, event, team_id, player_id, "out")
    ):
        return True

    if class_name == "StatsFreeThrow":
        same_clock_sub_in = _has_same_clock_sub_role(events, event, team_id, player_id, "in")
        same_clock_sub_out = _has_same_clock_sub_role(events, event, team_id, player_id, "out")
        if (same_clock_sub_in or same_clock_sub_out) and _is_technical_free_throw(event):
            return True
        if same_clock_sub_out:
            return True
        if same_clock_sub_in and _has_same_clock_replacement_free_throw_context(
            events,
            event,
            team_id,
            player_id,
        ):
            return True

    if class_name == "StatsRebound" and _has_same_clock_sub_role(events, event, team_id, player_id, "in"):
        return True

    return False


def _check_event_players(
    game_id: str,
    events: List[object],
    player_team_map: Dict[int, int],
) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []

    for event in events:
        period = int(getattr(event, "period", 0) or 0)
        clock = str(getattr(event, "clock", "") or "")
        class_name = event.__class__.__name__
        is_technical_or_ejection = _is_technical_or_ejection_event(event)
        if class_name.endswith("StartOfPeriod") or class_name.endswith("EndOfPeriod"):
            continue

        current_lineups = _normalize_lineups(getattr(event, "current_players", {}))
        previous_lineups = _normalize_lineups(
            getattr(getattr(event, "previous_event", None), "current_players", {})
        )

        for field in ("player1", "player2", "player3"):
            raw_player_id = getattr(event, f"{field}_id", None)
            player_id = pd.to_numeric(raw_player_id, errors="coerce")
            if pd.isna(player_id):
                continue
            player_int = int(player_id)
            if player_int <= 0 or player_int >= TEAM_ID_FLOOR:
                continue

            team_id = player_team_map.get(player_int)
            if not team_id:
                for lineup_team_id, lineup_players in current_lineups.items():
                    if player_int in lineup_players:
                        team_id = lineup_team_id
                        break
                if not team_id:
                    for lineup_team_id, lineup_players in previous_lineups.items():
                        if player_int in lineup_players:
                            team_id = lineup_team_id
                            break
            if not team_id:
                continue

            current_team_lineup = _lineup_for_team(current_lineups, team_id)
            previous_team_lineup = _lineup_for_team(previous_lineups, team_id)
            on_current = player_int in current_team_lineup
            on_previous = player_int in previous_team_lineup

            status = None
            if class_name == "StatsSubstitution":
                if field == "player1":
                    if not on_previous and not _has_same_clock_live_credit(events, event, player_int):
                        status = "sub_out_player_missing_from_previous_lineup"
                elif field == "player2":
                    if not on_current:
                        status = "sub_in_player_missing_from_current_lineup"
                else:
                    continue
            else:
                # Technicals and ejections can be charged after a player has already
                # been subbed out, so they are not reliable on-court contradiction
                # signals for this audit.
                if is_technical_or_ejection:
                    continue
                if on_current:
                    continue
                if _is_same_clock_control_eligible_credit(
                    events,
                    event,
                    field,
                    int(team_id),
                    player_int,
                ):
                    continue
                status = "same_clock_boundary_conflict" if on_previous else "off_court_event_credit"

            if status is None:
                continue

            rows.append(
                {
                    "game_id": game_id,
                    "event_num": _event_number(event),
                    "period": period,
                    "clock": clock,
                    "event_class": class_name,
                    "player_field": field,
                    "player_id": player_int,
                    "player_name": _player_name(event, field, player_int),
                    "team_id": int(team_id),
                    "status": status,
                    "on_current_lineup": bool(on_current),
                    "on_previous_lineup": bool(on_previous),
                    "current_team_lineup": json.dumps(current_team_lineup),
                    "previous_team_lineup": json.dumps(previous_team_lineup),
                    "event_description": _event_description(event),
                }
            )

    return pd.DataFrame(rows)


def audit_event_player_on_court(
    game_ids: Iterable[str | int],
    parquet_path: Path = DEFAULT_PARQUET_PATH,
    db_path: Path = DEFAULT_DB_PATH,
    file_directory: Path = DEFAULT_FILE_DIRECTORY,
    pbp_row_overrides_path: Path | None = None,
    pbp_stat_overrides_path: Path | None = None,
    boxscore_source_overrides_path: Path | None = None,
    period_starter_parquet_paths: Iterable[Path] | None = None,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    issue_frames: List[pd.DataFrame] = []

    for raw_game_id in game_ids:
        game_id = _normalize_game_id(raw_game_id)
        darko_df, possessions, _ = _load_game_context(
            game_id,
            parquet_path=parquet_path,
            db_path=db_path,
            file_directory=file_directory,
            pbp_row_overrides_path=pbp_row_overrides_path,
            pbp_stat_overrides_path=pbp_stat_overrides_path,
            boxscore_source_overrides_path=boxscore_source_overrides_path,
            period_starter_parquet_paths=period_starter_parquet_paths,
        )
        events = _collect_game_events(possessions)
        player_team_map = _build_player_team_map(darko_df)
        issue_frames.append(_check_event_players(game_id, events, player_team_map))

    issue_columns = [
        "game_id",
        "event_num",
        "period",
        "clock",
        "event_class",
        "player_field",
        "player_id",
        "player_name",
        "team_id",
        "status",
        "on_current_lineup",
        "on_previous_lineup",
        "current_team_lineup",
        "previous_team_lineup",
        "event_description",
    ]

    issues_df = (
        pd.concat(issue_frames, ignore_index=True)
        if issue_frames
        else pd.DataFrame(columns=issue_columns)
    )
    issues_df = issues_df.reindex(columns=issue_columns)
    if not issues_df.empty:
        issues_df = issues_df.drop_duplicates(
            subset=["game_id", "event_num", "player_id", "team_id", "status"]
        )

    summary = {
        "games": int(len({_normalize_game_id(game_id) for game_id in game_ids})),
        "issue_rows": int(len(issues_df)),
        "issue_games": int(issues_df["game_id"].nunique()) if not issues_df.empty else 0,
        "status_counts": dict(Counter(issues_df["status"])) if not issues_df.empty else {},
    }
    if issues_df.empty:
        return issues_df, summary

    return issues_df.sort_values(["game_id", "period", "event_num", "player_field"]), summary


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Audit whether players credited on events were on court in the current parser state."
    )
    parser.add_argument("--game-ids", nargs="+", required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--parquet-path", type=Path, default=DEFAULT_PARQUET_PATH)
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB_PATH)
    parser.add_argument("--file-directory", type=Path, default=DEFAULT_FILE_DIRECTORY)
    parser.add_argument("--pbp-row-overrides-path", type=Path)
    parser.add_argument("--pbp-stat-overrides-path", type=Path)
    parser.add_argument("--boxscore-source-overrides-path", type=Path)
    parser.add_argument("--period-starter-parquet-paths", type=Path, nargs="*")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    issues_df, summary = audit_event_player_on_court(
        game_ids=args.game_ids,
        parquet_path=args.parquet_path,
        db_path=args.db_path,
        file_directory=args.file_directory,
        pbp_row_overrides_path=args.pbp_row_overrides_path,
        pbp_stat_overrides_path=args.pbp_stat_overrides_path,
        boxscore_source_overrides_path=args.boxscore_source_overrides_path,
        period_starter_parquet_paths=args.period_starter_parquet_paths,
    )
    issues_df.to_csv(args.output_dir / "event_player_on_court_issues.csv", index=False)
    (args.output_dir / "summary.json").write_text(
        json.dumps(summary, indent=2),
        encoding="utf-8",
    )
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
