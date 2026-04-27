from __future__ import annotations

import argparse
import json
import math
from collections import Counter, deque
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple

import pandas as pd

from historic_backfill.audits.core.minutes_plus_minus import _prepare_darko_df, load_official_boxscore_df
from historic_backfill.audits.cross_source.period_starters import (
    DEFAULT_DB_PATH,
    DEFAULT_PARQUET_PATH,
    DEFAULT_TPDEV_PBP_PATH,
    _normalize_game_id,
    build_period_starter_audit,
)
from historic_backfill.audits.cross_source.bbr_boxscore_loader import (
    DEFAULT_BBR_DB_PATH,
    DEFAULT_PLAYER_CROSSWALK_PATH,
    load_bbr_boxscore_df,
)
from historic_backfill.runners.cautious_rerun import install_local_boxscore_wrapper, load_v9b_namespace
from historic_backfill.audits.cross_source.minute_reference_sources import (
    DEFAULT_PBPSTATS_PLAYER_BOX_PATH,
    load_pbpstats_player_box_frame,
    load_tpdev_pbp_minutes_frame,
)


ROOT = Path(__file__).resolve().parent
DEFAULT_FILE_DIRECTORY = ROOT
DEFAULT_TPDEV_BOX_PATH = (
    ROOT.parent / "fixed_data" / "raw_input_data" / "tpdev_data" / "tpdev_box.parq"
)
DEFAULT_PBPSTATS_BOX_PATH = DEFAULT_PBPSTATS_PLAYER_BOX_PATH
SECONDS_MATCH_TOLERANCE = 1.0
_GAME_CONTEXT_NAMESPACE_CACHE: Dict[Tuple[Path, Path, int], Dict[str, Any]] = {}
_GAME_CONTEXT_SEASON_PBP_CACHE: Dict[Tuple[Path, int], pd.DataFrame] = {}


def _season_from_game_id(game_id: str | int) -> int:
    gid = _normalize_game_id(game_id)
    yy = int(gid[3:5])
    return 1901 + yy if yy >= 50 else 2001 + yy


def _period_length_seconds(period: int) -> int:
    return 720 if int(period) <= 4 else 300


def _period_start_clock(period: int) -> str:
    total = _period_length_seconds(period)
    minutes = total // 60
    seconds = total % 60
    return f"{minutes}:{seconds:02d}"


def _parse_clock_seconds_remaining(clock: str | None) -> float:
    if clock is None:
        return 0.0
    text = str(clock).strip()
    if text == "":
        return 0.0
    if ":" not in text:
        return float(text)
    minutes_text, seconds_text = text.split(":", 1)
    return int(minutes_text) * 60 + float(seconds_text)


def _elapsed_seconds(period: int, clock: str | None) -> float:
    period = int(period)
    seconds_remaining = _parse_clock_seconds_remaining(clock)
    elapsed_before = 0
    for prior_period in range(1, period):
        elapsed_before += _period_length_seconds(prior_period)
    return elapsed_before + (_period_length_seconds(period) - seconds_remaining)


def _normalize_lineups(lineups: Dict[Any, Iterable[Any]] | None) -> Dict[int, List[int]]:
    normalized: Dict[int, List[int]] = {}
    if not isinstance(lineups, dict):
        return normalized
    for raw_team_id, raw_players in lineups.items():
        team_id = pd.to_numeric(raw_team_id, errors="coerce")
        if pd.isna(team_id) or int(team_id) <= 0:
            continue
        players: List[int] = []
        for raw_player in raw_players or []:
            player_id = pd.to_numeric(raw_player, errors="coerce")
            if pd.isna(player_id):
                continue
            player_int = int(player_id)
            if player_int <= 0 or player_int in players:
                continue
            players.append(player_int)
        if players:
            normalized[int(team_id)] = players
    return normalized


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


def _collect_game_events(possessions) -> List[object]:
    linked_events = _iter_linked_events(possessions)
    linked_index = {id(event): index for index, event in enumerate(linked_events)}

    queue: deque[object] = deque(linked_events)
    for possession in getattr(possessions, "items", []):
        for event in getattr(possession, "events", []):
            queue.append(event)

    collected: Dict[int, object] = {}
    while queue:
        event = queue.popleft()
        if event is None or id(event) in collected:
            continue
        collected[id(event)] = event
        queue.append(getattr(event, "previous_event", None))
        queue.append(getattr(event, "next_event", None))

    def sort_key(event: object) -> Tuple[int, float, int, int]:
        period = int(getattr(event, "period", 0) or 0)
        clock = str(getattr(event, "clock", "") or "")
        linked_pos = linked_index.get(id(event), 10**9)
        event_num = int(getattr(event, "event_num", 0) or 0)
        return (period, -_parse_clock_seconds_remaining(clock), linked_pos, event_num)

    return sorted(collected.values(), key=sort_key)


def _is_scoring_event(event: object) -> bool:
    return bool(
        hasattr(event, "is_made")
        and getattr(event, "is_made", False)
        and event.__class__.__name__ in {"StatsFieldGoal", "StatsFreeThrow"}
    )


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


def _count_same_clock_substitution_scoring_events(events: List[object]) -> int:
    count = 0
    for index, event in enumerate(events):
        if not _is_scoring_event(event):
            continue
        window = _collect_same_clock_window(events, index)
        if any(window_event.__class__.__name__ == "StatsSubstitution" for window_event in window):
            count += 1
    return count


def _load_game_context(
    game_id: str | int,
    parquet_path: Path,
    db_path: Path,
    file_directory: Path = DEFAULT_FILE_DIRECTORY,
) -> Tuple[pd.DataFrame, Any, Dict[int, str]]:
    normalized_game_id = _normalize_game_id(game_id)
    season = _season_from_game_id(normalized_game_id)
    db_path = db_path.resolve()
    parquet_path = parquet_path.resolve()
    file_directory = file_directory.resolve()

    namespace_key = (db_path, file_directory, season)
    namespace = _GAME_CONTEXT_NAMESPACE_CACHE.get(namespace_key)
    if namespace is None:
        namespace = load_v9b_namespace()
        namespace["DB_PATH"] = db_path
        install_local_boxscore_wrapper(
            namespace,
            db_path,
            file_directory=file_directory,
            allowed_seasons=[season],
        )
        _GAME_CONTEXT_NAMESPACE_CACHE[namespace_key] = namespace

    season_pbp_key = (parquet_path, season)
    season_pbp_df = _GAME_CONTEXT_SEASON_PBP_CACHE.get(season_pbp_key)
    if season_pbp_df is None:
        season_pbp_df = namespace["load_pbp_from_parquet"](str(parquet_path), season=season)
        _GAME_CONTEXT_SEASON_PBP_CACHE[season_pbp_key] = season_pbp_df

    darko_df, possessions = namespace["generate_darko_hybrid"](normalized_game_id, season_pbp_df)

    prepared_darko = _prepare_darko_df(darko_df)
    name_map = {
        int(player_id): str(player_name)
        for player_id, player_name in zip(
            prepared_darko["player_id"].astype(int),
            prepared_darko["player_name"],
        )
    }
    return darko_df, possessions, name_map


def _load_tpdev_boxscore_df(path: Path, game_id: str | int) -> pd.DataFrame:
    empty = pd.DataFrame(
        columns=[
            "game_id",
            "player_id",
            "team_id",
            "player_name_tpdev_box",
            "Minutes_tpdev_box",
            "Plus_Minus_tpdev_box",
        ]
    )
    if not path.exists():
        return empty

    game_int = int(_normalize_game_id(game_id))
    df = pd.read_parquet(
        path,
        filters=[("Game_SingleGame", "==", game_int)],
        columns=[
            "Game_SingleGame",
            "Team_SingleGame",
            "NbaDotComID",
            "FullName",
            "Minutes",
            "Plus_Minus",
        ],
    )
    if df.empty:
        return empty

    df["game_id"] = df["Game_SingleGame"].apply(_normalize_game_id)
    df["player_id"] = pd.to_numeric(df["NbaDotComID"], errors="coerce").fillna(0).astype(int)
    df["team_id"] = pd.to_numeric(df["Team_SingleGame"], errors="coerce").fillna(0).astype(int)
    df["player_name_tpdev_box"] = df["FullName"].fillna("").astype(str)
    df["Minutes_tpdev_box"] = pd.to_numeric(df["Minutes"], errors="coerce")
    df["Plus_Minus_tpdev_box"] = pd.to_numeric(df["Plus_Minus"], errors="coerce")
    return df[
        [
            "game_id",
            "player_id",
            "team_id",
            "player_name_tpdev_box",
            "Minutes_tpdev_box",
            "Plus_Minus_tpdev_box",
        ]
    ].copy()


def _build_player_stints(events: List[object], name_map: Dict[int, str]) -> pd.DataFrame:
    open_stints: Dict[Tuple[int, int], Dict[str, Any]] = {}
    stint_index_by_player: Counter[Tuple[int, int]] = Counter()
    rows: List[Dict[str, Any]] = []
    last_period = None

    def open_player(
        team_id: int,
        player_id: int,
        period: int,
        clock: str,
        reason: str,
    ) -> None:
        key = (team_id, player_id)
        if key in open_stints:
            return
        stint_index_by_player[key] += 1
        open_stints[key] = {
            "game_id": _normalize_game_id(getattr(events[0], "game_id", "")),
            "team_id": team_id,
            "player_id": player_id,
            "player_name": name_map.get(player_id, str(player_id)),
            "stint_index": stint_index_by_player[key],
            "start_period": int(period),
            "start_clock": str(clock),
            "start_elapsed_seconds": _elapsed_seconds(period, clock),
            "start_reason": reason,
        }

    def close_player(
        team_id: int,
        player_id: int,
        period: int,
        clock: str,
        reason: str,
    ) -> None:
        key = (team_id, player_id)
        stint = open_stints.pop(key, None)
        if stint is None:
            return
        end_elapsed_seconds = _elapsed_seconds(period, clock)
        rows.append(
            {
                "game_id": stint["game_id"],
                "team_id": team_id,
                "player_id": player_id,
                "player_name": stint["player_name"],
                "stint_index": stint["stint_index"],
                "start_period": stint["start_period"],
                "start_clock": stint["start_clock"],
                "end_period": int(period),
                "end_clock": str(clock),
                "duration_seconds": max(0.0, end_elapsed_seconds - stint["start_elapsed_seconds"]),
                "start_reason": stint["start_reason"],
                "end_reason": reason,
            }
        )

    def close_all(period: int, clock: str, reason: str) -> None:
        for team_id, player_id in list(open_stints.keys()):
            close_player(team_id, player_id, period, clock, reason)

    for event in events:
        class_name = event.__class__.__name__
        period = int(getattr(event, "period", 0) or 0)
        clock = str(getattr(event, "clock", "") or "")
        if clock == "":
            clock = _period_start_clock(period) if class_name.endswith("StartOfPeriod") else "0:00"

        current_lineups = _normalize_lineups(getattr(event, "current_players", {}))
        previous_lineups = _normalize_lineups(
            getattr(getattr(event, "previous_event", None), "current_players", {})
        )

        if class_name.endswith("StartOfPeriod"):
            if open_stints and last_period and period != last_period:
                close_all(last_period, "0:00", "end_of_period")
            for team_id, players in current_lineups.items():
                for player_id in players:
                    open_player(team_id, player_id, period, clock, "start_of_period")
            last_period = period
            continue

        if class_name.endswith("EndOfPeriod"):
            close_all(period, clock or "0:00", "end_of_period")
            last_period = period
            continue

        if not current_lineups or not previous_lineups:
            last_period = period
            continue

        if current_lineups == previous_lineups:
            last_period = period
            continue

        out_reason = "substitution_out" if class_name == "StatsSubstitution" else "lineup_change_out"
        in_reason = "substitution_in" if class_name == "StatsSubstitution" else "lineup_change_in"

        for team_id in sorted(set(previous_lineups) | set(current_lineups)):
            prev_players = previous_lineups.get(team_id, [])
            curr_players = current_lineups.get(team_id, [])
            exiting = [player_id for player_id in prev_players if player_id not in curr_players]
            entering = [player_id for player_id in curr_players if player_id not in prev_players]
            for player_id in exiting:
                close_player(team_id, player_id, period, clock, out_reason)
            for player_id in entering:
                open_player(team_id, player_id, period, clock, in_reason)

        last_period = period

    if open_stints:
        end_period = int(last_period or 0)
        close_all(end_period, "0:00", "end_of_game")

    return pd.DataFrame(rows).sort_values(
        ["team_id", "player_id", "stint_index"]
    ).reset_index(drop=True)


def _build_starter_mismatch_maps(starter_audit_df: pd.DataFrame) -> Tuple[set[Tuple[int, int]], set[Tuple[int, int]]]:
    missing_players: set[Tuple[int, int]] = set()
    extra_players: set[Tuple[int, int]] = set()
    if starter_audit_df.empty:
        return missing_players, extra_players

    for row in starter_audit_df.itertuples(index=False):
        if bool(getattr(row, "starter_sets_match", True)):
            continue
        if not getattr(row, "tpdev_starter_ids", []):
            continue
        team_id = int(row.team_id)
        for player_id in getattr(row, "missing_from_current_ids", []):
            missing_players.add((team_id, int(player_id)))
        for player_id in getattr(row, "extra_in_current_ids", []):
            extra_players.add((team_id, int(player_id)))
    return missing_players, extra_players


def _choose_consensus_seconds(row: pd.Series) -> Tuple[float | None, Tuple[str, ...]]:
    available = []
    for label in ("tpdev_pbp", "pbpstats_box", "official", "tpdev", "bbr"):
        value = row.get(f"{label}_seconds")
        if pd.notna(value):
            available.append((label, float(value)))

    if len(available) < 2:
        return (available[0][1], (available[0][0],)) if available else (None, tuple())

    agreeing_groups: List[Tuple[Tuple[str, ...], float]] = []
    for i, (label_a, value_a) in enumerate(available):
        group_labels = [label_a]
        group_values = [value_a]
        for label_b, value_b in available[i + 1 :]:
            if abs(value_a - value_b) <= SECONDS_MATCH_TOLERANCE:
                group_labels.append(label_b)
                group_values.append(value_b)
        agreeing_groups.append((tuple(sorted(group_labels)), float(sum(group_values) / len(group_values))))

    agreeing_groups.sort(key=lambda item: (len(item[0]), item[0]), reverse=True)
    best_labels, best_value = agreeing_groups[0]
    if len(best_labels) >= 2:
        return best_value, best_labels
    return float(sum(value for _, value in available) / len(available)), tuple(label for label, _ in available)


def _choose_preferred_reference_seconds(row: pd.Series) -> Tuple[float | None, Tuple[str, ...]]:
    for label in ("tpdev_pbp", "pbpstats_box", "official", "tpdev", "bbr"):
        value = row.get(f"{label}_seconds")
        if pd.notna(value):
            return float(value), (label,)
    return None, tuple()


def _classify_largest_discrepancy_cause(
    row: pd.Series,
    missing_starter_players: set[Tuple[int, int]],
    extra_starter_players: set[Tuple[int, int]],
    same_clock_substitution_scoring_events: int,
) -> str:
    team_player = (int(row["team_id"]), int(row["player_id"]))
    output_seconds = float(row["output_seconds"])
    reference_seconds = row.get("preferred_seconds")
    if pd.isna(reference_seconds):
        reference_seconds = row.get("consensus_seconds")
    if pd.isna(reference_seconds):
        reference_seconds = None
    largest_discrepancy_seconds = float(row["largest_discrepancy_seconds"])
    if largest_discrepancy_seconds <= SECONDS_MATCH_TOLERANCE:
        return "none"

    output_matches_non_official = any(
        bool(row.get(f"output_matches_{label}", False))
        for label in ("pbpstats_box", "tpdev_pbp", "tpdev", "bbr")
    )
    official_matches_non_output = any(
        bool(row.get(f"official_matches_{label}", False))
        for label in ("pbpstats_box", "tpdev_pbp", "tpdev", "bbr")
    )

    if output_matches_non_official and not bool(row.get("output_matches_official", False)):
        return "source disagreement"
    if not official_matches_non_output and not bool(row.get("output_matches_official", False)):
        return "source disagreement"

    if team_player in missing_starter_players or team_player in extra_starter_players:
        return "silent carryover"

    if reference_seconds is not None:
        delta_seconds = output_seconds - float(reference_seconds)
        if same_clock_substitution_scoring_events > 0 and abs(delta_seconds) > SECONDS_MATCH_TOLERANCE:
            return "wrong substitution clock attribution"
        if delta_seconds < -SECONDS_MATCH_TOLERANCE:
            return "missing sub-in"
        if delta_seconds > SECONDS_MATCH_TOLERANCE:
            return "missing sub-out"

    return "source disagreement"


def _build_player_minutes_recon(
    darko_df: pd.DataFrame,
    stints_df: pd.DataFrame,
    game_id: str,
    db_path: Path,
    tpdev_box_path: Path,
    tpdev_pbp_path: Path,
    pbpstats_box_path: Path,
    bbr_db_path: Path,
    player_crosswalk_path: Path,
    same_clock_substitution_scoring_events: int,
    missing_starter_players: set[Tuple[int, int]],
    extra_starter_players: set[Tuple[int, int]],
) -> pd.DataFrame:
    prepared_darko = _prepare_darko_df(darko_df)
    prepared_darko = prepared_darko[prepared_darko["game_id"] == game_id].copy()
    prepared_darko["output_seconds"] = prepared_darko["Minutes_output"] * 60.0

    official_df = load_official_boxscore_df(db_path, game_id).copy()
    official_df["official_seconds"] = official_df["Minutes_official"] * 60.0

    tpdev_df = _load_tpdev_boxscore_df(tpdev_box_path, game_id).copy()
    tpdev_df["tpdev_seconds"] = tpdev_df["Minutes_tpdev_box"] * 60.0

    pbpstats_df = load_pbpstats_player_box_frame(pbpstats_box_path, [game_id]).copy()
    tpdev_pbp_df = load_tpdev_pbp_minutes_frame(tpdev_pbp_path, [game_id]).copy()

    bbr_df = load_bbr_boxscore_df(
        game_id,
        nba_raw_db_path=db_path,
        bbr_db_path=bbr_db_path,
        crosswalk_path=player_crosswalk_path,
    ).copy()
    bbr_df["bbr_seconds"] = bbr_df["Minutes_bbr_box"] * 60.0

    stint_seconds = (
        stints_df.groupby(["game_id", "team_id", "player_id"], as_index=False)["duration_seconds"]
        .sum()
        .rename(columns={"duration_seconds": "stint_seconds"})
    )

    merged = prepared_darko.merge(
        official_df[
            ["game_id", "team_id", "player_id", "player_name", "Minutes_official", "official_seconds"]
        ],
        on=["game_id", "team_id", "player_id"],
        how="outer",
        suffixes=("_output", "_official"),
    )
    merged = merged.merge(
        tpdev_df[
            ["game_id", "team_id", "player_id", "player_name_tpdev_box", "Minutes_tpdev_box", "tpdev_seconds"]
        ],
        on=["game_id", "team_id", "player_id"],
        how="left",
    )
    merged = merged.merge(
        pbpstats_df,
        on=["game_id", "team_id", "player_id"],
        how="left",
    )
    merged = merged.merge(
        tpdev_pbp_df,
        on=["game_id", "team_id", "player_id"],
        how="left",
    )
    merged = merged.merge(
        bbr_df[
            ["game_id", "team_id", "player_id", "player_name_bbr_box", "Minutes_bbr_box", "bbr_seconds"]
        ],
        on=["game_id", "team_id", "player_id"],
        how="left",
    )
    merged = merged.merge(stint_seconds, on=["game_id", "team_id", "player_id"], how="left")

    merged["player_name"] = (
        merged.get("player_name_output")
        .fillna(merged.get("player_name_official"))
        .fillna(merged.get("player_name_pbpstats_box"))
        .fillna(merged.get("player_name_tpdev_box"))
        .fillna(merged.get("player_name_bbr_box"))
        .fillna("")
        .astype(str)
    )
    merged["output_seconds"] = pd.to_numeric(merged.get("output_seconds", math.nan), errors="coerce")
    merged["stint_seconds"] = pd.to_numeric(merged.get("stint_seconds", 0.0), errors="coerce").fillna(0.0)
    merged["official_seconds"] = pd.to_numeric(merged.get("official_seconds", math.nan), errors="coerce")
    merged["tpdev_seconds"] = pd.to_numeric(merged.get("tpdev_seconds", math.nan), errors="coerce")
    merged["pbpstats_box_seconds"] = pd.to_numeric(
        merged.get("pbpstats_box_seconds", math.nan), errors="coerce"
    )
    merged["tpdev_pbp_seconds"] = pd.to_numeric(
        merged.get("tpdev_pbp_seconds", math.nan), errors="coerce"
    )
    merged["bbr_seconds"] = pd.to_numeric(merged.get("bbr_seconds", math.nan), errors="coerce")

    merged["output_vs_stints_diff_seconds"] = merged["output_seconds"] - merged["stint_seconds"]

    for label in ("pbpstats_box", "tpdev_pbp", "official", "tpdev", "bbr"):
        merged[f"output_diff_vs_{label}_seconds"] = merged["output_seconds"] - merged[f"{label}_seconds"]
        merged[f"output_abs_diff_vs_{label}_seconds"] = merged[f"output_diff_vs_{label}_seconds"].abs()
        merged[f"output_matches_{label}"] = (
            merged[f"output_abs_diff_vs_{label}_seconds"] <= SECONDS_MATCH_TOLERANCE
        )

    merged["official_matches_pbpstats_box"] = (
        (merged["official_seconds"] - merged["pbpstats_box_seconds"]).abs() <= SECONDS_MATCH_TOLERANCE
    )
    merged["official_matches_tpdev_pbp"] = (
        (merged["official_seconds"] - merged["tpdev_pbp_seconds"]).abs() <= SECONDS_MATCH_TOLERANCE
    )
    merged["official_matches_tpdev"] = (
        (merged["official_seconds"] - merged["tpdev_seconds"]).abs() <= SECONDS_MATCH_TOLERANCE
    )
    merged["official_matches_bbr"] = (
        (merged["official_seconds"] - merged["bbr_seconds"]).abs() <= SECONDS_MATCH_TOLERANCE
    )

    consensus_values = merged.apply(_choose_consensus_seconds, axis=1)
    merged["consensus_seconds"] = [item[0] for item in consensus_values]
    merged["consensus_sources"] = [json.dumps(list(item[1])) for item in consensus_values]
    merged["consensus_diff_seconds"] = merged["output_seconds"] - merged["consensus_seconds"]
    merged["consensus_abs_diff_seconds"] = merged["consensus_diff_seconds"].abs()
    preferred_values = merged.apply(_choose_preferred_reference_seconds, axis=1)
    merged["preferred_seconds"] = [item[0] for item in preferred_values]
    merged["preferred_sources"] = [json.dumps(list(item[1])) for item in preferred_values]
    merged["output_diff_vs_preferred_seconds"] = merged["output_seconds"] - merged["preferred_seconds"]
    merged["output_abs_diff_vs_preferred_seconds"] = merged["output_diff_vs_preferred_seconds"].abs()
    merged["output_matches_preferred"] = (
        merged["output_abs_diff_vs_preferred_seconds"] <= SECONDS_MATCH_TOLERANCE
    )

    discrepancy_source_cols = {
        "pbpstats_box": "output_abs_diff_vs_pbpstats_box_seconds",
        "tpdev_pbp": "output_abs_diff_vs_tpdev_pbp_seconds",
        "official": "output_abs_diff_vs_official_seconds",
        "tpdev": "output_abs_diff_vs_tpdev_seconds",
        "bbr": "output_abs_diff_vs_bbr_seconds",
    }
    largest_sources: List[str] = []
    largest_values: List[float] = []
    for row in merged.to_dict(orient="records"):
        source_diffs = {
            label: float(row[column])
            for label, column in discrepancy_source_cols.items()
            if not pd.isna(row.get(column))
        }
        if not source_diffs:
            largest_sources.append("")
            largest_values.append(0.0)
            continue
        largest_source = max(source_diffs, key=source_diffs.get)
        largest_sources.append(largest_source)
        largest_values.append(source_diffs[largest_source])
    merged["largest_discrepancy_source"] = largest_sources
    merged["largest_discrepancy_seconds"] = largest_values
    merged["largest_discrepancy_cause"] = merged.apply(
        lambda row: _classify_largest_discrepancy_cause(
            row,
            missing_starter_players=missing_starter_players,
            extra_starter_players=extra_starter_players,
            same_clock_substitution_scoring_events=same_clock_substitution_scoring_events,
        ),
        axis=1,
    )

    return merged[
        [
            "game_id",
            "team_id",
            "player_id",
            "player_name",
            "output_seconds",
            "stint_seconds",
            "output_vs_stints_diff_seconds",
            "pbpstats_box_seconds",
            "output_diff_vs_pbpstats_box_seconds",
            "tpdev_pbp_seconds",
            "output_diff_vs_tpdev_pbp_seconds",
            "official_seconds",
            "output_diff_vs_official_seconds",
            "tpdev_seconds",
            "output_diff_vs_tpdev_seconds",
            "bbr_seconds",
            "output_diff_vs_bbr_seconds",
            "preferred_seconds",
            "output_diff_vs_preferred_seconds",
            "preferred_sources",
            "consensus_seconds",
            "consensus_diff_seconds",
            "consensus_sources",
            "largest_discrepancy_source",
            "largest_discrepancy_seconds",
            "largest_discrepancy_cause",
            "output_matches_pbpstats_box",
            "output_matches_tpdev_pbp",
            "output_matches_official",
            "output_matches_tpdev",
            "output_matches_bbr",
            "output_matches_preferred",
            "official_matches_pbpstats_box",
            "official_matches_tpdev_pbp",
            "official_matches_tpdev",
            "official_matches_bbr",
        ]
    ].sort_values(["team_id", "player_id"]).reset_index(drop=True)


def build_player_stint_trace(
    game_id: str | int,
    parquet_path: Path = DEFAULT_PARQUET_PATH,
    db_path: Path = DEFAULT_DB_PATH,
    file_directory: Path = DEFAULT_FILE_DIRECTORY,
    tpdev_box_path: Path = DEFAULT_TPDEV_BOX_PATH,
    tpdev_pbp_path: Path = DEFAULT_TPDEV_PBP_PATH,
    pbpstats_box_path: Path = DEFAULT_PBPSTATS_BOX_PATH,
    bbr_db_path: Path = DEFAULT_BBR_DB_PATH,
    player_crosswalk_path: Path = DEFAULT_PLAYER_CROSSWALK_PATH,
) -> Tuple[pd.DataFrame, pd.DataFrame, Dict[str, Any]]:
    normalized_game_id = _normalize_game_id(game_id)
    darko_df, possessions, name_map = _load_game_context(
        normalized_game_id,
        parquet_path=parquet_path,
        db_path=db_path,
        file_directory=file_directory,
    )
    events = _collect_game_events(possessions)
    stints_df = _build_player_stints(events, name_map)

    starter_audit_df = build_period_starter_audit(
        [normalized_game_id],
        parquet_path=parquet_path,
        db_path=db_path,
        tpdev_pbp_path=tpdev_pbp_path,
    )
    missing_starter_players, extra_starter_players = _build_starter_mismatch_maps(
        starter_audit_df
    )
    same_clock_substitution_scoring_events = _count_same_clock_substitution_scoring_events(events)

    recon_df = _build_player_minutes_recon(
        darko_df=darko_df,
        stints_df=stints_df,
        game_id=normalized_game_id,
        db_path=db_path,
        tpdev_box_path=tpdev_box_path,
        tpdev_pbp_path=tpdev_pbp_path,
        pbpstats_box_path=pbpstats_box_path,
        bbr_db_path=bbr_db_path,
        player_crosswalk_path=player_crosswalk_path,
        same_clock_substitution_scoring_events=same_clock_substitution_scoring_events,
        missing_starter_players=missing_starter_players,
        extra_starter_players=extra_starter_players,
    )

    summary = {
        "game_id": normalized_game_id,
        "events": int(len(events)),
        "stints": int(len(stints_df)),
        "players": int(len(recon_df)),
        "players_with_source_minutes_mismatch": int(
            (recon_df["largest_discrepancy_seconds"] > SECONDS_MATCH_TOLERANCE).sum()
        ),
        "players_with_output_vs_stints_diff": int(
            (recon_df["output_vs_stints_diff_seconds"].abs() > SECONDS_MATCH_TOLERANCE).sum()
        ),
        "same_clock_substitution_scoring_events": int(same_clock_substitution_scoring_events),
        "period_starter_mismatch_rows": int((~starter_audit_df["starter_sets_match"]).sum())
        if not starter_audit_df.empty
        else 0,
        "cause_counts": dict(Counter(recon_df["largest_discrepancy_cause"])),
    }
    return stints_df, recon_df, summary


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Trace player stints and minutes reconciliation for selected games."
    )
    parser.add_argument("--game-ids", nargs="+", required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--parquet-path", type=Path, default=DEFAULT_PARQUET_PATH)
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB_PATH)
    parser.add_argument("--file-directory", type=Path, default=DEFAULT_FILE_DIRECTORY)
    parser.add_argument("--tpdev-box-path", type=Path, default=DEFAULT_TPDEV_BOX_PATH)
    parser.add_argument("--tpdev-pbp-path", type=Path, default=DEFAULT_TPDEV_PBP_PATH)
    parser.add_argument("--pbpstats-box-path", type=Path, default=DEFAULT_PBPSTATS_BOX_PATH)
    parser.add_argument("--bbr-db-path", type=Path, default=DEFAULT_BBR_DB_PATH)
    parser.add_argument(
        "--player-crosswalk-path", type=Path, default=DEFAULT_PLAYER_CROSSWALK_PATH
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    summaries = []
    for raw_game_id in args.game_ids:
        game_id = _normalize_game_id(raw_game_id)
        game_dir = args.output_dir / game_id
        game_dir.mkdir(parents=True, exist_ok=True)

        stints_df, recon_df, summary = build_player_stint_trace(
            game_id=game_id,
            parquet_path=args.parquet_path,
            db_path=args.db_path,
            file_directory=args.file_directory,
            tpdev_box_path=args.tpdev_box_path,
            tpdev_pbp_path=args.tpdev_pbp_path,
            pbpstats_box_path=args.pbpstats_box_path,
            bbr_db_path=args.bbr_db_path,
            player_crosswalk_path=args.player_crosswalk_path,
        )
        stints_df.to_csv(game_dir / "player_stints.csv", index=False)
        recon_df.to_csv(game_dir / "player_minutes_recon.csv", index=False)
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
