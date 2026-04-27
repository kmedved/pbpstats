from __future__ import annotations

import json
from collections import Counter
from typing import Any, Dict, Iterable, List, Optional, Tuple


TEAM_ID_FLOOR = 1000000000
LINEUP_SIGNAL_STATUSES = {
    "off_court_event_credit",
    "same_clock_boundary_conflict",
}
SUBSTITUTION_SIGNAL_STATUSES = {
    "sub_out_player_missing_from_previous_lineup",
    "sub_in_player_missing_from_current_lineup",
}
_MANUAL_ADMIN_CLASS_SUFFIXES = (
    "StartOfPeriod",
    "EndOfPeriod",
    "Timeout",
    "Replay",
)
_DEADBALL_CLASS_SUFFIXES = (
    "Substitution",
    "Timeout",
    "Replay",
    "JumpBall",
    "StartOfPeriod",
    "Foul",
    "Violation",
    "FreeThrow",
)


def _event_number(event: object) -> Optional[int]:
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


def _normalize_lineups(lineups: Dict[Any, Iterable[Any]] | None) -> Dict[int, List[int]]:
    normalized: Dict[int, List[int]] = {}
    if not isinstance(lineups, dict):
        return normalized
    for raw_team_id, raw_players in lineups.items():
        try:
            team_id = int(raw_team_id)
        except (TypeError, ValueError):
            continue
        if team_id <= 0:
            continue
        players: List[int] = []
        for raw_player in raw_players or []:
            try:
                player_id = int(raw_player)
            except (TypeError, ValueError):
                continue
            if player_id <= 0 or player_id in players:
                continue
            players.append(player_id)
        if players:
            normalized[team_id] = players
    return normalized


def _lineup_for_team(lineups: Dict[int, List[int]], team_id: int) -> List[int]:
    return list(lineups.get(int(team_id), []))


def _class_name(event: object) -> str:
    return event.__class__.__name__


def _is_substitution_event(event: object) -> bool:
    return _class_name(event).endswith("Substitution")


def _is_period_boundary_event(event: object) -> bool:
    class_name = _class_name(event)
    return class_name.endswith("StartOfPeriod") or class_name.endswith("EndOfPeriod")


def _is_technical_or_ejection_event(event: object) -> bool:
    class_name = _class_name(event)
    if class_name.endswith("Ejection"):
        return True
    return bool(
        getattr(event, "is_technical", False)
        or getattr(event, "is_double_technical", False)
    )


def _is_deadball_event(event: object) -> bool:
    class_name = _class_name(event)
    return class_name.endswith(_DEADBALL_CLASS_SUFFIXES)


def _is_admin_only_event(event: object) -> bool:
    if _is_technical_or_ejection_event(event):
        return True
    return _class_name(event).endswith(_MANUAL_ADMIN_CLASS_SUFFIXES)


def _iter_player_fields() -> tuple[str, ...]:
    return ("player1", "player2", "player3")


def _player_name(event: object, field: str, player_id: int) -> str:
    for attr in (f"{field}_name", f"{field}_name_initial", f"{field}_name_i"):
        value = getattr(event, attr, None)
        if value:
            return str(value)
    return str(player_id)


def _infer_team_id_for_player(
    event: object,
    player_id: int,
    current_lineups: Dict[int, List[int]],
    previous_lineups: Dict[int, List[int]],
    player_field: str,
) -> Optional[int]:
    for lineup_team_id, lineup_players in current_lineups.items():
        if player_id in lineup_players:
            return lineup_team_id
    for lineup_team_id, lineup_players in previous_lineups.items():
        if player_id in lineup_players:
            return lineup_team_id
    if player_field == "player1" or (
        _is_substitution_event(event) and player_field == "player2"
    ):
        team_id = getattr(event, "team_id", None)
        try:
            team_int = int(team_id)
        except (TypeError, ValueError):
            return None
        return team_int if team_int > 0 else None
    return None


def _event_sort_key(event: object, index: int) -> Tuple[int, float, int, int]:
    try:
        period = int(getattr(event, "period", 0) or 0)
    except (TypeError, ValueError):
        period = 0
    try:
        seconds_remaining = float(getattr(event, "seconds_remaining", 0.0) or 0.0)
    except (TypeError, ValueError):
        seconds_remaining = 0.0
    event_num = _event_number(event) or 0
    return (period, -seconds_remaining, index, event_num)


def collect_intraperiod_contradictions(
    events: Iterable[object],
    *,
    game_id: str | int | None = None,
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    sorted_events = list(events)

    for event_index, event in enumerate(sorted_events):
        if _is_period_boundary_event(event):
            continue

        current_lineups = _normalize_lineups(getattr(event, "current_players", {}))
        previous_lineups = _normalize_lineups(
            getattr(getattr(event, "previous_event", None), "current_players", {})
        )

        for field in _iter_player_fields():
            raw_player_id = getattr(event, f"{field}_id", None)
            try:
                player_id = int(raw_player_id)
            except (TypeError, ValueError):
                continue
            if player_id <= 0 or player_id >= TEAM_ID_FLOOR:
                continue

            team_id = _infer_team_id_for_player(
                event,
                player_id,
                current_lineups,
                previous_lineups,
                field,
            )
            if not team_id:
                continue

            current_team_lineup = _lineup_for_team(current_lineups, team_id)
            previous_team_lineup = _lineup_for_team(previous_lineups, team_id)
            on_current = player_id in current_team_lineup
            on_previous = player_id in previous_team_lineup

            status = None
            if _is_substitution_event(event):
                if field == "player1" and not on_previous:
                    status = "sub_out_player_missing_from_previous_lineup"
                elif field == "player2" and not on_current:
                    status = "sub_in_player_missing_from_current_lineup"
            else:
                if _is_admin_only_event(event):
                    continue
                if on_current:
                    continue
                status = (
                    "same_clock_boundary_conflict"
                    if on_previous
                    else "off_court_event_credit"
                )

            if status is None:
                continue

            rows.append(
                {
                    "game_id": str(game_id).zfill(10) if game_id is not None else None,
                    "event_index": event_index,
                    "event_num": _event_number(event),
                    "period": int(getattr(event, "period", 0) or 0),
                    "clock": str(getattr(event, "clock", "") or ""),
                    "event_class": _class_name(event),
                    "event_description": _event_description(event),
                    "player_field": field,
                    "player_id": player_id,
                    "player_name": _player_name(event, field, player_id),
                    "team_id": int(team_id),
                    "status": status,
                    "on_current_lineup": bool(on_current),
                    "on_previous_lineup": bool(on_previous),
                    "current_team_lineup": list(current_team_lineup),
                    "previous_team_lineup": list(previous_team_lineup),
                }
            )

    rows.sort(key=lambda row: (row["period"], row["event_index"], row["player_id"], row["team_id"], row["player_field"]))
    return rows


def _same_clock_window_bounds(events: List[object], index: int) -> Tuple[int, int]:
    event = events[index]
    period = getattr(event, "period", None)
    clock = getattr(event, "clock", None)

    start = index
    while start > 0:
        previous_event = events[start - 1]
        if getattr(previous_event, "period", None) != period:
            break
        if getattr(previous_event, "clock", None) != clock:
            break
        start -= 1

    end = index
    while end + 1 < len(events):
        next_event = events[end + 1]
        if getattr(next_event, "period", None) != period:
            break
        if getattr(next_event, "clock", None) != clock:
            break
        end += 1

    return start, end


def _find_period_start_index(events: List[object], start_index: int, period: int) -> int:
    for index in range(start_index, -1, -1):
        if int(getattr(events[index], "period", 0) or 0) != int(period):
            return index + 1
    return 0


def _find_previous_same_team_substitution_index(
    events: List[object],
    *,
    start_index: int,
    period: int,
    team_id: int,
) -> Optional[int]:
    for index in range(start_index - 1, -1, -1):
        event = events[index]
        if int(getattr(event, "period", 0) or 0) != int(period):
            break
        if not _is_substitution_event(event):
            continue
        try:
            event_team_id = int(getattr(event, "team_id"))
        except (TypeError, ValueError):
            continue
        if event_team_id == int(team_id):
            return index
    return None


def _collect_deadball_anchors(
    events: List[object],
    *,
    start_index: int,
    period: int,
    team_id: int,
) -> List[Dict[str, Any]]:
    if start_index < 0 or start_index >= len(events):
        return []

    lower_bound = _find_previous_same_team_substitution_index(
        events,
        start_index=start_index,
        period=period,
        team_id=team_id,
    )
    if lower_bound is None:
        lower_bound = _find_period_start_index(events, start_index, period)

    anchors: List[Dict[str, Any]] = []
    seen_windows = set()
    for index in range(start_index, lower_bound - 1, -1):
        event = events[index]
        if int(getattr(event, "period", 0) or 0) != int(period):
            break
        window = _same_clock_window_bounds(events, index)
        if window in seen_windows:
            continue
        seen_windows.add(window)
        window_start, window_end = window
        if window_start < lower_bound or window_end > start_index:
            continue
        window_events = events[window_start : window_end + 1]
        if not any(_is_deadball_event(window_event) for window_event in window_events):
            continue
        anchor_event = None
        for window_event in window_events:
            if _is_deadball_event(window_event):
                anchor_event = window_event
                break
        if anchor_event is None:
            continue
        anchors.append(
            {
                "anchor_index": window_start,
                "anchor_end_index": window_end,
                "anchor_event_num": _event_number(anchor_event),
                "anchor_clock": str(getattr(anchor_event, "clock", "") or ""),
                "window_start_event_num": _event_number(events[window_start]),
                "window_end_event_num": _event_number(events[window_end]),
                "same_clock_as_first_contradiction": (
                    str(getattr(anchor_event, "clock", "") or "")
                    == str(getattr(events[start_index], "clock", "") or "")
                ),
                "lower_bound_index": lower_bound,
            }
        )
    return anchors


def _find_latest_deadball_anchor(
    events: List[object], *, start_index: int, period: int, team_id: int
) -> Optional[Dict[str, Any]]:
    anchors = _collect_deadball_anchors(
        events,
        start_index=start_index,
        period=period,
        team_id=team_id,
    )
    return anchors[0] if anchors else None


def _deadball_choice_kind(
    anchors: List[Dict[str, Any]], chosen_anchor: Optional[Dict[str, Any]]
) -> Optional[str]:
    if not anchors or chosen_anchor is None:
        return None
    unique_anchor_indices = sorted({int(anchor["anchor_index"]) for anchor in anchors})
    if len(unique_anchor_indices) == 1:
        return "only_winning"
    chosen_index = int(chosen_anchor["anchor_index"])
    if chosen_index == unique_anchor_indices[0]:
        return "earliest_winning"
    if chosen_index == unique_anchor_indices[-1]:
        return "latest_winning"
    return "middle_winning"


def _candidate_anchor_apply_positions(
    deadball_anchor: Dict[str, Any],
    *,
    start_index: int,
) -> List[Dict[str, Any]]:
    positions: List[Dict[str, Any]] = []
    window_start_index = int(deadball_anchor["anchor_index"])
    window_end_index = int(deadball_anchor["anchor_end_index"])

    if window_start_index <= int(start_index):
        positions.append(
            {
                "apply_start_index": window_start_index,
                "deadball_apply_position": "window_start",
            }
        )

    after_window_index = window_end_index + 1
    if after_window_index <= int(start_index) and after_window_index != window_start_index:
        positions.append(
            {
                "apply_start_index": after_window_index,
                "deadball_apply_position": "after_window_end",
            }
        )
    return positions


def _find_next_same_team_substitution_index(
    events: List[object],
    *,
    start_index: int,
    period: int,
    team_id: int,
) -> Optional[int]:
    for index in range(start_index + 1, len(events)):
        event = events[index]
        if int(getattr(event, "period", 0) or 0) != int(period):
            break
        if not _is_substitution_event(event):
            continue
        try:
            event_team_id = int(getattr(event, "team_id"))
        except (TypeError, ValueError):
            continue
        if event_team_id == int(team_id):
            return index
    return None


def _find_period_end_index(events: List[object], start_index: int, period: int) -> int:
    end_index = start_index
    for index in range(start_index + 1, len(events)):
        event = events[index]
        if int(getattr(event, "period", 0) or 0) != int(period):
            break
        end_index = index
    return end_index


def _later_explicit_reentry_support(
    events: List[object],
    *,
    start_index: int,
    period: int,
    team_id: int,
    player_id: int,
) -> bool:
    next_role, _ = _find_next_explicit_sub_role(
        events,
        start_index=start_index,
        period=period,
        team_id=team_id,
        player_id=player_id,
    )
    return next_role == "in"


def _find_next_explicit_sub_role(
    events: List[object],
    *,
    start_index: int,
    period: int,
    team_id: int,
    player_id: int,
) -> Tuple[Optional[str], Optional[int]]:
    for index in range(start_index + 1, len(events)):
        event = events[index]
        if int(getattr(event, "period", 0) or 0) != int(period):
            break
        if not _is_substitution_event(event):
            continue
        try:
            event_team_id = int(getattr(event, "team_id"))
        except (TypeError, ValueError):
            continue
        if event_team_id != int(team_id):
            continue
        try:
            incoming = int(getattr(event, "incoming_player_id"))
        except (TypeError, ValueError):
            incoming = None
        try:
            outgoing = int(getattr(event, "outgoing_player_id"))
        except (TypeError, ValueError):
            outgoing = None
        if incoming == int(player_id):
            return "in", _event_number(event)
        if outgoing == int(player_id):
            return "out", _event_number(event)
    return None, None


def _find_next_explicit_sub_involvement_index(
    events: List[object],
    *,
    start_index: int,
    period: int,
    team_id: int,
    player_ids: Iterable[int],
) -> Optional[int]:
    tracked_ids = {int(player_id) for player_id in player_ids if int(player_id) > 0}
    if not tracked_ids:
        return None
    for index in range(start_index + 1, len(events)):
        event = events[index]
        if int(getattr(event, "period", 0) or 0) != int(period):
            break
        if not _is_substitution_event(event):
            continue
        try:
            event_team_id = int(getattr(event, "team_id"))
        except (TypeError, ValueError):
            continue
        if event_team_id != int(team_id):
            continue
        try:
            incoming = int(getattr(event, "incoming_player_id"))
        except (TypeError, ValueError):
            incoming = None
        try:
            outgoing = int(getattr(event, "outgoing_player_id"))
        except (TypeError, ValueError):
            outgoing = None
        if incoming in tracked_ids or outgoing in tracked_ids:
            return index
    return None


def _find_previous_explicit_sub_role(
    events: List[object],
    *,
    start_index: int,
    period: int,
    team_id: int,
    player_id: int,
) -> Tuple[Optional[str], Optional[int]]:
    for index in range(start_index - 1, -1, -1):
        event = events[index]
        if int(getattr(event, "period", 0) or 0) != int(period):
            continue
        if not _is_substitution_event(event):
            continue
        try:
            event_team_id = int(getattr(event, "team_id"))
        except (TypeError, ValueError):
            continue
        if event_team_id != int(team_id):
            continue
        try:
            incoming = int(getattr(event, "incoming_player_id"))
        except (TypeError, ValueError):
            incoming = None
        try:
            outgoing = int(getattr(event, "outgoing_player_id"))
        except (TypeError, ValueError):
            outgoing = None
        if incoming == int(player_id):
            return "in", _event_number(event)
        if outgoing == int(player_id):
            return "out", _event_number(event)
    return None, None


def _player_has_live_credit(
    events: List[object],
    *,
    start_index: int,
    end_index: int,
    team_id: int,
    player_id: int,
) -> bool:
    for index in range(start_index, min(end_index + 1, len(events))):
        event = events[index]
        if _is_admin_only_event(event):
            continue
        current_lineups = _normalize_lineups(getattr(event, "current_players", {}))
        previous_lineups = _normalize_lineups(
            getattr(getattr(event, "previous_event", None), "current_players", {})
        )
        for field in _iter_player_fields():
            try:
                field_player_id = int(getattr(event, f"{field}_id", None))
            except (TypeError, ValueError):
                continue
            if field_player_id != int(player_id):
                continue
            inferred_team_id = _infer_team_id_for_player(
                event,
                field_player_id,
                current_lineups,
                previous_lineups,
                field,
            )
            if inferred_team_id == int(team_id):
                return True
    return False


def _approx_window_seconds(events: List[object], start_index: int, end_index: int) -> float:
    if start_index < 0 or end_index < start_index or start_index >= len(events):
        return 0.0
    try:
        start_seconds_remaining = float(getattr(events[start_index], "seconds_remaining", 0.0) or 0.0)
    except (TypeError, ValueError):
        start_seconds_remaining = 0.0
    period = int(getattr(events[start_index], "period", 0) or 0)

    end_seconds_remaining = 0.0
    for index in range(end_index + 1, len(events)):
        next_event = events[index]
        if int(getattr(next_event, "period", 0) or 0) != period:
            break
        try:
            end_seconds_remaining = float(getattr(next_event, "seconds_remaining", 0.0) or 0.0)
        except (TypeError, ValueError):
            end_seconds_remaining = 0.0
        break
    return max(0.0, start_seconds_remaining - end_seconds_remaining)


def _evaluate_player_out_candidate(
    events: List[object],
    *,
    period: int,
    team_id: int,
    player_in_id: int,
    player_out_id: int,
    group_rows: List[Dict[str, Any]],
    start_index: int,
    apply_start_index: int,
    contradiction_end_index: int,
    evaluation_end_index: int,
    deadball_anchor: Dict[str, Any],
    deadball_apply_position: str,
    substitution_context: Dict[str, Any],
    period_repeat_contradiction_support: int,
) -> Dict[str, Any]:
    first_row = group_rows[0]
    base_lineup = list(first_row["current_team_lineup"])
    replacement_lineup = [
        player_in_id if player_id == player_out_id else player_id
        for player_id in base_lineup
    ]
    lineup_size_consistency = int(
        len(replacement_lineup) == 5 and len(set(replacement_lineup)) == 5
    )
    contradictions_removed = len(group_rows)
    player_out_live_credit = _player_has_live_credit(
        events,
        start_index=apply_start_index,
        end_index=evaluation_end_index,
        team_id=team_id,
        player_id=player_out_id,
    )
    new_contradictions_introduced = 1 if player_out_live_credit else 0
    next_player_out_role, _ = _find_next_explicit_sub_role(
        events,
        start_index=contradiction_end_index,
        period=period,
        team_id=team_id,
        player_id=player_out_id,
    )
    later_reentry_support = int(next_player_out_role == "in")
    later_explicit_sub_out_penalty = int(next_player_out_role == "out")
    player_out_silence_support = int(not player_out_live_credit)
    same_clock_cluster_consistency = int(
        deadball_anchor["same_clock_as_first_contradiction"]
        or any(
            row["status"] == "same_clock_boundary_conflict"
            for row in group_rows
        )
    )
    player_in_next_role, _ = _find_next_explicit_sub_role(
        events,
        start_index=contradiction_end_index,
        period=period,
        team_id=team_id,
        player_id=player_in_id,
    )
    player_in_later_sub_out_support = int(player_in_next_role == "out")
    incoming_missing_current_support = int(
        substitution_context["matching_sub_in_missing_current"]
    )
    matching_sub_out_candidate_support = int(
        substitution_context["matching_sub_out_player_id"] == int(player_out_id)
    )
    previous_player_in_role, _ = _find_previous_explicit_sub_role(
        events,
        start_index=start_index,
        period=period,
        team_id=team_id,
        player_id=player_in_id,
    )
    previous_player_out_role, _ = _find_previous_explicit_sub_role(
        events,
        start_index=start_index,
        period=period,
        team_id=team_id,
        player_id=player_out_id,
    )
    prior_player_in_sub_out_support = int(previous_player_in_role == "out")
    prior_player_out_sub_in_support = int(previous_player_out_role == "in")
    prior_complementary_swap_support = int(
        prior_player_in_sub_out_support == 1 and prior_player_out_sub_in_support == 1
    )
    prior_repeat_swap_support = int(
        prior_complementary_swap_support == 1
        and int(period_repeat_contradiction_support) == 1
    )
    broken_substitution_context = int(substitution_context["broken_substitution_context"])
    local_confidence_score = (
        contradictions_removed * 100
        - new_contradictions_introduced * 1000
        + later_reentry_support * 20
        - later_explicit_sub_out_penalty * 25
        + player_in_later_sub_out_support * 15
        + player_out_silence_support * 10
        + incoming_missing_current_support * 15
        + matching_sub_out_candidate_support * 40
        + prior_player_in_sub_out_support * 10
        + prior_player_out_sub_in_support * 10
        + prior_complementary_swap_support * 30
        + prior_repeat_swap_support * 20
        + same_clock_cluster_consistency * 5
        + lineup_size_consistency
        - broken_substitution_context * 1000
    )
    return {
        "player_out_id": int(player_out_id),
        "replacement_lineup": replacement_lineup,
        "contradictions_removed": contradictions_removed,
        "new_contradictions_introduced": new_contradictions_introduced,
        "later_explicit_reentry_support": later_reentry_support,
        "later_explicit_sub_out_penalty": later_explicit_sub_out_penalty,
        "player_in_later_sub_out_support": player_in_later_sub_out_support,
        "player_out_silence_support": player_out_silence_support,
        "incoming_missing_current_support": incoming_missing_current_support,
        "matching_sub_out_candidate_support": matching_sub_out_candidate_support,
        "prior_player_in_sub_out_support": prior_player_in_sub_out_support,
        "prior_player_out_sub_in_support": prior_player_out_sub_in_support,
        "prior_complementary_swap_support": prior_complementary_swap_support,
        "period_repeat_contradiction_support": int(period_repeat_contradiction_support),
        "prior_repeat_swap_support": prior_repeat_swap_support,
        "broken_substitution_context": broken_substitution_context,
        "same_clock_cluster_consistency": same_clock_cluster_consistency,
        "lineup_size_consistency": lineup_size_consistency,
        "local_confidence_score": local_confidence_score,
        "deadball_anchor": dict(deadball_anchor),
        "apply_start_index": int(apply_start_index),
        "deadball_apply_position": str(deadball_apply_position),
    }


def _promotion_decision(
    best_score: Optional[int],
    runner_up_score: Optional[int],
    best_eval: Optional[Dict[str, Any]],
    *,
    deadball_anchor: Optional[Dict[str, Any]],
) -> Tuple[bool, str]:
    if best_eval is None:
        return False, "no_candidate"
    if deadball_anchor is None:
        return False, "no_deadball_anchor"
    if best_eval["new_contradictions_introduced"] > 0:
        return False, "introduces_new_contradiction"
    if best_eval["broken_substitution_context"] > 0:
        return False, "broken_substitution_context"
    if best_eval["contradictions_removed"] <= 0:
        return False, "removes_no_contradiction"
    if best_eval["lineup_size_consistency"] != 1:
        return False, "invalid_lineup_size"
    if best_eval["player_out_silence_support"] != 1:
        return False, "outgoing_player_not_silent"
    context_support_total = (
        best_eval["later_explicit_reentry_support"]
        + best_eval["player_in_later_sub_out_support"]
        + best_eval["incoming_missing_current_support"]
        + best_eval["matching_sub_out_candidate_support"]
        + best_eval["prior_repeat_swap_support"]
    )
    if context_support_total <= 0:
        return False, "insufficient_local_context"
    if (
        best_eval.get("deadball_apply_position") == "after_window_end"
        and (
            best_eval["later_explicit_reentry_support"]
            + best_eval["matching_sub_out_candidate_support"]
            + best_eval["prior_repeat_swap_support"]
        )
        <= 0
    ):
        return False, "after_window_requires_stronger_context"
    if runner_up_score is not None and best_score is not None and best_score - runner_up_score < 10:
        return False, "ambiguous_runner_up"
    if best_score is None or best_score < 110:
        return False, "low_local_confidence"
    return True, "auto_apply"


def _build_substitution_context(
    contradictions: List[Dict[str, Any]],
    *,
    player_in_id: int,
    period: int,
    team_id: int,
    window_start_index: int,
    window_end_index: int,
    current_team_lineup: List[int],
) -> Dict[str, Any]:
    relevant_rows = [
        row
        for row in contradictions
        if row["period"] == period
        and row["team_id"] == team_id
        and window_start_index <= int(row["event_index"]) <= window_end_index
        and row["status"] in SUBSTITUTION_SIGNAL_STATUSES
    ]
    sub_in_rows = [
        row
        for row in relevant_rows
        if row["status"] == "sub_in_player_missing_from_current_lineup"
        and int(row["player_id"]) == int(player_in_id)
    ]
    if not sub_in_rows:
        return {
            "matching_sub_in_missing_current": False,
            "matching_sub_out_player_id": None,
            "broken_substitution_context": False,
        }

    sub_in_row = sorted(
        sub_in_rows,
        key=lambda row: (int(row["event_index"]), int(row["event_num"] or 0)),
    )[0]
    matching_sub_out_rows = [
        row
        for row in relevant_rows
        if row["status"] == "sub_out_player_missing_from_previous_lineup"
        and row["event_index"] == sub_in_row["event_index"]
    ]
    if not matching_sub_out_rows:
        matching_sub_out_rows = [
            row
            for row in relevant_rows
            if row["status"] == "sub_out_player_missing_from_previous_lineup"
            and row["clock"] == sub_in_row["clock"]
        ]
    matching_sub_out_player_id = (
        int(matching_sub_out_rows[0]["player_id"]) if matching_sub_out_rows else None
    )
    broken_substitution_context = bool(
        matching_sub_out_player_id is not None
        and matching_sub_out_player_id not in current_team_lineup
    )
    return {
        "matching_sub_in_missing_current": True,
        "matching_sub_out_player_id": matching_sub_out_player_id,
        "broken_substitution_context": broken_substitution_context,
    }


def _find_repair_propagation_end_index(
    events: List[object],
    *,
    start_index: int,
    period: int,
    team_id: int,
    player_in_id: int,
    player_out_id: int,
) -> int:
    next_involvement_index = _find_next_explicit_sub_involvement_index(
        events,
        start_index=start_index,
        period=period,
        team_id=team_id,
        player_ids=[player_in_id, player_out_id],
    )
    if next_involvement_index is not None:
        return max(start_index, next_involvement_index - 1)
    return _find_period_end_index(events, start_index, period)


def _build_propagated_override_event_indices(
    events: List[object],
    *,
    start_index: int,
    end_index: int,
    team_id: int,
    player_in_id: int,
    player_out_id: int,
) -> List[int]:
    indices: List[int] = []
    for index in range(start_index, min(end_index + 1, len(events))):
        current_lineups = _normalize_lineups(getattr(events[index], "current_players", {}))
        team_lineup = _lineup_for_team(current_lineups, team_id)
        if len(team_lineup) != 5:
            continue
        if player_in_id in team_lineup:
            continue
        if player_out_id not in team_lineup:
            continue
        indices.append(index)
    return indices


def build_intraperiod_missing_sub_candidates(
    events: Iterable[object],
    *,
    game_id: str | int | None = None,
) -> List[Dict[str, Any]]:
    raw_events = list(events)
    sorted_events = [
        event
        for _, event in sorted(
            enumerate(raw_events),
            key=lambda item: _event_sort_key(item[1], item[0]),
        )
    ]
    contradictions = collect_intraperiod_contradictions(sorted_events, game_id=game_id)
    candidates: List[Dict[str, Any]] = []
    consumed_contradictions = set()

    for contradiction_index, row in enumerate(contradictions):
        if contradiction_index in consumed_contradictions:
            continue
        if row["status"] not in LINEUP_SIGNAL_STATUSES:
            continue

        start_index = int(row["event_index"])
        period = int(row["period"])
        team_id = int(row["team_id"])
        player_in_id = int(row["player_id"])

        next_sub_index = _find_next_same_team_substitution_index(
            sorted_events,
            start_index=start_index,
            period=period,
            team_id=team_id,
        )
        group_end_limit = (
            next_sub_index - 1
            if next_sub_index is not None
            else _find_period_end_index(sorted_events, start_index, period)
        )

        group_positions: List[int] = []
        group_rows: List[Dict[str, Any]] = []
        for candidate_index in range(contradiction_index, len(contradictions)):
            other = contradictions[candidate_index]
            if other["period"] != period or other["team_id"] != team_id or other["player_id"] != player_in_id:
                continue
            if other["event_index"] < start_index or other["event_index"] > group_end_limit:
                continue
            if other["status"] not in LINEUP_SIGNAL_STATUSES:
                continue
            group_positions.append(candidate_index)
            group_rows.append(other)

        if not group_rows:
            continue
        consumed_contradictions.update(group_positions)

        first_row = group_rows[0]
        current_team_lineup = list(first_row["current_team_lineup"])
        deadball_anchors = _collect_deadball_anchors(
            sorted_events,
            start_index=start_index,
            period=period,
            team_id=team_id,
        )
        period_repeat_contradiction_support = int(
            sum(
                1
                for other in contradictions
                if other["period"] == period
                and other["team_id"] == team_id
                and other["player_id"] == player_in_id
                and other["status"] in LINEUP_SIGNAL_STATUSES
            )
            >= 2
        )
        if len(current_team_lineup) != 5 or player_in_id in current_team_lineup:
            deadball_anchor = deadball_anchors[0] if deadball_anchors else None
            candidates.append(
                {
                    "game_id": row["game_id"],
                    "period": period,
                    "team_id": team_id,
                    "player_in_id": player_in_id,
                    "player_out_id": None,
                    "first_contradicted_event_num": first_row["event_num"],
                    "last_contradicted_event_num": group_rows[-1]["event_num"],
                    "start_event_num": first_row["event_num"],
                    "end_event_num": group_rows[-1]["event_num"],
                    "deadball_event_num": deadball_anchor["anchor_event_num"] if deadball_anchor else None,
                    "deadball_clock": deadball_anchor["anchor_clock"] if deadball_anchor else None,
                    "deadball_window_start_event_num": (
                        deadball_anchor["window_start_event_num"] if deadball_anchor else None
                    ),
                    "deadball_window_end_event_num": (
                        deadball_anchor["window_end_event_num"] if deadball_anchor else None
                    ),
                    "runner_up_deadball_event_num": None,
                    "runner_up_deadball_clock": None,
                    "runner_up_deadball_window_start_event_num": None,
                    "runner_up_deadball_window_end_event_num": None,
                    "deadball_apply_position": None,
                    "runner_up_deadball_apply_position": None,
                    "deadball_choice_kind": _deadball_choice_kind(deadball_anchors, deadball_anchor),
                    "best_vs_runner_up_confidence_gap": None,
                    "forward_simulation_contradiction_delta": 0,
                    "contradictions_removed": 0,
                    "new_contradictions_introduced": 0,
                    "later_explicit_reentry_support": 0,
                    "player_out_silence_support": 0,
                    "same_clock_cluster_consistency": 0,
                    "lineup_size_consistency": 0,
                    "local_confidence_score": None,
                    "runner_up_local_confidence_score": None,
                    "approx_window_seconds": _approx_window_seconds(
                        sorted_events, start_index, int(group_rows[-1]["event_index"])
                    ),
                    "promotion_decision": "invalid_base_lineup",
                    "auto_apply": False,
                    "override_lineup_player_ids": None,
                    "override_event_indices": [],
                    "contradiction_status_counts": json.dumps(
                        dict(Counter(item["status"] for item in group_rows)),
                        sort_keys=True,
                    ),
                    "evidence_event_nums": json.dumps(
                        sorted(
                            {
                                int(event_num)
                                for event_num in [item["event_num"] for item in group_rows]
                                if event_num is not None
                            }
                        )
                    ),
                }
            )
            continue

        deadball_anchor = deadball_anchors[0] if deadball_anchors else None
        last_index = int(group_rows[-1]["event_index"])
        evaluations = []
        for candidate_anchor in deadball_anchors:
            for apply_position in _candidate_anchor_apply_positions(
                candidate_anchor,
                start_index=start_index,
            ):
                apply_start_index = int(apply_position["apply_start_index"])
                substitution_context = _build_substitution_context(
                    contradictions,
                    player_in_id=player_in_id,
                    period=period,
                    team_id=team_id,
                    window_start_index=int(candidate_anchor["anchor_index"]),
                    window_end_index=group_end_limit,
                    current_team_lineup=current_team_lineup,
                )
                for player_out_id in current_team_lineup:
                    if int(player_out_id) == player_in_id:
                        continue
                    evaluations.append(
                        _evaluate_player_out_candidate(
                            sorted_events,
                            period=period,
                            team_id=team_id,
                            player_in_id=player_in_id,
                            player_out_id=player_out_id,
                            group_rows=group_rows,
                            start_index=start_index,
                            apply_start_index=apply_start_index,
                            contradiction_end_index=last_index,
                            evaluation_end_index=group_end_limit,
                            deadball_anchor=candidate_anchor,
                            deadball_apply_position=str(apply_position["deadball_apply_position"]),
                            substitution_context=substitution_context,
                            period_repeat_contradiction_support=period_repeat_contradiction_support,
                        )
                    )
        evaluations.sort(
            key=lambda item: (
                item["local_confidence_score"],
                item["contradictions_removed"],
                -item["new_contradictions_introduced"],
                item["later_explicit_reentry_support"],
                item["player_out_silence_support"],
                item["apply_start_index"],
            ),
            reverse=True,
        )
        best_eval = evaluations[0] if evaluations else None
        runner_up_eval = next(
            (
                item
                for item in evaluations[1:]
                if best_eval is not None and int(item["player_out_id"]) != int(best_eval["player_out_id"])
            ),
            None,
        )
        runner_up_score = (
            runner_up_eval["local_confidence_score"] if runner_up_eval is not None else None
        )
        best_score = best_eval["local_confidence_score"] if best_eval is not None else None
        auto_apply, decision = _promotion_decision(
            best_score,
            runner_up_score,
            best_eval,
            deadball_anchor=deadball_anchor,
        )

        override_event_indices: List[int] = []
        override_start_event_num = None
        override_end_event_num = None
        chosen_deadball_anchor = best_eval["deadball_anchor"] if best_eval is not None else deadball_anchor
        approx_window_seconds = _approx_window_seconds(
            sorted_events,
            int(best_eval["apply_start_index"]) if best_eval is not None else (
                int(chosen_deadball_anchor["anchor_index"]) if chosen_deadball_anchor else start_index
            ),
            last_index,
        )
        if auto_apply and best_eval is not None:
            propagation_end_index = _find_repair_propagation_end_index(
                sorted_events,
                start_index=int(best_eval["apply_start_index"]),
                period=period,
                team_id=team_id,
                player_in_id=player_in_id,
                player_out_id=int(best_eval["player_out_id"]),
            )
            override_event_indices = _build_propagated_override_event_indices(
                sorted_events,
                start_index=int(best_eval["apply_start_index"]),
                end_index=propagation_end_index,
                team_id=team_id,
                player_in_id=player_in_id,
                player_out_id=int(best_eval["player_out_id"]),
            )
            if override_event_indices:
                override_start_event_num = _event_number(sorted_events[override_event_indices[0]])
                override_end_event_num = _event_number(sorted_events[override_event_indices[-1]])
                approx_window_seconds = _approx_window_seconds(
                    sorted_events,
                    override_event_indices[0],
                    override_event_indices[-1],
                )
                if approx_window_seconds <= 0:
                    auto_apply = False
                    decision = "zero_duration_window"
                    override_event_indices = []
                    override_start_event_num = None
                    override_end_event_num = None
            else:
                auto_apply = False
                decision = "no_effective_window"

        candidate = {
            "game_id": row["game_id"],
            "period": period,
            "team_id": team_id,
            "player_in_id": player_in_id,
            "player_out_id": best_eval["player_out_id"] if best_eval is not None else None,
            "first_contradicted_event_num": first_row["event_num"],
            "last_contradicted_event_num": group_rows[-1]["event_num"],
            "start_event_num": first_row["event_num"],
            "end_event_num": group_rows[-1]["event_num"],
            "deadball_event_num": (
                chosen_deadball_anchor["anchor_event_num"] if chosen_deadball_anchor else None
            ),
            "deadball_clock": (
                chosen_deadball_anchor["anchor_clock"] if chosen_deadball_anchor else None
            ),
            "deadball_window_start_event_num": (
                chosen_deadball_anchor["window_start_event_num"] if chosen_deadball_anchor else None
            ),
            "deadball_window_end_event_num": (
                chosen_deadball_anchor["window_end_event_num"] if chosen_deadball_anchor else None
            ),
            "runner_up_deadball_event_num": (
                runner_up_eval["deadball_anchor"]["anchor_event_num"]
                if runner_up_eval is not None
                else None
            ),
            "runner_up_deadball_clock": (
                runner_up_eval["deadball_anchor"]["anchor_clock"]
                if runner_up_eval is not None
                else None
            ),
            "runner_up_deadball_window_start_event_num": (
                runner_up_eval["deadball_anchor"]["window_start_event_num"]
                if runner_up_eval is not None
                else None
            ),
            "runner_up_deadball_window_end_event_num": (
                runner_up_eval["deadball_anchor"]["window_end_event_num"]
                if runner_up_eval is not None
                else None
            ),
            "deadball_apply_position": (
                best_eval["deadball_apply_position"] if best_eval is not None else None
            ),
            "runner_up_deadball_apply_position": (
                runner_up_eval["deadball_apply_position"] if runner_up_eval is not None else None
            ),
            "deadball_choice_kind": _deadball_choice_kind(deadball_anchors, chosen_deadball_anchor),
            "best_vs_runner_up_confidence_gap": (
                int(best_score - runner_up_score)
                if best_score is not None and runner_up_score is not None
                else None
            ),
            "forward_simulation_contradiction_delta": (
                int(best_eval["contradictions_removed"] - best_eval["new_contradictions_introduced"])
                if best_eval
                else 0
            ),
            "contradictions_removed": best_eval["contradictions_removed"] if best_eval else 0,
            "new_contradictions_introduced": best_eval["new_contradictions_introduced"] if best_eval else 0,
            "later_explicit_reentry_support": best_eval["later_explicit_reentry_support"] if best_eval else 0,
            "later_explicit_sub_out_penalty": best_eval["later_explicit_sub_out_penalty"] if best_eval else 0,
            "player_in_later_sub_out_support": best_eval["player_in_later_sub_out_support"] if best_eval else 0,
            "player_out_silence_support": best_eval["player_out_silence_support"] if best_eval else 0,
            "incoming_missing_current_support": best_eval["incoming_missing_current_support"] if best_eval else 0,
            "matching_sub_out_candidate_support": best_eval["matching_sub_out_candidate_support"] if best_eval else 0,
            "prior_player_in_sub_out_support": best_eval["prior_player_in_sub_out_support"] if best_eval else 0,
            "prior_player_out_sub_in_support": best_eval["prior_player_out_sub_in_support"] if best_eval else 0,
            "prior_complementary_swap_support": best_eval["prior_complementary_swap_support"] if best_eval else 0,
            "period_repeat_contradiction_support": best_eval["period_repeat_contradiction_support"] if best_eval else 0,
            "prior_repeat_swap_support": best_eval["prior_repeat_swap_support"] if best_eval else 0,
            "broken_substitution_context": best_eval["broken_substitution_context"] if best_eval else 0,
            "same_clock_cluster_consistency": best_eval["same_clock_cluster_consistency"] if best_eval else 0,
            "lineup_size_consistency": best_eval["lineup_size_consistency"] if best_eval else 0,
            "local_confidence_score": best_score,
            "runner_up_local_confidence_score": runner_up_score,
            "approx_window_seconds": approx_window_seconds,
            "promotion_decision": decision,
            "auto_apply": auto_apply,
            "override_lineup_player_ids": best_eval["replacement_lineup"] if best_eval else None,
            "override_event_indices": override_event_indices,
            "override_start_event_num": override_start_event_num,
            "override_end_event_num": override_end_event_num,
            "contradiction_status_counts": json.dumps(
                dict(Counter(item["status"] for item in group_rows)),
                sort_keys=True,
            ),
            "evidence_event_nums": json.dumps(
                sorted(
                    {
                        int(event_num)
                        for event_num in [item["event_num"] for item in group_rows]
                        if event_num is not None
                    }
                )
            ),
        }
        candidates.append(candidate)

    accepted_indices_by_team: Dict[int, set[int]] = {}
    for candidate in sorted(
        candidates,
        key=lambda item: (
            0 if item["auto_apply"] else 1,
            -(item["local_confidence_score"] or -10**9),
            item["period"],
            item["start_event_num"] or 0,
        ),
    ):
        if not candidate["auto_apply"]:
            continue
        team_id = int(candidate["team_id"])
        override_indices = {int(index) for index in candidate.get("override_event_indices", [])}
        if accepted_indices_by_team.get(team_id, set()) & override_indices:
            candidate["auto_apply"] = False
            candidate["promotion_decision"] = "rejected_overlap"
            candidate["override_event_indices"] = []
            candidate["override_start_event_num"] = None
            candidate["override_end_event_num"] = None
            continue
        accepted_indices_by_team.setdefault(team_id, set()).update(override_indices)

    candidates.sort(
        key=lambda item: (
            int(item["period"]),
            int(item["team_id"]),
            int(item["start_event_num"] or 0),
            int(item["player_in_id"]),
        )
    )
    return candidates


def build_generated_lineup_override_lookup(
    events: Iterable[object],
    *,
    game_id: str | int | None = None,
) -> Tuple[Dict[int, Dict[int, List[int]]], List[Dict[str, Any]]]:
    raw_events = list(events)
    sorted_events = [
        event
        for _, event in sorted(
            enumerate(raw_events),
            key=lambda item: _event_sort_key(item[1], item[0]),
        )
    ]
    candidates = build_intraperiod_missing_sub_candidates(sorted_events, game_id=game_id)
    lookup: Dict[int, Dict[int, List[int]]] = {}
    for candidate in candidates:
        if not candidate["auto_apply"]:
            continue
        team_id = int(candidate["team_id"])
        player_in_id = int(candidate["player_in_id"])
        player_out_id = int(candidate["player_out_id"] or 0)
        if player_out_id <= 0:
            continue
        for event_index in candidate.get("override_event_indices", []):
            try:
                event = sorted_events[int(event_index)]
            except (IndexError, TypeError, ValueError):
                continue
            current_lineups = _normalize_lineups(getattr(event, "current_players", {}))
            team_lineup = _lineup_for_team(current_lineups, team_id)
            if len(team_lineup) != 5:
                continue
            if player_in_id in team_lineup or player_out_id not in team_lineup:
                continue
            replacement_lineup = [
                player_in_id if player_id == player_out_id else player_id
                for player_id in team_lineup
            ]
            if len(set(replacement_lineup)) != 5:
                continue
            lookup.setdefault(int(event_index), {})[team_id] = replacement_lineup
    return lookup, candidates
