from __future__ import annotations

from collections import deque
from typing import Any, Dict, Iterable, List, Tuple

import pandas as pd


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
